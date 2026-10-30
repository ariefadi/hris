from django.http import JsonResponse
from django.shortcuts import redirect, render
from django.utils.decorators import method_decorator
from django.views import View
from django.views.decorators.csrf import csrf_exempt

from management.payment_db import (
    build_payment_summary,
    enrich_payments_with_estimates,
    insert_transactions,
    list_payment_credentials,
    list_payments,
    list_transactions,
    serialize_txn_rows,
    upsert_payments,
)
from management.payment_insights import build_payment_insights
from management.payment_utils import fetch_adsense_payments_for_mail, parse_payment_csv


def _require_admin(request):
    return request.session.get('hris_admin')


def _account_ids(request):
    raw = str(request.GET.get('selected_account') or request.POST.get('selected_account') or '').strip()
    return [part.strip() for part in raw.split(',') if part.strip()]


def _page_accounts(source, admin):
    return list_payment_credentials(source, admin)


def _resolve_account(source, admin, account_id):
    account_id = str(account_id or '').strip()
    for item in _page_accounts(source, admin):
        if str(item.get('account_id') or '') == account_id:
            return item
    return None


class PaymentAdsenseView(View):
    def dispatch(self, request, *args, **kwargs):
        if not _require_admin(request):
            return redirect('/management/admin/login')
        return super().dispatch(request, *args, **kwargs)

    def get(self, request):
        admin = request.session.get('hris_admin', {})
        return render(request, 'admin/report_payment/adsense/index.html', {
            'title': 'Payment AdSense',
            'user': admin,
            'data_account': _page_accounts('adsense', admin),
            'can_sync': True,
            'source': 'adsense',
        })


class PaymentAdxView(View):
    def dispatch(self, request, *args, **kwargs):
        if not _require_admin(request):
            return redirect('/management/admin/login')
        return super().dispatch(request, *args, **kwargs)

    def get(self, request):
        admin = request.session.get('hris_admin', {})
        return render(request, 'admin/report_payment/adx/index.html', {
            'title': 'Payment AdX',
            'user': admin,
            'data_account': _page_accounts('adx', admin),
            'can_sync': False,
            'source': 'adx',
        })


class _PaymentDataView(View):
    source = ''

    def dispatch(self, request, *args, **kwargs):
        if not _require_admin(request):
            return JsonResponse({'status': False, 'error': 'Unauthorized'}, status=401)
        return super().dispatch(request, *args, **kwargs)

    def get(self, request):
        try:
            account_ids = _account_ids(request)
            status = str(request.GET.get('status') or '').strip().lower()
            start_date = str(request.GET.get('start_date') or '').strip()
            end_date = str(request.GET.get('end_date') or '').strip()
            category = str(request.GET.get('category') or '').strip().lower()
            policy_only = str(request.GET.get('policy_only') or '').strip() in ('1', 'true', 'yes')
            payments = enrich_payments_with_estimates(
                self.source,
                list_payments(
                    self.source,
                    account_ids=account_ids,
                    status=status or None,
                    start_date=start_date or None,
                    end_date=end_date or None,
                ),
            )
            transactions = serialize_txn_rows(list_transactions(
                self.source,
                account_ids=account_ids,
                start_date=start_date or None,
                end_date=end_date or None,
                category=category or None,
                policy_only=policy_only,
            ))
            try:
                insights = build_payment_insights(
                    self.source,
                    transactions,
                    account_ids=account_ids,
                    start_date=start_date or None,
                    end_date=end_date or None,
                )
            except Exception:
                insights = {'invalid': {}, 'account_ads': {}}
            return JsonResponse({
                'status': True,
                'data': {
                    'payments': payments,
                    'transactions': transactions,
                    'summary': build_payment_summary(payments, transactions),
                    'invalid': insights.get('invalid') or {},
                    'account_ads': insights.get('account_ads') or {},
                },
            })
        except Exception as exc:
            return JsonResponse({'status': False, 'error': str(exc)})


class PaymentAdsenseDataView(_PaymentDataView):
    source = 'adsense'


class PaymentAdxDataView(_PaymentDataView):
    source = 'adx'


@method_decorator(csrf_exempt, name='dispatch')
class PaymentAdsenseSyncView(View):
    def dispatch(self, request, *args, **kwargs):
        if not _require_admin(request):
            return JsonResponse({'status': False, 'error': 'Unauthorized'}, status=401)
        return super().dispatch(request, *args, **kwargs)

    def post(self, request):
        admin = request.session.get('hris_admin', {})
        selected_ids = set(_account_ids(request))
        accounts = _page_accounts('adsense', admin)
        if selected_ids:
            accounts = [item for item in accounts if str(item.get('account_id') or '') in selected_ids]
        if not accounts:
            return JsonResponse({'status': False, 'error': 'Tidak ada akun AdSense yang bisa ditarik'})

        by_mail = {}
        for account in accounts:
            mail = str(account.get('user_mail') or '').strip()
            if not mail:
                continue
            by_mail.setdefault(mail, []).append(account)

        pulled = 0
        saved = 0
        errors = []
        synced_accounts = 0
        for user_mail, mail_accounts in by_mail.items():
            result = fetch_adsense_payments_for_mail(user_mail)
            if not result.get('status'):
                errors.append(f"{user_mail}: {result.get('error') or 'gagal menarik payments'}")
                continue
            payments = result.get('data') or []
            pulled += len(payments)
            synced_accounts += 1
            for account in mail_accounts:
                saved += upsert_payments('adsense', account, payments, source_kind='api')
            for extra in result.get('errors') or []:
                errors.append(f'{user_mail}: {extra}')

        return JsonResponse({
            'status': True if pulled or not errors else False,
            'message': (
                f'Berhasil menarik {pulled} baris payment dari {synced_accounts} kredensial.'
                if pulled else 'Tidak ada payment yang ditarik.'
            ),
            'saved': saved,
            'errors': errors,
        })


@method_decorator(csrf_exempt, name='dispatch')
class _PaymentImportView(View):
    source = ''

    def dispatch(self, request, *args, **kwargs):
        if not _require_admin(request):
            return JsonResponse({'status': False, 'error': 'Unauthorized'}, status=401)
        return super().dispatch(request, *args, **kwargs)

    def post(self, request):
        admin = request.session.get('hris_admin', {})
        account_id = str(request.POST.get('account_id') or '').strip()
        upload = request.FILES.get('file')
        if not account_id:
            return JsonResponse({'status': False, 'error': 'Pilih akun tujuan import'})
        if not upload:
            return JsonResponse({'status': False, 'error': 'Unggah file CSV/Excel Transactions atau Payments'})
        account = _resolve_account(self.source, admin, account_id)
        if not account:
            return JsonResponse({'status': False, 'error': 'Akun tidak ditemukan atau tidak punya akses'})
        try:
            rows = parse_payment_csv(upload, upload.name)
        except Exception as exc:
            return JsonResponse({'status': False, 'error': str(exc)})
        if not rows:
            return JsonResponse({'status': False, 'error': 'File tidak berisi baris transaksi yang bisa dibaca'})
        inserted, skipped = insert_transactions(self.source, account, rows, source_file=upload.name)
        return JsonResponse({
            'status': True,
            'message': f'Import selesai: {inserted} baris baru, {skipped} duplikat dilewati.',
            'inserted': inserted,
            'skipped': skipped,
            'parsed': len(rows),
        })


class PaymentAdsenseImportView(_PaymentImportView):
    source = 'adsense'


class PaymentAdxImportView(_PaymentImportView):
    source = 'adx'

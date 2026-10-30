import json

from django.http import JsonResponse
from django.shortcuts import redirect, render
from django.views import View
from django.views.decorators.csrf import csrf_exempt
from django.utils.decorators import method_decorator

from management.database import data_mysql
from management.domain_expiry import fetch_domain_detail, fetch_expiry, registrable_domain
from management.domain_expiry_db import (
    alert_items,
    ensure_table,
    google_domain_row,
    list_inventory,
    local_domain_record,
    upsert_result,
)


def _require_admin(request):
    return request.session.get('hris_admin')


def ensure_domain_checker_menu(db=None, admin=None):
    db = db or data_mysql()
    admin = admin or {}
    targets = (
        '/management/admin/domain_checker',
        'management/admin/domain_checker',
    )
    try:
        placeholders = ','.join(['%s'] * len(targets))
        if db.execute_query(f'SELECT nav_id FROM app_menu WHERE nav_url IN ({placeholders}) LIMIT 1', targets):
            if db.cur_hris.fetchone():
                return
        sql_ref = '''
            SELECT nav_id, portal_id, nav_parent, nav_order, nav_url
            FROM app_menu
            WHERE nav_url LIKE %s
            ORDER BY LENGTH(nav_url) DESC
            LIMIT 1
        '''
        ref = None
        for like in ('%payment_adx%', '%adx_account%', '%adsense_account%', '%dashboard%'):
            if db.execute_query(sql_ref, (like,)):
                ref = db.cur_hris.fetchone()
                if ref:
                    break
        if not ref:
            return
        from settings.sistem import generate_next_nav_id
        portal_id = ref.get('portal_id')
        nav_id = generate_next_nav_id(db, portal_id)
        if not nav_id:
            return
        ref_url = str(ref.get('nav_url') or '')
        nav_url = '/management/admin/domain_checker' if str(ref_url).startswith('/') else 'management/admin/domain_checker'
        db.execute_query(
            '''
            INSERT INTO app_menu
            (nav_id, portal_id, nav_name, nav_url, nav_icon, nav_parent, nav_order, active_st, display_st, mdb, mdb_name, mdd)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
            ''',
            (
                nav_id,
                portal_id,
                'Domain Expired',
                nav_url,
                'bi bi-calendar2-x',
                ref.get('nav_parent') or '',
                int(ref.get('nav_order') or 0) + 1,
                '1',
                '1',
                admin.get('user_id', ''),
                admin.get('user_alias', ''),
            ),
        )
        roles = []
        if db.execute_query('SELECT role_id, role_tp FROM app_menu_role WHERE nav_id = %s', (ref.get('nav_id'),)):
            roles = db.fetch_all() or []
        for role in roles:
            db.execute_query(
                'INSERT INTO app_menu_role (role_id, nav_id, role_tp) VALUES (%s, %s, %s)',
                (role.get('role_id'), nav_id, role.get('role_tp') or '1111'),
            )
        db.commit()
    except Exception as exc:
        print(f'[ERROR] Gagal membuat menu Domain Expired: {exc}')


class DomainCheckerView(View):
    def dispatch(self, request, *args, **kwargs):
        if not _require_admin(request):
            return redirect('admin_login')
        return super().dispatch(request, *args, **kwargs)

    def get(self, request):
        admin = request.session.get('hris_admin', {})
        db = data_mysql()
        ensure_table(db)
        ensure_domain_checker_menu(db, admin)
        return render(request, 'admin/domain_checker/index.html', {
            'title': 'Domain Expired',
            'user': admin,
        })


class DomainCheckerDataView(View):
    def dispatch(self, request, *args, **kwargs):
        if not _require_admin(request):
            return JsonResponse({'status': False, 'error': 'Unauthorized'}, status=401)
        return super().dispatch(request, *args, **kwargs)

    def get(self, request):
        data = list_inventory()
        return JsonResponse({'status': True, **data})


@method_decorator(csrf_exempt, name='dispatch')
class DomainCheckerCheckView(View):
    def dispatch(self, request, *args, **kwargs):
        if not _require_admin(request):
            return JsonResponse({'status': False, 'error': 'Unauthorized'}, status=401)
        return super().dispatch(request, *args, **kwargs)

    def post(self, request):
        try:
            payload = json.loads(request.body.decode('utf-8') or '{}')
        except Exception:
            payload = {}
        raw_list = payload.get('domains') or []
        if isinstance(raw_list, str):
            raw_list = [part.strip() for part in raw_list.split(',') if part.strip()]
        if not raw_list:
            inventory = list_inventory()
            raw_list = [row.get('domain') for row in inventory.get('rows') or [] if row.get('domain')]
        domains = []
        seen = set()
        for raw in raw_list:
            domain = registrable_domain(raw)
            if domain and domain not in seen:
                seen.add(domain)
                domains.append(domain)
        results = []
        for domain in domains[:12]:
            result = fetch_expiry(domain)
            upsert_result(result)
            results.append(result)
        remaining = max(0, len(domains) - len(results))
        return JsonResponse({
            'status': True,
            'checked': results,
            'remaining': remaining,
        })


class DomainCheckerDetailView(View):
    def dispatch(self, request, *args, **kwargs):
        if not _require_admin(request):
            return JsonResponse({'status': False, 'error': 'Unauthorized'}, status=401)
        return super().dispatch(request, *args, **kwargs)

    def get(self, request):
        domain = registrable_domain(request.GET.get('domain') or '')
        if not domain:
            return JsonResponse({'status': False, 'error': 'Domain tidak valid'}, status=400)
        detail = fetch_domain_detail(domain)
        upsert_result({
            'domain': domain,
            'expired_date': detail.get('expired_date') or None,
            'days_left': detail.get('days_left'),
            'registrar': detail.get('registrar') or '',
            'status': detail.get('status') or '',
            'source': detail.get('source') or '',
            'checked_at': detail.get('checked_at') or '',
            'note': detail.get('note') or '',
            'not_found': detail.get('not_found'),
        })
        local = local_domain_record(domain)
        if local and local.get('notes') and not detail.get('price'):
            notes = str(local.get('notes') or '')
            lowered = notes.lower()
            if 'harga' in lowered or 'rp' in lowered or 'usd' in lowered:
                detail['price'] = notes
                detail['price_note'] = 'Diambil dari catatan domain di master inventory.'
        return JsonResponse({
            'status': True,
            'detail': detail,
            'google': google_domain_row(domain) or {},
            'local': local or {},
        })


class DomainCheckerAlertView(View):
    def dispatch(self, request, *args, **kwargs):
        if not _require_admin(request):
            return JsonResponse({'status': False, 'error': 'Unauthorized'}, status=401)
        return super().dispatch(request, *args, **kwargs)

    def get(self, request):
        admin = request.session.get('hris_admin') or {}
        ensure_table()
        ensure_domain_checker_menu(admin=admin)
        items = alert_items()
        return JsonResponse({
            'status': True,
            'count': len(items),
            'items': items[:12],
        })

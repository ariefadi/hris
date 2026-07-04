from django.core.management.base import BaseCommand
from django.core.management import call_command
from datetime import datetime, date
import os
import uuid

from management.database import data_mysql
from management.rekap_month import month_bounds, parse_pull_date, resolve_rekap_target_month
from management.utils import fetch_user_adx_account_data
from management.utils_adsense import fetch_adsense_traffic_per_domain_advanced


def convert_to_idr(amount, currency_code):
    try:
        code = (currency_code or 'IDR').strip().upper()
        base = float(amount or 0.0)
        if code == 'IDR':
            return base
        env_key = f'EXCHANGE_RATE_{code}_IDR'
        if os.getenv(env_key):
            rate = float(os.getenv(env_key))
            return base * rate
        default_rates = {
            'USD': float(os.getenv('USD_IDR_RATE', '16000')),
            'EUR': float(os.getenv('EUR_IDR_RATE', '17500')),
            'SGD': float(os.getenv('SGD_IDR_RATE', '12000')),
            'HKG': float(os.getenv('HKG_IDR_RATE', '3000')),
            'GBP': float(os.getenv('GBP_IDR_RATE', '20000')),
        }
        rate = default_rates.get(code)
        return base * rate if rate else base
    except Exception:
        return float(amount or 0.0)


def force_usd_by_domain(domain):
    d = str(domain or '').strip().lower()
    if not d:
        return False
    usd_domains = ('uaetiming', 'valoranewspekanbaru')
    return any(k in d for k in usd_domains)


def to_float(val):
    try:
        s = str(val or '').replace(',', '').replace('%', '').strip()
        if not s:
            return 0.0
        return float(s)
    except Exception:
        return 0.0


def pct_to_int(val):
    v = to_float(val)
    if 0 < v <= 1:
        v = v * 100.0
    return int(round(v))


class Command(BaseCommand):
    help = (
        "Tarik rekap bulanan AdSense per domain ke data_adsense_rekap. "
        "Default otomatis: tanggal 1-10 setiap bulan, rekap bulan sebelumnya (N-1)."
    )

    def add_arguments(self, parser):
        parser.add_argument('--tahun', type=str, default=None)
        parser.add_argument('--bulan', type=str, default=None)
        parser.add_argument('--tanggal-tarik', type=str, default=None)
        parser.add_argument('--force', action='store_true', default=False)

    def handle(self, *args, **kwargs):
        today = date.today()
        try:
            pull_date = parse_pull_date(kwargs.get('tanggal_tarik'))
        except ValueError as exc:
            self.stdout.write(self.style.ERROR(str(exc)))
            return

        try:
            target_year, target_month = resolve_rekap_target_month(
                today,
                kwargs.get('tahun'),
                kwargs.get('bulan'),
                bool(kwargs.get('force')),
            )
        except ValueError as exc:
            self.stdout.write(self.style.ERROR(str(exc)))
            return

        if target_year is None:
            self.stdout.write(self.style.WARNING(
                f"Lewati cron rekap AdSense: hari ini tanggal {today.day} (window otomatis tanggal 1-10)."
            ))
            return

        start_date, end_date = month_bounds(target_year, target_month)
        pull_date_str = pull_date.strftime('%Y-%m-%d')
        rekap_tahun = f"{target_year:04d}"
        rekap_bulan = f"{target_month:02d}"
        now_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        self.stdout.write(self.style.WARNING(
            f"Mulai rekap AdSense: periode {start_date} s/d {end_date}, tanggal tarik={pull_date_str}"
        ))

        db = data_mysql()
        creds = db.get_all_app_credentials()
        if not creds.get('status'):
            self.stdout.write(self.style.ERROR(f"Gagal mengambil kredensial: {creds.get('error')}"))
            return

        rows = creds.get('data') or []
        if not rows:
            self.stdout.write(self.style.ERROR("Tidak ada kredensial aktif di app_credentials."))
            return

        total_insert = 0
        total_error = 0

        for cred in rows:
            user_mail = cred.get('user_mail')
            account_id = cred.get('account_id')
            account_name = cred.get('account_name') or user_mail or '-'
            if not user_mail or not account_id:
                continue

            try:
                acct_info = fetch_user_adx_account_data(user_mail)
                currency_code = 'IDR'
                if isinstance(acct_info, dict) and acct_info.get('status'):
                    currency_code = (acct_info.get('data', {}) or {}).get('currency_code') or 'IDR'
                currency_code = (currency_code or 'IDR').strip().upper()

                res = fetch_adsense_traffic_per_domain_advanced(
                    user_mail,
                    start_date,
                    end_date,
                    site_filter='%',
                    report_level='site',
                )
                if not res or not res.get('status'):
                    total_error += 1
                    err = res.get('error') if isinstance(res, dict) else 'Unknown error'
                    if isinstance(err, str) and ('No AdSense accounts found' in err or 'no adsense' in err.lower()):
                        self.stdout.write(self.style.WARNING(f"Tidak ada akun AdSense untuk {user_mail}: {err}"))
                        continue
                    self.stdout.write(self.style.ERROR(
                        f"Gagal fetch AdSense rekap untuk {user_mail} ({account_name}): {err}"
                    ))
                    continue

                res_currency = (res.get('currency_code') or '').strip().upper()
                if res_currency:
                    currency_code = res_currency

                for item in res.get('data', []) or []:
                    try:
                        domain = item.get('domain') or '-'
                        impressions = int(item.get('impressions', 0) or 0)
                        clicks = int(item.get('clicks', 0) or 0)
                        revenue = float(item.get('revenue', 0.0) or 0.0)
                        page_views = int(item.get('page_views', 0) or 0)
                        ad_requests = int(item.get('ad_requests', 0) or 0)

                        currency_for_item = 'USD' if force_usd_by_domain(domain) else currency_code
                        revenue_idr = convert_to_idr(revenue, currency_for_item)

                        ctr = (clicks / impressions * 100) if impressions > 0 else 0.0
                        cpc = (revenue_idr / clicks) if clicks > 0 else 0.0
                        cpm = ((revenue_idr / impressions) * 1000) if impressions > 0 else 0.0
                        page_views_rpm_idr = (revenue_idr / page_views * 1000) if page_views > 0 else 0.0

                        ad_requests_coverage = to_float(item.get('ad_requests_coverage', 0.0) or 0.0)
                        if (not ad_requests_coverage) and ad_requests > 0:
                            ad_requests_coverage = (float(impressions) / float(ad_requests)) * 100.0

                        record = {
                            'data_adsense_rekap_id': str(uuid.uuid4()),
                            'account_id': account_id,
                            'data_adsense_rekap_tahun': rekap_tahun,
                            'data_adsense_rekap_bulan': rekap_bulan,
                            'data_adsense_rekap_tanggal': pull_date_str,
                            'data_adsense_rekap_domain': domain,
                            'data_adsense_rekap_impresi': impressions,
                            'data_adsense_rekap_click': clicks,
                            'data_adsense_rekap_ctr': float(ctr),
                            'data_adsense_rekap_cpc': int(round(cpc)),
                            'data_adsense_rekap_cpm': int(round(cpm)),
                            'data_adsense_rekap_page_views': page_views,
                            'data_adsense_rekap_page_views_rpm': int(round(page_views_rpm_idr)),
                            'data_adsense_rekap_ad_requests': ad_requests,
                            'data_adsense_rekap_ad_requests_coverage': pct_to_int(ad_requests_coverage),
                            'data_adsense_rekap_active_view_viewability': pct_to_int(item.get('active_view_viewability', 0.0)),
                            'data_adsense_rekap_active_view_measurability': pct_to_int(item.get('active_view_measurability', 0.0)),
                            'data_adsense_rekap_active_view_time': int(round(to_float(item.get('active_view_time', 0.0)))),
                            'data_adsense_rekap_revenue': int(round(revenue_idr)),
                            'mdb': '0',
                            'mdb_name': 'Cron Rekap Bulanan',
                            'mdd': now_str,
                        }

                        del_res = db.delete_data_adsense_rekap(
                            account_id,
                            domain,
                            rekap_tahun,
                            rekap_bulan,
                            pull_date_str,
                        )
                        if del_res.get('hasil', {}).get('status'):
                            affected = del_res.get('hasil', {}).get('affected', 0)
                            if affected:
                                self.stdout.write(self.style.WARNING(
                                    f"Hapus rekap AdSense existing ({affected}): {account_name}/{domain}"
                                ))

                        ins = db.insert_data_adsense_rekap(record)
                        if ins.get('hasil', {}).get('status'):
                            total_insert += 1
                        else:
                            total_error += 1
                            self.stdout.write(self.style.ERROR(
                                f"Gagal insert rekap AdSense {account_name}/{domain}: {ins.get('hasil', {})}"
                            ))
                    except Exception as ie:
                        total_error += 1
                        self.stdout.write(self.style.ERROR(
                            f"Gagal proses domain AdSense {account_name}: {ie}"
                        ))
            except Exception as exc:
                total_error += 1
                self.stdout.write(self.style.ERROR(
                    f"Gagal memproses kredensial AdSense {account_name}: {exc}"
                ))

        if total_insert:
            try:
                call_command(
                    'sync_clickhouse',
                    tables='data_adsense_rekap',
                    since=pull_date_str,
                )
            except Exception as exc:
                self.stdout.write(self.style.ERROR(f"Gagal sync ClickHouse data_adsense_rekap: {exc}"))

        self.stdout.write(self.style.SUCCESS(
            f"Selesai rekap AdSense {rekap_tahun}-{rekap_bulan} (tarik {pull_date_str}). "
            f"Berhasil insert: {total_insert}, gagal: {total_error}."
        ))

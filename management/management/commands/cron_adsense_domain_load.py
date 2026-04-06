from django.core.management.base import BaseCommand
from django.core.management import call_command
from datetime import datetime, timedelta
import os
import uuid

from management.database import data_mysql
from management.utils import fetch_user_adx_account_data
from management.utils_adsense import get_user_adsense_client, extract_domain_from_ad_unit, fetch_adsense_traffic_per_domain_advanced


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


def fetch_adsense_traffic_per_domain(user_mail, start_date, end_date, site_filter='%'):
    return fetch_adsense_traffic_per_domain_advanced(
        user_mail,
        start_date,
        end_date,
        site_filter=site_filter,
        report_level='subdomain',
    )


class Command(BaseCommand):
    help = "Tarik dan simpan data AdSense per tanggal & domain ke tabel data_adsense_domain, default hari ini. Kredensial diambil dari app_credentials."

    def add_arguments(self, parser):
        parser.add_argument(
            '--tanggal',
            type=str,
            default='%',
            help='Tanggal tunggal (YYYY-MM-DD) atau kosong untuk default hari ini.',
        )

    def handle(self, *args, **kwargs):
        tanggal = kwargs.get('tanggal')

        if tanggal and tanggal != '%':
            start_date = tanggal
            end_date = tanggal
        else:
            today_dt = datetime.now().date()
            start_date = today_dt.strftime('%Y-%m-%d')
            end_date = today_dt.strftime('%Y-%m-%d')

        self.stdout.write(
            self.style.WARNING(
                f"Menarik dan menyimpan AdSense per domain untuk range {start_date} s/d {end_date}."
            )
        )

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

        start_dt = datetime.strptime(start_date, '%Y-%m-%d').date()
        end_dt = datetime.strptime(end_date, '%Y-%m-%d').date()
        day_count = (end_dt - start_dt).days + 1

        for i in range(day_count):
            day_dt = start_dt + timedelta(days=i)
            day_str = day_dt.strftime('%Y-%m-%d')
            self.stdout.write(self.style.SUCCESS(f"Proses tanggal: {day_str}"))

            for cred in rows:
                try:
                    user_mail = cred.get('user_mail')
                    account_id = cred.get('account_id')
                    account_name = cred.get('account_name')
                    if not user_mail or not account_id:
                        continue
                    acct_info = fetch_user_adx_account_data(user_mail)
                    currency_code = 'IDR'
                    if isinstance(acct_info, dict) and acct_info.get('status'):
                        currency_code = (acct_info.get('data', {}) or {}).get('currency_code') or 'IDR'
                    currency_code = (currency_code or 'IDR').strip().upper()
                    res = fetch_adsense_traffic_per_domain(user_mail, day_str, day_str, site_filter='%')
                    if not res or not res.get('status'):
                        total_error += 1
                        err = res.get('error') if isinstance(res, dict) else 'Unknown error'
                        if isinstance(err, str) and ('No AdSense accounts found' in err or 'no adsense' in err.lower()):
                            self.stdout.write(self.style.WARNING(f"Tidak ada akun AdSense untuk {user_mail}: {err}"))
                            continue
                        self.stdout.write(
                            self.style.ERROR(
                                f"Gagal fetch AdSense untuk {user_mail} ({account_name}): {err}"
                            )
                        )
                        continue

                    res_currency = (res.get('currency_code') or '').strip().upper()
                    if res_currency:
                        currency_code = res_currency

                    for item in res.get('data', []) or []:
                        try:
                            def _to_float(val):
                                try:
                                    s = str(val or '').replace(',', '').replace('%', '').strip()
                                    if not s:
                                        return 0.0
                                    return float(s)
                                except Exception:
                                    return 0.0

                            def _pct_to_int(val):
                                v = _to_float(val)
                                if 0 < v <= 1:
                                    v = v * 100.0
                                return int(round(v))

                            domain = item.get('domain') or '-'
                            impressions = int(item.get('impressions', 0) or 0)
                            clicks = int(item.get('clicks', 0) or 0)
                            revenue = float(item.get('revenue', 0.0) or 0.0)

                            page_views = int(item.get('page_views', 0) or 0)
                            ad_requests = int(item.get('ad_requests', 0) or 0)

                            revenue_idr = convert_to_idr(revenue, currency_code)

                            ctr = (clicks / impressions * 100) if impressions > 0 else 0.0
                            cpc = (revenue_idr / clicks) if clicks > 0 else 0.0
                            cpm = ((revenue_idr / impressions) * 1000) if impressions > 0 else 0.0

                            page_views_rpm_idr = (revenue_idr / page_views * 1000) if page_views > 0 else 0.0

                            ad_requests_coverage = _to_float(item.get('ad_requests_coverage', 0.0) or 0.0)
                            if (not ad_requests_coverage) and ad_requests > 0:
                                ad_requests_coverage = (float(impressions) / float(ad_requests)) * 100.0

                            record = {
                                'account_id': account_id,
                                'data_adsense_tanggal': day_str,
                                'data_adsense_domain': domain,
                                'data_adsense_impresi': impressions,
                                'data_adsense_click': clicks,
                                'data_adsense_ctr': float(ctr),
                                'data_adsense_cpc': int(round(cpc)),
                                'data_adsense_cpm': int(round(cpm)),
                                'data_adsense_page_views': page_views,
                                'data_adsense_page_views_rpm': int(round(page_views_rpm_idr)),
                                'data_adsense_ad_requests': ad_requests,
                                'data_adsense_ad_requests_coverage': _pct_to_int(ad_requests_coverage),
                                'data_adsense_active_view_viewability': _pct_to_int(item.get('active_view_viewability', 0.0)),
                                'data_adsense_active_view_measurability': _pct_to_int(item.get('active_view_measurability', 0.0)),
                                'data_adsense_active_view_time': int(round(_to_float(item.get('active_view_time', 0.0)))),
                                'data_adsense_revenue': int(round(revenue_idr)),
                                'mdb': '0',
                                'mdb_name': 'Cron Job',
                                'mdd': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                            }

                            try:
                                sql_del = (
                                    "DELETE FROM data_adsense_domain "
                                    "WHERE account_id=%s "
                                    "AND DATE(data_adsense_tanggal)=%s "
                                    "AND data_adsense_domain=%s"
                                )
                                if db.execute_query(sql_del, (account_id, day_str, domain)):
                                    affected = db.cur_hris.rowcount if db.cur_hris else 0
                                    if db.commit() and affected:
                                        self.stdout.write(
                                            self.style.WARNING(
                                                f"Membersihkan data existing AdSense Domain ({affected} baris) untuk {day_str}."
                                            )
                                        )
                            except Exception as e:
                                self.stdout.write(self.style.ERROR(f"Error saat menghapus data existing AdSense Domain: {e}"))

                            ins = db.insert_data_adsense_domain(record)
                            if ins.get('hasil', {}).get('status'):
                                total_insert += 1
                                params_log_new = {
                                    'log_adsense_id': str(uuid.uuid4()),
                                    'account_id': record.get('account_id'),
                                    'log_adsense_tanggal': record.get('data_adsense_tanggal'),
                                    'log_adsense_domain': record.get('data_adsense_domain'),
                                    'log_adsense_impresi': record.get('data_adsense_impresi'),
                                    'log_adsense_click': record.get('data_adsense_click'),
                                    'log_adsense_cpc': record.get('data_adsense_cpc'),
                                    'log_adsense_ctr': float(record.get('data_adsense_ctr') or 0.0),
                                    'log_adsense_cpm': record.get('data_adsense_cpm'),
                                    'log_adsense_page_views': record.get('data_adsense_page_views'),
                                    'log_adsense_page_views_rpm': record.get('data_adsense_page_views_rpm'),
                                    'log_adsense_ad_requests': record.get('data_adsense_ad_requests'),
                                    'log_adsense_ad_requests_coverage': record.get('data_adsense_ad_requests_coverage'),
                                    'log_adsense_active_view_viewability': record.get('data_adsense_active_view_viewability'),
                                    'log_adsense_active_view_measurability': record.get('data_adsense_active_view_measurability'),
                                    'log_adsense_active_view_time': record.get('data_adsense_active_view_time'),
                                    'log_adsense_revenue': record.get('data_adsense_revenue'),
                                    'mdb': '0',
                                    'mdb_name': 'Log Snapshot',
                                    'mdd': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                                }
                                try:
                                    db.insert_log_adsense_domain_log(params_log_new)
                                except Exception:
                                    pass
                            else:
                                total_error += 1
                                self.stdout.write(
                                    self.style.ERROR(
                                        f"Gagal insert untuk {user_mail} - {account_name}: {ins.get('hasil', {}).get('data')}"
                                    )
                                )
                        except Exception as ie:
                            total_error += 1
                            self.stdout.write(
                                self.style.ERROR(
                                    f"Gagal proses baris domain untuk {user_mail} - {account_name}: {ie}"
                                )
                            )
                except Exception as e:
                    total_error += 1
                    self.stdout.write(
                        self.style.ERROR(
                            f"Gagal memproses kredensial {cred.get('user_mail','Unknown')}: {e}"
                        )
                    )

        if total_insert:
            try:
                self.stdout.write(self.style.WARNING(
                    f"Sync ClickHouse: data_adsense_domain since={start_date} (delete lalu insert)"
                ))
                call_command(
                    'sync_clickhouse',
                    tables='data_adsense_domain',
                    since=start_date,
                )
            except Exception as e:
                self.stdout.write(self.style.ERROR(f"Gagal sync ClickHouse data_adsense_domain: {e}"))

            try:
                self.stdout.write(self.style.WARNING(
                    f"Sync ClickHouse: log_adsense_domain since={start_date} (delete lalu insert)"
                ))
                call_command(
                    'sync_clickhouse',
                    tables='log_adsense_domain',
                    since=start_date,
                )
            except Exception as e:
                self.stdout.write(self.style.ERROR(f"Gagal sync ClickHouse log_adsense_domain: {e}"))

        self.stdout.write(self.style.SUCCESS(f"Selesai. Berhasil insert: {total_insert}, gagal: {total_error}."))
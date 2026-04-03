# Top-level imports and helper
from django.core.management.base import BaseCommand
from django.core.management import call_command
from datetime import datetime, timedelta, date
from management.database import data_mysql
from management.utils import fetch_adx_traffic_account_by_user
from management.utils import fetch_user_adx_account_data
import os
import uuid

def convert_to_idr(amount, currency_code):
    try:
        code = (currency_code or 'IDR').upper()
        base = float(amount or 0.0)
        if code == 'IDR':
            return base
        # Priority: environment variable EXCHANGE_RATE_<CODE>_IDR
        env_key = f'EXCHANGE_RATE_{code}_IDR'
        if os.getenv(env_key):
            rate = float(os.getenv(env_key))
            return base * rate
        # Common fallbacks for popular currencies (can be overridden by env)
        default_rates = {
            'USD': float(os.getenv('USD_IDR_RATE', '16000')),
            'EUR': float(os.getenv('EUR_IDR_RATE', '17500')),
            'SGD': float(os.getenv('SGD_IDR_RATE', '12000')),
            'HKG': float(os.getenv('HKG_IDR_RATE', '3000')),
            'GBP': float(os.getenv('GBP_IDR_RATE', '20000')),
        }
        rate = default_rates.get(code)
        return base * rate if rate else base  # If unknown currency, no conversion
    except Exception:
        return float(amount or 0.0)

class Command(BaseCommand):
    help = "Tarik dan simpan data Ad Manager (AdX) per tanggal & domain ke tabel data_adx untuk 7 hari kebelakang. Kredensial diambil dari app_credentials."
    def add_arguments(self, parser):
        parser.add_argument(
            '--tanggal',
            type=str,
            default='%',
            help='Tanggal tunggal (YYYY-MM-DD). Jika tidak diisi, ambil 7 hari kebelakang termasuk hari ini.'
        )
    def handle(self, *args, **kwargs):
        tanggal = kwargs.get('tanggal')
        # Hitung range tanggal: default 7 hari terakhir (termasuk hari ini)
        if tanggal and tanggal != '%':
            start_date = tanggal
            end_date = tanggal
        else:
            today_dt = datetime.now().date()
            start_date = (today_dt - timedelta(days=3)).strftime('%Y-%m-%d')
            end_date = today_dt.strftime('%Y-%m-%d')
        self.stdout.write(self.style.WARNING(
            f"Menarik dan menyimpan AdX per domain untuk range {start_date} s/d {end_date} (7 hari)."
        ))
        db = data_mysql()
        # Ambil semua kredensial dari app_credentials
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
        # Loop per hari agar data per tanggal tersimpan akurat
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
                    account_name = cred.get('account_name')
                    if not user_mail:
                        continue
                    # Ambil currency_code akun user sekali sebelum fetch data
                    acct_info = fetch_user_adx_account_data(user_mail)
                    currency_code = 'IDR'
                    if isinstance(acct_info, dict) and acct_info.get('status'):
                        currency_code = (acct_info.get('data', {}) or {}).get('currency_code') or 'IDR'
                    # Ambil data AdX per domain untuk 1 hari (start=end)
                    # report_level='ad_unit_to_site' akan merge advanced metrics (Ad Exchange Display) ke baris SITE_NAME
                    res = fetch_adx_traffic_account_by_user(user_mail, day_dt, day_dt, selected_sites=None, report_level='ad_unit_to_site')
                    if not res or not res.get('status'):
                        total_error += 1
                        self.stdout.write(self.style.ERROR(
                            f"Gagal fetch AdX untuk {user_mail} ({account_name}): {res.get('error') if isinstance(res, dict) else 'Unknown error'}"
                        ))
                        continue
                    for item in res.get('data', []) or []:
                        try:
                            def _to_float(val):
                                s = str(val or '').replace(',', '').replace('%', '').strip()
                                if not s:
                                    return 0.0
                                try:
                                    return float(s)
                                except Exception:
                                    return 0.0

                            def _to_int(val):
                                return int(_to_float(val))

                            impressions = _to_int(item.get('impressions', 0))
                            clicks = _to_int(item.get('clicks', 0))
                            revenue = _to_float(item.get('revenue', 0.0))

                            site_name = str(item.get('site_name') or '').strip()
                            if not site_name:
                                continue

                            cpc = _to_float(item.get('cpc', 0.0))
                            ctr = _to_float(item.get('ctr', 0.0))
                            ecpm = _to_float(item.get('ecpm', 0.0))
                            cpm = ecpm

                            total_requests = _to_int(item.get('total_requests', 0))
                            responses_served = _to_int(item.get('responses_served', 0))
                            match_rate = _to_float(item.get('match_rate', 0.0))
                            fill_rate = _to_float(item.get('fill_rate', 0.0))
                            active_view_pct_viewable = _to_float(item.get('active_view_pct_viewable', 0.0))
                            active_view_avg_time_sec = _to_float(item.get('active_view_avg_time_sec', 0.0))

                            # Konversi revenue ke IDR jika currency bukan IDR
                            revenue_idr = convert_to_idr(revenue, currency_code)

                            record = {
                                'data_adx_domain_id': str(uuid.uuid4()),
                                'account_id': cred.get('account_id') or '',
                                'data_adx_domain_tanggal': day_str,
                                'data_adx_domain': site_name,
                                'data_adx_domain_impresi': impressions,
                                'data_adx_domain_click': clicks,
                                'data_adx_domain_cpc': int(round(cpc)),
                                'data_adx_domain_ctr': ctr,
                                'data_adx_domain_cpm': int(round(cpm)),
                                'data_adx_domain_ecpm': int(round(ecpm)),
                                'data_adx_domain_total_requests': total_requests,
                                'data_adx_domain_responses_served': responses_served,
                                'data_adx_domain_match_rate': int(round(match_rate)),
                                'data_adx_domain_fill_rate': int(round(fill_rate)),
                                'data_adx_domain_active_view_pct_viewable': int(round(active_view_pct_viewable)),
                                'data_adx_domain_active_view_avg_time_sec': int(round(active_view_avg_time_sec)),
                                'data_adx_domain_revenue': int(round(revenue_idr)),
                                'mdb': '0',
                                'mdb_name': 'Cron Job',
                                'mdd': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                            }
                            # Bersihkan data existing untuk range agar idempotent
                            try:
                                del_res = db.delete_data_adx_domain_by_date_account(cred.get('account_id'), day_str, site_name)
                                if del_res.get('hasil', {}).get('status'):
                                    affected = del_res.get('hasil', {}).get('affected', 0)
                                    self.stdout.write(self.style.WARNING(
                                        f"Membersihkan data existing AdX Domain ({affected} baris) untuk range {start_date} s/d {end_date}."
                                    ))
                                else:
                                    self.stdout.write(self.style.ERROR(
                                        f"Gagal menghapus data existing AdX Domain: {del_res.get('hasil', {}).get('data')}"
                                    ))
                            except Exception as e:
                                self.stdout.write(self.style.ERROR(f"Error saat menghapus data existing AdX Domain: {e}"))
                            ins = db.insert_data_adx_domain(record)
                            if ins.get('hasil', {}).get('status'):
                                total_insert += 1
                            else:
                                total_error += 1
                                self.stdout.write(self.style.ERROR(
                                    f"Gagal insert untuk {user_mail} - {account_name}: {ins.get('hasil', {}).get('data')}"
                                ))
                        except Exception as ie:
                            total_error += 1
                            self.stdout.write(self.style.ERROR(
                                f"Gagal proses baris domain untuk {user_mail} - {account_name}: {ie}"
                            ))
                except Exception as e:
                    total_error += 1
                    self.stdout.write(self.style.ERROR(
                        f"Gagal memproses kredensial {cred.get('user_mail','Unknown')}: {e}"
                    ))
        if total_insert:
            try:
                self.stdout.write(self.style.WARNING(
                    f"Sync ClickHouse: data_adx_domain since={start_date} (delete lalu insert)"
                ))
                call_command(
                    'sync_clickhouse',
                    tables='data_adx_domain',
                    since=start_date,
                )
            except Exception as e:
                self.stdout.write(self.style.ERROR(f"Gagal sync ClickHouse data_adx_domain: {e}"))

        self.stdout.write(self.style.SUCCESS(
            f"Selesai. Berhasil insert: {total_insert}, gagal: {total_error}."
        ))
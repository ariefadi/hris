# Module imports & helper
from django.core.management.base import BaseCommand
from django.core.management import call_command
from datetime import datetime, timedelta, date
from management.database import data_mysql
from management.utils import fetch_adx_traffic_per_country
from management.utils import fetch_user_adx_account_data
import os

def convert_to_idr(amount, currency_code):
    try:
        code = (currency_code or 'IDR').upper()
        base = float(amount or 0.0)
        if code == 'IDR':
            return base
        # Prioritas: EXCHANGE_RATE_<CODE>_IDR
        env_key = f'EXCHANGE_RATE_{code}_IDR'
        if os.getenv(env_key):
            rate = float(os.getenv(env_key))
            return base * rate
        # Fallback populer (bisa di-override via env)
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

class Command(BaseCommand):
    help = "Tarik dan simpan data Ad Manager (AdX) per negara ke data_adx, default 7 hari terakhir. Kredensial diambil dari app_credentials."
    def add_arguments(self, parser):
        parser.add_argument(
            '--tanggal',
            type=str,
            default='%',
            help='Tanggal tunggal (YYYY-MM-DD) atau kosong untuk default 7 hari terakhir.'
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
            f"Menarik dan menyimpan AdX per negara untuk range {start_date} s/d {end_date} (7 hari)."
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
                    # Ambil currency code akun user
                    acct_info = fetch_user_adx_account_data(user_mail)
                    currency_code = 'IDR'
                    if isinstance(acct_info, dict) and acct_info.get('status'):
                        currency_code = (acct_info.get('data', {}) or {}).get('currency_code') or 'IDR'
                    # Ambil data AdX per negara untuk 1 hari (start=end)
                    res = fetch_adx_traffic_per_country(day_dt, day_dt, user_mail, selected_sites=None, countries_list=None)
                    if not res or not res.get('status'):
                        total_error += 1
                        self.stdout.write(self.style.ERROR(
                            f"Gagal fetch AdX untuk {user_mail} ({account_name}): {res.get('error') if isinstance(res, dict) else 'Unknown error'}"
                        ))
                        continue
                    for item in res.get('data', []):
                        try:
                            def _to_float(val):
                                try:
                                    s = str(val or '').replace(',', '').replace('%', '').strip()
                                    if not s:
                                        return 0.0
                                    return float(s)
                                except Exception:
                                    return 0.0

                            def _to_int(val):
                                return int(_to_float(val))

                            impressions = _to_int(item.get('impressions', 0))
                            clicks = _to_int(item.get('clicks', 0))
                            revenue = _to_float(item.get('revenue', 0.0))

                            country_cd = (item.get('country_code') or '').upper().strip()
                            site_name = (item.get('site_name') or '').strip()
                            if not country_cd or not site_name:
                                continue

                            revenue_idr = convert_to_idr(revenue, currency_code)

                            ctr = _to_float(item.get('ctr', 0.0) or (clicks / impressions * 100 if impressions > 0 else 0.0))
                            cpc_idr = (revenue_idr / clicks) if clicks > 0 else 0.0
                            ecpm_idr = (revenue_idr / impressions * 1000) if impressions > 0 else 0.0

                            total_requests = _to_int(item.get('total_requests', 0))
                            responses_served = _to_int(item.get('responses_served', 0))
                            match_rate = _to_float(item.get('match_rate', 0.0))
                            fill_rate = _to_float(item.get('fill_rate', 0.0))
                            active_view_pct_viewable = _to_float(item.get('active_view_pct_viewable', 0.0))
                            active_view_avg_time_sec = _to_float(item.get('active_view_avg_time_sec', 0.0))

                            record = {
                                'account_id': cred.get('account_id') or '',
                                'data_adx_country_tanggal': day_str,
                                'data_adx_country_cd': country_cd,
                                'data_adx_country_nm': item.get('country_name') or '',
                                'data_adx_country_domain': site_name,
                                'data_adx_country_impresi': impressions,
                                'data_adx_country_click': clicks,
                                'data_adx_country_ctr': ctr,
                                'data_adx_country_cpc': int(round(cpc_idr)),
                                'data_adx_country_cpm': int(round(ecpm_idr)),
                                'data_adx_country_ecpm': int(round(ecpm_idr)),
                                'data_adx_country_total_requests': total_requests,
                                'data_adx_country_response_served': responses_served,
                                'data_adx_country_match_rate': int(round(match_rate)),
                                'data_adx_country_fill_rate': int(round(fill_rate)),
                                'data_adx_country_active_view_pct_viewable': int(round(active_view_pct_viewable)),
                                'data_adx_country_active_view_avg_time_sec': int(round(active_view_avg_time_sec)),
                                'data_adx_country_revenue': int(round(revenue_idr)),
                                'mdb': '0',
                                'mdb_name': 'Cron Job',
                                'mdd': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                            }
                            # Bersihkan data existing untuk range per account agar idempotent
                            try:
                                del_res = db.delete_data_adx_country_by_date(cred.get('account_id'), day_str, country_cd, site_name)
                                if del_res.get('hasil', {}).get('status'):
                                    affected = del_res.get('hasil', {}).get('affected', 0)
                                    self.stdout.write(self.style.WARNING(
                                        f"Membersihkan data existing AdX ({affected} baris) untuk range {start_date} s/d {end_date}."
                                    ))
                                else:
                                    self.stdout.write(self.style.ERROR(
                                        f"Gagal menghapus data existing AdX untuk {user_mail} ({account_name}): {del_res.get('hasil', {}).get('data')}"
                                    ))
                            except Exception as e:
                                self.stdout.write(self.style.ERROR(f"Error saat menghapus data existing AdX: {e}"))
                            ins = db.insert_data_adx_country(record)
                            if ins.get('hasil', {}).get('status'):
                                total_insert += 1
                            else:
                                total_error += 1
                                self.stdout.write(self.style.ERROR(
                                    f"Gagal insert untuk {user_mail} - {account_name}: {ins.get('hasil', {}).get('data')} | country={country_cd} | site={site_name}"
                                ))
                        except Exception as ie:
                            total_error += 1
                            self.stdout.write(self.style.ERROR(
                                f"Gagal proses baris negara untuk {user_mail} - {account_name}: {ie}"
                            ))
                except Exception as e:
                    total_error += 1
                    self.stdout.write(self.style.ERROR(
                        f"Gagal memproses kredensial {cred.get('user_mail','Unknown')}: {e}"
                    ))
        if total_insert:
            try:
                self.stdout.write(self.style.WARNING(
                    f"Sync ClickHouse: data_adx_country since={start_date} (delete lalu insert)"
                ))
                call_command(
                    'sync_clickhouse',
                    tables='data_adx_country',
                    since=start_date,
                )
            except Exception as e:
                self.stdout.write(self.style.ERROR(f"Gagal sync ClickHouse data_adx_country: {e}"))

        self.stdout.write(self.style.SUCCESS(
            f"Selesai. Berhasil insert: {total_insert}, gagal: {total_error}."
        ))
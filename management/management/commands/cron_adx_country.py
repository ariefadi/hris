# Module imports & helper
from django.core.management.base import BaseCommand
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
            start_date = (today_dt - timedelta(days=6)).strftime('%Y-%m-%d')
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
                            # Map dan hitung metrik sesuai schema data_adx
                            impressions = int(item.get('impressions', 0) or 0)
                            clicks = int(item.get('clicks', 0) or 0)
                            revenue = float(item.get('revenue', 0.0) or 0.0)
                            # Konversi revenue ke IDR
                            revenue_idr = convert_to_idr(revenue, currency_code)
                            # Hitung turunan dari revenue yang sudah IDR agar konsisten
                            cpc = float(item.get('cpc', 0.0) or (revenue_idr / clicks if clicks > 0 else 0.0))
                            cpm = float(item.get('ecpm', 0.0) or (revenue_idr / impressions * 1000 if impressions > 0 else 0.0))
                            record = {
                                'account_id': cred.get('account_id') or '',
                                'data_adx_country_tanggal': day_str,
                                'data_adx_country_cd': (item.get('country_code') or '').upper(),
                                'data_adx_country_nm': item.get('country_name') or '',
                                'data_adx_country_domain': item.get('site_name') or '',
                                'data_adx_country_impresi': impressions,
                                'data_adx_country_click': clicks,
                                'data_adx_country_cpc': cpc,
                                'data_adx_country_ctr': float(item.get('ctr', 0.0) or (clicks / impressions * 100 if impressions > 0 else 0.0)),
                                'data_adx_country_cpm': cpm,
                                'data_adx_country_revenue': revenue_idr,
                                'mdb': '0',
                                'mdb_name': 'Cron Job',
                                'mdd': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                            }
                            # Bersihkan data existing untuk range per account agar idempotent
                            try:
                                del_res = db.delete_data_adx_country_by_date(cred.get('account_id'), day_str, record.get('data_adx_country_cd'), record.get('data_adx_country_domain'))
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
                                    f"Gagal insert untuk {user_mail} - {account_name}: {ins.get('hasil', {}).get('data')}"
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
        self.stdout.write(self.style.SUCCESS(
            f"Selesai. Berhasil insert: {total_insert}, gagal: {total_error}."
        ))
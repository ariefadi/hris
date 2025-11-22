from django.core.management.base import BaseCommand
from datetime import datetime, timedelta, date
from management.database import data_mysql
from management.utils import fetch_adx_traffic_account_by_user

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
            start_date = (today_dt - timedelta(days=6)).strftime('%Y-%m-%d')
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
                    # Ambil data AdX per domain untuk 1 hari (start=end)
                    res = fetch_adx_traffic_account_by_user(user_mail, day_dt, day_dt, selected_sites=None)
                    if not res or not res.get('status'):
                        total_error += 1
                        self.stdout.write(self.style.ERROR(
                            f"Gagal fetch AdX untuk {user_mail} ({account_name}): {res.get('error') if isinstance(res, dict) else 'Unknown error'}"
                        ))
                        continue
                    for item in res.get('data', []) or []:
                        try:
                            impressions = int(item.get('impressions', 0) or 0)
                            clicks = int(item.get('clicks', 0) or 0)
                            revenue = float(item.get('revenue', 0.0) or 0.0)
                            cpc = float(item.get('cpc', 0.0) or 0.0)
                            ctr = float(item.get('ctr', 0.0) or 0.0)
                            cpm = float(item.get('ecpm', 0.0) or 0.0)

                            record = {
                                'account_id': cred.get('account_id') or '',
                                'data_adx_domain_tanggal': day_str,
                                'data_adx_domain': item.get('site_name') or '',
                                'data_adx_domain_impresi': impressions,
                                'data_adx_domain_click': clicks,
                                'data_adx_domain_cpc': cpc,
                                'data_adx_domain_ctr': ctr,
                                'data_adx_domain_cpm': cpm,
                                'data_adx_domain_revenue': revenue,
                                'mdb': '0',
                                'mdb_name': 'Cron Job',
                                'mdd': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                            }
                            # Bersihkan data existing untuk range agar idempotent
                            try:
                                del_res = db.delete_data_adx_domain_by_date_account(cred.get('account_id'), day_str, item.get('site_name'))
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
        self.stdout.write(self.style.SUCCESS(
            f"Selesai. Berhasil insert: {total_insert}, gagal: {total_error}."
        ))
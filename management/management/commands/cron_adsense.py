from django.core.management.base import BaseCommand
from datetime import datetime, timedelta

from management.database import data_mysql
from management.utils_adsense import fetch_adsense_traffic_per_country


class Command(BaseCommand):
    help = 'Tarik dan simpan data AdSense per negara ke tabel data_adsense untuk 3 hari kebelakang menggunakan semua kredensial aktif.'

    def handle(self, *args, **options):
        db = data_mysql()

        creds_result = db.get_all_app_credentials()
        if not creds_result.get('status'):
            self.stdout.write(self.style.ERROR('Gagal mengambil app_credentials: {}'.format(creds_result.get('error'))))
            return

        credentials_list = creds_result.get('data', [])
        if not credentials_list:
            self.stdout.write(self.style.WARNING('Tidak ada kredensial di app_credentials.'))
            return

        # Loop 3 hari kebelakang: hari ini dan 2 hari sebelumnya
        today = datetime.now().date()
        for offset in range(0, 3):
            target_day = today - timedelta(days=offset)
            day_str = target_day.strftime('%Y-%m-%d')
            self.stdout.write(self.style.NOTICE(f'Proses AdSense per negara untuk tanggal {day_str}'))

            # Hapus data existing untuk tanggal ini agar tidak duplikat
            del_res = db.delete_data_adsense_country_by_date_range(day_str, day_str)
            if not del_res.get('hasil', {}).get('status'):
                self.stdout.write(self.style.WARNING('Gagal menghapus data tanggal {}: {}'.format(
                    day_str, del_res.get('hasil', {}).get('data', 'Unknown error')
                )))

            # Proses setiap kredensial aktif
            for cred in credentials_list:
                try:
                    is_active = str(cred.get('is_active', '1')).strip()
                    if is_active != '1':
                        continue

                    user_mail = cred.get('user_mail')
                    account_id = cred.get('account_id')
                    mdb = cred.get('mdb')
                    mdb_name = cred.get('mdb_name')

                    if not user_mail:
                        continue

                    self.stdout.write(f'- Ambil data untuk {user_mail} pada {day_str}')
                    result = fetch_adsense_traffic_per_country(user_mail, day_str, day_str, site_filter='%')

                    if not result.get('status'):
                        err = result.get('error', 'Unknown error')
                        # Lewati jika akun tidak memiliki AdSense; tampilkan info ringan
                        if 'No AdSense accounts found' in err or 'no adsense' in err.lower():
                            self.stdout.write(self.style.WARNING(f'  Tidak ada akun AdSense untuk {user_mail}: {err}'))
                            continue
                        self.stdout.write(self.style.WARNING(f'  Gagal fetch untuk {user_mail}: {err}'))
                        continue

                    for item in result.get('data', []) or []:
                        try:
                            record = {
                                'account_id': account_id,
                                'data_adsense_tanggal': day_str,
                                'data_adsense_country_cd': item.get('country_code') or '',
                                'data_adsense_country_nm': item.get('country') or '',
                                'data_adsense_domain': '-',  # Tidak ada dimensi domain pada laporan per negara
                                'data_adsense_impresi': int(item.get('impressions', 0) or 0),
                                'data_adsense_click': int(item.get('clicks', 0) or 0),
                                'data_adsense_ctr': float(item.get('ctr', 0.0) or 0.0),
                                'data_adsense_cpc': float(item.get('cpc', 0.0) or 0.0),
                                'data_adsense_cpm': float(item.get('cpm', 0.0) or 0.0),
                                'data_adsense_revenue': float(item.get('revenue', 0.0) or 0.0),
                                'mdb': mdb,
                                'mdb_name': mdb_name,
                                'mdd': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                            }
                            ins = db.insert_data_adsense_country(record)
                            if not ins.get('hasil', {}).get('status'):
                                self.stdout.write(self.style.WARNING('  Gagal insert country {}: {}'.format(
                                    record['data_adsense_country_nm'], ins.get('hasil', {}).get('data', 'Unknown error')
                                )))
                        except Exception as e:
                            self.stdout.write(self.style.WARNING(f'  Error memproses item: {e}'))
                except Exception as e:
                    mail_hint = cred.get('user_mail') or 'unknown'
                    self.stdout.write(self.style.WARNING(f'  Error memproses kredensial {mail_hint}: {e}'))
                    continue

            self.stdout.write(self.style.SUCCESS(f'Selesai proses tanggal {day_str}'))

        self.stdout.write(self.style.SUCCESS('Done: cron_adsense per negara untuk 3 hari kebelakang'))
from django.core.management.base import BaseCommand
from datetime import datetime, timedelta
import os
from management.database import data_mysql
from management.utils import fetch_user_adx_account_data
from management.utils_adsense import get_user_adsense_client, extract_domain_from_ad_unit

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


def fetch_adsense_traffic_per_country_domain(user_mail, start_date, end_date, site_filter='%'):
    try:
        client_result = get_user_adsense_client(user_mail)
        if not client_result.get('status'):
            return client_result

        service = client_result['service']
        accounts = service.accounts().list().execute()
        accounts_list = accounts.get('accounts', []) or []
        if not accounts_list:
            return {'status': False, 'error': 'No AdSense accounts found'}

        acc0 = accounts_list[0]
        account_name = acc0.get('name') or ''
        currency_code = acc0.get('currencyCode') or acc0.get('currency_code') or ''

        start_parts = start_date.split('-')
        end_parts = end_date.split('-')

        report_request = service.accounts().reports().generate(
            account=account_name,
            dateRange='CUSTOM',
            startDate_year=int(start_parts[0]),
            startDate_month=int(start_parts[1]),
            startDate_day=int(start_parts[2]),
            endDate_year=int(end_parts[0]),
            endDate_month=int(end_parts[1]),
            endDate_day=int(end_parts[2]),
            dimensions=['COUNTRY_NAME', 'COUNTRY_CODE', 'AD_UNIT_NAME'],
            metrics=['IMPRESSIONS', 'CLICKS', 'ESTIMATED_EARNINGS', 'AD_REQUESTS'],
        )

        if site_filter and site_filter != '%':
            try:
                report_request = report_request.filter(f'AD_UNIT_NAME=~"{site_filter}"')
            except Exception:
                pass

        report = report_request.execute()
        try:
            report_currency = report.get('currencyCode') or report.get('currency_code')
            if not report_currency:
                meta = report.get('metadata') or {}
                report_currency = meta.get('currencyCode') or meta.get('currency_code')
            if report_currency:
                currency_code = report_currency
        except Exception:
            pass
        currency_code = (currency_code or '').strip().upper()

        agg = {}
        for row in report.get('rows', []) or []:
            cells = row.get('cells', []) or []
            country_name = cells[0].get('value') if len(cells) > 0 else 'Unknown'
            country_code = cells[1].get('value') if len(cells) > 1 else ''
            ad_unit_name = cells[2].get('value') if len(cells) > 2 else 'Unknown'
            impressions = int(float(cells[3].get('value'))) if len(cells) > 3 else 0
            clicks = int(float(cells[4].get('value'))) if len(cells) > 4 else 0
            earnings = float(cells[5].get('value')) if len(cells) > 5 else 0.0

            domain = extract_domain_from_ad_unit(ad_unit_name) or '-'
            key = ((country_code or '').upper(), country_name or '', domain)

            cur = agg.get(key)
            if not cur:
                cur = {'impressions': 0, 'clicks': 0, 'earnings': 0.0}
                agg[key] = cur

            cur['impressions'] += impressions
            cur['clicks'] += clicks
            cur['earnings'] += earnings

        results = []
        for (cc, cn, domain), v in agg.items():
            impressions = int(v.get('impressions', 0) or 0)
            clicks = int(v.get('clicks', 0) or 0)
            earnings = float(v.get('earnings', 0.0) or 0.0)
            results.append(
                {
                    'country': cn,
                    'country_code': cc,
                    'domain': domain,
                    'impressions': impressions,
                    'clicks': clicks,
                    'revenue': earnings,
                }
            )

        results.sort(key=lambda x: x.get('revenue', 0.0) or 0.0, reverse=True)

        return {'status': True, 'data': results, 'currency_code': currency_code}
    except Exception as e:
        return {'status': False, 'error': f'Error fetching AdSense per country+domain: {str(e)}'}


class Command(BaseCommand):
    help = "Tarik dan simpan data AdSense per negara & domain ke data_adsense, default hari ini. Kredensial diambil dari app_credentials."

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
            start_date = (today_dt - timedelta(days=6)).strftime('%Y-%m-%d')
            end_date = today_dt.strftime('%Y-%m-%d')

        self.stdout.write(
            self.style.WARNING(
                f"Menarik dan menyimpan AdSense per negara untuk range {start_date} s/d {end_date}."
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

                    res = fetch_adsense_traffic_per_country_domain(user_mail, day_str, day_str, site_filter='%')
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
                            country_cd = (item.get('country_code') or '').upper()
                            country_nm = item.get('country') or ''
                            domain = item.get('domain') or '-'

                            impressions = int(item.get('impressions', 0) or 0)
                            clicks = int(item.get('clicks', 0) or 0)
                            revenue = float(item.get('revenue', 0.0) or 0.0)

                            revenue_idr = convert_to_idr(revenue, currency_code)
                            ctr = (clicks / impressions * 100) if impressions > 0 else 0.0
                            cpc = (revenue_idr / clicks) if clicks > 0 else 0.0
                            cpm = ((revenue_idr / impressions) * 1000) if impressions > 0 else 0.0

                            record = {
                                'account_id': account_id,
                                'data_adsense_country_tanggal': day_str,
                                'data_adsense_country_cd': country_cd,
                                'data_adsense_country_nm': country_nm,
                                'data_adsense_country_domain': domain,
                                'data_adsense_country_impresi': impressions,
                                'data_adsense_country_click': clicks,
                                'data_adsense_country_ctr': float(ctr),
                                'data_adsense_country_cpc': float(cpc),
                                'data_adsense_country_cpm': float(cpm),
                                'data_adsense_country_revenue': float(revenue_idr),
                                'mdb': '0',
                                'mdb_name': 'Cron Job',
                                'mdd': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                            }
                            try:
                                sql_del = (
                                    "DELETE FROM data_adsense_country "
                                    "WHERE account_id=%s "
                                    "AND DATE(data_adsense_country_tanggal)=%s "
                                    "AND data_adsense_country_cd=%s "
                                    "AND data_adsense_country_domain=%s"
                                )
                                if db.execute_query(sql_del, (account_id, day_str, country_cd, domain)):
                                    affected = db.cur_hris.rowcount if db.cur_hris else 0
                                    if db.commit():
                                        if affected:
                                            self.stdout.write(
                                                self.style.WARNING(
                                                    f"Membersihkan data existing AdSense ({affected} baris) untuk {day_str}."
                                                )
                                            )
                            except Exception as e:
                                self.stdout.write(self.style.ERROR(f"Error saat menghapus data existing AdSense: {e}"))

                            ins = db.insert_data_adsense_country(record)
                            if ins.get('hasil', {}).get('status'):
                                total_insert += 1
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
                                    f"Gagal proses baris negara untuk {user_mail} - {account_name}: {ie}"
                                )
                            )
                except Exception as e:
                    total_error += 1
                    self.stdout.write(
                        self.style.ERROR(
                            f"Gagal memproses kredensial {cred.get('user_mail','Unknown')}: {e}"
                        )
                    )
        self.stdout.write(self.style.SUCCESS(f"Selesai. Berhasil insert: {total_insert}, gagal: {total_error}."))
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


def _bulk_insert_data_adx_country(db, rows):
    if not rows:
        return True
    if not db.ensure_connection():
        return False
    cur = db.mysql_cur
    sql = (
        "INSERT INTO data_adx_country ("
        "account_id,data_adx_country_tanggal,data_adx_country_cd,data_adx_country_nm,data_adx_country_domain,"
        "data_adx_country_impresi,data_adx_country_click,data_adx_country_ctr,data_adx_country_cpc,data_adx_country_cpm,data_adx_country_ecpm,"
        "data_adx_country_total_requests,data_adx_country_responses_served,data_adx_country_match_rate,data_adx_country_fill_rate,"
        "data_adx_country_active_view_pct_viewable,data_adx_country_active_view_avg_time_sec,data_adx_country_revenue,mdb,mdb_name,mdd"
        ") VALUES (" + ",".join(["%s"] * 21) + ")"
    )
    cur.executemany(sql, rows)
    return db.commit()


def _bulk_insert_log_adx_country(db, rows):
    if not rows:
        return True
    if not db.ensure_connection():
        return False
    cur = db.mysql_cur
    sql = (
        "INSERT INTO log_adx_country ("
        "account_id,log_adx_country_tanggal,log_adx_country_cd,log_adx_country_nm,log_adx_country_domain,"
        "log_adx_country_impresi,log_adx_country_click,log_adx_country_cpc,log_adx_country_ctr,log_adx_country_cpm,log_adx_country_revenue,"
        "mdb,mdb_name,mdd"
        ") VALUES (" + ",".join(["%s"] * 14) + ")"
    )
    cur.executemany(sql, rows)
    return db.commit()


class Command(BaseCommand):
    help = "Tarik dan simpan data Ad Manager (AdX) per negara ke data_adx, default hari ini. Kredensial diambil dari app_credentials."
    def add_arguments(self, parser):
        parser.add_argument(
            '--tanggal',
            type=str,
            default='%',
            help='Tanggal tunggal (YYYY-MM-DD) atau kosong untuk default hari ini.'
        )

    def handle(self, *args, **kwargs):
        tanggal_arg = str(kwargs.get('tanggal') or '').strip()
        today_dt = datetime.now().date()
        start_date = today_dt.strftime('%Y-%m-%d')
        end_date = today_dt.strftime('%Y-%m-%d')
        if tanggal_arg and tanggal_arg != '%':
            start_date = tanggal_arg
            end_date = tanggal_arg

        self.stdout.write(self.style.WARNING(
            f"Menarik dan menyimpan AdX per negara untuk range {start_date} s/d {end_date}."
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
                    currency_code = 'IDR'
                    if not hasattr(self, '_adx_currency_cache'):
                        self._adx_currency_cache = {}
                    cache = getattr(self, '_adx_currency_cache')
                    if user_mail in cache:
                        currency_code = cache.get(user_mail) or 'IDR'
                    else:
                        acct_info = fetch_user_adx_account_data(user_mail)
                        if isinstance(acct_info, dict) and acct_info.get('status'):
                            currency_code = (acct_info.get('data', {}) or {}).get('currency_code') or 'IDR'
                        cache[user_mail] = currency_code

                    res = fetch_adx_traffic_per_country(day_dt, day_dt, user_mail, selected_sites=None, countries_list=None)
                    if not res or not res.get('status'):
                        total_error += 1
                        self.stdout.write(self.style.ERROR(
                            f"Gagal fetch AdX untuk {user_mail} ({account_name}): {res.get('error') if isinstance(res, dict) else 'Unknown error'}"
                        ))
                        continue

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

                    account_id = cred.get('account_id') or ''
                    now_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    data_rows = []
                    log_rows = []

                    for item in (res.get('data', []) or []):
                        try:
                            country_cd = (item.get('country_code') or '').upper().strip()
                            site_name = (item.get('site_name') or '').strip()
                            if not country_cd or not site_name:
                                continue

                            impressions = _to_int(item.get('impressions', 0))
                            clicks = _to_int(item.get('clicks', 0))
                            revenue = _to_float(item.get('revenue', 0.0))
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

                            data_rows.append((
                                account_id,
                                day_str,
                                country_cd,
                                item.get('country_name') or '',
                                site_name,
                                impressions,
                                clicks,
                                float(ctr),
                                int(round(cpc_idr)),
                                int(round(ecpm_idr)),
                                int(round(ecpm_idr)),
                                total_requests,
                                responses_served,
                                int(round(match_rate)),
                                int(round(fill_rate)),
                                int(round(active_view_pct_viewable)),
                                int(round(active_view_avg_time_sec)),
                                int(round(revenue_idr)),
                                '0',
                                'Cron Job',
                                now_str,
                            ))

                            log_rows.append((
                                account_id,
                                day_str,
                                country_cd,
                                item.get('country_name') or '',
                                site_name,
                                impressions,
                                clicks,
                                int(round(cpc_idr)),
                                float(ctr),
                                int(round(ecpm_idr)),
                                int(round(revenue_idr)),
                                '0',
                                'Log Snapshot',
                                now_str,
                            ))
                        except Exception as ie:
                            total_error += 1
                            self.stdout.write(self.style.ERROR(
                                f"Gagal proses baris negara untuk {user_mail} - {account_name}: {ie}"
                            ))

                    if data_rows:
                        try:
                            sql_del = "DELETE FROM data_adx_country WHERE account_id=%s AND data_adx_country_tanggal LIKE %s"
                            db.execute_query(sql_del, (account_id, f"{day_str}%"))
                            db.commit()
                            sql_del_log = "DELETE FROM log_adx_country WHERE account_id=%s AND log_adx_country_tanggal LIKE %s"
                            db.execute_query(sql_del_log, (account_id, f"{day_str}%"))
                            db.commit()

                            ok_data = _bulk_insert_data_adx_country(db, data_rows)
                            ok_log = _bulk_insert_log_adx_country(db, log_rows)
                            if ok_data and ok_log:
                                total_insert += len(data_rows)
                            else:
                                total_error += len(data_rows)
                                self.stdout.write(self.style.ERROR(
                                    f"Gagal bulk insert AdX untuk {user_mail} ({account_name}) tanggal {day_str}"
                                ))
                        except Exception as e:
                            total_error += len(data_rows)
                            self.stdout.write(self.style.ERROR(
                                f"Gagal bulk insert AdX untuk {user_mail} ({account_name}) tanggal {day_str}: {e}"
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

            try:
                self.stdout.write(self.style.WARNING(
                    f"Sync ClickHouse: log_adx_country since={start_date} (delete lalu insert)"
                ))
                call_command(
                    'sync_clickhouse',
                    tables='log_adx_country',
                    since=start_date,
                )
            except Exception as e:
                self.stdout.write(self.style.ERROR(f"Gagal sync ClickHouse log_adx_country: {e}"))

        self.stdout.write(self.style.SUCCESS(
            f"Selesai. Berhasil insert: {total_insert}, gagal: {total_error}."
        ))
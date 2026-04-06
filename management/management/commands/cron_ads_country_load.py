from django.core.management.base import BaseCommand
from django.core.management import call_command
from datetime import datetime, timedelta
from collections import defaultdict

try:
    from facebook_business.api import FacebookAdsApi
    from facebook_business.adobjects.adaccount import AdAccount
    from facebook_business.adobjects.adsinsights import AdsInsights
    from facebook_business.exceptions import FacebookRequestError
except Exception:
    FacebookAdsApi = None
    AdAccount = None
    AdsInsights = None
    FacebookRequestError = Exception

import requests
import urllib3
from management.database import data_mysql
from management.utils import get_country_name_from_code


def _bulk_insert_data_ads_country(db, rows):
    if not rows:
        return True
    if not db.ensure_connection():
        return False
    cur = db.mysql_cur
    sql = (
        "INSERT INTO data_ads_country ("
        "account_ads_id,data_ads_country_cd,data_ads_country_nm,data_ads_domain,data_ads_campaign_nm,data_ads_country_tanggal,"
        "data_ads_country_spend,data_ads_country_impresi,data_ads_country_click,data_ads_country_reach,data_ads_country_cpr,"
        "data_ads_country_cpc,data_ads_country_frekuensi,data_ads_country_lpv,data_ads_country_lpv_rate,mdb,mdb_name,mdd"
        ") VALUES (" + ",".join(["%s"] * 18) + ")"
    )
    cur.executemany(sql, rows)
    return db.commit()


def _bulk_insert_log_ads_country(db, rows):
    if not rows:
        return True
    if not db.ensure_connection():
        return False
    cur = db.mysql_cur
    sql = (
        "INSERT INTO log_ads_country ("
        "account_ads_id,log_ads_country_cd,log_ads_country_nm,log_ads_domain,log_ads_campaign_nm,log_ads_country_tanggal,"
        "log_ads_country_spend,log_ads_country_impresi,log_ads_country_click,log_ads_country_reach,log_ads_country_cpr,"
        "log_ads_country_cpc,log_ads_country_frekuensi,log_ads_country_lpv,log_ads_country_lpv_rate,mdb,mdb_name,mdd"
        ") VALUES (" + ",".join(["%s"] * 18) + ")"
    )
    cur.executemany(sql, rows)
    return db.commit()

class Command(BaseCommand):
    def add_arguments(self, parser):
        parser.add_argument(
            '--tanggal', type=str, default=None,
            help='Tanggal format YYYY-MM-DD. Jika tidak diisi, default: hari ini.'
        )
        parser.add_argument(
            '--start', type=str, default=None,
            help='Tanggal mulai (YYYY-MM-DD) untuk range.'
        )
        parser.add_argument(
            '--end', type=str, default=None,
            help='Tanggal akhir (YYYY-MM-DD) untuk range.'
        )
        parser.add_argument(
            '--domain', type=str, default='%',
            help='Filter sub-domain (nama campaign mengandung). Default: %'
        )

    def handle(self, *args, **kwargs):
        if FacebookAdsApi is None:
            self.stdout.write(self.style.ERROR('Facebook SDK (facebook_business) belum terpasang. cron_ads_country_load tidak bisa dijalankan.'))
            return

        db = data_mysql()
        rs_account = (db.master_account_ads() or {}).get('data') or []
        total_insert = 0
        total_error = 0

        today_dt = datetime.now().date()
        start_date = today_dt.strftime('%Y-%m-%d')
        end_date = today_dt.strftime('%Y-%m-%d')

        tanggal_arg = str(kwargs.get('tanggal') or '').strip()
        start_arg = str(kwargs.get('start') or '').strip()
        end_arg = str(kwargs.get('end') or '').strip()
        if start_arg and end_arg:
            start_date = start_arg
            end_date = end_arg
        elif tanggal_arg:
            start_date = tanggal_arg
            end_date = tanggal_arg

        domain_filter = str(kwargs.get('domain') or '%').strip() or '%'

        for account_data in rs_account:
            try:
                FacebookAdsApi.init(access_token=account_data['access_token'])
                account = AdAccount(account_data['account_id'])

                fields = [
                    AdsInsights.Field.ad_id,
                    AdsInsights.Field.ad_name,
                    AdsInsights.Field.adset_id,
                    AdsInsights.Field.campaign_id,
                    AdsInsights.Field.campaign_name,
                    AdsInsights.Field.spend,
                    AdsInsights.Field.reach,
                    AdsInsights.Field.impressions,
                    'cost_per_result',
                    AdsInsights.Field.actions,
                    AdsInsights.Field.frequency,
                    AdsInsights.Field.date_start,
                ]

                params = {
                    'level': 'campaign',
                    'time_range': {'since': start_date, 'until': end_date},
                    'breakdowns': ['country'],
                    'time_increment': 1,
                    'limit': 1000,
                }
                if domain_filter != '%':
                    params['filtering'] = [{
                        'field': 'campaign.name',
                        'operator': 'CONTAIN',
                        'value': domain_filter,
                    }]

                try:
                    insights = account.get_insights(fields=fields, params=params)
                except (FacebookRequestError, requests.exceptions.RequestException, urllib3.exceptions.HTTPError, Exception) as e:
                    self.stdout.write(self.style.ERROR(f"[ERROR] Gagal mengambil insights Facebook: {e}"))
                    insights = []

                per_day_data = defaultdict(list)
                per_day_log = defaultdict(list)
                now_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

                def pick_action(actions, action_type):
                    for a in actions or []:
                        if a.get('action_type') == action_type:
                            try:
                                return float(a.get('value', 0) or 0)
                            except (TypeError, ValueError):
                                return 0.0
                    return 0.0

                for item in insights:
                    try:
                        tanggal_row = item.get('date_start')
                        if not tanggal_row:
                            continue

                        campaign_name = item.get('campaign_name') or ''
                        domain_value = (campaign_name.split('_')[0] if campaign_name else '')
                        if domain_filter != '%':
                            domain_value = domain_filter

                        spend = float(item.get('spend', 0) or 0)
                        impressions = int(item.get('impressions', 0) or 0)
                        reach = int(item.get('reach', 0) or 0)

                        frequency_val = item.get('frequency')
                        try:
                            frequency = float(frequency_val) if frequency_val not in [None, ""] else 0.0
                        except (TypeError, ValueError):
                            frequency = 0.0

                        actions = item.get('actions', [])
                        clicks_val = pick_action(actions, 'link_click')
                        lpv_val = pick_action(actions, 'landing_page_view')
                        lpv_rate = round((lpv_val / clicks_val) * 100, 2) if clicks_val else 0.0

                        cpr_val = 0.0
                        for cpr_item in item.get('cost_per_result', []):
                            if cpr_item.get('indicator') == 'actions:link_click':
                                values = cpr_item.get('values', [])
                                if values:
                                    try:
                                        cpr_val = float(values[0].get('value'))
                                    except (TypeError, ValueError):
                                        cpr_val = 0.0
                                break

                        cpc = round(spend / clicks_val, 2) if clicks_val else 0.0

                        country_code_val = item.get('country') or ''
                        country_name_val = get_country_name_from_code(country_code_val) if country_code_val else ''

                        per_day_data[str(tanggal_row)].append((
                            account_data['account_id'],
                            country_code_val,
                            country_name_val,
                            domain_value,
                            campaign_name,
                            str(tanggal_row),
                            round(spend, 2),
                            impressions,
                            int(clicks_val),
                            reach,
                            round(cpr_val, 2),
                            cpc,
                            round(frequency, 2),
                            round(lpv_val, 2),
                            lpv_rate,
                            '0',
                            'Cron Job',
                            now_str,
                        ))

                        per_day_log[str(tanggal_row)].append((
                            account_data['account_id'],
                            country_code_val,
                            country_name_val,
                            domain_value,
                            campaign_name,
                            str(tanggal_row),
                            round(spend, 2),
                            impressions,
                            int(clicks_val),
                            reach,
                            round(cpr_val, 2),
                            cpc,
                            round(frequency, 2),
                            round(lpv_val, 2),
                            lpv_rate,
                            '0',
                            'Log Snapshot',
                            now_str,
                        ))
                    except Exception as ie:
                        total_error += 1
                        self.stdout.write(self.style.ERROR(
                            f"Gagal proses baris negara untuk akun {account_data.get('account_name','Unknown')}: {ie}"
                        ))

                for day_str, rows in per_day_data.items():
                    if not rows:
                        continue
                    try:
                        if domain_filter != '%':
                            sql_del = (
                                "DELETE FROM data_ads_country WHERE account_ads_id=%s AND data_ads_country_tanggal=%s AND data_ads_domain=%s"
                            )
                            db.execute_query(sql_del, (account_data['account_id'], day_str, domain_filter))
                            db.commit()
                            sql_del_log = (
                                "DELETE FROM log_ads_country WHERE account_ads_id=%s AND log_ads_country_tanggal=%s AND log_ads_domain=%s"
                            )
                            db.execute_query(sql_del_log, (account_data['account_id'], day_str, domain_filter))
                            db.commit()
                        else:
                            sql_del = "DELETE FROM data_ads_country WHERE account_ads_id=%s AND data_ads_country_tanggal=%s"
                            db.execute_query(sql_del, (account_data['account_id'], day_str))
                            db.commit()
                            sql_del_log = "DELETE FROM log_ads_country WHERE account_ads_id=%s AND log_ads_country_tanggal=%s"
                            db.execute_query(sql_del_log, (account_data['account_id'], day_str))
                            db.commit()

                        ok_data = _bulk_insert_data_ads_country(db, rows)
                        ok_log = _bulk_insert_log_ads_country(db, per_day_log.get(day_str) or [])
                        if ok_data and ok_log:
                            total_insert += len(rows)
                        else:
                            total_error += len(rows)
                            self.stdout.write(self.style.ERROR(
                                f"Gagal bulk insert untuk akun {account_data.get('account_name','Unknown')} tanggal {day_str}"
                            ))
                    except Exception as e:
                        total_error += len(rows)
                        self.stdout.write(self.style.ERROR(
                            f"Gagal bulk sync untuk akun {account_data.get('account_name','Unknown')} tanggal {day_str}: {e}"
                        ))

            except Exception as e:
                total_error += 1
                self.stdout.write(self.style.ERROR(
                    f"Gagal memproses akun {account_data.get('account_name','Unknown')}: {e}"
                ))

        if total_insert:
            try:
                self.stdout.write(self.style.WARNING(
                    f"Sync ClickHouse: data_ads_country since={start_date} (delete lalu insert)"
                ))
                call_command(
                    'sync_clickhouse',
                    tables='data_ads_country',
                    since=start_date,
                )
            except Exception as e:
                self.stdout.write(self.style.ERROR(f"Gagal sync ClickHouse data_ads_country: {e}"))

            try:
                self.stdout.write(self.style.WARNING(
                    f"Sync ClickHouse: log_ads_country since={start_date} (delete lalu insert)"
                ))
                call_command(
                    'sync_clickhouse',
                    tables='log_ads_country',
                    since=start_date,
                )
            except Exception as e:
                self.stdout.write(self.style.ERROR(f"Gagal sync ClickHouse log_ads_country: {e}"))

        self.stdout.write(self.style.SUCCESS(
            f"Selesai. Berhasil insert: {total_insert}, gagal: {total_error}."
        ))
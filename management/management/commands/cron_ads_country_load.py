from django.core.management.base import BaseCommand
from django.core.management import call_command
from datetime import datetime, timedelta
from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.adsinsights import AdsInsights
from facebook_business.exceptions import FacebookRequestError
import requests
import time
import urllib3
from management.database import data_mysql
from management.utils import get_country_name_from_code

COUNTRY_CODE_MAX_LEN = 5
FETCH_MAX_RETRIES = 3
FETCH_RETRY_BASE_SECONDS = 5


def _normalize_country_code(raw):
    code = str(raw or '').strip()
    if not code or code.lower() in ('unknown', 'unkno', 'none', 'null'):
        return ''
    if len(code) == 2:
        return code.upper()
    return code[:2].upper()


def _is_retryable_fb_error(exc):
    if not isinstance(exc, FacebookRequestError):
        return True
    code = exc.api_error_code()
    subcode = exc.api_error_subcode()
    if code in (1, 2, 4, 17, 32, 613):
        return True
    if subcode in (1504044, 2446079):
        return True
    message = str(exc).lower()
    return any(
        phrase in message
        for phrase in (
            'temporarily unavailable',
            'unexpected error',
            'unknown error',
            'reduce the amount of data',
            'rate limit',
        )
    )


def _fetch_insights_with_retry(account, fields, params, stdout, account_name):
    last_error = None
    for attempt in range(1, FETCH_MAX_RETRIES + 1):
        try:
            return list(account.get_insights(fields=fields, params=params)), None
        except (FacebookRequestError, requests.exceptions.RequestException, urllib3.exceptions.HTTPError) as exc:
            last_error = exc
            if attempt >= FETCH_MAX_RETRIES or not _is_retryable_fb_error(exc):
                break
            wait_seconds = FETCH_RETRY_BASE_SECONDS * attempt
            stdout.write(
                stdout.style.WARNING(
                    f"[RETRY {attempt}/{FETCH_MAX_RETRIES}] {account_name}: {exc} "
                    f"(coba lagi dalam {wait_seconds}s)"
                )
            )
            time.sleep(wait_seconds)
    return None, last_error


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
        parser.add_argument(
            '--account', type=str, default=None,
            help='Filter account_ads_id atau nama akun (partial match).'
        )

    def handle(self, *args, **kwargs):
        db = data_mysql()
        rs_account = db.master_account_ads()['data']
        total_insert = 0
        total_error = 0
        failed_accounts = []

        tanggal = kwargs.get('tanggal')
        start = kwargs.get('start')
        end = kwargs.get('end')
        account_filter = (kwargs.get('account') or '').strip()

        if start and end:
            start_date = start
            end_date = end
        elif tanggal and tanggal != '%':
            start_date = tanggal
            end_date = tanggal
        else:
            today_dt = datetime.now().date()
            start_date = today_dt.strftime('%Y-%m-%d')
            end_date = today_dt.strftime('%Y-%m-%d')

        if account_filter:
            needle = account_filter.lower()
            rs_account = [
                account_data for account_data in rs_account
                if needle in str(account_data.get('account_id', '')).lower()
                or needle in str(account_data.get('account_name', '')).lower()
            ]

        for account_data in rs_account:
            account_name = account_data.get('account_name', 'Unknown')
            try:
                domain_filter = kwargs.get('domain')
                FacebookAdsApi.init(access_token=account_data['access_token'])
                account = AdAccount(account_data['account_id'])

                fields = [
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
                if domain_filter and str(domain_filter).strip() != '%':
                    params['filtering'] = [{
                        'field': 'campaign.name',
                        'operator': 'CONTAIN',
                        'value': domain_filter,
                    }]

                insights, fetch_error = _fetch_insights_with_retry(
                    account, fields, params, self.stdout, account_name
                )
                if fetch_error is not None:
                    total_error += 1
                    failed_accounts.append(account_name)
                    self.stdout.write(self.style.ERROR(
                        f"[ERROR] Gagal mengambil insights Facebook untuk akun {account_name} "
                        f"({account_data.get('account_id')}): {fetch_error}"
                    ))
                    continue

                try:
                    if domain_filter and str(domain_filter).strip() != '%':
                        sql_del = (
                            "DELETE FROM data_ads_country "
                            "WHERE account_ads_id=%s "
                            "AND DATE(data_ads_country_tanggal) BETWEEN %s AND %s "
                            "AND data_ads_domain LIKE %s"
                        )
                        db.execute_query(
                            sql_del,
                            (
                                account_data['account_id'],
                                start_date,
                                end_date,
                                f"%{str(domain_filter).strip()}%",
                            ),
                        )
                    else:
                        sql_del = (
                            "DELETE FROM data_ads_country "
                            "WHERE account_ads_id=%s "
                            "AND DATE(data_ads_country_tanggal) BETWEEN %s AND %s"
                        )
                        db.execute_query(
                            sql_del,
                            (account_data['account_id'], start_date, end_date),
                        )
                    db.commit()
                except Exception as de:
                    total_error += 1
                    failed_accounts.append(account_name)
                    self.stdout.write(self.style.ERROR(
                        f"Gagal pre-delete data_ads_country akun {account_name}: {de}"
                    ))
                    continue

                def pick_action(actions, action_type):
                    for a in actions or []:
                        if a.get('action_type') == action_type:
                            try:
                                return float(a.get('value', 0) or 0)
                            except (TypeError, ValueError):
                                return 0.0
                    return 0.0

                account_insert = 0
                for item in insights:
                    try:
                        tanggal_row = item.get('date_start')
                        if not tanggal_row:
                            continue
                        campaign_id = item.get('campaign_id') or ''
                        campaign_name = item.get('campaign_name') or ''
                        domain_value = (campaign_name.split('_')[0] if campaign_name else '')
                        if domain_filter and str(domain_filter).strip() != '%':
                            domain_value = str(domain_filter).strip()

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

                        country_code_val = _normalize_country_code(item.get('country'))
                        country_name_val = get_country_name_from_code(country_code_val) if country_code_val else ''

                        now_ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                        record = {
                            'account_ads_id': account_data['account_id'],
                            'data_ads_domain': domain_value,
                            'data_ads_country_cd': country_code_val,
                            'data_ads_country_nm': country_name_val,
                            'data_ads_campaign_id': campaign_id,
                            'data_ads_campaign_nm': campaign_name,
                            'data_ads_country_tanggal': tanggal_row,
                            'data_ads_country_spend': round(spend, 2),
                            'data_ads_country_impresi': impressions,
                            'data_ads_country_click': int(clicks_val),
                            'data_ads_country_reach': reach,
                            'data_ads_country_cpr': round(cpr_val, 2),
                            'data_ads_country_cpc': cpc,
                            'data_ads_country_frekuensi': round(frequency, 2),
                            'data_ads_country_lpv': round(lpv_val, 2),
                            'data_ads_country_lpv_rate': lpv_rate,
                            'mdb': '0',
                            'mdb_name': 'Cron Job',
                            'mdd': now_ts,
                        }
                        res = db.insert_data_ads_country(record)
                        if res.get('hasil', {}).get('status'):
                            total_insert += 1
                            account_insert += 1
                            params_log_new = {
                                'account_ads_id': record.get('account_ads_id'),
                                'log_ads_country_cd': str(record.get('data_ads_country_cd') or '')[:2],
                                'log_ads_country_nm': record.get('data_ads_country_nm'),
                                'log_ads_domain': record.get('data_ads_domain'),
                                'log_ads_campaign_id': record.get('data_ads_campaign_id'),
                                'log_ads_campaign_nm': record.get('data_ads_campaign_nm'),
                                'log_ads_country_tanggal': record.get('data_ads_country_tanggal'),
                                'log_ads_country_spend': record.get('data_ads_country_spend'),
                                'log_ads_country_impresi': record.get('data_ads_country_impresi'),
                                'log_ads_country_click': record.get('data_ads_country_click'),
                                'log_ads_country_reach': record.get('data_ads_country_reach'),
                                'log_ads_country_cpr': record.get('data_ads_country_cpr'),
                                'log_ads_country_cpc': record.get('data_ads_country_cpc'),
                                'log_ads_country_frekuensi': record.get('data_ads_country_frekuensi'),
                                'log_ads_country_lpv': record.get('data_ads_country_lpv'),
                                'log_ads_country_lpv_rate': record.get('data_ads_country_lpv_rate'),
                                'mdb': '0',
                                'mdb_name': 'Log Snapshot',
                                'mdd': now_ts,
                            }
                            try:
                                db.insert_log_ads_country_log(params_log_new)
                            except Exception:
                                pass
                        else:
                            total_error += 1
                    except Exception as ie:
                        total_error += 1
                        self.stdout.write(self.style.ERROR(
                            f"Gagal proses baris negara untuk akun {account_name}: {ie}"
                        ))

                self.stdout.write(
                    f"Akun {account_name}: {account_insert} baris diinsert "
                    f"({len(insights)} baris dari Facebook)."
                )

            except Exception as e:
                total_error += 1
                failed_accounts.append(account_name)
                self.stdout.write(self.style.ERROR(
                    f"Gagal memproses akun {account_name}: {e}"
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
        if failed_accounts:
            unique_failed = sorted(set(failed_accounts))
            self.stdout.write(self.style.ERROR(
                f"Akun gagal fetch (data lama dipertahankan): {', '.join(unique_failed)}"
            ))

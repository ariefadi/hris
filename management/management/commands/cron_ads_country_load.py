from django.core.management.base import BaseCommand
from datetime import datetime, timedelta
from collections import defaultdict
from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.adsinsights import AdsInsights
from facebook_business.exceptions import FacebookRequestError
import requests
import urllib3
from management.database import data_mysql
from management.utils import get_country_name_from_code

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
        rs_account = data_mysql().master_account_ads()['data']
        total_insert = 0
        total_error = 0
        # Default: hari ini
        today_dt = datetime.now().date()
        start_date = today_dt.strftime('%Y-%m-%d')
        end_date = today_dt.strftime('%Y-%m-%d')
        for account_data in rs_account:
            try:
                domain_filter = kwargs.get('domain')
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
                    AdsInsights.Field.date_start
                ]
                params = {
                    'level': 'campaign',
                    'time_range': {
                        'since': start_date,
                        'until': end_date
                    },
                    'breakdowns': ['country'],
                    'time_increment': 1,
                    'limit': 1000,
                }
                if domain_filter and str(domain_filter).strip() != '%':
                    params['filtering'] = [{
                        'field': 'campaign.name',
                        'operator': 'CONTAIN',
                        'value': str(domain_filter).strip()
                    }]
                try:
                    insights = account.get_insights(fields=fields, params=params)
                except (FacebookRequestError, requests.exceptions.RequestException, urllib3.exceptions.HTTPError, Exception) as e:
                    print(f"[ERROR] Gagal mengambil insights Facebook: {e}")
                    insights = []
                for item in insights:
                    try:
                        tanggal_row = item.get('date_start')
                        if not tanggal_row:
                            continue
                        campaign_name = item.get('campaign_name') or ''
                        domain_value = (campaign_name.split('_')[0] if campaign_name else '')
                        if domain_filter and str(domain_filter).strip() != '%':
                            domain_value = str(domain_filter).strip()
                        spend = float(item.get('spend', 0) or 0)
                        impressions = int(item.get('impressions', 0) or 0)
                        reach = int(item.get('reach', 0) or 0)
                        # Actions: link_click
                        clicks_val = 0.0
                        for action in item.get('actions', []):
                            if action.get('action_type') == 'link_click':
                                try:
                                    clicks_val = float(action.get('value', 0) or 0)
                                except (TypeError, ValueError):
                                    clicks_val = 0.0
                                break
                        # Cost per result (link_click)
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
                        # Facebook Insights dengan breakdown 'country' mengembalikan field 'country' (kode ISO2)
                        country_code_val = item.get('country') or ''
                        country_name_val = get_country_name_from_code(country_code_val) if country_code_val else ''
                        record = {
                            'account_ads_id': account_data['account_id'],
                            'data_ads_domain': domain_value,
                            'data_ads_country_cd': country_code_val,
                            'data_ads_country_nm': country_name_val,
                            'data_ads_campaign_nm': campaign_name,
                            'data_ads_country_tanggal': tanggal_row,
                            'data_ads_country_spend': round(spend, 2),
                            'data_ads_country_impresi': impressions,
                            'data_ads_country_click': int(clicks_val),
                            'data_ads_country_reach': reach,
                            'data_ads_country_cpr': round(cpr_val, 2),
                            'data_ads_country_cpc': cpc,
                            'mdb': '0',
                            'mdb_name': 'Cron Job',
                            'mdd': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                        }
                        # Hapus data existing pada rentang tanggal agar ditimpa data baru
                        try:
                            del_res = data_mysql().delete_data_ads_country_by_date_account(record['account_id'], record['data_ads_country_cd'], record['data_ads_domain'], record['data_ads_campaign_nm'], record['data_ads_country_tanggal'])
                            if del_res.get('hasil', {}).get('status'):
                                affected = del_res.get('hasil', {}).get('affected', 0)
                                self.stdout.write(self.style.WARNING(
                                    f"Membersihkan data existing ({affected} baris) untuk akun {account_data.get('account_name','Unknown')}."
                                ))
                            else:
                                self.stdout.write(self.style.ERROR(
                                    f"Gagal menghapus data existing untuk range {start_date} s/d {end_date}: {del_res.get('hasil', {}).get('data')}"
                                ))
                        except Exception as e:
                            self.stdout.write(self.style.ERROR(
                                f"Error saat menghapus data existing: {e}"
                            ))
                        res = data_mysql().insert_data_ads_country(record)
                        if res.get('hasil', {}).get('status'):
                            total_insert += 1
                        else:
                            total_error += 1
                    except Exception as ie:
                        total_error += 1
                        self.stdout.write(self.style.ERROR(
                            f"Gagal insert baris negara untuk akun {account_data.get('account_name','Unknown')}: {ie}"
                        ))

            except Exception as e:
                total_error += 1
                self.stdout.write(self.style.ERROR(
                    f"Gagal memproses akun {account_data.get('account_name','Unknown')}: {e}"
                ))

        self.stdout.write(self.style.SUCCESS(
            f"Selesai. Berhasil insert: {total_insert}, gagal: {total_error}."
        ))
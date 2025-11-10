from django.core.management.base import BaseCommand
from datetime import datetime
from collections import defaultdict
from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.adsinsights import AdsInsights
from facebook_business.adobjects.campaign import Campaign
from management.database import data_mysql


class Command(BaseCommand):
    @staticmethod
    def normalize_fb_datetime(value):
        # Normalize FB ISO strings like 'YYYY-MM-DDTHH:MM:SS+0700' to 'YYYY-MM-DD HH:MM:SS'
        if value in [None, ""]:
            return None
        if isinstance(value, datetime):
            return value.strftime('%Y-%m-%d %H:%M:%S')
        if isinstance(value, str):
            # If ISO with 'T', take first 19 chars and replace 'T' with space
            # e.g., '2025-10-21T08:01:54+0700' -> '2025-10-21 08:01:54'
            if 'T' in value and len(value) >= 19:
                core = value[:19]
                return core.replace('T', ' ')
            # If only date 'YYYY-MM-DD', append '00:00:00'
            if len(value) == 10 and value.count('-') == 2:
                return f"{value} 00:00:00"
            # Fallback: return as-is (assumed already MySQL-compatible)
            return value
        return None

    @staticmethod
    def map_fb_status_to_int(value):
        # Map Facebook campaign status to tinyint for DB
        if not value:
            return 0
        val = str(value).upper()
        if val == 'ACTIVE':
            return 1
        if val in ['PAUSED', 'DELETED', 'ARCHIVED']:
            return 0
        # Unknown statuses default to 0 (inactive)
        return 0
    def add_arguments(self, parser):
        parser.add_argument(
            '--tanggal', type=str, default=None,
            help='Tanggal format YYYY-MM-DD. Default: hari ini.'
        )

    def handle(self, *args, **kwargs):
        tanggal = kwargs.get('tanggal')
        rs_account = data_mysql().master_account_ads()['data']

        total_insert = 0
        total_error = 0

        for account_data in rs_account:
            try:
                FacebookAdsApi.init(access_token=account_data['access_token'])
                account = AdAccount(account_data['account_id'])

                if not tanggal or tanggal == '%':
                    today = datetime.now().strftime('2025-11-01')
                else:
                    today = tanggal

                time_range = {
                    'since': today,
                    'until': today,
                }
                params = {
                    'level': 'campaign',
                    'time_range': time_range,
                }

                campaign_configs = account.get_campaigns(fields=[
                    Campaign.Field.id,
                    Campaign.Field.name,
                    Campaign.Field.status,
                    Campaign.Field.daily_budget,
                    Campaign.Field.start_time,
                    Campaign.Field.stop_time,
                ])
                campaign_map = {
                    c['id']: {
                        'name': c.get('name'),
                        'status': c.get('status'),
                        'daily_budget': float(c.get('daily_budget') or 0),
                        'start_time': c.get('start_time'),
                        'stop_time': c.get('stop_time'),
                    } for c in campaign_configs
                }

                campaign_aggregates = defaultdict(lambda: {
                    'status': '',
                    'start_time': '',
                    'stop_time': '',
                    'campaign_name': '',
                    'daily_budget': 0.0,
                })

                insights = account.get_insights(
                    fields=[
                        AdsInsights.Field.campaign_id,
                        AdsInsights.Field.campaign_name,
                        AdsInsights.Field.actions,
                    ],
                    params=params,
                )

                for row in insights:
                    campaign_id = row.get('campaign_id')
                    if not campaign_id:
                        continue
                    config = campaign_map.get(campaign_id, {})
                    agg = campaign_aggregates[campaign_id]
                    agg['campaign_name'] = row.get('campaign_name')
                    if not agg['status']:
                        agg['status'] = config.get('status')
                        agg['daily_budget'] = float(config.get('daily_budget') or 0)
                        agg['start_time'] = config.get('start_time')
                        agg['stop_time'] = config.get('stop_time')

                for _, agg in campaign_aggregates.items():
                    record = {
                        'master_date': today,
                        'account_ads_id': account_data['account_id'],
                        'master_domain': (agg['campaign_name'] or '').split('_')[0],
                        'master_campaign_nm': agg['campaign_name'],
                        'master_budget': agg.get('daily_budget', 0.0),
                        'master_date_start': self.normalize_fb_datetime(agg.get('start_time')),
                        'master_date_end': self.normalize_fb_datetime(agg.get('stop_time')),
                        'master_status': self.map_fb_status_to_int(agg.get('status')),
                        'mdb': '0',
                        'mdb_name': 'Cron Job',
                        'mdd': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    }
                    res = data_mysql().insert_data_master_ads(record)
                    if res.get('hasil', {}).get('status'):
                        total_insert += 1
                    else:
                        total_error += 1

            except Exception as e:
                total_error += 1
                self.stdout.write(self.style.ERROR(
                    f"Gagal memproses akun {account_data.get('account_name','Unknown')}: {e}"
                ))

        self.stdout.write(self.style.SUCCESS(
            f"Selesai. Berhasil insert: {total_insert}, gagal: {total_error}."
        ))
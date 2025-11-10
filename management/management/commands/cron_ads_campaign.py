from django.core.management.base import BaseCommand
from datetime import datetime, timedelta
from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.adsinsights import AdsInsights
from facebook_business.adobjects.campaign import Campaign
from management.database import data_mysql

class Command(BaseCommand):
    def handle(self, *args, **kwargs):
        rs_account = data_mysql().master_account_ads()
        all_data = []
        for data in rs_account:
            campaign_aggregates = defaultdict(lambda: {
                'spend': 0.0,
                'reach': 0,
                'impressions': 0,
                'clicks': 0,
                'cpr': 0.0,
                'daily_budget': 0.0,
                'frequency': 0.0,
            })
            FacebookAdsApi.init(access_token=data['access_token'])
            account = AdAccount(data['account_id'])
            campaign_configs = account.get_campaigns(fields=[
                Campaign.Field.id,
                Campaign.Field.name,
                Campaign.Field.status,
                Campaign.Field.daily_budget
            ])
            campaign_map = {
                c['id']: {
                    'name': c.get('name'),
                    'status': c.get('status'),
                    'daily_budget': float(c.get('daily_budget') or 0)
                } for c in campaign_configs
            }
            fields = [
                AdsInsights.Field.campaign_id,
                AdsInsights.Field.campaign_name,
                AdsInsights.Field.spend,
                AdsInsights.Field.reach,
                AdsInsights.Field.impressions,
                'cost_per_result',
                AdsInsights.Field.actions
            ]
            params = {
                'level': 'campaign',
                'time_range': {
                    'since': start_date,
                    'until': end_date
                },
                'limit': 1000
            }
            insights = account.get_insights(fields=fields, params=params)
            for item in insights:
                campaign_id = item.get('campaign_id')
                if not campaign_id:
                    continue
                config = campaign_map.get(campaign_id, {})
                agg = campaign_aggregates[campaign_id]
                agg['campaign_name'] = item.get('campaign_name')
                agg['spend'] += float(item.get('spend', 0))
                agg['reach'] += int(item.get('reach', 0))
                agg['impressions'] += int(item.get('impressions', 0))
                if agg['reach'] > 0:
                    frequency = float(agg['impressions']/agg['reach'])
                    agg['frequency'] = frequency
                else:
                    agg['frequency'] = 0.0
                cost_per_result = None
                for cpr_item in item.get('cost_per_result', []):
                    if cpr_item.get('indicator') == 'actions:link_click':
                        values = cpr_item.get('values', [])
                        if values:
                            cost_per_result = values[0].get('value')
                        break
                agg['cpr'] += float(cost_per_result)
                result_action_type = 'link_click'
                result_count = 0
                for action in item.get('actions', []):
                    if action.get('action_type') == result_action_type:
                        result_count = float(action.get('value', 0))
                        break
                if result_count not in [None, ""]:
                    agg['clicks'] += result_count
                if not agg['status']:
                    agg['status'] = config.get('status')
                    agg['daily_budget'] += float(config.get('daily_budget', 0))
            for campaign_id, agg in campaign_aggregates.items():
                all_data.append({
                    'data_ads_id': data['account_id'],
                    'campaign_id': campaign_id,
                    'campaign_name': agg['campaign_name'],
                    'budget': agg['daily_budget'],
                    'spend': round(agg['spend'], 2),
                    'impressions': agg['impressions'],
                    'reach': agg['reach'],
                    'clicks': agg['clicks'],
                    'frequency': agg['frequency'],
                    'cpr': agg['cpr'],
                    'mdb': '0',
                    'mdb_name': 'Cron Job',
                    'mdd': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                })
        data = data_mysql().insert_data_ads_campaign(all_data)
        hasil = {
            "status": data['hasil']['status'],
            "message": data['hasil']['message']
        }
        return JsonResponse(hasil)
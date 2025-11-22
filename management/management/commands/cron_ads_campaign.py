from django.core.management.base import BaseCommand
from datetime import datetime, timedelta
from collections import defaultdict
from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.adsinsights import AdsInsights
from facebook_business.adobjects.campaign import Campaign
from management.database import data_mysql

class Command(BaseCommand):
    def add_arguments(self, parser):
        parser.add_argument(
            '--tanggal', type=str, default=None,
            help='Tanggal format YYYY-MM-DD. Jika tidak diisi, default: 7 hari terakhir.'
        )
        parser.add_argument(
            '--start', type=str, default=None,
            help='Tanggal mulai (YYYY-MM-DD) untuk range.'
        )
        parser.add_argument(
            '--end', type=str, default=None,
            help='Tanggal akhir (YYYY-MM-DD) untuk range.'
        )

    def handle(self, *args, **kwargs):
        tanggal = kwargs.get('tanggal')
        start = kwargs.get('start')
        end = kwargs.get('end')
        rs_account = data_mysql().master_account_ads()['data']
        total_insert = 0
        total_error = 0
        # Tentukan tanggal (since/until) untuk insights
        if start and end:
            start_date = start
            end_date = end
        else:
            if tanggal and tanggal != '%':
                # Single hari sesuai --tanggal
                start_date = tanggal
                end_date = tanggal
            else:
                # Default: 7 hari terakhir (termasuk hari ini)
                today_dt = datetime.now().date()
                start_date = (today_dt - timedelta(days=6)).strftime('%Y-%m-%d')
                end_date = today_dt.strftime('%Y-%m-%d')
        for account_data in rs_account:
            try:
                # Aggregate per campaign per tanggal
                campaign_aggregates = {}
                FacebookAdsApi.init(access_token=account_data['access_token'])
                account = AdAccount(account_data['account_id'])
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
                    AdsInsights.Field.actions,
                    AdsInsights.Field.date_start
                ]
                params = {
                    'level': 'campaign',
                    'time_range': {
                        'since': start_date,
                        'until': end_date
                    },
                    'time_increment': 1,
                    'limit': 1000
                }
                insights = account.get_insights(fields=fields, params=params)
                for item in insights:
                    campaign_id = item.get('campaign_id')
                    if not campaign_id:
                        continue
                    config = campaign_map.get(campaign_id, {})
                    tanggal_row = item.get('date_start')
                    if not tanggal_row:
                        continue
                    key = (campaign_id, tanggal_row)
                    agg = campaign_aggregates.setdefault(key, {
                        'campaign_name': '',
                        'spend': 0.0,
                        'reach': 0,
                        'impressions': 0,
                        'clicks': 0.0,
                        'cpr': 0.0,
                        'tanggal': tanggal_row,
                        'status': config.get('status'),
                    })
                    agg['campaign_name'] = item.get('campaign_name')
                    agg['spend'] += float(item.get('spend', 0) or 0)
                    agg['reach'] += int(item.get('reach', 0) or 0)
                    agg['impressions'] += int(item.get('impressions', 0) or 0)
                    # Cost per result (link_click)
                    cost_per_result_val = None
                    for cpr_item in item.get('cost_per_result', []):
                        if cpr_item.get('indicator') == 'actions:link_click':
                            values = cpr_item.get('values', [])
                            if values:
                                cost_per_result_val = values[0].get('value')
                            break
                    if cost_per_result_val not in [None, ""]:
                        try:
                            agg['cpr'] += float(cost_per_result_val)
                        except (TypeError, ValueError):
                            pass
                    # Actions: link_clicks
                    result_action_type = 'link_click'
                    result_count = 0
                    for action in item.get('actions', []):
                        if action.get('action_type') == result_action_type:
                            try:
                                result_count = float(action.get('value', 0) or 0)
                            except (TypeError, ValueError):
                                result_count = 0
                            break
                    if result_count not in [None, ""]:
                        agg['clicks'] += result_count
                    # Status sudah diambil dari config jika tersedia
                # Insert per-campaign aggregate ke data_ads_campaign
                for (_, _), agg in campaign_aggregates.items():
                    # Hitung CPC aman
                    cpc = round(agg['spend'] / agg['clicks'], 2) if agg['clicks'] else 0.0
                    record = {
                        'account_ads_id': account_data['account_id'],
                        'data_ads_domain': (agg['campaign_name'] or '').split('_')[0],
                        'data_ads_campaign_nm': agg['campaign_name'] or '',
                        'data_ads_tanggal': agg['tanggal'],
                        'data_ads_spend': round(agg['spend'], 2),
                        'data_ads_impresi': int(agg['impressions']),
                        'data_ads_click': int(agg['clicks']),
                        'data_ads_reach': int(agg['reach']),
                        'data_ads_cpr': round(agg['cpr'], 2),
                        'data_ads_cpc': cpc,
                        'mdb': '0',
                        'mdb_name': 'Cron Job',
                        'mdd': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    }
                    # Hapus data existing pada rentang tanggal agar ditimpa data baru
                    try:
                        del_res = data_mysql().delete_data_ads_campaign_by_date_account(record['account_ads_id'], record['data_ads_domain'], record['data_ads_campaign_nm'], record['data_ads_tanggal'])
                        if del_res.get('hasil', {}).get('status'):
                            affected = del_res.get('hasil', {}).get('affected', 0)
                            self.stdout.write(self.style.WARNING(
                                f"Membersihkan data existing ({affected} baris) untuk range {start_date} s/d {end_date}."
                            ))
                        else:
                            self.stdout.write(self.style.ERROR(
                                f"Gagal menghapus data existing untuk range {start_date} s/d {end_date}: {del_res.get('hasil', {}).get('data')}"
                            ))
                    except Exception as e:
                        self.stdout.write(self.style.ERROR(
                            f"Error saat menghapus data existing: {e}"
                        ))
                    res = data_mysql().insert_data_ads_campaign(record)
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
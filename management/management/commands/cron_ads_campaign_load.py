from django.core.management.base import BaseCommand
from django.core.management import call_command
from datetime import datetime, timedelta
from collections import defaultdict
import uuid
from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.adsinsights import AdsInsights
from facebook_business.adobjects.campaign import Campaign
from management.database import data_mysql

class Command(BaseCommand):
    def add_arguments(self, parser):
        parser.add_argument(
            '--tanggal', type=str, default=None,
            help='Tanggal format YYYY-MM-DD. Jika tidak diisi, default: hari ini terakhir.'
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

        db = data_mysql()
        rs_account = db.master_account_ads()['data']
        total_insert = 0
        total_error = 0

        today_dt = datetime.now().date()
        if start and end:
            start_date = start
            end_date = end
        elif tanggal and tanggal != '%':
            start_date = tanggal
            end_date = tanggal
        else:
            start_date = today_dt.strftime('%Y-%m-%d')
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
                    AdsInsights.Field.date_start,
                    AdsInsights.Field.frequency,
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
                        'frequency': 0.0,
                        'lpv': 0.0,
                        'lpv_rate': 0.0,
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
                    # Frequency
                    frequency_val = item.get('frequency')
                    if frequency_val not in [None, ""]:
                        try:
                            agg['frequency'] += float(frequency_val)
                        except (TypeError, ValueError):
                            pass
                    def pick_action(actions, action_type):
                        for a in actions or []:
                            if a.get('action_type') == action_type:
                                try:
                                    return float(a.get('value', 0) or 0)
                                except (TypeError, ValueError):
                                    return 0.0
                        return 0.0
                    actions = item.get('actions', [])
                    link_clicks = pick_action(actions, 'link_click')
                    lpv = pick_action(actions, 'landing_page_view')
                    if link_clicks:
                        agg['clicks'] += float(link_clicks)
                    if lpv:
                        agg['lpv'] += float(lpv)
                    # Status sudah diambil dari config jika tersedia
                # Insert per-campaign aggregate ke data_ads_campaign
                for (_, _), agg in campaign_aggregates.items():
                    clicks = float(agg.get('clicks') or 0)
                    spend = float(agg.get('spend') or 0)
                    lpv = float(agg.get('lpv') or 0)
                    cpc = round(spend / clicks, 2) if clicks else 0.0
                    lpv_rate = round((lpv / clicks) * 100, 2) if clicks else 0.0
                    record = {
                        'account_ads_id': account_data['account_id'],
                        'data_ads_domain': (agg['campaign_name'] or '').split('_')[0],
                        'data_ads_campaign_nm': agg['campaign_name'] or '',
                        'data_ads_tanggal': agg['tanggal'],
                        'data_ads_spend': round(spend, 2),
                        'data_ads_impresi': int(agg['impressions']),
                        'data_ads_click': int(clicks),
                        'data_ads_reach': int(agg['reach']),
                        'data_ads_cpr': round(agg['cpr'], 2),
                        'data_ads_cpc': cpc,
                        'data_ads_frekuensi': round(float(agg.get('frequency') or 0), 2),
                        'data_ads_lpv': round(lpv, 2),
                        'data_ads_lpv_rate': lpv_rate,
                        'mdb': '0',
                        'mdb_name': 'Cron Job',
                        'mdd': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    }
                    try:
                        del_res = db.delete_data_ads_campaign_by_date_account(
                            account_data['account_id'],
                            (agg['campaign_name'] or '').split('_')[0],
                            agg['campaign_name'],
                            agg['tanggal'],
                        )
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

                    res = db.insert_data_ads_campaign(record)
                    if res.get('hasil', {}).get('status'):
                        total_insert += 1
                        params_log_new = {
                            'log_ads_id': str(uuid.uuid4()),
                            'account_ads_id': record.get('account_ads_id'),
                            'log_ads_domain': record.get('data_ads_domain'),
                            'log_ads_campaign_nm': record.get('data_ads_campaign_nm'),
                            'log_ads_tanggal': record.get('data_ads_tanggal'),
                            'log_ads_spend': int(round(float(record.get('data_ads_spend') or 0))),
                            'log_ads_impresi': record.get('data_ads_impresi'),
                            'log_ads_click': record.get('data_ads_click'),
                            'log_ads_reach': record.get('data_ads_reach'),
                            'log_ads_cpr': record.get('data_ads_cpr'),
                            'log_ads_cpc': record.get('data_ads_cpc'),
                            'log_ads_frekuensi': record.get('data_ads_frekuensi'),
                            'log_ads_lpv': record.get('data_ads_lpv'),
                            'log_ads_lpv_rate': record.get('data_ads_lpv_rate'),
                            'mdb': '0',
                            'mdb_name': 'Log Snapshot',
                            'mdd': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                        }
                        try:
                            db.insert_log_ads_campaign_log(params_log_new)
                        except Exception:
                            pass
                    else:
                        total_error += 1
            except Exception as e:
                total_error += 1
                self.stdout.write(self.style.ERROR(
                    f"Gagal memproses akun {account_data.get('account_name','Unknown')}: {e}"
                ))

        if total_insert:
            try:
                self.stdout.write(self.style.WARNING(
                    f"Sync ClickHouse: data_ads_campaign since={start_date} (delete lalu insert)"
                ))
                call_command(
                    'sync_clickhouse',
                    tables='data_ads_campaign',
                    since=start_date,
                )
            except Exception as e:
                self.stdout.write(self.style.ERROR(f"Gagal sync ClickHouse data_ads_campaign: {e}"))

            try:
                self.stdout.write(self.style.WARNING(
                    f"Sync ClickHouse: log_ads_campaign since={start_date} (delete lalu insert)"
                ))
                call_command(
                    'sync_clickhouse',
                    tables='log_ads_campaign',
                    since=start_date,
                )
            except Exception as e:
                self.stdout.write(self.style.ERROR(f"Gagal sync ClickHouse log_ads_campaign: {e}"))

        self.stdout.write(self.style.SUCCESS(
            f"Selesai. Berhasil insert: {total_insert}, gagal: {total_error}."
        ))
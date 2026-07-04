from django.core.management.base import BaseCommand
from django.core.management import call_command
from datetime import datetime, date
import calendar
import uuid
from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.adsinsights import AdsInsights
from management.database import data_mysql


def pick_action(actions, action_type):
    for action in actions or []:
        if action.get('action_type') == action_type:
            try:
                return float(action.get('value', 0) or 0)
            except (TypeError, ValueError):
                return 0.0
    return 0.0


def extract_domain(campaign_name):
    return (campaign_name or '').split('_')[0].strip().lower()


def shift_month(year, month, delta):
    month += int(delta)
    while month < 1:
        month += 12
        year -= 1
    while month > 12:
        month -= 12
        year += 1
    return year, month


def month_bounds(year, month):
    last_day = calendar.monthrange(int(year), int(month))[1]
    start = f"{int(year):04d}-{int(month):02d}-01"
    end = f"{int(year):04d}-{int(month):02d}-{last_day:02d}"
    return start, end


def parse_pull_date(value):
    if not value:
        return date.today()
    raw = str(value).strip()
    try:
        return datetime.strptime(raw, '%Y-%m-%d').date()
    except ValueError as exc:
        raise ValueError(f"Format --tanggal-tarik tidak valid: {raw} (gunakan YYYY-MM-DD)") from exc


class Command(BaseCommand):
    help = (
        "Tarik rekap bulanan Facebook Ads per domain ke data_ads_rekap. "
        "Default otomatis: tanggal 1-10 setiap bulan, rekap bulan sebelumnya (N-1). "
        "Contoh: tanggal 2 Juni menarik rekap Mei (1-31 Mei)."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            '--tahun', type=str, default=None,
            help='Tahun rekap target (YYYY). Wajib dipasangkan dengan --bulan jika diisi manual.'
        )
        parser.add_argument(
            '--bulan', type=str, default=None,
            help='Bulan rekap target (1-12 atau 01-12).'
        )
        parser.add_argument(
            '--tanggal-tarik', type=str, default=None,
            help='Tanggal saat rekap ditarik (YYYY-MM-DD). Default: hari ini.'
        )
        parser.add_argument(
            '--force', action='store_true', default=False,
            help='Abaikan window otomatis tanggal 1-10 (untuk backfill manual).'
        )

    def _resolve_target_month(self, today, tahun, bulan, force):
        if tahun or bulan:
            if not tahun or not bulan:
                raise ValueError('Gunakan --tahun dan --bulan bersama-sama.')
            year = int(str(tahun).strip())
            month = int(str(bulan).strip())
            if month < 1 or month > 12:
                raise ValueError(f'Bulan tidak valid: {bulan}')
            return year, month

        if not force and not (1 <= today.day <= 10):
            return None, None
        return shift_month(today.year, today.month, -1)

    def handle(self, *args, **kwargs):
        today = date.today()
        try:
            pull_date = parse_pull_date(kwargs.get('tanggal_tarik'))
        except ValueError as exc:
            self.stdout.write(self.style.ERROR(str(exc)))
            return

        try:
            target_year, target_month = self._resolve_target_month(
                today,
                kwargs.get('tahun'),
                kwargs.get('bulan'),
                bool(kwargs.get('force')),
            )
        except ValueError as exc:
            self.stdout.write(self.style.ERROR(str(exc)))
            return

        if target_year is None:
            self.stdout.write(self.style.WARNING(
                f"Lewati cron rekap: hari ini tanggal {today.day} (window otomatis tanggal 1-10). "
                "Gunakan --tahun/--bulan atau --force untuk backfill manual."
            ))
            return

        start_date, end_date = month_bounds(target_year, target_month)
        pull_date_str = pull_date.strftime('%Y-%m-%d')
        rekap_tahun = f"{target_year:04d}"
        rekap_bulan = f"{target_month:02d}"

        self.stdout.write(self.style.WARNING(
            f"Mulai rekap FB Ads: periode {start_date} s/d {end_date} "
            f"(tahun={rekap_tahun}, bulan={rekap_bulan}), tanggal tarik={pull_date_str}"
        ))

        db = data_mysql()
        rs_account = db.master_account_ads()['data']
        total_insert = 0
        total_error = 0
        now_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        fields = [
            AdsInsights.Field.campaign_id,
            AdsInsights.Field.campaign_name,
            AdsInsights.Field.spend,
            AdsInsights.Field.reach,
            AdsInsights.Field.impressions,
            AdsInsights.Field.actions,
            AdsInsights.Field.frequency,
        ]
        params_base = {
            'level': 'campaign',
            'time_range': {
                'since': start_date,
                'until': end_date,
            },
            'limit': 1000,
        }

        for account_data in rs_account:
            account_name = account_data.get('account_name', 'Unknown')
            try:
                domain_aggregates = {}
                FacebookAdsApi.init(access_token=account_data['access_token'])
                account = AdAccount(account_data['account_id'])

                insights = account.get_insights(fields=fields, params=params_base)
                for item in insights:
                    campaign_name = item.get('campaign_name') or ''
                    domain = extract_domain(campaign_name)
                    if not domain:
                        continue

                    agg = domain_aggregates.setdefault(domain, {
                        'spend': 0.0,
                        'reach': 0,
                        'impressions': 0,
                        'clicks': 0.0,
                        'frequency': 0.0,
                        'lpv': 0.0,
                    })

                    agg['spend'] += float(item.get('spend', 0) or 0)
                    agg['reach'] += int(item.get('reach', 0) or 0)
                    agg['impressions'] += int(item.get('impressions', 0) or 0)

                    frequency_val = item.get('frequency')
                    if frequency_val not in [None, '']:
                        try:
                            agg['frequency'] += float(frequency_val)
                        except (TypeError, ValueError):
                            pass

                    actions = item.get('actions', []) or []
                    link_clicks = pick_action(actions, 'link_click')
                    lpv = pick_action(actions, 'landing_page_view')
                    if link_clicks:
                        agg['clicks'] += float(link_clicks)
                    if lpv:
                        agg['lpv'] += float(lpv)

                for domain, agg in domain_aggregates.items():
                    clicks = float(agg.get('clicks') or 0)
                    spend = float(agg.get('spend') or 0)
                    lpv = float(agg.get('lpv') or 0)
                    cpc = round(spend / clicks, 2) if clicks else 0.0
                    cpr = cpc
                    lpv_rate = round((lpv / clicks) * 100, 2) if clicks else 0.0

                    record = {
                        'data_ads_rekap_id': str(uuid.uuid4()),
                        'account_ads_id': account_data['account_id'],
                        'data_ads_domain': domain,
                        'data_ads_rekap_tahun': rekap_tahun,
                        'data_ads_rekap_bulan': rekap_bulan,
                        'data_ads_rekap_tanggal': pull_date_str,
                        'data_ads_rekap_spend': int(round(spend)),
                        'data_ads_rekap_impresi': int(agg.get('impressions') or 0),
                        'data_ads_rekap_click': int(round(clicks)),
                        'data_ads_rekap_reach': int(agg.get('reach') or 0),
                        'data_ads_rekap_cpr': cpr,
                        'data_ads_rekap_cpc': cpc,
                        'data_ads_rekap_frekuensi': int(round(float(agg.get('frequency') or 0))),
                        'data_ads_rekap_lpv': int(round(lpv)),
                        'data_ads_rekap_lpv_rate': lpv_rate,
                        'mdb': '0',
                        'mdb_name': 'Cron Rekap Bulanan',
                        'mdd': now_str,
                    }

                    try:
                        del_res = db.delete_data_ads_rekap(
                            record['account_ads_id'],
                            record['data_ads_domain'],
                            record['data_ads_rekap_tahun'],
                            record['data_ads_rekap_bulan'],
                            record['data_ads_rekap_tanggal'],
                        )
                        if del_res.get('hasil', {}).get('status'):
                            affected = del_res.get('hasil', {}).get('affected', 0)
                            if affected:
                                self.stdout.write(self.style.WARNING(
                                    f"Hapus rekap existing ({affected} baris): "
                                    f"{account_name} / {domain} / {rekap_tahun}-{rekap_bulan} / tarik {pull_date_str}"
                                ))
                    except Exception as exc:
                        self.stdout.write(self.style.ERROR(
                            f"Error hapus rekap existing {account_name}/{domain}: {exc}"
                        ))

                    res = db.insert_data_ads_rekap(record)
                    if res.get('hasil', {}).get('status'):
                        total_insert += 1
                    else:
                        total_error += 1
                        self.stdout.write(self.style.ERROR(
                            f"Gagal insert rekap {account_name}/{domain}: {res.get('hasil', {})}"
                        ))

            except Exception as exc:
                total_error += 1
                self.stdout.write(self.style.ERROR(
                    f"Gagal memproses akun {account_name}: {exc}"
                ))

        if total_insert:
            try:
                self.stdout.write(self.style.WARNING(
                    f"Sync ClickHouse: data_ads_rekap since={pull_date_str} (delete lalu insert)"
                ))
                call_command(
                    'sync_clickhouse',
                    tables='data_ads_rekap',
                    since=pull_date_str,
                )
            except Exception as exc:
                self.stdout.write(self.style.ERROR(f"Gagal sync ClickHouse data_ads_rekap: {exc}"))

        self.stdout.write(self.style.SUCCESS(
            f"Selesai rekap {rekap_tahun}-{rekap_bulan} (tarik {pull_date_str}). "
            f"Berhasil insert: {total_insert}, gagal: {total_error}."
        ))

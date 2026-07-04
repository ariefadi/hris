from django.core.management.base import BaseCommand
from django.core.management import call_command
from datetime import datetime, date
import os
import uuid

from management.database import data_mysql
from management.rekap_month import month_bounds, parse_pull_date, resolve_rekap_target_month
from management.utils import fetch_adx_traffic_account_by_user, fetch_user_adx_account_data


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


def to_float(val):
    try:
        return float(str(val or '').replace(',', '').replace('%', '').strip() or 0)
    except (TypeError, ValueError):
        return 0.0


def to_int(val):
    try:
        return int(float(str(val or '').replace(',', '').replace('%', '').strip() or 0))
    except (TypeError, ValueError):
        return 0


def aggregate_adx_rows(rows_data):
    by_site = {}
    for item in rows_data or []:
        site_name = (item.get('site_name') or '').strip()
        if not site_name:
            continue
        agg = by_site.setdefault(site_name, {
            'impressions': 0,
            'clicks': 0,
            'revenue': 0.0,
            'total_requests': 0,
            'responses_served': 0,
            'view_weight': 0,
            'view_pct_sum': 0.0,
            'view_time_sum': 0.0,
            'match_rate_sum': 0.0,
            'fill_rate_sum': 0.0,
            'rate_rows': 0,
        })
        impressions = to_int(item.get('impressions', 0))
        agg['impressions'] += impressions
        agg['clicks'] += to_int(item.get('clicks', 0))
        agg['revenue'] += to_float(item.get('revenue', 0.0))
        agg['total_requests'] += to_int(item.get('total_requests', 0))
        agg['responses_served'] += to_int(item.get('responses_served', 0))

        view_pct = to_float(item.get('active_view_pct_viewable', 0.0))
        view_time = to_float(item.get('active_view_avg_time_sec', 0.0))
        if impressions > 0:
            agg['view_weight'] += impressions
            agg['view_pct_sum'] += view_pct * impressions
            agg['view_time_sum'] += view_time * impressions

        agg['match_rate_sum'] += to_float(item.get('match_rate', 0.0))
        agg['fill_rate_sum'] += to_float(item.get('fill_rate', 0.0))
        agg['rate_rows'] += 1

    out = []
    for site_name, agg in by_site.items():
        impressions = agg['impressions']
        clicks = agg['clicks']
        revenue = agg['revenue']
        total_requests = agg['total_requests']
        responses_served = agg['responses_served']

        ctr = (clicks / impressions * 100) if impressions > 0 else 0.0
        cpc = (revenue / clicks) if clicks > 0 else 0.0
        ecpm = ((revenue / impressions) * 1000) if impressions > 0 else 0.0

        if total_requests > 0 and responses_served > 0:
            fill_rate = (float(responses_served) / float(total_requests)) * 100.0
            match_rate = fill_rate
        elif agg['rate_rows'] > 0:
            fill_rate = agg['fill_rate_sum'] / agg['rate_rows']
            match_rate = agg['match_rate_sum'] / agg['rate_rows']
        else:
            fill_rate = 0.0
            match_rate = 0.0

        if agg['view_weight'] > 0:
            active_view_pct_viewable = agg['view_pct_sum'] / agg['view_weight']
            active_view_avg_time_sec = agg['view_time_sum'] / agg['view_weight']
        else:
            active_view_pct_viewable = 0.0
            active_view_avg_time_sec = 0.0

        out.append({
            'site_name': site_name,
            'impressions': impressions,
            'clicks': clicks,
            'revenue': revenue,
            'cpc': cpc,
            'ctr': ctr,
            'ecpm': ecpm,
            'cpm': ecpm,
            'total_requests': total_requests,
            'responses_served': responses_served,
            'match_rate': match_rate,
            'fill_rate': fill_rate,
            'active_view_pct_viewable': active_view_pct_viewable,
            'active_view_avg_time_sec': active_view_avg_time_sec,
        })
    return out


class Command(BaseCommand):
    help = (
        "Tarik rekap bulanan AdX per domain ke data_adx_rekap. "
        "Default otomatis: tanggal 1-10 setiap bulan, rekap bulan sebelumnya (N-1)."
    )

    def add_arguments(self, parser):
        parser.add_argument('--tahun', type=str, default=None)
        parser.add_argument('--bulan', type=str, default=None)
        parser.add_argument('--tanggal-tarik', type=str, default=None)
        parser.add_argument('--force', action='store_true', default=False)

    def handle(self, *args, **kwargs):
        today = date.today()
        try:
            pull_date = parse_pull_date(kwargs.get('tanggal_tarik'))
        except ValueError as exc:
            self.stdout.write(self.style.ERROR(str(exc)))
            return

        try:
            target_year, target_month = resolve_rekap_target_month(
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
                f"Lewati cron rekap AdX: hari ini tanggal {today.day} (window otomatis tanggal 1-10)."
            ))
            return

        start_date, end_date = month_bounds(target_year, target_month)
        start_dt = datetime.strptime(start_date, '%Y-%m-%d').date()
        end_dt = datetime.strptime(end_date, '%Y-%m-%d').date()
        pull_date_str = pull_date.strftime('%Y-%m-%d')
        rekap_tahun = f"{target_year:04d}"
        rekap_bulan = f"{target_month:02d}"
        now_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        self.stdout.write(self.style.WARNING(
            f"Mulai rekap AdX: periode {start_date} s/d {end_date}, tanggal tarik={pull_date_str}"
        ))

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

        for cred in rows:
            user_mail = cred.get('user_mail')
            account_id = cred.get('account_id')
            account_name = cred.get('account_name') or user_mail or '-'
            if not user_mail or not account_id:
                continue

            try:
                acct_info = fetch_user_adx_account_data(user_mail)
                currency_code = 'IDR'
                if isinstance(acct_info, dict) and acct_info.get('status'):
                    currency_code = (acct_info.get('data', {}) or {}).get('currency_code') or 'IDR'
                currency_code = (currency_code or 'IDR').strip().upper()

                res = fetch_adx_traffic_account_by_user(
                    user_mail,
                    start_dt,
                    end_dt,
                    selected_sites=None,
                    report_level='ad_unit_to_site',
                )
                if not res or not res.get('status'):
                    total_error += 1
                    err = res.get('error') if isinstance(res, dict) else 'Unknown error'
                    self.stdout.write(self.style.ERROR(
                        f"Gagal fetch AdX rekap untuk {user_mail} ({account_name}): {err or 'Unknown error'}"
                    ))
                    continue

                aggregated = aggregate_adx_rows(res.get('data', []) or [])
                if not aggregated:
                    self.stdout.write(self.style.WARNING(
                        f"Data AdX rekap kosong untuk {user_mail} ({account_name}) periode {start_date} s/d {end_date}."
                    ))
                    continue

                for item in aggregated:
                    try:
                        site_name = item.get('site_name') or '-'
                        revenue_idr = convert_to_idr(item.get('revenue', 0.0), currency_code)
                        impressions = int(item.get('impressions') or 0)
                        clicks = int(item.get('clicks') or 0)
                        cpc_idr = (revenue_idr / clicks) if clicks > 0 else 0.0
                        cpm_idr = ((revenue_idr / impressions) * 1000) if impressions > 0 else 0.0

                        record = {
                            'data_adx_rekap_id': str(uuid.uuid4()),
                            'account_id': account_id,
                            'data_adx_rekap_tahun': rekap_tahun,
                            'data_adx_rekap_bulan': rekap_bulan,
                            'data_adx_rekap_tanggal': pull_date_str,
                            'data_adx_rekap_domain': site_name,
                            'data_adx_rekap_impresi': impressions,
                            'data_adx_rekap_click': clicks,
                            'data_adx_rekap_cpc': int(round(cpc_idr)),
                            'data_adx_rekap_ctr': float(item.get('ctr') or 0.0),
                            'data_adx_rekap_cpm': int(round(cpm_idr)),
                            'data_adx_rekap_ecpm': int(round(cpm_idr)),
                            'data_adx_rekap_total_requests': int(item.get('total_requests') or 0),
                            'data_adx_rekap_responses_served': int(item.get('responses_served') or 0),
                            'data_adx_rekap_match_rate': int(round(float(item.get('match_rate') or 0))),
                            'data_adx_rekap_fill_rate': int(round(float(item.get('fill_rate') or 0))),
                            'data_adx_rekap_active_view_pct_viewable': int(round(float(item.get('active_view_pct_viewable') or 0))),
                            'data_adx_rekap_active_view_avg_time_sec': int(round(float(item.get('active_view_avg_time_sec') or 0))),
                            'data_adx_rekap_revenue': int(round(revenue_idr)),
                            'mdb': '0',
                            'mdb_name': 'Cron Rekap Bulanan',
                            'mdd': now_str,
                        }

                        del_res = db.delete_data_adx_rekap(
                            account_id,
                            site_name,
                            rekap_tahun,
                            rekap_bulan,
                            pull_date_str,
                        )
                        if del_res.get('hasil', {}).get('status'):
                            affected = del_res.get('hasil', {}).get('affected', 0)
                            if affected:
                                self.stdout.write(self.style.WARNING(
                                    f"Hapus rekap AdX existing ({affected}): {account_name}/{site_name}"
                                ))

                        ins = db.insert_data_adx_rekap(record)
                        if ins.get('hasil', {}).get('status'):
                            total_insert += 1
                        else:
                            total_error += 1
                            self.stdout.write(self.style.ERROR(
                                f"Gagal insert rekap AdX {account_name}/{site_name}: {ins.get('hasil', {})}"
                            ))
                    except Exception as ie:
                        total_error += 1
                        self.stdout.write(self.style.ERROR(
                            f"Gagal proses domain AdX {account_name}: {ie}"
                        ))
            except Exception as exc:
                total_error += 1
                self.stdout.write(self.style.ERROR(
                    f"Gagal memproses kredensial AdX {account_name}: {exc}"
                ))

        if total_insert:
            try:
                call_command(
                    'sync_clickhouse',
                    tables='data_adx_rekap',
                    since=pull_date_str,
                )
            except Exception as exc:
                self.stdout.write(self.style.ERROR(f"Gagal sync ClickHouse data_adx_rekap: {exc}"))

        self.stdout.write(self.style.SUCCESS(
            f"Selesai rekap AdX {rekap_tahun}-{rekap_bulan} (tarik {pull_date_str}). "
            f"Berhasil insert: {total_insert}, gagal: {total_error}."
        ))

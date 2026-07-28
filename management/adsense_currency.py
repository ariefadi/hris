"""Konversi pendapatan AdSense ke IDR untuk cron / ETL."""
import os


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


def force_usd_by_domain(domain):
    d = str(domain or '').strip().lower()
    if not d:
        return False
    usd_domains = ('uaetiming', 'valoranewspekanbaru', 'sharpdrivers')
    extra = os.getenv('ADSENSE_FORCE_USD_DOMAIN_SUBSTR', '')
    keys = list(usd_domains) + [x.strip().lower() for x in extra.split(',') if x.strip()]
    return any(k in d for k in keys)


def resolve_adsense_report_currency(res):
    """Mata uang dari respons fetch AdSense (account + laporan). Tanpa fallback AdX."""
    if not isinstance(res, dict):
        return 'IDR'
    c = str(res.get('currency_code') or '').strip().upper()
    return c or 'IDR'


def effective_currency_for_row(report_currency, domain, revenue=0.0, impressions=0):
    del revenue, impressions  # hanya currency API / daftar domain, tanpa tebak-tebakan CPM
    if force_usd_by_domain(domain):
        return 'USD'
    cur = str(report_currency or 'IDR').strip().upper() or 'IDR'
    return cur


def revenue_amount_to_idr(revenue, report_currency, domain, impressions=0):
    cur = effective_currency_for_row(report_currency, domain, revenue, impressions)
    return convert_to_idr(revenue, cur)

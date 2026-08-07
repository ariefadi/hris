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
            'HKD': float(os.getenv('HKD_IDR_RATE', os.getenv('HKG_IDR_RATE', '2050'))),
            'GBP': float(os.getenv('GBP_IDR_RATE', '20000')),
        }
        rate = default_rates.get(code)
        return base * rate if rate else base
    except Exception:
        return float(amount or 0.0)


def resolve_adsense_report_currency(res):
    """Mata uang dari respons fetch AdSense (METRIC_CURRENCY di header laporan)."""
    if not isinstance(res, dict):
        return 'IDR'
    c = str(res.get('currency_code') or '').strip().upper()
    return c or 'IDR'


def effective_currency_for_row(report_currency, domain, revenue=0.0, impressions=0):
    """Gunakan mata uang dari laporan AdSense; IDR tidak dikonversi lagi."""
    del domain, revenue, impressions
    return str(report_currency or 'IDR').strip().upper() or 'IDR'


def revenue_amount_to_idr(revenue, report_currency, domain, impressions=0):
    cur = effective_currency_for_row(report_currency, domain, revenue, impressions)
    return convert_to_idr(revenue, cur)

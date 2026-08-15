from datetime import datetime, date


def _db():
    try:
        from .database import data_mysql
    except Exception:
        from management.database import data_mysql
    return data_mysql()


def parse_last_update(value):
    """Normalize DB/ClickHouse last_update values to datetime for Django templates."""
    if value in (None, ''):
        return None
    if isinstance(value, datetime):
        return value
    if isinstance(value, date):
        return datetime.combine(value, datetime.min.time())
    s = str(value).strip()
    if not s:
        return None
    for fmt in ('%Y-%m-%d %H:%M:%S', '%Y-%m-%d'):
        try:
            if fmt == '%Y-%m-%d':
                return datetime.strptime(s[:10], fmt)
            return datetime.strptime(s[:19], fmt)
        except ValueError:
            continue
    try:
        return datetime.fromisoformat(s.replace('Z', '+00:00'))
    except ValueError:
        return None


def last_update_from_resp(resp):
    data = (resp or {}).get('data')
    if isinstance(data, dict):
        return parse_last_update(data.get('last_update'))
    if isinstance(data, list):
        vals = [
            parse_last_update(row.get('last_update'))
            for row in data
            if isinstance(row, dict) and row.get('last_update') not in (None, '')
        ]
        vals = [v for v in vals if v is not None]
        return max(vals) if vals else None
    return None


def resolve_adx_last_update(start_date=None, end_date=None, account_list=None, domain_list=None):
    db = _db()
    if start_date and end_date:
        if domain_list:
            resp = db.get_last_update_adx_monitoring_by_domain_params(start_date, end_date, account_list, domain_list)
        else:
            resp = db.get_last_update_adx_monitoring_by_params(start_date, end_date, account_list)
        return last_update_from_resp(resp)
    return last_update_from_resp(db.get_last_update_adx_traffic_country())


def resolve_adsense_last_update(start_date=None, end_date=None, account_list=None, domain_list=None):
    db = _db()
    if start_date and end_date:
        if domain_list:
            resp = db.get_last_update_adsense_monitoring_by_domain_params(start_date, end_date, account_list, domain_list)
        else:
            resp = db.get_last_update_adsense_monitoring_by_params(start_date, end_date, account_list)
        return last_update_from_resp(resp)
    return last_update_from_resp(db.get_last_update_adsense_traffic_country())


def resolve_ads_campaign_last_update(start_date=None, end_date=None, account_list=None, domain_list=None):
    db = _db()
    if start_date and end_date:
        resp = db.get_last_update_ads_campaign_by_params(start_date, end_date, account_list, domain_list)
        return last_update_from_resp(resp)
    return last_update_from_resp(db.get_last_update_ads_traffic_per_domain())


def serialize_last_update(value):
    parsed = parse_last_update(value)
    if parsed is None:
        return ''
    return parsed.strftime('%Y-%m-%d %H:%M:%S')

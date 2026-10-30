"""Persistence + Google-connected domain inventory for expiry checks."""
from __future__ import annotations

import json
from datetime import datetime

from management.database import data_mysql
from management.domain_expiry import RENEW_DAYS, registrable_domain, row_tone

TABLE = 'data_domain_expiry'


def _db(db=None):
    return db or data_mysql()


def ensure_table(db=None):
    db = _db(db)
    sql = f"""
        CREATE TABLE IF NOT EXISTS {TABLE} (
            domain VARCHAR(253) NOT NULL,
            expired_date DATE DEFAULT NULL,
            days_left INT DEFAULT NULL,
            registrar VARCHAR(255) DEFAULT NULL,
            rdap_status VARCHAR(512) DEFAULT NULL,
            source VARCHAR(32) DEFAULT NULL,
            last_checked DATETIME DEFAULT NULL,
            notes TEXT,
            not_found TINYINT(1) NOT NULL DEFAULT 0,
            PRIMARY KEY (domain),
            KEY idx_domain_expiry_days (days_left),
            KEY idx_domain_expiry_checked (last_checked)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """
    db.execute_query(sql)
    db.commit()
    return True


def _google_site_rows(db=None):
    db = _db(db)
    sql = """
        SELECT LOWER(TRIM(b.data_adx_domain)) AS site_host,
               a.account_id AS account_id,
               a.account_name AS account_name,
               a.user_mail AS user_mail,
               'adx' AS channel
        FROM app_credentials a
        INNER JOIN data_adx_domain b ON a.account_id = b.account_id
        WHERE b.data_adx_domain IS NOT NULL AND TRIM(b.data_adx_domain) <> ''
        GROUP BY site_host, a.account_id, a.account_name, a.user_mail
        UNION ALL
        SELECT LOWER(TRIM(b.data_adsense_domain)) AS site_host,
               a.account_id AS account_id,
               a.account_name AS account_name,
               a.user_mail AS user_mail,
               'adsense' AS channel
        FROM app_credentials a
        INNER JOIN data_adsense_domain b ON a.account_id = b.account_id
        WHERE b.data_adsense_domain IS NOT NULL AND TRIM(b.data_adsense_domain) <> ''
        GROUP BY site_host, a.account_id, a.account_name, a.user_mail
    """
    if not db.execute_query(sql):
        return []
    return db.fetch_all() or []


def list_google_domains(db=None):
    grouped = {}
    for row in _google_site_rows(db):
        apex = registrable_domain((row or {}).get('site_host'))
        if not apex:
            continue
        item = grouped.setdefault(apex, {
            'domain': apex,
            'accounts': [],
            'channels': set(),
            'account_names': [],
            'mails': [],
            '_seen': set(),
        })
        channel = str((row or {}).get('channel') or '').lower()
        account_id = str((row or {}).get('account_id') or '')
        seen_key = (account_id, channel)
        if seen_key in item['_seen']:
            continue
        item['_seen'].add(seen_key)
        if channel:
            item['channels'].add(channel)
        name = str((row or {}).get('account_name') or (row or {}).get('user_mail') or account_id or '').strip()
        mail = str((row or {}).get('user_mail') or '').strip()
        if name and name not in item['account_names']:
            item['account_names'].append(name)
        if mail and mail not in item['mails']:
            item['mails'].append(mail)
        item['accounts'].append({
            'account_id': account_id,
            'account_name': name,
            'user_mail': mail,
            'channel': channel,
        })
    out = []
    for domain, item in grouped.items():
        item.pop('_seen', None)
        channels = sorted(item['channels'])
        out.append({
            'domain': domain,
            'channels': channels,
            'channel_label': ' + '.join(x.upper() if x != 'adsense' else 'AdSense' for x in channels) or '-',
            'account_label': ', '.join(item['account_names'][:4]) + ('…' if len(item['account_names']) > 4 else ''),
            'accounts': item['accounts'],
            'google_mail': ', '.join(item['mails'][:3]),
        })
    out.sort(key=lambda x: x['domain'])
    return out


def _cached_map(db=None):
    db = _db(db)
    ensure_table(db)
    if not db.execute_query(f'SELECT * FROM {TABLE}'):
        return {}
    rows = db.fetch_all() or []
    out = {}
    for row in rows:
        domain = str((row or {}).get('domain') or '').strip().lower()
        if domain:
            out[domain] = row
    return out


def local_domain_record(domain, db=None):
    domain = str(domain or '').strip().lower()
    if not domain:
        return None
    db = _db(db)
    sql = """
        SELECT domain, provider, registrar, domain_status, registration_date,
               expiration_date, nameservers, primary_ip, contact_email, notes
        FROM data_domains
        WHERE LOWER(domain) = %s
        LIMIT 1
    """
    try:
        if not db.execute_query(sql, (domain,)):
            return None
        row = db.cur_hris.fetchone()
    except Exception:
        return None
    if not row:
        return None
    for key in ('registration_date', 'expiration_date'):
        val = row.get(key)
        if hasattr(val, 'isoformat'):
            row[key] = val.isoformat()
    ns = row.get('nameservers')
    if isinstance(ns, (bytes, bytearray)):
        ns = ns.decode('utf-8', 'ignore')
    if isinstance(ns, str) and ns.strip():
        try:
            row['nameservers'] = json.loads(ns)
        except Exception:
            row['nameservers'] = [part.strip() for part in ns.split(',') if part.strip()]
    return row


def google_domain_row(domain, db=None):
    domain = str(domain or '').strip().lower()
    if not domain:
        return None
    for item in list_google_domains(db):
        if item.get('domain') == domain:
            return item
    return None


def upsert_result(result, db=None):
    db = _db(db)
    ensure_table(db)
    domain = str((result or {}).get('domain') or '').strip().lower()
    if not domain:
        return False
    expired = result.get('expired_date') or None
    days = result.get('days_left')
    sql = f"""
        INSERT INTO {TABLE}
            (domain, expired_date, days_left, registrar, rdap_status, source, last_checked, notes, not_found)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            expired_date=VALUES(expired_date),
            days_left=VALUES(days_left),
            registrar=VALUES(registrar),
            rdap_status=VALUES(rdap_status),
            source=VALUES(source),
            last_checked=VALUES(last_checked),
            notes=VALUES(notes),
            not_found=VALUES(not_found)
    """
    ok = db.execute_query(sql, (
        domain,
        expired,
        days,
        result.get('registrar') or '',
        result.get('status') or '',
        result.get('source') or '',
        result.get('checked_at') or datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        result.get('note') or '',
        1 if result.get('not_found') else 0,
    ))
    if ok:
        db.commit()
    return bool(ok)


def serialize_row(google_row, cached=None):
    cached = cached or {}
    days = cached.get('days_left')
    try:
        days = int(days) if days is not None and str(days) != '' else None
    except (TypeError, ValueError):
        days = None
    expired = cached.get('expired_date')
    if hasattr(expired, 'isoformat'):
        expired = expired.isoformat()
    last_checked = cached.get('last_checked')
    if hasattr(last_checked, 'strftime'):
        last_checked = last_checked.strftime('%Y-%m-%d %H:%M:%S')
    not_found = bool(cached.get('not_found'))
    expired_label = expired or ''
    if not_found:
        expired_label = 'Tidak terdaftar'
        tone = 'unregistered'
    elif days is not None:
        tone = row_tone(days)
        if not expired_label and cached and (cached.get('notes') or cached.get('source')):
            expired_label = 'Tidak ditemukan'
    elif cached and (cached.get('notes') or cached.get('source') or last_checked):
        tone = 'unknown'
        if not expired_label:
            expired_label = 'Tidak ditemukan'
    else:
        tone = 'pending'
    return {
        'domain': google_row.get('domain'),
        'expired_date': expired_label,
        'expired_raw': expired or '',
        'days_left': days,
        'registrar': cached.get('registrar') or '',
        'status': cached.get('rdap_status') or '',
        'source': cached.get('source') or '',
        'last_checked': last_checked or '',
        'notes': cached.get('notes') or '',
        'not_found': not_found,
        'tone': tone,
        'channels': google_row.get('channels') or [],
        'channel_label': google_row.get('channel_label') or '-',
        'account_label': google_row.get('account_label') or '-',
        'google_mail': google_row.get('google_mail') or '',
        'needs_renew': days is not None and int(days) <= RENEW_DAYS,
    }


def list_inventory(db=None):
    google_rows = list_google_domains(db)
    cached = _cached_map(db)
    rows = [serialize_row(item, cached.get(item['domain'])) for item in google_rows]
    summary = {
        'total': len(rows),
        'healthy': 0,
        'watch': 0,
        'soon': 0,
        'expired': 0,
        'unknown': 0,
        'pending': 0,
        'unregistered': 0,
    }
    for row in rows:
        tone = row.get('tone') or 'pending'
        summary[tone] = summary.get(tone, 0) + 1
    return {'rows': rows, 'summary': summary}


def alert_items(db=None):
    data = list_inventory(db)
    alerts = []
    for row in data.get('rows') or []:
        if not row.get('needs_renew'):
            continue
        days = row.get('days_left')
        if days is None:
            continue
        alerts.append({
            'domain': row.get('domain'),
            'days_left': days,
            'expired_date': row.get('expired_raw') or row.get('expired_date'),
            'tone': row.get('tone'),
            'account_label': row.get('account_label'),
        })
    alerts.sort(key=lambda x: (x.get('days_left') is None, x.get('days_left')))
    return alerts

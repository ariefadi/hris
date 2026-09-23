"""
Sinkronisasi baris tertentu dari MySQL ke ClickHouse (HTTP JSONEachRow).
Digunakan setelah insert/update/delete app_credentials agar reporting CH langsung mutakhir.
"""
from __future__ import annotations

import json
import logging
import os
from datetime import date, datetime

try:
    import requests
except Exception:
    requests = None

logger = logging.getLogger(__name__)


def clickhouse_app_credentials_sync_enabled() -> bool:
    flag = str(os.getenv('CH_SYNC_APP_CREDENTIALS', '1') or '').strip().lower()
    if flag in ('0', 'false', 'no', 'off'):
        return False
    host = (
        os.getenv('CH_HOST')
        or os.getenv('REPORT_DB_HOST')
        or os.getenv('DB_REPORT_HOST')
    )
    return bool(str(host or '').strip())


def _ch_config():
    from django.conf import settings
    host = (
        os.getenv('CH_HOST')
        or getattr(settings, 'CH_HOST', None)
        or os.getenv('REPORT_DB_HOST')
        or os.getenv('DB_REPORT_HOST')
        or '127.0.0.1'
    )
    raw_port = str(
        os.getenv('CH_PORT')
        or getattr(settings, 'CH_PORT', '')
        or os.getenv('REPORT_DB_PORT')
        or os.getenv('DB_REPORT_PORT')
        or '8123'
    ).strip()
    try:
        port = int(raw_port)
    except (TypeError, ValueError):
        port = 8123
    user = (
        os.getenv('CH_USER')
        or getattr(settings, 'CH_USER', None)
        or os.getenv('REPORT_DB_USER')
        or os.getenv('DB_REPORT_USER')
        or 'default'
    )
    password = (
        os.getenv('CH_PASSWORD')
        or getattr(settings, 'CH_PASSWORD', None)
        or os.getenv('REPORT_DB_PASSWORD')
        or os.getenv('DB_REPORT_PASSWORD')
        or ''
    )
    database = (
        os.getenv('CH_DB')
        or getattr(settings, 'CH_DB', None)
        or os.getenv('REPORT_DB_NAME')
        or os.getenv('DB_REPORT_NAME')
        or 'hris_trendHorizone'
    )
    timeout = int(os.getenv('CH_HTTP_TIMEOUT', '60'))
    return host, port, user, password, database, timeout


def _ch_post(sql_text: str) -> None:
    if requests is None:
        raise RuntimeError('requests library tidak tersedia untuk sync ClickHouse.')
    host, port, user, password, database, timeout = _ch_config()
    base = f'http://{host}:{port}/'
    params = {'database': database} if database else {}
    auth = (user, password or '') if user else None
    resp = requests.post(
        base,
        params=params,
        auth=auth,
        data=sql_text.encode('utf-8'),
        timeout=timeout,
    )
    if resp.status_code >= 400:
        body = (resp.text or '').strip()
        if len(body) > 2000:
            body = body[:2000] + '...'
        raise RuntimeError(f'ClickHouse HTTP error status={resp.status_code} body={body}')


def _json_row(row: dict) -> dict:
    import decimal
    out = {}
    for k, v in (row or {}).items():
        if v is None:
            out[k] = None
            continue
        if isinstance(v, datetime):
            out[k] = v.strftime('%Y-%m-%d %H:%M:%S')
        elif isinstance(v, date):
            out[k] = v.strftime('%Y-%m-%d')
        elif isinstance(v, decimal.Decimal):
            out[k] = float(v)
        elif isinstance(v, (bytes, bytearray)):
            try:
                out[k] = v.decode('utf-8', errors='replace')
            except Exception:
                out[k] = str(v)
        else:
            if k == 'mdb' and str(v) == '0':
                continue
            out[k] = v
    return out


def _fetch_mysql_app_credentials_row(user_mail=None, account_id=None):
    from management.database import data_mysql
    db = data_mysql()
    if account_id is not None and str(account_id).strip():
        sql = 'SELECT * FROM app_credentials WHERE account_id = %s LIMIT 1'
        params = (account_id,)
    elif user_mail:
        sql = 'SELECT * FROM app_credentials WHERE user_mail = %s LIMIT 1'
        params = (user_mail,)
    else:
        return None
    if not db.execute_query(sql, params):
        return None
    return db.cur_hris.fetchone()


def delete_app_credentials_from_clickhouse(account_id) -> bool:
    if not clickhouse_app_credentials_sync_enabled():
        return False
    aid = str(account_id or '').strip()
    if not aid:
        return False
    try:
        _ch_post(
            f"ALTER TABLE app_credentials DELETE WHERE account_id = {int(aid)} "
            f"SETTINGS mutations_sync=1"
        )
        return True
    except Exception as e:
        logger.warning('ClickHouse delete app_credentials account_id=%s failed: %s', aid, e)
        return False


def sync_app_credentials_to_clickhouse(user_mail=None, account_id=None) -> bool:
    """
    Upsert satu baris app_credentials ke ClickHouse (delete by account_id lalu insert).
    """
    if not clickhouse_app_credentials_sync_enabled():
        return False
    row = _fetch_mysql_app_credentials_row(user_mail=user_mail, account_id=account_id)
    if not row:
        if account_id is not None:
            return delete_app_credentials_from_clickhouse(account_id)
        return False
    aid = row.get('account_id') if isinstance(row, dict) else None
    if aid is None:
        return False
    try:
        delete_app_credentials_from_clickhouse(aid)
        payload = (
            'INSERT INTO app_credentials FORMAT JSONEachRow\n'
            + json.dumps(_json_row(row), ensure_ascii=False, default=str)
            + '\n'
        )
        _ch_post(payload)
        logger.info('Synced app_credentials to ClickHouse account_id=%s user_mail=%s', aid, row.get('user_mail'))
        return True
    except Exception as e:
        logger.warning(
            'ClickHouse sync app_credentials failed account_id=%s user_mail=%s: %s',
            aid,
            row.get('user_mail') if isinstance(row, dict) else None,
            e,
        )
        return False

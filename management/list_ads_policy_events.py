import uuid
from datetime import date, datetime, timedelta, timezone

from management.list_adsense_policy_events import (
    _domain_from_url,
    _event_id_for,
    _extract_message_body,
    _format_gmail_api_error,
    _header_value,
    _parse_email_date,
    _urls_from_text,
    build_gmail_service,
    fetch_message_full,
    list_message_ids,
)

TABLE_NAME = 'ads_policy_events'

META_GMAIL_QUERY = (
    '(from:facebookmail.com OR from:meta.com OR from:facebook.com) '
    '(policy OR violation OR "ad account" OR "ads account" OR disabled OR restricted OR '
    '"policy violation" OR rejected OR suspended OR "account quality" OR appeal)'
)


def _gmail_scope_error_message(refresh_err=''):
    return _format_gmail_api_error(refresh_err)


def _classify_meta(subject, body, snippet):
    s = f"{subject} {snippet} {body}".lower()

    if any(k in s for k in ('account disabled', 'ad account disabled', 'disabled your account', 'suspended')):
        event_type = 'account_disabled'
    elif any(k in s for k in ('ad rejected', 'ad was rejected', 'creative rejected', 'disapproved')):
        event_type = 'ad_rejected'
    elif any(k in s for k in ('restricted', 'limit reached', 'spending limit', 'delivery limited')):
        event_type = 'restricted'
    elif any(k in s for k in ('appeal', 'review request', 'under review')):
        event_type = 'under_review'
    else:
        event_type = 'policy_violation'

    if any(k in s for k in ('disabled', 'suspended', 'terminated', 'permanently restricted', 'rejected')):
        severity = 'error'
    elif event_type in ('account_disabled', 'ad_rejected'):
        severity = 'error'
    else:
        severity = 'warning'

    if any(k in s for k in ('resolved', 'reinstated', 'approved', 'no further action', 'issue fixed', 'restored')):
        status = 'resolved'
    else:
        status = 'active'

    return event_type, severity, status


def _ensure_table_columns(db):
    sql = """
        SELECT COLUMN_NAME
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = DATABASE()
          AND TABLE_NAME = %s
    """
    if not db.execute_query(sql, (TABLE_NAME,)):
        return None, getattr(db, 'last_error', '') or 'Gagal baca schema'
    rows = db.cur_hris.fetchall() or []
    cols = {r.get('COLUMN_NAME') for r in rows if r.get('COLUMN_NAME')}
    if not cols:
        return None, f"Tabel tidak ditemukan: {TABLE_NAME}"
    return cols, None


def _exists_event(db, event_id):
    sql = f"SELECT 1 AS ok FROM {TABLE_NAME} WHERE event_id = %s LIMIT 1"
    if not db.execute_query(sql, (event_id,)):
        return False
    return bool(db.cur_hris.fetchone())


def _ensure_table_exists(db):
    sql = f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            event_id VARCHAR(64) NOT NULL PRIMARY KEY,
            event_date DATE NULL,
            event_time VARCHAR(32) NULL,
            account_id VARCHAR(64) NULL,
            account_name VARCHAR(255) NULL,
            user_mail VARCHAR(255) NULL,
            subject VARCHAR(255) NULL,
            from_email VARCHAR(64) NULL,
            domain VARCHAR(255) NULL,
            url TEXT NULL,
            event_type VARCHAR(64) NULL,
            severity VARCHAR(32) NULL,
            status VARCHAR(32) NULL,
            source VARCHAR(32) NULL,
            raw_subject TEXT NULL,
            raw_body MEDIUMTEXT NULL,
            created_at VARCHAR(150) NULL,
            mdd VARCHAR(32) NULL,
            KEY idx_ads_policy_mdd (mdd),
            KEY idx_ads_policy_user_mail (user_mail),
            KEY idx_ads_policy_event_date (event_date)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """
    if not db.execute_query(sql):
        return getattr(db, 'last_error', '') or 'Gagal membuat tabel ads_policy_events'
    try:
        db.commit()
    except Exception:
        pass
    return None


def _fetch_meta_account_credentials(db, account_filter=None):
    sql = """
        SELECT
            m.account_ads_id,
            m.account_name,
            m.account_email AS user_mail,
            m.account_id,
            c.client_id,
            c.client_secret,
            c.refresh_token
        FROM master_account_ads m
        LEFT JOIN app_credentials c
          ON LOWER(TRIM(c.user_mail)) = LOWER(TRIM(m.account_email))
         AND c.is_active = '1'
        WHERE TRIM(COALESCE(m.account_email, '')) != ''
        ORDER BY m.account_name ASC
    """
    if not db.execute_query(sql):
        return [], getattr(db, 'last_error', '') or 'Gagal membaca master_account_ads'

    rows = db.cur_hris.fetchall() or []
    account_filters = account_filter if isinstance(account_filter, (list, tuple, set)) else ([account_filter] if account_filter else [])
    account_filters = [str(v).strip().lower() for v in account_filters if str(v).strip()]
    if account_filters:
        rows = [
            r for r in rows
            if any(
                flt in str(r.get('account_name') or '').strip().lower()
                or flt in str(r.get('user_mail') or '').strip().lower()
                or flt == str(r.get('account_ads_id') or '').strip().lower()
                or flt == str(r.get('account_id') or '').strip().lower()
                for flt in account_filters
            )
        ]
    return rows, None


def sync_ads_policy_events(db, days=180, max_per_user=200, sync_date=None, account_filter=None):
    err = _ensure_table_exists(db)
    if err:
        return {'status': False, 'error': err}

    cols, err = _ensure_table_columns(db)
    if err:
        return {'status': False, 'error': err}

    try:
        days = int(days)
    except Exception:
        days = 180
    try:
        max_per_user = int(max_per_user)
    except Exception:
        max_per_user = 200

    target_date = None
    if isinstance(sync_date, datetime):
        target_date = sync_date.date()
    elif isinstance(sync_date, date):
        target_date = sync_date
    elif str(sync_date or '').strip():
        try:
            target_date = datetime.strptime(str(sync_date).strip(), '%Y-%m-%d').date()
        except Exception:
            target_date = None

    base_query = META_GMAIL_QUERY
    if target_date:
        next_date = target_date + timedelta(days=1)
        query = f"{base_query} after:{target_date.strftime('%Y/%m/%d')} before:{next_date.strftime('%Y/%m/%d')}"
    else:
        query = f'{base_query} newer_than:{days}d'

    credentials_rows, err = _fetch_meta_account_credentials(db, account_filter=account_filter)
    if err:
        return {'status': False, 'error': err}
    if not credentials_rows:
        requested = ', '.join(account_filter) if isinstance(account_filter, (list, tuple, set)) and account_filter else '-'
        return {'status': False, 'error': f"Akun Meta Ads tidak ditemukan: {requested}"}

    steps = []
    inserted = 0
    skipped = 0

    for r in credentials_rows:
        account_id = str(r.get('account_id') or r.get('account_ads_id') or '').strip()
        account_name = str(r.get('account_name') or '')
        user_mail = str(r.get('user_mail') or '')
        client_id = r.get('client_id')
        client_secret = r.get('client_secret')
        refresh_token = r.get('refresh_token')

        step = {
            'account_id': account_id,
            'account_name': account_name,
            'user_mail': user_mail,
            'status': True,
            'inserted': 0,
            'skipped': 0,
            'error': '',
        }

        if not (client_id and client_secret and refresh_token):
            step['status'] = False
            step['error'] = (
                f"Belum ada OAuth Gmail untuk {user_mail or account_name}. "
                "Klik Izinkan Gmail lalu login dengan email akun Meta Ads tersebut."
            )
            steps.append(step)
            continue

        try:
            service, e = build_gmail_service(client_id, client_secret, refresh_token)
            if e:
                step['status'] = False
                step['error'] = _gmail_scope_error_message(e)
                steps.append(step)
                continue

            ids = list_message_ids(service, query=query, max_results=max_per_user)
            for mid in ids:
                msg = fetch_message_full(service, mid)

                event_id = _event_id_for(user_mail, msg.get('gmail_message_id'))
                if _exists_event(db, event_id):
                    step['skipped'] += 1
                    skipped += 1
                    continue

                dt = msg.get('dt_internal') or msg.get('dt_header') or datetime.now(timezone.utc)
                event_date = dt.date().isoformat()
                event_time = dt.strftime('%Y-%m-%d %H:%M:%S')

                subject = str(msg.get('subject') or '')
                from_email = str(msg.get('from_email') or '')
                raw_subject = subject
                raw_body = str(msg.get('body') or '')

                urls = _urls_from_text(raw_body) or _urls_from_text(msg.get('snippet') or '') or _urls_from_text(subject)
                url = urls[0] if urls else ''
                domain = _domain_from_url(url)

                event_type, severity, status = _classify_meta(subject, raw_body, msg.get('snippet') or '')

                now = datetime.now(timezone.utc)
                created_at = now.isoformat()
                mdd = now.strftime('%Y-%m-%d %H:%M:%S')

                row = {
                    'event_id': event_id,
                    'event_date': event_date,
                    'event_time': event_time,
                    'account_id': account_id[:64],
                    'account_name': account_name,
                    'user_mail': user_mail,
                    'subject': subject[:225],
                    'from_email': from_email[:50],
                    'domain': domain,
                    'url': url,
                    'event_type': event_type,
                    'severity': severity,
                    'status': status,
                    'source': 'gmail',
                    'raw_subject': raw_subject,
                    'raw_body': raw_body,
                    'created_at': created_at[:150],
                    'mdd': mdd,
                }

                insert_cols = [c for c in row.keys() if c in cols]
                placeholders = ','.join(['%s'] * len(insert_cols))
                col_list = ','.join(insert_cols)
                sql_ins = f"INSERT INTO {TABLE_NAME} ({col_list}) VALUES ({placeholders})"
                params = [row[c] for c in insert_cols]

                ok = db.execute_query(sql_ins, params)
                if not ok:
                    step['status'] = False
                    step['error'] = getattr(db, 'last_error', '') or 'Gagal insert'
                    break

                try:
                    db.commit()
                except Exception:
                    pass

                step['inserted'] += 1
                inserted += 1

        except Exception as e:
            step['status'] = False
            step['error'] = str(e)

        steps.append(step)

    failed = sum(1 for s in steps if not s.get('status'))

    if failed:
        message = f"Syncronize selesai dengan {failed} akun gagal."
    elif inserted == 0 and skipped == 0:
        message = "Tidak ada email policy Meta Ads yang cocok pada periode ini."
    elif inserted == 0 and skipped > 0:
        message = "Tidak ada email policy Meta Ads baru (semua sudah tersimpan)."
    else:
        message = f"Syncronize berhasil. Baru: {inserted}, skip: {skipped}."

    return {
        'status': True,
        'message': message,
        'table': TABLE_NAME,
        'query': query,
        'sync_date': target_date.isoformat() if target_date else '',
        'account_filter': list(account_filter) if isinstance(account_filter, (list, tuple, set)) else str(account_filter or ''),
        'inserted': inserted,
        'skipped': skipped,
        'failed': failed,
        'steps': steps,
    }


def list_ads_policy_events(db, limit=200):
    err = _ensure_table_exists(db)
    if err:
        return {'status': False, 'error': err, 'table': TABLE_NAME, 'columns': [], 'rows': []}

    cols, err = _ensure_table_columns(db)
    if err:
        return {'status': False, 'error': err, 'table': TABLE_NAME, 'columns': [], 'rows': []}

    try:
        limit = int(limit)
    except Exception:
        limit = 200

    preferred = [
        'event_date',
        'event_time',
        'account_name',
        'account_id',
        'user_mail',
        'event_type',
        'severity',
        'status',
        'source',
        'subject',
        'domain',
        'url',
        'raw_body',
        'from_email',
        'created_at',
        'mdd',
        'event_id',
    ]
    display_cols = [c for c in preferred if c in cols] + [c for c in cols if c not in preferred]

    order_col = 'mdd' if 'mdd' in cols else ('event_time' if 'event_time' in cols else None)
    select_cols = ','.join(display_cols)
    if order_col:
        sql = f"SELECT {select_cols} FROM {TABLE_NAME} ORDER BY {order_col} DESC LIMIT %s"
    else:
        sql = f"SELECT {select_cols} FROM {TABLE_NAME} LIMIT %s"

    if not db.execute_query(sql, (limit,)):
        return {'status': False, 'error': getattr(db, 'last_error', '') or 'Gagal query data', 'table': TABLE_NAME, 'columns': display_cols, 'rows': []}

    dict_rows = db.cur_hris.fetchall() or []
    rows = []
    for r in dict_rows:
        rows.append([r.get(c, '') for c in display_cols])

    return {
        'status': True,
        'table': TABLE_NAME,
        'columns': display_cols,
        'rows': rows,
        'items': dict_rows,
    }


def list_meta_ads_policy_accounts(db):
    sql = """
        SELECT account_name, account_email AS user_mail, account_ads_id
        FROM master_account_ads
        WHERE TRIM(COALESCE(account_email, '')) != ''
        ORDER BY account_name ASC
    """
    if not db.execute_query(sql):
        return []
    return db.cur_hris.fetchall() or []

import base64
import re
import uuid
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials

try:
    from googleapiclient.discovery import build
except Exception:
    build = None


GMAIL_READONLY_SCOPE = 'https://www.googleapis.com/auth/gmail.readonly'
TABLE_NAME = 'adsense_policy_events'


def _parse_email_date(date_header):
    v = str(date_header or '').strip()
    if not v:
        return None
    try:
        dt = parsedate_to_datetime(v)
        if dt and dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except Exception:
        return None


def _header_value(headers, name):
    n = str(name or '').strip().lower()
    for h in headers or []:
        if str(h.get('name') or '').strip().lower() == n:
            return str(h.get('value') or '')
    return ''


def _urls_from_text(text):
    t = str(text or '')
    if not t:
        return []
    urls = re.findall(r'(https?://[^\s<>"\')\]]+)', t, flags=re.IGNORECASE)
    cleaned = []
    for u in urls:
        u = u.strip().rstrip('.,;:)')
        if u and u not in cleaned:
            cleaned.append(u)
    return cleaned


def _domain_from_url(url):
    u = str(url or '').strip()
    if not u:
        return ''
    m = re.match(r'^https?://([^/]+)', u, flags=re.IGNORECASE)
    if not m:
        return ''
    host = (m.group(1) or '').strip().lower()
    host = host.split(':')[0]
    return host


def _extract_message_body(payload):
    if not payload:
        return ''

    def decode_body(data):
        if not data:
            return ''
        try:
            raw = base64.urlsafe_b64decode(data.encode('utf-8'))
            return raw.decode('utf-8', errors='replace')
        except Exception:
            return ''

    def walk(part):
        out = []
        if not part:
            return out
        body = part.get('body') or {}
        data = body.get('data')
        mime = str(part.get('mimeType') or '').lower()

        if data and (mime.startswith('text/plain') or mime.startswith('text/html') or not mime):
            out.append(decode_body(data))

        for p in (part.get('parts') or []):
            out.extend(walk(p))
        return out

    chunks = walk(payload)
    text = '\n'.join([c for c in chunks if c]).strip()
    if len(text) > 50000:
        text = text[:50000]
    return text


def _classify(subject, body, snippet):
    s = f"{subject} {snippet} {body}".lower()

    if any(k in s for k in ('invalid traffic', 'invalid activity')):
        event_type = 'invalid_traffic'
    elif any(k in s for k in ('limited ads', 'ad serving is limited', 'limited ad serving', 'ad serving limited')):
        event_type = 'limited_ads'
    else:
        event_type = 'violation'

    if any(k in s for k in ('disabled', 'suspended', 'terminated', 'account disabled', 'site disabled')):
        severity = 'error'
    elif event_type == 'invalid_traffic':
        severity = 'error'
    else:
        severity = 'warning'

    if any(k in s for k in ('resolved', 'issue resolved', 'no further action', 'we have lifted')):
        status = 'resolved'
    else:
        status = 'active'

    return event_type, severity, status


def build_gmail_service(client_id, client_secret, refresh_token):
    if build is None:
        return None, "googleapiclient tidak tersedia"

    cid = str(client_id or '').strip()
    csec = str(client_secret or '').strip()
    rt = str(refresh_token or '').strip()
    if not (cid and csec and rt):
        return None, "Kredensial tidak lengkap (client_id/client_secret/refresh_token)"

    creds = Credentials(
        token=None,
        refresh_token=rt,
        token_uri='https://oauth2.googleapis.com/token',
        client_id=cid,
        client_secret=csec,
        scopes=[GMAIL_READONLY_SCOPE],
    )
    creds.refresh(Request())
    service = build('gmail', 'v1', credentials=creds, cache_discovery=False)
    return service, None


def list_message_ids(service, query, max_results=200):
    ids = []
    page_token = None

    while True:
        remaining = max_results - len(ids)
        if remaining <= 0:
            break

        req = service.users().messages().list(
            userId='me',
            q=query,
            maxResults=min(100, remaining),
            pageToken=page_token,
            includeSpamTrash=False,
        )
        resp = req.execute() or {}
        for m in (resp.get('messages') or []):
            mid = str(m.get('id') or '').strip()
            if mid:
                ids.append(mid)

        page_token = resp.get('nextPageToken')
        if not page_token:
            break

    return ids


def fetch_message_full(service, message_id):
    msg = service.users().messages().get(
        userId='me',
        id=message_id,
        format='full',
    ).execute() or {}

    payload = msg.get('payload') or {}
    headers = payload.get('headers') or []

    subject = _header_value(headers, 'Subject')
    from_v = _header_value(headers, 'From')
    date_v = _header_value(headers, 'Date')

    dt_hdr = _parse_email_date(date_v)
    internal_ms = msg.get('internalDate')
    dt_internal = None
    try:
        if internal_ms is not None:
            dt_internal = datetime.fromtimestamp(int(internal_ms) / 1000.0, tz=timezone.utc)
    except Exception:
        dt_internal = None

    body = _extract_message_body(payload)
    snippet = str(msg.get('snippet') or '')

    return {
        'gmail_message_id': str(msg.get('id') or ''),
        'gmail_thread_id': str(msg.get('threadId') or ''),
        'subject': subject,
        'from_email': from_v,
        'date_header': date_v,
        'dt_header': dt_hdr,
        'dt_internal': dt_internal,
        'snippet': snippet,
        'body': body,
    }


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


def _event_id_for(user_mail, gmail_message_id):
    base = f"{str(user_mail or '').strip().lower()}:{str(gmail_message_id or '').strip()}"
    return str(uuid.uuid5(uuid.NAMESPACE_URL, base))


def _exists_event(db, event_id):
    sql = f"SELECT 1 AS ok FROM {TABLE_NAME} WHERE event_id = %s LIMIT 1"
    if not db.execute_query(sql, (event_id,)):
        return False
    return bool(db.cur_hris.fetchone())


def sync_adsense_policy_events(db, days=180, max_per_user=200):
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

    query = f'from:adsense-noreply@google.com (policy OR violation OR "Policy issue" OR "limited ads" OR "invalid traffic") newer_than:{days}d'

    sql_creds = """
        SELECT account_id, account_name, user_mail, client_id, client_secret, refresh_token
        FROM app_credentials
        WHERE is_active = '1'
        ORDER BY account_name ASC
    """
    if not db.execute_query(sql_creds):
        return {'status': False, 'error': getattr(db, 'last_error', '') or 'Gagal membaca app_credentials'}

    credentials_rows = db.cur_hris.fetchall() or []
    steps = []
    inserted = 0
    skipped = 0

    for r in credentials_rows:
        account_id_raw = r.get('account_id')
        try:
            account_id = int(account_id_raw)
        except Exception:
            account_id = 0

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

        try:
            service, e = build_gmail_service(client_id, client_secret, refresh_token)
            if e:
                step['status'] = False
                step['error'] = e
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

                event_type, severity, status = _classify(subject, raw_body, msg.get('snippet') or '')

                now = datetime.now(timezone.utc)
                created_at = now.isoformat()
                mdd = now.strftime('%Y-%m-%d %H:%M:%S')

                row = {
                    'event_id': event_id,
                    'event_date': event_date,
                    'event_time': event_time,
                    'account_id': account_id,
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

    failed = 0
    for s in steps:
        if not s.get('status'):
            failed += 1

    if failed:
        message = f"Syncronize selesai dengan {failed} akun gagal."
    elif inserted == 0 and skipped == 0:
        message = "Tidak ada email policy yang cocok pada periode ini."
    elif inserted == 0 and skipped > 0:
        message = "Tidak ada email policy baru (semua sudah tersimpan)."
    else:
        message = f"Syncronize berhasil. Baru: {inserted}, skip: {skipped}."

    return {
        'status': True,
        'message': message,
        'table': TABLE_NAME,
        'query': query,
        'inserted': inserted,
        'skipped': skipped,
        'failed': failed,
        'steps': steps,
    }


def list_adsense_policy_events(db, limit=200):
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
        'from_email',
        'domain',
        'url',
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

    return {'status': True, 'table': TABLE_NAME, 'columns': display_cols, 'rows': rows}
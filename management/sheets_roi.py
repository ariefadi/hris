"""ROI TODAY rows and the private Google Spreadsheet updater."""

from datetime import datetime, timedelta
import json
import os
import subprocess

import requests
from django.db import connection
from django.shortcuts import redirect
from django.urls import reverse
from django.views import View
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from googleapiclient.discovery import build

from management.database import data_mysql, normalize_roi_domain_merge_key

SHARE_EMAIL = 'ariefadi24@trendhorizone.id'
PUBLIC_REDIRECT_URI = 'https://kiwipixel.com/management/admin/oauth/callback/'
SPREADSHEET_TITLE = 'Earning Database 2026'
SHEET_TITLE = 'ROI TODAY'
HISTORY_TITLE = 'RIWAYAT'
HEADER = ('Account', 'Site', 'Date', 'Earnings', 'Modal', 'ROI', 'Status')
SCOPES = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive.file',
    'https://www.googleapis.com/auth/userinfo.email',
]

VAR_DIR = '/var/www/html/hris/var'
CLIENT_PATH = os.path.join(VAR_DIR, 'roi_spreadsheet_client.json')
TOKEN_PATH = os.path.join(VAR_DIR, 'roi_spreadsheet_token.json')
SHEET_PATH = os.path.join(VAR_DIR, 'roi_spreadsheet.json')
STATE_PATH = os.path.join(VAR_DIR, 'roi_spreadsheet_state.json')

# Hourly pulls that must finish before the sheet is rewritten.
PULL_JOBS = (
    ('ads_country', '/var/log/hris/cron_ads_country_load.log', '/var/run/hris_ads_country_load.lock', 35),
    ('ads_campaign', '/var/log/hris/cron_ads_campaign_load.log', '/var/run/hris_ads_campaign_load.lock', 35),
    ('adx_country', '/var/log/hris/cron_adx_country_load.log', '/var/run/hris_adx_country_load.lock', 30),
    ('adx_domain', '/var/log/hris/cron_adx_domain_load.log', '/var/run/hris_adx_domain_load.lock', 30),
    ('adsense_country', '/var/log/hris/cron_adsense_country_load.log', '/var/run/hris_adsense_country_load.lock', 35),
    ('adsense_domain', '/var/log/hris/cron_adsense_domain_load.log', '/var/run/hris_adsense_domain_load.lock', 35),
)


def display_site(value):
    site = str(value or '').strip().lower().strip('.')
    if site.startswith('www.'):
        site = site[4:]
    for suffix in ('.adx', '.disp', '.display'):
        if site.endswith(suffix):
            site = site[:-len(suffix)].strip('.')
    return site


def _prefer_site_label(current, candidate):
    def score(value):
        text = str(value or '')
        rank = 0
        for tld in ('.co.id', '.web.id', '.com', '.net', '.org', '.id', '.io', '.top'):
            if text.endswith(tld):
                rank += 10
                break
        return rank + min(len(text), 80) / 1000.0
    if score(candidate) > score(current or ''):
        return candidate
    return current or candidate


def _slot_for(now, minute):
    slot = now.replace(minute=minute, second=0, microsecond=0)
    if now < slot:
        slot -= timedelta(hours=1)
    return slot


def target_cycle(now=None):
    """Hour whose :30/:35 pulls are the latest cycle we are allowed to publish."""
    now = now or datetime.now()
    cycle = now.replace(minute=0, second=0, microsecond=0)
    if now.minute < 30:
        cycle -= timedelta(hours=1)
    return cycle


def _lock_busy(path):
    try:
        result = subprocess.run(
            ['flock', '-n', path, '-c', 'true'],
            capture_output=True,
            timeout=5,
        )
    except Exception:
        return True
    return result.returncode != 0


def pull_status(now=None):
    now = now or datetime.now()
    cycle = target_cycle(now)
    pending = []
    for name, log_path, lock_path, minute in PULL_JOBS:
        slot = cycle.replace(minute=minute)
        if _lock_busy(lock_path):
            pending.append(name)
            continue
        try:
            mtime = datetime.fromtimestamp(os.path.getmtime(log_path))
        except OSError:
            pending.append(name)
            continue
        if mtime < slot:
            pending.append(name)
    return {
        'cycle': cycle,
        'cycle_key': cycle.strftime('%Y-%m-%d-%H'),
        'ready': not pending,
        'pending': pending,
    }


def _query_rows(sql, params):
    db = data_mysql()
    if not db.execute_query(sql, tuple(params)):
        raise RuntimeError('Query laporan gagal')
    rows = db.fetch_all() or []
    if not isinstance(rows, list):
        raise RuntimeError('Query laporan tidak mengembalikan baris')
    return rows


def _account_names():
    names = {}
    with connection.cursor() as cursor:
        cursor.execute("SELECT account_id, account_name FROM app_credentials")
        for account_id, account_name in cursor.fetchall():
            key = str(account_id or '').strip()
            label = str(account_name or '').strip()
            if key and label:
                names[key] = label
    return names


def build_roi_rows(report_date):
    """One row per account + site for the report date.

    Earnings are AdX plus AdSense. Modal is Facebook spend for that site,
    attached once to the account with the larger earnings.
    """
    day = str(report_date)
    adx_rows = _query_rows(
        """
        SELECT toString(account_id) AS account_id,
               data_adx_country_domain AS domain,
               sum(data_adx_country_revenue) AS revenue
        FROM data_adx_country
        WHERE toDate(data_adx_country_tanggal) = toDate(%s)
        GROUP BY account_id, domain
        """,
        (day,),
    )
    adsense_rows = _query_rows(
        """
        SELECT toString(account_id) AS account_id,
               data_adsense_country_domain AS domain,
               sum(data_adsense_country_revenue) AS revenue
        FROM data_adsense_country
        WHERE toDate(data_adsense_country_tanggal) = toDate(%s)
        GROUP BY account_id, domain
        """,
        (day,),
    )
    ads_rows = _query_rows(
        """
        SELECT data_ads_domain AS domain,
               sum(data_ads_country_spend) AS spend
        FROM data_ads_country
        WHERE toDate(data_ads_country_tanggal) = toDate(%s)
        GROUP BY domain
        """,
        (day,),
    )

    grouped = {}
    for source_rows in (adx_rows, adsense_rows):
        for row in source_rows:
            account_id = str((row or {}).get('account_id') or '').strip()
            label = display_site((row or {}).get('domain'))
            site_key = normalize_roi_domain_merge_key(label)
            if not account_id or not site_key:
                continue
            bucket = grouped.setdefault((account_id, site_key), {'earnings': 0.0, 'site': label})
            bucket['site'] = _prefer_site_label(bucket.get('site'), label)
            try:
                bucket['earnings'] += float((row or {}).get('revenue') or 0)
            except (TypeError, ValueError):
                pass

    spend_by_site = {}
    for row in ads_rows:
        site_key = normalize_roi_domain_merge_key(display_site((row or {}).get('domain')))
        if not site_key:
            continue
        try:
            spend_by_site[site_key] = spend_by_site.get(site_key, 0.0) + float((row or {}).get('spend') or 0)
        except (TypeError, ValueError):
            pass

    owner = {}
    for (account_id, site), bucket in grouped.items():
        current = owner.get(site)
        if current is None or bucket['earnings'] > current[0]:
            owner[site] = (bucket['earnings'], account_id)

    names = _account_names()
    items = []
    for (account_id, site), bucket in grouped.items():
        earnings = round(bucket['earnings'], 2)
        modal = 0.0
        if owner.get(site, (None, None))[1] == account_id:
            modal = round(spend_by_site.get(site, 0.0), 2)
        matched = modal > 0
        roi = round(((earnings - modal) / modal) * 100, 2) if matched else None
        items.append({
            'account': names.get(account_id) or account_id,
            'site': bucket.get('site') or site,
            'date': day,
            'earnings': earnings,
            'modal': modal if matched else None,
            'roi': roi,
            'status': 'MATCHED' if matched else 'NO SPEND',
        })
    items.sort(key=lambda item: (item['account'].lower(), item['site']))
    return items


def _ensure_var():
    os.makedirs(VAR_DIR, mode=0o750, exist_ok=True)


def _read_json(path):
    try:
        with open(path, 'r', encoding='utf-8') as handle:
            return json.load(handle)
    except (OSError, ValueError):
        return {}


def _write_json(path, payload):
    _ensure_var()
    tmp = path + '.tmp'
    with open(tmp, 'w', encoding='utf-8') as handle:
        json.dump(payload, handle)
    os.chmod(tmp, 0o600)
    if os.geteuid() == 0:
        # Root cron and the www-data web worker must both read these files.
        owner = os.stat(VAR_DIR)
        os.chown(tmp, owner.st_uid, owner.st_gid)
    os.replace(tmp, path)


def _sheets_client_file():
    """Client from the Google Cloud JSON download (``{"web": {...}}``), if present."""
    data = _read_json(CLIENT_PATH)
    data = data.get('web') or data.get('installed') or data
    client_id = str(data.get('client_id') or '').strip()
    client_secret = str(data.get('client_secret') or '').strip()
    if client_id and client_secret:
        return client_id, client_secret
    return None


def _client_secret(client_id):
    own = _sheets_client_file()
    if own and own[0] == client_id:
        return own[1]
    with connection.cursor() as cursor:
        cursor.execute(
            "SELECT client_secret FROM app_credentials WHERE client_id=%s AND client_secret<>'' LIMIT 1",
            [client_id],
        )
        row = cursor.fetchone()
    if not row or not row[0]:
        raise RuntimeError('Client Google untuk spreadsheet tidak ditemukan')
    return row[0]


def _oauth_client():
    own = _sheets_client_file()
    if own:
        return own
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT client_id, client_secret
            FROM app_credentials
            WHERE is_active='1' AND client_id<>'' AND client_secret<>''
            ORDER BY mdd DESC
            LIMIT 1
            """
        )
        row = cursor.fetchone()
    if not row:
        raise RuntimeError('Tidak ada kredensial Google yang aktif')
    return row[0], row[1]


class RoiSpreadsheetConnectView(View):
    def dispatch(self, request, *args, **kwargs):
        if 'hris_admin' not in request.session:
            return redirect('admin_login')
        return super().dispatch(request, *args, **kwargs)

    def get(self, request):
        return redirect(build_authorize_url(PUBLIC_REDIRECT_URI))


def build_authorize_url(redirect_uri):
    client_id, _secret = _oauth_client()
    params = {
        'client_id': client_id,
        'redirect_uri': redirect_uri,
        'response_type': 'code',
        'scope': ' '.join(SCOPES),
        'access_type': 'offline',
        'prompt': 'consent',
        'include_granted_scopes': 'false',
        'login_hint': SHARE_EMAIL,
        'state': 'flow:sheets',
    }
    return 'https://accounts.google.com/o/oauth2/v2/auth?' + requests.compat.urlencode(params)


def exchange_sheets_code(auth_code, redirect_uri):
    client_id, client_secret = _oauth_client()
    response = requests.post(
        'https://oauth2.googleapis.com/token',
        data={
            'code': auth_code,
            'client_id': client_id,
            'client_secret': client_secret,
            'redirect_uri': redirect_uri,
            'grant_type': 'authorization_code',
        },
        timeout=30,
    )
    payload = response.json()
    if response.status_code != 200 or not payload.get('refresh_token'):
        message = payload.get('error_description') or payload.get('error') or 'Google menolak otorisasi'
        raise RuntimeError(message)
    access = payload.get('access_token')
    profile = requests.get(
        'https://www.googleapis.com/oauth2/v2/userinfo',
        headers={'Authorization': 'Bearer ' + access},
        timeout=20,
    )
    email = str((profile.json() or {}).get('email') or '').strip().lower()
    if email != SHARE_EMAIL:
        raise RuntimeError(
            'Login Google harus memakai ' + SHARE_EMAIL + '. Akun yang masuk: ' + (email or '-')
        )
    _write_json(TOKEN_PATH, {
        'refresh_token': payload['refresh_token'],
        'client_id': client_id,
        'email': email,
    })
    return email


def sheets_credentials():
    saved = _read_json(TOKEN_PATH)
    refresh = str(saved.get('refresh_token') or '').strip()
    client_id = str(saved.get('client_id') or '').strip()
    if not refresh or not client_id:
        raise RuntimeError('Spreadsheet belum dihubungkan ke ' + SHARE_EMAIL)
    own = _sheets_client_file()
    if own and own[0] != client_id:
        raise RuntimeError(
            'Client Google spreadsheet sudah diganti; hubungkan ulang lewat '
            '/management/admin/roi_spreadsheet/connect memakai ' + SHARE_EMAIL
        )
    creds = Credentials(
        token=None,
        refresh_token=refresh,
        token_uri='https://oauth2.googleapis.com/token',
        client_id=client_id,
        client_secret=_client_secret(client_id),
        scopes=SCOPES,
    )
    creds.refresh(Request())
    return creds


def _sheet_file():
    saved = _read_json(SHEET_PATH)
    sheet_id = str(saved.get('spreadsheet_id') or '').strip()
    return sheet_id


def _save_sheet_file(spreadsheet_id, url):
    _write_json(SHEET_PATH, {'spreadsheet_id': spreadsheet_id, 'url': url})


def _tabs(sheets, spreadsheet_id):
    meta = sheets.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
    return {
        sheet.get('properties', {}).get('title'): sheet
        for sheet in meta.get('sheets') or []
    }


def _ensure_tab(sheets, spreadsheet_id, title):
    tabs = _tabs(sheets, spreadsheet_id)
    if title not in tabs:
        sheets.spreadsheets().batchUpdate(
            spreadsheetId=spreadsheet_id,
            body={'requests': [{'addSheet': {'properties': {
                'title': title,
                'gridProperties': {'frozenRowCount': 1},
            }}}]},
        ).execute()
        tabs = _tabs(sheets, spreadsheet_id)
    return tabs[title]


def _apply_layout(sheets, spreadsheet_id, sheet):
    sheet_id = sheet.get('properties', {}).get('sheetId', 0)
    has_status_rule = bool(sheet.get('conditionalFormats'))
    requests_body = [
        {
            'updateSheetProperties': {
                'properties': {
                    'sheetId': sheet_id,
                    'gridProperties': {'frozenRowCount': 1},
                },
                'fields': 'gridProperties.frozenRowCount',
            }
        },
        {
            'repeatCell': {
                'range': {'sheetId': sheet_id, 'startRowIndex': 0, 'endRowIndex': 1},
                'cell': {
                    'userEnteredFormat': {
                        'backgroundColor': {'red': 0.12, 'green': 0.23, 'blue': 0.37},
                        'textFormat': {
                            'bold': True,
                            'foregroundColor': {'red': 1, 'green': 1, 'blue': 1},
                        },
                        'horizontalAlignment': 'CENTER',
                    }
                },
                'fields': 'userEnteredFormat(backgroundColor,textFormat,horizontalAlignment)',
            }
        },
        {
            'repeatCell': {
                'range': {
                    'sheetId': sheet_id,
                    'startRowIndex': 1,
                    'startColumnIndex': 3,
                    'endColumnIndex': 5,
                },
                'cell': {'userEnteredFormat': {'numberFormat': {'type': 'NUMBER', 'pattern': '"Rp"#,##0.00'}}},
                'fields': 'userEnteredFormat.numberFormat',
            }
        },
        {
            'repeatCell': {
                'range': {
                    'sheetId': sheet_id,
                    'startRowIndex': 1,
                    'startColumnIndex': 5,
                    'endColumnIndex': 6,
                },
                'cell': {'userEnteredFormat': {'numberFormat': {'type': 'NUMBER', 'pattern': '0.00"%"'}}},
                'fields': 'userEnteredFormat.numberFormat',
            }
        },
    ]
    widths = (260, 240, 120, 150, 150, 110, 120)
    for index, width in enumerate(widths):
        requests_body.append({
            'updateDimensionProperties': {
                'range': {
                    'sheetId': sheet_id,
                    'dimension': 'COLUMNS',
                    'startIndex': index,
                    'endIndex': index + 1,
                },
                'properties': {'pixelSize': width},
                'fields': 'pixelSize',
            }
        })
    status_rule = {
        'ranges': [{
            'sheetId': sheet_id,
            'startRowIndex': 1,
            'startColumnIndex': 6,
            'endColumnIndex': 7,
        }],
        'booleanRule': {
            'condition': {'type': 'TEXT_EQ', 'values': [{'userEnteredValue': 'MATCHED'}]},
            'format': {'textFormat': {'foregroundColor': {'red': 0.08, 'green': 0.5, 'blue': 0.24}, 'bold': True}},
        },
    }
    if has_status_rule:
        requests_body.append({'updateConditionalFormatRule': {
            'sheetId': sheet_id, 'index': 0, 'rule': status_rule,
        }})
    else:
        requests_body.append({'addConditionalFormatRule': {'rule': status_rule, 'index': 0}})
    sheets.spreadsheets().batchUpdate(
        spreadsheetId=spreadsheet_id,
        body={'requests': requests_body},
    ).execute()


def _row_values(item):
    return [
        item['account'],
        item['site'],
        item['date'],
        item['earnings'],
        '' if item['modal'] is None else item['modal'],
        '' if item['roi'] is None else item['roi'],
        item['status'],
    ]


def _write_tab(sheets, spreadsheet_id, title, body_rows):
    sheets.spreadsheets().values().clear(
        spreadsheetId=spreadsheet_id,
        range=title,
    ).execute()
    sheets.spreadsheets().values().update(
        spreadsheetId=spreadsheet_id,
        range=title + '!A1',
        valueInputOption='RAW',
        body={'values': [list(HEADER)] + body_rows},
    ).execute()


def _history_rows(sheets, spreadsheet_id, rows_by_date):
    """Existing RIWAYAT rows with the given dates replaced, newest date first."""
    existing = sheets.spreadsheets().values().get(
        spreadsheetId=spreadsheet_id,
        range=HISTORY_TITLE + '!A2:G',
        valueRenderOption='UNFORMATTED_VALUE',
    ).execute().get('values') or []
    kept = []
    for row in existing:
        row = list(row) + [''] * (len(HEADER) - len(row))
        if str(row[2]) and str(row[2]) not in rows_by_date:
            kept.append(row[:len(HEADER)])
    for items in rows_by_date.values():
        kept.extend(_row_values(item) for item in items)
    kept.sort(key=lambda row: (str(row[0]).lower(), str(row[1])))
    kept.sort(key=lambda row: str(row[2]), reverse=True)
    return kept


def publish_rows(report_date, rows, history=None):
    """Rewrite ROI TODAY and replace ``history`` dates (default: report_date) in RIWAYAT."""
    creds = sheets_credentials()
    sheets = build('sheets', 'v4', credentials=creds, cache_discovery=False)
    spreadsheet_id = _sheet_file()
    created = False
    if not spreadsheet_id:
        created_body = sheets.spreadsheets().create(body={
            'properties': {
                'title': SPREADSHEET_TITLE,
                'locale': 'in_ID',
                'timeZone': 'Asia/Jakarta',
            },
            'sheets': [{
                'properties': {
                    'title': SHEET_TITLE,
                    'gridProperties': {'frozenRowCount': 1},
                }
            }],
        }).execute()
        spreadsheet_id = created_body['spreadsheetId']
        created = True
    url = 'https://docs.google.com/spreadsheets/d/' + spreadsheet_id

    today_sheet = _ensure_tab(sheets, spreadsheet_id, SHEET_TITLE)
    _write_tab(sheets, spreadsheet_id, SHEET_TITLE, [_row_values(item) for item in rows])
    _apply_layout(sheets, spreadsheet_id, today_sheet)

    history = dict(history or {})
    history[str(report_date)] = rows
    history_sheet = _ensure_tab(sheets, spreadsheet_id, HISTORY_TITLE)
    history_body = _history_rows(sheets, spreadsheet_id, history)
    _write_tab(sheets, spreadsheet_id, HISTORY_TITLE, history_body)
    _apply_layout(sheets, spreadsheet_id, history_sheet)

    _save_sheet_file(spreadsheet_id, url)
    return {
        'url': url,
        'created': created,
        'rows': len(rows),
        'history_rows': len(history_body),
        'history_dates': len({row[2] for row in history_body}),
    }


def already_published(cycle_key):
    return str(_read_json(STATE_PATH).get('cycle_key') or '') == str(cycle_key)


def mark_published(cycle_key, url, row_count):
    _write_json(STATE_PATH, {
        'cycle_key': cycle_key,
        'url': url,
        'rows': row_count,
        'updated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
    })

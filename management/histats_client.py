"""Live Histats client — no database persistence.

Histats does not publish a documented public REST API. This client talks to the
same JSON endpoints the official Histats dashboard uses (viewstats + HST_GET_*),
optionally after signing in with HISTATS_EMAIL / HISTATS_PASSWORD.
"""

from __future__ import annotations

import json
import os
import re
import base64
import threading
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import unquote

try:
    from requests.utils import dict_from_cookiejar
except Exception:
    dict_from_cookiejar = None

try:
    import requests
except Exception:
    requests = None

BASE = 'https://www.histats.com'
UA = (
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) '
    'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36'
)

_lock = threading.Lock()
_token_cache: Dict[str, Tuple[str, float, Dict[str, Any]]] = {}
_snapshot_cache: Dict[str, Dict[str, Any]] = {}
_live_cache: Dict[str, Dict[str, Any]] = {}
_TOKEN_TTL = 8 * 60
_SNAPSHOT_TTL = 12
_LIVE_TTL = 4


class HistatsAuthError(RuntimeError):
    def __init__(self, message: str, need_login: bool = False):
        super().__init__(message)
        self.need_login = need_login


def _env(name: str, default: str = '') -> str:
    return str(os.getenv(name) or default).strip()


def has_account() -> bool:
    return bool(_env('HISTATS_EMAIL') or _env('HISTATS_USER') or _env('HISTATS_MAIL'))


def configured_sites() -> List[Dict[str, str]]:
    """Parse HISTATS_SITES=sid:domain,sid:domain (stats themselves are never stored)."""
    raw = _env('HISTATS_SITES')
    if not raw:
        path = _env('HISTATS_SITES_FILE')
        if path and os.path.isfile(path):
            try:
                with open(path, 'r', encoding='utf-8') as fh:
                    data = json.load(fh)
                if isinstance(data, list):
                    out = []
                    for row in data:
                        sid = str((row or {}).get('sid') or '').strip()
                        domain = str((row or {}).get('domain') or sid).strip()
                        if sid:
                            out.append({'sid': sid, 'domain': domain or sid})
                    return out
            except Exception:
                pass
        return []
    out = []
    if raw.startswith('['):
        try:
            data = json.loads(raw)
            for row in data or []:
                sid = str((row or {}).get('sid') or '').strip()
                domain = str((row or {}).get('domain') or sid).strip()
                if sid:
                    out.append({'sid': sid, 'domain': domain or sid})
            return out
        except Exception:
            pass
    for part in raw.split(','):
        part = part.strip()
        if not part:
            continue
        if ':' in part:
            sid, domain = part.split(':', 1)
        else:
            sid, domain = part, part
        sid = sid.strip()
        domain = domain.strip() or sid
        if sid.isdigit() or sid:
            out.append({'sid': sid, 'domain': domain})
    return out


def _new_session(cookies: Optional[Any] = None) -> 'requests.Session':
    if requests is None:
        raise RuntimeError('requests is not installed')
    sess = requests.Session()
    sess.headers.update({
        'User-Agent': UA,
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'id-ID,id;q=0.9,en-US;q=0.8,en;q=0.7',
        'Accept-Encoding': 'gzip, deflate',
    })
    restore_cookies(sess, cookies)
    return sess


def cookies_dump(sess: 'requests.Session') -> List[Dict[str, str]]:
    rows = []
    for cookie in sess.cookies:
        name = str(getattr(cookie, 'name', '') or '')
        value = str(getattr(cookie, 'value', '') or '')
        if not name or not value:
            continue
        rows.append({
            'name': name,
            'value': value,
            'domain': str(getattr(cookie, 'domain', '') or 'www.histats.com'),
            'path': str(getattr(cookie, 'path', '') or '/'),
        })
    return rows


def cookies_dict(sess: 'requests.Session') -> Dict[str, str]:
    dumped = cookies_dump(sess)
    return {row['name']: row['value'] for row in dumped}


def restore_cookies(sess: 'requests.Session', cookies: Optional[Any]) -> None:
    if not cookies:
        return
    rows: List[Dict[str, str]]
    if isinstance(cookies, dict):
        rows = [{'name': str(k), 'value': str(v), 'domain': 'www.histats.com', 'path': '/'} for k, v in cookies.items()]
    else:
        rows = [row for row in cookies if isinstance(row, dict)]
    for row in rows:
        name = str(row.get('name') or '').strip()
        value = str(row.get('value') or '')
        if not name or not value:
            continue
        domain = str(row.get('domain') or 'www.histats.com').strip() or 'www.histats.com'
        path = str(row.get('path') or '/').strip() or '/'
        for host in dict.fromkeys([domain, 'www.histats.com', '.histats.com', 'histats.com', 'static.histats.com']):
            try:
                sess.cookies.set(name, value, domain=host, path=path)
            except Exception:
                continue


def env_credentials() -> Tuple[str, str]:
    email = _env('HISTATS_EMAIL') or _env('HISTATS_USER') or _env('HISTATS_MAIL')
    password = _env('HISTATS_PASSWORD') or _env('HISTATS_PASS')
    return email, password


def _page_logged_in(html: str) -> bool:
    text = html or ''
    if re.search(r'OBJ_summary\.user_logged\s*=\s*1', text):
        return True
    low = text.lower()
    if 'you are not logged in' in low:
        return False
    if re.search(r'>\s*log\s*-?out\s*<', low) or 'dologout' in low:
        return True
    return False


def login(sess: 'requests.Session', user: str, password: str) -> bool:
    user = str(user or '').strip()
    password = str(password or '')
    if not user or not password:
        return False
    try:
        sess.get(f'{BASE}/', timeout=20)
        resp = sess.post(
            f'{BASE}/viewstats/?DOLOGIN=1',
            data={'user': user, 'pass': password, 'B1': 'Login'},
            headers={
                'Origin': BASE,
                'Referer': f'{BASE}/',
                'Content-Type': 'application/x-www-form-urlencoded',
            },
            timeout=20,
            allow_redirects=True,
        )
        if _page_logged_in(resp.text or ''):
            return True
        probe = sess.get(f'{BASE}/?act=21', timeout=15, allow_redirects=True)
        if _page_logged_in(probe.text or ''):
            return True
        static_probe = sess.get(f'https://static.histats.com/?act=21', timeout=15, allow_redirects=True)
        return _page_logged_in(static_probe.text or '')
    except Exception:
        return False


def discover_sites(cookies: Optional[Dict[str, str]] = None) -> List[Dict[str, str]]:
    sites = configured_sites()
    by_sid = {s['sid']: s for s in sites}
    sess = _new_session(cookies)
    email, password = env_credentials()
    if email and password and not cookies:
        login(sess, email, password)
    html = ''
    for url in (f'{BASE}/?act=21', f'{BASE}/viewstats/?act=21', f'{BASE}/'):
        try:
            resp = sess.get(url, timeout=20)
            html += '\n' + (resp.text or '')
        except Exception:
            continue
    for sid, domain in re.findall(
        r'sid=(\d+)[^>]*>\s*([a-z0-9][a-z0-9\.\-:]{2,})',
        html,
        flags=re.I,
    ):
        sid = str(sid).strip()
        domain = str(domain).strip().rstrip('/').lower()
        if sid and sid not in by_sid:
            by_sid[sid] = {'sid': sid, 'domain': domain}
    for sid in re.findall(r'viewstats/\?sid=(\d+)', html, flags=re.I):
        sid = str(sid).strip()
        if sid and sid not in by_sid:
            by_sid[sid] = {'sid': sid, 'domain': sid}
    return list(by_sid.values())


def _extract_token(html: str) -> str:
    if not html:
        return ''
    patterns = [
        r"OBJ_summary\.sockTOKEN\s*=\s*'([^']+)'",
        r'OBJ_summary\.sockTOKEN\s*=\s*"([^"]+)"',
        r"sockTOKEN\s*=\s*'([^']+)'",
        r'sockTOKEN\s*=\s*"([^"]+)"',
        r"CC\s*=\s*'([^']{16,})'",
    ]
    for pat in patterns:
        m = re.search(pat, html)
        if m:
            return m.group(1).strip()
    return ''


def _extract_meta(html: str) -> Dict[str, Any]:
    meta: Dict[str, Any] = {}
    if not html:
        return meta
    m = re.search(r'<title>([^<]+)</title>', html, flags=re.I)
    if m:
        title = re.sub(r'\s+', ' ', m.group(1)).strip()
        title = re.sub(r'^Histats\s*-\s*', '', title, flags=re.I)
        meta['title'] = title
    m = re.search(r'(https?://)?([a-z0-9][a-z0-9\.\-]{2,}\.[a-z]{2,})', html, flags=re.I)
    if m:
        meta['domain'] = m.group(2).lower()
    for key, pat in (
        ('created_at', r'(Tanggal Pembuatan|Created)[^<]{0,40}([0-9]{1,2}[:.][0-9]{2}[:.][0-9]{2}\s+\d{1,2}\s+\w+\s+\d{4})'),
        ('timezone', r'(Time zone|Zona waktu)[^<]{0,40}([0-9:\s,A-Za-z/_\-]+)'),
        ('category', r'(Kategori|Category)[^<]{0,40}([A-Za-z ]+)'),
    ):
        m = re.search(pat, html, flags=re.I)
        if m:
            meta[key] = re.sub(r'\s+', ' ', m.group(2)).strip()
    return meta


def _js_json_loads(blob: str) -> Any:
    raw = str(blob or '').strip()
    if not raw:
        return None
    for candidate in (
        raw.encode('utf-8').decode('unicode_escape'),
        raw.replace('\\"', '"').replace('\\\\', '\\'),
        raw,
    ):
        try:
            return json.loads(candidate)
        except Exception:
            continue
    return None


def _parse_js_json_calls(html: str, fn_name: str) -> List[Any]:
    out: List[Any] = []
    if not html:
        return out
    pattern = re.compile(re.escape(fn_name) + r'\("((?:\\.|[^"\\])*)"\)')
    for match in pattern.finditer(html):
        obj = _js_json_loads(match.group(1))
        if obj is not None:
            out.append(obj)
    return out


def _move_to_day_begin(ts: int) -> int:
    ts = _as_int(ts)
    if not ts:
        return 0
    try:
        dt = datetime.fromtimestamp(ts, timezone.utc).replace(hour=0, minute=0, second=1, microsecond=0)
        return int(dt.timestamp())
    except Exception:
        return ts


def _format_unix(ts: int, tz_name: str = '') -> str:
    if not ts:
        return ''
    try:
        dt = datetime.fromtimestamp(int(ts))
        if tz_name:
            try:
                from zoneinfo import ZoneInfo
                dt = datetime.fromtimestamp(int(ts), ZoneInfo(tz_name))
            except Exception:
                pass
        return dt.strftime('%H:%M:%S %d %B %Y')
    except Exception:
        return ''


def _day_labels(start_ts: int, count: int) -> List[str]:
    labels: List[str] = []
    if not start_ts or count <= 0:
        return [str(i + 1) for i in range(max(count, 0))]
    try:
        start = datetime.utcfromtimestamp(int(start_ts))
    except Exception:
        return [str(i + 1) for i in range(count)]
    for i in range(count):
        day = start + timedelta(days=i)
        labels.append(day.strftime('%d %b'))
    return labels


def _day_axis(start_ts: int, count: int) -> Dict[str, List[Any]]:
    labels: List[str] = []
    ticks: List[str] = []
    weeks: List[str] = []
    sundays: List[bool] = []
    if not start_ts or count <= 0:
        n = max(count, 0)
        return {
            'labels': [str(i + 1) for i in range(n)],
            'ticks': [str(i + 1) for i in range(n)],
            'weeks': [''] * n,
            'sundays': [False] * n,
        }
    try:
        start = datetime.utcfromtimestamp(int(start_ts))
    except Exception:
        n = max(count, 0)
        return {
            'labels': [str(i + 1) for i in range(n)],
            'ticks': [str(i + 1) for i in range(n)],
            'weeks': [''] * n,
            'sundays': [False] * n,
        }
    for i in range(count):
        day = start + timedelta(days=i)
        labels.append(day.strftime('%d %b'))
        ticks.append(str(day.day))
        is_sun = day.weekday() == 6
        sundays.append(is_sun)
        weeks.append(f"{day.strftime('%a').lower()} {day.day} {day.strftime('%B')}" if is_sun else '')
    return {'labels': labels, 'ticks': ticks, 'weeks': weeks, 'sundays': sundays}


def _parse_viewstats_html(html: str) -> Dict[str, Any]:
    parsed: Dict[str, Any] = {
        'hourly': {},
        'daily': {},
        'daily_labels': [],
        'hourly_compare': {},
        'compare': {},
        'domain': '',
        'title': '',
        'category': '',
        'created_at': '',
        'timezone': '',
        'last_hit': '',
        'site_time': 0,
        'gmt_time': 0,
        'daily_start_ts': 0,
        't_medio': 0,
    }
    if not html:
        return parsed
    tz = ''
    m = re.search(r"OBJ_SITE\.timezone_str\s*=\s*'([^']+)'", html)
    if m:
        tz = m.group(1).strip()
        parsed['timezone'] = tz
    m = re.search(r'OBJ_SITE\.lht\s*=\s*(\d+)', html)
    if m:
        parsed['last_hit'] = _format_unix(_as_int(m.group(1)), tz)
    m = re.search(r'OBJ_SITE\.site_time\s*=\s*(\d+)', html)
    if m:
        parsed['site_time'] = _as_int(m.group(1))
    m = re.search(r'OBJ_SITE\.gmt_time\s*=\s*(\d+)', html)
    if m:
        parsed['gmt_time'] = _as_int(m.group(1))
    for info in _parse_js_json_calls(html, 'json_parse'):
        if not isinstance(info, dict):
            continue
        keys = {str(k) for k in info.keys()}
        if keys and keys <= {'1', '7', '14'}:
            compare = {}
            for key, val in info.items():
                ts = _as_int(val[0] if isinstance(val, list) and val else val)
                if ts:
                    compare[_as_int(key)] = ts
            if compare:
                parsed['compare'] = compare
            continue
        if not any(k in info for k in ('url', 'total_hits', 'dateiscr', 'PRTcat', 'title')):
            continue
        parsed['domain'] = str(info.get('url') or parsed['domain'] or '').strip()
        parsed['title'] = str(info.get('title') or parsed['title'] or '').strip()
        parsed['category'] = str(info.get('PRTcat') or parsed['category'] or '').strip()
        parsed['created_at'] = _format_unix(_as_int(info.get('dateiscr')), tz) or parsed['created_at']

    best_hourly = None
    best_hourly_ts = -1
    best_hourly_sum = -1
    best_daily = None
    best_daily_n = -1
    best_daily_ts = 0
    hourly_compare: Dict[int, Dict[str, Any]] = {}
    hourly_additionals: Dict[int, Dict[str, Any]] = {}
    horizon = parsed.get('gmt_time') or int(time.time())
    hourly_candidates = []
    for obj in _parse_js_json_calls(html, '_init_from_JSON'):
        if not isinstance(obj, dict):
            continue
        segs = obj.get('AR_segments') if isinstance(obj.get('AR_segments'), dict) else {}
        hits = segs.get('h') or []
        vis = segs.get('v') or []
        rng = str(obj.get('range') or '').strip().lower()
        ts = _as_int(obj.get('UNIXTIME'))
        total = sum(_as_int(x) for x in hits) + sum(_as_int(x) for x in vis)
        if rng == 'h' and len(hits) >= 24:
            hourly_compare[ts] = segs
            add = obj.get('AR_additionals') if isinstance(obj.get('AR_additionals'), dict) else {}
            if add:
                hourly_additionals[ts] = add
            hourly_candidates.append((ts, total, segs))
        elif rng == 'd' and len(hits) >= 14:
            if len(hits) > best_daily_n or (len(hits) == best_daily_n and total > 0):
                best_daily_n = len(hits)
                best_daily_ts = ts
                best_daily = segs
    parsed['hourly_compare'] = hourly_compare
    today_key = _move_to_day_begin(parsed.get('site_time') or 0)
    chosen_ts = 0
    if today_key and today_key in hourly_compare:
        best_hourly = hourly_compare[today_key]
        chosen_ts = today_key
    else:
        usable = [row for row in hourly_candidates if row[0] <= horizon + 3600]
        if not usable:
            usable = hourly_candidates
        for ts, total, segs in usable:
            if ts > best_hourly_ts or (ts == best_hourly_ts and total >= best_hourly_sum):
                best_hourly_ts = ts
                best_hourly_sum = total
                best_hourly = segs
                chosen_ts = ts
    if best_hourly:
        parsed['hourly'] = best_hourly
        add = hourly_additionals.get(chosen_ts) or {}
        parsed['t_medio'] = _as_int(add.get('t_medio'))
    if best_daily:
        parsed['daily'] = best_daily
        parsed['daily_start_ts'] = best_daily_ts
        axis = _day_axis(best_daily_ts, len(best_daily.get('h') or []))
        parsed['daily_labels'] = axis['labels']
        parsed['daily_ticks'] = axis['ticks']
        parsed['daily_weeks'] = axis['weeks']
        parsed['daily_sundays'] = axis['sundays']
    return parsed


def _is_protected_response(resp) -> bool:
    url = str(getattr(resp, 'url', '') or '')
    text = str(getattr(resp, 'text', '') or '')
    loc = ''
    try:
        loc = str((resp.headers or {}).get('Location') or '')
    except Exception:
        loc = ''
    blob = f'{url}\n{loc}\n{text[:2000]}'
    return 'redir_protected_stats' in blob


def _encode_cc(token: str) -> str:
    return base64.b64encode(str(token or '').encode('utf-8')).decode('ascii')


def _merge_viewstats_meta(html: str) -> Dict[str, Any]:
    meta = _extract_meta(html)
    parsed = _parse_viewstats_html(html)
    for key in ('domain', 'title', 'category', 'created_at', 'timezone'):
        if parsed.get(key) and not meta.get(key):
            meta[key] = parsed[key]
        elif parsed.get(key) and key in ('domain', 'timezone', 'created_at', 'category', 'title'):
            meta[key] = parsed[key]
    meta['charts'] = parsed
    if parsed.get('last_hit'):
        meta['last_hit'] = parsed['last_hit']
    if parsed.get('t_medio'):
        meta['t_medio'] = parsed['t_medio']
    return meta


def _get_token(sess: 'requests.Session', sid: str) -> Tuple[str, Dict[str, Any], str]:
    now = time.time()
    cache_key = sid + ':' + str(sorted(cookies_dict(sess).items()))
    cached = _token_cache.get(cache_key)
    if cached and cached[0] and (now - cached[1]) < _TOKEN_TTL:
        meta = cached[2] if len(cached) > 2 else {}
        charts = (meta or {}).get('charts') or {}
        hourly = charts.get('hourly') if isinstance(charts, dict) else {}
        daily = charts.get('daily') if isinstance(charts, dict) else {}
        if (isinstance(hourly, dict) and (hourly.get('h') or hourly.get('v'))) or (
            isinstance(daily, dict) and daily.get('h')
        ):
            return cached[0], meta, 'cached'
    html = ''
    last_url = ''
    best_token = ''
    best_meta: Dict[str, Any] = {}
    best_url = ''
    for url in (
        f'{BASE}/viewstats/?sid={sid}&act=2&f=1',
        f'https://static.histats.com/viewstats/?sid={sid}&act=2&f=1',
        f'{BASE}/viewstats/?sid={sid}&act=2',
    ):
        resp = sess.get(url, timeout=20, allow_redirects=True)
        html = resp.text or ''
        last_url = str(resp.url or url)
        if _is_protected_response(resp) and not _extract_token(html):
            continue
        token = _extract_token(html)
        meta = _merge_viewstats_meta(html)
        charts = (meta or {}).get('charts') or {}
        has_charts = bool(
            ((charts.get('hourly') or {}).get('h') if isinstance(charts.get('hourly'), dict) else None)
            or ((charts.get('daily') or {}).get('h') if isinstance(charts.get('daily'), dict) else None)
        )
        if token and has_charts:
            _token_cache[cache_key] = (token, now, meta)
            return token, meta, last_url
        if token and not best_token:
            best_token, best_meta, best_url = token, meta, last_url
        elif has_charts:
            best_meta, best_url = meta, last_url
    if best_token:
        _token_cache[cache_key] = (best_token, now, best_meta)
        return best_token, best_meta, best_url
    meta = best_meta or _merge_viewstats_meta(html)
    if 'redir_protected_stats' in last_url or 'redir_protected_stats' in html:
        raise HistatsAuthError(
            'SID Histats ini diproteksi. Login dulu dengan akun Histats pemilik website.',
            need_login=True,
        )
    return '', meta, last_url


def _post_histats(
    sess: 'requests.Session',
    endpoint: str,
    sid: str,
    token: str,
    extra: Optional[Dict[str, Any]] = None,
    referer: str = '',
) -> Any:
    url = f'{BASE}/viewstats/{endpoint}'
    headers = {
        'X-Requested-With': 'XMLHttpRequest',
        'Referer': referer or f'{BASE}/viewstats/?sid={sid}&act=2&f=1',
        'Origin': BASE,
        'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
        'Accept': 'application/json, text/javascript, */*; q=0.01',
        'Accept-Encoding': 'gzip, deflate',
    }
    encoded_cc = _encode_cc(token)
    extra_clean = {
        str(key): str(val)
        for key, val in (extra or {}).items()
        if val is not None and str(val) != ''
    }
    bodies = [
        {'AR_REQ[sid]': sid, 'AR_REQ[CC]': encoded_cc, 'AR_REQ[thcl]': '0', 'dbg': '1', **extra_clean},
        {'AR_REQ[sid]': sid, 'AR_REQ[CC]': token, 'AR_REQ[thcl]': '0', 'dbg': '1', **extra_clean},
    ]
    for data in bodies:
        resp = sess.post(url, data=data, headers=headers, timeout=20)
        text = (resp.text or '').strip()
        if not text or text in ('error=11', 'err:1', 'error'):
            continue
        try:
            parsed = json.loads(text)
        except Exception:
            parsed = {'raw': text}
        if parsed:
            return parsed
    return None


def _parse_widget_blob(text: str) -> Dict[str, int]:
    out: Dict[str, int] = {}
    for match in re.finditer(r'(\d+)([^=#]+)=([^#]+)', text or ''):
        label = re.sub(r'\s+', ' ', match.group(2)).strip().lower()
        raw_val = re.match(r'[+-]?\d+(?:[.,]\d+)?', str(match.group(3) or '').strip())
        value = _as_int(raw_val.group(0) if raw_val else 0)
        if label in ('vis. today', 'vis today', 'visitors today', 'today visitors'):
            out['today_visitors'] = value
        elif label in ('pag. today', 'pag today', 'pages today', 'today pages'):
            out['today_pageviews'] = value
        elif label in ('pages', 'pageviews', 'hits', 'page views'):
            out['total_pageviews'] = value
        elif label in ('visits', 'visitors', 'unique'):
            out['total_visitors'] = value
        elif label in ('online', 'users online', 'user online'):
            out['online'] = value
        elif 'first' in label:
            out['first_time'] = value
    return out


def _fetch_widget_summary(sid: str) -> Dict[str, Any]:
    if requests is None:
        return {}
    sess = _new_session()
    merged: Dict[str, int] = {}
    urls = [
        f'https://s4.histats.com/stats/0.gif?{sid}&1',
        f'https://s4.histats.com/stats/0.php?{sid}&1',
        f'https://s10.histats.com/stats/0.gif?{sid}&1',
        f'https://s10.histats.com/stats/0.php?{sid}&1',
    ]
    for url in urls:
        try:
            resp = sess.get(
                url,
                timeout=12,
                headers={'Referer': 'https://www.histats.com/', 'Accept': '*/*'},
            )
            parsed = _parse_widget_blob(resp.text or '')
            for key, value in parsed.items():
                if value and key not in merged:
                    merged[key] = value
        except Exception:
            continue
    if not merged:
        return {}
    return {
        'livearray': {
            'livesummary': {
                'tot_h': merged.get('total_pageviews', 0),
                'tot_v': merged.get('total_visitors', 0),
                'tod_h': merged.get('today_pageviews', 0),
                'tod_v': merged.get('today_visitors', 0),
                'tod_nv': merged.get('first_time', 0),
                'cur_online': merged.get('online', 0),
            }
        }
    }


def _as_int(v: Any) -> int:
    try:
        if v is None or v == '':
            return 0
        if isinstance(v, str):
            v = v.replace(',', '').replace('.', '') if v.count('.') > 1 else v.replace(',', '')
        return int(float(v))
    except Exception:
        return 0


def _as_float(v: Any) -> float:
    try:
        if v is None or v == '':
            return 0.0
        if isinstance(v, str):
            v = v.replace(',', '')
        return float(v)
    except Exception:
        return 0.0


def _first(obj: Dict[str, Any], *keys: str, default: Any = None) -> Any:
    if not isinstance(obj, dict):
        return default
    lower = {str(k).lower(): v for k, v in obj.items()}
    for key in keys:
        if key in obj and obj[key] not in (None, ''):
            return obj[key]
        lk = key.lower()
        if lk in lower and lower[lk] not in (None, ''):
            return lower[lk]
    return default


def _series_from(value: Any, length: Optional[int] = None) -> List[int]:
    if value is None:
        return [0] * (length or 0)
    if isinstance(value, str):
        parts = re.split(r'[,\|;]+', value.strip())
        nums = [_as_int(p) for p in parts if p != '']
    elif isinstance(value, dict):
        try:
            items = sorted(value.items(), key=lambda kv: int(str(kv[0])))
        except Exception:
            items = list(value.items())
        nums = [_as_int(v) for _, v in items]
    elif isinstance(value, (list, tuple)):
        if value and isinstance(value[0], dict):
            nums = [
                _as_int(_first(row, 'pv', 'pageviews', 'page_views', 'hits', 'y', 'v', default=0))
                for row in value
            ]
        elif value and isinstance(value[0], (list, tuple)):
            nums = [_as_int(row[0] if row else 0) for row in value]
        else:
            nums = [_as_int(v) for v in value]
    else:
        nums = []
    if length:
        if len(nums) < length:
            nums = nums + [0] * (length - len(nums))
        return nums[:length]
    return nums


def _float_series_from(value: Any, length: Optional[int] = 24) -> List[float]:
    if value is None:
        return [0.0] * (length or 0)
    nums: List[float] = []
    if isinstance(value, (list, tuple)) and value and not isinstance(value[0], (dict, list, tuple)):
        nums = [_as_float(v) for v in value]
    else:
        nums = [_as_float(v) for v in _series_from(value, None)]
    size = length or 0
    if size:
        if len(nums) < size:
            nums = nums + [0.0] * (size - len(nums))
        return nums[:size]
    return nums


def _ratio_series(num: List[Any], den: List[Any], length: int = 24, as_pct: bool = False) -> List[float]:
    out: List[float] = []
    for i in range(length):
        n = _as_float(num[i] if i < len(num) else 0)
        d = _as_float(den[i] if i < len(den) else 0)
        val = (n / d) if d else 0.0
        out.append(round(val * 100.0 if as_pct else val, 2))
    return out


def _derive_online_avg(vis: List[int], length: int, rng: str = 'h') -> List[int]:
    """Histats computes onlineAVG in the browser, not in the JSON payload.

    hourly: visitors / 6
    daily:  visitors / (6 * 24)
    monthly: visitors / (6 * 24 * 30)
    """
    kind = str(rng or 'h').lower()
    if kind == 'd':
        divisor = 6 * 24
    elif kind == 'm':
        divisor = 6 * 24 * 30
    else:
        divisor = 6
    out: List[int] = []
    for i in range(length):
        visitors = _as_int(vis[i] if i < len(vis) else 0)
        out.append(int(visitors / divisor) if visitors > 0 else 0)
    return out


def _pack_hour_metrics(segs: Dict[str, Any], length: int = 24, rng: str = '') -> Dict[str, Any]:
    segs = segs if isinstance(segs, dict) else {}
    n = max(int(length or 0), 1)
    hits = _series_from(segs.get('h'), n)
    vis = _series_from(segs.get('v'), n)
    first = _series_from(segs.get('nv'), n)
    stacked_pv, stacked_vis, stacked_first = _stack_histats_bars(hits, vis, first)
    bounce = _float_series_from(segs.get('bbR') or segs.get('bbRP'), n)
    if _is_zero_series(bounce):
        bounce = _ratio_series(_series_from(segs.get('bb'), n), vis, n, True)
    elif bounce and max(bounce) <= 1.5:
        bounce = [round(x * 100.0, 2) for x in bounce]
    ppv = _float_series_from(segs.get('ppv'), n)
    if _is_zero_series(ppv):
        ppv = _ratio_series(hits, vis, n, False)
    nvis = _float_series_from(segs.get('nvR'), n)
    if _is_zero_series(nvis):
        nvis = _ratio_series(first, vis, n, True)
    elif nvis and max(nvis) <= 1.5:
        nvis = [round(x * 100.0, 2) for x in nvis]
    if not rng:
        rng = 'd' if n > 24 else 'h'
    online = _float_series_from(segs.get('onlineAVG'), n)
    if _is_zero_series(online):
        online = _derive_online_avg(vis, n, rng)
    return {
        'pageviews': stacked_pv,
        'visitors': stacked_vis,
        'first_time': stacked_first,
        'bounce': bounce,
        'ppv': [round(_as_float(x), 2) for x in ppv],
        'nvis': nvis,
        'online': [_as_int(x) for x in online],
        'raw_visitors': vis,
        'raw_first': first,
        'raw_pageviews': hits,
    }


def _pick_summary_blob(payload: Any) -> Dict[str, Any]:
    if not isinstance(payload, dict):
        return {}
    for path in (
        ('livearray', 'livesummary'),
        ('livearray', 'summary'),
        ('livesummary',),
        ('summary',),
        ('data',),
    ):
        cur: Any = payload
        ok = True
        for key in path:
            if not isinstance(cur, dict) or key not in cur:
                ok = False
                break
            cur = cur[key]
        if ok and isinstance(cur, dict):
            return cur
    if any(k in payload for k in ('cur_online', 'today_pageviews', 'pageviews')):
        return payload
    return {}


def _is_zero_series(values: Any) -> bool:
    if not values:
        return True
    return all(_as_int(x) == 0 for x in values)


def _chart_has_data(block: Any) -> bool:
    if not isinstance(block, dict):
        return False
    for key in ('pageviews', 'visitors', 'first_time'):
        if not _is_zero_series(block.get(key)):
            return True
    return False


def _stack_histats_bars(
    hits: List[int],
    vis: List[int],
    first: List[int],
) -> Tuple[List[int], List[int], List[int]]:
    """Match Histats stacked columns: first-time + returning + extra pageviews = hits."""
    n = max(len(hits or []), len(vis or []), len(first or []), 0)
    if n <= 0:
        return [], [], []

    def pad(xs: List[int]) -> List[int]:
        xs = [_as_int(v) for v in (xs or [])]
        if len(xs) < n:
            xs = xs + [0] * (n - len(xs))
        return xs[:n]

    hits_n, vis_n, first_n = pad(hits), pad(vis), pad(first)
    extra, returning, first_out = [], [], []
    for h, v, nv in zip(hits_n, vis_n, first_n):
        nv = min(nv, v) if v else nv
        first_out.append(nv)
        returning.append(max(v - nv, 0))
        extra.append(max(h - v, 0))
    return extra, returning, first_out


def _mean_series(rows: List[List[int]], length: int = 24) -> List[int]:
    out = [0] * length
    if not rows:
        return out
    for idx in range(length):
        vals = []
        for row in rows:
            if idx < len(row):
                vals.append(_as_int(row[idx]))
        if vals:
            out[idx] = int(round(sum(vals) / len(vals)))
    return out


def _hourly_forecast_extras(
    hours_pv: List[int],
    hours_vis: List[int],
    hours_first: List[int],
    hour_idx: int,
    charts: Dict[str, Any],
) -> Tuple[List[int], List[int], List[int]]:
    extra_h = [0] * 24
    extra_v = [0] * 24
    extra_nv = [0] * 24
    compare = (charts or {}).get('compare') if isinstance(charts, dict) else {}
    hourly_cmp = (charts or {}).get('hourly_compare') if isinstance(charts, dict) else {}
    if not isinstance(hourly_cmp, dict):
        return extra_h, extra_v, extra_nv
    refs_h, refs_v, refs_nv = [], [], []
    for key in (7, 1, 14):
        ts = _as_int((compare or {}).get(key) or (compare or {}).get(str(key)))
        segs = hourly_cmp.get(ts) if ts else None
        if not isinstance(segs, dict):
            continue
        if not (segs.get('h') or segs.get('v')):
            continue
        refs_h.append(_series_from(segs.get('h'), 24))
        refs_v.append(_series_from(segs.get('v'), 24))
        refs_nv.append(_series_from(segs.get('nv'), 24))
    if not refs_h:
        today_key = _move_to_day_begin((charts or {}).get('site_time') or 0)
        for ts, segs in hourly_cmp.items():
            if _as_int(ts) == today_key or not isinstance(segs, dict):
                continue
            if not (segs.get('h') or segs.get('v')):
                continue
            refs_h.append(_series_from(segs.get('h'), 24))
            refs_v.append(_series_from(segs.get('v'), 24))
            refs_nv.append(_series_from(segs.get('nv'), 24))
    if not refs_h:
        return extra_h, extra_v, extra_nv
    fc_h = _mean_series(refs_h)
    fc_v = _mean_series(refs_v)
    fc_nv = _mean_series(refs_nv)
    hour_idx = min(max(_as_int(hour_idx), 0), 23)
    for i in range(24):
        if i < hour_idx:
            continue
        if i == hour_idx:
            extra_h[i] = max(fc_h[i] - _as_int(hours_pv[i] if i < len(hours_pv) else 0), 0)
            extra_v[i] = max(fc_v[i] - _as_int(hours_vis[i] if i < len(hours_vis) else 0), 0)
            extra_nv[i] = max(fc_nv[i] - _as_int(hours_first[i] if i < len(hours_first) else 0), 0)
        else:
            extra_h[i] = fc_h[i]
            extra_v[i] = fc_v[i]
            extra_nv[i] = fc_nv[i]
    return extra_h, extra_v, extra_nv


def _pct_delta(current: int, reference: int) -> Optional[float]:
    if not reference:
        return None
    return round((float(current) / float(reference) - 1.0) * 100.0, 1)


def _trim_daily_series(
    days_pv: List[int],
    days_vis: List[int],
    days_first: List[int],
    labels: List[str],
    start_ts: int = 0,
) -> Tuple[List[int], List[int], List[int], List[str]]:
    n = len(days_pv or [])
    if n <= 0:
        return days_pv, days_vis, days_first, labels
    last = -1
    for i in range(n):
        vis = days_vis[i] if i < len(days_vis) else 0
        first = days_first[i] if i < len(days_first) else 0
        if _as_int(days_pv[i]) or _as_int(vis) or _as_int(first):
            last = i
    today_idx = None
    if start_ts:
        try:
            start = datetime.utcfromtimestamp(int(start_ts)).date()
            today_idx = (datetime.utcnow().date() - start).days
        except Exception:
            today_idx = None
    if today_idx is not None:
        last = max(last, min(max(today_idx, 0), n - 1))
    if last < 0:
        return days_pv, days_vis, days_first, labels
    end = last + 1
    return (
        days_pv[:end],
        (days_vis or [])[:end],
        (days_first or [])[:end],
        (labels or [])[:end],
    )


def _site_hour(meta: Dict[str, Any]) -> int:
    charts = (meta or {}).get('charts') if isinstance(meta, dict) else {}
    site_time = _as_int((charts or {}).get('site_time'))
    if site_time:
        try:
            return datetime.utcfromtimestamp(site_time).hour
        except Exception:
            pass
    tz_name = str((meta or {}).get('timezone') or '')
    if tz_name:
        try:
            from zoneinfo import ZoneInfo
            return datetime.now(ZoneInfo(tz_name)).hour
        except Exception:
            pass
    return datetime.now().hour


def _normalize_summary(sid: str, payload: Any, meta: Dict[str, Any]) -> Dict[str, Any]:
    blob = _pick_summary_blob(payload)
    online = _as_int(_first(blob, 'cur_online', 'online', 'users_online', 'usersonline', default=0))
    today_pv = _as_int(_first(blob, 'tod_h', 'today_pageviews', 'today_pv', 'pageviews_today', 'pv_today', 'todayhits', default=0))
    today_vis = _as_int(_first(blob, 'tod_v', 'today_visitors', 'today_unique', 'visitors_today', 'uv_today', 'today_visits', default=0))
    today_first = _as_int(_first(blob, 'tod_nv', 'today_first', 'first_time', 'firsttime', 'new_visitors', 'today_new', default=0))
    total_pv = _as_int(_first(blob, 'tot_h', 'total_pageviews', 'total_pv', 'pageviews', 'hits', default=0))
    total_vis = _as_int(_first(blob, 'tot_v', 'total_visitors', 'total_unique', 'visitors', 'uniques', default=0))
    avg_time = _as_int(_first(blob, 't_medio', 'avg_time', 'time_avg', 'avg_visit', 'average_time', default=0))
    ppv = _as_float(_first(blob, 'ppv', 'pages_per_visit', 'pageviews_per_visit', default=0))
    if ppv <= 0 and today_vis > 0:
        ppv = round(today_pv / today_vis, 2)
    pages_per_visit_all = _as_float(_first(blob, 'total_ppv', default=0))
    if pages_per_visit_all <= 0 and total_vis > 0:
        pages_per_visit_all = round(total_pv / max(total_vis, 1), 2)

    hours_pv = _series_from(_first(blob, 'hours', 'hours_pv', 'hour_pv', 'today_hours'), 24)
    hours_vis = _series_from(_first(blob, 'hours_vis', 'hours_uv', 'hourly_visitors', 'hour_uv'), 24)
    hours_first = _series_from(_first(blob, 'hours_first', 'hourly_first', 'hour_first'), 24)
    if hours_vis == [0] * 24 and isinstance(_first(blob, 'hours'), list):
        raw_hours = _first(blob, 'hours')
        if raw_hours and isinstance(raw_hours[0], dict):
            hours_pv = _series_from([_first(r, 'pv', 'pageviews', default=0) for r in raw_hours], 24)
            hours_vis = _series_from([_first(r, 'visitors', 'uv', 'unique', default=0) for r in raw_hours], 24)
            hours_first = _series_from([_first(r, 'first', 'new', default=0) for r in raw_hours], 24)

    charts = (meta or {}).get('charts') or {}
    chart_hourly = charts.get('hourly') if isinstance(charts, dict) else {}
    if isinstance(chart_hourly, dict) and (chart_hourly.get('h') or chart_hourly.get('v')):
        hours_pv = _series_from(chart_hourly.get('h'), 24)
        hours_vis = _series_from(chart_hourly.get('v'), 24)
        hours_first = _series_from(chart_hourly.get('nv'), 24)
    live_hourly = blob.get('hourly') if isinstance(blob.get('hourly'), dict) else {}
    for key, row in (live_hourly or {}).items():
        try:
            idx = int(str(key))
        except Exception:
            continue
        if idx < 0 or idx > 23:
            continue
        if isinstance(row, dict):
            if row.get('h') is not None:
                hours_pv[idx] = _as_int(row.get('h'))
            if row.get('v') is not None:
                hours_vis[idx] = _as_int(row.get('v'))
            if row.get('nv') is not None:
                hours_first[idx] = _as_int(row.get('nv'))
        else:
            hours_pv[idx] = _as_int(row)

    days_pv = _series_from(_first(blob, 'days', 'days_pv', 'daily', 'last60', 'month_pv'), None)
    days_vis = _series_from(_first(blob, 'days_vis', 'days_uv', 'daily_visitors'), None)
    days_first = _series_from(_first(blob, 'days_first', 'daily_first'), None)
    if days_pv and isinstance(_first(blob, 'days'), list) and _first(blob, 'days') and isinstance(_first(blob, 'days')[0], dict):
        raw_days = _first(blob, 'days')
        days_pv = [_as_int(_first(r, 'pv', 'pageviews', default=0)) for r in raw_days]
        days_vis = [_as_int(_first(r, 'visitors', 'uv', default=0)) for r in raw_days]
        days_first = [_as_int(_first(r, 'first', 'new', default=0)) for r in raw_days]
        day_labels = [
            str(_first(r, 'date', 'day', 'label', default='')) for r in raw_days
        ]
    else:
        day_labels = []

    chart_daily = charts.get('daily') if isinstance(charts, dict) else {}
    if isinstance(chart_daily, dict) and (chart_daily.get('h') or chart_daily.get('v')):
        days_pv = _series_from(chart_daily.get('h'), None)
        days_vis = _series_from(chart_daily.get('v'), None)
        days_first = _series_from(chart_daily.get('nv'), None)
        day_labels = list(charts.get('daily_labels') or [])

    day_ticks = list((charts.get('daily_ticks') or []) if isinstance(charts, dict) else [])
    day_weeks = list((charts.get('daily_weeks') or []) if isinstance(charts, dict) else [])
    day_sundays = list((charts.get('daily_sundays') or []) if isinstance(charts, dict) else [])
    daily_start_ts = _as_int(charts.get('daily_start_ts') if isinstance(charts, dict) else 0)
    orig_n = len(days_pv)
    start_i = max(orig_n - 62, 0) if orig_n > 62 else 0
    if start_i:
        days_pv = days_pv[start_i:]
        days_vis = (days_vis or [0] * orig_n)[start_i:]
        days_first = (days_first or [0] * orig_n)[start_i:]
        day_labels = (day_labels or [])[start_i:]
        day_ticks = day_ticks[start_i:]
        day_weeks = day_weeks[start_i:]
        day_sundays = day_sundays[start_i:]
        if daily_start_ts:
            daily_start_ts += start_i * 86400
    if days_vis and len(days_vis) < len(days_pv):
        days_vis = days_vis + [0] * (len(days_pv) - len(days_vis))
    if days_first and len(days_first) < len(days_pv):
        days_first = days_first + [0] * (len(days_pv) - len(days_first))
    if not days_vis:
        days_vis = [0] * len(days_pv)
    if not days_first:
        days_first = [0] * len(days_pv)
    if day_labels and len(day_labels) < len(days_pv):
        day_labels = day_labels + [str(i + 1) for i in range(len(day_labels), len(days_pv))]
    elif not day_labels:
        day_labels = [str(i + 1) for i in range(len(days_pv))]

    days_pv, days_vis, days_first, day_labels = _trim_daily_series(
        days_pv,
        days_vis,
        days_first,
        day_labels,
        daily_start_ts,
    )
    day_n = len(days_pv)
    day_ticks = day_ticks[:day_n]
    day_weeks = day_weeks[:day_n]
    day_sundays = day_sundays[:day_n]
    if daily_start_ts and (len(day_ticks) < day_n or not any(day_weeks)):
        axis = _day_axis(daily_start_ts, day_n)
        day_labels = axis['labels']
        day_ticks = axis['ticks']
        day_weeks = axis['weeks']
        day_sundays = axis['sundays']
    daily_src = chart_daily if isinstance(chart_daily, dict) else {}
    if start_i or day_n:
        clipped: Dict[str, Any] = {}
        end_i = start_i + day_n
        for key, val in daily_src.items():
            if isinstance(val, list):
                clipped[key] = val[start_i:end_i]
            else:
                clipped[key] = val
        daily_src = clipped
    daily_metrics = _pack_hour_metrics(daily_src, day_n or 1, 'd')
    if day_n:
        for key in ('bounce', 'ppv', 'nvis', 'online', 'raw_visitors', 'raw_first', 'raw_pageviews'):
            daily_metrics[key] = (daily_metrics.get(key) or [])[:day_n]

    period_pv = sum(_as_int(x) for x in days_pv)
    period_vis = sum(_as_int(x) for x in days_vis)
    period_first = sum(_as_int(x) for x in days_first)
    period_ppv = round(period_pv / period_vis, 2) if period_vis else 0.0
    period_share = round((period_first / period_vis) * 100.0, 2) if period_vis else 0.0

    forecast_hour = _as_int(_first(blob, 'forecast_hour', 'cur_hour_forecast', default=0))
    forecast_day = _as_int(_first(blob, 'forecast_day', 'cur_day_forecast', default=0))
    last_hit = str(_first(blob, 'last_hit', 'last_hits', 'last_time', default='') or '')
    if not last_hit:
        last_hit = str((meta or {}).get('last_hit') or '')
    if not avg_time:
        avg_time = _as_int((meta or {}).get('t_medio') or (charts.get('t_medio') if isinstance(charts, dict) else 0))

    hour_idx = min(max(_site_hour(meta or {}), 0), 23)
    extra_h, extra_v, extra_nv = _hourly_forecast_extras(
        hours_pv, hours_vis, hours_first, hour_idx, charts if isinstance(charts, dict) else {},
    )
    if not forecast_hour:
        forecast_hour = _as_int(hours_vis[hour_idx] if hour_idx < len(hours_vis) else 0) + extra_v[hour_idx]
    if not forecast_day:
        remaining_vis = extra_v[hour_idx] + sum(extra_v[hour_idx + 1:])
        if remaining_vis:
            forecast_day = today_vis + remaining_vis
        elif today_vis:
            forecast_day = int(round(today_vis * 24 / max(hour_idx + 1, 1)))

    chart_hours_pv, chart_hours_vis, chart_hours_first = _stack_histats_bars(
        hours_pv, hours_vis, hours_first
    )
    chart_fc_pv, chart_fc_vis, chart_fc_first = _stack_histats_bars(
        extra_h, extra_v, extra_nv
    )
    chart_days_pv, chart_days_vis, chart_days_first = _stack_histats_bars(
        days_pv, days_vis, days_first
    )
    today_metrics = _pack_hour_metrics(chart_hourly if isinstance(chart_hourly, dict) else {})
    compare_days: Dict[str, Any] = {}
    compare_map = charts.get('compare') if isinstance(charts, dict) else {}
    hourly_cmp = charts.get('hourly_compare') if isinstance(charts, dict) else {}
    if isinstance(hourly_cmp, dict):
        for key in (1, 7, 14):
            ts = _as_int((compare_map or {}).get(key) or (compare_map or {}).get(str(key)))
            segs = None
            if ts:
                segs = hourly_cmp.get(ts) or hourly_cmp.get(str(ts))
            if not isinstance(segs, dict):
                continue
            packed = _pack_hour_metrics(segs)
            packed['ts'] = ts
            packed['offset'] = key
            packed['label'] = _id_date_label(ts)
            packed['totals'] = {
                'pageviews': sum(_as_int(x) for x in (packed.get('raw_pageviews') or [])),
                'visitors': sum(_as_int(x) for x in (packed.get('raw_visitors') or [])),
                'first_time': sum(_as_int(x) for x in (packed.get('raw_first') or [])),
            }
            compare_days[str(key)] = packed
        if not compare_days:
            today_key = _move_to_day_begin(_as_int(charts.get('site_time')))
            others = []
            for ts, segs in hourly_cmp.items():
                if _as_int(ts) == today_key or not isinstance(segs, dict):
                    continue
                others.append((_as_int(ts), segs))
            others.sort(key=lambda row: row[0], reverse=True)
            for key, row in zip((1, 7, 14), others):
                packed = _pack_hour_metrics(row[1])
                packed['ts'] = row[0]
                packed['offset'] = key
                packed['label'] = _id_date_label(row[0])
                packed['totals'] = {
                    'pageviews': sum(_as_int(x) for x in (packed.get('raw_pageviews') or [])),
                    'visitors': sum(_as_int(x) for x in (packed.get('raw_visitors') or [])),
                    'first_time': sum(_as_int(x) for x in (packed.get('raw_first') or [])),
                }
                compare_days[str(key)] = packed

    forecast_vs = {'hour_7': None, 'hour_14': None, 'day_7': None, 'day_14': None}
    if isinstance(hourly_cmp, dict):
        for key in (7, 14):
            ts = _as_int((compare_map or {}).get(key) or (compare_map or {}).get(str(key)))
            segs = hourly_cmp.get(ts) or hourly_cmp.get(str(ts)) if ts else None
            if not isinstance(segs, dict):
                packed = compare_days.get(str(key)) or {}
                vis_ref = packed.get('raw_visitors') or []
            else:
                vis_ref = _series_from(segs.get('v'), 24)
            hour_ref = _as_int(vis_ref[hour_idx] if hour_idx < len(vis_ref) else 0)
            day_ref = sum(_as_int(x) for x in vis_ref)
            forecast_vs[f'hour_{key}'] = _pct_delta(forecast_hour, hour_ref)
            forecast_vs[f'day_{key}'] = _pct_delta(forecast_day, day_ref)

    total_first = _as_int(_first(blob, 'tot_nv', 'total_first', 'total_new', default=0))
    if not total_first:
        total_first = sum(_as_int(x) for x in (daily_metrics.get('raw_first') or days_first or []))
    first_share = round((total_first / total_vis) * 100.0, 2) if total_vis else 0.0

    out = {
        'sid': sid,
        'domain': meta.get('domain') or '',
        'title': meta.get('title') or '',
        'account': {
            'created_at': meta.get('created_at') or '',
            'timezone': meta.get('timezone') or '',
            'category': meta.get('category') or '',
        },
        'totals': {
            'pageviews': total_pv,
            'visitors': total_vis,
            'pages_per_visit': pages_per_visit_all,
            'first_time': total_first,
            'first_share': first_share,
        },
        'today': {
            'pageviews': today_pv,
            'visitors': today_vis,
            'first_time': today_first,
            'online': online,
            'avg_time_sec': avg_time,
            'pages_per_visit': ppv,
            'last_hit': last_hit,
        },
        'hourly': {
            'pageviews': chart_hours_pv,
            'visitors': chart_hours_vis,
            'first_time': chart_hours_first,
            'forecast_pageviews': chart_fc_pv,
            'forecast_visitors': chart_fc_vis,
            'forecast_first_time': chart_fc_first,
            'current_hour': hour_idx,
            'bounce': today_metrics.get('bounce') or [],
            'ppv': today_metrics.get('ppv') or [],
            'nvis': today_metrics.get('nvis') or [],
            'online': today_metrics.get('online') or [],
            'label': _id_date_label(_as_int((charts or {}).get('site_time')) or int(time.time())),
            'compare': compare_days,
        },
        'daily': {
            'labels': day_labels,
            'ticks': day_ticks,
            'weeks': day_weeks,
            'sundays': day_sundays,
            'pageviews': chart_days_pv,
            'visitors': chart_days_vis,
            'first_time': chart_days_first,
            'bounce': daily_metrics.get('bounce') or [],
            'ppv': daily_metrics.get('ppv') or [],
            'nvis': daily_metrics.get('nvis') or [],
            'online': daily_metrics.get('online') or [],
            'totals': {
                'pageviews': period_pv,
                'visitors': period_vis,
                'first_time': period_first,
                'first_share': period_share,
                'pages_per_visit': period_ppv,
            },
        },
        'forecast': {
            'hour_visitors': forecast_hour,
            'day_visitors': forecast_day,
            'hour_vs_7': forecast_vs.get('hour_7'),
            'hour_vs_14': forecast_vs.get('hour_14'),
            'day_vs_7': forecast_vs.get('day_7'),
            'day_vs_14': forecast_vs.get('day_14'),
        },
        'fetched_at': int(time.time()),
        'source': meta.get('source') or 'histats',
    }
    prev = _snapshot_cache.get(sid)
    deltas = {
        'pageviews': 0,
        'visitors': 0,
        'first_time': 0,
        'online': 0,
        'total_pageviews': 0,
        'total_visitors': 0,
    }
    if prev and isinstance(prev.get('today'), dict):
        deltas['pageviews'] = today_pv - _as_int(prev['today'].get('pageviews'))
        deltas['visitors'] = today_vis - _as_int(prev['today'].get('visitors'))
        deltas['first_time'] = today_first - _as_int(prev['today'].get('first_time'))
        deltas['online'] = online - _as_int(prev['today'].get('online'))
        deltas['total_pageviews'] = total_pv - _as_int((prev.get('totals') or {}).get('pageviews'))
        deltas['total_visitors'] = total_vis - _as_int((prev.get('totals') or {}).get('visitors'))
    out['deltas'] = deltas
    _snapshot_cache[sid] = out
    return out


def fetch_summary(
    sid: str,
    cookies: Optional[Any] = None,
    credentials: Optional[Dict[str, str]] = None,
) -> Tuple[Dict[str, Any], Any]:
    sid = str(sid or '').strip()
    if not sid:
        raise ValueError('Histats SID kosong')
    if requests is None:
        raise RuntimeError('The requests library is not installed')

    now = time.time()
    cached = _snapshot_cache.get(sid)
    if cached and (now - int(cached.get('fetched_at') or 0)) < _SNAPSHOT_TTL:
        if _chart_has_data(cached.get('hourly')) or _chart_has_data(cached.get('daily')):
            return cached, cookies or []
        if not _as_int((cached.get('totals') or {}).get('pageviews')):
            return cached, cookies or []

    sess = _new_session(cookies)
    authenticated = bool(cookies)
    creds = credentials or {}
    user = str(creds.get('user') or creds.get('email') or '').strip()
    password = str(creds.get('pass') or creds.get('password') or '')
    login_failed = False
    if user and password:
        if login(sess, user, password):
            authenticated = True
        else:
            login_failed = True
    elif not cookies:
        email, env_password = env_credentials()
        if email and env_password:
            if not login(sess, email, env_password):
                raise HistatsAuthError(
                    'Login Histats dari .env gagal. Periksa HISTATS_EMAIL / HISTATS_PASSWORD.',
                    need_login=True,
                )
            authenticated = True

    token = ''
    meta: Dict[str, Any] = {}
    payload = None
    try:
        token, meta, _final_url = _get_token(sess, sid)
    except HistatsAuthError:
        token, meta = '', {}

    if token:
        payload = _post_histats(sess, 'HST_GET_SUMMARY.php', sid, token)
        if payload is None:
            for key in list(_token_cache.keys()):
                if key.startswith(sid + ':'):
                    _token_cache.pop(key, None)
            try:
                token, meta2, _final_url = _get_token(sess, sid)
                meta.update(meta2 or {})
            except HistatsAuthError:
                pass
            if token:
                payload = _post_histats(sess, 'HST_GET_SUMMARY.php', sid, token)

    source = 'histats'
    if payload is None:
        widget = _fetch_widget_summary(sid)
        if widget:
            payload = widget
            source = 'histats-counter'
            meta.setdefault('domain', '')
        elif authenticated:
            raise HistatsAuthError(
                'Login Histats berhasil, tetapi SID ini tidak bisa dibaca. Pastikan SID milik akun tersebut.',
                need_login=False,
            )
        elif login_failed:
            raise HistatsAuthError(
                'Login Histats gagal. Periksa email dan password akun Histats.',
                need_login=True,
            )
        else:
            raise HistatsAuthError(
                'Gagal mengambil data Histats. Login dulu dengan akun Histats pemilik SID ini.',
                need_login=True,
            )
    meta['source'] = source
    if source == 'histats':
        try:
            meta['charts'] = _fill_hourly_compare(sess, sid, (meta or {}).get('charts') or {})
        except Exception:
            pass
    return _normalize_summary(sid, payload, meta), cookies_dump(sess)


_LIVE_INFO = {
    1: 'Windows XP', 2: 'Windows 2000', 3: 'Windows Server 2003', 4: 'Linux', 5: 'OS X',
    6: 'Windows 98', 7: 'Windows ME', 8: 'Win95', 9: 'Windows NT4', 10: 'Windows',
    11: 'BSD', 12: 'SunOS', 13: 'Unix', 14: 'OS2', 15: 'Windows Vista', 16: 'Windows 7',
    17: 'Windows CE', 18: 'iPod', 19: 'iPhone', 20: 'Macintosh', 21: 'Mac PowerPC',
    22: 'PowerPC', 23: 'Java OS', 24: 'BlackBerry', 25: 'Samsung Mobile', 26: 'SonyEricsson',
    27: 'Symbian OS', 28: 'Playstation', 29: 'Wii', 30: 'Android', 31: 'iPad', 100: 'Other OS',
    101: 'IE', 102: 'IE 5.5', 103: 'IE 5.0', 104: 'IE 6.0', 105: 'IE 7.0', 106: 'Opera',
    107: 'Safari', 108: 'Camino', 109: 'Firefox', 110: 'Firefox 2', 111: 'Firefox 1.5',
    112: 'Firefox 1.0', 113: 'Netscape', 116: 'Firefox 3', 117: 'Chrome', 118: 'AOL',
    120: 'IE 8', 121: 'Firefox 3.5', 129: 'Safari mobile', 130: 'Android', 131: 'Opera Mini',
    132: 'Firefox 3.6', 133: 'Chrome', 134: 'Chrome', 135: 'Safari 5', 136: 'Firefox',
    137: 'Firefox', 138: 'Firefox', 139: 'IE 9', 140: 'IE 10', 141: 'Chrome', 142: 'Chrome',
    143: 'Chrome', 144: 'Chrome', 145: 'Chrome', 146: 'Chrome', 147: 'Chrome', 148: 'Chrome',
    195: 'IE', 196: 'Firefox', 197: 'Chrome', 198: 'Safari', 199: 'Mobile', 200: 'Other',
    201: '1600 x 1200', 202: '1400 x 1050', 203: '1280 x 1024', 204: '1152 x 864',
    205: '1024 x 768', 206: '800 x 600', 207: '640 x 480', 208: '1440 x 900',
    209: '1680 x 1050', 210: '1360 x 768', 211: '1366 x 768', 212: '1920 x 1200',
    213: '1366 x 768', 214: 'width ~ 600', 215: 'width ~ 700', 216: 'width ~ 800',
    217: 'width ~ 900', 218: 'width ~ 1000', 219: 'width ~ 1300', 220: 'width ~ 1600',
    221: 'width ~ 1900', 222: 'width ~ 2200', 223: 'width ~ 2600', 224: '1900 x 1200',
    300: 'Other res',
}

_LIVE_TABS = {
    'summary': '1',
    'recent': '2',
    'visitors': '3',
    'pages': '4',
    'referrers': '5',
    'geo': '6',
}


def _histats_ip(value: Any) -> str:
    raw = str(value or '').strip()
    if not raw:
        return ''
    if '_' in raw:
        raw = raw.split('_', 1)[1]
    try:
        num = int(raw)
    except Exception:
        return str(value or '')
    parts = []
    cur = num
    for _ in range(4):
        parts.append(str(cur % 256))
        cur //= 256
    return '.'.join(reversed(parts))


def _ago_txt(seconds: int) -> str:
    """Histats-style elapsed text, e.g. 6\" / 2'17\" / 1h5'30\"."""
    sec = max(0, _as_int(seconds))
    parts: List[str] = []
    if sec > 3599:
        parts.append(f'{sec // 3600}h')
        sec %= 3600
    if sec > 59:
        parts.append(f"{sec // 60}'")
        sec %= 60
    parts.append(f'{sec}"')
    return ''.join(parts)


def _live_name(code: Any) -> str:
    return _LIVE_INFO.get(_as_int(code), '')


def _iter_map(value: Any):
    if isinstance(value, dict):
        return list(value.items())
    if isinstance(value, list):
        return [(idx, row) for idx, row in enumerate(value)]
    return []


def _decode_txt(value: Any) -> str:
    text = str(value or '').strip()
    if not text:
        return ''
    try:
        import html as html_lib
        text = html_lib.unescape(text)
    except Exception:
        pass
    prev = None
    for _ in range(4):
        if text == prev:
            break
        prev = text
        try:
            text = unquote(text)
        except Exception:
            break
    text = re.sub(r'^(https?)::+/+', r'\1://', text, flags=re.I)
    text = re.sub(r'<[^>]+>', '', text)
    return re.sub(r'\s+', ' ', text).strip()


def _build_geo_index(blob: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    countries: Dict[str, Dict[str, str]] = {}
    raw_countries = blob.get('countryID') if isinstance(blob.get('countryID'), dict) else {}
    for key, ids in raw_countries.items():
        parts = str(key or '').split('::')
        code = str(parts[0] if parts else '').strip().lower()
        name = str(parts[1] if len(parts) > 1 else '').strip()
        state = str(parts[2] if len(parts) > 2 else '').strip()
        for city_id in str(ids or '').split('::'):
            city_id = city_id.strip()
            if city_id:
                countries[city_id] = {'code': code, 'country': name, 'state': state}
    out: Dict[str, Dict[str, Any]] = {}
    raw_cities = blob.get('cityID') if isinstance(blob.get('cityID'), dict) else {}
    for city_id, raw in raw_cities.items():
        parts = str(raw or '').split('::')
        geo = dict(countries.get(str(city_id), {}))
        geo.update({
            'city': str(parts[0] if parts else '').strip(),
            'lat': str(parts[1] if len(parts) > 1 else '').strip(),
            'lon': str(parts[2] if len(parts) > 2 else '').strip(),
        })
        out[str(city_id)] = geo
    for city_id, geo in countries.items():
        out.setdefault(str(city_id), geo)
    return out


def _geo_of(index: Dict[str, Dict[str, Any]], code: Any) -> Dict[str, str]:
    row = index.get(str(code or '')) or {}
    return {
        'country_code': str(row.get('code') or '').lower(),
        'country': str(row.get('country') or ''),
        'state': str(row.get('state') or ''),
        'city': str(row.get('city') or ''),
        'lat': str(row.get('lat') or ''),
        'lon': str(row.get('lon') or ''),
    }


def _last_path_title(vis_path: Any) -> Tuple[str, str]:
    if not isinstance(vis_path, dict) or not vis_path:
        return '', ''
    try:
        last_key = max(vis_path.keys(), key=lambda k: _as_int(k))
    except Exception:
        last_key = list(vis_path.keys())[-1]
    row = vis_path.get(last_key) or {}
    if not isinstance(row, dict):
        return '', ''
    return _decode_txt(row.get('t')), _decode_txt(row.get('u'))


def _vis_path_rows(vis_path: Any, cur_time: int) -> List[Dict[str, Any]]:
    if not isinstance(vis_path, dict) or not vis_path:
        return []
    ordered = sorted(vis_path.items(), key=lambda item: _as_int(item[0]))
    out: List[Dict[str, Any]] = []
    for _, row in ordered:
        if not isinstance(row, dict):
            continue
        secs = max(0, cur_time - _as_int(row.get('tt'))) if cur_time else 0
        title = _decode_txt(row.get('t')) or _decode_txt(row.get('u'))
        url = _decode_txt(row.get('u'))
        out.append({
            'ago': _ago_txt(secs),
            'seconds': secs,
            'title': title,
            'url': url,
        })
    return out


def _normalize_live(
    sid: str,
    payload: Any,
    meta: Dict[str, Any],
    tab: str,
    page: int = 0,
    rowspp: int = 50,
) -> Dict[str, Any]:
    blob = payload if isinstance(payload, dict) else {}
    live = blob.get('livearray') if isinstance(blob.get('livearray'), dict) else blob
    if not isinstance(live, dict):
        live = {}
    geo_index = _build_geo_index(live)
    cur_time = _as_int(live.get('cur_time'))
    ip_meta = live.get('last_hits_ip') if isinstance(live.get('last_hits_ip'), dict) else {}

    recent = []
    for _, row in _iter_map(live.get('last_hits_row')):
        if not isinstance(row, dict):
            continue
        ip_key = str(row.get('ip') or '')
        info = ip_meta.get(ip_key) if isinstance(ip_meta.get(ip_key), dict) else {}
        if not info and ip_key:
            info = ip_meta.get(_as_int(ip_key)) if isinstance(ip_meta.get(_as_int(ip_key)), dict) else {}
        geo = _geo_of(geo_index, info.get('ctr'))
        title = _decode_txt(row.get('t')) or _decode_txt(row.get('u'))
        ref = _decode_txt(info.get('ref'))
        keyword = _decode_txt(info.get('k'))
        kind = 'search' if info.get('idse') else ('bookmark' if ref == 'b' else ('referrer' if ref else ''))
        secs = max(0, cur_time - _as_int(row.get('tt'))) if cur_time else 0
        recent.append({
            'ago': _ago_txt(secs),
            'seconds': secs,
            'title': title,
            'url': _decode_txt(row.get('u')),
            'ip': _histats_ip(row.get('ip')),
            'hits': _as_int(info.get('h')),
            'os': _live_name(info.get('os')),
            'browser': _live_name(info.get('brw')),
            'resolution': _live_name(info.get('res')),
            'referrer': '' if ref == 'b' else (keyword or ref),
            'referrer_kind': kind,
            'referrer_label': 'Bookmark / Direct' if ref == 'b' else (keyword or ref),
            **geo,
        })

    pages = []
    total_pages = _as_int(live.get('count_live_pages')) or 1
    for _, row in _iter_map(live.get('top_url')):
        if not isinstance(row, dict):
            continue
        hits = _as_int(row.get('h'))
        pages.append({
            'hits': hits,
            'title': _decode_txt(row.get('t')) or _decode_txt(row.get('u')),
            'url': _decode_txt(row.get('u')),
            'share': round((hits / total_pages) * 100.0, 2) if total_pages else 0.0,
        })
    pages.sort(key=lambda item: item['hits'], reverse=True)
    page_hits_total = sum(item['hits'] for item in pages) or total_pages or 1
    for item in pages:
        item['share'] = round((item['hits'] / page_hits_total) * 100.0, 2)

    visitors = []
    for ip_key, row in _iter_map(live.get('top_ip')):
        if not isinstance(row, dict):
            continue
        geo = _geo_of(geo_index, row.get('ctr'))
        path = _vis_path_rows(row.get('vis_path'), cur_time)
        title, url = _last_path_title(row.get('vis_path'))
        if not title and path:
            title = path[-1].get('title') or ''
            url = path[-1].get('url') or ''
        last_ago = path[-1]['ago'] if path else ''
        history = list(reversed(path[:-1])) if len(path) > 1 else []
        ref = _decode_txt(row.get('ref'))
        keyword = _decode_txt(row.get('k'))
        kind = 'search' if row.get('idse') else ('bookmark' if ref == 'b' else ('referrer' if ref else ''))
        visitors.append({
            'hits': _as_int(row.get('h')),
            'ip': _histats_ip(row.get('ip') or ip_key),
            'os': _live_name(row.get('os')),
            'browser': _live_name(row.get('brw')),
            'resolution': _live_name(row.get('res')),
            'title': title,
            'url': url,
            'last_ago': last_ago,
            'path': history,
            'referrer': '' if ref == 'b' else (keyword or ref),
            'referrer_kind': kind,
            'referrer_label': 'Bookmark / Direct' if ref == 'b' else (keyword or ref),
            **geo,
        })
    visitors.sort(key=lambda item: item['hits'], reverse=True)

    referrers = []
    raw_refs = list(_iter_map(live.get('top_ref')))
    ref_hits_total = 0
    parsed_refs = []
    for key, row in raw_refs:
        if not isinstance(row, dict):
            row = {'h': _as_int(row)}
        hits = _as_int(row.get('h'))
        ref_hits_total += hits
        parsed_refs.append((str(key), row, hits))
    ref_hits_total = max(ref_hits_total, 1)
    for raw_key, row, hits in parsed_refs:
        keyword = _decode_txt(row.get('k'))
        if raw_key == 'b' or _decode_txt(raw_key) == 'b':
            label, kind, url = 'Bookmark / Direct', 'bookmark', ''
        elif row.get('idse') and keyword:
            label, kind, url = keyword, 'search', _decode_txt(raw_key)
        else:
            label, kind, url = _decode_txt(raw_key) or raw_key, 'referrer', _decode_txt(raw_key)
        referrers.append({
            'hits': hits,
            'label': label,
            'url': url if url != 'b' else '',
            'kind': kind,
            'share': round((hits / ref_hits_total) * 100.0, 2),
        })
    referrers.sort(key=lambda item: item['hits'], reverse=True)

    geo_rows = []
    country_tot: Dict[str, Dict[str, Any]] = {}
    for city_id, count in _iter_map(live.get('citystats')):
        hits = _as_int(count)
        geo = _geo_of(geo_index, city_id)
        row = {'count': hits, **geo}
        geo_rows.append(row)
        code = geo.get('country_code') or ''
        bucket = country_tot.setdefault(code or geo.get('country') or 'unknown', {
            'code': code,
            'name': geo.get('country') or 'Unknown',
            'count': 0,
        })
        bucket['count'] += hits
    geo_rows.sort(key=lambda item: item['count'], reverse=True)
    countries = sorted(country_tot.values(), key=lambda item: item['count'], reverse=True)

    page_n = max(_as_int(page), 0)
    rows_n = max(_as_int(rowspp), 1)
    return {
        'sid': sid,
        'domain': (meta or {}).get('domain') or '',
        'title': (meta or {}).get('title') or '',
        'tab': tab,
        'page': page_n,
        'rowspp': rows_n,
        'users_online': _as_int(live.get('count_users_online')),
        'pages_browsing': _as_int(live.get('count_live_pages')),
        'totals': {
            'recent': _as_int(live.get('last_hits_rows')) or len(recent),
            'visitors': _as_int(live.get('top_ip_rows')) or len(visitors),
            'pages': _as_int(live.get('top_url_rows')) or len(pages),
            'referrers': _as_int(live.get('top_ref_rows')) or len(referrers),
            'geo': len(geo_rows),
            'countries': len(countries),
        },
        'recent': recent,
        'pages': pages,
        'visitors': visitors,
        'referrers': referrers,
        'geo': geo_rows,
        'countries': countries,
        'fetched_at': int(time.time()),
        'source': (meta or {}).get('source') or 'histats',
    }


def fetch_live(
    sid: str,
    tab: str = 'summary',
    page: int = 0,
    rowspp: int = 0,
    cookies: Optional[Any] = None,
    credentials: Optional[Dict[str, str]] = None,
) -> Tuple[Dict[str, Any], Any]:
    sid = str(sid or '').strip()
    if not sid:
        raise ValueError('Histats SID kosong')
    if requests is None:
        raise RuntimeError('The requests library is not installed')
    tab_key = str(tab or 'summary').strip().lower()
    tipo = _LIVE_TABS.get(tab_key) or (_LIVE_TABS.get(tab_key.replace('-', '_')) or '1')
    if tab_key in _LIVE_TABS.values():
        tipo = tab_key
        tab_key = next((name for name, code in _LIVE_TABS.items() if code == tipo), 'summary')
    rows = _as_int(rowspp)
    if rows <= 0:
        rows = 10 if tipo == '1' else 50
    rows = min(max(rows, 1), 200)
    page_n = max(_as_int(page), 0)
    cache_key = f'{sid}:{tipo}:{page_n}:{rows}'
    now = time.time()
    cached = _live_cache.get(cache_key)
    if cached and (now - int(cached.get('fetched_at') or 0)) < _LIVE_TTL:
        return cached, cookies or []

    sess = _new_session(cookies)
    authenticated = bool(cookies)
    creds = credentials or {}
    user = str(creds.get('user') or creds.get('email') or '').strip()
    password = str(creds.get('pass') or creds.get('password') or '')
    login_failed = False
    if user and password:
        if login(sess, user, password):
            authenticated = True
        else:
            login_failed = True
    elif not cookies:
        email, env_password = env_credentials()
        if email and env_password:
            if not login(sess, email, env_password):
                raise HistatsAuthError(
                    'Login Histats dari .env gagal. Periksa HISTATS_EMAIL / HISTATS_PASSWORD.',
                    need_login=True,
                )
            authenticated = True

    token = ''
    meta: Dict[str, Any] = {}
    payload = None
    try:
        token, meta, _final_url = _get_token(sess, sid)
    except HistatsAuthError:
        token, meta = '', {}
    extra = {
        'AR_REQ[type]': tipo,
        'AR_REQ[rowspp]': str(rows),
        'AR_REQ[timestart]': '0',
        'AR_REQ[page]': str(page_n),
    }
    referer = f'{BASE}/viewstats/?sid={sid}&act=18'
    if token:
        payload = _post_histats(sess, 'HST_GET_LIVE.php', sid, token, extra=extra, referer=referer)
        if payload is None:
            for key in list(_token_cache.keys()):
                if key.startswith(sid + ':'):
                    _token_cache.pop(key, None)
            try:
                token, meta2, _final_url = _get_token(sess, sid)
                meta.update(meta2 or {})
            except HistatsAuthError:
                pass
            if token:
                payload = _post_histats(sess, 'HST_GET_LIVE.php', sid, token, extra=extra, referer=referer)

    if payload is None:
        if authenticated:
            raise HistatsAuthError(
                'Login Histats berhasil, tetapi data Users online tidak bisa dibaca. Pastikan SID milik akun tersebut.',
                need_login=False,
            )
        if login_failed:
            raise HistatsAuthError(
                'Login Histats gagal. Periksa email dan password akun Histats.',
                need_login=True,
            )
        raise HistatsAuthError(
            'Gagal mengambil Users online. Login dulu dengan akun Histats pemilik SID ini.',
            need_login=True,
        )

    err = _as_int(payload.get('error')) if isinstance(payload, dict) else 0
    if err and err not in (13,):
        raise HistatsAuthError(
            'Histats menolak permintaan Users online. Login ulang atau coba SID lain.',
            need_login=err in (11, 12),
        )
    meta['source'] = 'histats'
    out = _normalize_live(
        sid,
        payload if err != 13 else {},
        meta,
        tab_key,
        page=page_n,
        rowspp=rows,
    )
    _live_cache[cache_key] = out
    return out, cookies_dump(sess)


_TRAFFIC_TTL = 12
_traffic_cache: Dict[str, Dict[str, Any]] = {}
_MONTHS_ID = [
    'Januari', 'Februari', 'Maret', 'April', 'Mei', 'Juni',
    'Juli', 'Agustus', 'September', 'Oktober', 'November', 'Desember',
]
_MONTHS_SHORT = ['Jan', 'Feb', 'Mar', 'Apr', 'Mei', 'Jun', 'Jul', 'Agu', 'Sep', 'Okt', 'Nov', 'Des']


def _utc_dt(ts: int) -> datetime:
    ts = _as_int(ts) or int(time.time())
    return datetime.fromtimestamp(ts, timezone.utc)


def _unix_day_begin(ts: int) -> int:
    dt = _utc_dt(ts).replace(hour=0, minute=0, second=1, microsecond=0)
    return int(dt.timestamp())


def _unix_month_begin(ts: int) -> int:
    dt = _utc_dt(ts).replace(day=1, hour=0, minute=0, second=1, microsecond=0)
    return int(dt.timestamp())


def _unix_year_begin(ts: int) -> int:
    dt = _utc_dt(ts).replace(month=1, day=1, hour=0, minute=0, second=1, microsecond=0)
    return int(dt.timestamp())


def _shift_month(ts: int, delta: int) -> int:
    dt = _utc_dt(ts)
    month = dt.month - 1 + int(delta)
    year = dt.year + month // 12
    month = month % 12 + 1
    day = min(dt.day, 28)
    try:
        out = dt.replace(year=year, month=month, day=dt.day, hour=0, minute=0, second=1, microsecond=0)
    except ValueError:
        out = dt.replace(year=year, month=month, day=day, hour=0, minute=0, second=1, microsecond=0)
    return int(out.timestamp())


def _normalize_traffic_t(ts: int, rng: str) -> int:
    ts = _as_int(ts)
    if not ts:
        return 0
    kind = str(rng or 'h').lower()
    if kind == 'd':
        return _unix_month_begin(ts)
    if kind == 'm':
        return _unix_year_begin(ts)
    return _unix_day_begin(ts)


def _open_histats_session(
    cookies: Optional[Any] = None,
    credentials: Optional[Dict[str, str]] = None,
) -> Tuple['requests.Session', bool, bool]:
    sess = _new_session(cookies)
    authenticated = bool(cookies)
    login_failed = False
    creds = credentials or {}
    user = str(creds.get('user') or creds.get('email') or '').strip()
    password = str(creds.get('pass') or creds.get('password') or '')
    if user and password:
        if login(sess, user, password):
            authenticated = True
        else:
            login_failed = True
    elif not cookies:
        email, env_password = env_credentials()
        if email and env_password:
            if login(sess, email, env_password):
                authenticated = True
            else:
                login_failed = True
    return sess, authenticated, login_failed


def _parse_traffic_page(html: str) -> Dict[str, Any]:
    parsed: Dict[str, Any] = {
        'timezone': '',
        'site_time': 0,
        'gmt_time': 0,
        'domain': '',
        'title': '',
        'blocks': [],
    }
    if not html:
        return parsed
    m = re.search(r"OBJ_SITE\.timezone_str\s*=\s*'([^']+)'", html)
    if m:
        parsed['timezone'] = m.group(1).strip()
    m = re.search(r'OBJ_SITE\.site_time\s*=\s*(\d+)', html)
    if m:
        parsed['site_time'] = _as_int(m.group(1))
    m = re.search(r'OBJ_SITE\.gmt_time\s*=\s*(\d+)', html)
    if m:
        parsed['gmt_time'] = _as_int(m.group(1))
    for info in _parse_js_json_calls(html, 'json_parse'):
        if not isinstance(info, dict):
            continue
        if info.get('url'):
            parsed['domain'] = str(info.get('url') or '').strip()
        if info.get('title'):
            parsed['title'] = str(info.get('title') or '').strip()
    for obj in _parse_js_json_calls(html, '_init_from_JSON'):
        if not isinstance(obj, dict):
            continue
        segs = obj.get('AR_segments') if isinstance(obj.get('AR_segments'), dict) else {}
        if not segs:
            continue
        parsed['blocks'].append({
            'range': str(obj.get('range') or '').strip().lower(),
            'unix': _as_int(obj.get('UNIXTIME')),
            'segs': segs,
            'totals': obj.get('AR_TOTALS') if isinstance(obj.get('AR_TOTALS'), dict) else {},
            'additionals': obj.get('AR_additionals') if isinstance(obj.get('AR_additionals'), dict) else {},
        })
    return parsed


def _get_act3(
    sess: 'requests.Session',
    sid: str,
    t1: int,
    t2: int,
    mode: str,
    rng: str,
) -> Tuple[str, str]:
    q = [f'sid={sid}', 'act=3', 'f=1', f't_mode={mode}', f't_rg={rng}']
    if t1:
        q.append(f't_1={int(t1)}')
    if t2:
        q.append(f't_2={int(t2)}')
    query = '&'.join(q)
    last_html = ''
    last_url = ''
    for url in (
        f'{BASE}/viewstats/?{query}',
        f'https://static.histats.com/viewstats/?{query}',
    ):
        resp = sess.get(url, timeout=20, allow_redirects=True)
        last_html = resp.text or ''
        last_url = str(resp.url or url)
        if _parse_js_json_calls(last_html, '_init_from_JSON'):
            return last_html, last_url
        if _is_protected_response(resp):
            continue
    return last_html, last_url


def _id_date_label(ts: int) -> str:
    dt = _utc_dt(_as_int(ts) or int(time.time()))
    return f'{dt.day} {_MONTHS_ID[dt.month - 1]}'


def _fill_hourly_compare(sess: 'requests.Session', sid: str, charts: Dict[str, Any]) -> Dict[str, Any]:
    charts = dict(charts or {})
    compare_map = dict(charts.get('compare') or {})
    hourly_cmp: Dict[Any, Any] = dict(charts.get('hourly_compare') or {})
    site_time = _as_int(charts.get('site_time')) or int(time.time())
    today = _unix_day_begin(site_time)

    def _segs_for(ts: int) -> Optional[Dict[str, Any]]:
        for key in (ts, str(ts), _as_int(ts)):
            row = hourly_cmp.get(key)
            if isinstance(row, dict) and (row.get('h') or row.get('v')):
                return row
        return None

    for key in (1, 7, 14):
        ts = _as_int(compare_map.get(key) or compare_map.get(str(key)))
        if not ts:
            ts = _unix_day_begin(today - key * 86400)
            compare_map[key] = ts
        if _segs_for(ts):
            continue
        try:
            html, _ = _get_act3(sess, sid, ts, 0, 'normal', 'h')
            page = _parse_traffic_page(html)
            block = _pick_block(page.get('blocks') or [], 'h', ts)
            segs = (block or {}).get('segs') if isinstance(block, dict) else None
            if isinstance(segs, dict) and (segs.get('h') or segs.get('v')):
                hourly_cmp[ts] = segs
        except Exception:
            continue
    charts['compare'] = compare_map
    charts['hourly_compare'] = hourly_cmp
    return charts


def _clip_segs(segs: Dict[str, Any], start: int, end: int) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for key, val in (segs or {}).items():
        if isinstance(val, list):
            out[key] = val[start:end]
        else:
            out[key] = val
    return out


def _concat_segs(blocks: List[Dict[str, Any]]) -> Dict[str, Any]:
    merged: Dict[str, Any] = {}
    lists: Dict[str, List[Any]] = {}
    for block in blocks:
        segs = block.get('segs') if isinstance(block.get('segs'), dict) else {}
        for key, val in segs.items():
            if isinstance(val, list):
                lists.setdefault(key, []).extend(val)
            elif key not in merged:
                merged[key] = val
    merged.update(lists)
    return merged


def _sum_totals(blocks: List[Dict[str, Any]]) -> Dict[str, int]:
    out = {'h': 0, 'v': 0, 'nv': 0, 'bb': 0, 'tb': 0}
    for block in blocks:
        tot = block.get('totals') if isinstance(block.get('totals'), dict) else {}
        segs = block.get('segs') if isinstance(block.get('segs'), dict) else {}
        for key in out:
            val = _as_int(tot.get(key))
            if not val:
                val = sum(_as_int(x) for x in (segs.get(key) or []))
            out[key] += val
    return out


def _slice_daily_blocks(blocks: List[Dict[str, Any]], start_ts: int, end_ts: int) -> Dict[str, Any]:
    start_ts = _unix_day_begin(start_ts)
    end_ts = _unix_day_begin(end_ts or start_ts)
    if end_ts < start_ts:
        start_ts, end_ts = end_ts, start_ts
    parts: List[Dict[str, Any]] = []
    for block in sorted(blocks, key=lambda row: _as_int(row.get('unix'))):
        unix = _as_int(block.get('unix'))
        segs = block.get('segs') if isinstance(block.get('segs'), dict) else {}
        n = len(segs.get('h') or segs.get('v') or [])
        if not unix or n <= 0:
            continue
        start_i = int((start_ts - unix) // 86400)
        end_i = int((end_ts - unix) // 86400) + 1
        start_i = max(0, start_i)
        end_i = min(n, max(start_i + 1, end_i))
        if start_i >= n or end_i <= 0:
            continue
        clipped = _clip_segs(segs, start_i, end_i)
        parts.append({
            'range': 'd',
            'unix': unix + start_i * 86400,
            'segs': clipped,
            'totals': {},
            'additionals': block.get('additionals') or {},
        })
    if not parts:
        return {}
    return {
        'range': 'd',
        'unix': parts[0]['unix'],
        'segs': _concat_segs(parts) if len(parts) > 1 else parts[0]['segs'],
        'totals': _sum_totals(parts),
        'additionals': parts[0].get('additionals') or {},
    }


def _kpis_from_packed(packed: Dict[str, Any], totals: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    totals = totals if isinstance(totals, dict) else {}
    pv = _as_int(totals.get('h')) or sum(_as_int(x) for x in (packed.get('raw_pageviews') or []))
    vis = _as_int(totals.get('v')) or sum(_as_int(x) for x in (packed.get('raw_visitors') or []))
    first = _as_int(totals.get('nv')) or sum(_as_int(x) for x in (packed.get('raw_first') or []))
    ppv = round(pv / vis, 2) if vis else 0.0
    share = round((first / vis) * 100.0, 1) if vis else 0.0
    return {
        'pageviews': pv,
        'visitors': vis,
        'first_time': first,
        'pages_per_visit': ppv,
        'new_ratio': share,
    }


def _traffic_axis(rng: str, unix: int, count: int, concat_hours: bool = False) -> Dict[str, List[Any]]:
    n = max(_as_int(count), 0)
    kind = str(rng or 'h').lower()
    if kind == 'h':
        if concat_hours and n > 24 and unix:
            labels, ticks, weeks, sundays = [], [], [], []
            start = _utc_dt(unix)
            for i in range(n):
                hour = i % 24
                day = start + timedelta(hours=i)
                labels.append(f'{day.day} {hour:02d}:00 – {hour:02d}:59')
                ticks.append(f'{hour:02d}:00')
                weeks.append(day.strftime('%d %b') if hour == 0 else '')
                sundays.append(day.weekday() == 6 and hour == 0)
            return {'labels': labels, 'ticks': ticks, 'weeks': weeks, 'sundays': sundays}
        ticks = [f'{i:02d}:00' for i in range(n)]
        labels = [f'{i:02d}:00 – {i:02d}:59' for i in range(n)]
        return {'labels': labels, 'ticks': ticks, 'weeks': [''] * n, 'sundays': [False] * n}
    if kind == 'm':
        labels, ticks = [], []
        start = _utc_dt(unix)
        for i in range(n):
            month = start.month - 1 + i
            year = start.year + month // 12
            month = month % 12
            labels.append(f'{_MONTHS_ID[month]} {year}')
            ticks.append(_MONTHS_SHORT[month])
        return {'labels': labels, 'ticks': ticks, 'weeks': [''] * n, 'sundays': [False] * n}
    return _day_axis(unix, n)


def _traffic_table(packed: Dict[str, Any], axis: Dict[str, List[Any]], rng: str) -> List[Dict[str, Any]]:
    vis = packed.get('raw_visitors') or []
    pv = packed.get('raw_pageviews') or []
    first = packed.get('raw_first') or []
    bounce = packed.get('bounce') or []
    ppv = packed.get('ppv') or []
    nvis = packed.get('nvis') or []
    n = max(len(vis), len(pv), len(first), 0)
    total_vis = sum(_as_int(x) for x in vis) or 0
    max_vis = max((_as_int(x) for x in vis), default=0) or 1
    labels = axis.get('labels') or []
    rows: List[Dict[str, Any]] = []
    kind = str(rng or 'h').lower()
    for i in range(n):
        v = _as_int(vis[i] if i < len(vis) else 0)
        h = _as_int(pv[i] if i < len(pv) else 0)
        nv = _as_int(first[i] if i < len(first) else 0)
        if kind == 'h' and n <= 24:
            label = f'{i:02d}:00 – {i:02d}:59'
        else:
            label = str(labels[i] if i < len(labels) else i)
        share = round((v / total_vis) * 100.0, 1) if total_vis else 0.0
        bar = round((v / max_vis) * 100.0, 1) if max_vis else 0.0
        rows.append({
            'label': label,
            'share': share,
            'bar': bar,
            'visitors': v,
            'pageviews': h,
            'ppv': round(_as_float(ppv[i] if i < len(ppv) else 0), 2),
            'new_visitors': nv,
            'new_ratio': round(_as_float(nvis[i] if i < len(nvis) else 0), 0),
            'bounce': round(_as_float(bounce[i] if i < len(bounce) else 0), 1),
        })
    return rows


def _activity_map(block: Optional[Dict[str, Any]]) -> Dict[str, int]:
    out: Dict[str, int] = {}
    if not isinstance(block, dict):
        return out
    unix = _as_int(block.get('unix'))
    segs = block.get('segs') if isinstance(block.get('segs'), dict) else {}
    vis = segs.get('v') or segs.get('h') or []
    if not unix:
        return out
    start = _utc_dt(unix).replace(hour=0, minute=0, second=0, microsecond=0)
    for i, val in enumerate(vis):
        day = start + timedelta(days=i)
        out[day.strftime('%Y-%m-%d')] = _as_int(val)
    return out


def _calendar_payload(
    selected_ts: int,
    view_ts: int,
    activity: Dict[str, int],
    today_ts: int,
) -> Dict[str, Any]:
    selected = _utc_dt(selected_ts or view_ts)
    view = _utc_dt(view_ts or selected_ts)
    view = view.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    today = _utc_dt(today_ts or int(time.time())).date()
    selected_date = selected.date()
    first = view.date().replace(day=1)
    start = first - timedelta(days=first.weekday())  # Monday
    if start != first:
        start = start - timedelta(days=7)
    weeks = []
    cursor = start
    labeled = False
    for _ in range(6):
        days = []
        for _dow in range(7):
            iso = cursor.strftime('%Y-%m-%d')
            ts = int(datetime(cursor.year, cursor.month, cursor.day, 0, 0, 1, tzinfo=timezone.utc).timestamp())
            in_month = cursor.month == view.month
            days.append({
                'd': cursor.day,
                'ts': ts,
                'iso': iso,
                'month': cursor.month,
                'muted': not in_month,
                'weekend': cursor.weekday() >= 5,
                'selected': cursor == selected_date,
                'today': cursor == today,
                'has_data': _as_int(activity.get(iso)) > 0,
            })
            cursor = cursor + timedelta(days=1)
        month_label = ''
        if not labeled and days and all(int(day.get('month') or 0) != view.month for day in days):
            month_label = _MONTHS_ID[int(days[0]['month']) - 1]
            labeled = True
        weeks.append({'month_label': month_label, 'days': days})
        after_view = (cursor.year, cursor.month) > (view.year, view.month)
        if after_view and cursor.day > 7:
            break
    return {
        'title': f'{selected.day} {_MONTHS_ID[selected.month - 1]}',
        'year': view.year,
        'month': view.month,
        'month_name': _MONTHS_ID[view.month - 1],
        'weekdays': ['M', 'T', 'W', 'T', 'F', 'S', 'S'],
        'weeks': weeks,
        'selected_ts': int(selected.replace(hour=0, minute=0, second=1, microsecond=0).timestamp()),
    }


def _series_payload(block: Dict[str, Any], rng: str, concat_hours: bool = False) -> Dict[str, Any]:
    segs = block.get('segs') if isinstance(block.get('segs'), dict) else {}
    n = len(segs.get('h') or segs.get('v') or [])
    packed = _pack_hour_metrics(segs, n or 1, rng)
    axis = _traffic_axis(rng, _as_int(block.get('unix')), n, concat_hours=concat_hours)
    kpis = _kpis_from_packed(packed, block.get('totals'))
    table = _traffic_table(packed, axis, rng)
    return {
        'unix': _as_int(block.get('unix')),
        'range': rng,
        'labels': axis.get('labels') or [],
        'ticks': axis.get('ticks') or [],
        'weeks': axis.get('weeks') or [],
        'sundays': axis.get('sundays') or [],
        'pageviews': packed.get('pageviews') or [],
        'visitors': packed.get('visitors') or [],
        'first_time': packed.get('first_time') or [],
        'bounce': packed.get('bounce') or [],
        'ppv': packed.get('ppv') or [],
        'nvis': packed.get('nvis') or [],
        'online': packed.get('online') or [],
        'kpis': kpis,
        'table': table,
    }


def _fallback_blocks(meta: Dict[str, Any], rng: str) -> List[Dict[str, Any]]:
    charts = (meta or {}).get('charts') if isinstance(meta, dict) else {}
    if not isinstance(charts, dict):
        return []
    site_time = _as_int(charts.get('site_time')) or int(time.time())
    if rng == 'h' and isinstance(charts.get('hourly'), dict) and (charts['hourly'].get('h') or charts['hourly'].get('v')):
        return [{
            'range': 'h',
            'unix': _unix_day_begin(site_time),
            'segs': charts.get('hourly') or {},
            'totals': {},
            'additionals': {},
        }]
    if rng == 'd' and isinstance(charts.get('daily'), dict) and (charts['daily'].get('h') or charts['daily'].get('v')):
        return [{
            'range': 'd',
            'unix': _as_int(charts.get('daily_start_ts')) or _unix_month_begin(site_time),
            'segs': charts.get('daily') or {},
            'totals': {},
            'additionals': {},
        }]
    return []


def _pick_block(blocks: List[Dict[str, Any]], rng: str, t1: int) -> Optional[Dict[str, Any]]:
    matches = [row for row in blocks if str(row.get('range') or '') == rng] or blocks
    if not matches:
        return None
    if t1:
        exact = [row for row in matches if _as_int(row.get('unix')) == t1]
        if exact:
            return exact[0]
        closest = min(matches, key=lambda row: abs(_as_int(row.get('unix')) - t1))
        return closest
    return sorted(matches, key=lambda row: _as_int(row.get('unix')))[-1]


def fetch_traffic(
    sid: str,
    rng: str = 'h',
    t1: int = 0,
    t2: int = 0,
    mode: str = 'normal',
    cookies: Optional[Any] = None,
    credentials: Optional[Dict[str, str]] = None,
) -> Tuple[Dict[str, Any], Any]:
    sid = str(sid or '').strip()
    if not sid:
        raise ValueError('Histats SID kosong')
    if requests is None:
        raise RuntimeError('The requests library is not installed')
    rng = str(rng or 'h').strip().lower()
    if rng not in ('h', 'd', 'm'):
        rng = 'h'
    mode = str(mode or 'normal').strip().lower()
    if mode not in ('normal', 'range', 'compare'):
        mode = 'normal'
    t1 = _as_int(t1)
    t2 = _as_int(t2)
    cache_key = f'{sid}:{rng}:{mode}:{t1}:{t2}'
    now = time.time()
    cached = _traffic_cache.get(cache_key)
    if cached and (now - int(cached.get('fetched_at') or 0)) < _TRAFFIC_TTL:
        return cached, cookies or []

    sess, authenticated, login_failed = _open_histats_session(cookies, credentials)
    token = ''
    meta: Dict[str, Any] = {}
    try:
        token, meta, _final_url = _get_token(sess, sid)
    except HistatsAuthError:
        token, meta = '', {}

    charts = (meta or {}).get('charts') if isinstance(meta, dict) else {}
    site_time = _as_int((charts or {}).get('site_time')) or int(time.time())
    selected_ts = t1 or site_time
    fetch_t1 = _normalize_traffic_t(selected_ts, rng)
    fetch_t2 = _normalize_traffic_t(t2, rng) if t2 and mode != 'normal' else 0
    html, last_url = _get_act3(
        sess, sid, fetch_t1, fetch_t2 if mode != 'normal' else 0, mode, rng,
    )
    page = _parse_traffic_page(html)
    blocks = list(page.get('blocks') or [])
    if not blocks:
        blocks = _fallback_blocks(meta, rng)
    if mode == 'range' and rng == 'd' and (not blocks or len(blocks) < 1 or t2):
        month_blocks: List[Dict[str, Any]] = []
        cur = _unix_month_begin(selected_ts)
        end = _unix_month_begin(t2 or selected_ts)
        if end < cur:
            cur, end = end, cur
        guard = 0
        while cur <= end and guard < 14:
            extra_html, _ = _get_act3(sess, sid, cur, 0, 'normal', 'd')
            extra_page = _parse_traffic_page(extra_html)
            month_blocks.extend(extra_page.get('blocks') or [])
            if not page.get('site_time') and extra_page.get('site_time'):
                page = extra_page
            cur = _shift_month(cur, 1)
            guard += 1
        sliced = _slice_daily_blocks(month_blocks, selected_ts, t2 or selected_ts)
        if sliced:
            blocks = [sliced]

    if not blocks:
        blob = f'{last_url}\n{html[:2000]}'
        if 'redir_protected_stats' in blob or not token:
            if login_failed:
                raise HistatsAuthError(
                    'Login Histats gagal. Periksa email dan password akun Histats.',
                    need_login=True,
                )
            if authenticated:
                raise HistatsAuthError(
                    'Login Histats berhasil, tetapi Traffic stats SID ini tidak bisa dibaca.',
                    need_login=False,
                )
            raise HistatsAuthError(
                'Gagal mengambil Traffic stats. Login dulu dengan akun Histats pemilik SID ini.',
                need_login=True,
            )
        raise HistatsAuthError('Histats tidak mengirim data Traffic stats untuk tanggal ini.', need_login=False)

    if page.get('site_time'):
        site_time = page['site_time']
    primary_blocks = [row for row in blocks if str(row.get('range') or rng) == rng] or blocks
    concat_hours = False
    compare_block = None
    if mode == 'range' and rng == 'h' and len(primary_blocks) > 1:
        ordered = sorted(primary_blocks, key=lambda row: _as_int(row.get('unix')))
        primary = {
            'range': 'h',
            'unix': _as_int(ordered[0].get('unix')),
            'segs': _concat_segs(ordered),
            'totals': _sum_totals(ordered),
            'additionals': ordered[0].get('additionals') or {},
        }
        concat_hours = True
    elif mode == 'compare' and len(primary_blocks) >= 2:
        primary = _pick_block(primary_blocks, rng, fetch_t1) or primary_blocks[0]
        compare_block = _pick_block(primary_blocks, rng, fetch_t2) if fetch_t2 else primary_blocks[1]
        if compare_block is primary:
            compare_block = next((row for row in primary_blocks if row is not primary), primary_blocks[-1])
    else:
        primary = _pick_block(primary_blocks, rng, fetch_t1) or primary_blocks[0]

    series = _series_payload(primary, rng, concat_hours=concat_hours)
    compare_series = _series_payload(compare_block, rng) if compare_block else None

    cal_source = None
    if rng == 'd' and not concat_hours:
        cal_source = primary
    else:
        month_begin = _unix_month_begin(selected_ts)
        cal_html, _ = _get_act3(sess, sid, month_begin, 0, 'normal', 'd')
        cal_page = _parse_traffic_page(cal_html)
        cal_source = _pick_block(cal_page.get('blocks') or [], 'd', month_begin)
        if cal_page.get('domain') and not page.get('domain'):
            page['domain'] = cal_page['domain']
            page['title'] = cal_page.get('title') or page.get('title')
    calendar = _calendar_payload(
        selected_ts,
        selected_ts,
        _activity_map(cal_source),
        site_time,
    )

    heading = {
        'h': 'Daily stats',
        'd': 'Daily stats',
        'm': 'Monthly stats',
    }.get(rng, 'Daily stats')
    domain = page.get('domain') or (meta or {}).get('domain') or ''
    title = page.get('title') or (meta or {}).get('title') or domain
    out = {
        'sid': sid,
        'domain': domain,
        'title': title,
        'range': rng,
        'mode': mode,
        't1': _as_int(primary.get('unix')) or fetch_t1,
        't2': _as_int((compare_block or {}).get('unix')) if compare_block else fetch_t2,
        'timezone': page.get('timezone') or (meta or {}).get('timezone') or '',
        'site_time': site_time,
        'heading': heading,
        'kpis': series.get('kpis') or {},
        'series': series,
        'compare': compare_series,
        'calendar': calendar,
        'fetched_at': int(time.time()),
        'source': 'histats',
    }
    _traffic_cache[cache_key] = out
    return out, cookies_dump(sess)


_INFOV_TTL = 12
_infovis_cache: Dict[str, Dict[str, Any]] = {}
_INFOV_SECTIONS = {
    'OS': 'OS',
    'BRW': 'Browser',
    'RES': 'Screen resolution',
    'ADDON': 'Toolbars',
}
_INFOV_NAMES = dict(_LIVE_INFO)
_INFOV_NAMES.update({
    22: 'PowerPC', 23: 'Java OS', 28: 'Playstation', 29: 'Wii',
    32: 'Windows 8', 33: 'Windows Phone', 34: 'Windows 8.1', 35: 'Windows 10',
    36: 'Windows 11', 37: 'Chrome OS',
    108: 'Camino', 111: 'Firefox 1.5', 112: 'Firefox 1.0', 113: 'Netscape',
    118: 'AOL', 119: 'Avant Browser', 122: 'Playstation', 124: 'SeaMonkey',
    125: 'Konqueror', 127: 'IE mobile', 141: 'Chrome 7',
    195: 'IE', 196: 'Firefox', 197: 'Chrome', 198: 'Safari', 199: 'Mobile', 200: 'Other',
    201: '1600 x 1200', 202: '1400 x 1050', 203: '1280 x 1024', 204: '1152 x 864',
    205: '1024 x 768', 206: '800 x 600', 207: '640 x 480', 208: '1440 x 900',
    209: '1680 x 1050', 210: '1360 x 768', 211: '1366 x 768', 212: '1920 x 1200',
    224: '1900 x 1200', 300: 'Other res',
    501: 'Trident', 502: 'Google toolbar', 503: 'Infopath', 504: 'OfficeLive',
    506: 'Windows 64 bit', 507: 'MSN toolbar', 508: 'Googlebot', 513: 'Maxthon',
    515: 'Alexa', 516: 'Tablet', 521: 'ASK toolbar',
})


def _identa_seconds(value: Any) -> str:
    sec = _as_int(value)
    if sec <= 0:
        return '-'
    hours = sec // 3600
    sec = sec % 3600
    minutes = sec // 60
    sec = sec % 60
    out = ''
    if hours:
        out += f'{hours}h'
    if minutes:
        out += f"{minutes}'"
    if sec:
        out += f'{sec}"'
    return out or '-'


def _infovis_icon(section: str, name: str) -> str:
    text = str(name or '').lower()
    if section == 'OS':
        if 'android' in text:
            return 'android'
        if any(k in text for k in ('iphone', 'ipad', 'ipod', 'mac', 'os x', 'osx', 'apple', 'ios')):
            return 'apple'
        if any(k in text for k in ('linux', 'unix', 'bsd', 'sunos')):
            return 'linux'
        if 'win' in text or 'vista' in text:
            return 'windows'
        if 'black' in text:
            return 'blackberry'
        return 'other'
    if section == 'BRW':
        if 'chrome' in text:
            return 'chrome'
        if 'firefox' in text or 'fire' in text:
            return 'firefox'
        if 'safari' in text:
            return 'safari'
        if text.startswith('ie') or 'internet' in text:
            return 'ie'
        if 'opera' in text:
            return 'opera'
        return 'other'
    if section == 'RES':
        return 'screen'
    return 'plugin'


def _post_infovis(sess: 'requests.Session', payload: Dict[str, Any], referer: str) -> Any:
    blob = json.dumps(payload, separators=(',', ':'), ensure_ascii=True)
    encoded = base64.b64encode(blob.encode('utf-8')).decode('ascii')
    headers = {
        'X-Requested-With': 'XMLHttpRequest',
        'Referer': referer,
        'Origin': BASE,
        'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
        'Accept': 'application/json, text/javascript, */*; q=0.01',
        'Accept-Encoding': 'gzip, deflate',
    }
    resp = sess.post(
        f'{BASE}/viewstats/HST_GET_INFOV.php',
        data={'JSON64': encoded},
        headers=headers,
        timeout=25,
    )
    text = (resp.text or '').strip()
    if not text or text.startswith('err') or text.startswith('error'):
        return {'raw': text, 'error': _as_int(re.sub(r'\D+', '', text) or 0)}
    try:
        return json.loads(text)
    except Exception:
        return {'raw': text}


def _get_act9(
    sess: 'requests.Session',
    sid: str,
    t1: int,
    t2: int,
    mode: str,
    rng: str,
) -> Tuple[str, str]:
    q = [f'sid={sid}', 'act=9', 'f=1', f't_mode={mode}', f't_rg={rng}']
    if t1:
        q.append(f't_1={int(t1)}')
    if t2:
        q.append(f't_2={int(t2)}')
    query = '&'.join(q)
    last_html = ''
    last_url = ''
    for url in (f'{BASE}/viewstats/?{query}', f'https://static.histats.com/viewstats/?{query}'):
        resp = sess.get(url, timeout=20, allow_redirects=True)
        last_html = resp.text or ''
        last_url = str(resp.url or url)
        if 'OBJ_infovis' in last_html or '_init_from_JSON' in last_html:
            return last_html, last_url
    return last_html, last_url


def _extract_infovis_token(html: str) -> str:
    if not html:
        return ''
    m = re.search(r"OBJ_infovis\.sockTOKEN\s*=\s*'([^']+)'", html)
    if m:
        return m.group(1).strip()
    return _extract_token(html)


def _rank_change(rank_val: int, prev_rank: int) -> Dict[str, Any]:
    if not rank_val or not prev_rank or prev_rank >= 99999:
        return {'dir': '', 'pct': 0}
    pct = int((float(rank_val) / float(prev_rank)) * 100)
    if pct < 100:
        delta = (100 - pct) * -1
    else:
        delta = pct - 100
    direction = 'up' if delta > 0 else ('eq' if delta == 0 else 'dw')
    return {'dir': direction, 'pct': delta}


def _normalize_infovis(sid: str, payload: Any, meta: Dict[str, Any], section: str) -> Dict[str, Any]:
    root = payload.get('root') if isinstance(payload, dict) else {}
    if not isinstance(root, dict):
        root = {}
    date_stats = root.get('DateStats') if isinstance(root.get('DateStats'), dict) else {}
    date_key = ''
    if date_stats:
        date_key = sorted(date_stats.keys(), key=lambda k: _as_int(k))[-1]
    block = date_stats.get(date_key) if date_key else {}
    if not isinstance(block, dict):
        block = {}
    totals = block.get('totalsStats') if isinstance(block.get('totalsStats'), dict) else {}
    stats = block.get('STATS') if isinstance(block.get('STATS'), dict) else {}
    cat_stats = block.get('catStats') if isinstance(block.get('catStats'), dict) else {}
    key_totals = root.get('KeyTotalsStats') if isinstance(root.get('KeyTotalsStats'), dict) else {}
    now = int(time.time())
    rows: List[Dict[str, Any]] = []
    for key, row in stats.items():
        if not isinstance(row, dict):
            continue
        kh = _as_int(row.get('Kh') or key)
        name = str(row.get('name') or _INFOV_NAMES.get(kh) or '').strip() or ('-' if str(key) == '0' else str(key))
        visitors = _as_int(row.get('V'))
        pages = _as_int(row.get('PV'))
        new_vis = _as_int(row.get('a_NV'))
        ppv = round(_as_float(row.get('a_HV')) / 10.0, 1)
        tpp = _as_int(row.get('a_TPP'))
        bounce = round(_as_float(row.get('a_BB_RATE')) / 10.0, 1)
        rank = _rank_change(_as_int(row.get('rankVal')), _as_int(row.get('prevRankVal')))
        tstart = _as_int((key_totals.get(str(kh)) or {}).get('TStart') if isinstance(key_totals.get(str(kh)), dict) else 0)
        is_new = bool(tstart and (tstart * 86400) > (now - 604800))
        unique_bounce = round((_as_float(row.get('UB')) / visitors) * 100.0, 1) if visitors and _as_int(row.get('UB')) else 0.0
        rows.append({
            'id': kh,
            'name': name,
            'icon': _infovis_icon(section, name),
            'new_visitors': new_vis,
            'visitors': visitors,
            'pageviews': pages,
            'ppv': ppv,
            'tpp': tpp,
            'tpp_txt': _identa_seconds(tpp),
            'bounce': bounce,
            'bounce_ub': unique_bounce,
            'rank': _as_int(row.get('printPos')) + 1,
            'rank_dir': rank['dir'],
            'rank_pct': rank['pct'],
            'is_new': is_new,
            'freshness': max(0, now - (tstart * 86400)) if tstart else 0,
        })
    rows.sort(key=lambda item: (-item['visitors'], item['rank']))
    for idx, row in enumerate(rows, 1):
        row['rank'] = idx
    total_vis = _as_int(totals.get('V')) or sum(item['visitors'] for item in rows) or 1
    total_new = _as_int(totals.get('a_NV')) or sum(item['new_visitors'] for item in rows)
    total_pv = _as_int(totals.get('PV')) or sum(item['pageviews'] for item in rows)
    max_vis = max((item['visitors'] for item in rows), default=1) or 1
    for row in rows:
        row['share'] = round((row['visitors'] / total_vis) * 100.0, 1) if total_vis else 0.0
        row['bar'] = round((row['visitors'] / max_vis) * 100.0, 1) if max_vis else 0.0
        row['new_share'] = round((row['new_visitors'] / total_new) * 100.0, 1) if total_new else 0.0

    pies: List[Dict[str, Any]] = []
    for cat_name, cat_rows in cat_stats.items():
        items = []
        if isinstance(cat_rows, dict):
            nested_vals = list(cat_rows.values())
            keyed = bool(nested_vals) and all(isinstance(v, dict) and 'V' in v for v in nested_vals)
            if keyed:
                for label, item in cat_rows.items():
                    if not isinstance(item, dict):
                        continue
                    items.append({
                        'name': str(label),
                        'visitors': _as_int(item.get('V')),
                        'pageviews': _as_int(item.get('PV')),
                        'new_visitors': _as_int(item.get('a_NV')),
                    })
            else:
                source = nested_vals
                if source and isinstance(source[0], list):
                    source = source[0]
                for item in source if isinstance(source, list) else []:
                    if not isinstance(item, dict):
                        continue
                    cats = item.get('categories') if isinstance(item.get('categories'), dict) else {}
                    label = str(cats.get(cat_name) or item.get('name') or '').strip()
                    if not label:
                        continue
                    items.append({
                        'name': label,
                        'visitors': _as_int(item.get('V')),
                        'pageviews': _as_int(item.get('PV')),
                        'new_visitors': _as_int(item.get('a_NV')),
                    })
        elif isinstance(cat_rows, list):
            for item in cat_rows:
                if not isinstance(item, dict):
                    continue
                cats = item.get('categories') if isinstance(item.get('categories'), dict) else {}
                label = str(cats.get(cat_name) or item.get('name') or '').strip()
                if not label:
                    continue
                items.append({
                    'name': label,
                    'visitors': _as_int(item.get('V')),
                    'pageviews': _as_int(item.get('PV')),
                    'new_visitors': _as_int(item.get('a_NV')),
                })
        items.sort(key=lambda item: item['visitors'], reverse=True)
        if items:
            pies.append({'name': str(cat_name), 'items': items})
    if not pies and section == 'OS' and rows:
        grouped: Dict[str, int] = {}
        grouped_pv: Dict[str, int] = {}
        for row in rows:
            icon = row['icon']
            label = {
                'android': 'Android',
                'apple': 'Apple',
                'linux': 'Linux',
                'windows': 'Windows',
            }.get(icon, 'Other')
            grouped[label] = grouped.get(label, 0) + row['visitors']
            grouped_pv[label] = grouped_pv.get(label, 0) + row['pageviews']
        pies.append({
            'name': 'Os Type',
            'items': [
                {'name': name, 'visitors': vis, 'pageviews': grouped_pv.get(name, 0), 'new_visitors': 0}
                for name, vis in sorted(grouped.items(), key=lambda kv: kv[1], reverse=True)
            ],
        })
        device: Dict[str, int] = {'Mobile': 0, 'Computer': 0, 'Other': 0}
        for row in rows:
            name_l = row['name'].lower()
            if row['icon'] == 'android' or 'iphone' in name_l or 'ipad' in name_l or 'mobile' in name_l:
                device['Mobile'] += row['visitors']
            elif row['icon'] in ('windows', 'linux') or 'mac' in name_l:
                device['Computer'] += row['visitors']
            elif row['icon'] == 'apple':
                device['Mobile'] += row['visitors']
            else:
                device['Other'] += row['visitors']
        pies.append({
            'name': 'Device Type',
            'items': [
                {'name': name, 'visitors': vis, 'pageviews': 0, 'new_visitors': 0}
                for name, vis in device.items() if vis
            ],
        })

    return {
        'sid': sid,
        'domain': (meta or {}).get('domain') or '',
        'title': (meta or {}).get('title') or '',
        'section': section,
        'timezone': (meta or {}).get('timezone') or '',
        'site_time': _as_int((meta or {}).get('site_time')),
        'totals': {
            'visitors': total_vis if total_vis != 1 or rows else 0,
            'pageviews': total_pv,
            'new_visitors': total_new,
            'pages_per_visit': round(total_pv / total_vis, 2) if total_vis else 0.0,
        },
        'rows': rows,
        'pies': pies,
        'fetched_at': int(time.time()),
        'source': 'histats',
    }


def fetch_visitors(
    sid: str,
    section: str = 'OS',
    rng: str = 'h',
    t1: int = 0,
    t2: int = 0,
    mode: str = 'normal',
    sort: str = 'V',
    cookies: Optional[Any] = None,
    credentials: Optional[Dict[str, str]] = None,
) -> Tuple[Dict[str, Any], Any]:
    sid = str(sid or '').strip()
    if not sid:
        raise ValueError('Histats SID kosong')
    if requests is None:
        raise RuntimeError('The requests library is not installed')
    section = str(section or 'OS').strip().upper()
    if section not in _INFOV_SECTIONS:
        section = 'OS'
    rng = str(rng or 'h').strip().lower()
    if rng == 'm':
        # Visitors details Monthly = month bucket (t_rg=d). t_rg=m is empty on INFOV.
        rng = 'd'
    if rng not in ('h', 'd'):
        rng = 'h'
    mode = str(mode or 'normal').strip().lower()
    if mode not in ('normal', 'range', 'compare'):
        mode = 'normal'
    sort = str(sort or 'V').strip() or 'V'
    t1 = _as_int(t1)
    t2 = _as_int(t2)
    cache_key = f'{sid}:{section}:{rng}:{mode}:{t1}:{t2}:{sort}'
    now = time.time()
    cached = _infovis_cache.get(cache_key)
    if cached and (now - int(cached.get('fetched_at') or 0)) < _INFOV_TTL:
        return cached, cookies or []

    sess, authenticated, login_failed = _open_histats_session(cookies, credentials)
    try:
        _token, meta, _final_url = _get_token(sess, sid)
    except HistatsAuthError:
        _token, meta = '', {}

    charts = (meta or {}).get('charts') if isinstance(meta, dict) else {}
    site_time = _as_int((charts or {}).get('site_time')) or int(time.time())
    selected_ts = t1 or site_time
    fetch_t1 = _unix_month_begin(selected_ts) if rng == 'd' else _unix_day_begin(selected_ts)
    fetch_t2 = (_unix_month_begin(t2) if rng == 'd' else _unix_day_begin(t2)) if t2 and mode != 'normal' else 0
    html, last_url = _get_act9(sess, sid, fetch_t1, fetch_t2 if mode != 'normal' else 0, mode, rng)
    token = _extract_infovis_token(html)
    page = _parse_traffic_page(html)
    if page.get('site_time'):
        site_time = page['site_time']
        meta = dict(meta or {})
        meta['site_time'] = site_time
        meta['timezone'] = page.get('timezone') or meta.get('timezone')
        meta['domain'] = page.get('domain') or meta.get('domain')
        meta['title'] = page.get('title') or meta.get('title')
    t_o = '0'
    m = re.search(r'OBJ_SITE\.time_offset\s*=\s*([^;\s]+)', html or '')
    if m:
        t_o = m.group(1).strip()
    if not token:
        blob = f'{last_url}\n{html[:2000]}'
        if login_failed:
            raise HistatsAuthError('Login Histats gagal. Periksa email dan password akun Histats.', need_login=True)
        if authenticated:
            raise HistatsAuthError('Login Histats berhasil, tetapi Visitors details SID ini tidak bisa dibaca.', need_login=False)
        raise HistatsAuthError(
            'Gagal mengambil Visitors details. Login dulu dengan akun Histats pemilik SID ini.',
            need_login=True,
        )

    payload = {
        'filters': {'getCategory': 1, 'sectionsAR': [section]},
        'idcall': 1,
        'sid': str(sid),
        't_rg': rng,
        'req': 'SY',
        'PG_s': '0',
        'PG_e': '1000',
        'PG_so': sort,
        'PG_soa': '1',
        't_o': t_o,
        'trendly_key': '',
        'trendly': '',
        'gbfilter_range': '',
        'gbfilter_exp': '',
        'req_datatype': 'date_stats',
        't_mode': mode,
        't_1': str(fetch_t1),
        't_2': fetch_t2 if fetch_t2 else 0,
        'CC': token,
    }
    referer = f'{BASE}/viewstats/?sid={sid}&act=9&f=1'
    data = _post_infovis(sess, payload, referer)
    err = _as_int(data.get('error')) if isinstance(data, dict) else 0
    if err and err not in (13, 0):
        raise HistatsAuthError(
            'Histats menolak permintaan Visitors details. Login ulang atau coba SID lain.',
            need_login=err in (11, 12, 10003),
        )
    if not isinstance(data, dict) or not data.get('root'):
        if login_failed:
            raise HistatsAuthError('Login Histats gagal. Periksa email dan password akun Histats.', need_login=True)
        raise HistatsAuthError(
            'Histats tidak mengirim data Visitors details untuk tanggal ini.',
            need_login=not authenticated,
        )

    out = _normalize_infovis(sid, data, meta, section)
    cal_source = None
    month_begin = _unix_month_begin(selected_ts)
    try:
        cal_html, _ = _get_act3(sess, sid, month_begin, 0, 'normal', 'd')
        cal_page = _parse_traffic_page(cal_html)
        cal_source = _pick_block(cal_page.get('blocks') or [], 'd', month_begin)
    except Exception:
        cal_source = None
    out['calendar'] = _calendar_payload(selected_ts, selected_ts, _activity_map(cal_source), site_time)
    out['range'] = rng
    out['mode'] = mode
    out['t1'] = fetch_t1
    out['t2'] = fetch_t2
    out['heading'] = 'Visitors details'
    _infovis_cache[cache_key] = out
    return out, cookies_dump(sess)


_GEO_TTL = 12
_geoloc_cache: Dict[str, Dict[str, Any]] = {}
_GEO_SECTIONS = {'CO': 'country', 'CY': 'city'}


def _geo_name(value: Any) -> str:
    text = str(value or '').strip()
    if ';' in text:
        text = text.split(';', 1)[0].strip()
    return text or '-'


def _get_act10(
    sess: 'requests.Session',
    sid: str,
    t1: int,
    t2: int,
    mode: str,
    rng: str,
) -> Tuple[str, str]:
    q = [f'sid={sid}', 'act=10', 'f=1', f't_mode={mode}', f't_rg={rng}']
    if t1:
        q.append(f't_1={int(t1)}')
    if t2:
        q.append(f't_2={int(t2)}')
    query = '&'.join(q)
    last_html = ''
    last_url = ''
    for url in (f'{BASE}/viewstats/?{query}', f'https://static.histats.com/viewstats/?{query}'):
        resp = sess.get(url, timeout=20, allow_redirects=True)
        last_html = resp.text or ''
        last_url = str(resp.url or url)
        if 'OBJ_geoloc' in last_html or 'f_print_GEOLOC' in last_html:
            return last_html, last_url
    return last_html, last_url


def _extract_geoloc_token(html: str) -> str:
    if not html:
        return ''
    m = re.search(r"OBJ_geoloc\.sockTOKEN\s*=\s*'([^']+)'", html)
    if m:
        return m.group(1).strip()
    return _extract_token(html)


def _normalize_geoloc(sid: str, payload: Any, meta: Dict[str, Any], section: str) -> Dict[str, Any]:
    root = payload.get('root') if isinstance(payload, dict) else {}
    if not isinstance(root, dict):
        root = {}
    date_stats = root.get('DateStats') if isinstance(root.get('DateStats'), dict) else {}
    date_key = ''
    if date_stats:
        date_key = sorted(date_stats.keys(), key=lambda k: _as_int(k))[-1]
    block = date_stats.get(date_key) if date_key else {}
    if not isinstance(block, dict):
        block = {}
    totals = block.get('totalsStats') if isinstance(block.get('totalsStats'), dict) else {}
    stats = block.get('STATS') if isinstance(block.get('STATS'), dict) else {}
    key_totals = root.get('KeyTotalsStats') if isinstance(root.get('KeyTotalsStats'), dict) else {}
    now = int(time.time())
    rows: List[Dict[str, Any]] = []
    for key, row in stats.items():
        if not isinstance(row, dict):
            continue
        country = row.get('countryData') if isinstance(row.get('countryData'), dict) else {}
        city = row.get('cityData') if isinstance(row.get('cityData'), dict) else {}
        loc = city or country
        iso = str(loc.get('nat2Ch') or '').strip().upper()
        country_name = _geo_name(loc.get('nation'))
        city_name = _geo_name(loc.get('city')) if city else ''
        name = city_name if section == 'CY' else country_name
        kh = _as_int(row.get('Kh') or loc.get('coID') or key)
        visitors = _as_int(row.get('V'))
        pages = _as_int(row.get('PV'))
        new_vis = _as_int(row.get('a_NV'))
        ppv = round(_as_float(row.get('a_HV')) / 10.0, 1) if row.get('a_HV') is not None else 0.0
        tpp = _as_int(row.get('a_TPP'))
        bounce = round(_as_float(row.get('a_BB_RATE')) / 10.0, 1) if row.get('a_BB_RATE') is not None else 0.0
        rank = _rank_change(_as_int(row.get('rankVal')), _as_int(row.get('prevRankVal')))
        tstart = _as_int((key_totals.get(str(kh)) or {}).get('TStart') if isinstance(key_totals.get(str(kh)), dict) else 0)
        is_new = bool(tstart and (tstart * 86400) > (now - 604800))
        lat = _as_float(loc.get('lat'))
        lon = _as_float(loc.get('lon') if loc.get('lon') is not None else loc.get('lng'))
        rows.append({
            'id': kh,
            'name': name or '-',
            'city': city_name or '-',
            'country': country_name or '-',
            'iso': iso,
            'flag': iso.lower(),
            'state': str(loc.get('state') or '').strip(),
            'new_visitors': new_vis,
            'visitors': visitors,
            'pageviews': pages,
            'ppv': ppv,
            'tpp': tpp,
            'tpp_txt': _identa_seconds(tpp) if tpp else '-',
            'bounce': bounce,
            'lat': lat,
            'lon': lon,
            'rank': _as_int(row.get('printPos')) + 1,
            'rank_dir': rank['dir'],
            'rank_pct': rank['pct'],
            'is_new': is_new,
        })
    rows.sort(key=lambda item: (-item['visitors'], item['rank']))
    for idx, row in enumerate(rows, 1):
        row['rank'] = idx
    total_vis = _as_int(totals.get('V')) or sum(item['visitors'] for item in rows) or 1
    total_new = _as_int(totals.get('a_NV')) or sum(item['new_visitors'] for item in rows)
    total_pv = _as_int(totals.get('PV')) or sum(item['pageviews'] for item in rows)
    max_vis = max((item['visitors'] for item in rows), default=1) or 1
    for row in rows:
        row['share'] = round((row['visitors'] / total_vis) * 100.0, 1) if total_vis else 0.0
        row['bar'] = round((row['visitors'] / max_vis) * 100.0, 1) if max_vis else 0.0
        row['new_share'] = round((row['new_visitors'] / total_new) * 100.0, 1) if total_new else 0.0

    pie_items = [
        {'name': row['name'], 'iso': row['iso'], 'visitors': row['visitors'], 'pageviews': row['pageviews']}
        for row in rows[:8]
        if row['name'] and row['name'] != '-'
    ]
    leftover = sum(row['visitors'] for row in rows[8:])
    if leftover and pie_items:
        pie_items.append({'name': 'Other', 'iso': '', 'visitors': leftover, 'pageviews': 0})
    pies = [{'name': 'Geolocation', 'items': pie_items}] if pie_items else []
    points = [
        {
            'name': row['city'] if section == 'CY' else row['name'],
            'country': row['country'],
            'iso': row['iso'],
            'lat': row['lat'],
            'lon': row['lon'],
            'visitors': row['visitors'],
            'share': row['share'],
        }
        for row in rows
        if row['lat'] or row['lon']
    ]
    return {
        'sid': sid,
        'domain': (meta or {}).get('domain') or '',
        'title': (meta or {}).get('title') or '',
        'section': section,
        'timezone': (meta or {}).get('timezone') or '',
        'site_time': _as_int((meta or {}).get('site_time')),
        'totals': {
            'visitors': 0 if total_vis == 1 and not rows else total_vis,
            'pageviews': total_pv,
            'new_visitors': total_new,
        },
        'rows': rows,
        'pies': pies,
        'points': points,
        'fetched_at': int(time.time()),
        'source': 'histats',
    }


def fetch_geolocation(
    sid: str,
    section: str = 'CO',
    rng: str = 'h',
    t1: int = 0,
    t2: int = 0,
    mode: str = 'normal',
    sort: str = 'V',
    cookies: Optional[Any] = None,
    credentials: Optional[Dict[str, str]] = None,
) -> Tuple[Dict[str, Any], Any]:
    sid = str(sid or '').strip()
    if not sid:
        raise ValueError('Histats SID kosong')
    if requests is None:
        raise RuntimeError('The requests library is not installed')
    section = str(section or 'CO').strip().upper()
    if section not in _GEO_SECTIONS:
        section = 'CO'
    rng = str(rng or 'h').strip().lower()
    if rng == 'm':
        rng = 'd'
    if rng not in ('h', 'd'):
        rng = 'h'
    mode = str(mode or 'normal').strip().lower()
    if mode not in ('normal', 'range', 'compare'):
        mode = 'normal'
    sort = str(sort or 'V').strip() or 'V'
    t1 = _as_int(t1)
    t2 = _as_int(t2)
    cache_key = f'{sid}:{section}:{rng}:{mode}:{t1}:{t2}:{sort}'
    now = time.time()
    cached = _geoloc_cache.get(cache_key)
    if cached and (now - int(cached.get('fetched_at') or 0)) < _GEO_TTL:
        return cached, cookies or []

    sess, authenticated, login_failed = _open_histats_session(cookies, credentials)
    try:
        _token, meta, _final_url = _get_token(sess, sid)
    except HistatsAuthError:
        _token, meta = '', {}

    charts = (meta or {}).get('charts') if isinstance(meta, dict) else {}
    site_time = _as_int((charts or {}).get('site_time')) or int(time.time())
    selected_ts = t1 or site_time
    fetch_t1 = _unix_month_begin(selected_ts) if rng == 'd' else _unix_day_begin(selected_ts)
    fetch_t2 = (_unix_month_begin(t2) if rng == 'd' else _unix_day_begin(t2)) if t2 and mode != 'normal' else 0
    html, last_url = _get_act10(sess, sid, fetch_t1, fetch_t2 if mode != 'normal' else 0, mode, rng)
    token = _extract_geoloc_token(html)
    page = _parse_traffic_page(html)
    if page.get('site_time'):
        site_time = page['site_time']
        meta = dict(meta or {})
        meta['site_time'] = site_time
        meta['timezone'] = page.get('timezone') or meta.get('timezone')
        meta['domain'] = page.get('domain') or meta.get('domain')
        meta['title'] = page.get('title') or meta.get('title')
    t_o = '0'
    m = re.search(r'ptrDATE\.tOFFSET\s*=\s*([^;\s]+)', html or '')
    if not m:
        m = re.search(r'OBJ_SITE\.time_offset\s*=\s*([^;\s]+)', html or '')
    if m:
        t_o = m.group(1).strip()
    if not token:
        if login_failed:
            raise HistatsAuthError('Login Histats gagal. Periksa email dan password akun Histats.', need_login=True)
        if authenticated:
            raise HistatsAuthError('Login Histats berhasil, tetapi Geolocation SID ini tidak bisa dibaca.', need_login=False)
        raise HistatsAuthError(
            'Gagal mengambil Geolocation. Login dulu dengan akun Histats pemilik SID ini.',
            need_login=True,
        )

    payload = {
        'idcall': 0,
        'sid': str(sid),
        't_rg': rng,
        'req': section,
        'PG_s': '0',
        'PG_e': '1000',
        'PG_so': sort,
        'PG_soa': '1',
        't_o': t_o,
        'trendly_key': '',
        'trendly': '',
        'gbfilter_range': '',
        'gbfilter_exp': '',
        'req_datatype': 'date_stats',
        't_mode': mode,
        'filters': '',
        't_1': str(fetch_t1),
        't_2': fetch_t2 if fetch_t2 else 0,
        'CC': token,
    }
    referer = f'{BASE}/viewstats/?sid={sid}&act=10&f=1'
    data = _post_infovis(sess, payload, referer)
    err = _as_int(data.get('error')) if isinstance(data, dict) else 0
    if err and err not in (13, 0):
        raise HistatsAuthError(
            'Histats menolak permintaan Geolocation. Login ulang atau coba SID lain.',
            need_login=err in (11, 12, 10003),
        )
    if not isinstance(data, dict) or not data.get('root'):
        if login_failed:
            raise HistatsAuthError('Login Histats gagal. Periksa email dan password akun Histats.', need_login=True)
        raise HistatsAuthError(
            'Histats tidak mengirim data Geolocation untuk tanggal ini.',
            need_login=not authenticated,
        )

    out = _normalize_geoloc(sid, data, meta, section)
    cal_source = None
    month_begin = _unix_month_begin(selected_ts)
    try:
        cal_html, _ = _get_act3(sess, sid, month_begin, 0, 'normal', 'd')
        cal_page = _parse_traffic_page(cal_html)
        cal_source = _pick_block(cal_page.get('blocks') or [], 'd', month_begin)
    except Exception:
        cal_source = None
    out['calendar'] = _calendar_payload(selected_ts, selected_ts, _activity_map(cal_source), site_time)
    out['range'] = rng
    out['mode'] = mode
    out['t1'] = fetch_t1
    out['t2'] = fetch_t2
    out['heading'] = 'Geolocation'
    _geoloc_cache[cache_key] = out
    return out, cookies_dump(sess)


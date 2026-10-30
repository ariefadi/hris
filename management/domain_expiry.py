"""RDAP + whois.vu domain expiry lookup, ported from the Google Apps Script."""
from __future__ import annotations

import json
import time
from datetime import date, datetime, timedelta
from urllib.parse import quote

try:
    import requests
except Exception:
    requests = None

try:
    import tldextract
except Exception:
    tldextract = None

RDAP_BASE = 'https://rdap.org/domain/'
RDAP_ENDPOINTS = {
    'us': 'https://rdap.nic.us/domain/',
    'id': 'https://rdap.pandi.id/rdap/domain/',
    'com': 'https://rdap.verisign.com/com/v1/domain/',
    'net': 'https://rdap.verisign.com/net/v1/domain/',
    'org': 'https://rdap.publicinterestregistry.net/rdap/org/domain/',
    'my': 'https://rdap.mynicregistry.my/rdap/domain/',
}
WHOIS_VU_BASE = 'https://api.whois.vu/?q='
REQUEST_TIMEOUT = 12
SLEEP_SECONDS = 0.45
USER_AGENT = 'Kiwipixel-DomainExpiry/1.0'
RENEW_DAYS = 30
HEALTHY_DAYS = 90


def normalize_host(raw):
    text = str(raw or '').strip().lower()
    if not text:
        return ''
    text = text.replace('\\', '/')
    text = text.split('://')[-1]
    text = text.split('/')[0].split('?')[0].split('#')[0].split(':')[0]
    text = text.strip('.')
    if text.startswith('www.'):
        text = text[4:]
    if not text or ' ' in text or text in ('unknown', 'anonymous', 'n/a', '-'):
        return ''
    if all(part.isdigit() for part in text.split('.')):
        return ''
    return text


def registrable_domain(raw):
    host = normalize_host(raw)
    if not host:
        return ''
    if tldextract is not None:
        try:
            ext = tldextract.extract(host)
            if ext.domain and ext.suffix:
                return f'{ext.domain}.{ext.suffix}'.lower()
            return ''
        except Exception:
            pass
    parts = [p for p in host.split('.') if p]
    if len(parts) >= 2:
        return '.'.join(parts[-2:])
    return ''


def rdap_url(domain):
    tld = str(domain or '').rsplit('.', 1)[-1].lower()
    base = RDAP_ENDPOINTS.get(tld) or RDAP_BASE
    return base + quote(domain, safe='')


def _http_get_json(url):
    if requests is None:
        return 0, None, 'library requests tidak tersedia'
    try:
        resp = requests.get(
            url,
            timeout=REQUEST_TIMEOUT,
            allow_redirects=True,
            headers={'User-Agent': USER_AGENT, 'Accept': 'application/rdap+json, application/json'},
        )
        text = resp.text or ''
        data = None
        try:
            data = json.loads(text) if text else {}
        except Exception:
            data = None
        return resp.status_code, data, ''
    except Exception as exc:
        return 0, None, str(exc)[:200]


def parse_datetime(raw):
    if raw is None or raw == '':
        return None
    if isinstance(raw, datetime):
        return raw
    if isinstance(raw, date):
        return datetime(raw.year, raw.month, raw.day)
    if isinstance(raw, (int, float)) and raw > 100000000:
        try:
            return datetime.utcfromtimestamp(int(raw))
        except Exception:
            return None
    text = str(raw).strip()
    if text.isdigit() and len(text) >= 10:
        try:
            return datetime.utcfromtimestamp(int(text[:10]))
        except Exception:
            pass
    text = text.replace('Z', '+00:00')
    try:
        return datetime.fromisoformat(text).replace(tzinfo=None)
    except Exception:
        pass
    for fmt in ('%Y-%m-%d', '%Y-%m-%d %H:%M:%S', '%d-%b-%Y', '%Y.%m.%d', '%Y-%m-%dT%H:%M:%S'):
        try:
            return datetime.strptime(text[:19], fmt)
        except Exception:
            continue
    return None


def parse_date(raw):
    dt = parse_datetime(raw)
    return dt.date() if dt else None


def _fmt_dt(raw):
    dt = parse_datetime(raw)
    if not dt:
        return ''
    if dt.hour or dt.minute or dt.second:
        return dt.strftime('%Y-%m-%d %H:%M:%S')
    return dt.strftime('%Y-%m-%d')


def _vcard_value(vcard, key):
    rows = []
    if isinstance(vcard, list) and len(vcard) >= 2 and isinstance(vcard[1], list):
        rows = vcard[1]
    for item in rows:
        if isinstance(item, list) and item and str(item[0]).lower() == key and len(item) >= 4 and item[3]:
            return str(item[3]).replace('tel:', '')
    return ''


def _entity_info(ent):
    vcard = (ent or {}).get('vcardArray') or []
    links = (ent or {}).get('links') or []
    url = ''
    for link in links:
        if (link or {}).get('href'):
            url = str(link.get('href'))
            break
    iana = ''
    for pub in (ent or {}).get('publicIds') or []:
        if 'IANA' in str((pub or {}).get('type') or '').upper():
            iana = str((pub or {}).get('identifier') or '')
    abuse = {'email': '', 'phone': ''}
    for child in (ent or {}).get('entities') or []:
        if 'abuse' in ((child or {}).get('roles') or []):
            abuse['email'] = _vcard_value(child.get('vcardArray'), 'email')
            abuse['phone'] = _vcard_value(child.get('vcardArray'), 'tel')
    return {
        'name': _vcard_value(vcard, 'fn') or str((ent or {}).get('handle') or ''),
        'email': _vcard_value(vcard, 'email'),
        'phone': _vcard_value(vcard, 'tel'),
        'org': _vcard_value(vcard, 'org'),
        'url': url,
        'iana_id': iana,
        'abuse_email': abuse['email'],
        'abuse_phone': abuse['phone'],
        'roles': (ent or {}).get('roles') or [],
    }


def _parse_whois_text(text):
    out = {}
    mapping = {
        'creation date': 'registered_date',
        'created date': 'registered_date',
        'created on': 'registered_date',
        'registry expiry date': 'expired_date',
        'registrar registration expiration date': 'expired_date',
        'expiration date': 'expired_date',
        'updated date': 'last_changed',
        'last updated': 'last_changed',
        'registrar': 'registrar',
        'registrar url': 'registrar_url',
        'registrar iana id': 'registrar_iana',
        'registrar abuse contact email': 'abuse_email',
        'registrar abuse contact phone': 'abuse_phone',
        'registrar whois server': 'whois_server',
        'dnssec': 'dnssec',
        'registry domain id': 'handle',
        'registrant organization': 'registrant_org',
        'registrant name': 'registrant_name',
        'registrant email': 'registrant_email',
    }
    nameservers = []
    for line in str(text or '').splitlines():
        if ':' not in line:
            continue
        key, val = line.split(':', 1)
        key_n = key.strip().lower()
        val_n = val.strip()
        if not val_n:
            continue
        if key_n in ('name server', 'nserver'):
            nameservers.append(val_n.lower())
            continue
        mapped = mapping.get(key_n)
        if mapped and mapped not in out:
            out[mapped] = val_n
    if nameservers:
        out['nameservers'] = nameservers
    return out


def days_left(exp):
    exp_date = parse_date(exp)
    if not exp_date:
        return None
    return (exp_date - date.today()).days


def _registrar_from_rdap(data):
    for ent in data.get('entities') or []:
        roles = ent.get('roles') or []
        if 'registrar' not in roles:
            continue
        vcard = ent.get('vcardArray') or []
        if len(vcard) >= 2 and isinstance(vcard[1], list):
            for item in vcard[1]:
                if isinstance(item, list) and item and item[0] == 'fn' and len(item) >= 4 and item[3]:
                    return str(item[3])
        if ent.get('handle'):
            return str(ent.get('handle'))
    return ''


def _rdap_blob(data):
    title = str((data or {}).get('title') or '')
    desc = ' '.join(str(x) for x in ((data or {}).get('description') or []) if x)
    return (title + ' ' + desc).lower()


def _rdap_no_service(data):
    return 'no rdap service' in _rdap_blob(data)


def _rdap_unregistered(data):
    blob = _rdap_blob(data)
    return 'available for registration' in blob or blob.strip().startswith('not found')


def try_rdap(domain):
    code, data, err = _http_get_json(rdap_url(domain))
    if _rdap_no_service(data):
        return {
            'ok': False,
            'not_found': False,
            'no_service': True,
            'expired_date': None,
            'registrar': '',
            'status': '',
            'source': 'rdap',
            'note': 'RDAP tidak tersedia untuk TLD ini, lanjut WHOIS',
        }
    if code == 404 or _rdap_unregistered(data):
        return {
            'ok': False,
            'not_found': True,
            'expired_date': None,
            'registrar': '',
            'status': '',
            'source': 'rdap',
            'note': 'Domain tidak terdaftar (RDAP 404)',
        }
    if err:
        return {'ok': False, 'not_found': False, 'note': 'RDAP error: ' + err}
    if code != 200 or not isinstance(data, dict):
        return {'ok': False, 'not_found': False, 'note': f'RDAP HTTP {code or "-"}'}

    exp_raw = None
    for event in data.get('events') or []:
        action = str((event or {}).get('eventAction') or '').lower()
        if 'expir' in action and event.get('eventDate'):
            exp_raw = event.get('eventDate')
            break
    exp_date = parse_date(exp_raw)
    status = ''
    if isinstance(data.get('status'), list):
        status = ', '.join(str(x) for x in data.get('status') if x)
    return {
        'ok': bool(exp_date),
        'not_found': False,
        'expired_date': exp_date.isoformat() if exp_date else None,
        'registrar': _registrar_from_rdap(data),
        'status': status,
        'source': 'rdap',
        'note': '' if exp_date else 'RDAP tidak mengembalikan tanggal kadaluarsa',
    }


def try_whois_vu(domain):
    code, data, err = _http_get_json(WHOIS_VU_BASE + quote(domain, safe=''))
    if err:
        return {'ok': False, 'not_found': False, 'note': 'whois.vu error: ' + err}
    if code != 200 or not isinstance(data, dict):
        return {'ok': False, 'not_found': False, 'note': f'whois.vu HTTP {code or "-"}'}
    available = str(data.get('available') or '').strip().lower()
    whois_text = str(data.get('whois') or '')
    parsed = _parse_whois_text(whois_text)
    if available in ('yes', 'true', '1') or 'available for registration' in whois_text.lower():
        return {
            'ok': False,
            'not_found': True,
            'expired_date': None,
            'registrar': '',
            'status': '',
            'source': 'whois.vu',
            'note': 'Domain tidak terdaftar (WHOIS)',
        }
    exp_date = parse_date(
        data.get('expires') or data.get('expiration_date') or data.get('expiry') or parsed.get('expired_date') or ''
    )
    status = data.get('status') or data.get('statuses') or parsed.get('status') or ''
    if isinstance(status, list):
        status = ', '.join(str(x) for x in status if x)
    registrar = str(data.get('registrar') or parsed.get('registrar') or '')
    return {
        'ok': bool(exp_date),
        'not_found': False,
        'expired_date': exp_date.isoformat() if exp_date else None,
        'registrar': registrar,
        'status': str(status or ''),
        'source': 'whois.vu',
        'note': '' if exp_date else 'whois.vu tidak mengembalikan tanggal kadaluarsa',
    }


def fetch_expiry(domain):
    domain = registrable_domain(domain) or normalize_host(domain)
    empty = {
        'ok': False,
        'not_found': False,
        'domain': domain,
        'expired_date': None,
        'days_left': None,
        'registrar': '',
        'status': '',
        'source': '',
        'note': 'Domain kosong',
    }
    if not domain:
        return empty
    rdap = try_rdap(domain)
    chosen = rdap
    if not rdap.get('ok'):
        vu = try_whois_vu(domain)
        if vu.get('ok'):
            chosen = vu
        elif vu.get('not_found') or rdap.get('not_found'):
            chosen = {
                'ok': False,
                'not_found': True,
                'expired_date': None,
                'registrar': '',
                'status': '',
                'source': vu.get('source') or rdap.get('source') or 'rdap',
                'note': vu.get('note') or rdap.get('note') or 'Domain tidak terdaftar',
            }
        else:
            notes = [rdap.get('note'), vu.get('note')]
            chosen = {
                'ok': False,
                'not_found': False,
                'expired_date': None,
                'registrar': '',
                'status': '',
                'source': '',
                'note': 'RDAP & whois.vu gagal: ' + ' | '.join(x for x in notes if x),
            }
    result = {
        'ok': bool(chosen.get('ok')),
        'not_found': bool(chosen.get('not_found')),
        'domain': domain,
        'expired_date': chosen.get('expired_date'),
        'days_left': days_left(chosen.get('expired_date')) if chosen.get('expired_date') else None,
        'registrar': chosen.get('registrar') or '',
        'status': chosen.get('status') or '',
        'source': chosen.get('source') or '',
        'note': chosen.get('note') or '',
        'checked_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
    }
    if result['not_found'] and not result['note']:
        result['note'] = 'Domain tidak terdaftar (RDAP 404)'
    return result


def _empty_detail(domain, note=''):
    return {
        'ok': False,
        'not_found': False,
        'domain': domain,
        'handle': '',
        'registered_date': '',
        'expired_date': None,
        'last_changed': '',
        'last_rdap_update': '',
        'days_left': None,
        'age_years': None,
        'registrar': '',
        'registrar_url': '',
        'registrar_iana': '',
        'abuse_email': '',
        'abuse_phone': '',
        'status': '',
        'statuses': [],
        'nameservers': [],
        'dnssec': '',
        'premium': False,
        'registrant_org': '',
        'registrant_name': '',
        'source': '',
        'note': note,
        'price': '',
        'price_note': 'Harga beli domain tidak dipublikasikan di RDAP/WHOIS publik. Cek invoice registrar untuk nominalnya.',
        'whois_raw': '',
        'events': [],
        'domain_type': '',
        'whois_server': '',
        'tenure_years': None,
        'checked_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
    }


def fetch_domain_detail(domain):
    domain = registrable_domain(domain) or normalize_host(domain)
    detail = _empty_detail(domain, 'Domain kosong')
    if not domain:
        return detail
    detail['note'] = ''
    rdap_code, rdap_data, rdap_err = _http_get_json(rdap_url(domain))
    vu_code, vu_data, vu_err = _http_get_json(WHOIS_VU_BASE + quote(domain, safe=''))
    whois_text = ''
    if isinstance(vu_data, dict):
        whois_text = str(vu_data.get('whois') or '')
        parsed_whois = _parse_whois_text(whois_text)
        if vu_data.get('created'):
            parsed_whois.setdefault('registered_date', _fmt_dt(vu_data.get('created')))
        if vu_data.get('expires'):
            parsed_whois.setdefault('expired_date', parse_date(vu_data.get('expires')))
        if vu_data.get('updated'):
            parsed_whois.setdefault('last_changed', _fmt_dt(vu_data.get('updated')))
        if vu_data.get('registrar'):
            parsed_whois.setdefault('registrar', vu_data.get('registrar'))
        if isinstance(vu_data.get('statuses'), list):
            parsed_whois.setdefault('statuses', [str(x) for x in vu_data.get('statuses') if x])
        detail['whois_raw'] = whois_text[:4000]
        for key, val in parsed_whois.items():
            if val and not detail.get(key):
                detail[key] = val
        if vu_data.get('type'):
            detail['domain_type'] = str(vu_data.get('type') or '')
        if vu_code == 200:
            detail['source'] = 'whois.vu'
        vu_available = str(vu_data.get('available') or '').strip().lower()
        if vu_available in ('yes', 'true', '1') or 'available for registration' in whois_text.lower():
            detail['not_found'] = True
            detail['ok'] = False
            detail['note'] = 'Domain tidak terdaftar (WHOIS)'
            detail['source'] = 'whois.vu'
            detail['registered_date'] = ''
            detail['expired_date'] = None
            detail['registrar'] = ''
            detail['nameservers'] = []
            detail['statuses'] = []
            detail['status'] = ''
    rdap_unregistered = rdap_code == 404 or _rdap_unregistered(rdap_data)
    if rdap_unregistered and not _rdap_no_service(rdap_data):
        if detail.get('expired_date') or detail.get('registered_date'):
            detail['not_found'] = False
            detail['ok'] = True
            detail['note'] = ''
        else:
            detail['not_found'] = True
            detail['ok'] = False
            if not detail.get('note'):
                detail['note'] = 'Domain tidak terdaftar (RDAP 404)'
            if not detail.get('source'):
                detail['source'] = 'rdap'
            return detail
    if rdap_code == 200 and isinstance(rdap_data, dict) and not detail.get('not_found'):
        detail['ok'] = True
        detail['source'] = 'rdap'
        detail['handle'] = str(rdap_data.get('handle') or detail.get('handle') or '')
        detail['premium'] = bool(rdap_data.get('premiumDomain'))
        statuses = rdap_data.get('status') or []
        if isinstance(statuses, list):
            detail['statuses'] = [str(x) for x in statuses if x]
            detail['status'] = ', '.join(detail['statuses'])
        events = []
        for event in rdap_data.get('events') or []:
            action = str((event or {}).get('eventAction') or '')
            stamp = _fmt_dt((event or {}).get('eventDate'))
            events.append({'action': action, 'date': stamp})
            act = action.lower()
            if 'registr' in act:
                detail['registered_date'] = stamp
            elif 'expir' in act:
                detail['expired_date'] = parse_date((event or {}).get('eventDate'))
            elif 'last changed' in act or act == 'last changed':
                detail['last_changed'] = stamp
            elif 'last update of rdap' in act:
                detail['last_rdap_update'] = stamp
        detail['events'] = events
        detail['nameservers'] = [
            str((ns or {}).get('ldhName') or '').lower()
            for ns in (rdap_data.get('nameservers') or [])
            if (ns or {}).get('ldhName')
        ] or detail.get('nameservers') or []
        dnssec = rdap_data.get('secureDNS') or {}
        if isinstance(dnssec, dict):
            detail['dnssec'] = 'signed' if dnssec.get('delegationSigned') else 'unsigned'
        for ent in rdap_data.get('entities') or []:
            info = _entity_info(ent)
            roles = [str(r).lower() for r in info.get('roles') or []]
            if 'registrar' in roles:
                detail['registrar'] = info.get('name') or detail.get('registrar')
                detail['registrar_url'] = info.get('url') or detail.get('registrar_url')
                detail['registrar_iana'] = info.get('iana_id') or detail.get('registrar_iana')
                detail['abuse_email'] = info.get('abuse_email') or info.get('email') or detail.get('abuse_email')
                detail['abuse_phone'] = info.get('abuse_phone') or info.get('phone') or detail.get('abuse_phone')
            if 'registrant' in roles:
                detail['registrant_name'] = info.get('name') or detail.get('registrant_name')
                detail['registrant_org'] = info.get('org') or detail.get('registrant_org')
    elif rdap_err:
        detail['note'] = 'RDAP error: ' + rdap_err
    elif rdap_code and rdap_code != 200:
        detail['note'] = f'RDAP HTTP {rdap_code}' + ((' | ' + (vu_err or '')) if vu_err else '')

    if isinstance(detail.get('expired_date'), date) and not isinstance(detail.get('expired_date'), datetime):
        detail['expired_date'] = detail['expired_date'].isoformat()
    elif detail.get('expired_date') and not isinstance(detail.get('expired_date'), str):
        parsed = parse_date(detail.get('expired_date'))
        detail['expired_date'] = parsed.isoformat() if parsed else str(detail.get('expired_date'))
    else:
        parsed = parse_date(detail.get('expired_date'))
        detail['expired_date'] = parsed.isoformat() if parsed else (detail.get('expired_date') or '')

    registered = parse_date(detail.get('registered_date'))
    expired = parse_date(detail.get('expired_date'))
    if registered:
        detail['registered_date'] = registered.isoformat()
        detail['age_years'] = round((date.today() - registered).days / 365.25, 1)
    if registered and expired:
        detail['tenure_years'] = max(1, int(round((expired - registered).days / 365.25)))
    detail['days_left'] = days_left(expired) if expired else None
    if not detail.get('status') and detail.get('statuses'):
        detail['status'] = ', '.join(detail.get('statuses') or [])
    if detail.get('ok') or detail.get('expired_date') or detail.get('registered_date'):
        detail['ok'] = True
    detail['checked_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    return detail


def fetch_expiry_many(domains, pause=SLEEP_SECONDS):
    seen = []
    out = []
    for raw in domains or []:
        domain = registrable_domain(raw) or normalize_host(raw)
        if not domain or domain in seen:
            continue
        seen.append(domain)
        out.append(fetch_expiry(domain))
        if pause:
            time.sleep(pause)
    return out


def row_tone(days):
    if days is None:
        return 'unknown'
    try:
        num = int(days)
    except (TypeError, ValueError):
        return 'unknown'
    if num < 0:
        return 'expired'
    if num <= RENEW_DAYS:
        return 'soon'
    if num > HEALTHY_DAYS:
        return 'healthy'
    return 'watch'

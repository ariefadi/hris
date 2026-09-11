"""Utility untuk mendeteksi IP klien dan geolokasi berbasis IP."""

import ipaddress
import os
from typing import List, Optional, Tuple

import requests

try:
    import pycountry
except Exception:
    pycountry = None


def _is_public_ip(ip_str: str) -> bool:
    try:
        return ipaddress.ip_address((ip_str or '').strip()).is_global
    except ValueError:
        return False


def get_client_ip(request) -> str:
    """Ambil IP publik klien dari header proxy atau REMOTE_ADDR."""
    candidates: List[str] = []

    forwarded_for = request.META.get('HTTP_X_FORWARDED_FOR')
    if forwarded_for:
        candidates.extend(part.strip() for part in forwarded_for.split(',') if part.strip())

    for header in (
        'HTTP_CF_CONNECTING_IP',
        'HTTP_TRUE_CLIENT_IP',
        'HTTP_X_REAL_IP',
        'HTTP_X_CLIENT_IP',
        'HTTP_FORWARDED',
    ):
        value = request.META.get(header)
        if not value:
            continue
        if header == 'HTTP_FORWARDED' and 'for=' in value.lower():
            for segment in value.split(';'):
                segment = segment.strip()
                if segment.lower().startswith('for='):
                    ip_part = segment[4:].strip().strip('"').split(':')[0]
                    if ip_part:
                        candidates.append(ip_part)
        else:
            candidates.append(value.strip())

    remote_addr = (request.META.get('REMOTE_ADDR') or '').strip()
    if remote_addr:
        candidates.append(remote_addr)

    for ip in candidates:
        if _is_public_ip(ip):
            return ip

    return candidates[0] if candidates else ''


def _country_label(country_code: Optional[str]) -> Optional[str]:
    code = (country_code or '').strip()
    if not code:
        return None
    if len(code) > 2:
        return code
    if pycountry:
        try:
            country = pycountry.countries.get(alpha_2=code.upper())
            if country and country.name:
                return country.name
        except Exception:
            pass
    return code.upper()


def _format_location(city=None, region=None, country=None, postal=None) -> Optional[str]:
    parts = []
    for value in (city, region, _country_label(country)):
        text = (value or '').strip()
        if text and text not in parts:
            parts.append(text)
    if not parts:
        return None
    location = ', '.join(parts)
    postal_text = (postal or '').strip()
    if postal_text:
        location = f'{location} {postal_text}'
    return location


def _lookup_ipinfo(ip_address: str) -> Tuple[str, List[Optional[str]], Optional[str]]:
    token = os.getenv('IPINFO_TOKEN', '').strip()
    url = f'https://ipinfo.io/{ip_address}/json'
    headers = {'Authorization': f'Bearer {token}'} if token else None
    response = requests.get(url, headers=headers, timeout=5)
    response.raise_for_status()
    data = response.json()
    if not isinstance(data, dict):
        return ip_address, [None, None], None

    lat_long: List[Optional[str]] = [None, None]
    loc_val = data.get('loc')
    if loc_val:
        tmp = loc_val.split(',')
        if len(tmp) == 2:
            lat_long = [tmp[0].strip(), tmp[1].strip()]

    resolved_ip = data.get('ip', ip_address) or ip_address
    location = _format_location(
        city=data.get('city'),
        region=data.get('region'),
        country=data.get('country'),
        postal=data.get('postal'),
    )
    return resolved_ip, lat_long, location


def _lookup_ip_api(ip_address: str) -> Tuple[str, List[Optional[str]], Optional[str]]:
    response = requests.get(
        f'http://ip-api.com/json/{ip_address}',
        params={
            'fields': 'status,message,query,country,regionName,city,zip,lat,lon',
        },
        timeout=5,
    )
    response.raise_for_status()
    data = response.json()
    if not isinstance(data, dict) or data.get('status') != 'success':
        raise ValueError(data.get('message') or 'ip-api lookup failed')

    lat = data.get('lat')
    lon = data.get('lon')
    lat_long = [str(lat), str(lon)] if lat is not None and lon is not None else [None, None]
    location = _format_location(
        city=data.get('city'),
        region=data.get('regionName'),
        country=data.get('country'),
        postal=data.get('zip'),
    )
    return data.get('query', ip_address) or ip_address, lat_long, location


def resolve_ip_location(ip_address: str) -> Tuple[str, List[Optional[str]], Optional[str]]:
    """
    Lookup geolokasi berdasarkan IP klien.
    ip-api.com diprioritaskan karena lebih akurat untuk IP Indonesia;
    ipinfo.io hanya dipakai sebagai fallback.
    """
    lat_long: List[Optional[str]] = [None, None]
    if not ip_address or ip_address in ('127.0.0.1', '::1') or not _is_public_ip(ip_address):
        return ip_address, lat_long, None

    # ip-api lebih akurat untuk wilayah Indonesia (contoh: Klaten vs Boyolali)
    for lookup in (_lookup_ip_api, _lookup_ipinfo):
        try:
            return lookup(ip_address)
        except Exception:
            continue

    return ip_address, lat_long, None


def parse_browser_coords(lat_raw, lng_raw):
    try:
        lat = float(lat_raw)
        lng = float(lng_raw)
    except (TypeError, ValueError):
        return None
    if lat == 0 and lng == 0:
        return None
    if not (-90 <= lat <= 90 and -180 <= lng <= 180):
        return None
    return lat, lng


def reverse_geocode(lat: float, lng: float) -> Optional[str]:
    """Ubah koordinat GPS menjadi alamat (Nominatim / OpenStreetMap)."""
    try:
        response = requests.get(
            'https://nominatim.openstreetmap.org/reverse',
            params={
                'lat': lat,
                'lon': lng,
                'format': 'jsonv2',
                'accept-language': 'id',
                'zoom': 18,
                'addressdetails': 1,
            },
            headers={
                'User-Agent': 'Kiwipixel-HRIS/1.0 (login-location)',
                'Accept': 'application/json',
            },
            timeout=6,
        )
        response.raise_for_status()
        data = response.json()
    except Exception:
        return None
    if not isinstance(data, dict):
        return None

    address = data.get('address') or {}
    parts = []
    road = None
    for key in ('road', 'pedestrian', 'footway', 'residential'):
        text = str(address.get(key) or '').strip()
        if text:
            road = text
            break
    locality = None
    for key in ('village', 'hamlet', 'suburb', 'neighbourhood', 'city_district'):
        text = str(address.get(key) or '').strip()
        if text:
            locality = text
            break
    city = None
    for key in ('city', 'town', 'municipality', 'county'):
        text = str(address.get(key) or '').strip()
        if text:
            city = text
            break
    for text in (road, locality, city, str(address.get('state') or '').strip(), str(address.get('country') or '').strip()):
        if text and text not in parts:
            parts.append(text)
    if not parts:
        display = str(data.get('display_name') or '').strip()
        if not display:
            return None
        parts = [part.strip() for part in display.split(',') if part.strip()][:4]
    location = ', '.join(parts)
    postal = str(address.get('postcode') or '').strip()
    if postal and postal not in location:
        location = f'{location} {postal}'
    return location or None


def resolve_login_location(request, ip_address: str = ''):
    """
    Prioritaskan GPS browser (akurat), IP hanya cadangan.
    Return: (ip, [lat, lng], lokasi, source) dengan source 'gps' atau 'ip'.
    """
    ip_address = ip_address or get_client_ip(request)
    coords = coords_from_request(request)
    if coords:
        lat, lng = coords
        location = reverse_geocode(lat, lng)
        if not location:
            location = f'{lat:.5f}, {lng:.5f}'
        return ip_address, [str(lat), str(lng)], location, 'gps'

    resolved_ip, lat_long, location = resolve_ip_location(ip_address)
    return resolved_ip, lat_long, location, 'ip'


def coords_from_cookie(request):
    raw = str((request.COOKIES.get('hris_login_gps') or '')).strip()
    if not raw:
        return None
    parts = raw.split(',')
    if len(parts) != 2:
        return None
    return parse_browser_coords(parts[0].strip(), parts[1].strip())


def coords_from_request(request):
    lat = request.POST.get('latitude')
    lng = request.POST.get('longitude')
    content_type = (request.content_type or '').lower()
    if (not lat or not lng) and 'application/json' in content_type:
        try:
            import json
            body = request.body.decode('utf-8') if isinstance(request.body, (bytes, bytearray)) else (request.body or '')
            payload = json.loads(body) if body else {}
            lat = payload.get('latitude', lat)
            lng = payload.get('longitude', lng)
        except Exception:
            pass
    coords = parse_browser_coords(lat, lng)
    if coords:
        return coords
    return coords_from_cookie(request)

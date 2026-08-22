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

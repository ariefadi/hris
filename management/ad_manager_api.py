"""Auto-detect versi Google Ad Manager API yang masih aktif."""

import logging
import os
import re
from typing import List, Optional

import requests

logger = logging.getLogger(__name__)

CACHE_KEY = 'ad_manager_api_active_version'
CACHE_VERSIONS_KEY = 'ad_manager_api_active_versions'
CACHE_TTL_SECONDS = int(os.getenv('AD_MANAGER_API_CACHE_TTL', str(6 * 3600)))
WSDL_PROBE_TIMEOUT = int(os.getenv('AD_MANAGER_API_PROBE_TIMEOUT', '8'))
DEFAULT_ENDPOINT = 'https://ads.google.com'
PROBE_SERVICE = 'NetworkService'
FALLBACK_VERSION = os.getenv('AD_MANAGER_API_VERSION', 'v202508').strip() or 'v202508'


def _version_sort_key(version: str) -> tuple:
    match = re.match(r'v(\d{4})(\d{2})', version or '')
    if match:
        return (int(match.group(1)), int(match.group(2)))
    return (0, 0)


def _candidate_versions() -> List[str]:
    candidates: List[str] = []
    seen = set()

    def add(version: Optional[str]) -> None:
        version = (version or '').strip()
        if version and version not in seen:
            seen.add(version)
            candidates.append(version)

    env_versions = os.getenv('AD_MANAGER_API_VERSIONS', '').strip()
    if env_versions:
        for item in env_versions.split(','):
            add(item)

    add(os.getenv('AD_MANAGER_API_VERSION', '').strip())

    library_versions: List[str] = []
    try:
        from googleads import ad_manager

        library_versions = sorted(ad_manager._SERVICE_MAP.keys(), key=_version_sort_key)
        for version in library_versions:
            add(version)
    except Exception:
        pass

    if library_versions:
        match = re.match(r'v(\d{4})(\d{2})', library_versions[-1])
        if match:
            year = int(match.group(1))
            month = int(match.group(2))
            for _ in range(6):
                month += 1
                if month > 12:
                    month = 1
                    year += 1
                add(f'v{year}{month:02d}')

    add(FALLBACK_VERSION)
    return sorted(candidates, key=_version_sort_key, reverse=True)


def _probe_wsdl(version: str) -> bool:
    url = f'{DEFAULT_ENDPOINT}/apis/ads/publisher/{version}/{PROBE_SERVICE}?wsdl'
    try:
        response = requests.get(url, timeout=WSDL_PROBE_TIMEOUT)
        return response.status_code == 200 and len(response.text) > 1000
    except Exception as exc:
        logger.debug('WSDL probe failed for %s: %s', version, exc)
        return False


def discover_working_versions(max_versions: int = 3) -> List[str]:
    working: List[str] = []
    for version in _candidate_versions():
        if len(working) >= max_versions:
            break
        if _probe_wsdl(version):
            working.append(version)
    return working


def _read_cache() -> List[str]:
    try:
        from django.core.cache import cache

        cached = cache.get(CACHE_VERSIONS_KEY)
        if cached:
            return list(cached)
    except Exception:
        pass
    return []


def _write_cache(versions: List[str]) -> None:
    if not versions:
        return
    try:
        from django.core.cache import cache

        cache.set(CACHE_VERSIONS_KEY, versions, CACHE_TTL_SECONDS)
        cache.set(CACHE_KEY, versions[0], CACHE_TTL_SECONDS)
    except Exception:
        pass


def invalidate_ad_manager_api_cache() -> None:
    try:
        from django.core.cache import cache

        cache.delete(CACHE_KEY)
        cache.delete(CACHE_VERSIONS_KEY)
    except Exception:
        pass


def get_ad_manager_api_versions(force_refresh: bool = False) -> List[str]:
    if not force_refresh:
        cached = _read_cache()
        if cached:
            return cached

    working = discover_working_versions()
    if not working:
        working = [FALLBACK_VERSION]

    _write_cache(working)
    for version in working:
        ensure_ad_manager_service_map(version)
    return working


def get_ad_manager_api_version(force_refresh: bool = False) -> str:
    versions = get_ad_manager_api_versions(force_refresh=force_refresh)
    return versions[0]


def ensure_ad_manager_service_map(version: str) -> None:
    try:
        from googleads import ad_manager

        if version in ad_manager._SERVICE_MAP:
            return
        fallback = sorted(ad_manager._SERVICE_MAP.keys(), key=_version_sort_key)[-1]
        ad_manager._SERVICE_MAP[version] = ad_manager._SERVICE_MAP[fallback]
    except Exception:
        pass


def refresh_ad_manager_api_versions() -> dict:
    previous = _read_cache()
    invalidate_ad_manager_api_cache()
    versions = get_ad_manager_api_versions(force_refresh=True)
    return {
        'status': bool(versions),
        'previous_versions': previous,
        'primary_version': versions[0] if versions else None,
        'versions': versions,
        'changed': bool(previous and versions and previous[0] != versions[0]),
    }


def ensure_ad_manager_api_ready(force_refresh: bool = False) -> str:
    """Pastikan versi API aktif sudah ter-cache sebelum cron/pull data dijalankan."""
    return get_ad_manager_api_version(force_refresh=force_refresh)


def is_api_version_error(error: Exception) -> bool:
    message = str(error or '').lower()
    markers = (
        '404',
        'not found',
        'unrecognized version',
        'soaptransport',
        'wsdl',
        'networkservice?wsdl',
    )
    return any(marker in message for marker in markers)

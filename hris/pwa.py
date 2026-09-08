from pathlib import Path

from django.conf import settings
from django.http import HttpResponse, JsonResponse
from django.views.decorators.cache import never_cache
from django.views.decorators.http import require_GET


def _static_pwa(name):
    return Path(settings.BASE_DIR) / 'management' / 'static' / 'pwa' / name


@require_GET
def web_manifest(request):
    origin = request.build_absolute_uri('/').rstrip('/')
    payload = {
        'id': '/',
        'name': 'Kiwipixel Analytics',
        'short_name': 'Kiwipixel',
        'description': 'Portal admin analytics, monetisasi, dan manajemen iklan.',
        'start_url': '/management/admin/dashboard',
        'scope': '/',
        'display': 'standalone',
        'orientation': 'portrait-primary',
        'background_color': '#0f172a',
        'theme_color': '#4f46e5',
        'lang': 'id',
        'icons': [
            {
                'src': origin + '/static/pwa/icon-192.png',
                'sizes': '192x192',
                'type': 'image/png',
                'purpose': 'any',
            },
            {
                'src': origin + '/static/pwa/icon-512.png',
                'sizes': '512x512',
                'type': 'image/png',
                'purpose': 'any',
            },
            {
                'src': origin + '/static/pwa/icon-192.png',
                'sizes': '192x192',
                'type': 'image/png',
                'purpose': 'maskable',
            },
            {
                'src': origin + '/static/pwa/icon-512.png',
                'sizes': '512x512',
                'type': 'image/png',
                'purpose': 'maskable',
            },
        ],
    }
    response = JsonResponse(payload)
    response['Content-Type'] = 'application/manifest+json'
    response['Cache-Control'] = 'no-cache'
    return response


@never_cache
@require_GET
def favicon(request):
    path = _static_pwa('favicon.ico')
    response = HttpResponse(path.read_bytes(), content_type='image/x-icon')
    response['Cache-Control'] = 'public, max-age=3600'
    return response


@never_cache
@require_GET
def service_worker(request):
    path = _static_pwa('sw.js')
    response = HttpResponse(path.read_bytes(), content_type='application/javascript')
    response['Service-Worker-Allowed'] = '/'
    response['Cache-Control'] = 'no-cache'
    return response

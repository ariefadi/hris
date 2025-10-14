from threading import local
from django.conf import settings
from hris.settings import get_credentials_from_db
from django.http import HttpResponseRedirect
from django.urls import reverse
from django.contrib import messages
from django.conf import settings

_thread_locals = local()


def get_current_user_mail():
    """
    Fungsi helper untuk mendapatkan email user dari session
    """
    try:
        from django.core.handlers.wsgi import WSGIRequest
        from threading import current_thread
        request = getattr(current_thread(), '_current_request', None)
        if isinstance(request, WSGIRequest):
            return request.session.get('hris_admin', {}).get('user_mail')
    except:
        pass
    return None

class AuthMiddleware:
    def __init__(self, get_response):
        self.get_response = get_response

    def __call__(self, request):
        # Daftar path yang tidak perlu autentikasi
        excluded_paths = [
            reverse('admin_login'),
            reverse('admin_login_process'),
            '/accounts/login/google-oauth2/',
            '/accounts/complete/google-oauth2/',
            reverse('oauth_redirect'),
            '/management/admin/adx_sites_list'  # Allow AJAX requests for site filter
        ]
        
        # Cek apakah user sudah login
        if not request.session.get('hris_admin'):
            # Jika belum login dan bukan di halaman yang dikecualikan, redirect ke login
            if request.path not in excluded_paths:
                messages.warning(request, 'Silakan login terlebih dahulu')
                return HttpResponseRedirect(reverse('admin_login'))
        else:
            # Ambil user_id dan user_mail dari session
            user_id = request.session.get('hris_admin', {}).get('user_id')
            user_mail = request.session.get('hris_admin', {}).get('user_mail')

            # Update kredensial sebelum request diproses dengan parameter user_id dan user_mail
            request.oauth_user = {
                'user_id': user_id,
                'user_mail': user_mail
            }

        response = self.get_response(request)
        return response

class RequestMiddleware:
    def __init__(self, get_response):
        self.get_response = get_response

    def __call__(self, request):
        # Store request in thread local
        _thread_locals.request = request
        response = self.get_response(request)
        # Clean up
        if hasattr(_thread_locals, 'request'):
            del _thread_locals.request
        return response

class OAuthCredentialsMiddleware:
    def __init__(self, get_response):
        self.get_response = get_response

    def __call__(self, request):
        # Ambil user_id dan user_mail dari session
        user_id = request.session.get('hris_admin', {}).get('user_id')
        user_mail = request.session.get('hris_admin', {}).get('user_mail')
        
        # Update kredensial sebelum request diproses dengan parameter user_id dan user_mail
        request.oauth_user = {
            'user_id': user_id,
            'user_mail': user_mail
        }
        credentials = get_credentials_from_db(request)
        
        if credentials:
            # Update settings dengan kredensial dari database
            settings.GOOGLE_OAUTH2_CLIENT_ID = credentials['google_oauth2_client_id']
            settings.GOOGLE_OAUTH2_CLIENT_SECRET = credentials['google_oauth2_client_secret']
            settings.GOOGLE_ADS_CLIENT_ID = credentials['google_ads_client_id']
            settings.GOOGLE_ADS_CLIENT_SECRET = credentials['google_ads_client_secret']
            settings.GOOGLE_ADS_REFRESH_TOKEN = credentials['google_ads_refresh_token']
            settings.GOOGLE_AD_MANAGER_NETWORK_CODE = credentials['google_ad_manager_network_code']
        
        # Update social auth settings
        settings.SOCIAL_AUTH_GOOGLE_OAUTH2_KEY = settings.GOOGLE_OAUTH2_CLIENT_ID
        settings.SOCIAL_AUTH_GOOGLE_OAUTH2_SECRET = settings.GOOGLE_OAUTH2_CLIENT_SECRET

        # Pastikan Google OAuth meminta refresh token
        # Menambahkan parameter agar Google selalu mengembalikan refresh_token
        try:
            settings.SOCIAL_AUTH_GOOGLE_OAUTH2_AUTH_EXTRA_ARGUMENTS = {
                'access_type': 'offline',
                'prompt': 'consent',
                'include_granted_scopes': 'true'
            }
            # Scope tambahan jika diperlukan (mis. Google Ads)
            if not getattr(settings, 'SOCIAL_AUTH_GOOGLE_OAUTH2_SCOPE', None):
                settings.SOCIAL_AUTH_GOOGLE_OAUTH2_SCOPE = [
                    'openid', 'email', 'profile',
                    'https://www.googleapis.com/auth/adwords'
                ]
        except Exception:
            # Jangan blok request jika settings tidak bisa di-set
            pass

        response = self.get_response(request)
        return response
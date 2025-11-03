from threading import local
from django.conf import settings
from .credential_loader import get_credentials_from_db
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
            reverse('forgot_password'),
            '/accounts/login/google-oauth2/',
            '/accounts/complete/google-oauth2/',
            reverse('oauth_redirect'),
            '/management/admin/adx_sites_list',  # Allow AJAX requests for site filter
            # Allow utility endpoint to import env credentials without login
            reverse('app_credentials_import_env'),
            # Allow OAuth URL generation and callback without requiring session login
            reverse('generate_oauth_url_api'),
            reverse('oauth_callback_api')
        ]

        # Abaikan request untuk static/media/favicon/vite agar tidak men-trigger alert berulang
        excluded_prefixes = []
        try:
            excluded_prefixes.extend([
                getattr(settings, 'STATIC_URL', '/static/'),
                getattr(settings, 'MEDIA_URL', '/media/')
            ])
        except Exception:
            # Default prefix jika settings tidak tersedia
            excluded_prefixes.extend(['/static/', '/media/'])
        excluded_prefixes.extend(['/favicon.ico', '/@vite'])

        if any(request.path.startswith(prefix) for prefix in excluded_prefixes):
            return self.get_response(request)
        
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
        # Skip credential loading untuk OAuth login paths
        oauth_paths = [
            '/accounts/login/google-oauth2/',
            '/accounts/complete/google-oauth2/',
            '/management/admin/oauth_redirect'
        ]
        
        # Jika sedang dalam proses OAuth login, skip loading credentials
        if any(request.path.startswith(path) for path in oauth_paths):
            # Set default OAuth settings untuk proses login
            try:
                settings.SOCIAL_AUTH_GOOGLE_OAUTH2_AUTH_EXTRA_ARGUMENTS = {
                    'access_type': 'offline',
                    'prompt': 'select_account consent',
                    'include_granted_scopes': 'true'
                }
                # Scope tambahan jika diperlukan (mis. Google Ad Manager)
                if not getattr(settings, 'SOCIAL_AUTH_GOOGLE_OAUTH2_SCOPE', None):
                    settings.SOCIAL_AUTH_GOOGLE_OAUTH2_SCOPE = [
                        'openid', 'email', 'profile'
                    ]
            except Exception:
                # Jangan blok request jika settings tidak bisa di-set
                pass
            
            response = self.get_response(request)
            return response
        
        # Ambil user_id dan user_mail dari session
        user_id = request.session.get('hris_admin', {}).get('user_id')
        user_mail = request.session.get('hris_admin', {}).get('user_mail')
        
        # Hanya load credentials jika user sudah login
        if user_id and user_mail:
            # Update kredensial sebelum request diproses dengan parameter user_id dan user_mail
            request.oauth_user = {
                'user_id': user_id,
                'user_mail': user_mail
            }
            credentials = get_credentials_from_db(user_mail)
            
            if credentials:
                # Update settings dengan kredensial dari database
                settings.GOOGLE_OAUTH2_CLIENT_ID = credentials['client_id']
                settings.GOOGLE_OAUTH2_CLIENT_SECRET = credentials['client_secret']
                settings.GOOGLE_ADS_REFRESH_TOKEN = credentials['refresh_token']
                settings.GOOGLE_AD_MANAGER_NETWORK_CODE = credentials['network_code']
            
            # Update social auth settings
            settings.SOCIAL_AUTH_GOOGLE_OAUTH2_KEY = settings.GOOGLE_OAUTH2_CLIENT_ID
            settings.SOCIAL_AUTH_GOOGLE_OAUTH2_SECRET = settings.GOOGLE_OAUTH2_CLIENT_SECRET

        # Pastikan Google OAuth meminta refresh token
        # Menambahkan parameter agar Google selalu mengembalikan refresh_token
        try:
            settings.SOCIAL_AUTH_GOOGLE_OAUTH2_AUTH_EXTRA_ARGUMENTS = {
                'access_type': 'offline',
                'prompt': 'select_account consent',
                'include_granted_scopes': 'true'
            }
            # Scope tambahan jika diperlukan (mis. Google Ad Manager)
            if not getattr(settings, 'SOCIAL_AUTH_GOOGLE_OAUTH2_SCOPE', None):
                settings.SOCIAL_AUTH_GOOGLE_OAUTH2_SCOPE = [
                    'openid', 'email', 'profile'
                ]
        except Exception:
            # Jangan blok request jika settings tidak bisa di-set
            pass

        response = self.get_response(request)
        return response
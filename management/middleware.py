import time
from datetime import datetime
from threading import local

from django.conf import settings
from django.contrib import messages
from django.http import HttpResponseRedirect, JsonResponse
from django.urls import reverse

from .credential_loader import get_credentials_from_db

_thread_locals = local()

LAST_ACTIVITY_SESSION_KEY = 'hris_last_activity'
IDLE_KEEPALIVE_URL_NAMES = (
    'chat_heartbeat',
    'chat_messages',
)


def complete_admin_logout(request):
    """Catat logout_date di app_user_login, hapus presence chat, lalu flush session."""
    admin = request.session.get('hris_admin') or {}
    try:
        login_id = admin.get('login_id')
        if login_id:
            from .database import data_mysql
            data_mysql().update_login({
                'logout_date': datetime.now().strftime('%y-%m-%d %H:%M:%S'),
                'login_id': login_id,
            })
    except Exception as e:
        print(f"[ERROR] Gagal update data logout: {e}")
    try:
        uid = admin.get('user_id')
        if uid:
            from . import chat as chat_db
            from .database import data_mysql
            db_chat = data_mysql()
            chat_db.ensure_chat_tables(db_chat)
            chat_db.clear_presence(db_chat, uid)
            db_chat.close()
    except Exception as e:
        print(f"[ERROR] Gagal clear chat presence: {e}")
    request.session.flush()


def _idle_timeout_seconds():
    try:
        return max(60, int(getattr(settings, 'HRIS_IDLE_TIMEOUT_SECONDS', 900) or 900))
    except (TypeError, ValueError):
        return 900


def _is_ajax_request(request):
    if request.headers.get('X-Requested-With') == 'XMLHttpRequest':
        return True
    accept = (request.headers.get('Accept') or '').lower()
    if 'application/json' in accept and 'text/html' not in accept:
        return True
    content_type = (request.content_type or '').lower()
    return 'application/json' in content_type


def _url_name_path(name):
    try:
        return reverse(name)
    except Exception:
        return None


def _path_matches(request_path, target_path):
    if not target_path:
        return False
    path = (request_path or '').split('?')[0]
    return path == target_path or path.rstrip('/') == target_path.rstrip('/')


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

def find_menu_by_path(path):
    result = {
        'nav_id': None,
        'nav_url': '',
        'nav_name': '',
    }
    try:
        from .database import data_mysql
        db = data_mysql()
        q = """
            SELECT nav_id, nav_url, nav_name
            FROM app_menu
            WHERE nav_url IS NOT NULL AND nav_url <> ''
            ORDER BY LENGTH(nav_url) DESC
        """
        rows = []
        if db.execute_query(q):
            rows = db.cur_hris.fetchall() or []
        path_norm = '/' + (str(path or '').split('?')[0].lstrip('/').rstrip('/') or '')
        path_lower = path_norm.lower()
        for r in rows:
            try:
                url = r.get('nav_url') or ''
                nid = r.get('nav_id') or ''
                nname = r.get('nav_name') or ''
            except AttributeError:
                url = r[1]
                nid = r[0]
                nname = r[2] if len(r) > 2 else ''
            url = str(url or '').split('?')[0].strip()
            if url and not url.startswith('/'):
                url = '/' + url
            url_norm = (url.rstrip('/') or '/')
            url_lower = url_norm.lower()
            if url_lower and (path_lower == url_lower or path_lower.startswith(url_lower + '/')):
                result['nav_id'] = nid
                result['nav_url'] = url_norm
                result['nav_name'] = nname
                break
    except Exception:
        pass
    return result

class AuthMiddleware:
    def __init__(self, get_response):
        self.get_response = get_response
        self.logout_path = None
        self.keepalive_paths = []
        self._paths_ready = False

    def _ensure_paths(self):
        if self._paths_ready:
            return
        self.logout_path = _url_name_path('admin_logout')
        self.keepalive_paths = [
            path for path in (_url_name_path(name) for name in IDLE_KEEPALIVE_URL_NAMES) if path
        ]
        self._paths_ready = bool(self.logout_path)

    def _expire_idle_session(self, request):
        complete_admin_logout(request)
        login_url = reverse('admin_login') + '?timeout=1'
        if _is_ajax_request(request):
            return JsonResponse({
                'status': False,
                'code': 'idle_timeout',
                'error': 'Sesi berakhir karena tidak ada aktivitas selama 15 menit.',
            }, status=401)
        return HttpResponseRedirect(login_url)

    def __call__(self, request):
        self._ensure_paths()
        # Skip authentication for static files
        if request.path.startswith('/static/'):
            return self.get_response(request)
            
        # Daftar path yang tidak perlu autentikasi
        excluded_paths = [
            reverse('admin_login'),
            reverse('admin_login_process'),
            reverse('admin_register_account'),
            reverse('forgot_password'),
            '/accounts/login/google-oauth2/',
            '/accounts/complete/google-oauth2/',
            reverse('oauth_redirect'),
            '/management/admin/adx_sites_list',  # Allow AJAX requests for site filter
            '/management/admin/adsense_summary_data/',  # Allow AdSense summary data AJAX without redirect
            reverse('adsense_credentials_list'),  # Allow accounts list for AdSense filter
            reverse('ads_policy_events_credentials_list'),  # Allow accounts list for Meta Ads policy filter
            # Allow utility endpoint to import env credentials without login
            reverse('app_credentials_import_env'),
            # Allow OAuth URL generation and callback without requiring session login
            reverse('generate_oauth_url_api'),
            reverse('oauth_callback_api'),
            # Partner BM: kirim token tanpa session login admin
            reverse('facebook_partner_submit_token'),
            reverse('facebook_account_oauth_callback'),
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
            is_logout_request = _path_matches(request.path, self.logout_path)
            if not is_logout_request:
                last_activity = request.session.get(LAST_ACTIVITY_SESSION_KEY)
                try:
                    last_activity = float(last_activity) if last_activity is not None else None
                except (TypeError, ValueError):
                    last_activity = None
                now_ts = time.time()
                if last_activity is not None and (now_ts - last_activity) > _idle_timeout_seconds():
                    return self._expire_idle_session(request)
                is_keepalive = any(_path_matches(request.path, path) for path in self.keepalive_paths)
                if not is_keepalive:
                    request.session[LAST_ACTIVITY_SESSION_KEY] = now_ts

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

class AccessLogMiddleware:
    def __init__(self, get_response):
        self.get_response = get_response

    def __call__(self, request):
        admin = {}
        try:
            admin = dict(request.session.get('hris_admin') or {})
        except Exception:
            admin = {}
        response = self.get_response(request)
        try:
            from .activity_log import log_user_access
            log_user_access(request, response, admin)
        except Exception as e:
            print(f"[ERROR] Access log middleware: {e}")
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

class PermissionMiddleware:
    def __init__(self, get_response):
        self.get_response = get_response

    def __call__(self, request):
        path = request.path or ''
        
        if path.startswith('/static/') or path.startswith('/media/') or path == '/favicon.ico':
            return self.get_response(request)
        admin = request.session.get('hris_admin') or {}
        user_id = admin.get('user_id')
        user_mail = str(admin.get('user_mail') or '')
        
        try:
            req_user = getattr(request, 'user', None)
            if req_user and getattr(req_user, 'is_authenticated', False):
                if not user_mail:
                    user_mail = str(getattr(req_user, 'email', '') or '')
        except Exception:
            pass
        if not user_id and user_mail:
            try:
                from .database import data_mysql
                db = data_mysql()
                q_user = "SELECT user_id FROM app_users WHERE user_mail = %s LIMIT 1"
                if db.execute_query(q_user, (user_mail,)):
                    row = db.cur_hris.fetchone()
                    if row:
                        try:
                            user_id = row.get('user_id')
                        except AttributeError:
                            user_id = row[0]
            except Exception:
                pass
        if not user_id:
            return self.get_response(request)
        try:
            from .database import data_mysql
            db = data_mysql()
            nav_info = find_menu_by_path(path)
            nav_id = nav_info.get('nav_id')
            if not nav_id:
                request.menu_permissions = {'C': False, 'R': True, 'U': False, 'D': False, 'role_tp': '0000'}
                return self.get_response(request)
            roles = []
            if db.execute_query("SELECT role_id FROM app_user_role WHERE user_id=%s AND role_display='1'", (user_id,)):
                for rr in (db.cur_hris.fetchall() or []):
                    try:
                        roles.append(rr.get('role_id'))
                    except AttributeError:
                        roles.append(rr[0])
            role_tp = "0000"
            if roles:
                placeholders = ",".join(["%s"] * len(roles))
                sql = f"SELECT role_tp FROM app_menu_role WHERE nav_id=%s AND role_id IN ({placeholders})"
                params = (nav_id, *roles)
                if db.execute_query(sql, params):
                    rows = db.cur_hris.fetchall() or []
                    bits = [list(role_tp)]
                    for rr in rows:
                        try:
                            tp = (rr.get('role_tp') or '0000')
                        except AttributeError:
                            tp = rr[0] or '0000'
                        tp = (str(tp) + '0000')[:4]  # ensure length 4
                        bits.append(list(tp))
                    agg = ['0','0','0','0']
                    for b in bits:
                        for i in range(4):
                            agg[i] = '1' if (agg[i] == '1' or (b[i] == '1')) else '0'
                    role_tp = ''.join(agg)
            flags = {
                'C': role_tp[0] == '1',
                'R': role_tp[1] == '1',
                'U': role_tp[2] == '1',
                'D': role_tp[3] == '1',
                'role_tp': role_tp,
                'nav_id': nav_id
            }
            print(f"[PERM DEBUG] path={path} nav_id={nav_id} rle={role_tp}")
            request.menu_permissions = flags
            method = request.method.upper()
            need = 'R'
            if method in ['GET', 'HEAD', 'OPTIONS']:
                need = 'R'
            elif method == 'DELETE':
                need = 'D'
            elif method in ['PUT', 'PATCH']:
                need = 'U'
            else:
                pth = (path or '').lower()
                if 'delete' in pth or 'remove' in pth:
                    need = 'D'
                elif 'edit' in pth or 'update' in pth:
                    need = 'U'
                elif 'create' in pth or 'add' in pth or 'new' in pth:
                    need = 'C'
                else:
                    need = 'C'
            allowed = flags.get(need, False)
            print(f"[PERM DEBUG] path={path} nav_id={nav_id} rle={role_tp}")
            if not allowed:
                is_ajax = False
                try:
                    is_ajax = request.headers.get('X-Requested-With') == 'XMLHttpRequest'
                except Exception:
                    pass
                if is_ajax:
                    from django.http import JsonResponse
                    return JsonResponse({'status': False, 'error': 'Akses ditolak. Anda tidak memiliki izin untuk tindakan ini.'}, status=403)
                from django.http import HttpResponse
                return HttpResponse('Akses ditolak. Anda tidak memiliki izin untuk halaman ini.', status=403)
        except Exception:
            pass
        return self.get_response(request)

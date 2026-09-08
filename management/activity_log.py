import uuid
from datetime import datetime
from urllib.parse import urlparse

from .ip_utils import get_client_ip

_TABLE_READY = False

SKIP_PREFIXES = (
    '/static/',
    '/media/',
    '/favicon.ico',
    '/@vite',
    '/accounts/',
    '/sw.js',
    '/manifest.webmanifest',
)

SKIP_PATH_PARTS = (
    '/chat_heartbeat',
    '/chat_messages',
    '/session_duration',
    '/duration_activity/data',
    '/access_activity',
    '/pending_role_notifications',
    '/page_login_user',
    '/page_master_plan',
)

MUTATION_PATH_TOKENS = (
    'create', 'add', 'tambah', 'insert', 'new',
    'update', 'edit', 'ubah', 'save', 'simpan',
    'delete', 'remove', 'hapus',
    'download', 'export', 'excel', 'pdf',
    'send', 'forward',
)

ACTION_LABELS = {
    'login': 'Login',
    'logout': 'Logout',
    'view': 'Buka Menu',
    'create': 'Tambah Data',
    'update': 'Ubah Data',
    'delete': 'Hapus Data',
    'export': 'Unduh Data',
    'switch_portal': 'Ganti Portal',
    'other': 'Aktivitas Lain',
}

GENERIC_PATH_SLUGS = frozenset({
    'admin', 'management', 'settings', 'page', 'index', 'data', 'view',
    'list', 'detail', 'process', 'ajax', 'api',
})

PATH_PARENT_HINTS = {
    'update_daily_budget_monitoring_domain_campaign': 'dashboard',
    'update_campaign_status_monitoring_domain_campaign': 'dashboard',
}


def ensure_access_log_table(db=None):
    global _TABLE_READY
    if _TABLE_READY:
        return True
    close_db = False
    if db is None:
        from .database import data_mysql
        db = data_mysql()
        close_db = True
    sql = """
        CREATE TABLE IF NOT EXISTS app_user_access_log (
            log_id VARCHAR(36) NOT NULL,
            user_id VARCHAR(36) NOT NULL,
            login_id VARCHAR(36) DEFAULT NULL,
            activity_time DATETIME NOT NULL,
            method VARCHAR(10) NOT NULL,
            action_type VARCHAR(20) NOT NULL,
            path VARCHAR(500) NOT NULL,
            nav_id VARCHAR(36) DEFAULT NULL,
            menu_name VARCHAR(255) DEFAULT NULL,
            description VARCHAR(500) NOT NULL,
            ip_address VARCHAR(100) DEFAULT NULL,
            user_agent VARCHAR(500) DEFAULT NULL,
            status_code SMALLINT DEFAULT NULL,
            is_ajax TINYINT(1) NOT NULL DEFAULT 0,
            PRIMARY KEY (log_id),
            KEY idx_access_user_time (user_id, activity_time),
            KEY idx_access_time (activity_time),
            KEY idx_access_action (action_type),
            KEY idx_access_nav (nav_id)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """
    ok = bool(db.execute_query(sql))
    if ok:
        try:
            db.commit()
        except Exception:
            pass
        _TABLE_READY = True
    if close_db:
        try:
            db.close()
        except Exception:
            pass
    return ok


def _is_ajax(request):
    if request.headers.get('X-Requested-With') == 'XMLHttpRequest':
        return True
    accept = (request.headers.get('Accept') or '').lower()
    return 'application/json' in accept and 'text/html' not in accept


def _is_background_load(request):
    path_l = (request.path or '').lower()
    method = (request.method or 'GET').upper()
    is_ajax = _is_ajax(request)
    if 'dashboard_' in path_l:
        return True
    if method == 'GET' and is_ajax:
        return True
    if is_ajax and method in ('POST', 'PUT', 'PATCH'):
        if any(token in path_l for token in MUTATION_PATH_TOKENS):
            return False
        return True
    return False


def _should_skip(request, response):
    method = (request.method or 'GET').upper()
    if method in ('HEAD', 'OPTIONS'):
        return True
    path = (request.path or '').split('?')[0]
    path_l = path.lower()
    for prefix in SKIP_PREFIXES:
        if path_l.startswith(prefix) or path_l == prefix.rstrip('/'):
            return True
    for part in SKIP_PATH_PARTS:
        if part in path_l:
            return True
    if _is_background_load(request):
        return True
    status = getattr(response, 'status_code', 200) or 200
    if status < 200 or status >= 400:
        return True
    content_type = ''
    try:
        content_type = (response.get('Content-Type') or '').lower()
    except Exception:
        content_type = ''
    if method == 'GET' and 'application/json' in content_type:
        return True
    return False


def _path_label(path):
    parts = [p for p in (path or '').split('/') if p]
    if not parts:
        return 'Halaman'
    return parts[-1].replace('_', ' ').replace('-', ' ').title()


def _switch_portal_id(path):
    parts = [p for p in (path or '').split('/') if p]
    if len(parts) >= 2 and parts[-2] == 'switch_portal':
        return parts[-1]
    return None


def _portal_name(portal_id, db=None):
    if not portal_id:
        return None
    close_db = False
    try:
        if db is None:
            from .database import data_mysql
            db = data_mysql()
            close_db = True
        sql = '''
            SELECT COALESCE(NULLIF(portal_title, ''), NULLIF(portal_nm, ''), portal_id) AS portal_name
            FROM app_portal
            WHERE portal_id = %s
            LIMIT 1
        '''
        if db.execute_query(sql, (portal_id,)):
            row = db.cur_hris.fetchone() or {}
            if isinstance(row, dict):
                return row.get('portal_name')
            if row:
                return row[0]
    except Exception:
        return None
    finally:
        if close_db:
            try:
                db.close()
            except Exception:
                pass
    return None


def _infer_action(request):
    method = (request.method or 'GET').upper()
    path_l = (request.path or '').lower()
    if 'switch_portal' in path_l:
        return 'switch_portal'
    if 'logout' in path_l:
        return 'logout'
    if 'login' in path_l and method == 'POST':
        return 'login'
    if any(token in path_l for token in ('download', 'export', 'excel', 'pdf')):
        return 'export'
    if method == 'DELETE' or any(token in path_l for token in ('delete', 'remove', 'hapus')):
        return 'delete'
    if method in ('POST', 'PUT', 'PATCH'):
        if any(token in path_l for token in ('create', 'add', 'tambah', 'insert', 'new')):
            return 'create'
        if any(token in path_l for token in ('update', 'edit', 'ubah', 'save', 'simpan')):
            return 'update'
        return 'update'
    if method == 'GET':
        return 'view'
    return 'other'


def _normalize_menu_path(path):
    return '/' + (str(path or '').split('?')[0].lstrip('/').rstrip('/') or '')


def _last_slug(path):
    parts = [p for p in _normalize_menu_path(path).split('/') if p]
    return (parts[-1] if parts else '').lower()


def _menu_from_exact_path(path, catalog=None):
    if catalog:
        path_lower = _normalize_menu_path(path).lower()
        best = None
        best_len = -1
        for item in catalog:
            url = str(item.get('nav_url') or '').split('?')[0].strip()
            if url and not url.startswith('/'):
                url = '/' + url
            url_lower = (url.rstrip('/') or '/').lower()
            if url_lower and (path_lower == url_lower or path_lower.startswith(url_lower + '/')):
                if len(url_lower) > best_len:
                    best = item
                    best_len = len(url_lower)
        if best:
            return best['nav_id'], best['nav_name']
        return None, None
    try:
        from .middleware import find_menu_by_path
        info = find_menu_by_path(path)
        if info.get('nav_name'):
            return info.get('nav_id'), info.get('nav_name')
        return info.get('nav_id'), None
    except Exception:
        return None, None


def _load_menu_catalog(db=None):
    close_db = False
    rows = []
    try:
        if db is None:
            from .database import data_mysql
            db = data_mysql()
            close_db = True
        sql = '''
            SELECT nav_id, nav_url, nav_name
            FROM app_menu
            WHERE nav_url IS NOT NULL AND nav_url <> ''
        '''
        if db.execute_query(sql):
            rows = db.cur_hris.fetchall() or []
    except Exception:
        rows = []
    finally:
        if close_db:
            try:
                db.close()
            except Exception:
                pass
    catalog = []
    for row in rows:
        try:
            nav_id = row.get('nav_id')
            nav_url = row.get('nav_url') or ''
            nav_name = row.get('nav_name') or ''
        except AttributeError:
            nav_id = row[0] if row else None
            nav_url = row[1] if len(row) > 1 else ''
            nav_name = row[2] if len(row) > 2 else ''
        slug = _last_slug(nav_url)
        if not slug or not nav_name:
            continue
        catalog.append({
            'nav_id': nav_id,
            'nav_url': nav_url,
            'nav_name': nav_name,
            'slug': slug,
        })
    return catalog


def _menu_from_catalog(path, catalog):
    slug = _last_slug(path)
    if not slug or not catalog:
        return None, None
    hint = PATH_PARENT_HINTS.get(slug)
    if hint:
        for item in catalog:
            if item['slug'] == hint or item['slug'].endswith(hint):
                return item['nav_id'], item['nav_name']
        return None, hint.replace('_', ' ').title()
    best = None
    best_len = 0
    for item in catalog:
        menu_slug = item['slug']
        if not menu_slug or menu_slug in GENERIC_PATH_SLUGS or len(menu_slug) < 4:
            continue
        if menu_slug in slug or slug in menu_slug:
            if len(menu_slug) > best_len:
                best = item
                best_len = len(menu_slug)
    if best:
        return best['nav_id'], best['nav_name']
    return None, None


def _resolve_menu(path, referer_request=None, catalog=None):
    if catalog is None:
        catalog = _load_menu_catalog()
    nav_id, menu_name = _menu_from_exact_path(path, catalog)
    if menu_name:
        return nav_id, menu_name
    if referer_request is not None:
        try:
            referer = referer_request.META.get('HTTP_REFERER') or ''
        except Exception:
            referer = ''
        ref_path = urlparse(referer).path if referer else ''
        if ref_path:
            ref_id, ref_name = _menu_from_exact_path(ref_path, catalog)
            if ref_name:
                return ref_id, ref_name
    return _menu_from_catalog(path, catalog)


def _menu_info(request):
    path = request.path or ''
    path_l = path.lower()
    if 'logout' in path_l:
        return None, 'Logout'
    if '/login' in path_l or path_l.endswith('login'):
        return None, 'Login'
    portal_id = _switch_portal_id(path)
    if portal_id:
        portal_name = _portal_name(portal_id) or f'Portal {portal_id}'
        return None, portal_name
    nav_id = None
    try:
        flags = getattr(request, 'menu_permissions', None) or {}
        nav_id = flags.get('nav_id')
    except Exception:
        nav_id = None
    resolved_id, menu_name = _resolve_menu(path, referer_request=request)
    nav_id = resolved_id or nav_id
    if nav_id and not menu_name:
        try:
            from .database import data_mysql
            db = data_mysql()
            if db.execute_query('SELECT nav_name FROM app_menu WHERE nav_id=%s LIMIT 1', (nav_id,)):
                row = db.cur_hris.fetchone() or {}
                menu_name = row.get('nav_name') if isinstance(row, dict) else (row[0] if row else None)
            db.close()
        except Exception:
            pass
    return nav_id, menu_name


def _description(action, menu_name, path):
    label = menu_name or _path_label(path)
    if action == 'login':
        return 'Login ke aplikasi'
    if action == 'logout':
        return 'Logout dari aplikasi'
    if action == 'switch_portal':
        return f'Beralih ke portal {label}'
    if action == 'view':
        return f'Membuka menu {label}'
    if action == 'create':
        return f'Menambah data di {label}'
    if action == 'update':
        return f'Mengedit data di {label}'
    if action == 'delete':
        return f'Menghapus data di {label}'
    if action == 'export':
        return f'Mengunduh data {label}'
    return f'Mengakses {label}'


def repair_switch_portal_logs(db=None):
    close_db = False
    try:
        if db is None:
            from .database import data_mysql
            db = data_mysql()
            close_db = True
        sql = '''
            UPDATE app_user_access_log a
            INNER JOIN app_portal p
                ON a.path = CONCAT('/management/admin/switch_portal/', p.portal_id)
                OR a.path = CONCAT('/management/admin/switch_portal/', p.portal_id, '/')
            SET
                a.action_type = 'switch_portal',
                a.menu_name = COALESCE(NULLIF(p.portal_title, ''), NULLIF(p.portal_nm, ''), p.portal_id),
                a.description = CONCAT(
                    'Beralih ke portal ',
                    COALESCE(NULLIF(p.portal_title, ''), NULLIF(p.portal_nm, ''), p.portal_id)
                )
            WHERE a.path LIKE '%/switch_portal/%'
        '''
        if db.execute_query(sql):
            db.commit()
    except Exception as e:
        print(f"[ERROR] Gagal memperbaiki log ganti portal: {e}")
    finally:
        if close_db:
            try:
                db.close()
            except Exception:
                pass


def repair_login_logout_logs(db=None):
    close_db = False
    try:
        if db is None:
            from .database import data_mysql
            db = data_mysql()
            close_db = True
        updates = [
            (
                '''
                UPDATE app_user_access_log
                SET menu_name = 'Logout',
                    action_type = 'logout',
                    description = 'Logout dari aplikasi'
                WHERE path LIKE %s
                ''',
                ('%/logout%',),
            ),
            (
                '''
                UPDATE app_user_access_log
                SET menu_name = 'Login',
                    action_type = 'login',
                    description = 'Login ke aplikasi'
                WHERE path LIKE %s AND method = 'POST'
                ''',
                ('%/login%',),
            ),
            (
                '''
                UPDATE app_user_access_log
                SET menu_name = 'Login',
                    description = 'Membuka menu Login'
                WHERE path LIKE %s
                  AND (method = 'GET' OR method IS NULL)
                ''',
                ('%/login%',),
            ),
        ]
        for sql, params in updates:
            db.execute_query(sql, params)
        db.commit()
    except Exception as e:
        print(f"[ERROR] Gagal memperbaiki log login/logout: {e}")
    finally:
        if close_db:
            try:
                db.close()
            except Exception:
                pass


def repair_missing_menu_logs(db=None):
    close_db = False
    try:
        if db is None:
            from .database import data_mysql
            db = data_mysql()
            close_db = True
        catalog = _load_menu_catalog(db)
        sql = '''
            SELECT log_id, path, action_type
            FROM app_user_access_log
            WHERE (menu_name IS NULL OR menu_name = '' OR menu_name = '-')
              AND path NOT LIKE %s
              AND path NOT LIKE %s
              AND path NOT LIKE %s
        '''
        rows = []
        if db.execute_query(sql, ('%/login%', '%/logout%', '%/switch_portal/%')):
            rows = db.cur_hris.fetchall() or []
        resolved = {}
        for row in rows:
            try:
                log_id = row.get('log_id')
                path = row.get('path') or ''
                action = row.get('action_type') or 'other'
            except AttributeError:
                log_id = row[0]
                path = row[1] if len(row) > 1 else ''
                action = row[2] if len(row) > 2 else 'other'
            if path not in resolved:
                resolved[path] = _resolve_menu(path, catalog=catalog)
            nav_id, menu_name = resolved[path]
            if not menu_name:
                continue
            db.execute_query(
                '''
                UPDATE app_user_access_log
                SET menu_name = %s, nav_id = COALESCE(%s, nav_id), description = %s
                WHERE log_id = %s
                ''',
                (
                    (menu_name or '')[:255],
                    nav_id,
                    _description(action, menu_name, path)[:500],
                    log_id,
                ),
            )
        db.commit()
    except Exception as e:
        print(f"[ERROR] Gagal memperbaiki nama menu log: {e}")
    finally:
        if close_db:
            try:
                db.close()
            except Exception:
                pass


def cleanup_background_access_logs(db=None):
    close_db = False
    try:
        if db is None:
            from .database import data_mysql
            db = data_mysql()
            close_db = True
        sql = '''
            DELETE FROM app_user_access_log
            WHERE path LIKE %s
        '''
        if db.execute_query(sql, ('%/dashboard_%',)):
            db.commit()
    except Exception as e:
        print(f"[ERROR] Gagal membersihkan log background: {e}")
    finally:
        if close_db:
            try:
                db.close()
            except Exception:
                pass


def log_user_access(request, response, admin=None):
    admin = admin or {}
    session_admin = {}
    try:
        session_admin = request.session.get('hris_admin') or {}
    except Exception:
        session_admin = {}
    user_id = session_admin.get('user_id') or admin.get('user_id')
    if not user_id:
        return
    if _should_skip(request, response):
        return
    if not ensure_access_log_table():
        return

    action = _infer_action(request)
    nav_id, menu_name = _menu_info(request)
    path = (request.path or '')[:500]
    agent = (request.META.get('HTTP_USER_AGENT') or '')[:500]
    try:
        ip_address = get_client_ip(request)
    except Exception:
        ip_address = request.META.get('REMOTE_ADDR') or ''

    from .database import data_mysql
    db = data_mysql()
    try:
        sql = """
            INSERT INTO app_user_access_log (
                log_id, user_id, login_id, activity_time, method, action_type,
                path, nav_id, menu_name, description, ip_address, user_agent,
                status_code, is_ajax
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        db.execute_query(sql, (
            str(uuid.uuid4()),
            user_id,
            session_admin.get('login_id') or admin.get('login_id'),
            datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            (request.method or 'GET')[:10],
            action,
            path,
            nav_id,
            (menu_name or '')[:255] or None,
            _description(action, menu_name, path)[:500],
            (ip_address or '')[:100] or None,
            agent or None,
            getattr(response, 'status_code', None),
            1 if _is_ajax(request) else 0,
        ))
        db.commit()
    except Exception as e:
        print(f"[ERROR] Gagal catat access log: {e}")
    finally:
        try:
            db.close()
        except Exception:
            pass

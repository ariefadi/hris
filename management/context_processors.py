from django.conf import settings
from django.urls import reverse

from .database import data_mysql

_PLACEHOLDER_FOTO = {
    '',
    'none',
    'null',
    'default_avatar.png',
    'default.png',
    'user2-160x160.jpg',
}


def resolve_user_foto_url(foto):
    text = str(foto or '').strip()
    if not text or text.lower() in _PLACEHOLDER_FOTO:
        return ''
    if text.startswith(('http://', 'https://', '/')):
        return text
    media_url = str(getattr(settings, 'MEDIA_URL', '/media/') or '/media/')
    if not media_url.endswith('/'):
        media_url += '/'
    return media_url + text.lstrip('/')


def ensure_session_user_foto(request, db=None):
    """Isi user_foto di session dari database jika belum ada, lalu return URL siap pakai."""
    admin = request.session.get('hris_admin') or {}
    user_id = admin.get('user_id')
    if not user_id:
        return ''

    foto = admin.get('user_foto') if 'user_foto' in admin else None
    if foto is None:
        try:
            db = db or data_mysql()
            if db.execute_query(
                'SELECT user_foto FROM app_users WHERE user_id = %s LIMIT 1',
                (user_id,),
            ):
                row = db.cur_hris.fetchone() or {}
                foto = row.get('user_foto') if isinstance(row, dict) else ''
        except Exception:
            foto = ''
        try:
            updated = dict(admin)
            updated['user_foto'] = foto or ''
            request.session['hris_admin'] = updated
            request.session.modified = True
            admin = updated
        except Exception:
            admin = dict(admin)
            admin['user_foto'] = foto or ''

    return resolve_user_foto_url(admin.get('user_foto'))


def _idle_timeout_context():
    try:
        seconds = int(getattr(settings, 'HRIS_IDLE_TIMEOUT_SECONDS', 900) or 900)
    except (TypeError, ValueError):
        seconds = 900
    return {'hris_idle_timeout_seconds': max(60, seconds)}


def nav_context(request):
    """
    Provide global context for portals and menus based on the logged-in user.
    - Sets/uses `active_portal_id` in session
    - Exposes `global_portals`, `global_portal_menus`, `active_portal_id`
    """
    idle_ctx = _idle_timeout_context()
    admin = request.session.get('hris_admin') or {}
    user_id = admin.get('user_id')
    idle_ctx['hris_session_login_at'] = admin.get('login_date') or ''
    idle_ctx['header_user_foto'] = ''
    try:
        idle_ctx['hris_session_duration_url'] = reverse('users_session_duration')
        idle_ctx['hris_duration_activity_url'] = reverse('users_duration_activity')
    except Exception:
        idle_ctx['hris_session_duration_url'] = '/settings/users/session_duration'
        idle_ctx['hris_duration_activity_url'] = '/settings/users/duration_activity'
    if not user_id:
        return idle_ctx

    db = data_mysql()
    idle_ctx['header_user_foto'] = ensure_session_user_foto(request, db=db)

    # Fetch accessible portals for the user via role -> role_menu -> menu -> portal
    portals = []
    try:
        q_portals = '''
            SELECT DISTINCT p.portal_id,
               COALESCE(p.portal_title, p.portal_nm) AS portal_title,
               p.portal_icon
        FROM app_user_role ur
        JOIN app_menu_role rm ON rm.role_id = ur.role_id
        JOIN app_menu m ON m.nav_id = rm.nav_id
        JOIN app_portal p ON p.portal_id = m.portal_id
        WHERE ur.user_id = %s AND m.display_st = '1' AND m.active_st = '1'
        ORDER BY portal_title
        '''
        if db.execute_query(q_portals, (user_id,)):
            portals = db.cur_hris.fetchall() or []
    except Exception:
        portals = []

    # Determine active portal id
    active_portal_id = request.session.get('active_portal_id')
    if not active_portal_id and portals:
        active_portal_id = portals[0].get('portal_id')
        try:
            request.session['active_portal_id'] = active_portal_id
        except Exception:
            pass

    # Fetch menus for the active portal
    menus = []
    try:
        if active_portal_id:
            q_menus = '''
                SELECT DISTINCT m.nav_id, m.nav_parent, m.nav_name, m.nav_url, m.nav_icon, m.nav_order
                FROM app_user_role ur
                JOIN app_menu_role rm ON rm.role_id = ur.role_id
                JOIN app_menu m ON m.nav_id = rm.nav_id
                WHERE ur.user_id = %s AND m.portal_id = %s AND m.display_st = '1' AND m.active_st = '1'
                ORDER BY COALESCE(m.nav_order, 999), m.nav_name ASC
            '''
            if db.execute_query(q_menus, (user_id, active_portal_id)):
                menus = db.cur_hris.fetchall() or []
    except Exception:
        menus = []

    # Build tree structure from flat menu list
    by_id = {m['nav_id']: {**m, 'children': []} for m in menus}
    roots = []
    for m in menus:
        parent = (m.get('nav_parent') or '').strip()
        if parent and parent in by_id:
            by_id[parent]['children'].append(by_id[m['nav_id']])
        else:
            roots.append(by_id[m['nav_id']])

    return {
        **idle_ctx,
        'global_portals': portals,
        'global_portal_menus': roots,
        'active_portal_id': active_portal_id,
    }
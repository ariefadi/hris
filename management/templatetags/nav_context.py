from django import template
from django.utils.safestring import mark_safe
from management.database import data_mysql

register = template.Library()

def _get_user_id_from_context(context):
    try:
        request = context.get('request')
        if request and request.session.get('hris_admin'):
            return request.session['hris_admin'].get('user_id')
    except Exception:
        pass
    # Fallback: some templates pass `user` dict
    user = context.get('user') or {}
    return user.get('user_id')

def _fetch_user_portals(user_id):
    """Return list of portals accessible to the user via role->menu->portal relations."""
    if not user_id:
        return []
    db = data_mysql()
    sql = '''
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
    try:
        if not db.execute_query(sql, (user_id,)):
            return []
        return db.cur_hris.fetchall() or []
    except Exception:
        return []

def _fetch_portal_menus(user_id, portal_id):
    """Return flat list of menus for given portal accessible to the user."""
    if not user_id or not portal_id:
        return []
    db = data_mysql()
    sql = '''
        SELECT m.nav_id, m.nav_name, m.nav_url, m.nav_icon, m.nav_parent, m.nav_order
        FROM app_user_role ur
        JOIN app_menu_role rm ON rm.role_id = ur.role_id
        JOIN app_menu m ON m.nav_id = rm.nav_id
        WHERE ur.user_id = %s AND m.portal_id = %s AND m.display_st = '1' AND m.active_st = '1'
        ORDER BY COALESCE(m.nav_order, 999), m.nav_name
    '''
    try:
        if not db.execute_query(sql, (user_id, portal_id)):
            return []
        return db.cur_hris.fetchall() or []
    except Exception:
        return []

@register.simple_tag(takes_context=True)
def active_portal_id(context):
    """Return current active portal id from session, or first available portal id."""
    request = context.get('request')
    user_id = _get_user_id_from_context(context)
    current = None
    if request:
        current = request.session.get('active_portal_id')
    if current:
        return current
    portals = _fetch_user_portals(user_id)
    return portals[0]['portal_id'] if portals else ''

@register.simple_tag(takes_context=True)
def user_portals(context):
    """Return list of accessible portals for the current user."""
    user_id = _get_user_id_from_context(context)
    return _fetch_user_portals(user_id)

@register.simple_tag(takes_context=True)
def portal_menus(context, portal_id=None):
    """Return list of accessible menus for the specified or active portal."""
    user_id = _get_user_id_from_context(context)
    if not portal_id:
        portal_id = active_portal_id(context)
    return _fetch_portal_menus(user_id, portal_id)

# Recursive rendering for multi-level menus
@register.inclusion_tag('admin/menu_tree.html')
def render_menu_tree(items, current_path='/', level=1, max_depth=3):
    """Render a nested menu tree with active/open states up to max_depth levels."""
    def mark_states(node):
        url = (node.get('nav_url') or '').strip()
        own_active = ('/' + url) in (current_path or '') if url else False
        children = node.get('children') or []
        any_child_open = False
        for child in children:
            child_open = mark_states(child)
            if child_open or child.get('is_active'):
                any_child_open = True
        node['is_active'] = own_active
        node['is_open'] = own_active or any_child_open
        return node['is_open']

    for item in items or []:
        mark_states(item)
    return {
        'items': items or [],
        'current_path': current_path or '',
        'level': level or 1,
        'max_depth': max_depth or 3,
    }
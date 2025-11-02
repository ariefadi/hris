from .database import data_mysql

def nav_context(request):
    """
    Provide global context for portals and menus based on the logged-in user.
    - Sets/uses `active_portal_id` in session
    - Exposes `global_portals`, `global_portal_menus`, `active_portal_id`
    """
    admin = request.session.get('hris_admin') or {}
    user_id = admin.get('user_id')
    if not user_id:
        return {}

    db = data_mysql()

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
        'global_portals': portals,
        'global_portal_menus': roots,
        'active_portal_id': active_portal_id,
    }
from django.views import View
from django.shortcuts import render, redirect
from django.http import HttpResponseBadRequest
from django.contrib import messages
from django.utils.decorators import method_decorator
from django.views.decorators.csrf import csrf_exempt
from settings.database import data_mysql
from datetime import datetime

class Overview(View):
    def get(self, request):
        if 'hris_admin' not in request.session:
            return redirect('admin_login')
        admin = request.session.get('hris_admin', {})
        active_portal_id = request.session.get('active_portal_id', '12')
        context = {
            'user': admin,
            'active_portal_id': active_portal_id,
        }
        return render(request, 'overview.html', context)

# Helpers

def generate_next_portal_id(db: data_mysql):
    """Generate the next portal_id filling gaps: 10,20,...,90. If 20 is deleted, next is 20."""
    try:
        sql = """
            SELECT DISTINCT LEFT(portal_id, 1) AS n
            FROM app_portal
            ORDER BY n ASC
        """
        if not db.execute_query(sql):
            return None
        rows = db.cur_hris.fetchall() or []
        existing = set()
        for r in rows:
            try:
                val = int(str(r.get('n', '')).strip())
                if 1 <= val <= 9:
                    existing.add(val)
            except (ValueError, TypeError):
                continue
        for i in range(1, 10):
            if i not in existing:
                return f"{i}0"
        return None
    except Exception:
        return None


def generate_next_group_id(db: data_mysql):
    """Generate next group_id as two digits: '01', '02', ..., '98'. Returns None if >=99 or error."""
    try:
        sql = """
            SELECT group_id AS last_number
            FROM app_group
            ORDER BY group_id DESC
            LIMIT 1
        """
        if not db.execute_query(sql):
            return None
        row = db.cur_hris.fetchone()
        if row and row.get('last_number'):
            try:
                number = int(str(row['last_number']).strip()) + 1
            except (ValueError, TypeError):
                return None
            if number >= 99:
                return None
            num_str = str(number)
            zero = ''
            for i in range(len(num_str), 2):
                zero += '0'
            return f"{zero}{num_str}"
        else:
            return '01'
    except Exception:
        return None


def generate_next_nav_id(db: data_mysql, portal_id: str):
    """Generate the next unique nav_id as `<portal_id><8-digit>`.
    - Computes numeric max of the rightmost 8 digits per portal.
    - Ensures the generated candidate does not already exist; bumps until free.
    Note: Not concurrency-proof across processes; the insert code should still retry on duplicate.
    """
    try:
        portal_id = (portal_id or '').strip()
        if not portal_id:
            return None
        sql_max = """
            SELECT MAX(CAST(RIGHT(nav_id, 8) AS UNSIGNED)) AS last_number
            FROM app_menu
            WHERE portal_id = %s
        """
        if not db.execute_query(sql_max, (portal_id,)):
            number = 1
        else:
            row = db.cur_hris.fetchone() or {}
            last_number = row.get('last_number')
            try:
                number = (int(last_number) + 1) if last_number is not None else 1
            except (ValueError, TypeError):
                number = 1
        if number > 99999999:
            return None
        while True:
            candidate = f"{portal_id}{number:08d}"
            sql_exists = 'SELECT nav_id FROM app_menu WHERE nav_id = %s LIMIT 1'
            if not db.execute_query(sql_exists, (candidate,)):
                return candidate
            exists_row = db.cur_hris.fetchone()
            if not exists_row:
                return candidate
            number += 1
            if number > 99999999:
                return None
    except Exception:
        return None


def generate_next_role_id(db: data_mysql, group_id: str):
    """Generate next role_id scoped by group_id, like: '01' + '001' → '01001'."""
    try:
        group_id = (group_id or '').strip()
        if not group_id:
            return None
        sql = """
            SELECT RIGHT(role_id, 3) AS last_number
            FROM app_role
            WHERE group_id = %s
            ORDER BY role_id DESC
            LIMIT 1
        """
        if not db.execute_query(sql, (group_id,)):
            return f"{group_id}001"
        row = db.cur_hris.fetchone()
        if row and row.get('last_number') is not None:
            try:
                number = int(str(row['last_number']).strip()) + 1
            except (ValueError, TypeError):
                number = 1
        else:
            number = 1
        if number > 999:
            return None
        return f"{group_id}{number:03d}"
    except Exception:
        return None


# Sistem / Portal
class PortalIndexView(View):
    def get(self, request):
        if 'hris_admin' not in request.session:
            return redirect('admin_login')
        admin = request.session.get('hris_admin', {})
        active_portal_id = request.session.get('active_portal_id', '12')
        db = data_mysql()
        sql = """
            SELECT portal_id, portal_nm, site_title, site_desc, meta_keyword, meta_desc
            FROM app_portal
            ORDER BY portal_id ASC
        """
        portals = []
        if db.execute_query(sql):
            portals = db.cur_hris.fetchall() or []
        context = {
            'user': admin,
            'active_portal_id': active_portal_id,
            'portals': portals,
        }
        return render(request, 'sistem/portal/index.html', context)


class PortalCreateView(View):
    def post(self, request):
        if 'hris_admin' not in request.session:
            return redirect('admin_login')
        admin = request.session.get('hris_admin', {})
        db = data_mysql()
        portal_id = generate_next_portal_id(db)
        if not portal_id:
            messages.error(request, 'Gagal membuat portal: batas ID tercapai atau terjadi kesalahan.')
            return redirect('/settings/sistem/portal')

        portal_nm = request.POST.get('portal_nm', '').strip()
        site_title = request.POST.get('site_title', '').strip()
        site_desc = request.POST.get('site_desc', '').strip()
        meta_keyword = request.POST.get('meta_keyword', '').strip()
        meta_desc = request.POST.get('meta_desc', '').strip()

        if not portal_nm:
            messages.error(request, 'Nama Portal wajib diisi.')
            return redirect('/settings/sistem/portal')

        sql = """
            INSERT INTO app_portal
            (portal_id, portal_nm, portal_title, site_title, site_desc, meta_keyword, meta_desc, mdb, mdb_name, mdd)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
        """
        params = (
            portal_id,
            portal_nm,
            portal_nm,
            site_title,
            site_desc,
            meta_keyword,
            meta_desc,
            admin.get('user_id', ''),
            admin.get('user_alias', ''),
        )
        if db.execute_query(sql, params):
            db.commit()
            messages.success(request, 'Portal berhasil dibuat.')
        else:
            messages.error(request, 'Gagal membuat portal.')
        return redirect('/settings/sistem/portal')


class PortalEditView(View):
    def get(self, request, portal_id):
        if 'hris_admin' not in request.session:
            return redirect('admin_login')
        admin = request.session.get('hris_admin', {})
        active_portal_id = request.session.get('active_portal_id', '12')
        db = data_mysql()
        sql = """
            SELECT portal_id, portal_nm, site_title, site_desc, meta_keyword, meta_desc
            FROM app_portal
            WHERE portal_id = %s
            LIMIT 1
        """
        portal = None
        if db.execute_query(sql, (portal_id,)):
            portal = db.cur_hris.fetchone()
        if not portal:
            return redirect('/settings/sistem/portal')
        context = {
            'user': admin,
            'active_portal_id': active_portal_id,
            'portal': portal,
        }
        return render(request, 'sistem/portal/edit.html', context)

    def post(self, request, portal_id):
        if 'hris_admin' not in request.session:
            return redirect('admin_login')
        admin = request.session.get('hris_admin', {})
        db = data_mysql()
        portal_nm = request.POST.get('portal_nm', '').strip()
        site_title = request.POST.get('site_title', '').strip()
        site_desc = request.POST.get('site_desc', '').strip()
        meta_keyword = request.POST.get('meta_keyword', '').strip()
        meta_desc = request.POST.get('meta_desc', '').strip()
        if not portal_nm:
            messages.error(request, 'Nama Portal wajib diisi.')
            return redirect(f'/settings/sistem/portal/{portal_id}/edit')
        sql = """
            UPDATE app_portal SET
                portal_nm = %s,
                portal_title = %s,
                site_title = %s,
                site_desc = %s,
                meta_keyword = %s,
                meta_desc = %s,
                mdb = %s,
                mdb_name = %s,
                mdd = NOW()
            WHERE portal_id = %s
        """
        params = (
            portal_nm,
            portal_nm,
            site_title,
            site_desc,
            meta_keyword,
            meta_desc,
            admin.get('user_id', ''),
            admin.get('user_alias', ''),
            portal_id,
        )
        if db.execute_query(sql, params):
            db.commit()
            messages.success(request, 'Portal berhasil diperbarui.')
            return redirect('/settings/sistem/portal')
        else:
            messages.error(request, 'Gagal memperbarui portal.')
            return redirect(f'/settings/sistem/portal/{portal_id}/edit')


class PortalDeleteView(View):
    def post(self, request, portal_id):
        if 'hris_admin' not in request.session:
            return redirect('admin_login')
        db = data_mysql()
        sql = "DELETE FROM app_portal WHERE portal_id = %s"
        if db.execute_query(sql, (portal_id,)):
            db.commit()
            messages.success(request, 'Portal berhasil dihapus.')
        else:
            messages.error(request, 'Gagal menghapus portal.')
        return redirect('/settings/sistem/portal')


# Sistem / Groups
class GroupsIndexView(View):
    def get(self, request):
        if 'hris_admin' not in request.session:
            return redirect('admin_login')
        admin = request.session.get('hris_admin', {})
        active_portal_id = request.session.get('active_portal_id', '12')
        db = data_mysql()
        sql = """
            SELECT group_id, group_name, group_desc
            FROM app_group
            ORDER BY group_id ASC
        """
        groups = []
        if db.execute_query(sql):
            groups = db.cur_hris.fetchall() or []
        context = {
            'user': admin,
            'active_portal_id': active_portal_id,
            'groups': groups,
        }
        return render(request, 'sistem/groups/index.html', context)


class GroupsCreateView(View):
    def post(self, request):
        if 'hris_admin' not in request.session:
            return redirect('admin_login')
        admin = request.session.get('hris_admin', {})
        db = data_mysql()
        group_id = generate_next_group_id(db)
        if not group_id:
            messages.error(request, 'Gagal membuat group: batas ID tercapai atau terjadi kesalahan.')
            return redirect('/settings/sistem/groups')
        group_name = request.POST.get('group_name', '').strip()
        group_desc = request.POST.get('group_desc', '').strip()
        if not group_name:
            messages.error(request, 'Nama Group wajib diisi.')
            return redirect('/settings/sistem/groups')
        sql = """
            INSERT INTO app_group
            (group_id, group_name, group_desc, mdb, mdb_name, mdd)
            VALUES (%s, %s, %s, %s, %s, NOW())
        """
        params = (
            group_id,
            group_name,
            group_desc,
            admin.get('user_id', ''),
            admin.get('user_alias', ''),
        )
        if db.execute_query(sql, params):
            db.commit()
            messages.success(request, 'Group berhasil dibuat.')
        else:
            messages.error(request, 'Gagal membuat group.')
        return redirect('/settings/sistem/groups')


class GroupsEditView(View):
    def get(self, request, group_id):
        if 'hris_admin' not in request.session:
            return redirect('admin_login')
        admin = request.session.get('hris_admin', {})
        active_portal_id = request.session.get('active_portal_id', '12')
        db = data_mysql()
        sql = """
            SELECT group_id, group_name, group_desc
            FROM app_group
            WHERE group_id = %s
            LIMIT 1
        """
        group = None
        if db.execute_query(sql, (group_id,)):
            group = db.cur_hris.fetchone()
        if not group:
            return redirect('/settings/sistem/groups')
        context = {
            'user': admin,
            'active_portal_id': active_portal_id,
            'group': group,
        }
        return render(request, 'sistem/groups/edit.html', context)

    def post(self, request, group_id):
        if 'hris_admin' not in request.session:
            return redirect('admin_login')
        admin = request.session.get('hris_admin', {})
        db = data_mysql()
        group_name = request.POST.get('group_name', '').strip()
        group_desc = request.POST.get('group_desc', '').strip()
        if not group_name:
            messages.error(request, 'Nama Group wajib diisi.')
            return redirect(f'/settings/sistem/groups/{group_id}/edit')
        sql = """
            UPDATE app_group SET
                group_name = %s,
                group_desc = %s,
                mdb = %s,
                mdb_name = %s,
                mdd = NOW()
            WHERE group_id = %s
        """
        params = (
            group_name,
            group_desc,
            admin.get('user_id', ''),
            admin.get('user_alias', ''),
            group_id,
        )
        if db.execute_query(sql, params):
            db.commit()
            messages.success(request, 'Group berhasil diperbarui.')
            return redirect('/settings/sistem/groups')
        else:
            messages.error(request, 'Gagal memperbarui group.')
            return redirect(f'/settings/sistem/groups/{group_id}/edit')


class GroupsDeleteView(View):
    def post(self, request, group_id):
        if 'hris_admin' not in request.session:
            return redirect('admin_login')
        db = data_mysql()
        sql = "DELETE FROM app_group WHERE group_id = %s"
        if db.execute_query(sql, (group_id,)):
            db.commit()
            messages.success(request, 'Group berhasil dihapus.')
        else:
            messages.error(request, 'Gagal menghapus group.')
        return redirect('/settings/sistem/groups')


# Sistem / Menu
class MenuIndexView(View):
    def get(self, request):
        if 'hris_admin' not in request.session:
            return redirect('admin_login')
        admin = request.session.get('hris_admin', {})
        active_portal_id = request.session.get('active_portal_id', '12')
        db = data_mysql()
        sql = """
            SELECT a.*, COUNT(b.nav_id) AS total_menu
            FROM app_portal a
            LEFT JOIN app_menu b ON a.portal_id = b.portal_id
            GROUP BY a.portal_id
            ORDER BY a.portal_id ASC
        """
        portals = []
        if db.execute_query(sql):
            portals = db.cur_hris.fetchall() or []
        context = {
            'user': admin,
            'active_portal_id': active_portal_id,
            'portals': portals,
        }
        return render(request, 'sistem/menu/index.html', context)


class MenuEditView(View):
    def get(self, request, portal_id):
        if 'hris_admin' not in request.session:
            return redirect('admin_login')
        admin = request.session.get('hris_admin', {})
        active_portal_id = request.session.get('active_portal_id', '12')
        db = data_mysql()
        portal = None
        q_portal = '''
            SELECT portal_id, portal_nm, portal_title
            FROM app_portal
            WHERE portal_id = %s
            LIMIT 1
        '''
        if db.execute_query(q_portal, (portal_id,)):
            portal = db.cur_hris.fetchone()
        if not portal:
            return redirect('/settings/sistem/menu')

        sql_menus = '''
            SELECT nav_id, nav_parent, nav_name, nav_url, nav_icon, active_st, display_st
            FROM app_menu
            WHERE portal_id = %s
            ORDER BY COALESCE(nav_order, 999), nav_name ASC
        '''
        menus = []
        if db.execute_query(sql_menus, (portal_id,)):
            rows = db.cur_hris.fetchall() or []
            by_id = {}
            for m in rows:
                by_id[m['nav_id']] = {**m, 'children': []}
            roots = []
            for m in rows:
                parent = (m.get('nav_parent') or '').strip()
                if parent and parent in by_id:
                    by_id[parent]['children'].append(by_id[m['nav_id']])
                else:
                    roots.append(by_id[m['nav_id']])

            flat = []
            def walk(node, level=1):
                indent_prefix = '' if level <= 1 else ('-- ' * (level - 1))
                flat.append({**{k: v for k, v in node.items() if k != 'children'}, 'level': level, 'indent_prefix': indent_prefix})
                for child in node.get('children', []):
                    walk(child, level + 1)

            for r in roots:
                walk(r, 1)
            menus = flat

        context = {
            'user': admin,
            'active_portal_id': active_portal_id,
            'portal': portal,
            'menus': menus,
        }
        return render(request, 'sistem/menu/navigation.html', context)


class MenuItemCreateView(View):
    def _parent_options(self, db, portal_id):
        sql_menus = '''
            SELECT nav_id, nav_parent, nav_name
            FROM app_menu
            WHERE portal_id = %s
            ORDER BY COALESCE(nav_order, 999), nav_name ASC
        '''
        options = [{'nav_id': '', 'label': 'Top Level'}]
        rows = []
        if db.execute_query(sql_menus, (portal_id,)):
            rows = db.cur_hris.fetchall() or []
        by_id = {m['nav_id']: {**m, 'children': []} for m in rows}
        roots = []
        for m in rows:
            parent = (m.get('nav_parent') or '').strip()
            if parent and parent in by_id:
                by_id[parent]['children'].append(by_id[m['nav_id']])
            else:
                roots.append(by_id[m['nav_id']])
        def walk(node, level=1):
            prefix = '' if level <= 1 else ('-- ' * (level - 1))
            options.append({'nav_id': node['nav_id'], 'label': f"{prefix}{node['nav_name']}"})
            for child in node.get('children', []):
                walk(child, level + 1)
        for r in roots:
            walk(r, 1)
        return options

    def get(self, request, portal_id):
        if 'hris_admin' not in request.session:
            return redirect('admin_login')
        admin = request.session.get('hris_admin', {})
        active_portal_id = request.session.get('active_portal_id', '12')
        db = data_mysql()
        portal = None
        q_portal = '''
            SELECT portal_id, portal_nm, portal_title
            FROM app_portal
            WHERE portal_id = %s
            LIMIT 1
        '''
        if db.execute_query(q_portal, (portal_id,)):
            portal = db.cur_hris.fetchone()
        if not portal:
            return redirect('/settings/sistem/menu')
        parent_options = self._parent_options(db, portal_id)
        context = {
            'user': admin,
            'active_portal_id': active_portal_id,
            'portal': portal,
            'parent_options': parent_options,
        }
        return render(request, 'sistem/menu/add.html', context)

    def post(self, request, portal_id):
        if 'hris_admin' not in request.session:
            return redirect('admin_login')
        admin = request.session.get('hris_admin', {})
        db = data_mysql()
        nav_name = (request.POST.get('nav_name') or '').strip()
        nav_desc = (request.POST.get('nav_desc') or '').strip()
        nav_url = (request.POST.get('nav_url') or '').strip()
        nav_order_raw = (request.POST.get('nav_order') or '').strip()
        parent_id = (request.POST.get('parent_id') or '').strip()
        active_st = (request.POST.get('active_st') or '1').strip()
        display_st = (request.POST.get('display_st') or '1').strip()
        nav_icon = (request.POST.get('nav_icon') or '').strip()
        if not nav_name:
            return HttpResponseBadRequest('nav_name is required')
        try:
            nav_order = int(nav_order_raw) if nav_order_raw else None
        except ValueError:
            return HttpResponseBadRequest('nav_order must be an integer')
        nav_id = generate_next_nav_id(db, portal_id)
        if not nav_id:
            return HttpResponseBadRequest('Cannot generate new nav_id; limit reached or error')
        attempts = 3
        for i in range(attempts):
            sql = '''
                INSERT INTO app_menu
                (nav_id, portal_id, nav_name, nav_url, nav_icon, nav_parent, nav_order, active_st, display_st, mdb, mdb_name, mdd)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
            '''
            params = (
                nav_id, portal_id, nav_name, nav_url, nav_icon,
                parent_id, nav_order, active_st or '1', display_st or '1',
                admin.get('user_id', ''), admin.get('user_alias', ''),
            )
            if db.execute_query(sql, params):
                db.commit()
                messages.success(request, 'Menu berhasil ditambahkan.')
                return redirect(f'/settings/sistem/menu/{portal_id}/edit')
            check_sql = 'SELECT nav_id FROM app_menu WHERE nav_id = %s LIMIT 1'
            if db.execute_query(check_sql, (nav_id,)) and db.cur_hris.fetchone():
                nav_id = generate_next_nav_id(db, portal_id)
                if not nav_id:
                    break
                continue
            break
        messages.error(request, 'Gagal menambahkan menu: duplicate atau kesalahan database.')
        return redirect(f'/settings/sistem/menu/{portal_id}/add')


class MenuItemEditView(View):
    def _parent_options(self, db, portal_id, exclude_id=None):
        sql_menus = '''
            SELECT nav_id, nav_parent, nav_name
            FROM app_menu
            WHERE portal_id = %s
            ORDER BY COALESCE(nav_order, 999), nav_name ASC
        '''
        options = [{'nav_id': '', 'label': 'Top Level'}]
        rows = []
        if db.execute_query(sql_menus, (portal_id,)):
            rows = db.cur_hris.fetchall() or []
        by_id = {m['nav_id']: {**m, 'children': []} for m in rows}
        roots = []
        for m in rows:
            parent = (m.get('nav_parent') or '').strip()
            if parent and parent in by_id:
                by_id[parent]['children'].append(by_id[m['nav_id']])
            else:
                roots.append(by_id[m['nav_id']])
        def walk(node, level=1):
            if exclude_id and node.get('nav_id') == exclude_id:
                return
            prefix = '' if level <= 1 else ('-- ' * (level - 1))
            options.append({'nav_id': node['nav_id'], 'label': f"{prefix}{node['nav_name']}"})
            for child in node.get('children', []):
                walk(child, level + 1)
        for r in roots:
            walk(r, 1)
        return options

    def get(self, request, portal_id, nav_id):
        if 'hris_admin' not in request.session:
            return redirect('admin_login')
        admin = request.session.get('hris_admin', {})
        active_portal_id = request.session.get('active_portal_id', '12')
        db = data_mysql()
        portal = None
        q_portal = '''
            SELECT portal_id, portal_nm, portal_title
            FROM app_portal
            WHERE portal_id = %s
            LIMIT 1
        '''
        if db.execute_query(q_portal, (portal_id,)):
            portal = db.cur_hris.fetchone()
        if not portal:
            return redirect('/settings/sistem/menu')
        item = None
        q_item = '''
            SELECT nav_id, portal_id, nav_name, nav_url, nav_icon, nav_parent, nav_order, active_st, display_st
            FROM app_menu
            WHERE portal_id = %s AND nav_id = %s
            LIMIT 1
        '''
        if db.execute_query(q_item, (portal_id, nav_id)):
            item = db.cur_hris.fetchone()
        if not item:
            return redirect(f'/settings/sistem/menu/{portal_id}/edit')
        parent_options = self._parent_options(db, portal_id, exclude_id=nav_id)
        context = {
            'user': admin,
            'active_portal_id': active_portal_id,
            'portal': portal,
            'item': item,
            'parent_options': parent_options,
        }
        return render(request, 'sistem/menu/edit.html', context)

    def post(self, request, portal_id, nav_id):
        if 'hris_admin' not in request.session:
            return redirect('admin_login')
        admin = request.session.get('hris_admin', {})
        db = data_mysql()
        nav_name = (request.POST.get('nav_name') or '').strip()
        nav_desc = (request.POST.get('nav_desc') or '').strip()
        nav_url = (request.POST.get('nav_url') or '').strip()
        nav_order_raw = (request.POST.get('nav_order') or '').strip()
        parent_id = (request.POST.get('parent_id') or '').strip()
        active_st = (request.POST.get('active_st') or '1').strip()
        display_st = (request.POST.get('display_st') or '1').strip()
        nav_icon = (request.POST.get('nav_icon') or '').strip()
        if not nav_name:
            return HttpResponseBadRequest('nav_name is required')
        try:
            nav_order = int(nav_order_raw) if nav_order_raw else 0
        except ValueError:
            return HttpResponseBadRequest('nav_order must be an integer')
        if parent_id and parent_id == nav_id:
            return HttpResponseBadRequest('A menu cannot be its own parent')
        sql = '''
            UPDATE app_menu
            SET nav_name = %s,
                nav_url = %s,
                nav_icon = %s,
                nav_parent = %s,
                nav_order = %s,
                active_st = %s,
                display_st = %s,
                mdb = %s,
                mdb_name = %s,
                mdd = NOW()
            WHERE portal_id = %s AND nav_id = %s
        '''
        params = (
            nav_name, nav_url, nav_icon, parent_id,
            nav_order, active_st or '1', display_st or '1',
            admin.get('user_id', ''), admin.get('user_alias', ''),
            portal_id, nav_id,
        )
        if db.execute_query(sql, params):
            db.commit()
            messages.success(request, 'Menu berhasil diperbarui.')
        else:
            messages.error(request, 'Gagal memperbarui menu.')
        return redirect(f'/settings/sistem/menu/{portal_id}/edit')


class MenuItemDeleteView(View):
    def post(self, request, portal_id, nav_id):
        if 'hris_admin' not in request.session:
            return redirect('admin_login')
        db = data_mysql()

        # Build list of nav_ids to delete (nav_id and all its descendants)
        q_menus = '''
            SELECT nav_id, nav_parent
            FROM app_menu
            WHERE portal_id = %s
        '''
        if not db.execute_query(q_menus, (portal_id,)):
            messages.error(request, 'Gagal memuat data menu untuk penghapusan.')
            return redirect(f'/settings/sistem/menu/{portal_id}/edit')

        rows = db.cur_hris.fetchall() or []
        by_parent = {}
        for m in rows:
            parent = (m.get('nav_parent') or '').strip()
            by_parent.setdefault(parent, []).append(m['nav_id'])

        to_delete = set()
        stack = [nav_id]
        while stack:
            current = stack.pop()
            if current in to_delete:
                continue
            to_delete.add(current)
            children = by_parent.get(current, [])
            for child in children:
                stack.append(child)

        if not to_delete:
            messages.error(request, 'Menu tidak ditemukan untuk dihapus.')
            return redirect(f'/settings/sistem/menu/{portal_id}/edit')

        # Delete related role mappings first
        placeholders = ','.join(['%s'] * len(to_delete))
        sql_del_roles = f"DELETE FROM app_menu_role WHERE nav_id IN ({placeholders})"
        if not db.execute_query(sql_del_roles, tuple(to_delete)):
            messages.error(request, 'Gagal menghapus relasi role untuk menu.')
            return redirect(f'/settings/sistem/menu/{portal_id}/edit')

        # Delete menus (only for this portal)
        sql_del_menus = f"DELETE FROM app_menu WHERE portal_id = %s AND nav_id IN ({placeholders})"
        params = (portal_id, *tuple(to_delete))
        if db.execute_query(sql_del_menus, params):
            db.commit()
            messages.success(request, 'Menu berhasil dihapus.')
        else:
            messages.error(request, 'Gagal menghapus menu.')

        return redirect(f'/settings/sistem/menu/{portal_id}/edit')


# Sistem / Roles
class RolesIndexView(View):
    def get(self, request):
        if 'hris_admin' not in request.session:
            return redirect('admin_login')
        admin = request.session.get('hris_admin', {})
        active_portal_id = request.session.get('active_portal_id', '12')
        db = data_mysql()
        sql_roles = """
            SELECT r.role_id, r.group_id, g.group_name, r.role_nm, r.role_desc, r.default_page
            FROM app_role r
            LEFT JOIN app_group g ON g.group_id = r.group_id
            ORDER BY CAST(r.role_id AS UNSIGNED) ASC
        """
        roles = []
        if db.execute_query(sql_roles):
            roles = db.cur_hris.fetchall() or []
        sql_groups = """
            SELECT group_id, group_name
            FROM app_group
            ORDER BY group_id ASC
        """
        groups = []
        if db.execute_query(sql_groups):
            groups = db.cur_hris.fetchall() or []
        context = {
            'user': admin,
            'active_portal_id': active_portal_id,
            'roles': roles,
            'groups': groups,
        }
        return render(request, 'sistem/roles/index.html', context)


class RolesCreateView(View):
    def post(self, request):
        if 'hris_admin' not in request.session:
            return redirect('admin_login')
        admin = request.session.get('hris_admin', {})
        db = data_mysql()
        group_id = (request.POST.get('group_id') or '').strip()
        if not group_id:
            messages.error(request, 'Group wajib dipilih untuk membuat Role.')
            return redirect('/settings/sistem/roles')
        role_id = generate_next_role_id(db, group_id)
        if not role_id:
            messages.error(request, 'Gagal membuat role: batas ID tercapai atau terjadi kesalahan.')
            return redirect('/settings/sistem/roles')
        role_nm = (request.POST.get('role_nm') or '').strip()
        role_desc = (request.POST.get('role_desc') or '').strip()
        default_page = (request.POST.get('default_page') or '').strip()
        if not role_nm:
            messages.error(request, 'Nama Role wajib diisi.')
            return redirect('/settings/sistem/roles')
        sql = """
            INSERT INTO app_role
            (role_id, group_id, role_nm, role_desc, default_page, mdb, mdb_name, mdd)
            VALUES (%s, %s, %s, %s, %s, %s, %s, NOW())
        """
        params = (
            role_id,
            group_id,
            role_nm,
            role_desc,
            default_page,
            admin.get('user_id', ''),
            admin.get('user_alias', ''),
        )
        if db.execute_query(sql, params):
            db.commit()
            messages.success(request, 'Role berhasil dibuat.')
        else:
            messages.error(request, 'Gagal membuat role.')
        return redirect('/settings/sistem/roles')


class RolesEditView(View):
    def get(self, request, role_id):
        if 'hris_admin' not in request.session:
            return redirect('admin_login')
        admin = request.session.get('hris_admin', {})
        active_portal_id = request.session.get('active_portal_id', '12')
        db = data_mysql()
        sql = """
            SELECT role_id, group_id, role_nm, role_desc, default_page
            FROM app_role
            WHERE role_id = %s
            LIMIT 1
        """
        role = None
        if db.execute_query(sql, (role_id,)):
            role = db.cur_hris.fetchone()
        if not role:
            return redirect('/settings/sistem/roles')
        sql_groups = """
            SELECT group_id, group_name
            FROM app_group
            ORDER BY group_id ASC
        """
        groups = []
        if db.execute_query(sql_groups):
            groups = db.cur_hris.fetchall() or []
        context = {
            'user': admin,
            'active_portal_id': active_portal_id,
            'role': role,
            'groups': groups,
        }
        return render(request, 'sistem/roles/edit.html', context)

    def post(self, request, role_id):
        if 'hris_admin' not in request.session:
            return redirect('admin_login')
        admin = request.session.get('hris_admin', {})
        db = data_mysql()
        group_id = (request.POST.get('group_id') or '').strip()
        role_nm = (request.POST.get('role_nm') or '').strip()
        role_desc = (request.POST.get('role_desc') or '').strip()
        default_page = (request.POST.get('default_page') or '').strip()
        if not role_nm:
            messages.error(request, 'Nama Role wajib diisi.')
            return redirect(f'/settings/sistem/roles/{role_id}/edit')
        sql = """
            UPDATE app_role SET
                group_id = %s,
                role_nm = %s,
                role_desc = %s,
                default_page = %s,
                mdb = %s,
                mdb_name = %s,
                mdd = NOW()
            WHERE role_id = %s
        """
        params = (
            group_id,
            role_nm,
            role_desc,
            default_page,
            admin.get('user_id', ''),
            admin.get('user_alias', ''),
            role_id,
        )
        if db.execute_query(sql, params):
            db.commit()
            messages.success(request, 'Role berhasil diperbarui.')
            return redirect('/settings/sistem/roles')
        else:
            messages.error(request, 'Gagal memperbarui role.')
            return redirect(f'/settings/sistem/roles/{role_id}/edit')


class RolesDeleteView(View):
    def post(self, request, role_id):
        if 'hris_admin' not in request.session:
            return redirect('admin_login')
        db = data_mysql()
        sql = "DELETE FROM app_role WHERE role_id = %s"
        if db.execute_query(sql, (role_id,)):
            db.commit()
            messages.success(request, 'Role berhasil dihapus.')
        else:
            messages.error(request, 'Gagal menghapus role.')
        return redirect('/settings/sistem/roles')


# Sistem / Permissions
class PermissionsIndexView(View):
    def get(self, request):
        if 'hris_admin' not in request.session:
            return redirect('admin_login')
        admin = request.session.get('hris_admin', {})
        active_portal_id = request.session.get('active_portal_id', '12')
        db = data_mysql()
        sql = """
            SELECT b.group_name, a.role_id, a.group_id, a.role_nm, a.role_desc, a.default_page
            FROM app_role a
            INNER JOIN app_group b ON a.group_id = b.group_id
            ORDER BY b.group_id ASC, CAST(a.role_id AS UNSIGNED) ASC
        """
        roles = []
        if db.execute_query(sql):
            roles = db.cur_hris.fetchall() or []
        context = {
            'user': admin,
            'active_portal_id': active_portal_id,
            'roles': roles,
        }
        return render(request, 'sistem/permissions/index.html', context)


class PermissionsAccessUpdateView(View):
    def get(self, request, role_id):
        if 'hris_admin' not in request.session:
            return redirect('admin_login')
        admin = request.session.get('hris_admin', {})
        active_portal_id = request.session.get('active_portal_id', '12')
        db = data_mysql()
        role = None
        q_role = '''
            SELECT role_id, role_nm, role_desc, default_page
            FROM app_role
            WHERE role_id = %s
            LIMIT 1
        '''
        if db.execute_query(q_role, (role_id,)):
            role = db.cur_hris.fetchone()
        if not role:
            messages.error(request, 'Role tidak ditemukan.')
            return redirect('/settings/sistem/permissions')

        portals = []
        q_portal = '''
            SELECT portal_id, COALESCE(portal_title, portal_nm) AS portal_title, portal_nm
            FROM app_portal
            ORDER BY portal_id ASC
        '''
        if db.execute_query(q_portal):
            portals = db.cur_hris.fetchall() or []

        selected_portal_id = request.GET.get('portal_id') or active_portal_id or (portals[0]['portal_id'] if portals else '')

        menu_rows = []
        if selected_portal_id:
            q_all = '''
                SELECT m.nav_id, m.nav_parent, m.nav_name, m.nav_order
                FROM app_menu m
                WHERE m.portal_id = %s AND m.display_st = '1'
                ORDER BY COALESCE(m.nav_order, 999), m.nav_name ASC
            '''
            menus = []
            if db.execute_query(q_all, (selected_portal_id,)):
                menus = db.cur_hris.fetchall() or []

            role_map = {}
            q_map = '''
                SELECT nav_id, role_tp
                FROM app_menu_role
                WHERE role_id = %s
            '''
            if db.execute_query(q_map, (role_id,)):
                for row in (db.cur_hris.fetchall() or []):
                    role_map[row['nav_id']] = row.get('role_tp') or '0000'

            by_id = {m['nav_id']: {**m, 'children': []} for m in menus}
            roots = []
            for m in menus:
                parent = (m.get('nav_parent') or '').strip()
                if parent and parent in by_id:
                    by_id[parent]['children'].append(by_id[m['nav_id']])
                else:
                    roots.append(by_id[m['nav_id']])

            def walk(node, indent_prefix=''):
                rid = node['nav_id']
                menu_rows.append({
                    'nav_id': rid,
                    'nav_name': node.get('nav_name'),
                    'role_tp': role_map.get(rid, '0000'),
                    'indent': indent_prefix,
                })
                for child in node.get('children', []):
                    walk(child, indent_prefix + '--- ')

            for r in roots:
                walk(r, '')

        context = {
            'user': admin,
            'active_portal_id': active_portal_id,
            'role': role,
            'portals': portals,
            'selected_portal_id': selected_portal_id,
            'menu_rows': menu_rows,
        }
        return render(request, 'sistem/permissions/access_update.html', context)


class PermissionsFilterPortalProcessView(View):
    def post(self, request):
        if 'hris_admin' not in request.session:
            return redirect('admin_login')
        role_id = request.POST.get('role_id', '').strip()
        portal_id = request.POST.get('portal_id', '').strip()
        action = request.POST.get('save', '').strip()
        if action.lower() == 'reset':
            portal_id = ''
        url = f"/settings/sistem/permissions/access_update/{role_id}"
        if portal_id:
            url = f"{url}?portal_id={portal_id}"
        return redirect(url)


class PermissionsProcessView(View):
    def post(self, request):
        if 'hris_admin' not in request.session:
            return redirect('admin_login')
        admin = request.session.get('hris_admin', {})
        role_id = request.POST.get('role_id', '').strip()
        portal_id = request.POST.get('portal_id', '').strip()
        if not role_id or not portal_id:
            messages.error(request, 'Portal atau Role tidak valid.')
            return redirect('/settings/sistem/permissions')

        db = data_mysql()
        ok = True
        try:
            sql_delete_all = 'DELETE FROM app_menu_role WHERE role_id = %s'
            db.execute_query(sql_delete_all, (role_id,))

            q_menus = '''
                SELECT nav_id
                FROM app_menu
                WHERE portal_id = %s AND display_st = '1'
            '''
            if not db.execute_query(q_menus, (portal_id,)):
                messages.error(request, 'Gagal memuat daftar menu.')
                return redirect(f'/settings/sistem/permissions/access_update/{role_id}?portal_id={portal_id}')
            menus = db.cur_hris.fetchall() or []

            for m in menus:
                nav_id = m['nav_id']
                c = '1' if request.POST.get(f'rules[{nav_id}][C]') else '0'
                r = '1' if request.POST.get(f'rules[{nav_id}][R]') else '0'
                u = '1' if request.POST.get(f'rules[{nav_id}][U]') else '0'
                d = '1' if request.POST.get(f'rules[{nav_id}][D]') else '0'
                role_tp = f"{c}{r}{u}{d}"
                if role_tp != '0000':
                    sql_insert = '''
                        INSERT INTO app_menu_role (role_id, nav_id, role_tp)
                        VALUES (%s, %s, %s)
                    '''
                    db.execute_query(sql_insert, (role_id, nav_id, role_tp))
            db.commit()
        except Exception:
            ok = False

        if ok:
            messages.success(request, 'Data berhasil disimpan')
        else:
            messages.error(request, 'Data gagal disimpan')

        return redirect(f'/settings/sistem/permissions/access_update/{role_id}?portal_id={portal_id}')
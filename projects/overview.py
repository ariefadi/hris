from django.views import View
from django.shortcuts import render, redirect
from projects.database import data_mysql

class OverviewView(View):
    def get(self, request):
        if 'hris_admin' not in request.session:
            return redirect('admin_login')
        admin = request.session.get('hris_admin', {})
        active_portal_id = request.session.get('active_portal_id', '12')
        db = data_mysql()
        mp_completed = 0
        mp_waiting = 0
        sql_mp = """
            SELECT status, COUNT(*) AS total
            FROM data_media_partner
            WHERE status IN ('completed','waiting')
            GROUP BY status
        """
        if db.execute_query(sql_mp):
            rows = db.cur_hris.fetchall() or []
            for r in rows:
                try:
                    st = r.get('status')
                    total = r.get('total')
                except AttributeError:
                    st = r[0]
                    total = r[1]
                if str(st) == 'completed':
                    try:
                        mp_completed = int(total or 0)
                    except Exception:
                        mp_completed = 0
                elif str(st) == 'waiting':
                    try:
                        mp_waiting = int(total or 0)
                    except Exception:
                        mp_waiting = 0
        server_counts = {'active': 0, 'stopped': 0, 'terminated': 0, 'suspended': 0}
        sql_sv = """
            SELECT server_status, COUNT(*) AS total
            FROM data_servers
            WHERE server_status IN ('active','stopped','terminated','suspended')
            GROUP BY server_status
        """
        if db.execute_query(sql_sv):
            rows = db.cur_hris.fetchall() or []
            for r in rows:
                try:
                    st = r.get('server_status')
                    total = r.get('total')
                except AttributeError:
                    st = r[0]
                    total = r[1]
                k = str(st)
                if k in server_counts:
                    try:
                        server_counts[k] = int(total or 0)
                    except Exception:
                        server_counts[k] = 0
        def count_table(sql):
            c = 0
            if db.execute_query(sql):
                row = db.cur_hris.fetchone() or {}
                try:
                    c = int((row.get('total') if isinstance(row, dict) else row[0]) or 0)
                except Exception:
                    c = 0
            return c
        total_domains = count_table("SELECT COUNT(*) AS total FROM data_domains")
        total_subdomains = count_table("SELECT COUNT(*) AS total FROM data_subdomain")
        total_nieces = count_table("SELECT COUNT(*) AS total FROM data_niece")
        total_keywords = count_table("SELECT COUNT(*) AS total FROM data_keywords")
        context = {
            'user': admin,
            'active_portal_id': active_portal_id,
            'mp_completed': mp_completed,
            'mp_waiting': mp_waiting,
            'server_active': server_counts['active'],
            'server_stopped': server_counts['stopped'],
            'server_terminated': server_counts['terminated'],
            'server_suspended': server_counts['suspended'],
            'total_domains': total_domains,
            'total_subdomains': total_subdomains,
            'total_nieces': total_nieces,
            'total_keywords': total_keywords,
        }
        return render(request, 'projects/overview.html', context)

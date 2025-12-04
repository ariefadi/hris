from django.views import View
from django.shortcuts import render, redirect
from django.http import HttpResponseBadRequest, JsonResponse
from django.contrib import messages
from django.utils.decorators import method_decorator
from django.views.decorators.csrf import csrf_exempt
from projects.database import data_mysql
from datetime import datetime
from hris.mail import send_mail, Mail
import re
import random

class DraftIndexView(View):
    def get(self, request):
        if 'hris_admin' not in request.session:
            return redirect('admin_login')
        admin = request.session.get('hris_admin', {})
        active_portal_id = request.session.get('active_portal_id', '12')
        db = data_mysql()
        sql = """
            SELECT *
            FROM data_media_partner
            WHERE status = 'draft'
            ORDER BY COALESCE(mdd, request_date) DESC
        """
        partners = []
        if db.execute_query(sql):
            partners = db.cur_hris.fetchall() or []

        users = []
        q_users = """
            SELECT user_alias
            FROM app_users
            WHERE user_st = '1'
            ORDER BY user_alias ASC
        """
        if db.execute_query(q_users):
            users = db.cur_hris.fetchall() or []
            
        domains = []
        q_domains = """
            SELECT domain_id, domain AS domain_label
            FROM data_domains
            ORDER BY domain ASC
        """
        if db.execute_query(q_domains):
            domains = db.cur_hris.fetchall() or []

        statuses = ['draft','waiting','canceled','rejected','completed']
        context = {
            'user': admin,
            'active_portal_id': active_portal_id,
            'partners': partners,
            'users': users,
            'domains': domains,
            'statuses': statuses,
        }
        return render(request, 'task/draft/index.html', context)

class DraftCreateView(View):
    def post(self, request):
        if 'hris_admin' not in request.session:
            return redirect('admin_login')
        admin = request.session.get('hris_admin', {})
        db = data_mysql()
        now = datetime.now()
        base = now.strftime('%Y%m%d%H%M%S')
        rnd = f"{random.randint(0, 9999):04d}"
        partner_id = f"MP{base}{rnd}"[:20]
        partner_name = request.POST.get('partner_name', '').strip()
        partner_contact = request.POST.get('partner_contact', '').strip()
        partner_region = request.POST.get('partner_region', '').strip()
        request_date_raw = request.POST.get('request_date', '').strip()
        pic = request.POST.get('pic', '').strip()
        adnetwork = request.POST.get('adnetwork', '').strip() or 'adx'
        domain_ids = request.POST.getlist('domains')
        status = 'draft'
        try:
            fixed = request_date_raw.strip()
            request_date = datetime.strptime(fixed, '%Y-%m-%d') if fixed else None
        except ValueError:
            request_date = None
        is_ajax = (request.headers.get('x-requested-with') == 'XMLHttpRequest') or (
            request.META.get('HTTP_X_REQUESTED_WITH') == 'XMLHttpRequest'
        )
        if not partner_name:
            if is_ajax:
                return JsonResponse({'status': False, 'message': 'Nama partner wajib diisi.'}, status=400)
            messages.error(request, 'Nama partner wajib diisi.')
            return redirect('/projects/task/draft')
        # if not domain_ids:
        #     messages.error(request, 'Domain wajib dipilih.')
        #     return redirect('/projects/task/draft')
        sql = """
            INSERT INTO data_media_partner
            (partner_id, partner_name, partner_contact, partner_region, request_date, pic, status, adnetwork, mdb, mdb_name, mdd)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
        """
        params = (
            partner_id,
            partner_name,
            partner_contact or None,
            partner_region or None,
            request_date,
            pic or None,
            status,
            adnetwork,
            admin.get('user_id', ''),
            admin.get('user_alias', ''),
        )
        # if not domain_ids:
        #     if is_ajax:
        #         return JsonResponse({'status': False, 'message': 'Domain wajib dipilih.'}, status=400)
        #     return redirect('/projects/task/draft')

        if db.execute_query(sql, params):
            db.commit()
            sql_rel = """
                INSERT INTO data_media_partner_domain (partner_id, domain_id)
                VALUES (%s, %s)
            """
            for did in domain_ids:
                val = (str(did or '').strip())
                if not val:
                    continue
                try:
                    num = int(val)
                except ValueError:
                    continue
                db.execute_query(sql_rel, (partner_id, num))
            db.commit()
            if is_ajax:
                return JsonResponse({'status': True, 'partner_id': partner_id})
            messages.success(request, 'Project berhasil ditambahkan.')
            return redirect('/projects/task/draft')
        else:
            if is_ajax:
                return JsonResponse({'status': False, 'message': 'Gagal menambahkan project.'}, status=500)
            messages.error(request, 'Gagal menambahkan project.')
            return redirect('/projects/task/draft')

class DraftEditView(View):
    def get(self, request, partner_id):
        if 'hris_admin' not in request.session:
            return redirect('admin_login')
        admin = request.session.get('hris_admin', {})
        active_portal_id = request.session.get('active_portal_id', '12')
        db = data_mysql()
        sql = """
            SELECT partner_id, partner_name, partner_contact, partner_region,
                   request_date, pic, status, adnetwork, mdd
            FROM data_media_partner
            WHERE partner_id = %s
            LIMIT 1
        """
        partner = None
        if db.execute_query(sql, (partner_id,)):
            partner = db.cur_hris.fetchone()
        users = []
        q_users = """
            SELECT user_alias
            FROM app_users
            WHERE user_st = '1'
            ORDER BY user_alias ASC
        """
        if db.execute_query(q_users):
            users = db.cur_hris.fetchall() or []
        domains = []
        q_domains = """
            SELECT domain_id, domain AS domain_label
            FROM data_domains
            ORDER BY domain ASC
        """
        if db.execute_query(q_domains):
            domains = db.cur_hris.fetchall() or []
        selected_domain_ids = []
        q_rel = """
            SELECT domain_id
            FROM data_media_partner_domain
            WHERE partner_id = %s
        """
        if db.execute_query(q_rel, (partner_id,)):
            rows = db.cur_hris.fetchall() or []
            for r in rows:
                val = None
                try:
                    val = r.get('domain_id')
                except AttributeError:
                    try:
                        val = r[0]
                    except Exception:
                        val = None
                if val is None:
                    continue
                try:
                    selected_domain_ids.append(int(str(val)))
                except ValueError:
                    continue
        statuses = ['draft','waiting','canceled','rejected','completed']
        context = {
            'user': admin,
            'active_portal_id': active_portal_id,
            'partner': partner,
            'users': users,
            'domains': domains,
            'selected_domains': selected_domain_ids,
            'statuses': statuses,
        }
        return render(request, 'task/draft/edit.html', context)
    def post(self, request, partner_id):
        if 'hris_admin' not in request.session:
            return redirect('admin_login')
        admin = request.session.get('hris_admin', {})
        db = data_mysql()
        partner_name = request.POST.get('partner_name', '').strip()
        partner_contact = request.POST.get('partner_contact', '').strip()
        partner_region = request.POST.get('partner_region', '').strip()
        request_date_raw = request.POST.get('request_date', '').strip()
        pic = request.POST.get('pic', '').strip()
        adnetwork = request.POST.get('adnetwork', '').strip() or 'adx'
        status = request.POST.get('status', '').strip() or 'draft'
        update_action = request.POST.get('update', '').strip()
        domain_ids = request.POST.getlist('domains')
        try:
            fixed = request_date_raw.strip()
            request_date = datetime.strptime(fixed, '%Y-%m-%d') if fixed else None
        except ValueError:
            request_date = None
        is_ajax = (request.headers.get('x-requested-with') == 'XMLHttpRequest') or (
            request.META.get('HTTP_X_REQUESTED_WITH') == 'XMLHttpRequest'
        )
        if not partner_name:
            if is_ajax:
                return JsonResponse({'status': False, 'message': 'Nama partner wajib diisi.'}, status=400)
            messages.error(request, 'Nama partner wajib diisi.')
            return redirect('/projects/task/draft')
        # if not domain_ids:
        #     if is_ajax:
        #         return JsonResponse({'status': False, 'message': 'Domain wajib dipilih.'}, status=400)
        #     messages.error(request, 'Domain wajib dipilih.')
        #     return redirect('/projects/task/draft')
        if update_action == 'send':
            status = 'waiting'
        sql = """
            UPDATE data_media_partner SET
                partner_name = %s,
                partner_contact = %s,
                partner_region = %s,
                request_date = %s,
                pic = %s,
                adnetwork = %s,
                status = %s,
                mdb = %s,
                mdb_name = %s,
                mdd = NOW()
            WHERE partner_id = %s
        """
        params = (
            partner_name,
            partner_contact or None,
            partner_region or None,
            request_date,
            pic or None,
            adnetwork,
            status,
            admin.get('user_id', ''),
            admin.get('user_alias', ''),
            partner_id,
        )
        if db.execute_query(sql, params):
            db.commit()
            db.execute_query("DELETE FROM data_media_partner_domain WHERE partner_id = %s", (partner_id,))
            db.commit()
            sql_rel = """
                INSERT INTO data_media_partner_domain (partner_id, domain_id)
                VALUES (%s, %s)
            """
            for did in domain_ids:
                val = (str(did or '').strip())
                if not val:
                    continue
                try:
                    num = int(val)
                except ValueError:
                    continue
                db.execute_query(sql_rel, (partner_id, num))
            db.commit()
            if update_action == 'send':
                now = datetime.now()
                base = now.strftime('%Y%m%d%H%M%S')
                rnd = f"{random.randint(0, 9999):04d}"
                process_id = f"PR{base}{rnd}"[:20]
                admin_id = str(admin.get('user_id', ''))[:10]
                admin_alias = admin.get('user_alias', '')
                sql_proc = """
                    INSERT INTO data_media_process
                    (process_id, partner_id, flow_id, flow_revisi_id, process_st, action_st, catatan, mdb, mdb_name, mdd)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                """
                params_proc = (
                    process_id,
                    partner_id,
                    '1001',
                    None,
                    'waiting',
                    'process',
                    None,
                    admin_id,
                    admin_alias,
                )
                db.execute_query(sql_proc, params_proc)
                db.commit()
            if is_ajax:
                if update_action == 'send':
                    res = {'status': True, 'message': 'Berhasil diupdate dan dikirim.'}
                    res['process_id'] = process_id
                else:
                    res = {'status': True, 'message': 'Berhasil diupdate.'}
                return JsonResponse(res)
            messages.success(request, 'Draft partner berhasil diperbarui.')
            return redirect('/projects/task/draft')
        else:
            if is_ajax:
                return JsonResponse({'status': False, 'message': 'Gagal memperbarui draft partner.'}, status=500)
            messages.error(request, 'Gagal memperbarui draft partner.')
            return redirect('/projects/task/draft')

class DraftDeleteView(View):
    def post(self, request, partner_id):
        if 'hris_admin' not in request.session:
            return redirect('admin_login')
        db = data_mysql()
        sql = "DELETE FROM data_media_partner WHERE partner_id = %s"
        if db.execute_query(sql, (partner_id,)):
            db.commit()
            messages.success(request, 'Draft partner berhasil dihapus.')
        else:
            messages.error(request, 'Gagal menghapus draft partner.')
        return redirect('/projects/task/draft')

class MonitoringIndexView(View):
    def get(self, request):
        if 'hris_admin' not in request.session:
            return redirect('admin_login')
        admin = request.session.get('hris_admin', {})
        active_portal_id = request.session.get('active_portal_id', '12')
        db = data_mysql()
        sql = """
            SELECT mp.partner_id,
                   mp.partner_name,
                   mp.partner_contact,
                   mp.request_date,
                   mp.pic,
                   df.task_name
            FROM data_media_partner mp
            LEFT JOIN (
                SELECT p.partner_id, p.flow_id, p.mdd
                FROM data_media_process p
                JOIN (
                    SELECT partner_id, MAX(COALESCE(mdd, '0000-00-00 00:00:00')) AS max_mdd
                    FROM data_media_process
                    GROUP BY partner_id
                ) t ON t.partner_id = p.partner_id AND COALESCE(p.mdd, '0000-00-00 00:00:00') = t.max_mdd
                WHERE p.process_st = 'waiting'
            ) latest ON latest.partner_id = mp.partner_id
            LEFT JOIN data_flow df ON df.flow_id = latest.flow_id
            ORDER BY COALESCE(mp.mdd, mp.request_date) DESC
        """
        items = []
        if db.execute_query(sql):
            items = db.cur_hris.fetchall() or []
        context = {
            'user': admin,
            'active_portal_id': active_portal_id,
            'items': items,
        }
        return render(request, 'task/monitoring/index.html', context)

class TechnicalIndexView(View):
    def get(self, request):
        if 'hris_admin' not in request.session:
            return redirect('admin_login')
        admin = request.session.get('hris_admin', {})
        active_portal_id = request.session.get('active_portal_id', '12')
        db = data_mysql()
        sql = """
            SELECT mp.partner_id,
                   mp.partner_name,
                   mp.partner_contact,
                   mp.partner_region,
                   mp.request_date,
                   mp.pic,
                   mp.status,
                   mp.mdd,
                   MAX(pr.process_id) AS process_id
            FROM data_media_partner mp
            INNER JOIN (
                SELECT p.process_id, p.partner_id, p.flow_id, COALESCE(p.mdd, '0000-00-00 00:00:00') AS mdd
                FROM data_media_process p
                WHERE p.flow_id = %s AND p.process_st = 'waiting'
            ) pr ON pr.partner_id = mp.partner_id
            INNER JOIN data_flow df ON df.flow_id = pr.flow_id
            WHERE mp.status = 'waiting'
              AND pr.mdd = (
                  SELECT MAX(COALESCE(mdd, '0000-00-00 00:00:00'))
                  FROM data_media_process
                  WHERE partner_id = mp.partner_id AND flow_id = %s AND process_st = 'waiting'
              )
            GROUP BY mp.partner_id
            ORDER BY COALESCE(mp.mdd, mp.request_date) DESC
        """
        partners = []
        if db.execute_query(sql, ('1001', '1001')):
            partners = db.cur_hris.fetchall() or []
        subs_by_partner = {}
        sql_subs = """
            SELECT mp.partner_id,
                   d.domain,
                   s.subdomain,
                   s.cloudflare,
                   s.public_ipv4,
                   s.subdomain_id,
                   w.website,
                   w.website_user,
                   w.website_pass
            FROM data_media_partner mp
            INNER JOIN (
                SELECT p.partner_id, COALESCE(p.mdd, '0000-00-00 00:00:00') AS mdd
                FROM data_media_process p
                WHERE p.flow_id = %s AND p.process_st = 'waiting'
            ) pr ON pr.partner_id = mp.partner_id
            INNER JOIN data_flow df ON df.flow_id = %s
            INNER JOIN data_media_partner_domain pd ON pd.partner_id = mp.partner_id
            INNER JOIN data_domains d ON d.domain_id = pd.domain_id
            LEFT JOIN data_subdomain s ON s.domain_id = d.domain_id
            LEFT JOIN data_website w ON w.website_id = s.website_id
            WHERE mp.status = 'waiting'
              AND pr.mdd = (
                  SELECT MAX(COALESCE(mdd, '0000-00-00 00:00:00'))
                  FROM data_media_process
                  WHERE partner_id = mp.partner_id AND flow_id = %s AND process_st = 'waiting'
              )
            ORDER BY d.domain ASC, COALESCE(s.mdd, '0000-00-00 00:00:00') DESC
        """
        subs_rows = []
        if db.execute_query(sql_subs, ('1001', '1001', '1001')):
            subs_rows = db.cur_hris.fetchall() or []
        # Group sub rows by partner_id and attach to partners
        for r in subs_rows:
            pid = None
            try:
                pid = r.get('partner_id')
            except AttributeError:
                try:
                    pid = r[0]
                except Exception:
                    pid = None
            if pid is None:
                continue
            subs_by_partner.setdefault(pid, []).append(r)
        # attach and set domain from first sub row if available
        for i, p in enumerate(partners):
            try:
                pid = p.get('partner_id')
            except AttributeError:
                try:
                    pid = p[0]
                except Exception:
                    pid = None
            rows = subs_by_partner.get(pid, [])
            # attach
            try:
                p['subrows'] = rows
                if rows:
                    # set a representative domain
                    dname = rows[0].get('domain') if hasattr(rows[0], 'get') else None
                    p['domain'] = dname
            except TypeError:
                # p might be a tuple; skip attaching in that case
                pass
        statuses = ['draft','waiting','canceled','rejected','completed']
        providers = []
        q_providers = """
            SELECT provider
            FROM data_server_registrar_provider
            ORDER BY provider ASC
        """
        if db.execute_query(q_providers):
            providers = db.cur_hris.fetchall() or []
        # partner domains fetch endpoint will provide domains per partner
        context = {
            'user': admin,
            'active_portal_id': active_portal_id,
            'partners': partners,
            'statuses': statuses,
            'providers': providers,
        }
        return render(request, 'task/technical/index.html', context)

class TechnicalPartnerDomainsView(View):
    def get(self, request):
        if 'hris_admin' not in request.session:
            return JsonResponse({'status': False, 'message': 'Unauthorized'}, status=401)
        partner_id = (request.GET.get('partner_id') or '').strip()
        db = data_mysql()
        domains = []
        if partner_id:
            sql = """
                SELECT d.domain_id, d.domain
                FROM data_media_partner_domain pd
                JOIN data_domains d ON d.domain_id = pd.domain_id
                WHERE pd.partner_id = %s
                ORDER BY d.domain ASC
            """
            if db.execute_query(sql, (partner_id,)):
                domains = db.cur_hris.fetchall() or []
        return JsonResponse({'status': True, 'domains': domains})

class TechnicalLinkDomainView(View):
    def post(self, request):
        if 'hris_admin' not in request.session:
            return JsonResponse({'status': False, 'message': 'Unauthorized'}, status=401)
        db = data_mysql()
        admin = request.session.get('hris_admin', {})
        partner_id = (request.POST.get('partner_id') or '').strip()
        domain_id_raw = (request.POST.get('domain_id') or '').strip()
        try:
            domain_id = int(domain_id_raw)
        except Exception:
            domain_id = None
        if not partner_id or not domain_id:
            return JsonResponse({'status': False, 'message': 'Partner dan Domain wajib diisi.'}, status=400)
        db.execute_query(
            "INSERT IGNORE INTO data_media_partner_domain (partner_id, domain_id, mdb, mdb_name, mdd) VALUES (%s, %s, %s, %s, NOW())",
            (partner_id, domain_id, admin.get('user_id',''), admin.get('user_alias',''))
        )
        db.commit()
        return JsonResponse({'status': True})

class TechnicalSendView(View):
    def post(self, request):
        if 'hris_admin' not in request.session:
            return JsonResponse({'status': False, 'message': 'Unauthorized'}, status=401)
        db = data_mysql()
        admin = request.session.get('hris_admin', {})
        partner_id = (request.POST.get('partner_id') or '').strip()
        process_id_cur = (request.POST.get('process_id') or '').strip()
        if not partner_id:
            return JsonResponse({'status': False, 'message': 'Partner wajib diisi.'}, status=400)
        if not process_id_cur:
            return JsonResponse({'status': False, 'message': 'Process ID wajib diisi.'}, status=400)
        # pastikan proses saat ini ada
        if not db.execute_query("SELECT process_id FROM data_media_process WHERE process_id = %s LIMIT 1", (process_id_cur,)):
            return JsonResponse({'status': False, 'message': 'Proses saat ini tidak ditemukan.'}, status=404)
        row = db.cur_hris.fetchone() or {}
        try:
            existing_pid = row.get('process_id')
        except AttributeError:
            try:
                existing_pid = row[0]
            except Exception:
                existing_pid = None
        if not existing_pid:
            return JsonResponse({'status': False, 'message': 'Proses saat ini tidak valid.'}, status=400)
        # update proses saat ini menjadi selesai
        ok_upd = db.execute_query(
            """
            UPDATE data_media_process SET process_st=%s, action_st=%s, mdb=%s, mdb_finish=%s, mdd_finish=NOW()
            WHERE process_id=%s
            """,
            ('approve', 'done', str(admin.get('user_id', ''))[:10], admin.get('user_alias', ''), process_id_cur)
        )
        if not ok_upd:
            return JsonResponse({'status': False, 'message': 'Gagal memperbarui proses saat ini.'}, status=500)
        db.commit()
        now = datetime.now()
        base = now.strftime('%Y%m%d%H%M%S')
        rnd = f"{random.randint(0, 9999):04d}"
        process_id = f"PR{base}{rnd}"[:20]
        admin_id = str(admin.get('user_id', ''))[:10]
        admin_alias = admin.get('user_alias', '')
        sql_proc = """
            INSERT INTO data_media_process
            (process_id, partner_id, flow_id, flow_revisi_id, process_st, action_st, catatan, mdb, mdb_name, mdd)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
        """
        params_proc = (
            process_id,
            partner_id,
            '1002',
            None,
            'waiting',
            'process',
            None,
            admin_id,
            admin_alias,
        )
        if not db.execute_query(sql_proc, params_proc):
            return JsonResponse({'status': False, 'message': 'Gagal mengirim data ke proses berikutnya.'}, status=500)
        db.commit()
        return JsonResponse({'status': True, 'process_id': process_id})

class TechnicalServerLookupView(View):
    def get(self, request):
        if 'hris_admin' not in request.session:
            return JsonResponse({'status': False, 'message': 'Unauthorized'}, status=401)
        db = data_mysql()
        ip = (request.GET.get('ip') or request.POST.get('ip') or '').strip()
        partner_id = (request.GET.get('partner_id') or request.POST.get('partner_id') or '').strip()
        resp = {'status': True}
        srv = None
        if ip:
            sql_srv = """
                SELECT public_ipv4,
                       hostname,
                       label,
                       provider,
                       vcpu_count,
                       memory_gb,
                       ssh_user,
                       ssh_pass,
                       ssh_keys
                FROM data_servers
                WHERE public_ipv4 = %s
                ORDER BY COALESCE(mdd, '0000-00-00 00:00:00') DESC
                LIMIT 1
            """
            if db.execute_query(sql_srv, (ip,)):
                row = db.cur_hris.fetchone()
                srv = row or None
        resp['server'] = srv
        sd = None
        w = None
        subrows = []
        domains = []
        selected_domain_id = None
        if partner_id:
            q_domains = """
                SELECT d.domain_id, d.domain
                FROM data_media_partner_domain pd
                JOIN data_domains d ON d.domain_id = pd.domain_id
                WHERE pd.partner_id = %s
                ORDER BY d.domain ASC
            """
            if db.execute_query(q_domains, (partner_id,)):
                domains = db.cur_hris.fetchall() or []
            if not ip:
                sql_sds2 = """
                    SELECT s.subdomain, s.cloudflare, s.public_ipv4, s.website_id, s.domain_id,
                           w.website, w.website_user, w.website_pass
                    FROM data_media_partner_domain dmpd
                    JOIN data_subdomain s ON s.domain_id = dmpd.domain_id
                    LEFT JOIN data_website w ON w.website_id = s.website_id
                    WHERE dmpd.partner_id = %s
                    ORDER BY COALESCE(s.mdd, '0000-00-00 00:00:00') DESC
                    LIMIT 50
                """
                if db.execute_query(sql_sds2, (partner_id,)):
                    subrows = db.cur_hris.fetchall() or []
                    if subrows:
                        sd = subrows[0]
            if not ip:
                if sd and (sd.get('website_id') if hasattr(sd, 'get') else None):
                    sql_w = """
                        SELECT website, website_user, website_pass
                        FROM data_website
                        WHERE website_id = %s
                        LIMIT 1
                    """
                    wid = sd.get('website_id') if hasattr(sd, 'get') else None
                    if wid and db.execute_query(sql_w, (wid,)):
                        w = db.cur_hris.fetchone() or None
            try:
                selected_domain_id = sd.get('domain_id') if sd else None
            except AttributeError:
                selected_domain_id = None
        resp['subdomain'] = sd
        resp['website'] = w
        resp['subrows'] = (subrows if not ip else [])
        resp['domains'] = (domains if not ip else [])
        resp['selected_domain_id'] = selected_domain_id
        return JsonResponse(resp)

class TechnicalServerSaveView(View):
    def post(self, request):
        if 'hris_admin' not in request.session:
            return JsonResponse({'status': False, 'message': 'Unauthorized'}, status=401)
        admin = request.session.get('hris_admin', {})
        db = data_mysql()
        partner_id = request.POST.get('partner_id', '').strip()
        domain_id_raw = request.POST.get('domain_id', '').strip()
        try:
            selected_domain_id = int(domain_id_raw) if domain_id_raw else None
        except Exception:
            selected_domain_id = None
        srv_hostname = request.POST.get('srv_hostname', '').strip() or None
        srv_label = request.POST.get('srv_label', '').strip() or None
        srv_provider = request.POST.get('srv_provider', '').strip() or None
        srv_vcpu_count = request.POST.get('srv_vcpu_count', '').strip()
        srv_memory_gb = request.POST.get('srv_memory_gb', '').strip()
        srv_public_ipv4 = request.POST.get('srv_public_ipv4', '').strip() or None
        srv_ssh_user = request.POST.get('srv_ssh_user', '').strip() or None
        srv_ssh_pass = request.POST.get('srv_ssh_pass', '').strip() or None
        srv_ssh_keys = request.POST.get('srv_ssh_keys', '').strip() or None
        subdomains = [s.strip() for s in request.POST.getlist('sd_subdomain')]
        clouds = [s.strip() for s in request.POST.getlist('sd_cloudflare')]
        ips = [s.strip() for s in request.POST.getlist('sd_public_ipv4')]
        webs = [s.strip() for s in request.POST.getlist('ws_website')]
        wusers = [s.strip() for s in request.POST.getlist('ws_website_user')]
        wpasses = [s.strip() for s in request.POST.getlist('ws_website_pass')]
        try:
            vcpu = int(srv_vcpu_count) if srv_vcpu_count else 0
        except Exception:
            vcpu = None
        try:
            mem = int(srv_memory_gb) if srv_memory_gb else 0
        except Exception:
            mem = None
        if not partner_id:
            return JsonResponse({'status': False, 'message': 'Partner wajib diisi.'}, status=400)
        if not (subdomains or clouds or ips or webs or wusers or wpasses):
            return JsonResponse({'status': False, 'message': 'Data subdomain/website belum diisi.'}, status=400)
        def resolve_domain_id(ipv, sub):
            did = None
            if ipv:
                sql = """
                    SELECT s.domain_id
                    FROM data_media_partner_domain dmpd
                    JOIN data_subdomain s ON s.domain_id = dmpd.domain_id
                    WHERE dmpd.partner_id = %s AND s.public_ipv4 = %s
                    ORDER BY COALESCE(s.mdd, '0000-00-00 00:00:00') DESC
                    LIMIT 1
                """
                if db.execute_query(sql, (partner_id, ipv)):
                    rr = db.cur_hris.fetchone() or {}
                    try:
                        did = rr.get('domain_id')
                    except AttributeError:
                        try:
                            did = rr[0]
                        except Exception:
                            did = None
            if not did and sub:
                sql2 = """
                    SELECT s.domain_id
                    FROM data_media_partner_domain dmpd
                    JOIN data_subdomain s ON s.domain_id = dmpd.domain_id
                    WHERE dmpd.partner_id = %s AND s.subdomain = %s
                    ORDER BY COALESCE(s.mdd, '0000-00-00 00:00:00') DESC
                    LIMIT 1
                """
                if db.execute_query(sql2, (partner_id, sub)):
                    rr = db.cur_hris.fetchone() or {}
                    try:
                        did = rr.get('domain_id')
                    except AttributeError:
                        try:
                            did = rr[0]
                        except Exception:
                            did = None
            if not did:
                sql3 = """
                    SELECT pd.domain_id
                    FROM data_media_partner_domain pd
                    WHERE pd.partner_id = %s
                    ORDER BY COALESCE(pd.mdd, '0000-00-00 00:00:00') DESC
                    LIMIT 1
                """
                if db.execute_query(sql3, (partner_id,)):
                    rr = db.cur_hris.fetchone() or {}
                    try:
                        did = rr.get('domain_id')
                    except AttributeError:
                        try:
                            did = rr[0]
                        except Exception:
                            did = None
            return did
        server_id = None
        if srv_public_ipv4:
            if db.execute_query("SELECT server_id FROM data_servers WHERE public_ipv4 = %s LIMIT 1", (srv_public_ipv4,)):
                row = db.cur_hris.fetchone() or {}
                try:
                    server_id = row.get('server_id')
                except AttributeError:
                    try:
                        server_id = row[0]
                    except Exception:
                        server_id = None
            if not server_id:
                sql_i = """
                    INSERT INTO data_servers
                    (hostname, label, provider, vcpu_count, memory_gb, public_ipv4, ssh_user, ssh_pass, ssh_keys, mdb, mdb_name, mdd)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                """
                if not db.execute_query(sql_i, (srv_hostname, srv_label, srv_provider, vcpu, mem, srv_public_ipv4, srv_ssh_user, srv_ssh_pass, srv_ssh_keys, admin.get('user_id',''), admin.get('user_alias',''))):
                    return JsonResponse({'status': False, 'message': 'Gagal menambahkan server.'}, status=500)
                else:
                    db.commit()
                    try:
                        server_id = db.cur_hris.lastrowid
                    except Exception:
                        server_id = None
        else:
            # without server IPv4, we still allow subdomain/website insert, but notify client
            pass
        website_ids = []
        inserted_subdomains = 0
        row_errors = []
        for i in range(max(len(subdomains), len(clouds), len(ips), len(webs), len(wusers), len(wpasses))):
            sd_sub = (subdomains[i] if i < len(subdomains) else '').strip()
            sd_cf = (clouds[i] if i < len(clouds) else '').strip()
            sd_ip = (ips[i] if i < len(ips) else '').strip() or (srv_public_ipv4 or '')
            w_site = (webs[i] if i < len(webs) else '').strip()
            w_user = (wusers[i] if i < len(wusers) else '').strip()
            w_pass = (wpasses[i] if i < len(wpasses) else '').strip()
            if not (sd_sub or sd_cf or sd_ip or w_site or w_user or w_pass):
                continue
            did = resolve_domain_id(sd_ip, sd_sub)
            if not did and selected_domain_id:
                did = selected_domain_id
            if not did:
                row_errors.append({'row': i+1, 'message': 'Domain untuk baris ini tidak dapat ditentukan.'})
                continue
            # domain_id provided via POST or resolved; no need to insert partner-domain mapping here
            wid = None
            if w_site or w_user or w_pass:
                if not db.execute_query("INSERT INTO data_website (website, website_user, website_pass, mdb, mdb_name, mdd) VALUES (%s, %s, %s, %s, %s, NOW())", (
                    w_site, w_user, w_pass, admin.get('user_id',''), admin.get('user_alias','')
                )):
                    row_errors.append({'row': i+1, 'message': 'Gagal menambahkan data website.'})
                else:
                    db.commit()
                    try:
                        wid = db.cur_hris.lastrowid
                    except Exception:
                        wid = None
            sd_id = None
            sql_sd_exist = """
                SELECT s.subdomain_id
                FROM data_subdomain s
                WHERE s.domain_id = %s AND (s.subdomain = %s OR s.public_ipv4 = %s)
                ORDER BY COALESCE(s.mdd, '0000-00-00 00:00:00') DESC
                LIMIT 1
            """
            sql_sd_i = """
                INSERT INTO data_subdomain (subdomain, domain_id, website_id, cloudflare, public_ipv4, mdb, mdb_name, mdd)
                VALUES (%s, %s, %s, %s, %s, %s, %s, NOW())
            """
            if not db.execute_query(sql_sd_i, (sd_sub or None, did, wid, sd_cf or None, sd_ip or None, admin.get('user_id',''), admin.get('user_alias',''))):
                row_errors.append({'row': i+1, 'message': 'Gagal menambahkan subdomain.'})
            else:
                db.commit()
                inserted_subdomains += 1
            website_ids.append(wid)
        if inserted_subdomains == 0:
            msg = 'Tidak ada subdomain yang berhasil ditambahkan.'
            if row_errors:
                try:
                    msg = row_errors[0].get('message') or msg
                except Exception:
                    msg = msg
            return JsonResponse({'status': False, 'message': msg, 'errors': row_errors}, status=400)
        return JsonResponse({'status': True, 'server_id': server_id, 'website_ids': website_ids})
class TechnicalSubrowLookupView(View):
    def get(self, request):
        if 'hris_admin' not in request.session:
            return JsonResponse({'status': False, 'message': 'Unauthorized'}, status=401)
        partner_id = (request.GET.get('partner_id') or '').strip()
        subdomain_id_raw = (request.GET.get('subdomain_id') or '').strip()
        try:
            subdomain_id = int(subdomain_id_raw)
        except Exception:
            subdomain_id = None
        db = data_mysql()
        resp = {'status': True}
        subrow = None
        domain = None
        server = None
        website = None
        if subdomain_id:
            sql = """
                SELECT s.subdomain_id, s.subdomain, s.cloudflare, s.public_ipv4, s.domain_id, s.website_id,
                       d.domain
                FROM data_subdomain s
                JOIN data_domains d ON d.domain_id = s.domain_id
                WHERE s.subdomain_id = %s
                LIMIT 1
            """
            if db.execute_query(sql, (subdomain_id,)):
                subrow = db.cur_hris.fetchone() or None
            if subrow:
                if subrow.get('website_id'):
                    if db.execute_query("SELECT website, website_user, website_pass FROM data_website WHERE website_id = %s LIMIT 1", (subrow.get('website_id'),)):
                        website = db.cur_hris.fetchone() or None
                if subrow.get('public_ipv4'):
                    q = """
                        SELECT public_ipv4, hostname, label, provider, vcpu_count, memory_gb, ssh_user, ssh_pass, ssh_keys
                        FROM data_servers
                        WHERE public_ipv4 = %s
                        LIMIT 1
                    """
                    if db.execute_query(q, (subrow.get('public_ipv4'),)):
                        server = db.cur_hris.fetchone() or None
                domain = {'domain_id': subrow.get('domain_id'), 'domain': subrow.get('domain')}
        resp['subrow'] = subrow
        resp['domain'] = domain
        resp['server'] = server
        resp['website'] = website
        return JsonResponse(resp)

class TechnicalSubrowUpdateView(View):
    def post(self, request):
        if 'hris_admin' not in request.session:
            return JsonResponse({'status': False, 'message': 'Unauthorized'}, status=401)
        db = data_mysql()
        admin = request.session.get('hris_admin', {})
        subdomain_id_raw = request.POST.get('subdomain_id', '').strip()
        try:
            subdomain_id = int(subdomain_id_raw)
        except Exception:
            return JsonResponse({'status': False, 'message': 'Subdomain tidak valid'}, status=400)
        srv_hostname = request.POST.get('srv_hostname', '').strip() or None
        srv_label = request.POST.get('srv_label', '').strip() or None
        srv_provider = request.POST.get('srv_provider', '').strip() or None
        srv_vcpu_count = request.POST.get('srv_vcpu_count', '').strip()
        srv_memory_gb = request.POST.get('srv_memory_gb', '').strip()
        srv_public_ipv4 = request.POST.get('srv_public_ipv4', '').strip() or None
        srv_ssh_user = request.POST.get('srv_ssh_user', '').strip() or None
        srv_ssh_pass = request.POST.get('srv_ssh_pass', '').strip() or None
        srv_ssh_keys = request.POST.get('srv_ssh_keys', '').strip() or None
        sd_subdomain = request.POST.get('sd_subdomain', '').strip() or None
        sd_cloudflare = request.POST.get('sd_cloudflare', '').strip() or None
        sd_public_ipv4 = request.POST.get('sd_public_ipv4', '').strip() or srv_public_ipv4
        ws_website = request.POST.get('ws_website', '').strip() or None
        ws_website_user = request.POST.get('ws_website_user', '').strip() or None
        ws_website_pass = request.POST.get('ws_website_pass', '').strip() or None
        try:
            vcpu = int(srv_vcpu_count) if srv_vcpu_count else None
        except Exception:
            vcpu = None
        try:
            mem = int(srv_memory_gb) if srv_memory_gb else None
        except Exception:
            mem = None
        # get current subdomain
        sql_sd = """
            SELECT domain_id, website_id
            FROM data_subdomain
            WHERE subdomain_id = %s
            LIMIT 1
        """
        if not db.execute_query(sql_sd, (subdomain_id,)):
            return JsonResponse({'status': False, 'message': 'Subdomain tidak ditemukan'}, status=404)
        curr = db.cur_hris.fetchone() or {}
        domain_id = curr.get('domain_id')
        curr_wid = curr.get('website_id')
        errors = []
        # upsert server by public ipv4
        server_id = None
        if srv_public_ipv4:
            if db.execute_query("SELECT server_id FROM data_servers WHERE public_ipv4 = %s LIMIT 1", (srv_public_ipv4,)):
                r = db.cur_hris.fetchone() or {}
                try:
                    server_id = r.get('server_id')
                except AttributeError:
                    try:
                        server_id = r[0]
                    except Exception:
                        server_id = None
            if server_id:
                ok = db.execute_query(
                    """
                    UPDATE data_servers SET hostname=%s, label=%s, provider=%s, vcpu_count=%s, memory_gb=%s,
                        ssh_user=%s, ssh_pass=%s, ssh_keys=%s, mdb=%s, mdb_name=%s, mdd=NOW()
                    WHERE server_id=%s
                    """,
                    (srv_hostname, srv_label, srv_provider, vcpu, mem, srv_ssh_user, srv_ssh_pass, srv_ssh_keys, admin.get('user_id',''), admin.get('user_alias',''), server_id)
                )
                if not ok:
                    errors.append({'section': 'server', 'message': 'Gagal memperbarui server.'})
                else:
                    db.commit()
            else:
                if not db.execute_query(
                    """
                    INSERT INTO data_servers (hostname, label, provider, vcpu_count, memory_gb, public_ipv4, ssh_user, ssh_pass, ssh_keys, mdb, mdb_name, mdd)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                    """,
                    (srv_hostname, srv_label, srv_provider, vcpu, mem, srv_public_ipv4, srv_ssh_user, srv_ssh_pass, srv_ssh_keys, admin.get('user_id',''), admin.get('user_alias',''))
                ):
                    errors.append({'section': 'server', 'message': 'Gagal menambahkan server.'})
                else:
                    db.commit()
        # website update or insert
        wid = curr_wid
        if ws_website or ws_website_user or ws_website_pass:
            if wid:
                ok = db.execute_query("UPDATE data_website SET website=%s, website_user=%s, website_pass=%s, mdb=%s, mdb_name=%s, mdd=NOW() WHERE website_id=%s", (
                    ws_website, ws_website_user, ws_website_pass, admin.get('user_id',''), admin.get('user_alias',''), wid
                ))
                if not ok:
                    errors.append({'section': 'website', 'message': 'Gagal memperbarui website.'})
                else:
                    db.commit()
            else:
                if not db.execute_query("INSERT INTO data_website (website, website_user, website_pass, mdb, mdb_name, mdd) VALUES (%s, %s, %s, %s, %s, NOW())", (
                    ws_website, ws_website_user, ws_website_pass, admin.get('user_id',''), admin.get('user_alias','')
                )):
                    errors.append({'section': 'website', 'message': 'Gagal menambahkan website.'})
                else:
                    db.commit()
                    try:
                        wid = db.cur_hris.lastrowid
                    except Exception:
                        wid = None
        # update subdomain
        ok = db.execute_query(
            """
            UPDATE data_subdomain SET subdomain=%s, cloudflare=%s, public_ipv4=%s, website_id=%s, mdb=%s, mdb_name=%s, mdd=NOW()
            WHERE subdomain_id=%s
            """,
            (sd_subdomain, sd_cloudflare, sd_public_ipv4, wid, admin.get('user_id',''), admin.get('user_alias',''), subdomain_id)
        )
        if not ok:
            return JsonResponse({'status': False, 'message': 'Gagal memperbarui subdomain.'}, status=500)
        db.commit()
        if errors:
            return JsonResponse({'status': False, 'message': 'Sebagian data gagal diperbarui.', 'errors': errors}, status=400)
        return JsonResponse({'status': True, 'subdomain_id': subdomain_id})

class TechnicalSubrowDeleteView(View):
    def post(self, request):
        if 'hris_admin' not in request.session:
            return JsonResponse({'status': False, 'message': 'Unauthorized'}, status=401)
        db = data_mysql()
        subdomain_id_raw = (request.POST.get('subdomain_id') or '').strip()
        try:
            subdomain_id = int(subdomain_id_raw)
        except Exception:
            return JsonResponse({'status': False, 'message': 'Subdomain tidak valid'}, status=400)
        wid = None
        sql = """
            SELECT website_id
            FROM data_subdomain
            WHERE subdomain_id = %s
            LIMIT 1
        """
        if db.execute_query(sql, (subdomain_id,)):
            r = db.cur_hris.fetchone() or {}
            try:
                wid = r.get('website_id')
            except AttributeError:
                try:
                    wid = r[0]
                except Exception:
                    wid = None
        db.execute_query("DELETE FROM data_subdomain WHERE subdomain_id = %s", (subdomain_id,))
        db.commit()
        if wid:
            db.execute_query("DELETE FROM data_website WHERE website_id = %s", (wid,))
            db.commit()
        return JsonResponse({'status': True})

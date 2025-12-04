from django.views import View
from django.shortcuts import render, redirect
from django.http import JsonResponse
from django.contrib import messages
from projects.database import data_mysql
from datetime import datetime
import json
import pprint

class DomainIndexView(View):
    def get(self, request):
        if 'hris_admin' not in request.session:
            return redirect('admin_login')
        admin = request.session.get('hris_admin', {})
        active_portal_id = request.session.get('active_portal_id', '12')
        db = data_mysql()
        sql = """
            SELECT domain_id, domain, domain_status, expiration_date,
                   primary_ip, contact_email
            FROM data_domains
            ORDER BY domain ASC
        """
        domains = []
        if db.execute_query(sql):
            domains = db.cur_hris.fetchall() or []
        statuses = ['active','expired','pending_transfer','client_hold','server_hold']
        servers = []
        q_servers = """
            SELECT server_id, label
            FROM data_servers
            ORDER BY label ASC
        """
        if db.execute_query(q_servers):
            servers = db.cur_hris.fetchall() or []
        providers = []
        q_providers = """
            SELECT provider
            FROM data_server_registrar_provider
            ORDER BY provider ASC
        """
        if db.execute_query(q_providers):
            providers = db.cur_hris.fetchall() or []
        ipv4s = []
        q_ipv4s = """
            SELECT DISTINCT public_ipv4
            FROM data_servers
            WHERE public_ipv4 IS NOT NULL AND public_ipv4 <> ''
            ORDER BY public_ipv4 ASC
        """
        if db.execute_query(q_ipv4s):
            ipv4s = db.cur_hris.fetchall() or []
        context = {
            'user': admin,
            'active_portal_id': active_portal_id,
            'domains': domains,
            'statuses': statuses,
            'servers': servers,
            'providers': providers,
            'ipv4s': ipv4s,
        }
        return render(request, 'master/domain/index.html', context)

class DomainCreateView(View):
    def post(self, request):
        if 'hris_admin' not in request.session:
            return redirect('admin_login')
        admin = request.session.get('hris_admin', {})
        db = data_mysql()
        domain = request.POST.get('domain', '').strip()
        domain_status = request.POST.get('domain_status', '').strip() or 'active'
        server_id_raw = request.POST.get('server_id', '').strip()
        server_id = None
        try:
            sid = int(server_id_raw)
            if sid > 0:
                server_id = sid
        except Exception:
            server_id = None
        registration_date_raw = request.POST.get('registration_date', '').strip()
        expiration_date_raw = request.POST.get('expiration_date', '').strip()
        primary_ip = request.POST.get('primary_ip', '').strip()
        contact_email = request.POST.get('contact_email', '').strip()
        provider = request.POST.get('provider', '').strip()
        registrar = request.POST.get('registrar', '').strip()
        nameservers_text = request.POST.get('nameservers', '').strip()
        sub_subdomain = request.POST.getlist('sub_subdomain')
        sub_cloudflare = request.POST.getlist('sub_cloudflare')
        sub_public_ipv4 = request.POST.getlist('sub_public_ipv4')
        sub_website = request.POST.getlist('sub_website')
        sub_website_user = request.POST.getlist('sub_website_user')
        sub_website_pass = request.POST.getlist('sub_website_pass')
        tags_text = request.POST.get('tags', '').strip()
        notes = request.POST.get('notes', '').strip()
        try:
            registration_date = datetime.strptime(registration_date_raw, '%Y-%m-%d') if registration_date_raw else None
        except ValueError:
            registration_date = None
        try:
            expiration_date = datetime.strptime(expiration_date_raw, '%Y-%m-%d') if expiration_date_raw else None
        except ValueError:
            expiration_date = None

        is_ajax = (request.headers.get('x-requested-with') == 'XMLHttpRequest') or (
            request.META.get('HTTP_X_REQUESTED_WITH') == 'XMLHttpRequest'
        )
        if not domain:
            if is_ajax:
                return JsonResponse({'status': False, 'message': 'Domain wajib diisi.'}, status=400)
            messages.error(request, 'Domain wajib diisi.')
            return redirect('/projects/master/domain')

        try:
            nameservers = None
            tags = None
            if nameservers_text:
                nameservers = json.dumps([v.strip() for v in nameservers_text.split(',') if v.strip()])
            if tags_text:
                tags = json.dumps([v.strip() for v in tags_text.split(',') if v.strip()])
            sql = """
                INSERT INTO data_domains
                (domain, server_id, provider, registrar, domain_status, registration_date, expiration_date,
                 nameservers, primary_ip, contact_email, tags, notes, mdb, mdb_name, mdd)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
            """
            params = (
                domain,
                server_id,
                provider,
                registrar,
                domain_status,
                registration_date,
                expiration_date,
                nameservers,
                primary_ip,
                contact_email,
                tags,
                notes,
                admin.get('user_id', ''),
                admin.get('user_alias', ''),
            )
            if db.execute_query(sql, params):
                db.commit()
                did = None
                if db.execute_query("SELECT domain_id FROM data_domains WHERE domain = %s LIMIT 1", (domain,)):
                    row = db.cur_hris.fetchone() or {}
                    try:
                        did = row.get('domain_id')
                    except AttributeError:
                        try:
                            did = row[0]
                        except Exception:
                            did = None
                if did:
                    wids = []
                    if db.execute_query("SELECT website_id FROM data_subdomain WHERE domain_id = %s AND website_id IS NOT NULL", (did,)):
                        rows = db.cur_hris.fetchall() or []
                        for r in rows:
                            try:
                                wid = r.get('website_id')
                            except AttributeError:
                                try:
                                    wid = r[0]
                                except Exception:
                                    wid = None
                            if wid:
                                wids.append(wid)
                    db.execute_query("DELETE FROM data_subdomain WHERE domain_id = %s", (did,))
                    db.commit()
                    for wid in wids:
                        db.execute_query("DELETE FROM data_website WHERE website_id = %s", (wid,))
                    if wids:
                        db.commit()
                    count_rows = max(len(sub_subdomain), len(sub_cloudflare), len(sub_public_ipv4), len(sub_website), len(sub_website_user), len(sub_website_pass)) if any([sub_subdomain, sub_cloudflare, sub_public_ipv4, sub_website, sub_website_user, sub_website_pass]) else 0
                    first_website_id = None
                    for i in range(count_rows):
                        sd = (sub_subdomain[i] if i < len(sub_subdomain) else '').strip()
                        cf = (sub_cloudflare[i] if i < len(sub_cloudflare) else '').strip()
                        ipv4 = (sub_public_ipv4[i] if i < len(sub_public_ipv4) else '').strip()
                        wsite = (sub_website[i] if i < len(sub_website) else '').strip()
                        wuser = (sub_website_user[i] if i < len(sub_website_user) else '').strip()
                        wpass = (sub_website_pass[i] if i < len(sub_website_pass) else '').strip()
                        website_id = None
                        if wsite or wuser or wpass:
                            sql_w = """
                                INSERT INTO data_website (website, website_user, website_pass, mdb, mdb_name, mdd)
                                VALUES (%s, %s, %s, %s, %s, NOW())
                            """
                            params_w = (wsite or None, wuser or None, wpass or None, admin.get('user_id',''), admin.get('user_alias',''))
                            if db.execute_query(sql_w, params_w):
                                db.commit()
                                try:
                                    website_id = db.cur_hris.lastrowid
                                except Exception:
                                    website_id = None
                                if first_website_id is None:
                                    first_website_id = website_id
                        if sd or cf or ipv4 or website_id:
                            sql_sd = """
                                INSERT INTO data_subdomain (subdomain, domain_id, website_id, cloudflare, public_ipv4, mdb, mdb_name, mdd)
                                VALUES (%s, %s, %s, %s, %s, %s, %s, NOW())
                            """
                            params_sd = (sd or None, did, website_id, cf or None, ipv4 or None, admin.get('user_id',''), admin.get('user_alias',''))
                            db.execute_query(sql_sd, params_sd)
                    db.commit()
                    if first_website_id:
                        db.execute_query("UPDATE data_domains SET website_id=%s WHERE domain_id=%s", (first_website_id, did))
                        db.commit()
                if is_ajax:
                    return JsonResponse({'status': True, 'domain_id': did})
                messages.success(request, 'Domain berhasil ditambahkan.')
            else:
                if is_ajax:
                    return JsonResponse({'status': False, 'message': 'Gagal menambahkan domain.'}, status=500)
                messages.error(request, 'Gagal menambahkan domain.')
        except Exception as e:
            if is_ajax:
                return JsonResponse({'status': False, 'message': str(e)}, status=500)
            messages.error(request, f'Gagal menambahkan domain: {str(e)}')
        return redirect('/projects/master/domain')

class DomainEditView(View):
    def get(self, request, domain_id):
        if 'hris_admin' not in request.session:
            return redirect('admin_login')
        admin = request.session.get('hris_admin', {})
        active_portal_id = request.session.get('active_portal_id', '12')
        db = data_mysql()
        sql = """
            SELECT domain_id, domain, server_id, provider, registrar, domain_status,
                   registration_date, expiration_date, nameservers, primary_ip,
                   contact_email, tags, notes, website_id
            FROM data_domains
            WHERE domain_id = %s
            LIMIT 1
        """
        domain_row = None
        if db.execute_query(sql, (domain_id,)):
            domain_row = db.cur_hris.fetchone() or {}
        def _json_to_text(val):
            if val is None:
                return ''
            try:
                obj = val
                if isinstance(obj, (bytes, bytearray)):
                    obj = obj.decode('utf-8')
                obj = json.loads(obj) if isinstance(obj, str) else obj
                if isinstance(obj, list):
                    return ', '.join([str(x) for x in obj])
                return str(obj)
            except Exception:
                try:
                    return str(val)
                except Exception:
                    return ''
        nameservers_text = _json_to_text(domain_row.get('nameservers') if isinstance(domain_row, dict) else None)
        tags_text = _json_to_text(domain_row.get('tags') if isinstance(domain_row, dict) else None)
        subrows = []
        q_sub = """
            SELECT s.subdomain, s.cloudflare, s.public_ipv4, s.website_id,
                   w.website, w.website_user, w.website_pass
            FROM data_subdomain s
            LEFT JOIN data_website w ON w.website_id = s.website_id
            WHERE s.domain_id = %s
            ORDER BY s.subdomain ASC
        """
        if db.execute_query(q_sub, (domain_id,)):
            subrows = db.cur_hris.fetchall() or []
        statuses = ['active','expired','pending_transfer','client_hold','server_hold']
        servers = []
        q_servers = """
            SELECT server_id, label
            FROM data_servers
            ORDER BY label ASC
        """
        if db.execute_query(q_servers):
            servers = db.cur_hris.fetchall() or []
        providers = []
        q_providers = """
            SELECT provider
            FROM data_server_registrar_provider
            ORDER BY provider ASC
        """
        if db.execute_query(q_providers):
            providers = db.cur_hris.fetchall() or []
        ipv4s = []
        q_ipv4s = """
            SELECT DISTINCT public_ipv4
            FROM data_servers
            WHERE public_ipv4 IS NOT NULL AND public_ipv4 <> ''
            ORDER BY public_ipv4 ASC
        """
        if db.execute_query(q_ipv4s):
            ipv4s = db.cur_hris.fetchall() or []
        context = {
            'user': admin,
            'active_portal_id': active_portal_id,
            'domain': domain_row,
            'servers': servers,
            'nameservers_text': nameservers_text,
            'tags_text': tags_text,
            'subrows': subrows,
            'statuses': statuses,
            'providers': providers,
            'ipv4s': ipv4s,
        }
        return render(request, 'master/domain/edit.html', context)

    def post(self, request, domain_id):
        if 'hris_admin' not in request.session:
            return redirect('admin_login')
        admin = request.session.get('hris_admin', {})
        db = data_mysql()
        domain = request.POST.get('domain', '').strip()
        domain_status = request.POST.get('domain_status', '').strip() or 'active'
        server_id_raw = request.POST.get('server_id', '').strip()
        server_id = None
        try:
            sid = int(server_id_raw)
            if sid > 0:
                server_id = sid
        except Exception:
            server_id = None
        registration_date_raw = request.POST.get('registration_date', '').strip()
        expiration_date_raw = request.POST.get('expiration_date', '').strip()
        primary_ip = request.POST.get('primary_ip', '').strip() or None
        contact_email = request.POST.get('contact_email', '').strip() or None
        provider = request.POST.get('provider', '').strip() or None
        registrar = request.POST.get('registrar', '').strip() or None
        nameservers_text = request.POST.get('nameservers', '').strip()
        sub_subdomain = request.POST.getlist('sub_subdomain')
        sub_cloudflare = request.POST.getlist('sub_cloudflare')
        sub_public_ipv4 = request.POST.getlist('sub_public_ipv4')
        sub_website = request.POST.getlist('sub_website')
        sub_website_user = request.POST.getlist('sub_website_user')
        sub_website_pass = request.POST.getlist('sub_website_pass')
        tags_text = request.POST.get('tags', '').strip()
        notes = request.POST.get('notes', '').strip() or None
        try:
            registration_date = datetime.strptime(registration_date_raw, '%Y-%m-%d') if registration_date_raw else None
        except ValueError:
            registration_date = None
        try:
            expiration_date = datetime.strptime(expiration_date_raw, '%Y-%m-%d') if expiration_date_raw else None
        except ValueError:
            expiration_date = None
        is_ajax = (request.headers.get('x-requested-with') == 'XMLHttpRequest') or (
            request.META.get('HTTP_X_REQUESTED_WITH') == 'XMLHttpRequest'
        )
        if not domain:
            if is_ajax:
                return JsonResponse({'status': False, 'message': 'Domain wajib diisi.'}, status=400)
            messages.error(request, 'Domain wajib diisi.')
            return redirect('/projects/master/domain')
        try:
            nameservers = None
            tags = None
            if nameservers_text:
                nameservers = json.dumps([v.strip() for v in nameservers_text.split(',') if v.strip()])
            if tags_text:
                tags = json.dumps([v.strip() for v in tags_text.split(',') if v.strip()])
            sql = """
                UPDATE data_domains SET
                    domain = %s,
                    server_id = %s,
                    provider = %s,
                    registrar = %s,
                    domain_status = %s,
                    registration_date = %s,
                    expiration_date = %s,
                    nameservers = %s,
                    primary_ip = %s,
                    contact_email = %s,
                    tags = %s,
                    notes = %s,
                    mdb = %s,
                    mdb_name = %s,
                    mdd = NOW()
                WHERE domain_id = %s
            """
            params = (
                domain,
                server_id,
                provider,
                registrar,
                domain_status,
                registration_date,
                expiration_date,
                nameservers,
                primary_ip,
                contact_email,
                tags,
                notes,
                admin.get('user_id', ''),
                admin.get('user_alias', ''),
                domain_id,
            )
            if db.execute_query(sql, params):
                db.commit()
                wids_old = []
                if db.execute_query("SELECT subdomain, website_id FROM data_subdomain WHERE domain_id = %s", (domain_id,)):
                    rows_old = db.cur_hris.fetchall() or []
                    existing = {}
                    for r in rows_old:
                        try:
                            sdname = r.get('subdomain')
                            wid = r.get('website_id')
                        except AttributeError:
                            try:
                                sdname = r[0]
                                wid = r[1]
                            except Exception:
                                sdname = None
                                wid = None
                        if sdname:
                            existing[sdname] = wid
                    new_set = set([(s or '').strip() for s in sub_subdomain if (s or '').strip()])
                    for sdname, wid in existing.items():
                        if sdname not in new_set and wid:
                            wids_old.append(wid)
                db.execute_query("DELETE FROM data_subdomain WHERE domain_id = %s", (domain_id,))
                db.commit()
                if wids_old:
                    curr_wid = None
                    if db.execute_query("SELECT website_id FROM data_domains WHERE domain_id = %s LIMIT 1", (domain_id,)):
                        rr = db.cur_hris.fetchone() or {}
                        try:
                            curr_wid = rr.get('website_id')
                        except AttributeError:
                            try:
                                curr_wid = rr[0]
                            except Exception:
                                curr_wid = None
                    if curr_wid and curr_wid in wids_old:
                        db.execute_query("UPDATE data_domains SET website_id = NULL WHERE domain_id = %s", (domain_id,))
                        db.commit()
                for wid in wids_old:
                    db.execute_query("DELETE FROM data_website WHERE website_id = %s", (wid,))
                if wids_old:
                    db.commit()
                count_rows = max(len(sub_subdomain), len(sub_cloudflare), len(sub_public_ipv4), len(sub_website), len(sub_website_user), len(sub_website_pass)) if any([sub_subdomain, sub_cloudflare, sub_public_ipv4, sub_website, sub_website_user, sub_website_pass]) else 0
                first_website_id = None
                for i in range(count_rows):
                    sd = (sub_subdomain[i] if i < len(sub_subdomain) else '').strip()
                    cf = (sub_cloudflare[i] if i < len(sub_cloudflare) else '').strip()
                    ipv4 = (sub_public_ipv4[i] if i < len(sub_public_ipv4) else '').strip()
                    wsite = (sub_website[i] if i < len(sub_website) else '').strip()
                    wuser = (sub_website_user[i] if i < len(sub_website_user) else '').strip()
                    wpass = (sub_website_pass[i] if i < len(sub_website_pass) else '').strip()
                    website_id = None
                    if wsite or wuser or wpass:
                        sql_w = """
                            INSERT INTO data_website (website, website_user, website_pass, mdb, mdb_name, mdd)
                            VALUES (%s, %s, %s, %s, %s, NOW())
                        """
                        params_w = (wsite or None, wuser or None, wpass or None, admin.get('user_id',''), admin.get('user_alias',''))
                        if db.execute_query(sql_w, params_w):
                            db.commit()
                            try:
                                website_id = db.cur_hris.lastrowid
                            except Exception:
                                website_id = None
                            if first_website_id is None:
                                first_website_id = website_id
                    if sd or cf or ipv4 or website_id:
                        sql_sd = """
                            INSERT INTO data_subdomain (subdomain, domain_id, website_id, cloudflare, public_ipv4, mdb, mdb_name, mdd)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, NOW())
                        """
                        params_sd = (sd or None, domain_id, website_id, cf or None, ipv4 or None, admin.get('user_id',''), admin.get('user_alias',''))
                        db.execute_query(sql_sd, params_sd)
                db.commit()
                if first_website_id:
                    db.execute_query("UPDATE data_domains SET website_id=%s WHERE domain_id=%s", (first_website_id, domain_id))
                    db.commit()
                if is_ajax:
                    return JsonResponse({'status': True})
                messages.success(request, 'Domain berhasil diperbarui.')
                return redirect('/projects/master/domain')
            else:
                if is_ajax:
                    return JsonResponse({'status': False, 'message': 'Gagal memperbarui domain.'}, status=500)
                messages.error(request, 'Gagal memperbarui domain.')
        except Exception as e:
            if is_ajax:
                return JsonResponse({'status': False, 'message': str(e)}, status=500)
            messages.error(request, f'Gagal memperbarui domain: {str(e)}')
        return redirect('/projects/master/domain')

class DomainDeleteView(View):
    def post(self, request, domain_id):
        if 'hris_admin' not in request.session:
            return redirect('admin_login')
        db = data_mysql()
        sql = "DELETE FROM data_domains WHERE domain_id = %s"
        if db.execute_query(sql, (domain_id,)):
            db.commit()
            messages.success(request, 'Domain berhasil dihapus.')
        else:
            messages.error(request, 'Gagal menghapus domain.')
        return redirect('/projects/master/domain')

class ServerIndexView(View):
    def get(self, request):
        if 'hris_admin' not in request.session:
            return redirect('admin_login')
        admin = request.session.get('hris_admin', {})
        active_portal_id = request.session.get('active_portal_id', '12')
        db = data_mysql()
        sql = """
            SELECT server_id, hostname, label, region, server_status, public_ipv4, expires_at
            FROM data_servers
            ORDER BY mdd DESC
        """
        servers = []
        if db.execute_query(sql):
            servers = db.cur_hris.fetchall() or []
        statuses = ['active','stopped','suspended','terminated','provisioning','error']
        archs = ['x86_64','arm64','i386','armhf']
        disk_types = ['HDD','SSD','NVMe']
        currencies = ['IDR','USD','EUR']
        providers = []
        q_providers = """
            SELECT provider
            FROM data_server_registrar_provider
            ORDER BY provider ASC
        """
        if db.execute_query(q_providers):
            providers = db.cur_hris.fetchall() or []
        context = {
            'user': admin,
            'active_portal_id': active_portal_id,
            'servers': servers,
            'statuses': statuses,
            'archs': archs,
            'disk_types': disk_types,
            'currencies': currencies,
            'providers': providers,
        }
        return render(request, 'master/server/index.html', context)

class ServerCreateView(View):
    def post(self, request):
        if 'hris_admin' not in request.session:
            return redirect('admin_login')
        admin = request.session.get('hris_admin', {})
        db = data_mysql()
        hostname = request.POST.get('hostname', '').strip()
        label = request.POST.get('label', '').strip() or None
        provider = request.POST.get('provider', '').strip() or None
        region = request.POST.get('region', '').strip() or None
        location = request.POST.get('location', '').strip() or None
        server_code = request.POST.get('server_code', '').strip() or None
        server_name = request.POST.get('server_name', '').strip() or None
        server_status = request.POST.get('server_status', '').strip() or 'active'
        os_name = request.POST.get('os_name', '').strip() or None
        os_version = request.POST.get('os_version', '').strip() or None
        arch = request.POST.get('arch', '').strip() or None
        vcpu_count_raw = request.POST.get('vcpu_count', '').strip()
        memory_gb = request.POST.get('memory_gb', '').strip()
        swap_mb = request.POST.get('swap_mb', '').strip() or 0
        disk_gb = request.POST.get('disk_gb', '').strip()
        disk_type = request.POST.get('disk_type', '').strip() or None
        bandwidth_tb = request.POST.get('bandwidth_tb', '').strip() or 0
        network_speed_mbps = request.POST.get('network_speed_mbps', '').strip() or 0
        public_ipv4 = request.POST.get('public_ipv4', '').strip() or None
        public_ipv6 = request.POST.get('public_ipv6', '').strip() or None
        private_ipv4 = request.POST.get('private_ipv4', '').strip() or None
        private_ipv6 = request.POST.get('private_ipv6', '').strip() or None
        ssh_port_raw = request.POST.get('ssh_port', '').strip()
        ssh_user = request.POST.get('ssh_user', '').strip() or None
        ssh_fingerprint = request.POST.get('ssh_fingerprint', '').strip() or None
        ssh_keys_text = request.POST.get('ssh_keys', '').strip()
        ssh_pass = request.POST.get('ssh_pass', '').strip() or None
        cost_monthly_raw = request.POST.get('cost_monthly', '').strip()
        currency = request.POST.get('currency', '').strip() or 'IDR'
        currency_conversion_idr = request.POST.get('currency_conversion_idr', '').strip() or 0
        purchased_at_raw = request.POST.get('purchased_at', '').strip()
        expires_at_raw = request.POST.get('expires_at', '').strip()
        notes = request.POST.get('notes', '').strip() or None
        is_ajax = (request.headers.get('x-requested-with') == 'XMLHttpRequest') or (
            request.META.get('HTTP_X_REQUESTED_WITH') == 'XMLHttpRequest'
        )
        if not hostname:
            if is_ajax:
                return JsonResponse({'status': False, 'message': 'Hostname wajib diisi.'}, status=400)
            messages.error(request, 'Hostname wajib diisi.')
            return redirect('/projects/master/server')
        try:
            vcpu_count = int(vcpu_count_raw or 0) or None
        except Exception:
            vcpu_count = None
        try:
            ssh_port = int(ssh_port_raw) if ssh_port_raw else 22
        except Exception:
            ssh_port = 22
        try:
            cost_monthly = float(cost_monthly_raw) if cost_monthly_raw else None
        except Exception:
            cost_monthly = None
        try:
            purchased_at = datetime.strptime(purchased_at_raw, '%Y-%m-%d %H:%M:%S') if purchased_at_raw else None
        except Exception:
            try:
                purchased_at = datetime.strptime(purchased_at_raw, '%Y-%m-%d') if purchased_at_raw else None
            except Exception:
                purchased_at = None
        try:
            expires_at = datetime.strptime(expires_at_raw, '%Y-%m-%d %H:%M:%S') if expires_at_raw else None
        except Exception:
            try:
                expires_at = datetime.strptime(expires_at_raw, '%Y-%m-%d') if expires_at_raw else None
            except Exception:
                expires_at = None
        ssh_keys = None
        if ssh_keys_text:
            try:
                ssh_keys = json.dumps([v.strip() for v in ssh_keys_text.split(',') if v.strip()])
            except Exception:
                ssh_keys = None
        sql = """
            INSERT INTO data_servers
            (hostname, label, provider, region, location, server_code, server_name, server_status,
             os_name, os_version, arch, vcpu_count, memory_gb, swap_mb, disk_gb, disk_type,
             bandwidth_tb, network_speed_mbps, public_ipv4, public_ipv6, private_ipv4, private_ipv6,
             ssh_port, ssh_user, ssh_fingerprint, ssh_keys, ssh_pass, cost_monthly, currency,
             currency_conversion_idr, purchased_at, expires_at, notes, mdb, mdb_name)
            VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s
            )
        """
        params = (
            hostname,
            label,
            provider,
            region,
            location,
            server_code,
            server_name,
            server_status,
            os_name,
            os_version,
            arch,
            vcpu_count,
            memory_gb,
            swap_mb,
            disk_gb,
            disk_type,
            bandwidth_tb,
            network_speed_mbps,
            public_ipv4,
            public_ipv6,
            private_ipv4,
            private_ipv6,
            ssh_port,
            ssh_user,
            ssh_fingerprint,
            ssh_keys,
            ssh_pass,
            cost_monthly,
            currency,
            currency_conversion_idr,
            purchased_at,
            expires_at,
            notes,
            admin.get('user_id', ''),
            admin.get('user_alias', ''),
        )
        pprint.pprint(params)
        ok = False
        try:
            ok = db.execute_query(sql, params)
        except Exception as e:
            if is_ajax:
                return JsonResponse({'status': False, 'message': str(e)}, status=500)
            messages.error(request, f'Gagal menambahkan server: {str(e)}')
            return redirect('/projects/master/server')
        if ok:
            db.commit()
            if is_ajax:
                return JsonResponse({'status': True})
            messages.success(request, 'Server berhasil ditambahkan.')
        else:
            if is_ajax:
                return JsonResponse({'status': False, 'message': 'Gagal menambahkan server.'}, status=500)
            messages.error(request, 'Gagal menambahkan server.')
        return redirect('/projects/master/server')

class ServerEditView(View):
    def get(self, request, server_id):
        if 'hris_admin' not in request.session:
            return redirect('admin_login')
        admin = request.session.get('hris_admin', {})
        active_portal_id = request.session.get('active_portal_id', '12')
        db = data_mysql()
        sql = """
            SELECT * FROM data_servers WHERE server_id = %s LIMIT 1
        """
        row = None
        if db.execute_query(sql, (server_id,)):
            row = db.cur_hris.fetchone() or {}
        statuses = ['active','stopped','suspended','terminated','provisioning','error']
        archs = ['x86_64','arm64','i386','armhf']
        disk_types = ['HDD','SSD','NVMe']
        currencies = ['IDR','USD','EUR']
        providers = []
        q_providers = """
            SELECT provider
            FROM data_server_registrar_provider
            ORDER BY provider ASC
        """
        if db.execute_query(q_providers):
            providers = db.cur_hris.fetchall() or []
        def _json_to_text(val):
            if val is None:
                return ''
            try:
                obj = val
                if isinstance(obj, (bytes, bytearray)):
                    obj = obj.decode('utf-8')
                obj = json.loads(obj) if isinstance(obj, str) else obj
                if isinstance(obj, list):
                    return ', '.join([str(x) for x in obj])
                return str(obj)
            except Exception:
                try:
                    return str(val)
                except Exception:
                    return ''
        ssh_keys_text = _json_to_text(row.get('ssh_keys') if isinstance(row, dict) else None)
        context = {
            'user': admin,
            'active_portal_id': active_portal_id,
            'server': row,
            'statuses': statuses,
            'archs': archs,
            'disk_types': disk_types,
            'currencies': currencies,
            'ssh_keys_text': ssh_keys_text,
            'providers': providers,
        }
        return render(request, 'master/server/edit.html', context)

    def post(self, request, server_id):
        if 'hris_admin' not in request.session:
            return redirect('admin_login')
        admin = request.session.get('hris_admin', {})
        db = data_mysql()
        hostname = request.POST.get('hostname', '').strip()
        label = request.POST.get('label', '').strip() or None
        provider = request.POST.get('provider', '').strip() or None
        region = request.POST.get('region', '').strip() or None
        location = request.POST.get('location', '').strip() or None
        server_code = request.POST.get('server_code', '').strip() or None
        server_name = request.POST.get('server_name', '').strip() or None
        server_status = request.POST.get('server_status', '').strip() or 'active'
        os_name = request.POST.get('os_name', '').strip() or None
        os_version = request.POST.get('os_version', '').strip() or None
        arch = request.POST.get('arch', '').strip() or None
        vcpu_count_raw = request.POST.get('vcpu_count', '').strip()
        memory_gb_raw = request.POST.get('memory_gb', '').strip()
        swap_mb_raw = request.POST.get('swap_mb', '').strip()
        disk_gb_raw = request.POST.get('disk_gb', '').strip()
        disk_type = request.POST.get('disk_type', '').strip() or None
        bandwidth_tb_raw = request.POST.get('bandwidth_tb', '').strip()
        network_speed_mbps_raw = request.POST.get('network_speed_mbps', '').strip()
        public_ipv4 = request.POST.get('public_ipv4', '').strip() or None
        public_ipv6 = request.POST.get('public_ipv6', '').strip() or None
        private_ipv4 = request.POST.get('private_ipv4', '').strip() or None
        private_ipv6 = request.POST.get('private_ipv6', '').strip() or None
        ssh_port_raw = request.POST.get('ssh_port', '').strip()
        ssh_user = request.POST.get('ssh_user', '').strip() or None
        ssh_fingerprint = request.POST.get('ssh_fingerprint', '').strip() or None
        ssh_keys_text = request.POST.get('ssh_keys', '').strip()
        ssh_pass = request.POST.get('ssh_pass', '').strip() or None
        cost_monthly_raw = request.POST.get('cost_monthly', '').strip()
        currency = request.POST.get('currency', '').strip() or 'IDR'
        currency_conversion_idr_raw = request.POST.get('currency_conversion_idr', '').strip()
        purchased_at_raw = request.POST.get('purchased_at', '').strip()
        expires_at_raw = request.POST.get('expires_at', '').strip()
        notes = request.POST.get('notes', '').strip() or None
        is_ajax = (request.headers.get('x-requested-with') == 'XMLHttpRequest') or (
            request.META.get('HTTP_X_REQUESTED_WITH') == 'XMLHttpRequest'
        )
        if not hostname:
            if is_ajax:
                return JsonResponse({'status': False, 'message': 'Hostname wajib diisi.'}, status=400)
            messages.error(request, 'Hostname wajib diisi.')
            return redirect('/projects/master/server')
        try:
            vcpu_count = int(vcpu_count_raw or 0) or None
        except Exception:
            vcpu_count = None
        try:
            memory_gb = int(memory_gb_raw or 0) or None
        except Exception:
            memory_gb = None
        try:
            swap_mb = int(swap_mb_raw) if swap_mb_raw else None
        except Exception:
            swap_mb = None
        try:
            disk_gb = float(disk_gb_raw) if disk_gb_raw else None
        except Exception:
            disk_gb = None
        try:
            bandwidth_tb = float(bandwidth_tb_raw) if bandwidth_tb_raw else None
        except Exception:
            bandwidth_tb = None
        try:
            network_speed_mbps = int(network_speed_mbps_raw) if network_speed_mbps_raw else None
        except Exception:
            network_speed_mbps = None
        try:
            ssh_port = int(ssh_port_raw) if ssh_port_raw else 22
        except Exception:
            ssh_port = 22
        try:
            cost_monthly = float(cost_monthly_raw) if cost_monthly_raw else None
        except Exception:
            cost_monthly = None
        try:
            currency_conversion_idr = float(currency_conversion_idr_raw) if currency_conversion_idr_raw else None
        except Exception:
            currency_conversion_idr = None
        try:
            purchased_at = datetime.strptime(purchased_at_raw, '%Y-%m-%d %H:%M:%S') if purchased_at_raw else None
        except Exception:
            try:
                purchased_at = datetime.strptime(purchased_at_raw, '%Y-%m-%d') if purchased_at_raw else None
            except Exception:
                purchased_at = None
        try:
            expires_at = datetime.strptime(expires_at_raw, '%Y-%m-%d %H:%M:%S') if expires_at_raw else None
        except Exception:
            try:
                expires_at = datetime.strptime(expires_at_raw, '%Y-%m-%d') if expires_at_raw else None
            except Exception:
                expires_at = None
        ssh_keys = None
        if ssh_keys_text:
            try:
                ssh_keys = json.dumps([v.strip() for v in ssh_keys_text.split(',') if v.strip()])
            except Exception:
                ssh_keys = None
        sql = """
            UPDATE data_servers SET
                hostname = %s,
                label = %s,
                provider = %s,
                region = %s,
                location = %s,
                server_code = %s,
                server_name = %s,
                server_status = %s,
                os_name = %s,
                os_version = %s,
                arch = %s,
                vcpu_count = %s,
                memory_gb = %s,
                swap_mb = %s,
                disk_gb = %s,
                disk_type = %s,
                bandwidth_tb = %s,
                network_speed_mbps = %s,
                public_ipv4 = %s,
                public_ipv6 = %s,
                private_ipv4 = %s,
                private_ipv6 = %s,
                ssh_port = %s,
                ssh_user = %s,
                ssh_fingerprint = %s,
                ssh_keys = %s,
                ssh_pass = %s,
                cost_monthly = %s,
                currency = %s,
                currency_conversion_idr = %s,
                purchased_at = %s,
                expires_at = %s,
                notes = %s,
                mdb = %s,
                mdb_name = %s,
                mdd = NOW()
            WHERE server_id = %s
        """
        params = (
            hostname,
            label,
            provider,
            region,
            location,
            server_code,
            server_name,
            server_status,
            os_name,
            os_version,
            arch,
            vcpu_count,
            memory_gb,
            swap_mb,
            disk_gb,
            disk_type,
            bandwidth_tb,
            network_speed_mbps,
            public_ipv4,
            public_ipv6,
            private_ipv4,
            private_ipv6,
            ssh_port,
            ssh_user,
            ssh_fingerprint,
            ssh_keys,
            ssh_pass,
            cost_monthly,
            currency,
            currency_conversion_idr,
            purchased_at,
            expires_at,
            notes,
            admin.get('user_id', ''),
            admin.get('user_alias', ''),
            server_id,
        )
        if db.execute_query(sql, params):
            db.commit()
            if is_ajax:
                return JsonResponse({'status': True})
            messages.success(request, 'Server berhasil diperbarui.')
            return redirect('/projects/master/server')
        else:
            if is_ajax:
                return JsonResponse({'status': False, 'message': 'Gagal memperbarui server.'}, status=500)
            messages.error(request, 'Gagal memperbarui server.')
            return redirect('/projects/master/server')

class ServerDeleteView(View):
    def post(self, request, server_id):
        if 'hris_admin' not in request.session:
            return redirect('admin_login')
        db = data_mysql()
        sql = "DELETE FROM data_servers WHERE server_id = %s"
        if db.execute_query(sql, (server_id,)):
            db.commit()
            messages.success(request, 'Server berhasil dihapus.')
        else:
            messages.error(request, 'Gagal menghapus server.')
        return redirect('/projects/master/server')

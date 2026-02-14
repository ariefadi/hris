from django.views import View
from django.shortcuts import render, redirect
from django.http import JsonResponse
from django.utils.decorators import method_decorator
from django.views.decorators.csrf import csrf_exempt
from django.contrib import messages
from settings.database import data_mysql
from datetime import datetime, date, time
from django.conf import settings as django_settings
from django.urls import reverse
from urllib.parse import urlparse
import os
import uuid


def _get_media_root():
    media_root = getattr(django_settings, 'MEDIA_ROOT', None)
    if media_root:
        return str(media_root)
    try:
        return str(django_settings.BASE_DIR / 'media')
    except Exception:
        return os.path.abspath('media')


def _save_user_photo(uploaded_file, user_id):
    try:
        content_type = getattr(uploaded_file, 'content_type', '') or ''
        if not content_type.startswith('image/'):
            return (False, None, 'File harus berupa gambar')

        size = int(getattr(uploaded_file, 'size', 0) or 0)
        if size <= 0:
            return (False, None, 'File tidak valid')
        if size > 2 * 1024 * 1024:
            return (False, None, 'Ukuran file maksimal 2MB')

        original_name = getattr(uploaded_file, 'name', '') or ''
        _, ext = os.path.splitext(original_name)
        ext = (ext or '').lower()
        allowed_ext = {'.jpg', '.jpeg', '.png', '.webp'}
        if ext not in allowed_ext:
            ext = '.jpg'

        media_root = _get_media_root()
        rel_dir = os.path.join('users', str(user_id))
        abs_dir = os.path.join(media_root, rel_dir)
        os.makedirs(abs_dir, exist_ok=True)

        filename = f"{uuid.uuid4().hex}{ext}"
        abs_path = os.path.join(abs_dir, filename)

        with open(abs_path, 'wb') as f:
            for chunk in uploaded_file.chunks():
                f.write(chunk)

        url_path = f"/media/{rel_dir}/{filename}".replace('\\\\', '/')
        return (True, url_path, None)
    except Exception:
        return (False, None, 'Gagal menyimpan foto')


def _is_safe_internal_url(url):
    if not url:
        return False
    try:
        parsed = urlparse(str(url))
    except Exception:
        return False
    if parsed.scheme or parsed.netloc:
        return False
    path = parsed.path or ''
    return path.startswith('/')


def _normalize_back_url(request, candidate):
    if not candidate or not _is_safe_internal_url(candidate):
        return None
    try:
        cand_path = urlparse(str(candidate)).path or ''
        if cand_path == request.path:
            return None
    except Exception:
        return None
    return str(candidate)


def _get_profile_back_url(request):
    fallback = reverse('dashboard_admin')

    candidates = [
        request.GET.get('next'),
        request.session.get('profile_back_url'),
        request.META.get('HTTP_REFERER'),
    ]

    for c in candidates:
        resolved = _normalize_back_url(request, c)
        if resolved:
            try:
                request.session['profile_back_url'] = resolved
            except Exception:
                pass
            return resolved

    return fallback


def _json_safe(value):
    if value is None:
        return None

    if isinstance(value, datetime):
        try:
            return value.isoformat(sep=' ', timespec='seconds')
        except Exception:
            return str(value)

    if isinstance(value, date):
        try:
            return value.isoformat()
        except Exception:
            return str(value)

    if isinstance(value, time):
        try:
            return value.isoformat(timespec='seconds')
        except Exception:
            return str(value)

    if isinstance(value, (bytes, bytearray)):
        try:
            return value.decode('utf-8')
        except Exception:
            return str(value)

    if isinstance(value, dict):
        return {str(k): _json_safe(v) for k, v in value.items()}

    if isinstance(value, (list, tuple, set)):
        return [_json_safe(v) for v in value]

    return value


def _set_hris_admin_session(request, user_data):
    try:
        current = request.session.get('hris_admin') or {}
        src = user_data if isinstance(user_data, dict) else {}

        # Simpan subset field yang dibutuhkan UI (hindari user_pass dan field datetime)
        allowed_keys = [
            'user_id',
            'user_name',
            'user_alias',
            'user_mail',
            'user_telp',
            'user_alamat',
            'user_st',
            'user_foto',
        ]
        safe_payload = {k: src.get(k) for k in allowed_keys if k in src}

        request.session['hris_admin'] = {**current, **_json_safe(safe_payload)}
        request.session.modified = True
    except Exception:
        pass


class DataLoginUser(View):
    def dispatch(self, request, *args, **kwargs):
        if 'hris_admin' not in request.session:
            return redirect('admin_login')
        return super().dispatch(request, *args, **kwargs)

    def get(self, request):
        context = {
            'title': 'Data Login User',
            'user': request.session.get('hris_admin', {}),
        }
        return render(request, 'users/login_activity/index.html', context)

class page_login_user(View):
    def dispatch(self, request, *args, **kwargs):
        if 'hris_admin' not in request.session:
            return redirect('admin_login')
        elif 'hris_admin' not in request.session:
            return redirect('user_login')
        return super(page_login_user, self).dispatch(request, *args, **kwargs)
    def get(self, req):
        data_login_user = data_mysql().data_login_user()['data']
        hasil = {
            'hasil': "Data Login User",
            'data_login_user': data_login_user
        }
        return JsonResponse(hasil)

class MasterPlan(View):
    def dispatch(self, request, *args, **kwargs):
        if 'hris_admin' not in request.session:
            return redirect('admin_login')
        return super(MasterPlan, self).dispatch(request, *args, **kwargs)

    def get(self, req):
        data = {
            'title': 'Data Master Plan',
            'user': req.session['hris_admin'],
        }
        return render(req, 'users/master_plan/index.html', data)
    
class page_master_plan(View):
    def dispatch(self, request, *args, **kwargs):
        if 'hris_admin' not in request.session:
            return redirect('admin_login')
        elif 'hris_admin' not in request.session:
            return redirect('user_login')
        return super(page_master_plan, self).dispatch(request, *args, **kwargs)
    def get(self, req):
        data_master_plan = data_mysql().data_master_plan()['data']
        hasil = {
            'hasil': "Data Master Plan",
            'data_master_plan': data_master_plan
        }
        return JsonResponse(hasil)


class page_detail_master_plan(View):
    def dispatch(self, request, *args, **kwargs):
        if 'hris_admin' not in request.session:
            return redirect('admin_login')
        return super(page_detail_master_plan, self).dispatch(request, *args, **kwargs)
        
    def get(self, request, master_plan_id):
        try:
            # Ambil data master plan berdasarkan ID
            db = data_mysql()
            result = db.get_master_plan_by_id(master_plan_id)
            
            if result['status']:
                context = {
                    'master_plan_data': result['data'],
                    'master_plan_id': master_plan_id,
                    'title': 'Detail Master Plan',
                    'user': request.session['hris_admin']
                }
                return render(request, 'users/master_plan/detail.html', context)
            else:
                messages.error(request, 'Data master plan tidak ditemukan')
                return redirect('master_plan')
                
        except Exception as e:
            messages.error(request, f'Terjadi error: {str(e)}')
            return redirect('master_plan')

    def post(self, request, master_plan_id):
        try:
            # Handle update master plan
            data = {
                'master_plan_id': master_plan_id,
                'master_task_code': request.POST.get('master_task_code'),
                'master_task_plan': request.POST.get('master_task_plan'),
                'project_kategori': request.POST.get('project_kategori'),
                'urgency': request.POST.get('urgency'),
                'execute_status': request.POST.get('execute_status'),
                'catatan': request.POST.get('catatan'),
                'assignment_to': request.POST.get('assignment_to')
            }
            
            db = data_mysql()
            result = db.update_master_plan(data)
            
            if result['status']:
                messages.success(request, 'Master plan berhasil diupdate')
            else:
                messages.error(request, 'Gagal mengupdate master plan')
                
            return redirect('master_plan')
            
        except Exception as e:
            messages.error(request, f'Terjadi error: {str(e)}')
            return redirect('master_plan')


class add_master_plan(View):
    def dispatch(self, request, *args, **kwargs):
        if 'hris_admin' not in request.session:
            return redirect('admin_login')
        return super(add_master_plan, self).dispatch(request, *args, **kwargs)
        
    def get(self, request):
        # Ambil data users untuk dropdown assignment
        db = data_mysql()
        users_result = db.data_user_by_params()
        
        context = {
            'title': 'Tambah Master Plan',
            'user': request.session['hris_admin'],
            'users': users_result['data'] if users_result['status'] else []
        }
        return render(request, 'users/master_plan/add.html', context)


class post_tambah_master_plan(View):
    def dispatch(self, request, *args, **kwargs):
        if 'hris_admin' not in request.session:
            return redirect('admin_login')
        return super(post_tambah_master_plan, self).dispatch(request, *args, **kwargs)
        
    def post(self, request):
        try:
            # Generate UUID untuk master_plan_id
            import uuid
            master_plan_id = str(uuid.uuid4())
            
            # Ambil data dari form
            data = {
                'master_plan_id': master_plan_id,
                'master_task_code': request.POST.get('master_task_code'),
                'master_task_plan': request.POST.get('master_task_plan'),
                'project_kategori': request.POST.get('project_kategori'),
                'urgency': request.POST.get('urgency'),
                'execute_status': request.POST.get('execute_status', 'Pending'),
                'catatan': request.POST.get('catatan', ''),
                'submitted_task': request.session['hris_admin']['user_id'],  # User yang login
                'assignment_to': request.POST.get('assignment_to')
            }
            
            # Validasi data required
            required_fields = ['master_task_code', 'master_task_plan', 'project_kategori', 'urgency']
            for field in required_fields:
                if not data[field]:
                    messages.error(request, f'Field {field} harus diisi')
                    return redirect('add_master_plan')
            
            # Insert ke database
            db = data_mysql()
            result = db.insert_master_plan(data)
            
            if result['status']:
                messages.success(request, 'Master plan berhasil ditambahkan')
                return redirect('master_plan')
            else:
                messages.error(request, f'Gagal menambahkan master plan: {result["data"]}')
                return redirect('add_master_plan')
                
        except Exception as e:
            messages.error(request, f'Terjadi error: {str(e)}')
            return redirect('add_master_plan')
     
class UsersDataView(View):
    def dispatch(self, request, *args, **kwargs):
        if 'hris_admin' not in request.session:
            return redirect('admin_login')
        return super().dispatch(request, *args, **kwargs)

    def get(self, request):
        context = {
            'title': 'Data User',
            'user': request.session.get('hris_admin', {}),
        }
        return render(request, 'users/data/index.html', context)


class UsersDataListView(View):
    def dispatch(self, request, *args, **kwargs):
        if 'hris_admin' not in request.session:
            return redirect('admin_login')
        return super().dispatch(request, *args, **kwargs)

    def get(self, request):
        resp = data_mysql().data_user_by_params_with_roles()
        data_user = []
        try:
            if isinstance(resp, dict) and resp.get('status'):
                data_user = resp.get('data') or []
        except Exception:
            data_user = []
        return JsonResponse({
            'hasil': 'Data User',
            'data_user': data_user,
        })


class UsersDataGetByIdView(View):
    def dispatch(self, request, *args, **kwargs):
        if 'hris_admin' not in request.session:
            return redirect('admin_login')
        return super().dispatch(request, *args, **kwargs)

    def get(self, request, user_id):
        user_data = data_mysql().get_user_by_id(user_id)
        return JsonResponse({
            'status': user_data['status'],
            'data': user_data['data'],
        })


@method_decorator(csrf_exempt, name='dispatch')
class UsersDataCreateView(View):
    def dispatch(self, request, *args, **kwargs):
        if 'hris_admin' not in request.session:
            return redirect('admin_login')
        return super().dispatch(request, *args, **kwargs)

    def post(self, request):
        user_alias = request.POST.get('user_alias')
        user_name = request.POST.get('user_name')
        user_pass = request.POST.get('user_pass')
        user_mail = request.POST.get('user_mail')
        user_telp = request.POST.get('user_telp')
        user_alamat = request.POST.get('user_alamat')
        user_st = request.POST.get('user_st')

        # Validate required fields for standard form submit
        if not all([user_alias, user_name, user_pass, user_mail, user_st]):
            messages.error(request, 'Semua field wajib diisi!')
            return redirect('users_data_add')

        is_exist = data_mysql().is_exist_user({
            'user_alias': user_alias,
            'user_name': user_name,
        })
        if is_exist['hasil']['data'] is not None:
            messages.error(request, 'Data User Sudah Ada! Silahkan cek kembali datanya.')
            return redirect('users_data_add')
        data_insert = {
            'user_name': user_name,
            'user_pass': user_pass,
            'user_alias': user_alias,
            'user_mail': user_mail,
            'user_telp': user_telp,
            'user_alamat': user_alamat,
            'user_st': user_st,
            'user_foto': '',
            'mdb': request.session['hris_admin']['user_id'],
            'mdb_name': request.session['hris_admin']['user_alias'],
            'mdd': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        }
        data = data_mysql().insert_user(data_insert)
        if data.get('hasil', {}).get('status'):
            # Try to fetch the newly created user_id by unique email
            try:
                rs = data_mysql().data_user_by_params({'user_mail': user_mail})
                user_list = rs.get('data', []) if isinstance(rs, dict) else []
                new_user_id = user_list[0].get('user_id') if user_list else None
            except Exception:
                new_user_id = None
            messages.success(request, data.get('hasil', {}).get('message', 'Data Berhasil Disimpan'))
            if new_user_id:
                # Redirect to edit to allow setting roles
                return redirect('users_data_edit', user_id=new_user_id)
            # Fallback: go back to users list if we cannot resolve id
            return redirect('users_data')
        else:
            messages.error(request, data.get('hasil', {}).get('message', 'Gagal menyimpan data'))
            return redirect('users_data_add')


@method_decorator(csrf_exempt, name='dispatch')
class UsersDataUpdateView(View):
    def dispatch(self, request, *args, **kwargs):
        if 'hris_admin' not in request.session:
            return redirect('admin_login')
        return super().dispatch(request, *args, **kwargs)

    def post(self, request):
        user_id = request.POST.get('user_id')
        user_alias = request.POST.get('user_alias')
        user_name = request.POST.get('user_name')
        user_pass = request.POST.get('user_pass')
        user_mail = request.POST.get('user_mail')
        user_telp = request.POST.get('user_telp')
        user_alamat = request.POST.get('user_alamat')
        user_st = request.POST.get('user_st')

        # Password optional in edit: do not require user_pass
        if not all([user_id, user_alias, user_name, user_mail, user_st]):
            messages.error(request, 'Field bertanda wajib tidak boleh kosong!')
            return redirect('users_data_edit', user_id=user_id)

        user_foto_url = None
        user_foto_file = request.FILES.get('user_foto')
        if user_foto_file:
            ok, url, err = _save_user_photo(user_foto_file, user_id)
            if not ok:
                messages.error(request, err or 'Gagal upload foto')
                return redirect('users_data_edit', user_id=user_id)
            user_foto_url = url

        data_update = {
            'user_id': user_id,
            'user_name': user_name,
            'user_pass': user_pass,
            'user_alias': user_alias,
            'user_mail': user_mail,
            'user_telp': user_telp,
            'user_alamat': user_alamat,
            'user_st': user_st,
            'mdb': request.session['hris_admin']['user_id'],
            'mdb_name': request.session['hris_admin']['user_alias'],
            'mdd': datetime.now().strftime('%y-%m-%d %H:%M:%S'),
        }
        if user_foto_url:
            data_update['user_foto'] = user_foto_url

        data = data_mysql().update_user(data_update)
        if data.get('hasil', {}).get('status'):
            messages.success(request, data.get('hasil', {}).get('message', 'Data Berhasil Diupdate'))
            try:
                current = request.session.get('hris_admin', {})
                if current and str(current.get('user_id')) == str(user_id):
                    refreshed = data_mysql().get_user_by_id(user_id)
                    if isinstance(refreshed, dict) and refreshed.get('status') and refreshed.get('data'):
                        _set_hris_admin_session(request, refreshed.get('data'))
            except Exception:
                pass
        else:
            messages.error(request, data.get('hasil', {}).get('message', 'Gagal mengupdate data'))
        return redirect('users_data_edit', user_id=user_id)


class UserProfileEditPageView(View):
    def dispatch(self, request, *args, **kwargs):
        if 'hris_admin' not in request.session:
            return redirect('admin_login')
        return super().dispatch(request, *args, **kwargs)

    def get(self, request):
        admin = request.session.get('hris_admin', {})
        user_id = admin.get('user_id')
        if not user_id:
            return redirect('admin_login')

        resp = data_mysql().get_user_by_id(user_id)
        user_data = None
        try:
            if isinstance(resp, dict) and resp.get('status') and resp.get('data'):
                user_data = resp.get('data')
        except Exception:
            user_data = None

        if not user_data:
            messages.error(request, 'User tidak ditemukan atau data tidak tersedia.')
            return redirect('dashboard_admin')

        back_url = _get_profile_back_url(request)
        _set_hris_admin_session(request, user_data)
        admin = request.session.get('hris_admin', {})

        context = {
            'title': 'Profile',
            'user': admin,
            'user_id': user_id,
            'user_data': user_data,
            'is_profile': True,
            'back_url': back_url,
        }
        return render(request, 'users/data/edit.html', context)


class UserProfileUpdateView(View):
    def dispatch(self, request, *args, **kwargs):
        if 'hris_admin' not in request.session:
            return redirect('admin_login')
        return super().dispatch(request, *args, **kwargs)

    def post(self, request):
        admin = request.session.get('hris_admin', {})
        user_id = admin.get('user_id')
        if not user_id:
            return redirect('admin_login')

        resp = data_mysql().get_user_by_id(user_id)
        user_data = None
        try:
            if isinstance(resp, dict) and resp.get('status') and resp.get('data'):
                user_data = resp.get('data')
        except Exception:
            user_data = None

        if not user_data:
            messages.error(request, 'User tidak ditemukan atau data tidak tersedia.')
            return redirect('dashboard_admin')

        user_alias = request.POST.get('user_alias')
        user_name = request.POST.get('user_name')
        user_pass = request.POST.get('user_pass')
        user_mail = request.POST.get('user_mail')
        user_telp = request.POST.get('user_telp')
        user_alamat = request.POST.get('user_alamat')
        user_st = user_data.get('user_st')

        if not all([user_id, user_alias, user_name, user_mail, user_st is not None]):
            messages.error(request, 'Field bertanda wajib tidak boleh kosong!')
            return redirect('user_profile')

        user_foto_url = None
        user_foto_file = request.FILES.get('user_foto')
        if user_foto_file:
            ok, url, err = _save_user_photo(user_foto_file, user_id)
            if not ok:
                messages.error(request, err or 'Gagal upload foto')
                return redirect('user_profile')
            user_foto_url = url

        data_update = {
            'user_id': user_id,
            'user_name': user_name,
            'user_pass': user_pass,
            'user_alias': user_alias,
            'user_mail': user_mail,
            'user_telp': user_telp,
            'user_alamat': user_alamat,
            'user_st': user_st,
            'mdb': admin.get('user_id', ''),
            'mdb_name': admin.get('user_alias', ''),
            'mdd': datetime.now().strftime('%y-%m-%d %H:%M:%S'),
        }
        if user_foto_url:
            data_update['user_foto'] = user_foto_url

        data = data_mysql().update_user(data_update)
        if data.get('hasil', {}).get('status'):
            messages.success(request, data.get('hasil', {}).get('message', 'Profile berhasil diupdate'))
            try:
                refreshed = data_mysql().get_user_by_id(user_id)
                if isinstance(refreshed, dict) and refreshed.get('status') and refreshed.get('data'):
                    _set_hris_admin_session(request, refreshed.get('data'))
            except Exception:
                pass
        else:
            messages.error(request, data.get('hasil', {}).get('message', 'Gagal mengupdate profile'))
        return redirect('user_profile')


@method_decorator(csrf_exempt, name='dispatch')
class UsersDataDeleteView(View):
    def dispatch(self, request, *args, **kwargs):
        if 'hris_admin' not in request.session:
            return redirect('admin_login')
        return super().dispatch(request, *args, **kwargs)

    def post(self, request):
        user_id = request.POST.get('user_id')
        if not user_id:
            return JsonResponse({
                'status': False,
                'message': 'User ID tidak ditemukan'
            })
        db = data_mysql()
        try:
            if not db.execute_query('DELETE FROM app_users WHERE user_id = %s', (user_id,)):
                raise Exception('Gagal menjalankan perintah hapus user')
            if not db.commit():
                raise Exception('Gagal menyimpan perubahan hapus user')
            return JsonResponse({
                'status': True,
                'message': 'User berhasil dihapus'
            })
        except Exception as e:
            return JsonResponse({
                'status': False,
                'message': f'Gagal menghapus user: {str(e)}'
            })

class UsersDataAddPageView(View):
    def dispatch(self, request, *args, **kwargs):
        if 'hris_admin' not in request.session:
            return redirect('admin_login')
        return super().dispatch(request, *args, **kwargs)

    def get(self, request):
        # Load role list for UI (disabled until user is saved)
        db = data_mysql()
        roles_resp = db.list_roles_with_group()
        rs_roles = roles_resp.get('data', []) if isinstance(roles_resp, dict) else []
        context = {
            'title': 'Tambah User',
            'user': request.session.get('hris_admin', {}),
            'rs_roles': rs_roles,
            'roles_checked': [],
        }
        return render(request, 'users/data/add.html', context)


class UsersDataEditPageView(View):
    def dispatch(self, request, *args, **kwargs):
        if 'hris_admin' not in request.session:
            return redirect('admin_login')
        return super().dispatch(request, *args, **kwargs)

    def get(self, request, user_id):
        resp = data_mysql().get_user_by_id(user_id)
        user_data = None
        try:
            if isinstance(resp, dict) and resp.get('status') and resp.get('data'):
                user_data = resp.get('data')
        except Exception:
            user_data = None

        if not user_data:
            messages.error(request, 'User tidak ditemukan atau data tidak tersedia.')
            return redirect('users_data')

        # Load roles and pre-checked roles for user
        db = data_mysql()
        roles_resp = db.list_roles_with_group()
        rs_roles = roles_resp.get('data', []) if isinstance(roles_resp, dict) else []
        checked_resp = db.list_user_roles(user_id)
        roles_checked = checked_resp.get('data', []) if isinstance(checked_resp, dict) else []
        context = {
            'title': 'Edit User',
            'user': request.session.get('hris_admin', {}),
            'user_id': user_id,
            'user_data': user_data,
            'rs_roles': rs_roles,
            'roles_checked': roles_checked,
        }
        return render(request, 'users/data/edit.html', context)


@method_decorator(csrf_exempt, name='dispatch')
class UsersRolesUpdateView(View):
    def dispatch(self, request, *args, **kwargs):
        if 'hris_admin' not in request.session:
            return redirect('admin_login')
        return super().dispatch(request, *args, **kwargs)

    def post(self, request):
        user_id = request.POST.get('user_id')
        roles = request.POST.getlist('roles[]') or request.POST.getlist('roles')
        if not user_id:
            messages.error(request, 'User ID tidak ditemukan')
            return redirect('users_data')
        # Normalize role ids to strings
        roles = [str(r) for r in roles if str(r).strip()]
        resp = data_mysql().replace_user_roles(user_id, roles)
        if isinstance(resp, dict) and resp.get('status'):
            messages.success(request, 'Roles berhasil diupdate')
        else:
            messages.error(request, resp.get('message') if isinstance(resp, dict) else 'Gagal mengupdate roles')
        return redirect('users_data_edit', user_id=user_id)


class PendingRoleAssignmentsNotificationsView(View):
    def dispatch(self, request, *args, **kwargs):
        if 'hris_admin' not in request.session:
            return JsonResponse({'status': False, 'error': 'Unauthorized'}, status=401)
        return super().dispatch(request, *args, **kwargs)

    def get(self, request):
        admin = request.session.get('hris_admin', {})
        if not isinstance(admin, dict) or admin.get('super_st') == '0':
            return JsonResponse({'status': False, 'error': 'Forbidden'}, status=403)

        limit = request.GET.get('limit')
        try:
            limit = int(limit) if limit is not None else 10
        except Exception:
            limit = 10
        if limit <= 0:
            limit = 10
        if limit > 25:
            limit = 25

        db = data_mysql()

        count = 0
        items = []

        q_count = """
            SELECT COUNT(*) AS cnt
            FROM app_users u
            LEFT JOIN app_user_role ur ON ur.user_id = u.user_id
            WHERE ur.user_id IS NULL
        """

        q_list = """
            SELECT u.user_id, u.user_alias, u.user_name, u.user_mail, u.mdd
            FROM app_users u
            LEFT JOIN app_user_role ur ON ur.user_id = u.user_id
            WHERE ur.user_id IS NULL
            ORDER BY u.mdd DESC
            LIMIT %s
        """

        try:
            if db.execute_query(q_count):
                row = db.cur_hris.fetchone() or {}
                try:
                    count = int(row.get('cnt') if isinstance(row, dict) else row[0])
                except Exception:
                    count = 0

            if db.execute_query(q_list, (limit,)):
                rows = db.cur_hris.fetchall() or []
                for r in rows:
                    user_id = r.get('user_id') if isinstance(r, dict) else None
                    if not user_id:
                        continue
                    try:
                        edit_url = reverse('users_data_edit', kwargs={'user_id': user_id})
                    except Exception:
                        edit_url = None

                    items.append({
                        'user_id': user_id,
                        'user_alias': (r.get('user_alias') if isinstance(r, dict) else '') or '',
                        'user_name': (r.get('user_name') if isinstance(r, dict) else '') or '',
                        'user_mail': (r.get('user_mail') if isinstance(r, dict) else '') or '',
                        'mdd': _json_safe(r.get('mdd') if isinstance(r, dict) else None),
                        'edit_url': edit_url,
                    })
        except Exception:
            return JsonResponse({'status': False, 'error': 'Failed to fetch notifications'}, status=500)

        return JsonResponse({'status': True, 'count': count, 'data': items})
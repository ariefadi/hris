from django.views import View
from django.shortcuts import render, redirect
from django.http import JsonResponse
from django.utils.decorators import method_decorator
from django.views.decorators.csrf import csrf_exempt
from django.contrib import messages
from settings.database import data_mysql
from datetime import datetime


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
        data_user = data_mysql().data_user_by_params()['data']
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
        data = data_mysql().update_user(data_update)
        if data.get('hasil', {}).get('status'):
            messages.success(request, data.get('hasil', {}).get('message', 'Data Berhasil Diupdate'))
        else:
            messages.error(request, data.get('hasil', {}).get('message', 'Gagal mengupdate data'))
        return redirect('users_data_edit', user_id=user_id)


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
from collections import defaultdict
import os
import csv
from io import StringIO
from django.conf import settings
import pprint
from django.shortcuts import render, redirect
from django.views import View
from django.views.decorators.csrf import csrf_exempt
from django.utils.decorators import method_decorator
from django import template
from calendar import month, monthrange
from datetime import datetime, date, timedelta
from django.http import HttpResponse, JsonResponse, QueryDict
from management.database import data_mysql
from itertools import groupby, product
from django.core import serializers
from operator import itemgetter
import tempfile
from django.core.files.storage import FileSystemStorage
from django.template.loader import render_to_string
import pandas as pd
import io
from .crypto import sandi
import requests
import json
from geopy.geocoders import Nominatim
import uuid
import pycountry
from google_auth_oauthlib.flow import Flow
from management.oauth_utils import (
    generate_oauth_url_for_user, 
    exchange_code_for_refresh_token,
    handle_oauth_callback
)
import logging

logger = logging.getLogger(__name__)
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from googleads import ad_manager
from .utils import fetch_data_all_insights_data_all, fetch_data_all_insights_total_all, fetch_data_insights_account_range_all, fetch_data_all_insights, fetch_data_all_insights_total, fetch_data_insights_account_range, fetch_data_insights_account, fetch_data_insights_account_filter_all, fetch_daily_budget_per_campaign, fetch_status_per_campaign, fetch_data_insights_campaign_filter_sub_domain, fetch_data_insights_campaign_filter_account, fetch_data_country_facebook_ads, fetch_data_insights_by_country_filter_campaign, fetch_data_insights_by_country_filter_account, fetch_ad_manager_reports, fetch_ad_manager_inventory, fetch_adx_summary_data, fetch_adx_traffic_account_by_user, fetch_user_adx_account_data, fetch_adx_account_data, fetch_data_insights_all_accounts_by_subdomain, fetch_adx_traffic_per_country, fetch_roi_per_country, fetch_data_insights_by_country_filter_campaign_roi, fetch_data_insights_by_date_subdomain_roi

# OAuth views will be imported directly in urls.py to avoid circular imports

# Helper function untuk refresh token management
def ensure_refresh_token(email):
    """
    Helper function untuk memastikan refresh token tersedia untuk user
    Jika belum ada, akan generate dan simpan ke database
    """
    try:
        db = data_mysql()
        result = db.get_or_generate_refresh_token(email)
        return result
    except Exception as e:
        return {
            'hasil': {
                'status': False,
                'action': 'error',
                'refresh_token': None,
                'message': f'Error dalam ensure_refresh_token: {str(e)}'
            }
        }

def get_user_refresh_token(email):
    """
    Helper function untuk mendapatkan refresh token user dari database
    """
    try:
        db = data_mysql()
        result = db.check_refresh_token(email)
        return result
    except Exception as e:
        return {
            'hasil': {
                'status': False,
                'has_token': False,
                'refresh_token': None,
                'message': f'Error dalam get_user_refresh_token: {str(e)}'
            }
        }


geocode = Nominatim(user_agent="hris_trendHorizone")

data_bulan = {
    1: 'Januari',
    2: 'Februari',
    3: 'Maret',
    4: 'April',
    5: 'Mei',
    6: 'Juni',
    7: 'Juli',
    8: 'Agustus',
    9: 'September',
    10: 'Oktober',
    11: 'November',
    12: 'Desember'
}

# LOGIN / LOGOUT
kata_sandi = sandi()
# def redirect_login_user(res):
#     return redirect('admin_login')

def redirect_login_user(request):
    """
    Fungsi redirect untuk login user.
    """
    if request.user.is_authenticated and 'hris_admin' in request.session:
        return redirect('dashboard_admin')
    return redirect('admin_login')

# Create your views here.
class LoginAdmin(View):
    def dispatch(self, request, *args, **kwargs):
        if 'hris_admin' in request.session:
            return redirect('dashboard_admin')
        return super(LoginAdmin, self).dispatch(request, *args, **kwargs)
    def get(self, req):
        # Hapus pesan error OAuth setelah ditampilkan
        if 'oauth_error' in req.session:
            # Biarkan template menampilkan error terlebih dahulu
            # Error akan dihapus di request berikutnya
            pass
        return render(req, 'admin/login_admin.html')
    
    def post(self, req):
        # Hapus pesan error OAuth jika ada saat form disubmit
        if 'oauth_error' in req.session:
            del req.session['oauth_error']
        if 'oauth_error_details' in req.session:
            del req.session['oauth_error_details']
        # Redirect ke login process
        return redirect('admin_login_process')

class OAuthRedirectView(View):
    def get(self, request):
        if not request.user.is_authenticated:
            return redirect('admin_login')
            
        # Get user data from database based on email
        user_data = data_mysql().data_user_by_params(params={'user_mail': request.user.email})
        if not user_data['status'] or not user_data['data']:
            request.session['oauth_error'] = 'Email tidak terdaftar di sistem'
            return redirect('admin_login')
        
        # Insert login record
        login_id = str(uuid.uuid4())
        try:
            # Get location data
            response = requests.get("https://ipinfo.io/json")
            data = response.json()
            lat_long = data["loc"].split(",") if "loc" in data else [None, None]
            ip_address = data.get("ip", request.META.get('REMOTE_ADDR'))
            location = None
            if lat_long[0] and lat_long[1]:
                try:
                    geocode = Nominatim(user_agent="hris_trendHorizone")
                    location = geocode.reverse((lat_long), language='id')
                    location = location.address if location else None
                except:
                    location = None
        except:
            lat_long = [None, None]
            ip_address = request.META.get('REMOTE_ADDR')
            location = None

        data_insert = {
            'login_id': login_id,
            'user_id': user_data['data'][0]['user_id'],
            'login_date': datetime.now().strftime('%y-%m-%d %H:%M:%S'),
            'logout_date': None,
            'ip_address': ip_address,
            'user_agent': request.META.get('HTTP_USER_AGENT', ''),
            'latitude': lat_long[0] if len(lat_long) > 0 else None,
            'longitude': lat_long[1] if len(lat_long) > 1 else None,
            'lokasi': location,
            'mdb': user_data['data'][0]['user_id']
        }
        data_mysql().insert_login(data_insert)
        
        # Set session data
        request.session['hris_admin'] = {
            'login_id': login_id,
            'user_id': user_data['data'][0]['user_id'],
            'user_name': user_data['data'][0]['user_name'],
            'user_pass': '',  # Kosong untuk OAuth login
            'user_alias': user_data['data'][0]['user_alias'],
            'user_mail': user_data['data'][0]['user_mail']  # Tambahkan user_mail ke session
        }
        return redirect('dashboard_admin')

class LoginProcess(View):
    def post(self, req):
        username = req.POST.get('username')
        password = req.POST.get('password')
        if not username or not password:
            hasil = {
                'status': False,
                'data': f"Username dan Password tidak boleh kosong !",
                'message': "Silahkan isi username dan password anda."
            }
        else:
            rs_data = data_mysql().login_admin({
                'username': username,
                'password': password
            })
            if rs_data['data'] == None:
                hasil = {
                    'status': False,
                    'data': f"Username dan Password tidak ditemukan !",
                    'message': "Silahkan cek kembali username dan password anda."
                }
            else:
                # get lat lang
                response = requests.get("https://ipinfo.io/json")
                data = response.json()
                lat_long = data["loc"].split(",")
                ip_address = requests.get("https://api.ipify.org").text
                location = geocode.reverse((lat_long), language='id')
                # insert user login
                data_insert = {
                    'login_id': str(uuid.uuid4()),
                    'user_id': rs_data['data']['user_id'],
                    'login_date': datetime.now().strftime('%y-%m-%d %H:%M:%S'),
                    'logout_date': None,
                    'ip_address': ip_address,
                    'user_agent': req.META.get('HTTP_USER_AGENT', ''),
                    'latitude': lat_long[0] if len(lat_long) > 0 else None,
                    'longitude': lat_long[1] if len(lat_long) > 1 else None,
                    'lokasi':location.address if location.address else None,
                    'mdb': rs_data['data']['user_id']
                }
                data_login = data_mysql().insert_login(data_insert)
                login_id = data_login['hasil']['login_id']
                user_data = {
                    'login_id': login_id,
                    'user_id': rs_data['data']['user_id'],
                    'user_name': rs_data['data']['user_name'],
                    'user_pass': rs_data['data']['user_pass'],
                    'user_alias': rs_data['data']['user_alias'],
                    'user_mail': rs_data['data']['user_mail']  # Tambahkan user_mail ke session
                }
                req.session['hris_admin'] = user_data
                hasil = {
                    'status': True,
                    'data': "Login Berhasil",
                    'message': "Selamat Datang " + rs_data['data']['user_alias'] + " !",
                }
        return JsonResponse(hasil)

    
@csrf_exempt
def get_countries_facebook_ads(request):
    """Endpoint untuk mendapatkan daftar negara yang tersedia"""
    if 'hris_admin' not in request.session:
        return redirect('admin_login')
    try:
        # Ambil data negara dari AdX untuk periode 30 hari terakhir
        end_date = datetime.now().date()
        start_date = end_date - timedelta(days=30)
        # Ambil semua data negara tanpa filter
        rs_account = data_mysql().master_account_ads()
        result = fetch_data_country_facebook_ads(
            rs_account['data'],
            start_date.strftime('%Y-%m-%d'), 
            end_date.strftime('%Y-%m-%d')
        )
        countries = []
        for country_data in result: 
            country_name = country_data.get('name')
            country_code = country_data.get('code')
            if country_name:
                countries.append({
                    'code': country_code,
                    'name': country_name
                })
            
        # Sort berdasarkan nama negara
        countries.sort(key=lambda x: x['name'])
        return JsonResponse({
            'status': 'success',
            'countries': countries
        })
        
    except Exception as e:
        print(f"[ERROR] Gagal mengambil data negara: {e}")
        return JsonResponse({
            'status': 'error',
            'message': 'Gagal mengambil data negara.',
            'error': str(e)
        }, status=500)  # <- Ini penting

@csrf_exempt
def get_countries_adx(request):
    """Endpoint untuk mendapatkan daftar negara yang tersedia"""
    if 'hris_admin' not in request.session:
        return redirect('admin_login')
    try:
        # Ambil data negara dari AdX untuk periode 30 hari terakhir
        end_date = datetime.now().date()
        start_date = end_date - timedelta(days=30)
        # Ambil semua data negara tanpa filter
        user_mail = request.session['hris_admin']['user_mail']
        result = fetch_adx_traffic_per_country(
            start_date.strftime('%Y-%m-%d'), 
            end_date.strftime('%Y-%m-%d'),
            user_mail
        )
        print(f"Data Negara : {result}")
        
        # Validasi struktur result
        if not result:
            print("[WARNING] Result is None or empty")
            return JsonResponse({
                'status': 'error',
                'message': 'Tidak ada data yang tersedia.',
                'countries': []
            })
        
        if not isinstance(result, dict):
            print(f"[WARNING] Result is not a dict: {type(result)}")
            return JsonResponse({
                'status': 'error',
                'message': 'Format data tidak valid.',
                'countries': []
            })
        
        # Periksa apakah ada key 'data' dalam result
        if 'data' not in result:
            print(f"[WARNING] No 'data' key in result. Available keys: {list(result.keys())}")
            return JsonResponse({
                'status': 'error',
                'message': 'Data negara tidak tersedia.',
                'countries': []
            })
        
        # Periksa apakah data adalah list
        if not isinstance(result['data'], list):
            print(f"[WARNING] result['data'] is not a list: {type(result['data'])}")
            return JsonResponse({
                'status': 'error',
                'message': 'Format data negara tidak valid.',
                'countries': []
            })
        
        # Ekstrak daftar negara dari data yang tersedia
        countries = []
        for country_data in result['data']:
            if not isinstance(country_data, dict):
                print(f"[WARNING] Country data is not a dict: {type(country_data)}")
                continue
                
            country_name = country_data.get('country_name')
            country_code = country_data.get('country_code', '')
            
            if country_name:
                country_label = f"{country_name} ({country_code})" if country_code else country_name
                countries.append({
                    'code': country_code,
                    'name': country_label
                })
            
        # Sort berdasarkan nama negara
        countries.sort(key=lambda x: x['name'])
        
        return JsonResponse({
            'status': 'success',
            'countries': countries
        })
        
    except Exception as e:
        print(f"[ERROR] Gagal mengambil data negara: {e}")
        import traceback
        print(f"[ERROR] Traceback: {traceback.format_exc()}")
        return JsonResponse({
            'status': 'error',
            'message': 'Gagal mengambil data negara.',
            'error': str(e),
            'countries': []
        }, status=500)

class LogoutAdmin(View):
    def get(self, req):
        try:
            if 'hris_admin' in req.session and 'login_id' in req.session['hris_admin']:
                data_update = {
                    'logout_date': datetime.now().strftime('%y-%m-%d %H:%M:%S'),
                    'login_id': req.session['hris_admin']['login_id']
                }
                data_mysql().update_login(data_update)
        except Exception as e:
            print(f"[ERROR] Gagal update data logout: {e}")
        finally:
            req.session.flush()
            return redirect('admin_login')

# DASHBOARD
class DashboardAdmin(View):
    def dispatch(self, request, *args, **kwargs):
        if 'hris_admin' not in request.session:
            return redirect('admin_login')
        return super(DashboardAdmin, self).dispatch(request, *args, **kwargs)

    def get(self, req):
        data = {
            'title': 'Dashboard Admin',
            'user': req.session['hris_admin']
        }
        return render(req, 'admin/dashboard_admin.html', data)

class DashboardData(View):
    """API endpoint untuk data dashboard dengan statistik user dan login"""
    def dispatch(self, request, *args, **kwargs):
        if 'hris_admin' not in request.session:
            return redirect('/management/admin/login')
        return super().dispatch(request, *args, **kwargs)
    
    def get(self, req):
        try:
            # Ambil data user
            user_data = data_mysql().data_user_by_params()
            total_users = len(user_data['data']) if user_data['status'] else 0
            
            # Ambil data login user
            login_data = data_mysql().data_login_user()
            login_users = login_data['data'] if login_data['status'] else []
            
            # Hitung statistik login 7 hari terakhir
            today = datetime.now()
            seven_days_ago = today - timedelta(days=7)
            
            # Filter login 7 hari terakhir
            recent_logins = []
            daily_login_stats = {}
            
            for login in login_users:
                # Handle both string and datetime objects
                if isinstance(login['login_date'], str):
                    login_date = datetime.strptime(login['login_date'], '%Y-%m-%d %H:%M:%S')
                else:
                    login_date = login['login_date']
                
                if login_date >= seven_days_ago:
                    recent_logins.append(login)
                    date_key = login_date.strftime('%Y-%m-%d')
                    if date_key not in daily_login_stats:
                        daily_login_stats[date_key] = {'count': 0, 'unique_users': set()}
                    daily_login_stats[date_key]['count'] += 1
                    daily_login_stats[date_key]['unique_users'].add(login['user_id'])
            
            # Konversi set ke count untuk JSON serialization
            for date_key in daily_login_stats:
                daily_login_stats[date_key]['unique_users'] = len(daily_login_stats[date_key]['unique_users'])
            
            # Hitung user aktif (login dalam 7 hari terakhir)
            active_users = len(set([login['user_id'] for login in recent_logins]))
            
            # Siapkan data untuk chart
            chart_labels = []
            chart_login_counts = []
            chart_unique_users = []
            
            for i in range(7):
                date = (today - timedelta(days=6-i)).strftime('%Y-%m-%d')
                chart_labels.append((today - timedelta(days=6-i)).strftime('%d/%m'))
                
                if date in daily_login_stats:
                    chart_login_counts.append(daily_login_stats[date]['count'])
                    chart_unique_users.append(daily_login_stats[date]['unique_users'])
                else:
                    chart_login_counts.append(0)
                    chart_unique_users.append(0)
            
            dashboard_data = {
                'user_stats': {
                    'total_users': total_users,
                    'active_users': active_users,
                    'total_logins_7days': len(recent_logins),
                    'activity_rate': round((active_users / total_users * 100) if total_users > 0 else 0, 1)
                },
                'charts': {
                    'login_activity': {
                        'labels': chart_labels,
                        'login_counts': chart_login_counts,
                        'unique_users': chart_unique_users
                    }
                },
                'recent_logins': recent_logins[:10]  # 10 login terakhir
            }
            
            return JsonResponse({
                'status': True,
                'data': dashboard_data
            })
            
        except Exception as e:
            return JsonResponse({
                'status': False,
                'error': str(e)
            })

# Fungsi handler untuk halaman 404
def handler404(request, exception):
    # Gunakan template admin khusus untuk halaman 404
    return render(request, 'admin/404.html', status=404)

# Catch-all view untuk development (DEBUG=True) agar tetap merender 404 kustom
def dev_404(request):
    return render(request, 'admin/404.html', status=404)

# USER MANAGEMENT   
class DataUser(View):
    def dispatch(self, request, *args, **kwargs):
        if 'hris_admin' not in request.session:
            return redirect('admin_login')
        return super(DataUser, self).dispatch(request, *args, **kwargs)

    def get(self, req):
        data = {
            'title': 'Data User',
            'user': req.session['hris_admin'],
        }
        return render(req, 'admin/data_user/index.html', data)
    
class page_user(View):
    def dispatch(self, request, *args, **kwargs):
        if 'hris_admin' not in request.session:
            return redirect('admin_login')
        elif 'hris_admin' not in request.session:
            return redirect('user_login')
        return super(page_user, self).dispatch(request, *args, **kwargs)
    def get(self, req):
        data_user = data_mysql().data_user_by_params()['data']
        hasil = {
            'hasil': "Data User",
            'data_user': data_user
        }
        return JsonResponse(hasil)
    
class get_user_by_id(View):
    def dispatch(self, request, *args, **kwargs):
        if 'hris_admin' not in request.session:
            return redirect('admin_login')
        return super(get_user_by_id, self).dispatch(request, *args, **kwargs)
    
    def get(self, req, user_id):
        user_data = data_mysql().get_user_by_id(user_id)
        hasil = {
            'status': user_data['status'],
            'data': user_data['data']
        }
        return JsonResponse(hasil)

class post_edit_user(View):
    def dispatch(self, request, *args, **kwargs):
        if 'hris_admin' not in request.session:
            return redirect('admin_login')
        return super(post_edit_user, self).dispatch(request, *args, **kwargs)
    
    def post(self, req):
        user_id = req.POST.get('user_id')
        user_alias = req.POST.get('user_alias')
        user_name = req.POST.get('user_name')
        user_pass = req.POST.get('user_pass')
        user_mail = req.POST.get('user_mail')
        user_telp = req.POST.get('user_telp')
        user_alamat = req.POST.get('user_alamat')
        user_st = req.POST.get('user_st')
        
        if not all([user_id, user_alias, user_name, user_pass, user_mail, user_st]):
            hasil = {
                "status": False,
                "message": "Semua field wajib diisi!"
            }
        else:
            data_update = {
                'user_id': user_id,
                'user_name': user_name,
                'user_pass': user_pass,
                'user_alias': user_alias,
                'user_mail': user_mail,
                'user_telp': user_telp,
                'user_alamat': user_alamat,
                'user_st': user_st,
                'mdb': req.session['hris_admin']['user_id'],
                'mdb_name': req.session['hris_admin']['user_alias'],
                'mdd': datetime.now().strftime('%y-%m-%d %H:%M:%S')
            }
            data = data_mysql().update_user(data_update)
            hasil = {
                "status": data['hasil']['status'],
                "message": data['hasil']['message']
            }
        return JsonResponse(hasil)

class post_tambah_user(View):
    def post(self, req):
        user_alias = req.POST.get('user_alias')
        user_name = req.POST.get('user_name')
        user_pass = req.POST.get('user_pass')
        user_mail = req.POST.get('user_mail')
        user_telp = req.POST.get('user_telp')
        user_alamat = req.POST.get('user_alamat')
        user_st = req.POST.get('user_st')
        is_exist = data_mysql().is_exist_user({
            'user_alias'    : user_alias,
            'user_name'     : user_name,
            'user_pass'     : user_pass
        })
        if is_exist['hasil']['data'] != None :
            hasil = {
                "status": False,
                "message": "Data User Sudah Ada ! Silahkan di cek kembali datanya."
            }
        else:
            data_insert = {
                'user_name': user_name,
                'user_pass': user_pass,
                'user_alias': user_alias,
                'user_mail': user_mail,
                'user_telp': user_telp,
                'user_alamat': user_alamat,
                'user_st': user_st,
                'user_foto': '',
                'mdb': req.session['hris_admin']['user_id'],
                'mdb_name': req.session['hris_admin']['user_alias'],
                'mdd' : datetime.now().strftime('%y-%m-%d %H:%M:%S')
            }
            data = data_mysql().insert_user(data_insert)
            hasil = {
                "status": data['hasil']['status'],
                "message": data['hasil']['message']
            }
        return JsonResponse(hasil)

# REFRESH TOKEN MANAGEMENT
class RefreshTokenManagement(View):
    """View untuk mengelola refresh token user"""
    def dispatch(self, request, *args, **kwargs):
        if 'hris_admin' not in request.session:
            return redirect('admin_login')
        return super().dispatch(request, *args, **kwargs)
    
    def get(self, req):
        """Halaman management refresh token"""
        data = {
            'title': 'Refresh Token Management',
            'user': req.session['hris_admin'],
        }
        return render(req, 'admin/refresh_token/index.html', data)

class CheckRefreshTokenAPI(View):
    """API untuk mengecek status refresh token user"""
    def dispatch(self, request, *args, **kwargs):
        if 'hris_admin' not in request.session:
            return JsonResponse({'status': False, 'message': 'Unauthorized'})
        return super().dispatch(request, *args, **kwargs)
    
    def post(self, req):
        email = req.POST.get('email')
        if not email:
            return JsonResponse({
                'status': False,
                'message': 'Email parameter required'
            })
        
        result = get_user_refresh_token(email)
        return JsonResponse({
            'status': result['hasil']['status'],
            'has_token': result['hasil'].get('has_token', False),
            'message': result['hasil']['message']
        })

class GenerateRefreshTokenAPI(View):
    """API untuk generate refresh token baru untuk user"""
    def dispatch(self, request, *args, **kwargs):
        if 'hris_admin' not in request.session:
            return JsonResponse({'status': False, 'message': 'Unauthorized'})
        return super().dispatch(request, *args, **kwargs)
    
    def post(self, req):
        email = req.POST.get('email')
        force_generate = req.POST.get('force_generate', 'false').lower() == 'true'
        
        if not email:
            return JsonResponse({
                'status': False,
                'message': 'Email parameter required'
            })
        
        try:
            db = data_mysql()
            
            if force_generate:
                # Force generate refresh token baru
                result = db.generate_and_save_refresh_token(email)
                action = 'force_generated'
            else:
                # Cek dulu, generate hanya jika belum ada
                result = db.get_or_generate_refresh_token(email)
                action = result['hasil'].get('action', 'unknown')
            
            return JsonResponse({
                'status': result['hasil']['status'],
                'action': action,
                'message': result['hasil']['message']
            })
            
        except Exception as e:
            return JsonResponse({
                'status': False,
                'action': 'error',
                'message': f'Error: {str(e)}'
            })

class GetAllUsersRefreshTokenAPI(View):
    """API untuk mendapatkan status refresh token semua user"""
    def dispatch(self, request, *args, **kwargs):
        if 'hris_admin' not in request.session:
            return JsonResponse({'status': False, 'message': 'Unauthorized'})
        return super().dispatch(request, *args, **kwargs)
    
    def get(self, req):
        try:
            # Ambil semua user
            db = data_mysql()
            user_data = db.data_user_by_params()
            
            if not user_data['status']:
                return JsonResponse({
                    'status': False,
                    'message': 'Failed to fetch users'
                })
            
            users_with_token_status = []
            for user in user_data['data']:
                email = user.get('user_mail')
                if email:
                    token_result = db.check_refresh_token(email)
                    users_with_token_status.append({
                        'user_id': user.get('user_id'),
                        'user_name': user.get('user_name'),
                        'user_alias': user.get('user_alias'),
                        'user_mail': email,
                        'has_refresh_token': token_result['hasil'].get('has_token', False),
                        'token_status': 'Available' if token_result['hasil'].get('has_token', False) else 'Not Available'
                    })
            
            return JsonResponse({
                'status': True,
                'data': users_with_token_status
            })
            
        except Exception as e:
            return JsonResponse({
                'status': False,
                'message': f'Error: {str(e)}'
            })
    
class DataLoginUser(View):
    def dispatch(self, request, *args, **kwargs):
        if 'hris_admin' not in request.session:
            return redirect('admin_login')
        return super(DataLoginUser, self).dispatch(request, *args, **kwargs)

    def get(self, req):
        data = {
            'title': 'Data Login User',
            'user': req.session['hris_admin'],
        }
        return render(req, 'admin/data_login_user/index.html', data)
    
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
        return render(req, 'admin/master_plan/index.html', data)
    
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
                return render(request, 'admin/master_plan/detail.html', context)
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
        return render(request, 'admin/master_plan/add.html', context)


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
    

# FACEBOOK ADS
class SummaryFacebookAds(View):
    def dispatch(self, request, *args, **kwargs):
        if 'hris_admin' not in request.session:
            return redirect('admin_login')
        return super(SummaryFacebookAds, self).dispatch(request, *args, **kwargs)
    def get(self, req):
        data_account = data_mysql().master_account_ads()['data']
        today = datetime.now().strftime('%Y-%m-%d')
        seven_days_ago = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
        data = {
            'title': 'Data Summaryt Facebook Ads',
            'user': req.session['hris_admin'],
        }
        return render(req, 'admin/facebook_ads/summary/index.html', {'data_account': data_account, 'data': data, 'today': today, 'seven_days_ago': seven_days_ago})
    
class page_summary_facebook(View):
    def dispatch(self, request, *args, **kwargs):
        if 'hris_admin' not in request.session:
            return redirect('admin_login')
        elif 'hris_admin' not in request.session:
            return redirect('user_login')
        return super(page_summary_facebook, self).dispatch(request, *args, **kwargs)
    def get(self, req):
        tanggal_dari = req.GET.get('tanggal_dari')
        tanggal_sampai = req.GET.get('tanggal_sampai')
        data_account = req.GET.get('data_account')
        rs_account = data_mysql().master_account_ads()
        if data_account != '%':
            rs_data_account = data_mysql().master_account_ads_by_id({
                'data_account': data_account,
            })['data']
            data = fetch_data_all_insights(str(rs_data_account['access_token']), str(rs_data_account['account_id']), str(rs_data_account['account_name']),  str(tanggal_dari), str(tanggal_sampai))
            total = fetch_data_all_insights_total(str(rs_data_account['access_token']), str(rs_data_account['account_id']), str(tanggal_dari), str(tanggal_sampai))
            jumlah = fetch_data_insights_account_range(str(rs_data_account['access_token']), str(rs_data_account['account_id']), str(tanggal_dari), str(tanggal_sampai))
        else:
            data = fetch_data_all_insights_data_all(rs_account['data'], str(tanggal_dari), str(tanggal_sampai))
            total = fetch_data_all_insights_total_all(rs_account['data'], str(tanggal_dari), str(tanggal_sampai))
            jumlah = fetch_data_insights_account_range_all(rs_account['data'], str(tanggal_dari), str(tanggal_sampai))
        hasil = {
            'hasil': "Data Summary Facebook Ads",
            'data_summary': data,
            'data_total' : total,
            'data_jumlah' : jumlah
        }
        return JsonResponse(hasil)

class AccountFacebookAds(View):
    def dispatch(self, request, *args, **kwargs):
        if 'hris_admin' not in request.session:
            return redirect('admin_login')
        return super(AccountFacebookAds, self).dispatch(request, *args, **kwargs)
    def get(self, req):
        data = {
            'title': 'Data Account Facebook Ads',
            'user': req.session['hris_admin'],
        }
        return render(req, 'admin/facebook_ads/account/index.html', data)
    
class page_account_facebook(View):
    def dispatch(self, request, *args, **kwargs):
        if 'hris_admin' not in request.session:
            return redirect('admin_login')
        elif 'hris_admin' not in request.session:
            return redirect('user_login')
        return super(page_account_facebook, self).dispatch(request, *args, **kwargs)
    def get(self, req):
        data_account_ads = data_mysql().data_account_ads_by_params()['data']
        hasil = {
            'hasil': "Data Account Facebook Ads",
            'data_account_ads': data_account_ads
        }
        return JsonResponse(hasil)
    
class post_account_ads(View):
    def post(self, req):
        account_name = req.POST.get('account_name')
        account_email = req.POST.get('account_email')
        account_id = req.POST.get('account_id')
        app_id = req.POST.get('app_id')
        app_secret = req.POST.get('app_secret')
        access_token = req.POST.get('access_token')
        is_exist = data_mysql().is_exist_account_ads_by_params({
            'account_name'   : account_name,
            'account_email'  : account_email,
            'account_id'     : account_id,
            'app_id'         : app_id
        })
        if is_exist['hasil']['data'] != None :
            hasil = {
                "status": False,
                "message": "Data Account Ads Sudah Ada ! Silahkan di cek kembali datanya."
            }
        else:
            data_insert = {
                'account_name': account_name,
                'account_email': account_email,
                'account_id': account_id,
                'app_id': app_id,
                'app_secret': app_secret,
                'access_token': access_token,
                'account_owner': req.session['hris_admin']['user_id'],
                'mdb': req.session['hris_admin']['user_id'],
                'mdb_name': req.session['hris_admin']['user_alias'],
                'mdd' : datetime.now().strftime('%y-%m-%d %H:%M:%S')
            }
            data = data_mysql().insert_account_ads(data_insert)
            hasil = {
                "status": data['hasil']['status'],
                "message": data['hasil']['message']
            }
        return JsonResponse(hasil)
    
class PerAccountFacebookAds(View):
    def dispatch(self, request, *args, **kwargs):
        if 'hris_admin' not in request.session:
            return redirect('admin_login')
        return super(PerAccountFacebookAds, self).dispatch(request, *args, **kwargs)
    def get(self, req):
        data_account = data_mysql().master_account_ads()['data']
        rs_account = data_mysql().master_account_ads()
        data_campaign = fetch_data_insights_account_filter_all(rs_account['data'])
        today = datetime.now().strftime('%Y-%m-%d')
        data = {
            'title': 'Data Traffic Per Account Facebook Ads',
            'user': req.session['hris_admin'],
        }
        return render(req, 'admin/facebook_ads/per_account/index.html', {'data_account': data_account, 'data_campaign': data_campaign, 'data': data, 'today': today})
    
class page_per_account_facebook(View):
    def dispatch(self, request, *args, **kwargs):
        if 'hris_admin' not in request.session:
            return redirect('admin_login')
        elif 'hris_admin' not in request.session:
            return redirect('user_login')
        return super(page_per_account_facebook, self).dispatch(request, *args, **kwargs)
    def get(self, req):
        data_account = req.GET.get('data_account')
        tanggal = req.GET.get('tanggal')
        data_sub_domain = req.GET.get('data_sub_domain')
        
        # Jika tanggal kosong atau '%', gunakan tanggal hari ini
        if not tanggal or tanggal == '%':
            tanggal = datetime.now().strftime('%Y-%m-%d')
        
        # Normalisasi data_sub_domain
        if not data_sub_domain or data_sub_domain == '':
            data_sub_domain = '%'
        
        # Jika data_account kosong atau '%', gunakan semua account untuk filter sub domain
        if not data_account or data_account == '%':
            rs_account = data_mysql().master_account_ads()['data']
            data = fetch_data_insights_all_accounts_by_subdomain(rs_account, str(tanggal), str(data_sub_domain))
        else:
            # Gunakan account spesifik seperti sebelumnya
            rs_data_account = data_mysql().master_account_ads_by_id({
                'data_account': data_account,
            })['data']
            data = fetch_data_insights_account(str(rs_data_account['access_token']), str(rs_data_account['account_id']), str(tanggal), str(data_sub_domain), str(rs_data_account['account_name']))
        
        hasil = {
            'hasil': "Data Traffic Per Account",
            'data_per_account': data['data'],
            'total_per_account': data['total']
        }
        return JsonResponse(hasil)

class EditAccountFacebookAds(View):
    def dispatch(self, request, *args, **kwargs):
        if 'hris_admin' not in request.session:
            return redirect('admin_login')
        return super(EditAccountFacebookAds, self).dispatch(request, *args, **kwargs)
    
    def get(self, req, account_ads_id):
        rs_data_account = data_mysql().master_account_ads_by_id({
            'data_account': account_ads_id,
        })['data']
        
        if rs_data_account is None:
            return JsonResponse({
                'status': False,
                'message': 'Account not found'
            }, status=404)
        
        context = {
            'account_data': rs_data_account,
            'account_ads_id': account_ads_id
        }
        return render(req, 'admin/facebook_ads/edit_account.html', context)

class UpdateAccountFacebookAds(View):
    def dispatch(self, request, *args, **kwargs):
        if 'hris_admin' not in request.session:
            return redirect('admin_login')
        return super(UpdateAccountFacebookAds, self).dispatch(request, *args, **kwargs)
    
    def post(self, req):
        account_ads_id = req.POST.get('account_ads_id')
        account_name = req.POST.get('account_name')
        account_email = req.POST.get('account_email')
        account_id = req.POST.get('account_id')
        app_id = req.POST.get('app_id')
        app_secret = req.POST.get('app_secret')
        access_token = req.POST.get('access_token')
        
        # Validasi input
        if not all([account_ads_id, account_name, account_email, account_id, app_id, app_secret, access_token]):
            hasil = {
                'status': False,
                'message': 'Semua field harus diisi!'
            }
            return JsonResponse(hasil)
        
        # Cek apakah account exists
        existing_account = data_mysql().master_account_ads_by_id({
            'data_account': account_ads_id,
        })['data']
        
        if existing_account is None:
            hasil = {
                'status': False,
                'message': 'Account tidak ditemukan!'
            }
            return JsonResponse(hasil)
        
        print(req.session['hris_admin'])
        # Update data
        data_update = {
            'account_ads_id': account_ads_id,
            'account_name': account_name,
            'account_email': account_email,
            'account_id': account_id,
            'app_id': app_id,
            'app_secret': app_secret,
            'access_token': access_token,
            'mdb': req.session['hris_admin']['user_id'],
            'mdb_name': req.session['hris_admin']['user_name'],
            'mdd': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
        
        rs_update = data_mysql().update_account_ads(data_update)
        hasil = rs_update['hasil']
        
        # Invalidate cache after successful update
        if hasil.get('status', False):
            from .utils import invalidate_cache_on_data_update
            invalidate_cache_on_data_update(account_id, event_type='account_update')
        
        return JsonResponse(hasil)

class update_daily_budget_per_campaign(View):
    def post(self, req):
        account_id = req.POST.get('account_id')
        rs_data_account = data_mysql().master_account_ads_by_id({
            'data_account': account_id,
        })['data']
        campaign_id = req.POST.get('campaign_id')
        raw = req.POST.get('daily_budget', '0')
        cleaned = raw.replace("Rp", "").replace(".", "").replace(",", "").strip()
        try:
            daily_budget = int(cleaned)
            # daily_budget_in_cents = daily_budget * 100
        except ValueError:
            print(f"Invalid daily budget input: {raw}")
        data = fetch_daily_budget_per_campaign(str(rs_data_account['access_token']), str(rs_data_account['account_id']), str(campaign_id), daily_budget)
        
        # Invalidate cache after budget update
        if data.get('daily_budget'):
            from .utils import invalidate_cache_on_data_update
            invalidate_cache_on_data_update(rs_data_account['account_id'], campaign_id, 'budget_update')
        
        hasil = {
            'daily_budget': data['daily_budget']
        }
        return JsonResponse(hasil)
    
class update_switch_campaign(View):
    def post(self, req):
        try:
            account_id = req.POST.get('account_id')
            campaign_id = req.POST.get('campaign_id')
            status = req.POST.get('switch_campaign')
            
            # Validasi input
            if not campaign_id:
                return JsonResponse({
                    'success': False,
                    'message': 'Campaign ID tidak valid'
                })
            
            if not status:
                return JsonResponse({
                    'success': False,
                    'message': 'Status tidak valid'
                })
            
            # Jika account_id kosong atau '%', coba semua account
            if not account_id or account_id == '%':
                all_accounts = data_mysql().master_account_ads()['data']
                
                for account_data in all_accounts:
                    try:
                        data = fetch_status_per_campaign(
                            str(account_data['access_token']), 
                            str(campaign_id), 
                            str(status)
                        )
                        if 'error' not in data:
                            return JsonResponse({
                                'success': True,
                                'status': data['status'],
                                'message': 'Campaign berhasil diupdate'
                            })
                    except Exception:
                        continue  # Coba account berikutnya
                
                return JsonResponse({
                    'success': False,
                    'message': 'Campaign tidak ditemukan di semua account yang tersedia'
                })
            else:
                # Jika account_id spesifik, gunakan logika lama
                rs_data_account = data_mysql().master_account_ads_by_id({
                    'data_account': account_id,
                })['data']
                
                if not rs_data_account:
                    return JsonResponse({
                        'success': False,
                        'message': 'Account tidak ditemukan'
                    })
                
                data = fetch_status_per_campaign(str(rs_data_account['access_token']), str(campaign_id), str(status))
                
                if 'error' in data:
                    return JsonResponse({
                        'success': False,
                        'message': f'Gagal mengupdate campaign: {data["error"]}'
                    })
                
                return JsonResponse({
                    'success': True,
                    'status': data['status'],
                    'message': 'Campaign berhasil diupdate'
                })
            
        except Exception as e:
            return JsonResponse({
                'success': False,
                'message': f'Terjadi kesalahan: {str(e)}'
            })

class bulk_update_campaign_status(View):
    def post(self, req):
        try:
            account_id = req.POST.get('account_id')
            campaign_ids_json = req.POST.get('campaign_ids')
            status = req.POST.get('status')
            

            # Validasi input
            if not campaign_ids_json:
                return JsonResponse({
                    'success': False,
                    'message': 'Parameter campaign_ids tidak lengkap'
                })
            
            if not status:
                return JsonResponse({
                    'success': False,
                    'message': 'Parameter status tidak lengkap'
                })
            
            # Jika account_id kosong atau '%', ambil semua account
            if not account_id or account_id == '%':
                # Untuk bulk update tanpa account spesifik, kita perlu mengambil account dari campaign_ids
                # Ini akan ditangani di loop update campaign
                account_id = '%'
            
            # Parse campaign IDs
            import json
            campaign_ids = json.loads(campaign_ids_json)
            
            if not campaign_ids:
                return JsonResponse({
                    'success': False,
                    'message': 'Tidak ada campaign yang dipilih'
                })
            
            # Update each campaign
            success_count = 0
            failed_campaigns = []
            
            if account_id == '%':
                # Jika account_id adalah '%', ambil semua account dan cari yang sesuai untuk setiap campaign
                all_accounts = data_mysql().master_account_ads()['data']
                
                for campaign_id in campaign_ids:
                    campaign_updated = False
                    last_error = None
                    for account_data in all_accounts:
                        try:
                            # Coba update campaign dengan account ini
                            data = fetch_status_per_campaign(
                                str(account_data['access_token']), 
                                str(campaign_id), 
                                str(status)
                            )
                            if 'error' not in data:
                                success_count += 1
                                campaign_updated = True
                                break  # Campaign berhasil diupdate, lanjut ke campaign berikutnya
                            else:
                                last_error = data['error']
                        except Exception as e:
                            last_error = str(e)
                            continue  # Coba account berikutnya
                    
                    if not campaign_updated:
                        failed_campaigns.append({'id': campaign_id, 'error': last_error})
            else:
                # Jika account_id spesifik, gunakan logika lama
                rs_data_account = data_mysql().master_account_ads_by_id({
                    'data_account': account_id,
                })['data']
                
                if not rs_data_account:
                    return JsonResponse({
                        'success': False,
                        'message': 'Account tidak ditemukan'
                    })
                
                for campaign_id in campaign_ids:
                    try:
                        data = fetch_status_per_campaign(
                            str(rs_data_account['access_token']), 
                            str(campaign_id), 
                            str(status)
                        )
                        if 'error' not in data:
                            success_count += 1
                        else:
                            failed_campaigns.append({'id': campaign_id, 'error': data['error']})
                    except Exception as e:
                        failed_campaigns.append({'id': campaign_id, 'error': str(e)})
            
            if success_count == len(campaign_ids):
                return JsonResponse({
                    'success': True,
                    'message': f'Berhasil mengupdate {success_count} campaign'
                })
            elif success_count > 0:
                error_details = [f"Campaign {fc['id']}: {fc['error']}" for fc in failed_campaigns[:3]]  # Tampilkan 3 error pertama
                error_summary = "; ".join(error_details)
                if len(failed_campaigns) > 3:
                    error_summary += f" dan {len(failed_campaigns) - 3} error lainnya"
                return JsonResponse({
                    'success': True,
                    'message': f'Berhasil mengupdate {success_count} dari {len(campaign_ids)} campaign. Error: {error_summary}'
                })
            else:
                error_details = [f"Campaign {fc['id']}: {fc['error']}" for fc in failed_campaigns[:3]]  # Tampilkan 3 error pertama
                error_summary = "; ".join(error_details)
                if len(failed_campaigns) > 3:
                    error_summary += f" dan {len(failed_campaigns) - 3} error lainnya"
                return JsonResponse({
                    'success': False,
                    'message': f'Semua campaign gagal diupdate. Error: {error_summary}'
                })
                
        except Exception as e:
            return JsonResponse({
                'success': False,
                'message': f'Terjadi kesalahan: {str(e)}'
            })
    
class PerCampaignFacebookAds(View):
    def dispatch(self, request, *args, **kwargs):
        if 'hris_admin' not in request.session:
            return redirect('admin_login')
        return super(PerCampaignFacebookAds, self).dispatch(request, *args, **kwargs)
    def get(self, req):
        data_account = data_mysql().master_account_ads()['data']
        rs_account = data_mysql().master_account_ads()
        data_campaign = fetch_data_insights_account_filter_all(rs_account['data'])
        data = {
            'title': 'Data Traffic Per Campaign Facebook Ads',
            'user': req.session['hris_admin'],
        }
        return render(req, 'admin/facebook_ads/campaign/index.html', {'data_account': data_account, 'data_campaign': data_campaign, 'data': data})

class page_per_campaign_facebook(View):
    def dispatch(self, request, *args, **kwargs):
        if 'hris_admin' not in request.session:
            return redirect('admin_login')
        elif 'hris_admin' not in request.session:
            return redirect('user_login')
        return super(page_per_campaign_facebook, self).dispatch(request, *args, **kwargs)
    def get(self, req):
        tanggal_dari = req.GET.get('tanggal_dari')
        tanggal_sampai = req.GET.get('tanggal_sampai')
        data_sub_domain = req.GET.get('data_sub_domain')
        data_account = req.GET.get('data_account')
        rs_account = data_mysql().master_account_ads()
        if (data_sub_domain != '%' or data_sub_domain == '%') and data_account != '%':
            rs_data_account = data_mysql().master_account_ads_by_id({
                'data_account': data_account,
            })['data']
            data = fetch_data_insights_campaign_filter_account(str(rs_data_account['access_token']), str(rs_data_account['account_id']), str(rs_data_account['account_name']), str(tanggal_dari), str(tanggal_sampai), str(data_sub_domain))
        else:  
            data = fetch_data_insights_campaign_filter_sub_domain(rs_account['data'], str(tanggal_dari), str(tanggal_sampai), str(data_sub_domain))
        hasil = {
            'hasil': "Data Traffic Per Campaign",
            'data_campaign': data['data'],
            'total_campaign': data['total'],
        }
        return JsonResponse(hasil)
    
class PerCountryFacebookAds(View):
    def dispatch(self, request, *args, **kwargs):
        if 'hris_admin' not in request.session:
            return redirect('admin_login')
        return super(PerCountryFacebookAds, self).dispatch(request, *args, **kwargs)
    def get(self, req):
        data_account = data_mysql().master_account_ads()['data']
        rs_account = data_mysql().master_account_ads()
        data_campaign = fetch_data_insights_account_filter_all(rs_account['data'])
        data = {
            'title': 'Data Traffic Per Country Facebook Ads',
            'user': req.session['hris_admin'],
        }
        return render(req, 'admin/facebook_ads/country/index.html', {'data_account': data_account, 'data_campaign': data_campaign, 'data': data})
    
class page_per_country_facebook(View):
    def dispatch(self, request, *args, **kwargs):
        if 'hris_admin' not in request.session:
            return redirect('admin_login')
        elif 'hris_admin' not in request.session:
            return redirect('user_login')
        return super(page_per_country_facebook, self).dispatch(request, *args, **kwargs)
    
    def get(self, req):
        tanggal_dari = req.GET.get('tanggal_dari')
        tanggal_sampai = req.GET.get('tanggal_sampai')
        data_sub_domain = req.GET.get('data_sub_domain')
        data_account = req.GET.get('data_account')
        
        # Ambil parameter countries dari query string
        countries_param = req.GET.get('countries', '')
        selected_countries = []
        if countries_param:
            selected_countries = countries_param.split(',')

        rs_account = data_mysql().master_account_ads()
        if data_sub_domain != '%' and data_account != '%':
            rs_data_account = data_mysql().master_account_ads_by_id({
                'data_account': data_account,
            })['data']
            data = fetch_data_insights_by_country_filter_account(str(rs_data_account['access_token']), str(rs_data_account['account_id']), str(tanggal_dari), str(tanggal_sampai), str(data_sub_domain))
        else: 
            data = fetch_data_insights_by_country_filter_campaign(rs_account['data'], str(tanggal_dari), str(tanggal_sampai), str(data_sub_domain)) 
        
        # Filter data berdasarkan negara yang dipilih jika ada
        if selected_countries:
            filtered_data = []
            total_spend = 0
            total_impressions = 0
            total_reach = 0
            total_clicks = 0
            total_frequency = 0
            total_cpr = 0
            
            for item in data['data']:
                # Cek apakah negara ada dalam filter yang dipilih
                country_code = item.get('country_code', '')
                country_name = item.get('country', '')
                
                # Cek berdasarkan kode negara atau nama negara
                if country_code in selected_countries or country_name in selected_countries:
                    filtered_data.append(item)
                    total_spend += float(item.get('spend', 0))
                    total_impressions += int(item.get('impressions', 0))
                    total_reach += int(item.get('reach', 0))
                    total_clicks += int(item.get('clicks', 0))
                    total_frequency += float(item.get('frequency', 0))
                    total_cpr += float(item.get('cpr', 0))
            
            # Update data dengan hasil filter
            data['data'] = filtered_data
            data['total'] = [{
                'total_spend': total_spend,
                'total_impressions': total_impressions,
                'total_reach': total_reach,
                'total_click': total_clicks,
                'total_frequency': total_frequency / len(filtered_data) if filtered_data else 0,
                'total_cpr': total_cpr / len(filtered_data) if filtered_data else 0
            }]
        
        hasil = {
            'hasil': "Data Traffic Per Country",
            'data_country': data['data'],
            'total_country': data['total'],
        }
        
        # Debug: Print response structure
        print("DEBUG - Response structure:")
        print(f"total_country: {data['total']}")
        
        return JsonResponse(hasil)
    
    def post(self, req):
        import json
        tanggal_dari = req.POST.get('tanggal_dari')
        tanggal_sampai = req.POST.get('tanggal_sampai')
        data_sub_domain = req.POST.get('data_sub_domain')
        data_account = req.POST.get('data_account')
        selected_countries_json = req.POST.get('selected_countries', '[]')
        
        try:
            selected_countries = json.loads(selected_countries_json)
        except:
            selected_countries = []
        
        rs_account = data_mysql().master_account_ads()
        if data_sub_domain != '%' and data_account != '%':
            rs_data_account = data_mysql().master_account_ads_by_id({
                'data_account': data_account,
            })['data']
            data = fetch_data_insights_by_country_filter_account(str(rs_data_account['access_token']), str(rs_data_account['account_id']), str(tanggal_dari), str(tanggal_sampai), str(data_sub_domain))
        else: 
            data = fetch_data_insights_by_country_filter_campaign(rs_account['data'], str(tanggal_dari), str(tanggal_sampai), str(data_sub_domain)) 
        
        # Normalize total structure - utils.py returns total as array, we need object
        if 'total' in data and isinstance(data['total'], list) and len(data['total']) > 0:
            original_total = data['total'][0]
            data['total'] = {
                'impressions': original_total.get('total_impressions', 0),
                'spend': original_total.get('total_spend', 0),
                'clicks': original_total.get('total_click', 0),  # Note: utils.py uses 'total_click'
                'reach': original_total.get('total_reach', 0),
                'frequency': original_total.get('total_frequency', 0),
                'ctr': round((original_total.get('total_click', 0) / original_total.get('total_impressions', 1)) * 100, 2) if original_total.get('total_impressions', 0) > 0 else 0,
                'cost_per_result': original_total.get('total_cpr', 0)
            }
        
        print(f"DEBUG - Normalized total: {data.get('total', {})}")
        
        # Filter data berdasarkan negara yang dipilih
        if selected_countries and len(selected_countries) > 0:
            print(f"DEBUG - Filtering by countries: {selected_countries}")
            filtered_data = []
            for country_data in data['data']:
                # Ekstrak country code dari format "Country Name (CODE)"
                country_field = country_data.get('country', '')
                if '(' in country_field and ')' in country_field:
                    # Ekstrak kode negara dari dalam kurung
                    country_code = country_field.split('(')[-1].replace(')', '').strip()
                    if country_code in selected_countries:
                        filtered_data.append(country_data)
            data['data'] = filtered_data
            
            # Recalculate totals hanya jika ada data yang difilter
            if filtered_data:
                total_impressions = sum(int(item.get('impressions', 0)) for item in filtered_data)
                total_spend = sum(float(item.get('spend', 0)) for item in filtered_data)
                total_clicks = sum(int(item.get('clicks', 0)) for item in filtered_data)
                total_reach = sum(int(item.get('reach', 0)) for item in filtered_data)
                
                # Hitung frequency dan CPR yang benar berdasarkan total agregat
                frequency = round(total_impressions / total_reach, 2) if total_reach > 0 else 0
                ctr = round((total_clicks / total_impressions) * 100, 2) if total_impressions > 0 else 0
                cost_per_result = round(total_spend / total_clicks, 2) if total_clicks > 0 else 0
                
                data['total'] = {
                    'impressions': total_impressions,
                    'spend': total_spend,
                    'clicks': total_clicks,
                    'reach': total_reach,
                    'frequency': frequency,
                    'ctr': ctr,
                    'cost_per_result': cost_per_result
                }
                print(f"DEBUG - Recalculated total after filtering:")
                print(f"  - Total impressions: {total_impressions}")
                print(f"  - Total reach: {total_reach}")
                print(f"  - Total clicks: {total_clicks}")
                print(f"  - Total spend: {total_spend}")
                print(f"  - Calculated frequency: {frequency}")
                print(f"  - Calculated CPR: {cost_per_result}")
            else:
                # Jika tidak ada data setelah filter, set total ke 0
                data['total'] = {
                    'impressions': 0,
                    'spend': 0,
                    'clicks': 0,
                    'reach': 0,
                    'frequency': 0,
                    'ctr': 0,
                    'cost_per_result': 0
                }
                print("DEBUG - No data after filtering, set total to 0")
        else:
            print("DEBUG - No country filter applied, using original total")
        
        hasil = {
            'hasil': "Data Traffic Per Country",
            'data_country': data['data'],
            'total_country': data['total'],
        }
        return JsonResponse(hasil)


class page_ad_manager_reports(View):
    def dispatch(self, request, *args, **kwargs):
        if 'hris_admin' not in request.session:
            return redirect('admin_login')
        return super().dispatch(request, *args, **kwargs)
    
    def get(self, req):
        start_date = req.GET.get('start_date', '2024-01-01')
        end_date = req.GET.get('end_date', '2024-01-31')
        # Ambil data laporan
        reports = fetch_ad_manager_reports(start_date, end_date)
        inventory = fetch_ad_manager_inventory()
        context = {
            'reports': reports,
            'inventory': inventory,
            'start_date': start_date,
            'end_date': end_date
        }
        return render(req, 'admin/ad_manager/reports.html', context)

# ===== AdX Manager Views =====
class AdxSummaryView(View):
    """View untuk AdX Summary - menampilkan ringkasan data AdManager"""
    def dispatch(self, request, *args, **kwargs):
        if 'hris_admin' not in request.session:
            return redirect('admin_login')
        return super().dispatch(request, *args, **kwargs)
    
    def get(self, req):
        data = {
            'title': 'AdX Summary Dashboard',
            'user': req.session['hris_admin'],
        }
        return render(req, 'admin/adx_manager/summary/index.html', data)

class AdxSummaryDataView(View):
    """AJAX endpoint untuk data AdX Summary"""
    def dispatch(self, request, *args, **kwargs):
        if 'hris_admin' not in request.session:
            return redirect('admin_login')
        return super().dispatch(request, *args, **kwargs)
    
    def get(self, req):
        start_date = req.GET.get('start_date')
        end_date = req.GET.get('end_date')
        if not start_date or not end_date:      
            return JsonResponse({
                'status': False,
                'error': 'Start date and end date are required'
            })
        try:
            # Ambil user_id dari session
            user_id = req.session.get('hris_admin', {}).get('user_id')
            print(f"DEBUG - Session user_id: {user_id}")
            if not user_id:
                return JsonResponse({
                    'status': False,
                    'error': 'User ID tidak ditemukan dalam session'
                })
            
            # Ambil email user dari database berdasarkan user_id
            from management.database import data_mysql
            user_data = data_mysql().get_user_by_id(user_id)
            if not user_data['status'] or not user_data['data']:
                return JsonResponse({
                    'status': False,
                    'error': 'Data user tidak ditemukan dalam database'
                })
            
            user_mail = user_data['data']['user_mail']
            if not user_mail:
                return JsonResponse({
                    'status': False,
                    'error': 'Email user tidak ditemukan dalam database'
                })
            result = fetch_adx_traffic_account_by_user(user_mail, start_date, end_date)
            # Tambahkan data traffic hari ini
            today = datetime.now().strftime('%Y-%m-%d')
            today_result = fetch_adx_traffic_account_by_user(user_mail, today, today)
            
            # Tambahkan today_traffic ke result
            if today_result.get('status') and today_result.get('summary'):
                result['today_traffic'] = {
                    'impressions': today_result['summary'].get('total_impressions', 0),
                    'clicks': today_result['summary'].get('total_clicks', 0),
                    'revenue': today_result['summary'].get('total_revenue', 0),
                    'ctr': today_result['summary'].get('avg_ctr', 0)
                }
            else:
                result['today_traffic'] = {
                    'impressions': 0,
                    'clicks': 0,
                    'revenue': 0,
                    'ctr': 0
                }
            return JsonResponse(result)
            
        except Exception as e:
            print(f"Error in AdxSummaryDataView: {str(e)}")
            return JsonResponse({
                'status': False,
                'error': str(e)
            })

class AdxSummaryAdChangeDataView(View):
    """AJAX endpoint untuk data Ad Change di AdX Summary"""
    def dispatch(self, request, *args, **kwargs):
        if 'hris_admin' not in request.session:
            return redirect('admin_login')
        return super().dispatch(request, *args, **kwargs)
    
    def get(self, req):
        start_date = req.GET.get('start_date')
        end_date = req.GET.get('end_date')
        
        # Debug: Log all GET parameters
        print(f"[DEBUG] ===== ALL GET PARAMETERS =====")
        for key, value in req.GET.items():
            print(f"[DEBUG] {key}: {value}")
        print(f"[DEBUG] ===== END ALL GET PARAMETERS =====")
        
        # Handle site_filter - check both array format and string format
        site_filter_list = req.GET.getlist('site_filter[]')  # Array format
        site_filter_string = req.GET.get('site_filter', '')   # String format
        
        if site_filter_list:
            site_filter = ','.join(site_filter_list)
            print(f"[DEBUG] Using array format site_filter: {site_filter}")
        else:
            site_filter = site_filter_string
            print(f"[DEBUG] Using string format site_filter: {site_filter}")
        
        if not start_date or not end_date:
            return JsonResponse({
                'status': False,
                'error': 'Start date and end date are required'
            })
        
        try:
            from .utils import fetch_adx_ad_change_data
            result = fetch_adx_ad_change_data(start_date, end_date)
            
            # Apply site filter if provided
            if site_filter and site_filter != '%' and result.get('status'):
                filtered_data = []
                for item in result['data']:
                    if site_filter.lower() in item['ad_unit'].lower():
                        filtered_data.append(item)
                
                # Recalculate summary for filtered data
                if filtered_data:
                    total_impressions = sum(item['impressions'] for item in filtered_data)
                    total_clicks = sum(item['clicks'] for item in filtered_data)
                    total_revenue = sum(item['revenue'] for item in filtered_data)
                    total_cpc_revenue = sum(item['cpc_revenue'] for item in filtered_data)
                    total_cpm_revenue = sum(item['cpm_revenue'] for item in filtered_data)
                    
                    avg_ctr = (total_clicks / total_impressions * 100) if total_impressions > 0 else 0
                    avg_ecpm = (total_revenue / total_impressions * 1000) if total_impressions > 0 else 0
                    avg_cpc = (total_cpc_revenue / total_clicks) if total_clicks > 0 else 0
                    
                    result['data'] = filtered_data
                    result['summary'] = {
                        'total_impressions': total_impressions,
                        'total_clicks': total_clicks,
                        'total_revenue': total_revenue,
                        'total_cpc_revenue': total_cpc_revenue,
                        'total_cpm_revenue': total_cpm_revenue,
                        'avg_ctr': avg_ctr,
                        'avg_ecpm': avg_ecpm,
                        'avg_cpc': avg_cpc
                    }
                else:
                    result['data'] = []
                    result['summary'] = {
                        'total_impressions': 0,
                        'total_clicks': 0,
                        'total_revenue': 0,
                        'total_cpc_revenue': 0,
                        'total_cpm_revenue': 0,
                        'avg_ctr': 0,
                        'avg_ecpm': 0,
                        'avg_cpc': 0
                    }
            
            print(f"fetch_adx_ad_change_data returned: {result}")
            return JsonResponse(result)
            
        except Exception as e:
            print(f"Error in AdxSummaryAdChangeDataView: {str(e)}")
            return JsonResponse({
                'status': False,
                'error': str(e)
            })

class AdxActiveSitesView(View):
    """AJAX endpoint untuk mendapatkan daftar situs aktif"""
    def dispatch(self, request, *args, **kwargs):
        if 'hris_admin' not in request.session:
            return redirect('admin_login')
        return super().dispatch(request, *args, **kwargs)
    
    def get(self, req):
        try:
            from .utils import fetch_adx_active_sites
            result = fetch_adx_active_sites()
            print(f"fetch_adx_active_sites returned: {result}")
            return JsonResponse(result)
            
        except Exception as e:
            print(f"Error in AdxActiveSitesView: {str(e)}")
            return JsonResponse({
                'status': False,
                'error': str(e)
            })

class CacheStatsView(View):
    """View untuk monitoring cache statistics"""
    def dispatch(self, request, *args, **kwargs):
        if 'hris_admin' not in request.session:
            return redirect('admin_login')
        return super().dispatch(request, *args, **kwargs)
    
    def get(self, req):
        from .utils import get_cache_stats, clear_all_facebook_cache
        
        action = req.GET.get('action')
        
        if action == 'clear_cache':
            clear_result = clear_all_facebook_cache()
            return JsonResponse({
                'status': clear_result,
                'message': 'Cache cleared successfully' if clear_result else 'Failed to clear cache'
            })
        
        # Get cache statistics
        stats = get_cache_stats()
        return JsonResponse({
            'status': True,
            'data': stats
        })

class AdxAccountView(View):
    """View untuk AdX Account Data"""
    def dispatch(self, request, *args, **kwargs):
        if 'hris_admin' not in request.session:
            return redirect('admin_login')
        return super().dispatch(request, *args, **kwargs)
    
    def get(self, req):
        data = {
            'title': 'AdX Account Data',
            'user': req.session['hris_admin'],
        }
        return render(req, 'admin/adx_manager/account/index.html', data)

class AdxAccountDataView(View):
    """AJAX endpoint untuk data AdX Account"""
    def dispatch(self, request, *args, **kwargs):
        if 'hris_admin' not in request.session:
            return redirect('admin_login')
        return super().dispatch(request, *args, **kwargs)
    def get(self, req):
        try:
            result = fetch_adx_account_data()
            return JsonResponse(result)
        except Exception as e:
            return JsonResponse({
                'status': False,
                'error': str(e)
            })

class AdxUserAccountDataView(View):
    """AJAX endpoint untuk data AdX Account berdasarkan kredensial pengguna"""
    def dispatch(self, request, *args, **kwargs):
        if 'hris_admin' not in request.session:
            return redirect('admin_login')
        return super().dispatch(request, *args, **kwargs)
    
    def get(self, req):
        try:
            # Ambil user_id dari session
            user_id = req.session.get('hris_admin', {}).get('user_id')
            if not user_id:
                return JsonResponse({
                    'status': False,
                    'error': 'User ID tidak ditemukan dalam session'
                })
            
            # Ambil email user dari database berdasarkan user_id
            user_data = data_mysql().get_user_by_id(user_id)
            if not user_data['status'] or not user_data['data']:
                return JsonResponse({
                    'status': False,
                    'error': 'Data user tidak ditemukan dalam database'
                })
            user_mail = user_data['data']['user_mail']
            if not user_mail:
                return JsonResponse({
                    'status': False,
                    'error': 'Email user tidak ditemukan dalam database'
                })
            
            # Fetch comprehensive account data using user's credentials
            result = fetch_user_adx_account_data(user_mail)
            return JsonResponse(result)
            
        except Exception as e:
            return JsonResponse({
                'status': False,
                'error': str(e)
            })

class GenerateRefreshTokenView(View):
    """AJAX endpoint untuk generate refresh token otomatis dari database credentials"""
    def dispatch(self, request, *args, **kwargs):
        if 'hris_admin' not in request.session:
            return redirect('admin_login')
        return super().dispatch(request, *args, **kwargs)
    
    def post(self, req):
        try:
            # Ambil user_mail dari session
            user_mail = req.session.get('hris_admin', {}).get('user_mail')
            if not user_mail:
                return JsonResponse({
                    'status': False,
                    'error': 'User Mail tidak ditemukan dalam session'
                })
            
            # Debug logging
            print(f"DEBUG Generate Refresh Token - User Mail from session: {user_mail}")
            
            # Ambil data user dari database
            db = data_mysql()
            user_data = db.get_user_by_mail(user_mail)
            if not user_data['status'] or not user_data['data']:
                return JsonResponse({
                    'status': False,
                    'error': 'Data user tidak ditemukan dalam database'
                })
            
            user_info = user_data['data']
            user_mail = user_info['user_mail']
            
            print(f"DEBUG Generate Refresh Token - User email: {user_mail}")
            print(f"DEBUG Generate Refresh Token - Client ID: {user_info.get('google_ads_client_id')}")
            print(f"DEBUG Generate Refresh Token - Client Secret: {user_info.get('google_ads_client_secret')}")
            
            # Cek apakah user sudah memiliki client_id dan client_secret di database
            if not user_info.get('google_ads_client_id') or not user_info.get('google_ads_client_secret'):
                return JsonResponse({
                    'status': False,
                    'error': 'Client ID dan Client Secret belum dikonfigurasi untuk user ini. Silakan hubungi administrator.',
                    'action_required': 'configure_oauth_credentials'
                })
            
            # Generate refresh token menggunakan credentials dari database
            result = db.generate_refresh_token_from_db_credentials(user_mail)
            if result['status']:
                return JsonResponse({
                    'status': True,
                    'message': 'Refresh token berhasil di-generate dan disimpan',
                    'data': {
                        'user_mail': user_mail,
                        'refresh_token_generated': True,
                        'timestamp': result.get('timestamp', 'Unknown')
                    }
                })
            else:
                return JsonResponse({
                    'status': False,
                    'error': result.get('message', 'Gagal generate refresh token'),
                    'details': result.get('details', 'No additional details')
                })
                
        except Exception as e:
            return JsonResponse({
                'status': False,
                'error': f'Error saat generate refresh token: {str(e)}'
            })

class SaveOAuthCredentialsView(View):
    """AJAX endpoint untuk menyimpan OAuth credentials ke database"""
    def dispatch(self, request, *args, **kwargs):
        if 'hris_admin' not in request.session:
            return redirect('admin_login')
        return super().dispatch(request, *args, **kwargs)
    
    def post(self, req):
        try:
            client_id = req.POST.get('client_id')
            client_secret = req.POST.get('client_secret')
            ads_client_id = req.POST.get('client_id')
            ads_client_secret = req.POST.get('client_secret')
            network_code = req.POST.get('network_code')
            user_mail = req.POST.get('user_mail')  # Ubah dari user_mail ke user_mail
            
            # Debug logging
            print(f"DEBUG OAuth Save - Received data:")
            print(f"  client_id: {client_id}")
            print(f"  client_secret: {client_secret}")
            print(f"  network_code: {network_code}")
            print(f"  user_mail: {user_mail}")  # Update log message
            
            if not client_id or not client_secret or not network_code or not user_mail:  # Update condition
                print("DEBUG OAuth Save - Validation failed: missing fields")
                return JsonResponse({
                    'status': False,
                    'error': 'Client ID, Client Secret, Network Code, dan User Email harus diisi'
                })
            
            # Update OAuth credentials di database
            db = data_mysql()
            print(f"DEBUG OAuth Save - Calling update_oauth_credentials with email: {user_mail}")  # Update log message
            result = db.update_oauth_credentials(user_mail, client_id, client_secret, ads_client_id, ads_client_secret, network_code)  # Update parameter
            print(f"DEBUG OAuth Save - Database result: {result}")
            
            if result['status']:
                return JsonResponse({
                    'status': True,
                    'message': 'OAuth credentials berhasil disimpan',
                    'data': {
                        'user_mail': user_mail,  # Update field name
                        'client_id': client_id[:10] + '...',  # Hanya tampilkan sebagian untuk keamanan
                        'updated': True
                    }
                })
            else:
                return JsonResponse({
                    'status': False,
                    'error': result.get('message', 'Gagal menyimpan OAuth credentials')
                })
                
        except Exception as e:
            return JsonResponse({
                'status': False,
                'error': f'Error saat menyimpan OAuth credentials: {str(e)}'
            })

class OAuthCallbackView(View):
    """
    View untuk handle OAuth callback dari Google
    """
    
    def get(self, request):
        """Handle GET request dari Google OAuth redirect"""
        return render(request, 'admin/oauth_callback.html')
    
    @csrf_exempt
    def dispatch(self, *args, **kwargs):
        return super().dispatch(*args, **kwargs)
    
    def post(self, request):
        """Handle POST request untuk process authorization code"""
        try:
            # Parse JSON body atau form data
            if request.content_type == 'application/json':
                data = json.loads(request.body)
                auth_code = data.get('code')
            else:
                auth_code = request.POST.get('code')
            
            if not auth_code:
                return JsonResponse({
                    'status': False,
                    'message': 'Authorization code tidak ditemukan'
                })
            
            return handle_oauth_callback(request, auth_code)
            
        except json.JSONDecodeError:
            return JsonResponse({
                'status': False,
                'message': 'Invalid JSON data'
            })
        except Exception as e:
            logger.error(f"Error in OAuth callback: {str(e)}")
            return JsonResponse({
                'status': False,
                'message': f'Error: {str(e)}'
            })

class GenerateOAuthURLView(View):
    """View untuk generate OAuth URL dengan email yang diberikan"""
    def post(self, request):
        try:
            email = request.POST.get('email', '').strip()
            
            if not email:
                return JsonResponse({
                    'success': False,
                    'message': 'Email diperlukan'
                })
            
            # Import OAuth utilities
            from management.oauth_utils import generate_oauth_url_for_user
            
            # Generate OAuth URL
            oauth_url, error = generate_oauth_url_for_user(
                user_mail=email,
                scopes=['https://www.googleapis.com/auth/admanager']
            )
            
            if error:
                return JsonResponse({
                    'success': False,
                    'message': f'Gagal generate OAuth URL: {error}'
                })
            
            return JsonResponse({
                'success': True,
                'oauth_url': oauth_url,
                'message': 'OAuth URL berhasil di-generate'
            })
            
        except Exception as e:
            return JsonResponse({
                'success': False,
                'message': f'Error: {str(e)}'
            })

class ProcessOAuthCodeView(View):
    """View untuk memproses authorization code dan generate refresh token"""
    def post(self, request):
        try:
            email = request.POST.get('email', '').strip()
            auth_code = request.POST.get('auth_code', '').strip()
            
            if not email or not auth_code:
                return JsonResponse({
                    'success': False,
                    'message': 'Email dan authorization code diperlukan'
                })
            
            # Import OAuth utilities
            from management.oauth_utils import exchange_code_for_refresh_token
            from management.database import data_mysql
            
            # Exchange authorization code for refresh token
            refresh_token, token_data, error = exchange_code_for_refresh_token(auth_code)
            
            if error:
                return JsonResponse({
                    'success': False,
                    'message': f'Gagal exchange code: {error}'
                })
            
            if not refresh_token:
                return JsonResponse({
                    'success': False,
                    'message': 'Refresh token tidak ditemukan dalam response'
                })
            
            # Save refresh token to database for the specified email
            db = data_mysql()
            try:
                sql = """
                    UPDATE app_oauth_credentials 
                    SET google_ads_refresh_token = %s,
                        updated_at = NOW()
                    WHERE user_mail = %s
                """
                
                if db.execute_query(sql, (refresh_token, email)):
                    db.db_hris.commit()
                    
                    if db.cur_hris.rowcount > 0:
                        return JsonResponse({
                            'success': True,
                            'message': f'Refresh token berhasil di-generate dan disimpan untuk {email}'
                        })
                    else:
                        return JsonResponse({
                            'success': False,
                            'message': f'User {email} tidak ditemukan di tabel app_oauth_credentials'
                        })
                else:
                    return JsonResponse({
                        'success': False,
                        'message': 'Gagal mengeksekusi query database'
                    })
                    
            except Exception as db_error:
                return JsonResponse({
                    'success': False,
                    'message': f'Error database: {str(db_error)}'
                })
                
        except Exception as e:
            return JsonResponse({
                'success': False,
                'message': f'Error: {str(e)}'
            })

class AdxTrafficPerAccountView(View):
    """View untuk AdX Traffic Per Account"""
    def dispatch(self, request, *args, **kwargs):
        if 'hris_admin' not in request.session:
            return redirect('admin_login')
        return super().dispatch(request, *args, **kwargs)
    def get(self, req):
        data = {
            'title': 'AdX Traffic Per Account',
            'user': req.session['hris_admin'],
        }
        return render(req, 'admin/adx_manager/traffic_account/index.html', data)

class AdxTrafficPerAccountDataView(View):
    """AJAX endpoint untuk data AdX Traffic Per Account"""
    def dispatch(self, request, *args, **kwargs):
        if 'hris_admin' not in request.session:
            return redirect('admin_login')
        return super().dispatch(request, *args, **kwargs)
    
    def get(self, req):
        start_date = req.GET.get('start_date')
        end_date = req.GET.get('end_date')
        selected_sites = req.GET.get('selected_sites')
        try:
            # Ambil user_id dari session
            user_id = req.session.get('hris_admin', {}).get('user_id')
            # Ambil email user dari database berdasarkan user_id
            user_data = data_mysql().get_user_by_id(user_id)
            user_mail = user_data['data']['user_mail']
            # Format tanggal untuk AdManager API
            start_date_formatted = datetime.strptime(start_date, '%Y-%m-%d').strftime('%Y-%m-%d')
            end_date_formatted = datetime.strptime(end_date, '%Y-%m-%d').strftime('%Y-%m-%d')  
            # Gunakan fungsi baru yang mengambil data berdasarkan kredensial user
            result = fetch_adx_traffic_account_by_user(user_mail, start_date_formatted, end_date_formatted, selected_sites)
            return JsonResponse(result, safe=False)
        except Exception as e:
            return JsonResponse({
                'status': False,
                'error': str(e)
            })

class AdxSitesListView(View):
    """AJAX endpoint untuk mengambil daftar situs dari Ad Manager"""
    def dispatch(self, request, *args, **kwargs):
        if 'hris_admin' not in request.session:
            return redirect('admin_login')
        return super().dispatch(request, *args, **kwargs)
    
    def get(self, req):
        try:
            # Ambil user_id dari session
            user_id = req.session.get('hris_admin', {}).get('user_id')
            # Ambil email user dari database berdasarkan user_id
            from management.database import data_mysql
            user_data = data_mysql().get_user_by_id(user_id)
            user_mail = user_data['data']['user_mail']
            # Ambil daftar situs dari Ad Manager
            from management.utils import fetch_user_sites_list
            result = fetch_user_sites_list(user_mail)
            return JsonResponse(result, safe=False)
        except Exception as e:
            return JsonResponse({
                'status': False,
                'error': str(e)
            })

class AdxTrafficPerCampaignView(View):
    """View untuk AdX Traffic Per Campaign"""
    def dispatch(self, request, *args, **kwargs):
        if 'hris_admin' not in request.session:
            return redirect('admin_login')
        return super().dispatch(request, *args, **kwargs)
    
    def get(self, req):
        data = {
            'title': 'AdX Traffic Per Campaign',
            'user': req.session['hris_admin'],
        }
        return render(req, 'admin/adx_manager/traffic_campaign/index.html', data)

class AdxTrafficPerCampaignDataView(View):
    """AJAX endpoint untuk data AdX Traffic Per Campaign"""
    def dispatch(self, request, *args, **kwargs):
        if 'hris_admin' not in request.session:
            return redirect('admin_login')
        return super().dispatch(request, *args, **kwargs)
    
    def get(self, req):
        start_date = req.GET.get('start_date')
        end_date = req.GET.get('end_date')
        site_filter = req.GET.get('site_filter', '')
        
        if not start_date or not end_date:
            return JsonResponse({
                'status': False,
                'error': 'Start date and end date are required'
            })
        
        try:
            # Ambil user_id dari session
            user_id = req.session.get('hris_admin', {}).get('user_id')
            if not user_id:
                return JsonResponse({
                    'status': False,
                    'error': 'User ID tidak ditemukan dalam session'
                })
            
            # Ambil email user dari database berdasarkan user_id
            user_data = data_mysql().get_user_by_id(user_id)
            if not user_data['status'] or not user_data['data']:
                return JsonResponse({
                    'status': False,
                    'error': 'Data user tidak ditemukan dalam database'
                })
            
            user_mail = user_data['data']['user_mail']
            if not user_mail:
                return JsonResponse({
                    'status': False,
                    'error': 'Email user tidak ditemukan dalam database'
                })
            
            # Format tanggal untuk AdManager API
            start_date_formatted = datetime.strptime(start_date, '%Y-%m-%d').strftime('%Y-%m-%d')
            end_date_formatted = datetime.strptime(end_date, '%Y-%m-%d').strftime('%Y-%m-%d')
            
            # Filter situs jika kosong atau '%'
            filter_value = site_filter if site_filter and site_filter != '%' else None
            
            # Gunakan fungsi baru yang mengambil data berdasarkan kredensial user
            from management.utils import fetch_adx_traffic_campaign_by_user
            result = fetch_adx_traffic_campaign_by_user(
                user_mail, 
                start_date_formatted, 
                end_date_formatted, 
                filter_value
            )
            return JsonResponse(result, safe=False)
            
        except Exception as e:
            return JsonResponse({
                'status': False,
                'error': str(e)
            })

class AdxTrafficPerCountryView(View):
    """View untuk AdX Traffic Per Country"""
    def dispatch(self, request, *args, **kwargs):
        if 'hris_admin' not in request.session:
            return redirect('admin_login')
        return super().dispatch(request, *args, **kwargs)
    
    def get(self, req):
        data = {
            'title': 'AdX Traffic Per Country',
            'user': req.session['hris_admin'],
        }
        return render(req, 'admin/adx_manager/traffic_country/index.html', data)

class AdxTrafficPerCountryDataView(View):
    """AJAX endpoint untuk data AdX Traffic Per Country"""
    def dispatch(self, request, *args, **kwargs):
        if 'hris_admin' not in request.session:
            return redirect('admin_login')
        return super().dispatch(request, *args, **kwargs)
    
    def get(self, req):
        start_date = req.GET.get('start_date')
        end_date = req.GET.get('end_date')
        selected_sites = req.GET.get('selected_sites', '') 
        selected_countries = req.GET.get('selected_countries', '')
        try:
            # Format tanggal untuk AdManager API
            start_date_formatted = datetime.strptime(start_date, '%Y-%m-%d').strftime('%Y-%m-%d')
            end_date_formatted = datetime.strptime(end_date, '%Y-%m-%d').strftime('%Y-%m-%d')
            # Parse selected countries dari string yang dipisah koma
            countries_list = []
            if selected_countries and selected_countries.strip():
                countries_list = [country.strip() for country in selected_countries.split(',') if country.strip()]
            else:
                print("[DEBUG] No countries selected, will fetch all countries")
            # Ambil user_id dari session
            user_id = req.session.get('hris_admin', {}).get('user_id')
            # Ambil email user dari database berdasarkan user_id
            user_data = data_mysql().get_user_by_id(user_id)
            user_mail = user_data['data']['user_mail']
            result = fetch_adx_traffic_per_country(start_date_formatted, end_date_formatted, user_mail, selected_sites, countries_list)    
            print(f"[DEBUG] fetch_adx_traffic_per_country result: {result}")
            if isinstance(result, dict):
                if 'data' in result:
                    if result['data']:
                        print(f"[DEBUG] First data item: {result['data'][0]}")
                if 'summary' in result:
                    print(f"[DEBUG] Summary: {result['summary']}")
            return JsonResponse(result, safe=False)
            
        except Exception as e:
            return JsonResponse({
                'status': False,
                'error': str(e)
            })

def authorize(request):
    flow = Flow.from_client_secrets_file(
        settings.CLIENT_SECRETS_FILE,
        scopes=settings.SCOPES,
        redirect_uri=settings.REDIRECT_URI,
    )
    authorization_url, state = flow.authorization_url(
        access_type='offline',
        include_granted_scopes='true'
    )
    request.session['state'] = state
    return redirect(authorization_url)


def oauth2callback(request):
    state = request.session.get('state')
    flow = Flow.from_client_secrets_file(
        settings.CLIENT_SECRETS_FILE,
        scopes=settings.SCOPES,
        state=state,
        redirect_uri=settings.REDIRECT_URI,
    )
    flow.fetch_token(authorization_response=request.build_absolute_uri())

    credentials = flow.credentials

    # Simpan refresh token di session, untuk production simpan di database
    request.session['credentials'] = {
        'token': credentials.token,
        'refresh_token': credentials.refresh_token,
        'token_uri': credentials.token_uri,
        'client_id': credentials.client_id,
        'client_secret': credentials.client_secret,
        'scopes': credentials.scopes,
    }
    
    # Simpan refresh token ke database jika user sudah login
    if 'hris_admin' in request.session and credentials.refresh_token:
        try:
            # Gunakan email yang benar dari session
            user_mail = request.session['hris_admin'].get('user_mail')
            if user_mail:
                db = data_mysql()
                result = db.update_refresh_token(user_mail, credentials.refresh_token)
                # Sesuaikan dengan bentuk nilai kembalian terbaru (top-level 'status')
                if (isinstance(result, dict) and (
                    result.get('status') is True or
                    (result.get('hasil') and result['hasil'].get('status') is True)
                )):
                    print(f"[DEBUG] Google Ads refresh token saved to database for {user_mail}")
                else:
                    # Tangani dua kemungkinan bentuk respons
                    message = (
                        result.get('message') or
                        (result.get('hasil') or {}).get('message') or
                        'Unknown error'
                    ) if isinstance(result, dict) else 'Invalid result type'
                    print(f"[DEBUG] Failed to save Google Ads refresh token: {message}")
        except Exception as e:
            print(f"[DEBUG] Error saving Google Ads refresh token: {e}")
    
    return redirect('/fetch_report')


def fetch_report(request):
    creds_data = request.session.get('credentials')
    if not creds_data:
        return redirect('/authorize')

    creds = Credentials(
        token=creds_data['token'],
        refresh_token=creds_data['refresh_token'],
        token_uri=creds_data['token_uri'],
        client_id=creds_data['client_id'],
        client_secret=creds_data['client_secret'],
        scopes=creds_data['scopes'],
    )

    # Refresh token jika perlu
    creds.refresh(Request())

    # Setup Ad Manager client
    client = ad_manager.AdManagerClient.LoadFromStorage(path=None)
    client.oauth2_credentials = creds
    client.network_code = settings.GOOGLE_ADMGR_NETWORK_CODE

    report_downloader = client.GetDataDownloader(version='v202305')

    report_query = {
        'dimensions': ['DATE'],
        'columns': [
            'AD_SERVER_CLICKS',
            'AD_SERVER_CPM_AND_CPC_REVENUE',
            'AD_SERVER_CPM_AND_CPC',
            'AD_SERVER_ECPM',
        ],
        'dateRangeType': 'LAST_7_DAYS',
    }

    report_job = {'reportQuery': report_query}

    report_job_id = report_downloader.WaitForReport(report_job)
    
    # Use DownloadReportToFile with binary mode
    with tempfile.NamedTemporaryFile(mode='w+b', delete=True, suffix='.csv') as temp_file:
        report_downloader.DownloadReportToFile(report_job_id, 'CSV_DUMP', temp_file)
        
        # Seek to beginning and read the content, then decode to string
        temp_file.seek(0)
        report_csv = temp_file.read().decode('utf-8')

    # Parse CSV
    f = StringIO(report_csv)
    reader = csv.DictReader(f)
    rows = list(reader)

    # Render di template
    return render(request, 'reports/report.html', {'rows': rows})


# ===== ROI Traffic Per Country =====

class RoiTrafficPerCountryView(View):
    def dispatch(self, request, *args, **kwargs):
        if 'hris_admin' not in request.session:
            return redirect('admin_login')
        return super().dispatch(request, *args, **kwargs)
    
    def get(self, req):
        data_account = data_mysql().master_account_ads()['data']
        data = {
            'title': 'ROI Per Country',
            'user': req.session['hris_admin'],
        }
        return render(req, 'admin/report_roi/per_country/index.html', {'data_account': data_account, 'data': data})

class RoiTrafficPerCountryDataView(View):
    def dispatch(self, request, *args, **kwargs):
        if 'hris_admin' not in request.session:
            return redirect('admin_login')
        return super().dispatch(request, *args, **kwargs)
    
    def get(self, req):
        start_date = req.GET.get('start_date')
        end_date = req.GET.get('end_date')
        selected_sites = req.GET.get('selected_sites', '')
        selected_account = req.GET.get('selected_account', '')
        selected_countries = req.GET.get('selected_countries', '')
        try:
            # Format tanggal untuk AdManager API
            start_date_formatted = datetime.strptime(start_date, '%Y-%m-%d').strftime('%Y-%m-%d')
            end_date_formatted = datetime.strptime(end_date, '%Y-%m-%d').strftime('%Y-%m-%d')
            # Parse selected countries dari string yang dipisah koma
            countries_list = []
            if selected_countries and selected_countries.strip():
                countries_list = [country.strip() for country in selected_countries.split(',') if country.strip()]
            else:
                print("[DEBUG] No countries selected, will fetch all countries")
            # Ambil user_id dari session
            user_id = req.session.get('hris_admin', {}).get('user_id')
            # Ambil email user dari database berdasarkan user_id
            user_data = data_mysql().get_user_by_id(user_id)
            user_mail = user_data['data']['user_mail']
            data_adx = fetch_roi_per_country(start_date_formatted, end_date_formatted, user_mail, selected_sites, countries_list)
            if selected_account:
                rs_account = data_mysql().master_account_ads_by_params({
                    'data_account': selected_account,
                })['data']
            else:
                 rs_account = data_mysql().master_account_ads()['data']
            data_facebook = fetch_data_insights_by_country_filter_campaign_roi(rs_account, str(start_date_formatted), str(end_date_formatted), selected_sites) 
            # Proses penggabungan data AdX dan Facebook
            result = process_roi_traffic_country_data(data_adx, data_facebook)
            print(f" Hasil Akhir: {result}")
            
            # Filter hasil berdasarkan negara yang dipilih jika ada
            if countries_list and result.get('status') and result.get('data'):
                print(f"[DEBUG ROI] Original data has {len(result['data'])} countries")
                print(f"[DEBUG ROI] Countries to filter: {countries_list}")
                
                # Parse selected countries dari format "Country Name (CODE)" menjadi list nama negara
                parsed_filter_countries = []
                for country_item in countries_list:
                    country_item = country_item.strip()
                    if '(' in country_item and ')' in country_item:
                        # Extract country name dari format "Country Name (CODE)"
                        country_name = country_item.split('(')[0].strip()
                        parsed_filter_countries.append(country_name.lower())
                    else:
                        parsed_filter_countries.append(country_item.lower())
                
                print(f"[DEBUG ROI] Parsed filter countries: {parsed_filter_countries}")
                
                filtered_data = []
                for item in result['data']:
                    country_code = item.get('country_code', '').lower()
                    country_name = item.get('country', '').lower()
                    print(f"[DEBUG ROI] Checking country: '{country_name}' (code: '{country_code}')")
                    
                    # Check if country matches any in the filter list (case insensitive)
                    country_matched = False
                    for filter_country in parsed_filter_countries:
                        if country_name == filter_country or country_code == filter_country:
                            print(f"[DEBUG ROI] ✓ MATCH FOUND: '{country_name}' matches '{filter_country}'")
                            country_matched = True
                            break
                    
                    # Only add to filtered_data if country_matched is True
                    if country_matched:
                        filtered_data.append(item)
                        print(f"[DEBUG ROI] ✓ Added '{country_name}' to filtered results")
                    else:
                        print(f"[DEBUG ROI] ✗ No match found for '{country_name}' - EXCLUDED from results")
                
                result['data'] = filtered_data
                result['total_records'] = len(filtered_data)
                print(f"[DEBUG ROI] Manually filtered results to {len(filtered_data)} countries")
            
            return JsonResponse(result, safe=False)
            
        except Exception as e:
            return JsonResponse({
                'status': False,
                'error': str(e)
            })

def process_roi_traffic_country_data(data_adx, data_facebook):
    """Fungsi untuk menggabungkan data AdX dan Facebook berdasarkan kode negara dan menghitung ROI"""
    try:
        # Inisialisasi hasil
        combined_data = []
        # Buat mapping data Facebook berdasarkan country_cd
        facebook_spend_map = {}
        facebook_other_costs_map = {}
        facebook_click_map = {}
        unknown_spend = 0
        unknown_other_costs = 0
        if data_facebook and data_facebook.get('data'):
            for fb_item in data_facebook['data']:
                country_cd = fb_item.get('country_cd', 'unknown')
                spend = float(fb_item.get('spend', 0))
                other_costs = float(fb_item.get('other_costs', 0))
                facebook_spend_map[country_cd] = spend
                facebook_other_costs_map[country_cd] = other_costs
                facebook_click_map[country_cd] = int(fb_item.get('clicks', 0))
                # Simpan spend dan other_costs untuk country_cd "unknown"
                if country_cd == 'unknown':
                    unknown_spend = spend
                    unknown_other_costs = other_costs
        # Proses data AdX dan gabungkan dengan data Facebook
        if data_adx and data_adx.get('status') and data_adx.get('data'):
            for adx_item in data_adx['data']:
                country_name = adx_item.get('country_name', '')
                country_code = adx_item.get('country_code', '')
                impressions = int(adx_item.get('impressions', 0))
                clicks_adx = int(adx_item.get('clicks', 0))
                revenue = float(adx_item.get('revenue', 0))
                # Ambil spend dan biaya lainnya dari Facebook berdasarkan country_code
                spend = facebook_spend_map.get(country_code, 0)
                other_costs = facebook_other_costs_map.get(country_code, 0)
                click_fb = facebook_click_map.get(country_code, 0)
                if click_fb > 0:
                    clicks = click_fb
                else:
                    clicks = clicks_adx
                # Jika spend tidak tersedia untuk country_code, gunakan spend dari "unknown"
                if spend == 0 and unknown_spend > 0:
                    spend = 0
                if other_costs == 0 and unknown_other_costs > 0:
                    other_costs = 0
                
                # Hitung total biaya untuk ROI nett
                total_costs = spend + other_costs
                
                # Hitung metrik
                ctr = (clicks / impressions * 100) if impressions > 0 else 0
                cpc = (revenue / clicks) if clicks > 0 else 0
                ecpm = (revenue / impressions * 1000) if impressions > 0 else 0
                # ROI nett: ((revenue - total_costs) / total_costs * 100)
                roi = ((revenue - total_costs) / total_costs * 100) if total_costs > 0 else 0
                # Tambahkan ke hasil
                combined_data.append({
                    'country': country_name,
                    'country_code': country_code,
                    'impressions': impressions,
                    'spend': round(spend, 2),
                    'other_costs': round(other_costs, 2),
                    'total_costs': round(total_costs, 2),
                    'clicks': clicks,
                    'revenue': round(revenue, 2),
                    'ctr': round(ctr, 2),
                    'cpc': round(cpc, 4),
                    'ecpm': round(ecpm, 2),
                    'roi': round(roi, 2)
                })
        
        # Urutkan berdasarkan ROI tertinggi
        combined_data.sort(key=lambda x: x['roi'], reverse=True)
    
        return {
            'status': True,
            'data': combined_data,
            'total_records': len(combined_data)
        }
        
    except Exception as e:
        return {
            'status': False,
            'error': f'Error processing ROI traffic country data: {str(e)}',
            'data': []
        }

class RoiTrafficPerDomainView(View):
    """View untuk ROI Per Domain"""
    def dispatch(self, request, *args, **kwargs):
        if 'hris_admin' not in request.session:
            return redirect('admin_login')
        return super().dispatch(request, *args, **kwargs)
    def get(self, req):
        data_account = data_mysql().master_account_ads()['data']
        data = {
            'title': 'ROI Per Domain',
            'user': req.session['hris_admin'],
        }
        return render(req, 'admin/report_roi/per_domain/index.html', {'data_account': data_account, 'data': data})

class RoiTrafficPerDomainDataView(View):
    """AJAX endpoint untuk data ROI Traffic Per Domain"""
    def dispatch(self, request, *args, **kwargs):
        if 'hris_admin' not in request.session:
            return redirect('admin_login')
        return super().dispatch(request, *args, **kwargs)
    
    def get(self, req):
        start_date = req.GET.get('start_date')
        end_date = req.GET.get('end_date')
        selected_sites = req.GET.get('selected_sites', '')
        selected_account = req.GET.get('selected_account', '')
        try:
            # Ambil user_id dari session
            user_id = req.session.get('hris_admin', {}).get('user_id')
            # Ambil email user dari database berdasarkan user_id
            user_data = data_mysql().get_user_by_id(user_id)
            user_mail = user_data['data']['user_mail']
            # Format tanggal untuk AdManager API
            start_date_formatted = datetime.strptime(start_date, '%Y-%m-%d').strftime('%Y-%m-%d')
            end_date_formatted = datetime.strptime(end_date, '%Y-%m-%d').strftime('%Y-%m-%d')
            # Ambil data AdX (klik, ctr, cpc, eCPM, pendapatan)
            adx_result = fetch_adx_traffic_account_by_user(user_mail, start_date_formatted, end_date_formatted, selected_sites)
            # Ambil data Facebook (spend)
            if selected_account:
                rs_account = data_mysql().master_account_ads_by_params({
                    'data_account': selected_account,
                })['data']
            else:
                 rs_account = data_mysql().master_account_ads()['data']

            facebook_data = fetch_data_insights_by_date_subdomain_roi(
                rs_account, start_date_formatted, end_date_formatted, selected_sites
            )
            print(f"facebook_data: {facebook_data}")    
            # Gabungkan data Facebook dan AdX
            combined_data = []
            total_spend = 0
            total_revenue = 0
            total_clicks = 0
            total_other_costs = 0
            # Buat mapping data Facebook berdasarkan tanggal dan subdomain
            facebook_map = {}
            if facebook_data and facebook_data.get('data'):
                for fb_item in facebook_data['data']:
                    date_key = fb_item.get('date', '')
                    subdomain = fb_item.get('subdomain', '')
                    base_subdomain = extract_base_subdomain(subdomain)
                    key = f"{date_key}_{base_subdomain}"
                    facebook_map[key] = fb_item
            # Proses data AdX dan gabungkan dengan data Facebook
            if adx_result and adx_result.get('status') and adx_result.get('data'):
                for adx_item in adx_result['data']:
                    date_key = adx_item.get('date', '')
                    subdomain = adx_item.get('site_name', '')
                    base_subdomain = extract_base_subdomain(subdomain)
                    key = f"{date_key}_{base_subdomain}"
                    # Cari data Facebook yang sesuai
                    fb_data = facebook_map.get(key)
                    # Jika tidak ditemukan dengan key langsung, coba cari berdasarkan subdomain parsial
                    if not fb_data:
                        base_subdomain_alt = subdomain.replace('.com', '') if subdomain.endswith('.com') else subdomain
                        for fb_key, fb_item in facebook_map.items():
                            if date_key in fb_key and base_subdomain_alt in fb_key:
                                fb_data = fb_item
                                break
                    # Hitung spend dan biaya lainnya
                    spend = float((fb_data or {}).get('spend', 0))
                    other_costs = float((fb_data or {}).get('other_costs', 0))
                    clicks_fb = int((fb_data or {}).get('clicks', 0))
                    revenue = float(adx_item.get('revenue', 0))
                    if clicks_fb > 0:
                        clicks = clicks_fb
                    else:
                        clicks = int(adx_item.get('clicks', 0))
                    # Data berhasil dicocokkan dan dihitung
                    # Hitung ROI nett: (Revenue - Spend) / Spend * 100
                    roi = ((revenue - spend) / (spend)) * 100 if spend > 0 else 0
                    combined_item = {
                        'date': date_key,
                        'site_name': subdomain,  # Gunakan site_name untuk konsistensi dengan JavaScript
                        'spend': spend,
                        'clicks': clicks,
                        'ctr': float(adx_item.get('ctr', 0)),
                        'cpc': float(adx_item.get('cpc', 0)),
                        'ecpm': float(adx_item.get('ecpm', 0)),
                        'other_costs': other_costs,
                        'revenue': revenue,
                        'roi': roi
                    }
                    # Data berhasil digabungkan
                    combined_data.append(combined_item)
                    # Update totals
                    total_spend += spend
                    total_revenue += revenue
                    total_clicks += clicks
                    total_other_costs += other_costs
            # Hitung summary ROI nett
            total_costs_summary = total_spend + total_other_costs
            roi_nett_summary = 0
            if total_costs_summary > 0:
                roi_nett_summary = ((total_revenue - total_costs_summary) / total_costs_summary) * 100
            result = {
                'status': True,
                'data': combined_data,
                'summary': {
                    'total_clicks': total_clicks,
                    'total_spend': total_spend,
                    'roi_nett': roi_nett_summary,
                    'total_revenue': total_revenue
                }
            }
            return JsonResponse(result, safe=False)

        except Exception as e:
            return JsonResponse({
                'status': False,
                'error': str(e)
            })

def extract_base_subdomain(full_string):
    parts = full_string.split('.')
    if len(parts) >= 2:
        return '.'.join(parts[:2])
    return full_string  # fallback kalau tidak sesuai format

class RoiSummaryView(View):
    """View untuk ROI Summary - menampilkan ringkasan data ROI"""
    def dispatch(self, request, *args, **kwargs):
        if 'hris_admin' not in request.session:
            return redirect('admin_login')
        return super().dispatch(request, *args, **kwargs)
    
    def get(self, req):
        data_account = data_mysql().master_account_ads()['data']
        data = {
            'title': 'ROI Summary Dashboard',
            'user': req.session['hris_admin'],
        }
        return render(req, 'admin/report_roi/all_rekap/index.html', {'data_account': data_account, 'data': data})

class RoiSummaryAdChangeDataView(View):
    """AJAX endpoint untuk data Ad Change di AdX Summary"""
    def dispatch(self, request, *args, **kwargs):
        if 'hris_admin' not in request.session:
            return redirect('admin_login')
        return super().dispatch(request, *args, **kwargs)
    
    def get(self, req):
        start_date = req.GET.get('start_date')
        end_date = req.GET.get('end_date')
        site_filter = req.GET.get('site_filter', '')
        
        if not start_date or not end_date:
            return JsonResponse({
                'status': False,
                'error': 'Start date and end date are required'
            })
        
        try:
            from .utils import fetch_roi_ad_change_data
            result = fetch_roi_ad_change_data(start_date, end_date)
            
            # Apply site filter if provided
            if site_filter and site_filter != '%' and result.get('status'):
                filtered_data = []
                for item in result['data']:
                    if site_filter.lower() in item['ad_unit'].lower():
                        filtered_data.append(item)
                
                # Recalculate summary for filtered data
                if filtered_data:
                    total_impressions = sum(item['impressions'] for item in filtered_data)
                    total_clicks = sum(item['clicks'] for item in filtered_data)
                    total_revenue = sum(item['revenue'] for item in filtered_data)
                    total_cpc_revenue = sum(item['cpc_revenue'] for item in filtered_data)
                    total_cpm_revenue = sum(item['cpm_revenue'] for item in filtered_data)
                    
                    avg_ctr = (total_clicks / total_impressions * 100) if total_impressions > 0 else 0
                    avg_ecpm = (total_revenue / total_impressions * 1000) if total_impressions > 0 else 0
                    avg_cpc = (total_cpc_revenue / total_clicks) if total_clicks > 0 else 0
                    
                    result['data'] = filtered_data
                    result['summary'] = {
                        'total_impressions': total_impressions,
                        'total_clicks': total_clicks,
                        'total_revenue': total_revenue,
                        'total_cpc_revenue': total_cpc_revenue,
                        'total_cpm_revenue': total_cpm_revenue,
                        'avg_ctr': avg_ctr,
                        'avg_ecpm': avg_ecpm,
                        'avg_cpc': avg_cpc
                    }
                else:
                    result['data'] = []
                    result['summary'] = {
                        'total_impressions': 0,
                        'total_clicks': 0,
                        'total_revenue': 0,
                        'total_cpc_revenue': 0,
                        'total_cpm_revenue': 0,
                        'avg_ctr': 0,
                        'avg_ecpm': 0,
                        'avg_cpc': 0
                    }
            return JsonResponse(result)
            
        except Exception as e:
            return JsonResponse({
                'status': False,
                'error': str(e)
            })

class RoiActiveSitesView(View):
    """AJAX endpoint untuk mendapatkan daftar situs aktif"""
    def dispatch(self, request, *args, **kwargs):
        if 'hris_admin' not in request.session:
            return redirect('admin_login')
        return super().dispatch(request, *args, **kwargs)
    
    def get(self, req):
        try:
            from .utils import fetch_roi_active_sites
            result = fetch_roi_active_sites()
            return JsonResponse(result)
            
        except Exception as e:
            return JsonResponse({
                'status': False,
                'error': str(e)
            })
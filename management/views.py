from collections import defaultdict
import os
import pprint
from django.shortcuts import render, redirect
from django.views import View
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
from management.utils import get_last_5_days_indonesia_format
from geopy.geocoders import Nominatim
import uuid
from .utils import fetch_data_all_insights_data_all, fetch_data_all_insights_total_all, fetch_data_insights_account_range_all, fetch_data_all_insights, fetch_data_all_insights_total, fetch_data_insights_account_range, fetch_data_insights_account, fetch_data_insights_account_filter_all, fetch_daily_budget_per_campaign, fetch_status_per_campaign, fetch_data_insights_campaign_filter_sub_domain, fetch_data_insights_campaign_filter_account, fetch_data_insights_by_country_filter_campaign, fetch_data_insights_by_country_filter_account, fetch_ad_manager_reports, fetch_ad_manager_inventory, fetch_adx_summary_data, fetch_adx_account_data, fetch_adx_traffic_per_account, fetch_adx_traffic_per_campaign, fetch_adx_traffic_per_country, fetch_data_insights_all_accounts_by_subdomain


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
    # Cek apakah user sudah login via OAuth dan session sudah di-set
    if request.user.is_authenticated and 'hris_admin' in request.session:
        print(f"[DEBUG] User {request.user.email} authenticated with session, redirecting to dashboard")
        return redirect('dashboard_admin')  # 🚀 arahkan ke dashboard
    elif request.user.is_authenticated:
        # Jika user authenticated tapi session belum ada, logout dan arahkan ke login manual
        print(f"[DEBUG] User {request.user.email} authenticated but no session, logging out")
        from django.contrib.auth import logout
        logout(request)
        # Arahkan ke login manual untuk menghindari loop redirect
        return redirect('admin_login')
    else:
        # User tidak authenticated, arahkan ke login manual
        print(f"[DEBUG] User not authenticated, redirecting to manual login")
        return redirect('admin_login')

# Create your views here.
class LoginAdmin(View):
    def dispatch(self, request, *args, **kwargs):
        if 'hris_admin' in request.session:
            return redirect('dashboard_admin')
        return super(LoginAdmin, self).dispatch(request, *args, **kwargs)
    def get(self, req):
        return render(req, 'admin/login_admin.html')

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
                    'user_alias': rs_data['data']['user_alias']
                }
                req.session['hris_admin'] = user_data
                hasil = {
                    'status': True,
                    'data': "Login Berhasil",
                    'message': "Selamat Datang " + rs_data['data']['user_alias'] + " !",
                }
        return JsonResponse(hasil)

class LogoutAdmin(View):
    def get(self, req):
        data_update = {
            'logout_date': datetime.now().strftime('%y-%m-%d %H:%M:%S'),
            'login_id': req.session['hris_admin']['login_id']
        }
        data_mysql().update_login(data_update)
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
            'user': req.session['hris_admin'],
            # 'last_5_days': get_last_5_days_indonesia_format()
        }
        return render(req, 'admin/dashboard_admin.html', data)

class DashboardData(View):
    """API endpoint untuk data dashboard dengan statistik user dan login"""
    def dispatch(self, request, *args, **kwargs):
        if 'hris_admin' not in request.session:
            return redirect('admin_login')
        return super().dispatch(request, *args, **kwargs)
    
    def get(self, req):
        try:
            from datetime import datetime, timedelta
            
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
        
        # Update data
        data_update = {
            'account_ads_id': account_ads_id,
            'account_name': account_name,
            'account_email': account_email,
            'account_id': account_id,
            'app_id': app_id,
            'app_secret': app_secret,
            'access_token': access_token,
            'mub': req.session['hris_admin']['id'],
            'mub_name': req.session['hris_admin']['name'],
            'mud': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
        
        rs_update = data_mysql().update_account_ads(data_update)
        hasil = rs_update['hasil']
        
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
        rs_account = data_mysql().master_account_ads()
        if data_sub_domain != '%' and data_account != '%':
            rs_data_account = data_mysql().master_account_ads_by_id({
                'data_account': data_account,
            })['data']
            data = fetch_data_insights_by_country_filter_account(str(rs_data_account['access_token']), str(rs_data_account['account_id']), str(tanggal_dari), str(tanggal_sampai), str(data_sub_domain))
        else: 
            data = fetch_data_insights_by_country_filter_campaign(rs_account['data'], str(tanggal_dari), str(tanggal_sampai), str(data_sub_domain)) 
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
            # Format tanggal untuk AdManager API
            start_date_formatted = datetime.strptime(start_date, '%Y-%m-%d').strftime('%Y-%m-%d')
            end_date_formatted = datetime.strptime(end_date, '%Y-%m-%d').strftime('%Y-%m-%d')
            
            result = fetch_adx_summary_data(start_date_formatted, end_date_formatted)
            return JsonResponse(result)
            
        except Exception as e:
            return JsonResponse({
                'status': False,
                'error': str(e)
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
        account_filter = req.GET.get('account_filter', '')
        
        if not start_date or not end_date:
            return JsonResponse({
                'status': False,
                'error': 'Start date and end date are required'
            })
        
        try:
            # Format tanggal untuk AdManager API
            start_date_formatted = datetime.strptime(start_date, '%Y-%m-%d').strftime('%Y-%m-%d')
            end_date_formatted = datetime.strptime(end_date, '%Y-%m-%d').strftime('%Y-%m-%d')
            
            # Filter account jika kosong atau '%'
            filter_value = account_filter if account_filter and account_filter != '%' else None
            
            result = fetch_adx_traffic_per_account(start_date_formatted, end_date_formatted, filter_value)
            return JsonResponse(result)
            
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
        campaign_filter = req.GET.get('campaign_filter', '')
        
        if not start_date or not end_date:
            return JsonResponse({
                'status': False,
                'error': 'Start date and end date are required'
            })
        
        try:
            # Format tanggal untuk AdManager API
            start_date_formatted = datetime.strptime(start_date, '%Y-%m-%d').strftime('%Y-%m-%d')
            end_date_formatted = datetime.strptime(end_date, '%Y-%m-%d').strftime('%Y-%m-%d')
            
            # Filter campaign jika kosong atau '%'
            filter_value = campaign_filter if campaign_filter and campaign_filter != '%' else None
            
            result = fetch_adx_traffic_per_campaign(start_date_formatted, end_date_formatted, filter_value)
            return JsonResponse(result)
            
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
        country_filter = req.GET.get('country_filter', '')
        
        if not start_date or not end_date:
            return JsonResponse({
                'status': False,
                'error': 'Start date and end date are required'
            })
        
        try:
            # Format tanggal untuk AdManager API
            start_date_formatted = datetime.strptime(start_date, '%Y-%m-%d').strftime('%Y-%m-%d')
            end_date_formatted = datetime.strptime(end_date, '%Y-%m-%d').strftime('%Y-%m-%d')
            
            # Filter country jika kosong atau '%'
            filter_value = country_filter if country_filter and country_filter != '%' else None
            
            result = fetch_adx_traffic_per_country(start_date_formatted, end_date_formatted, filter_value)
            return JsonResponse(result)
            
        except Exception as e:
            return JsonResponse({
                'status': False,
                'error': str(e)
            })
        
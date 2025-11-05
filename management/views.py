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
from django.utils.http import url_has_allowed_host_and_scheme
from django.conf import settings
from django import template
from calendar import month, monthrange
from datetime import datetime, date, timedelta
from django.http import HttpResponse, JsonResponse, QueryDict
from .database import data_mysql
from itertools import groupby, product
from django.core import serializers
from operator import itemgetter
import tempfile
from django.core.files.storage import FileSystemStorage
from django.template.loader import render_to_string
from django.contrib import messages
from hris.mail import send_mail
from argon2 import PasswordHasher
import random
# Pandas can fail to import if numpy binary is incompatible; avoid crashing at module load
try:
    import pandas as pd
except Exception as _pandas_err:
    pd = None

try:
    import logging as _logging
    _logging.getLogger(__name__).warning("Pandas import failed; disabling pandas-dependent features: %s", _pandas_err)
except Exception:
    pass
import io
from .crypto import sandi
import requests
import json
import uuid
# Optional dependencies: guard imports to prevent module-level crashes
try:
    from geopy.geocoders import Nominatim
except Exception:
    Nominatim = None
try:
    import pycountry
except Exception:
    pycountry = None
from google_auth_oauthlib.flow import Flow
import urllib.parse
import uuid
from .oauth_utils import (
    generate_oauth_url_for_user, 
    exchange_code_for_refresh_token,
    handle_oauth_callback
)
import logging

logger = logging.getLogger(__name__)
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from googleads import ad_manager
from google_auth_oauthlib.flow import Flow
import os
from django.urls import reverse
from django.shortcuts import redirect
from .utils import fetch_data_all_insights_data_all, fetch_data_all_insights_total_all, fetch_data_insights_account_range_all, fetch_data_all_insights, fetch_data_all_insights_total, fetch_data_insights_account_range, fetch_data_insights_account, fetch_data_insights_account_filter_all, fetch_daily_budget_per_campaign, fetch_status_per_campaign, fetch_data_insights_campaign_filter_sub_domain, fetch_data_insights_campaign_filter_account, fetch_data_country_facebook_ads, fetch_data_insights_by_country_filter_campaign, fetch_data_insights_by_country_filter_account, fetch_user_sites_list, fetch_ad_manager_reports, fetch_ad_manager_inventory, fetch_adx_summary_data, fetch_adx_traffic_account_by_user, fetch_user_adx_account_data, fetch_adx_account_data, fetch_data_insights_all_accounts_by_subdomain, fetch_adx_traffic_per_country, fetch_roi_per_country, fetch_data_insights_by_country_filter_campaign_roi, fetch_data_insights_by_date_subdomain_roi

# Global default for active portal ID to avoid hard-coding across views
DEFAULT_ACTIVE_PORTAL_ID = '30'

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


# Initialize geocoder if available; else use None and handle in callers
geocode = Nominatim(user_agent="hris_trendHorizone") if Nominatim else None

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
            # Redirect to role default page if available
            try:
                admin = request.session.get('hris_admin', {})
                user_id = admin.get('user_id')
                if user_id:
                    db = data_mysql()
                    # Portal-aware default page selection (defaults to portal 12)
                    q = '''
                        SELECT DISTINCT r.default_page
                        FROM app_user_role ur
                        JOIN app_menu_role mr ON mr.role_id = ur.role_id
                        JOIN app_menu m ON m.menu_id = mr.menu_id
                        JOIN app_role r ON r.role_id = ur.role_id
                        WHERE ur.user_id = %s AND m.portal_id = %s
                        ORDER BY CASE WHEN ur.role_default = '1' THEN 0 ELSE 1 END
                        LIMIT 1
                    '''
                    default_page = None
                    portal_id = request.session.get('active_portal_id', DEFAULT_ACTIVE_PORTAL_ID)
                    if db.execute_query(q, (user_id, portal_id)):
                        row = db.cur_hris.fetchone() or {}
                        default_page = row.get('default_page')
                    target = default_page or 'management/admin/dashboard'
                    if not target.startswith('/'):
                        target = '/' + target
                    return redirect(target)
            except Exception:
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
        # Set default active portal on first login
        try:
            request.session['active_portal_id'] = DEFAULT_ACTIVE_PORTAL_ID
        except Exception:
            pass
        # Redirect to role default page if available
        try:
            db = data_mysql()
            # Portal-aware default page selection using active portal (defaults to 12)
            q = '''
                SELECT DISTINCT r.default_page
                FROM app_user_role ur
                JOIN app_menu_role mr ON mr.role_id = ur.role_id
                JOIN app_menu m ON m.menu_id = mr.menu_id
                JOIN app_role r ON r.role_id = ur.role_id
                WHERE ur.user_id = %s AND m.portal_id = %s
                ORDER BY CASE WHEN ur.role_default = '1' THEN 0 ELSE 1 END
                LIMIT 1
            '''
            default_page = None
            uid = user_data['data'][0]['user_id']
            portal_id = request.session.get('active_portal_id', DEFAULT_ACTIVE_PORTAL_ID)
            if db.execute_query(q, (uid, portal_id)):
                row = db.cur_hris.fetchone() or {}
                default_page = row.get('default_page')
            target = default_page or 'management/admin/dashboard'
            if not target.startswith('/'):
                target = '/' + target
            return redirect(target)
        except Exception:
            return redirect('dashboard_admin')

class SettingsOverview(View):
    def get(self, request):
        # Require authenticated admin session
        if 'hris_admin' not in request.session:
            return redirect('admin_login')
        admin = request.session.get('hris_admin', {})
        active_portal_id = request.session.get('active_portal_id', DEFAULT_ACTIVE_PORTAL_ID)
        context = {
            'user': admin,
            'active_portal_id': active_portal_id,
        }
        return render(request, 'admin/settings_overview.html', context)

class LoginProcess(View):
    def post(self, req):
        # Normalize input: trim whitespace to avoid accidental mismatch
        raw_username = req.POST.get('username')
        raw_password = req.POST.get('password')
        username = (raw_username or '').strip()
        password = (raw_password or '').strip()
        try:
            print(f"[LOGIN_DEBUG] Incoming POST username={username!r} has_password={(bool(password))} sessionid={req.session.session_key}")
        except Exception:
            pass
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
            try:
                print(f"[LOGIN_DEBUG] DB helper returned status={rs_data.get('status')} has_data={rs_data.get('data') is not None}")
            except Exception:
                pass
            # Check if login was successful and data exists
            if not rs_data.get('status', False) or rs_data.get('data') is None:
                # Handle both error cases and no user found cases
                error_message = rs_data.get('message', 'Username dan Password tidak ditemukan !')
                try:
                    print(f"[LOGIN_DEBUG] Login failed. message={error_message}")
                except Exception:
                    pass
                hasil = {
                    'status': False,
                    'data': error_message,
                    'message': "Silahkan cek kembali username dan password anda."
                }
            else:
                # Get IP and location with robust fallbacks
                lat_long = [None, None]
                ip_address = req.META.get('REMOTE_ADDR', '')
                location_address = None
                try:
                    resp = requests.get("https://ipinfo.io/json", timeout=5)
                    if resp.ok:
                        data_ipinfo = resp.json()
                        if isinstance(data_ipinfo, dict):
                            loc_val = data_ipinfo.get("loc")
                            if loc_val:
                                tmp = loc_val.split(",")
                                if len(tmp) == 2:
                                    lat_long = [tmp[0], tmp[1]]
                            ip_address = data_ipinfo.get('ip', ip_address) or ip_address
                except Exception:
                    pass
                try:
                    if not ip_address or ip_address in ("127.0.0.1", "::1"):
                        ip_resp = requests.get("https://api.ipify.org", timeout=5)
                        if ip_resp.status_code == 200 and ip_resp.text:
                            ip_address = ip_resp.text.strip()
                except Exception:
                    pass
                try:
                    if geocode and lat_long[0] and lat_long[1]:
                        lat_f = float(lat_long[0])
                        lon_f = float(lat_long[1])
                        loc = geocode.reverse((lat_f, lon_f), language='id')
                        location_address = loc.address if hasattr(loc, 'address') else None
                except Exception:
                    location_address = None
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
                    'lokasi': location_address,
                    'mdb': rs_data['data']['user_id']
                }
                data_login = data_mysql().insert_login(data_insert)
                login_id = data_login['hasil']['login_id']
                user_data = {
                    'login_id': login_id,
                    'user_id': rs_data['data']['user_id'],
                    'user_name': rs_data['data']['user_name'],
                    'user_pass': '',
                    'user_alias': rs_data['data']['user_alias'],
                    'user_mail': rs_data['data']['user_mail']  # Tambahkan user_mail ke session
                }
                req.session['hris_admin'] = user_data
                try:
                    print(f"[LOGIN_DEBUG] Session set for user_id={user_data['user_id']} login_id={login_id}")
                except Exception:
                    pass
                # Set default active portal on first login
                try:
                    req.session['active_portal_id'] = DEFAULT_ACTIVE_PORTAL_ID
                except Exception:
                    pass
                hasil = {
                    'status': True,
                    'data': "Login Berhasil",
                    'message': "Selamat Datang " + rs_data['data']['user_alias'] + " !",
                }
                try:
                    print(f"[LOGIN_DEBUG] Login success for username={username}")
                except Exception:
                    pass
        return JsonResponse(hasil)

    
@csrf_exempt
def get_countries_facebook_ads(request):
    """Endpoint untuk mendapatkan daftar negara yang tersedia"""
    if 'hris_admin' not in request.session:
        return redirect('admin_login')
    try:
        tanggal_dari = request.POST.get('tanggal_dari')
        tanggal_sampai = request.POST.get('tanggal_sampai')
        data_account = request.POST.get('data_account')
        data_sub_domain = request.POST.get('data_sub_domain')
        rs_data_account = data_mysql().master_account_ads_by_id({
            'data_account': data_account,
        })['data']
        # Ambil semua data negara tanpa filter
        result = fetch_data_country_facebook_ads(
            tanggal_dari, 
            tanggal_sampai,
            str(rs_data_account['access_token']), 
            str(rs_data_account['account_id']),
            data_sub_domain
        )
        print(f"Data Negara : {result}")
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
        selected_accounts = request.GET.get('selected_accounts')
        if selected_accounts:
            user_mail = selected_accounts
        else:
            user_mail = request.session.get('hris_admin', {}).get('user_mail')
        # Ambil data negara dari AdX untuk periode 30 hari terakhir
        end_date = datetime.now().date()
        start_date = end_date - timedelta(days=7)
        result = fetch_adx_traffic_per_country(
            start_date.strftime('%Y-%m-%d'), 
            end_date.strftime('%Y-%m-%d'),
            user_mail
        )
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

class ForgotPasswordView(View):
    def get(self, req):
        data = {
            'title': 'Forgot Password'
        }
        return render(req, 'admin/forgot_password.html', data)

    def post(self, req):
        username = req.POST.get('username', '').strip()
        email = req.POST.get('email', '').strip()

        if not username or not email:
            messages.error(req, 'Username and email are required.')
            return redirect('forgot_password')

        try:
            db = data_mysql()
            sql = """
                SELECT user_id, user_mail
                FROM app_users
                WHERE user_name = %s AND user_mail = %s
                LIMIT 1
            """
            has_row = db.execute_query(sql, (username, email))
            row = db.cur_hris.fetchone() if has_row else None

            if not row:
                messages.error(req, 'Record not found. Please check your username and email.')
                return redirect('forgot_password')

            # Generate new 6-digit password
            new_password_plain = f"{random.randint(0, 999999):06d}"

            # Hash using Argon2
            ph = PasswordHasher()
            new_hash = ph.hash(new_password_plain)

            # Update password in database
            update_sql = "UPDATE app_users SET user_pass = %s WHERE user_id = %s"
            user_id = row.get('user_id') if isinstance(row, dict) else (row[0] if row else None)
            db.execute_query(update_sql, (new_hash, user_id))
            db.commit()

            # Send email with the new password
            subject = 'Reset Password HRIS'
            context = {
                'subject': subject,
                'body': f'<p>Your new password is: <b>{new_password_plain}</b>. Please log in and change it immediately.</p>',
                'brand_name': getattr(settings, 'BRAND_NAME', 'Trend Horizone')
            }
            try:
                send_mail(to=[email], subject=subject, template='emails/simple.html', context=context)
            except Exception as e:
                print(f"[ERROR] Failed to send reset password email: {e}")
                messages.warning(req, 'Password has been reset, but email failed to send.')
                return redirect('admin_login')

            messages.success(req, 'A new password has been sent to your email.')
            return redirect('admin_login')

        except Exception as e:
            print(f"[ERROR] ForgotPasswordView: {e}")
            import traceback
            print(traceback.format_exc())
            messages.error(req, 'An error occurred while processing your request.')
            return redirect('forgot_password')

# DASHBOARD
class DashboardAdmin(View):
    def dispatch(self, request, *args, **kwargs):
        if 'hris_admin' not in request.session:
            return redirect('admin_login')
        return super(DashboardAdmin, self).dispatch(request, *args, **kwargs)

    def get(self, req):
        # Cek status OAuth user untuk menampilkan banner otorisasi jika token belum ada
        try:
            user_mail = req.session.get('hris_admin', {}).get('user_mail')
        except Exception:
            user_mail = None

        oauth_banner = {
            'show': False,
            'message': None
        }

        if user_mail:
            try:
                from management.oauth_utils import get_user_oauth_status
                status = get_user_oauth_status(user_mail)
                if status.get('status') and not status.get('data', {}).get('has_token', False):
                    oauth_banner['show'] = True
                    oauth_banner['message'] = (
                        'Akses Ad Manager belum diotorisasi untuk akun ini. '
                        'Silakan selesaikan otorisasi agar fitur AdX berfungsi.'
                    )
            except Exception:
                # Jika gagal cek status, jangan blokir dashboard; sembunyikan banner
                pass

        data = {
            'title': 'Dashboard Admin',
            'user': req.session['hris_admin'],
            'oauth_banner': oauth_banner
        }
        return render(req, 'admin/dashboard_admin.html', data)

class SwitchPortal(View):
    def dispatch(self, request, *args, **kwargs):
        if 'hris_admin' not in request.session:
            return redirect('admin_login')
        return super(SwitchPortal, self).dispatch(request, *args, **kwargs)

    def get(self, req, portal_id):
        # Validate user has access to the portal
        try:
            user_id = req.session.get('hris_admin', {}).get('user_id')
            db = data_mysql()
            sql = '''
                SELECT DISTINCT p.portal_id
                FROM app_user_role ur
                JOIN app_menu_role rm ON rm.role_id = ur.role_id
                JOIN app_menu m ON m.nav_id = rm.nav_id
                JOIN app_portal p ON p.portal_id = m.portal_id
                WHERE ur.user_id = %s AND p.portal_id = %s AND m.display_st = '1' AND m.active_st = '1'
                LIMIT 1
            '''
            has_access = False
            if db.execute_query(sql, (user_id, portal_id)):
                row = db.cur_hris.fetchone()
                has_access = bool(row)
            if has_access:
                req.session['active_portal_id'] = portal_id
                # Redirect to role default_page related to this portal
                try:
                    q_default = '''
                        SELECT r.default_page
                        FROM app_user_role ur
                        JOIN app_menu_role rm ON rm.role_id = ur.role_id
                        JOIN app_menu m ON m.nav_id = rm.nav_id
                        JOIN app_role r ON r.role_id = ur.role_id
                        WHERE ur.user_id = %s AND m.portal_id = %s
                        ORDER BY CASE WHEN ur.role_default = '1' THEN 0 ELSE 1 END
                        LIMIT 1
                    '''
                    default_page = None
                    if db.execute_query(q_default, (user_id, portal_id)):
                        rrow = db.cur_hris.fetchone() or {}
                        default_page = rrow.get('default_page')
                    if default_page:
                        target = default_page
                        if not target.startswith('/'):
                            target = '/' + target
                        return redirect(target)
                except Exception:
                    pass
        except Exception:
            pass
        # Fallback: dashboard if default_page not found
        return redirect('dashboard_admin')

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

from django.views.decorators.csrf import csrf_exempt
from django.utils.decorators import method_decorator

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
    def dispatch(self, request, *args, **kwargs):
        if 'hris_admin' not in request.session:
            return redirect('admin_login')
        return super(post_account_ads, self).dispatch(request, *args, **kwargs)
        
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
        tanggal = req.GET.get('tanggal')
        data_account = req.GET.get('data_account')
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
            data = fetch_data_insights_all_accounts_by_subdomain(str(tanggal), rs_account, str(data_sub_domain))
        else:
            # Gunakan account spesifik seperti sebelumnya
            rs_data_account = data_mysql().master_account_ads_by_id({
                'data_account': data_account,
            })['data']
            data = fetch_data_insights_account(str(tanggal), str(rs_data_account['access_token']), str(rs_data_account['account_id']), str(data_sub_domain), str(rs_data_account['account_name']))
        
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
        data_account = req.GET.get('data_account')
        data_sub_domain = req.GET.get('data_sub_domain')
        rs_account = data_mysql().master_account_ads()
        if (data_sub_domain != '%' or data_sub_domain == '%') and data_account != '%':
            rs_data_account = data_mysql().master_account_ads_by_id({
                'data_account': data_account,
            })['data']
            data = fetch_data_insights_campaign_filter_account(str(tanggal_dari), str(tanggal_sampai), str(rs_data_account['access_token']), str(rs_data_account['account_id']), str(rs_data_account['account_name']), str(data_sub_domain))
        else:  
            data = fetch_data_insights_campaign_filter_sub_domain(str(tanggal_dari), str(tanggal_sampai), rs_account['data'], str(data_sub_domain))
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
        data_account = req.GET.get('data_account')
        data_sub_domain = req.GET.get('data_sub_domain')
        # Ambil parameter countries dari query string
        countries_param = req.GET.get('countries', '')
        selected_countries = []
        if countries_param:
            selected_countries = countries_param.split(',')
        rs_data_account = data_mysql().master_account_ads_by_id({
            'data_account': data_account,
        })['data']
        data = fetch_data_insights_by_country_filter_account(str(tanggal_dari), str(tanggal_sampai), str(rs_data_account['access_token']), str(rs_data_account['account_id']), str(data_sub_domain))
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
                'total_frequency': total_frequency,
                'total_cpr': total_cpr
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
            data = fetch_data_insights_by_country_filter_account(str(tanggal_dari), str(tanggal_sampai), str(rs_data_account['access_token']), str(rs_data_account['account_id']), str(data_sub_domain))
        else: 
            data = fetch_data_insights_by_country_filter_campaign(str(tanggal_dari), str(tanggal_sampai), rs_account['data'], str(data_sub_domain)) 
        
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
        data_account_adx = data_mysql().get_all_adx_account_data()
        print(f"DEBUG - Raw AdX Account Data: {data_account_adx}")
        if not data_account_adx['status']:
            return JsonResponse({
                'status': False,
                'error': data_account_adx['data']
            })
        data = {
            'title': 'AdX Summary Dashboard',
            'user': req.session['hris_admin'],
            'data_account_adx': data_account_adx['data'],
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
        selected_accounts = req.GET.get('selected_accounts')
        selected_sites = req.GET.get('selected_sites')
        if selected_accounts:
            user_mail = selected_accounts.split(',')
        else:
            user_mail = req.session.get('hris_admin', {}).get('user_mail')
        if not start_date or not end_date:      
            return JsonResponse({
                'status': False,
                'error': 'Start date and end date are required'
            })
        try:
            result = fetch_adx_traffic_account_by_user(user_mail, start_date, end_date, selected_sites)
            # Tambahkan data traffic hari ini
            today = datetime.now().strftime('%Y-%m-%d')
            today_result = fetch_adx_traffic_account_by_user(user_mail, today, today, selected_sites)
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
        # Siapkan banner otorisasi OAuth jika token belum ada
        try:
            user_mail = req.session.get('hris_admin', {}).get('user_mail')
        except Exception:
            user_mail = None
        oauth_banner = {
            'show': False,
            'message': None
        }
        if user_mail:
            try:
                from management.oauth_utils import get_user_oauth_status
                status = get_user_oauth_status(user_mail)
                if status.get('status') and not status.get('data', {}).get('has_token', False):
                    oauth_banner['show'] = True
                    oauth_banner['message'] = (
                        'Akses Ad Manager belum diotorisasi untuk akun ini. '
                        'Silakan selesaikan otorisasi agar fitur AdX berfungsi.'
                    )
            except Exception:
                # Jika gagal cek status, sembunyikan banner agar halaman tetap tampil
                pass
        data_account_adx = data_mysql().get_all_adx_account_data()
        if not data_account_adx['status']:
            return JsonResponse({
                'status': False,
                'error': data_account_adx['data']
            })
        # Ambil data dari tabel app_credentials
        db = data_mysql()
        result = db.get_all_app_credentials()
        if result.get('status'):
            credentials_data = result.get('data', [])
        else:
            credentials_data = []
        # Tampilkan pesan sukses jika baru selesai OAuth
        oauth_success_msg = None
        if req.session.get('oauth_added_success'):
            oauth_success_msg = req.session.get('oauth_added_message', 'Kredensial berhasil ditambahkan.')
            # Hapus pesan setelah ditampilkan sekali
            try:
                del req.session['oauth_added_success']
                if 'oauth_added_message' in req.session:
                    del req.session['oauth_added_message']
            except Exception:
                pass
        data = {
            'title': 'AdX Account Data',
            'user': req.session['hris_admin'],
            'data_account_adx': data_account_adx['data'],
            'credentials_data': credentials_data,
            'total_accounts': len(credentials_data),
            'oauth_banner': oauth_banner,
            'oauth_success_msg': oauth_success_msg,
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
        # Prioritas: gunakan user_email dari parameter frontend jika ada
        user_email = req.GET.get('user_email')
        if user_email:
            # Gunakan email dari parameter frontend (dari Load Data button)
            user_mail = user_email
        else:
            # Fallback ke logika lama jika tidak ada parameter user_email
            selected_accounts = req.GET.get('selected_accounts')
            if selected_accounts:
                user_mail = selected_accounts.split(',')
            else:
                user_mail = req.session.get('hris_admin', {}).get('user_mail')
        try:
            # Fetch comprehensive account data using user's credentials
            result = fetch_user_adx_account_data(user_mail)
            # Enhance error message for better user feedback
            if not result.get('status', False):
                error_msg = result.get('error', 'Unknown error')
                # Provide more user-friendly error messages
                if 'Service MakeSoapRequest not found' in error_msg:
                    result['error'] = 'Terjadi masalah dengan koneksi Google Ad Manager. Silakan coba lagi dalam beberapa saat.'
                elif 'SOAP encoding' in error_msg:
                    result['error'] = 'Terjadi masalah encoding data. Data dasar akan ditampilkan.'
                elif 'credentials' in error_msg.lower():
                    result['error'] = 'Kredensial Google Ad Manager tidak valid atau belum dikonfigurasi.'
                elif 'network' in error_msg.lower():
                    result['error'] = 'Tidak dapat mengakses network Google Ad Manager. Periksa konfigurasi network code.'
            return JsonResponse(result)
        except Exception as e:
            error_msg = str(e)
            # Provide user-friendly error messages
            if 'Service MakeSoapRequest not found' in error_msg:
                error_msg = 'Terjadi masalah dengan koneksi Google Ad Manager. Silakan coba lagi dalam beberapa saat.'
            elif 'SOAP encoding' in error_msg:
                error_msg = 'Terjadi masalah encoding data. Silakan refresh halaman.'
            elif 'credentials' in error_msg.lower():
                error_msg = 'Kredensial Google Ad Manager tidak valid atau belum dikonfigurasi.'
            return JsonResponse({
                'status': False,
                'error': error_msg
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
            print(f"DEBUG Generate Refresh Token - Email data from DB: {user_mail}")
            if not user_mail:
                return JsonResponse({
                    'status': False,
                    'error': 'User Mail tidak ditemukan dalam session'
                })
            # Ambil data user dari database
            db = data_mysql()
            user_data = db.get_user_by_mail(user_mail)
            print(f"DEBUG Generate Refresh Token - User data from DB: {user_data}")
            if not user_data['status'] or not user_data['data']:
                return JsonResponse({
                    'status': False,
                    'error': 'Data user tidak ditemukan dalam database'
                })
            user_info = user_data['data']
            user_mail = user_info['user_mail']
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
        print("[SaveOAuthCredentialsView] POST request received!")
        print(f"  Request method: {req.method}")
        print(f"  Request path: {req.path}")
        print(f"  Session has hris_admin: {'hris_admin' in req.session}")
        print(f"  POST data keys: {list(req.POST.keys())}")
        
        try:
            client_id = req.POST.get('client_id')
            client_secret = req.POST.get('client_secret')
            network_code = req.POST.get('network_code')
            user_mail = req.POST.get('user_mail')
            admin = req.session.get('hris_admin', {})
            
            # Debug logging
            print("[SaveOAuthCredentialsView] Received data:")
            print(f"  client_id: {client_id}")
            print(f"  client_secret: {'(provided)' if client_secret else '(empty)'}")
            print(f"  network_code: {network_code}")
            print(f"  user_mail: {user_mail}")
            print(f"  admin session: {admin}")

            # Validasi input minimal
            if not client_id or not client_secret or not user_mail:
                return JsonResponse({
                    'status': False,
                    'error': 'Client ID, Client Secret, dan Email harus diisi'
                })

            # Siapkan metadata dan developer token
            developer_token = getattr(settings, 'GOOGLE_ADS_DEVELOPER_TOKEN', os.getenv('GOOGLE_ADS_DEVELOPER_TOKEN', ''))
            mdb = admin.get('user_id')
            mdb_name = admin.get('user_alias') or admin.get('user_name')
            account_id = admin.get('user_id')
            account_name = mdb_name or user_mail

            db = data_mysql()

            # Jika baris sudah ada, ambil refresh_token & network_code yang ada agar tidak terhapus
            existing_refresh_token = None
            existing_network_code = None
            try:
                sql = 'SELECT refresh_token, network_code FROM app_credentials WHERE user_mail = %s LIMIT 1'
                if db.execute_query(sql, (user_mail,)):
                    row = db.cur_hris.fetchone()
                    if row:
                        if isinstance(row, dict):
                            existing_refresh_token = row.get('refresh_token')
                            existing_network_code = row.get('network_code')
                        else:
                            existing_refresh_token = row[0]
                            existing_network_code = row[1]
            except Exception:
                pass

            # Tentukan nilai network_code akhir (prioritaskan input terbaru jika ada)
            final_network_code = network_code or existing_network_code

            exists = db.check_app_credentials_exist(user_mail)
            print(f"[SaveOAuthCredentialsView] Database check result: {exists}")
            
            if isinstance(exists, dict) and not exists.get('status', True):
                print(f"[SaveOAuthCredentialsView] Database check failed: {exists}")
                return JsonResponse({
                    'status': False,
                    'error': 'Gagal mengecek app_credentials di database'
                })

            if isinstance(exists, int) and exists > 0:
                print(f"[SaveOAuthCredentialsView] Updating existing credentials for {user_mail}")
                result = db.update_app_credentials(
                    user_mail,
                    account_name,
                    client_id,
                    client_secret,
                    existing_refresh_token,
                    final_network_code,
                    developer_token,
                    mdb,
                    mdb_name,
                    '1'
                )
                print(f"[SaveOAuthCredentialsView] Update result: {result}")
            else:
                print(f"[SaveOAuthCredentialsView] Inserting new credentials for {user_mail}")
                result = db.insert_app_credentials(
                    account_name,
                    user_mail,
                    client_id,
                    client_secret,
                    None,
                    final_network_code,
                    developer_token,
                    mdb,
                    mdb_name
                )
                print(f"[SaveOAuthCredentialsView] Insert result: {result}")

            if isinstance(result, dict) and result.get('status'):
                print(f"[SaveOAuthCredentialsView] SUCCESS: Credentials saved successfully")
                return JsonResponse({
                    'status': True,
                    'message': 'Kredensial berhasil disimpan ke app_credentials',
                    'data': {
                        'user_mail': user_mail,
                        'client_id': client_id[:10] + '...',
                        'updated': True,
                        'network_code': final_network_code
                    }
                })
            else:
                print(f"[SaveOAuthCredentialsView] FAILED: Database operation failed - {result}")
                return JsonResponse({
                    'status': False,
                    'error': (result.get('error') if isinstance(result, dict) else 'Gagal menyimpan app_credentials')
                })

        except Exception as e:
            return JsonResponse({
                'status': False,
                'error': f'Error saat menyimpan app_credentials: {str(e)}'
            })

# OAuth views telah dipindahkan ke oauth_views_package untuk konsistensi
# Gunakan oauth_views_package.oauth_views untuk semua operasi OAuth

class AdxTrafficPerAccountView(View):
    """View untuk AdX Traffic Per Account"""
    def dispatch(self, request, *args, **kwargs):
        if 'hris_admin' not in request.session:
            return redirect('admin_login')
        return super().dispatch(request, *args, **kwargs)
    def get(self, req):
        data_account_adx = data_mysql().get_all_adx_account_data()
        print(f"DEBUG AdxTrafficPerAccountView - data_account_adx: {data_account_adx}")
        if not data_account_adx['status']:
            return JsonResponse({
                'status': False,
                'error': data_account_adx['data']
            })
        data = {
            'title': 'AdX Traffic Per Account',
            'user': req.session['hris_admin'],
            'data_account_adx': data_account_adx['data'],
        }
        return render(req, 'admin/adx_manager/traffic_account/index.html', data)


class AdxAccountOAuthStartView(View):
    """Mulai flow OAuth untuk menambahkan kredensial ke app_credentials berdasarkan email aktif."""
    def dispatch(self, request, *args, **kwargs):
        if 'hris_admin' not in request.session:
            return redirect('admin_login')
        return super().dispatch(request, *args, **kwargs)

    def get(self, req):
        try:
            print("DEBUG: AdxAccountOAuthStartView called")
            logger.info(f"OAuth Start - GET parameters: {dict(req.GET)}")
            current_user = req.session.get('hris_admin', {})
            # Izinkan target email via query (?email=xxx); jika tidak ada, JANGAN paksa fallback ke email session
            target_mail = req.GET.get('email')
            user_id = current_user.get('user_id')
            logger.info(f"OAuth Start - target_mail: {target_mail}, user_id: {user_id}")

            # Ambil konfigurasi dari .env secara eksklusif
            client_id = os.getenv('GOOGLE_OAUTH2_CLIENT_ID')
            client_secret = os.getenv('GOOGLE_OAUTH2_CLIENT_SECRET')
            if not client_id or not client_secret:
                # Jika env tidak tersedia, tampilkan pesan di halaman
                req.session['oauth_added_success'] = False
                req.session['oauth_added_message'] = 'GOOGLE_OAUTH2_CLIENT_ID/SECRET tidak ditemukan di .env.'
                return redirect('adx_account')

            # Gunakan endpoint callback yang telah diseragamkan (tanpa query) agar cocok dengan Authorized Redirect URIs
            redirect_uri = req.build_absolute_uri(reverse('oauth_callback_api'))

            client_config = {
                'web': {
                    'client_id': client_id,
                    'client_secret': client_secret,
                    'auth_uri': 'https://accounts.google.com/o/oauth2/v2/auth',
                    'token_uri': 'https://oauth2.googleapis.com/token',
                    'redirect_uris': [redirect_uri]
                }
            }
            scopes = [
                # Scope dasar untuk identitas user (gunakan expanded form yang dikembalikan Google)
                'openid',
                'https://www.googleapis.com/auth/userinfo.email',
                'https://www.googleapis.com/auth/userinfo.profile',
                # Scope untuk Google Ad Manager
                'https://www.googleapis.com/auth/admanager',
                # Scope untuk Google AdSense
                'https://www.googleapis.com/auth/adsense'
            ]

            flow = Flow.from_client_config(client_config, scopes=scopes)
            flow.redirect_uri = redirect_uri

            # Bangun authorization URL secara manual untuk memastikan nilai parameter tepat (lowercase)
            # Gunakan state minimal untuk menandai flow AdX agar routing callback tepat
            import time
            timestamp = str(int(time.time()))
            state = f'flow:adx:t{timestamp}'  # Tambahkan timestamp untuk memaksa request baru
            params = {
                'client_id': client_id,
                'redirect_uri': redirect_uri,
                'scope': ' '.join(scopes),
                'response_type': 'code',
                'access_type': 'offline',
                'include_granted_scopes': 'false',  # Ubah ke false untuk memaksa scope baru
                'prompt': 'consent select_account',  # Tambahkan select_account untuk memaksa pilih akun
                'state': state,
            }
            # Hanya gunakan login_hint bila admin sengaja memilih email tertentu
            if target_mail:
                params['login_hint'] = target_mail
            authorization_url = 'https://accounts.google.com/o/oauth2/v2/auth?' + urllib.parse.urlencode(params)
            
            logger.info(f"OAuth Start - Generated authorization URL: {authorization_url}")
            logger.info(f"OAuth Start - Redirect URI: {redirect_uri}")

            # Simpan state di session untuk validasi callback
            req.session['oauth_flow_state'] = state
            # Simpan info user untuk callback
            # Simpan email yang dipilih hanya jika memang ada (opsional)
            if target_mail:
                req.session['oauth_flow_user_mail'] = target_mail
            req.session['oauth_flow_user_id'] = user_id
            # Simpan redirect_uri aktual agar token exchange memakai nilai identik
            req.session['oauth_flow_redirect_uri'] = redirect_uri

            # Pre-insert ke app_credentials agar baris tersedia sebelum callback
            # Pre-insert app_credentials hanya jika target_mail tersedia; jika tidak, biarkan callback yang menangani insert
            if target_mail:
                try:
                    db = data_mysql()
                    exists = db.check_app_credentials_exist(target_mail)
                    if isinstance(exists, int) and exists == 0:
                        developer_token = getattr(settings, 'GOOGLE_ADS_DEVELOPER_TOKEN', os.getenv('GOOGLE_ADS_DEVELOPER_TOKEN', ''))
                        mdb = current_user.get('user_id')
                        mdb_name = current_user.get('user_alias') or current_user.get('user_name')
                        account_name = mdb_name or target_mail
                        # Masukkan baris dengan client dari .env, refresh_token & network_code kosong
                        db.insert_app_credentials(
                            account_id=user_id,
                            account_name=account_name,
                            user_mail=target_mail,
                            client_id=client_id,
                            client_secret=client_secret,
                            refresh_token=None,
                            network_code=None,
                            developer_token=developer_token,
                            mdb=mdb,
                            mdb_name=mdb_name
                        )
                except Exception:
                    # Abaikan kegagalan pre-insert; proses akan tetap mencoba menyimpan saat callback
                    pass

            return redirect(authorization_url)
        except Exception as e:
            req.session['oauth_added_success'] = False
            req.session['oauth_added_message'] = f'Gagal memulai OAuth: {str(e)}'
            return redirect('adx_account')


class AdxAccountOAuthCallbackView(View):
    """Callback Google OAuth: simpan refresh_token dan network_code ke app_credentials."""
    def dispatch(self, request, *args, **kwargs):
        if 'hris_admin' not in request.session:
            return redirect('admin_login')
        return super().dispatch(request, *args, **kwargs)

    def get(self, req):
        logger.info("=== OAuth Callback Started ===")
        logger.info(f"OAuth Callback - GET parameters: {dict(req.GET)}")
        logger.info(f"OAuth Callback - Session keys: {list(req.session.keys())}")
        logger.info(f"OAuth Callback - User mail from session: {req.session.get('user_mail')}")
        logger.info(f"OAuth Callback - Client ID from session: {req.session.get('client_id')}")
        logger.info(f"OAuth Callback - Developer token from session: {req.session.get('developer_token')}")
        
        print("[DEBUG] OAuth Callback - Method called!")
        print(f"[DEBUG] OAuth Callback - State: {req.GET.get('state')}, Code present: {bool(req.GET.get('code'))}")
        try:
            state = req.GET.get('state')
            code = req.GET.get('code')
            expected_state = req.session.get('oauth_flow_state')

            # Ambil konfigurasi dari settings terlebih dahulu, fallback ke environment
            client_id = getattr(settings, 'GOOGLE_OAUTH2_CLIENT_ID', os.getenv('GOOGLE_OAUTH2_CLIENT_ID'))
            client_secret = getattr(settings, 'GOOGLE_OAUTH2_CLIENT_SECRET', os.getenv('GOOGLE_OAUTH2_CLIENT_SECRET'))
            if not client_id or not client_secret:
                req.session['oauth_added_success'] = False
                req.session['oauth_added_message'] = 'GOOGLE_OAUTH2_CLIENT_ID/SECRET tidak ter-set di environment.'
                return redirect('adx_account')

            redirect_uri = req.build_absolute_uri(reverse('adx_account_oauth_callback'))
            client_config = {
                'web': {
                    'client_id': client_id,
                    'client_secret': client_secret,
                    'auth_uri': 'https://accounts.google.com/o/oauth2/v2/auth',
                    'token_uri': 'https://oauth2.googleapis.com/token',
                    'redirect_uris': [redirect_uri]
                }
            }

            scopes = [
                # Scope dasar untuk identitas user (gunakan expanded form yang dikembalikan Google)
                'openid',
                'https://www.googleapis.com/auth/userinfo.email',
                'https://www.googleapis.com/auth/userinfo.profile',
                # Scope untuk Google Ad Manager
                'https://www.googleapis.com/auth/admanager',
                # Scope untuk Google AdSense
                'https://www.googleapis.com/auth/adsense'
            ]

            flow = Flow.from_client_config(client_config, scopes=scopes)
            flow.redirect_uri = redirect_uri

            # Validasi state jika tersedia
            if expected_state and state != expected_state:
                req.session['oauth_added_success'] = False
                req.session['oauth_added_message'] = 'OAuth state tidak valid.'
                return redirect('adx_account')

            # Tukar code dengan token
            flow.fetch_token(code=code)
            credentials = flow.credentials
            refresh_token = credentials.refresh_token

            if not refresh_token:
                # Paksa prompt consent agar refresh token didapat pada approval pertama
                req.session['oauth_added_success'] = False
                req.session['oauth_added_message'] = 'Refresh token tidak diterima. Ulangi dengan consent.'
                return redirect('adx_account')

            # Deteksi network_code otomatis
            network_code = None
            try:
                # Pastikan token segar
                credentials.refresh(Request())
                # Bangun Ad Manager client tanpa network_code
                ad_manager_client = ad_manager.AdManagerClient(credentials, 'HRIS AdX Integration')
                # Coba dapatkan semua networks yang dapat diakses
                try:
                    network_service = ad_manager_client.GetService('NetworkService')
                    networks = network_service.getAllNetworks()
                    if networks:
                        first = networks[0]
                        # Support dict atau object
                        network_code = (
                            getattr(first, 'networkCode', None)
                            or getattr(first, 'network_code', None)
                            or (first.get('networkCode') if isinstance(first, dict) else None)
                            or (first.get('network_code') if isinstance(first, dict) else None)
                        )
                except Exception:
                    # Fallback ke getCurrentNetwork jika getAllNetworks gagal
                    try:
                        network_service = ad_manager_client.GetService('NetworkService')
                        current_network = network_service.getCurrentNetwork()
                        network_code = (
                            getattr(current_network, 'networkCode', None)
                            or getattr(current_network, 'network_code', None)
                            or (current_network.get('networkCode') if isinstance(current_network, dict) else None)
                            or (current_network.get('network_code') if isinstance(current_network, dict) else None)
                        )
                    except Exception:
                        network_code = None
            except Exception:
                network_code = None

            # Ambil userinfo dari Google untuk mengisi account_id & account_name
            account_id = None
            account_name = None
            userinfo_email = None
            try:
                userinfo_resp = requests.get(
                    'https://openidconnect.googleapis.com/v1/userinfo',
                    headers={'Authorization': f'Bearer {credentials.token}'},
                    timeout=10
                )
                if userinfo_resp.status_code == 200:
                    info = userinfo_resp.json()
                    account_id = info.get('sub')
                    account_name = info.get('name') or info.get('email')
                    userinfo_email = info.get('email')
            except Exception:
                pass

            # Fallback jika userinfo tidak tersedia
            if not account_id:
                account_id = req.session.get('oauth_flow_user_id') or req.session.get('hris_admin', {}).get('user_id')
            if not account_name:
                account_name = req.session.get('hris_admin', {}).get('user_alias') or req.session.get('hris_admin', {}).get('user_name') or (req.session.get('oauth_flow_user_mail') or req.session.get('hris_admin', {}).get('user_mail'))

            # Siapkan metadata perekam (mdb/mdb_name) dan developer_token
            admin_session = req.session.get('hris_admin', {})
            mdb = admin_session.get('user_id')
            mdb_name = admin_session.get('user_alias') or admin_session.get('user_name')
            developer_token = getattr(settings, 'GOOGLE_ADS_DEVELOPER_TOKEN', os.getenv('GOOGLE_ADS_DEVELOPER_TOKEN', ''))

            # Simpan ke app_credentials (insert / update by user_mail) sesuai skema baru
            db = data_mysql()
            # Gunakan email aktif di browser jika tersedia dari userinfo
            user_mail = userinfo_email or req.session.get('oauth_flow_user_mail') or req.session.get('hris_admin', {}).get('user_mail')

            if not user_mail:
                req.session['oauth_added_success'] = False
                req.session['oauth_added_message'] = 'User email tidak ditemukan di session.'
                return redirect('adx_account')

            exists = db.check_app_credentials_exist(user_mail)
            if isinstance(exists, dict) and not exists.get('status', True):
                req.session['oauth_added_success'] = False
                req.session['oauth_added_message'] = 'Gagal mengecek app_credentials di database.'
                return redirect('adx_account')

            # Debug logging
            logger.info("=== OAuth Callback - Preparing to save credentials ===")
            logger.info(f"OAuth Callback - User mail: {user_mail}")
            logger.info(f"OAuth Callback - Account name: {account_name}")
            logger.info(f"OAuth Callback - Client ID: {client_id}")
            logger.info(f"OAuth Callback - Client secret length: {len(client_secret) if client_secret else 0}")
            logger.info(f"OAuth Callback - Refresh token length: {len(refresh_token) if refresh_token else 0}")
            logger.info(f"OAuth Callback - Network code: {network_code} (type: {type(network_code)})")
            logger.info(f"OAuth Callback - Developer token length: {len(developer_token) if developer_token else 0}")
            logger.info(f"OAuth Callback - MDB: {mdb}, MDB Name: {mdb_name}")
            
            print(f"[DEBUG] OAuth Callback - Attempting to save credentials for user: {user_mail}")
            print(f"[DEBUG] OAuth Callback - Network code detected: {network_code}")
            print(f"[DEBUG] OAuth Callback - Existing credentials check result: {exists}")
            logger.info(f"OAuth Callback - Attempting to save credentials for user: {user_mail}")
            logger.info(f"OAuth Callback - Network code detected: {network_code}")
            logger.info(f"OAuth Callback - Existing credentials check result: {exists}")

            if exists > 0:
                logger.info(f"OAuth Callback - Updating existing credentials for {user_mail}")
                result = db.update_app_credentials(
                    user_mail,
                    account_name,
                    client_id,
                    client_secret,
                    refresh_token,
                    network_code,
                    developer_token,
                    mdb,
                    mdb_name,
                    '1'
                )
            else:
                logger.info(f"OAuth Callback - Inserting new credentials for {user_mail}")
                result = db.insert_app_credentials(
                    account_name,
                    user_mail,
                    client_id,
                    client_secret,
                    refresh_token,
                    network_code,
                    developer_token,
                    mdb,
                    mdb_name
                )

            logger.info(f"OAuth Callback - Database operation result: {result}")

            if isinstance(result, dict) and result.get('status'):
                req.session['oauth_added_success'] = True
                if network_code:
                    req.session['oauth_added_message'] = f'Kredensial disimpan. Network Code: {network_code}'
                else:
                    req.session['oauth_added_message'] = 'Kredensial disimpan, namun network_code belum terdeteksi.'
                logger.info(f"OAuth Callback - Successfully saved credentials for {user_mail}")
            else:
                req.session['oauth_added_success'] = False
                req.session['oauth_added_message'] = result.get('error', 'Gagal menyimpan app_credentials.')
                logger.error(f"OAuth Callback - Failed to save credentials for {user_mail}: {result}")

            # Bersihkan state
            for k in ['oauth_flow_state', 'oauth_flow_user_mail', 'oauth_flow_user_id']:
                if k in req.session:
                    del req.session[k]

            return redirect('adx_account')
        except Exception as e:
            req.session['oauth_added_success'] = False
            req.session['oauth_added_message'] = f'Error pada callback OAuth: {str(e)}'
            return redirect('adx_account')

class AdxTrafficPerAccountDataView(View):
    """AJAX endpoint untuk data AdX Traffic Per Account"""
    def dispatch(self, request, *args, **kwargs):
        if 'hris_admin' not in request.session:
            return redirect('admin_login')
        return super().dispatch(request, *args, **kwargs)
    
    def get(self, req):
        start_date = req.GET.get('start_date')
        end_date = req.GET.get('end_date')
        selected_accounts = req.GET.get('selected_accounts')
        if selected_accounts:
            user_mail = selected_accounts.split(',')
        else:
            user_mail = req.session.get('hris_admin', {}).get('user_mail')
        selected_sites = req.GET.get('selected_sites')
        try:
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
        selected_accounts = req.GET.get('selected_accounts')
        if selected_accounts:
            user_mail = selected_accounts
        else:
            user_mail = req.session.get('hris_admin', {}).get('user_mail')
        try:
            # Ambil daftar situs dari Ad Manager
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
        data_account_adx = data_mysql().get_all_adx_account_data()
        if not data_account_adx['status']:
            return JsonResponse({
                'status': False,
                'error': data_account_adx['data']
            })
        data = {
            'title': 'AdX Traffic Per Country',
            'user': req.session['hris_admin'],
            'data_account_adx': data_account_adx['data'],
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
        selected_accounts = req.GET.get('selected_accounts')
        if selected_accounts:
            user_mail = selected_accounts.split(',')
        else:
            user_mail = req.session.get('hris_admin', {}).get('user_mail')
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

# OAuth functions removed - using standardized OAuth flow from oauth_views_package


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
        data_account_adx = data_mysql().get_all_adx_account_data()
        if not data_account_adx['status']:
            return JsonResponse({
                'status': False,
                'error': data_account_adx['data']
            })
        data_account = data_mysql().master_account_ads()['data']
        data = {
            'title': 'ROI Per Country',
            'user': req.session['hris_admin'],
            'data_account': data_account,
            'data_account_adx': data_account_adx['data'],
        }
        return render(req, 'admin/report_roi/per_country/index.html', data)

class RoiTrafficPerCountryDataView(View):
    def dispatch(self, request, *args, **kwargs):
        # Jika session tidak ada, untuk request AJAX kembalikan JSON error,
        # selain itu redirect ke halaman login.
        if 'hris_admin' not in request.session:
            is_ajax = False
            try:
                is_ajax = request.headers.get('X-Requested-With') == 'XMLHttpRequest'
            except Exception:
                pass
            if not is_ajax:
                is_ajax = request.META.get('HTTP_X_REQUESTED_WITH') == 'XMLHttpRequest'
            if is_ajax:
                return JsonResponse({
                    'status': False,
                    'error': 'Sesi berakhir atau tidak valid. Silakan login ulang.'
                })
            return redirect('admin_login')
        return super().dispatch(request, *args, **kwargs)
    
    def get(self, req):
        start_date = req.GET.get('start_date')
        end_date = req.GET.get('end_date')
        selected_account_adx = req.GET.get('selected_account_adx', '')
        selected_sites = req.GET.get('selected_sites', '')
        selected_account = req.GET.get('selected_account', '')
        selected_countries = req.GET.get('selected_countries', '')
        try:
            # Validasi parameter tanggal terlebih dahulu
            if not start_date or not end_date:
                return JsonResponse({
                    'status': False,
                    'error': 'Parameter tanggal tidak lengkap'
                })
            # Format tanggal untuk AdManager API
            start_date_formatted = datetime.strptime(start_date, '%Y-%m-%d').strftime('%Y-%m-%d')
            end_date_formatted = datetime.strptime(end_date, '%Y-%m-%d').strftime('%Y-%m-%d')
            # Parse selected countries dari string yang dipisah koma
            countries_list = []
            if selected_countries and selected_countries.strip():
                countries_list = [country.strip() for country in selected_countries.split(',') if country.strip()]
            else:
                print("[DEBUG] No countries selected, will fetch all countries")
            # Jika ada selected_account_adx dari frontend, gunakan sebagai user_mail
            if selected_account_adx:
                user_mail = selected_account_adx
            else:
                # Fallback ke session user_id dan ambil email dari database
                user_id = req.session.get('hris_admin', {}).get('user_id')
                if not user_id:
                    return JsonResponse({
                        'status': False,
                        'error': 'User ID tidak ditemukan dalam session'
                    })
                
                user_data = data_mysql().get_user_by_id(user_id)
                if not user_data.get('status') or not user_data.get('data'):
                    return JsonResponse({
                        'status': False,
                        'error': 'Data user tidak ditemukan'
                    })
                user_mail = user_data['data']['user_mail']
            data_adx = fetch_roi_per_country(start_date_formatted, end_date_formatted, user_mail, selected_sites, countries_list)
            print(f"adx_data: {data_adx}")
            if selected_account:
                rs_account = data_mysql().master_account_ads_by_params({
                    'data_account': selected_account,
                })['data']
            else:
                 rs_account = data_mysql().master_account_ads()['data']
            
            data_facebook = None
            if selected_sites != '':
                data_facebook = fetch_data_insights_by_country_filter_campaign_roi(
                    rs_account, str(start_date_formatted), str(end_date_formatted), selected_sites
                )
            else:
                # Extract unique site names from AdX result
                extracted_sites = []
                if data_adx and data_adx.get('status') and data_adx.get('data'):
                    unique_sites = set()
                    for adx_item in data_adx['data']:
                        site_name = adx_item.get('site_name', '').strip()
                        if site_name and site_name != 'Unknown':
                            unique_sites.add(site_name)
                    extracted_sites = list(unique_sites)
                    print(f"extracted_sites from AdX: {extracted_sites}")
                # Use extracted sites for Facebook data fetching
                if extracted_sites:
                    # Convert list to comma-separated string for Facebook function
                    extracted_sites_str = ','.join(extracted_sites)
                    data_facebook = fetch_data_insights_by_country_filter_campaign_roi(
                        rs_account, str(start_date_formatted), str(end_date_formatted), extracted_sites_str
                    )
                    print(f"facebook_data with extracted sites: {data_facebook}")
                else:
                    print("No valid sites found in AdX data, skipping Facebook data fetch")    
            # Proses penggabungan data AdX dan Facebook
            result = process_roi_traffic_country_data(data_adx, data_facebook)
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
        facebook_click_map = {}
        if data_facebook and data_facebook.get('data'):
            for fb_item in data_facebook['data']:
                country_cd = fb_item.get('country_cd', 'unknown')
                spend = float(fb_item.get('spend', 0))
                facebook_spend_map[country_cd] = spend
                facebook_click_map[country_cd] = int(fb_item.get('clicks', 0))
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
                click_fb = facebook_click_map.get(country_code, 0)
                if click_fb > 0:
                    clicks = click_fb
                else:
                    clicks = clicks_adx
                # Hitung metrik
                ctr = ((clicks / impressions) * 100) if impressions > 0 else 0
                cpc = (revenue / clicks) if clicks > 0 else 0
                ecpm = ((revenue / impressions) * 1000) if impressions > 0 else 0
                roi = (((revenue - spend)/spend)*100) if spend > 0 else 0
                
                # Tambahkan ke hasil
                if spend > 0 or roi > 0:
                    combined_data.append({
                        'country': country_name,
                        'country_code': country_code,
                        'impressions': impressions,
                        'spend': round(spend, 2),
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
        data_account_adx = data_mysql().get_all_adx_account_data()
        if not data_account_adx['status']:
            return JsonResponse({
                'status': False,
                'error': data_account_adx['data']
            })
        data_account = data_mysql().master_account_ads()['data']
        data = {
            'title': 'ROI Per Domain',
            'user': req.session['hris_admin'],
            'data_account': data_account,
            'data_account_adx': data_account_adx['data'],
        }
        return render(req, 'admin/report_roi/per_domain/index.html', data)

class RoiTrafficPerDomainDataView(View):
    """AJAX endpoint untuk data ROI Traffic Per Domain"""
    def dispatch(self, request, *args, **kwargs):
        if 'hris_admin' not in request.session:
            return redirect('admin_login')
        return super().dispatch(request, *args, **kwargs)
    def get(self, req):
        print("=== ROI Traffic Domain Data View Called ===")
        start_date = req.GET.get('start_date')
        end_date = req.GET.get('end_date')
        selected_accounts = req.GET.get('selected_account_adx')
        selected_sites = req.GET.get('selected_sites')
        selected_account = req.GET.get('selected_account')
        print(f"Parameters: start_date={start_date}, end_date={end_date}, selected_sites='{selected_sites}'")
        try:
            # Jika ada selected_account_adx dari frontend, gunakan sebagai user_mail
            if selected_accounts:
                user_mail = selected_accounts
            else:
                # Fallback ke session user_id dan ambil email dari database
                user_id = req.session.get('hris_admin', {}).get('user_id')
                if not user_id:
                    return JsonResponse({
                        'status': False,
                        'error': 'User ID tidak ditemukan dalam session'
                    })
                user_data = data_mysql().get_user_by_id(user_id)
                if not user_data.get('status') or not user_data.get('data'):
                    return JsonResponse({
                        'status': False,
                        'error': 'Data user tidak ditemukan'
                    })
                user_mail = user_data['data']['user_mail']
            # Format tanggal untuk AdManager API
            start_date_formatted = datetime.strptime(start_date, '%Y-%m-%d').strftime('%Y-%m-%d')
            end_date_formatted = datetime.strptime(end_date, '%Y-%m-%d').strftime('%Y-%m-%d')
            # Ambil data AdX (klik, ctr, cpc, eCPM, pendapatan)
            adx_result = fetch_adx_traffic_account_by_user(user_mail, start_date_formatted, end_date_formatted, selected_sites)
            # Ambil data Facebook (spend)
            if selected_account and selected_account != '%':
                rs_account = data_mysql().master_account_ads_by_params({
                    'data_account': selected_account,
                })['data']
            else:
                 rs_account = data_mysql().master_account_ads()['data']
            
            # Extract site names from AdX data if no sites selected
            facebook_data = None
            if selected_sites != '':
                facebook_data = fetch_data_insights_by_date_subdomain_roi(
                    rs_account, start_date_formatted, end_date_formatted, selected_sites
                )
            else:
                # Extract unique site names from AdX result
                extracted_sites = []
                if adx_result and adx_result.get('status') and adx_result.get('data'):
                    unique_sites = set()
                    for adx_item in adx_result['data']:
                        site_name = adx_item.get('site_name', '').strip()
                        if site_name and site_name != 'Unknown':
                            unique_sites.add(site_name)
                    extracted_sites = list(unique_sites)
                    print(f"extracted_sites from AdX: {extracted_sites}")
                
                # Use extracted sites for Facebook data fetching
                if extracted_sites:
                    # Convert list to comma-separated string for Facebook function
                    extracted_sites_str = ','.join(extracted_sites)
                    facebook_data = fetch_data_insights_by_date_subdomain_roi(
                        rs_account, start_date_formatted, end_date_formatted, extracted_sites_str
                    )
                    print(f"facebook_data with extracted sites: {facebook_data}")
                else:
                    print("No valid sites found in AdX data, skipping Facebook data fetch")    
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
        data_account_adx = data_mysql().get_all_adx_account_data()
        if not data_account_adx['status']:
            return JsonResponse({
                'status': False,
                'error': data_account_adx['data']
            })
        data_account = data_mysql().master_account_ads()['data']
        data = {
            'title': 'ROI Summary Dashboard',
            'user': req.session['hris_admin'],
            'data_account': data_account,
            'data_account_adx': data_account_adx['data'],
        }
        return render(req, 'admin/report_roi/all_rekap/index.html', data)

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

class ImportEnvAppCredentialsView(View):
    """Import nilai OAuth client dari environment (.env) ke tabel app_credentials.

    Dapat dipanggil tanpa login admin dengan memberikan query param `user_mail`.
    Jika login admin tersedia, akan memakai email admin aktif sebagai default.
    """
    def dispatch(self, request, *args, **kwargs):
        # Izinkan tanpa login agar bisa dieksekusi sebagai utilitas
        return super().dispatch(request, *args, **kwargs)

    def get(self, req):
        try:
            # Ambil email user dari query param atau session admin
            admin = req.session.get('hris_admin', {})
            user_mail = req.GET.get('user_mail') or admin.get('user_mail')
            if not user_mail:
                # Fallback ke DEFAULT_OAUTH_USER_MAIL jika disediakan
                user_mail = getattr(settings, 'DEFAULT_OAUTH_USER_MAIL', os.getenv('DEFAULT_OAUTH_USER_MAIL'))
            if not user_mail:
                return JsonResponse({'status': False, 'message': 'User email tidak tersedia. Berikan ?user_mail=EMAIL atau set DEFAULT_OAUTH_USER_MAIL.'}, status=400)

            # Ambil client dari settings/env
            client_id = getattr(settings, 'GOOGLE_OAUTH2_CLIENT_ID', os.getenv('GOOGLE_OAUTH2_CLIENT_ID'))
            client_secret = getattr(settings, 'GOOGLE_OAUTH2_CLIENT_SECRET', os.getenv('GOOGLE_OAUTH2_CLIENT_SECRET'))
            developer_token = getattr(settings, 'GOOGLE_ADS_DEVELOPER_TOKEN', os.getenv('GOOGLE_ADS_DEVELOPER_TOKEN', ''))

            if not client_id or not client_secret:
                return JsonResponse({
                    'status': False,
                    'message': 'GOOGLE_OAUTH2_CLIENT_ID/SECRET tidak ter-set di environment.'
                }, status=400)

            # Siapkan metadata: account_id/name dari session jika ada
            account_id = admin.get('user_id')
            account_name = admin.get('user_alias') or admin.get('user_name') or user_mail

            db = data_mysql()
            # Cek apakah sudah ada baris untuk user ini
            exists = db.check_app_credentials_exist(user_mail)
            if isinstance(exists, dict) and not exists.get('status', True):
                return JsonResponse({'status': False, 'message': 'Gagal mengecek app_credentials di database.'}, status=500)

            # Ambil refresh_token/network_code lama jika ada agar tidak terhapus
            existing_refresh_token = None
            existing_network_code = None
            if isinstance(exists, int) and exists > 0:
                try:
                    sql = 'SELECT refresh_token, network_code FROM app_credentials WHERE user_mail = %s LIMIT 1'
                    if db.execute_query(sql, (user_mail,)):
                        row = db.cur_hris.fetchone()
                        if row:
                            if isinstance(row, dict):
                                existing_refresh_token = row.get('refresh_token')
                                existing_network_code = row.get('network_code')
                            else:
                                existing_refresh_token = row[0]
                                existing_network_code = row[1]
                except Exception:
                    pass

            # Upsert ke app_credentials
            mdb = admin.get('user_id')
            mdb_name = admin.get('user_alias') or admin.get('user_name')

            if isinstance(exists, int) and exists > 0:
                result = db.update_app_credentials(
                    user_mail,
                    account_name,
                    client_id,
                    client_secret,
                    existing_refresh_token,
                    existing_network_code,
                    developer_token,
                    mdb,
                    mdb_name,
                    '1'
                )
            else:
                result = db.insert_app_credentials(
                    account_name,
                    user_mail,
                    client_id,
                    client_secret,
                    None,
                    None,
                    developer_token,
                    mdb,
                    mdb_name
                )

            if isinstance(result, dict) and result.get('status'):
                return JsonResponse({
                    'status': True,
                    'message': 'Berhasil menyimpan client dari environment ke app_credentials',
                    'user_mail': user_mail,
                    'client_id_saved': bool(client_id),
                    'client_secret_saved': bool(client_secret)
                })
            else:
                return JsonResponse({
                    'status': False,
                    'message': result.get('error', 'Gagal menyimpan app_credentials')
                }, status=500)
        except Exception as e:
            return JsonResponse({'status': False, 'message': f'Error: {str(e)}'}, status=500)


@method_decorator(csrf_exempt, name='dispatch')
class UpdateAccountNameView(View):
    def post(self, request):
        try:
            user_mail = request.POST.get('user_mail')
            new_account_name = request.POST.get('account_name')
            
            if not user_mail or not new_account_name:
                return JsonResponse({
                    'status': False,
                    'message': 'User mail dan account name harus diisi'
                }, status=400)
                
            # Update account name in database
            db = data_mysql()
            result = db.update_account_name(user_mail, new_account_name)
            
            if result['status']:
                return JsonResponse({
                    'status': True,
                    'message': 'Account name berhasil diupdate'
                })
            else:
                return JsonResponse({
                    'status': False,
                    'message': result.get('message', 'Gagal mengupdate account name')
                }, status=500)
                
        except Exception as e:
            return JsonResponse({
                'status': False,
                'message': f'Error: {str(e)}'
            }, status=500)
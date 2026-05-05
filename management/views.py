from collections import defaultdict
import os
import csv
from io import StringIO
import math
import re
import site
import inspect
from traceback import print_tb
import traceback
from typing import Any, Optional
import traceback
from django.conf import settings
import pprint
from django.shortcuts import render, redirect
from django.views import View
from django.views.decorators.csrf import csrf_exempt
from django.utils.decorators import method_decorator
from django.utils.http import url_has_allowed_host_and_scheme
from django.conf import settings
from django import template
from datetime import datetime, date, timedelta
from django.http import HttpResponse, JsonResponse, QueryDict
from django.core.management import call_command

from management.database import insert_df, query_df
try:
    from .database import data_mysql
except Exception:
    try:
        from management.database import data_mysql
    except Exception:
        from settings.database import data_mysql
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
import numpy as np
import time
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
from datetime import datetime
from django.http import JsonResponse
# Optional dependencies: guard imports to prevent module-level crashes
try:
    import tldextract
except Exception:
    tldextract = None
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

try:
    from googleads import ad_manager
except Exception:
    ad_manager = None

from google_auth_oauthlib.flow import Flow
import os
from django.urls import reverse
from django.shortcuts import redirect
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeoutError
from .utils import (
    fetch_data_all_insights_data_all,
    fetch_data_all_insights_total_all,
    fetch_data_insights_account_range_all,
    fetch_data_all_insights,
    fetch_data_all_insights_total,
    fetch_data_insights_account_range,
    fetch_data_insights_account,
    fetch_daily_budget_per_campaign,
    fetch_status_per_campaign,
    fetch_data_insights_campaign_filter_sub_domain,
    fetch_data_insights_campaign_filter_account,
    fetch_data_insights_by_country_filter_campaign,
    fetch_user_sites_list,
    fetch_ad_manager_reports,
    fetch_ad_manager_inventory,
    fetch_adx_traffic_account_by_user,
    fetch_user_adx_account_data,
    fetch_adx_account_data,
    fetch_data_insights_all_accounts_by_subdomain,
    fetch_adx_traffic_per_country,
    fetch_campaign_meta,
    get_telegram_chat_id_for_user,
    send_telegram_message_aiogram,
    _format_idr_number,
    generate_cache_key,
    get_cached_data,
    set_cached_data,
)

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
            'user_mail': user_data['data'][0]['user_mail'],  # Tambahkan user_mail ke session
            'super_st': user_data['data'][0]['super_st']  # Tambahkan superadmin ke session
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
                    'user_mail': rs_data['data']['user_mail'],  # Tambahkan user_mail ke session
                    'super_st': rs_data['data']['super_st']  # Tambahkan superadmin ke session
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

    
class RegisterAccountAdmin(View):
    def post(self, req):
        username = (req.POST.get('reg_username') or '').strip()
        alias = (req.POST.get('reg_alias') or '').strip()
        email = (req.POST.get('reg_email') or '').strip()
        telp = (req.POST.get('reg_telp') or '').strip()
        alamat = (req.POST.get('reg_alamat') or '').strip()
        password = (req.POST.get('reg_password') or '')
        password2 = (req.POST.get('reg_password2') or '')

        if not username or not alias or not email or not password or not password2:
            return JsonResponse({
                'status': False,
                'message': 'Username, Nama/Alias, Email, dan Password wajib diisi.'
            })

        if password != password2:
            return JsonResponse({
                'status': False,
                'message': 'Password dan Confirm Password tidak sama.'
            })

        if len(password) < 6:
            return JsonResponse({
                'status': False,
                'message': 'Password minimal 6 karakter.'
            })

        if not re.match(r'^[^@\s]+@[^@\s]+\.[^@\s]+$', email):
            return JsonResponse({
                'status': False,
                'message': 'Format email tidak valid.'
            })

        db = data_mysql()
        try:
            q_check = "SELECT user_id FROM app_users WHERE user_name = %s OR user_mail = %s LIMIT 1"
            if db.execute_query(q_check, (username, email)):
                row = db.cur_hris.fetchone()
                if row:
                    return JsonResponse({
                        'status': False,
                        'message': 'Username atau email sudah terdaftar.'
                    })
        except Exception:
            logger.exception('RegisterAccountAdmin duplicate check failed')
            return JsonResponse({
                'status': False,
                'message': 'Terjadi error saat memvalidasi account.'
            })

        admin = req.session.get('hris_admin', {})
        if not isinstance(admin, dict):
            admin = {}

        data_insert = {
            'user_name': username,
            'user_pass': password,
            'user_alias': alias,
            'user_mail': email,
            'user_telp': telp,
            'user_alamat': alamat,
            'user_st': '1',
            'user_foto': 'default_avatar.png',
            'mdb': admin.get('user_id'),
            'mdb_name': admin.get('user_alias') or admin.get('user_name') or 'Self Register',
            'mdd': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        }

        try:
            result = db.insert_user(data_insert)
            hasil = (result or {}).get('hasil', {})
            if hasil.get('status'):
                return JsonResponse({
                    'status': True,
                    'message': 'Account berhasil dibuat. Silakan login.'
                })
            return JsonResponse({
                'status': False,
                'message': hasil.get('message') or 'Gagal menyimpan data user.'
            })
        except Exception:
            logger.exception('RegisterAccountAdmin insert failed')
            return JsonResponse({
                'status': False,
                'message': 'Terjadi error saat membuat account.'
            })

@csrf_exempt
def get_countries_facebook_ads(request):
    """Endpoint untuk mendapatkan daftar negara yang tersedia"""
    if 'hris_admin' not in request.session:
        return redirect('admin_login')
    try:
        tanggal_sampai = datetime.now().date()
        tanggal_dari = tanggal_sampai - timedelta(days=7)
        data_account = request.GET.get('data_account')
        data_domain = request.GET.get('data_domain')
        selected_domain_list = []
        if data_domain:
            selected_domain_list = [str(s).strip() for s in data_domain.split(',') if s.strip()]
         # Gunakan cache untuk menghindari pemanggilan API berulang
        try:
            cache_key = generate_cache_key(
                'countries_facebook_ads',
                tanggal_dari.strftime('%Y-%m-%d'),
                tanggal_sampai.strftime('%Y-%m-%d'),
                data_account,
                selected_domain_list
            )
            cached_countries = get_cached_data(cache_key)
            if cached_countries is not None:
                return JsonResponse({
                    'status': 'success',
                    'countries': cached_countries
                })
        except Exception as _cache_err:
        # Jika cache bermasalah, lanjutkan tanpa memblokir proses
            print(f"[WARNING] countries_adx cache unavailable: {_cache_err}")
        # Sort berdasarkan nama negara
        result = data_mysql().fetch_country_ads_list(
            tanggal_dari.strftime('%Y-%m-%d'), 
            tanggal_sampai.strftime('%Y-%m-%d'),
            data_account,
            selected_domain_list
        )
        # Validasi struktur result
        if not result['hasil']['data']:
            return JsonResponse({
                'status': 'error',
                'message': 'Tidak ada data yang tersedia.',
                'countries': []
            })
        if not isinstance(result['hasil'], dict):
            return JsonResponse({
                'status': 'error',
                'message': 'Format data tidak valid.',
                'countries': []
            })
        
        # Periksa apakah ada key 'data' dalam result['hasil']
        if 'data' not in result['hasil']:
            return JsonResponse({
                'status': 'error',
                'message': 'Data negara tidak tersedia.',
                'countries': []
            })
        
        # Periksa apakah data adalah list
        if not isinstance(result['hasil']['data'], list):
            return JsonResponse({
                'status': 'error',
                'message': 'Format data negara tidak valid.',
                'countries': []
            })
        
        # Ekstrak daftar negara dari data yang tersedia dan hilangkan duplikasi
        countries = []
        seen = set()
        for country_data in result['hasil']['data']:
            if not isinstance(country_data, dict):
                continue
            country_name = (country_data.get('country_name') or '').strip()
            country_code = (country_data.get('country_code') or '').strip().upper()
            if country_code == 'TU':
                country_code = 'TR'
            if not country_name:
                continue
            # Gunakan code jika ada, jika tidak gunakan nama sebagai key dedup
            key = country_code or country_name.lower()
            if key in seen:
                continue
            seen.add(key)
            country_label = f"{country_name} ({country_code})" if country_code else country_name
            countries.append({
                'code': country_code,
                'name': country_label
            })
            
        # Sort berdasarkan nama negara
        countries.sort(key=lambda x: x['name'])
        # Simpan hasil ke cache agar panggilan berikutnya cepat
        try:
            set_cached_data(cache_key, countries, timeout=6 * 60 * 60)  # 6 jam
        except Exception as _cache_set_err:
            print(f"[WARNING] failed to cache countries_adx: {_cache_set_err}")
        return JsonResponse({
            'status': 'success',
            'countries': countries
        })
        
    except Exception as e:
        return JsonResponse({
            'status': 'error',
            'message': 'Gagal mengambil data negara.',
            'error': str(e),
            'countries': []
        }, status=500)

class FacebookDomainSuggestView(View):
    """AJAX endpoint suggest subdomain Facebook Ads (Select2)"""
    def dispatch(self, request, *args, **kwargs):
        if 'hris_admin' not in request.session:
            return redirect('admin_login')
        return super().dispatch(request, *args, **kwargs)

    def get(self, req):
        q = str(req.GET.get('q') or '').strip()
        start_date = str(req.GET.get('start_date') or '').strip()
        end_date = str(req.GET.get('end_date') or '').strip()
        selected_account = str(req.GET.get('selected_account') or '').strip()

        if not q:
            return JsonResponse({'results': []})

        if not start_date or not end_date:
            today = datetime.now().strftime('%Y-%m-%d')
            start_date = start_date or today
            end_date = end_date or today

        if selected_account == '':
            rs_account = data_mysql().master_account_ads()
            rows = (rs_account or {}).get('data') if isinstance(rs_account, dict) else []
            if not isinstance(rows, list):
                rows = []
            account_ids = [str((r or {}).get('account_id') or '').strip() for r in rows if str((r or {}).get('account_id') or '').strip()]
            selected_account = ','.join(account_ids)

        account_list = [s.strip() for s in selected_account.split(',') if s.strip()]
        like = f"%{q}%"
        limit = 100

        account_tokens = []
        for a in account_list:
            v = str(a or '').strip()
            if not v:
                continue
            account_tokens.append(v)
            if v.lower().startswith('act_'):
                account_tokens.append(v[4:])
            else:
                account_tokens.append(f"act_{v}")
        account_tokens = list(dict.fromkeys([x for x in account_tokens if x]))

        db = data_mysql()
        rows = []

        try:
            db._ensure_report_connection()
            db.cur_hris = db.report_cur
            where = [
                "toDate(b.data_ads_country_tanggal) BETWEEN toDate(%s) AND toDate(%s)",
                "lowerUTF8(b.data_ads_domain) LIKE lowerUTF8(%s)",
            ]
            params = [start_date, end_date, like]
            if account_tokens:
                acc_like = " OR ".join(["replaceRegexpAll(lowerUTF8(toString(b.account_ads_id)), '^act_', '') LIKE %s"] * len(account_tokens))
                where.append(f"({acc_like})")
                params.extend([f"%{str(a).lower().removeprefix('act_')}%" for a in account_tokens])
            sql = "\n".join([
                "SELECT DISTINCT b.data_ads_domain AS site_name",
                "FROM data_ads_country b",
                "WHERE " + " AND ".join(where),
                "ORDER BY site_name ASC",
                f"LIMIT {limit}",
            ])
            db.cur_hris.execute(sql, tuple(params))
            rows = db.fetch_all()
        except Exception:
            try:
                if db.ensure_connection():
                    db.cur_hris = db.mysql_cur
                    where = [
                        "b.data_ads_country_tanggal BETWEEN %s AND %s",
                        "b.data_ads_domain LIKE %s",
                    ]
                    params = [start_date, end_date, like]
                    if account_tokens:
                        acc_like = " OR ".join(["b.account_ads_id LIKE %s"] * len(account_tokens))
                        where.append(f"({acc_like})")
                        params.extend([f"%{a}%" for a in account_tokens])
                    sql = "\n".join([
                        "SELECT DISTINCT b.data_ads_domain AS site_name",
                        "FROM data_ads_country b",
                        "WHERE " + " AND ".join(where),
                        "ORDER BY site_name ASC",
                        f"LIMIT {limit}",
                    ])
                    db.cur_hris.execute(sql, tuple(params))
                    rows = db.fetch_all()
            except Exception:
                rows = []

        results = []
        seen = set()
        for r in (rows or []):
            site = str((r or {}).get('site_name') or '').strip()
            if not site:
                continue
            k = site.lower()
            if k in seen:
                continue
            seen.add(k)
            results.append({'id': site, 'text': site})
            if len(results) >= limit:
                break

        return JsonResponse({'results': results})

@csrf_exempt
def get_countries_adx(request):
    """Endpoint untuk mendapatkan daftar negara yang tersedia"""
    if 'hris_admin' not in request.session:
        return redirect('admin_login')
    try:
        # Ambil data negara dari AdX untuk periode 30 hari terakhir
        end_date = datetime.now().date()
        start_date = end_date - timedelta(days=7)
        selected_account = request.GET.get('selected_accounts')
        selected_domains = request.GET.get('selected_domains')
        selected_domain_list = []
        if selected_domains:
            selected_domain_list = [str(s).strip() for s in selected_domains.split(',') if s.strip()]
        # Gunakan cache untuk menghindari pemanggilan API berulang
        try:
            cache_key = generate_cache_key(
                'countries_adx',
                start_date.strftime('%Y-%m-%d'),
                end_date.strftime('%Y-%m-%d'),
                selected_domain_list or '',
                ','.join(selected_domain_list) or ''
            )
            cached_countries = get_cached_data(cache_key)
            if cached_countries is not None:
                return JsonResponse({
                    'status': 'success',
                    'countries': cached_countries
                })
        except Exception as _cache_err:
            # Jika cache bermasalah, lanjutkan tanpa memblokir proses
            print(f"[WARNING] countries_adx cache unavailable: {_cache_err}")
        result = data_mysql().fetch_country_list(
            start_date.strftime('%Y-%m-%d'),
            end_date.strftime('%Y-%m-%d'),
            selected_account,
            selected_domain_list,
        )
        # Validasi struktur result
        if not result['hasil']['data']:
            return JsonResponse({
                'status': 'error',
                'message': 'Tidak ada data yang tersedia.',
                'countries': []
            })
        
        if not isinstance(result['hasil'], dict):
            return JsonResponse({
                'status': 'error',
                'message': 'Format data tidak valid.',
                'countries': []
            })
        
        # Periksa apakah ada key 'data' dalam result['hasil']
        if 'data' not in result['hasil']:
            return JsonResponse({
                'status': 'error',
                'message': 'Data negara tidak tersedia.',
                'countries': []
            })
        
        # Periksa apakah data adalah list
        if not isinstance(result['hasil']['data'], list):
            return JsonResponse({
                'status': 'error',
                'message': 'Format data negara tidak valid.',
                'countries': []
            })
        
        # Ekstrak daftar negara dari data yang tersedia dan hilangkan duplikasi
        countries = []
        seen = set()
        for country_data in result['hasil']['data']:
            if not isinstance(country_data, dict):
                continue
            country_name = (country_data.get('country_name') or '').strip()
            country_code = (country_data.get('country_code') or '').strip().upper()
            if country_code == 'TU':
                country_code = 'TR'
            if not country_name:
                continue
            # Gunakan code jika ada, jika tidak gunakan nama sebagai key dedup
            key = country_code or country_name.lower()
            if key in seen:
                continue
            seen.add(key)
            country_label = f"{country_name} ({country_code})" if country_code else country_name
            countries.append({
                'code': country_code,
                'name': country_label
            })
            
        # Sort berdasarkan nama negara
        countries.sort(key=lambda x: x['name'])
        # Simpan hasil ke cache agar panggilan berikutnya cepat
        try:
            set_cached_data(cache_key, countries, timeout=6 * 60 * 60)  # 6 jam
        except Exception as _cache_set_err:
            print(f"[WARNING] failed to cache countries_adx: {_cache_set_err}")
        return JsonResponse({
            'status': 'success',
            'countries': countries
        })
        
    except Exception as e:
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
                messages.warning(req, 'Password has been reset, but email failed to send.')
                return redirect('admin_login')

            messages.success(req, 'A new password has been sent to your email.')
            return redirect('admin_login')

        except Exception as e:
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
    
@method_decorator(csrf_exempt, name='dispatch')
class DashboardScoringDataView(View):
    def dispatch(self, request, *args, **kwargs):
        if 'hris_admin' not in request.session:
            return JsonResponse({'status': False, 'error': 'Unauthorized'}, status=401)
        return super().dispatch(request, *args, **kwargs)

    def post(self, req):
        def safe_float(v):
            try: return float(v)
            except: return 0.0
        def clip(v, lo, hi):
            return max(lo, min(hi, v))
        def normalize_conf(v):
            raw = safe_float(v)
            return clip((raw / 100.0) if raw > 1.0 else raw, 0.0, 1.0)
        def derive_anomaly_cards(traffic, delivery, yield_score, revenue_score, risk, adj):
            cards = []
            if delivery <= -45: cards.append('AD_REQUEST_FILL_ISSUE')
            if yield_score <= -45: cards.append('ECPM_YIELD_DROP')
            if revenue_score <= -35: cards.append('REVENUE_DROP')
            if traffic <= -35: cards.append('TRAFFIC_DROP')
            if risk >= 70: cards.append('IVT_RISK')
            if adj <= -60: cards.append('NEG_ADJUSTMENT')
            return cards
        def campaign_maturity_profile(frame):
            if frame is None or frame.empty:
                return {
                    'campaign_count': 0,
                    'mature_campaign_count': 0,
                    'mature_campaign_ratio': 0.0,
                    'mature_spend_share': 0.0,
                }
            if 'days_active' not in frame.columns:
                return {
                    'campaign_count': 0,
                    'mature_campaign_count': 0,
                    'mature_campaign_ratio': 0.0,
                    'mature_spend_share': 0.0,
                }

            days_vals = pd.to_numeric(frame['days_active'], errors='coerce').fillna(0.0)
            spend_vals = pd.to_numeric(frame.get('spend', pd.Series([0.0] * len(frame), index=frame.index)), errors='coerce').fillna(0.0)
            revenue_vals = pd.to_numeric(frame.get('revenue_value', pd.Series([0.0] * len(frame), index=frame.index)), errors='coerce').fillna(0.0)
            signal_vals = pd.to_numeric(frame.get('signal_total', pd.Series([0.0] * len(frame), index=frame.index)), errors='coerce').fillna(0.0)
            label_vals = frame.get('final_label', frame.get('root_cause_label', pd.Series([''] * len(frame), index=frame.index))).astype(str).str.strip().str.upper()
            learning_labels = {'', 'LEARNING', 'DATA_INCOMPLETE', 'BELUM ADA HASIL'}
            non_learning_vals = (~label_vals.isin(learning_labels)).astype(int)
            camp = pd.Series([''] * len(frame), index=frame.index, dtype=object)
            if 'meta_campaign' in frame.columns:
                camp = frame['meta_campaign'].astype(str).str.strip().str.upper()
            if 'entity_key' in frame.columns:
                ek_camp = frame['entity_key'].astype(str).str.split('|').str[1].fillna('').astype(str).str.strip().str.upper()
                camp = camp.where(camp.ne(''), ek_camp)
            camp = camp.where(camp.ne(''), '__UNKNOWN__')

            date_col = None
            if 'scoring_date' in frame.columns:
                date_col = 'scoring_date'
            elif 'date' in frame.columns:
                date_col = 'date'

            base_df = pd.DataFrame({'campaign': camp, 'days_active': days_vals, 'spend': spend_vals, 'revenue_value': revenue_vals, 'signal_total': signal_vals, 'non_learning': non_learning_vals}, index=frame.index)
            if date_col is not None:
                base_df['maturity_date'] = pd.to_datetime(frame[date_col], errors='coerce').dt.date

            grp = base_df.groupby('campaign', dropna=False).agg(days_active=('days_active', 'max'), spend=('spend', 'sum'), revenue_value=('revenue_value', 'sum'), signal_total=('signal_total', 'sum'), non_learning=('non_learning', 'sum'))
            if date_col is not None:
                days_by_campaign = base_df.groupby('campaign', dropna=False)['maturity_date'].nunique(dropna=True)
                grp['history_days'] = pd.to_numeric(days_by_campaign, errors='coerce').fillna(0.0)
                grp['maturity_days'] = grp[['days_active', 'history_days']].max(axis=1)
            else:
                grp['maturity_days'] = grp['days_active']

            campaign_count = int(len(grp))
            if campaign_count <= 0:
                return {
                    'campaign_count': 0,
                    'mature_campaign_count': 0,
                    'mature_campaign_ratio': 0.0,
                    'mature_spend_share': 0.0,
                }

            grp['is_running'] = (grp['spend'] > 0) | (grp['revenue_value'] > 0) | (grp['signal_total'] > 0)
            mature_mask = (grp['maturity_days'] >= 3) | (grp['is_running'] & (grp['non_learning'] > 0))
            mature_campaign_count = int(mature_mask.sum())
            mature_campaign_ratio = float(mature_campaign_count / campaign_count)
            total_spend = float(pd.to_numeric(grp['spend'], errors='coerce').fillna(0.0).sum())
            mature_spend = float(pd.to_numeric(grp.loc[mature_mask, 'spend'], errors='coerce').fillna(0.0).sum()) if mature_campaign_count > 0 else 0.0
            mature_spend_share = float(mature_spend / total_spend) if total_spend > 0 else mature_campaign_ratio
            return {
                'campaign_count': campaign_count,
                'mature_campaign_count': mature_campaign_count,
                'mature_campaign_ratio': mature_campaign_ratio,
                'mature_spend_share': mature_spend_share,
            }
        def filter_running_non_learning_campaign_rows(frame):
            if frame is None or frame.empty:
                return frame
            camp = pd.Series([''] * len(frame), index=frame.index, dtype=object)
            if 'meta_campaign' in frame.columns:
                camp = frame['meta_campaign'].astype(str).str.strip().str.upper()
            if 'entity_key' in frame.columns:
                ek_camp = frame['entity_key'].astype(str).str.split('|').str[1].fillna('').astype(str).str.strip().str.upper()
                camp = camp.where(camp.ne(''), ek_camp)
            camp = camp.where(camp.ne(''), '__UNKNOWN__')
            days_vals = pd.to_numeric(frame.get('days_active', pd.Series([0.0] * len(frame), index=frame.index)), errors='coerce').fillna(0.0)
            spend_vals = pd.to_numeric(frame.get('spend', pd.Series([0.0] * len(frame), index=frame.index)), errors='coerce').fillna(0.0)
            revenue_vals = pd.to_numeric(frame.get('revenue_value', pd.Series([0.0] * len(frame), index=frame.index)), errors='coerce').fillna(0.0)
            signal_vals = pd.to_numeric(frame.get('signal_total', pd.Series([0.0] * len(frame), index=frame.index)), errors='coerce').fillna(0.0)
            label_vals = frame.get('final_label', frame.get('root_cause_label', pd.Series([''] * len(frame), index=frame.index))).astype(str).str.strip().str.upper()
            learning_labels = {'', 'LEARNING', 'DATA_INCOMPLETE', 'BELUM ADA HASIL'}
            non_learning_vals = (~label_vals.isin(learning_labels)).astype(int)
            grp = pd.DataFrame({'campaign': camp, 'days_active': days_vals, 'spend': spend_vals, 'revenue_value': revenue_vals, 'signal_total': signal_vals, 'non_learning': non_learning_vals}).groupby('campaign', dropna=False).agg(days_active=('days_active', 'max'), spend=('spend', 'sum'), revenue_value=('revenue_value', 'sum'), signal_total=('signal_total', 'sum'), non_learning=('non_learning', 'sum'))
            grp['is_running'] = (grp['spend'] > 0) | (grp['revenue_value'] > 0) | (grp['signal_total'] > 0)
            grp['is_learning'] = (grp['days_active'] < 3) & (grp['non_learning'] <= 0)
            eligible_campaigns = grp.index[(grp['is_running']) & (~grp['is_learning'])].tolist()
            if not eligible_campaigns:
                return frame
            return frame.loc[camp.isin(eligible_campaigns)].copy()

        def derive_score_decision(health, risk, adj, conf, dm, label, profit_strong, anomaly_cards, roi_value=0.0, source_mode='BLENDED'):
            negative_labels = ["TRAFFIC_DROP","SERVING_DROP","YIELD_DROP","VIEWABILITY_DROP","EFFICIENCY_DROP","REVENUE_DROP","NEGATIVE_MIXED","NEG_ADJUSTMENT"]
            label_up = str(label or '').strip().upper()
            source_mode_key = str(source_mode or 'BLENDED').strip().upper()
            single_source = source_mode_key in ["ADX_ONLY", "ADSENSE_ONLY"]
            down_score_cut = 52 if single_source else 55
            down_dm_cut = -12 if single_source else -10
            down_anomaly_cut = 3 if single_source else 2
            profit_component = clip((float(roi_value) + 1.0) * 50.0, 0.0, 100.0)
            score_raw = (((health + 100.0) / 2.0) * 0.25) + ((100.0 - risk) * 0.2) + (conf * 100.0 * 0.1) + (clip(dm + 50.0, 0.0, 100.0) * 0.15) + (profit_component * 0.30)
            score = int(round(clip(score_raw, 0.0, 100.0)))
            decision = "HOLD"
            severe_anomaly = label_up.startswith("RED_FLAG") or (risk >= 85 and conf >= 0.60) or (dm <= -70 and health <= -25) or ('IVT_RISK' in anomaly_cards)
            anomaly_pressure = len(anomaly_cards)
            if severe_anomaly or (risk >= 90 and conf >= 0.60 and score < 40) or (dm <= -65 and health <= -25 and conf >= 0.60):
                decision = "STOP"
            elif (label_up in ["POSITIVE_EXPANSION", "POSITIVE_RECOVERY"] and score >= 62 and risk < 60 and dm >= 6 and conf >= 0.45) or (score >= 72 and risk < 50 and dm >= 10 and conf >= 0.50):
                decision = "SCALE UP"
            elif ((label_up in negative_labels and ((score < down_score_cut and dm < down_dm_cut) or (health < -12 and adj < -18))) or (score < 40 and dm < -12 and (health < -15 or risk >= 72)) or (anomaly_pressure >= down_anomaly_cut and score < 62)) and (not (profit_strong and anomaly_pressure <= 1 and not severe_anomaly)):
                decision = "SCALE DOWN"

            profit_guard_hold = profit_strong and (risk < (45 if single_source else 40)) and (label_up in ["WATCH_DECAY", "WATCH_NEGATIVE"]) and (decision == "SCALE_DOWN" or decision == "SCALE DOWN")
            if profit_guard_hold:
                decision = "HOLD"
            return score, decision

        def derive_campaign_action(status_tag, margin, risk, health, join_status=''):
            tag = str(status_tag or '').strip().upper()
            st = str(join_status or '').strip().upper()
            m = safe_float(margin)
            r = safe_float(risk)
            h = safe_float(health)
            if tag.startswith('RED_FLAG') or ('FRAUD' in tag) or (r >= 85) or (m <= -90):
                return 'STOP'
            if ('PAUSE' in tag) or ('PAUSE' in st) or ((r >= 75) and (m <= -60)) or ((h <= -30) and (m <= -45)):
                return 'PAUSE'
            if ('NEG' in tag) or ('DROP' in tag) or (m <= -25) or (h <= -12) or (r >= 65):
                return 'SCALE DOWN'
            if ('POSITIVE' in tag) or ('RECOVERY' in tag) or ('EXPANSION' in tag) or ((m >= 20) and (r < 55) and (h >= 0)):
                return 'SCALE UP'
            return 'HOLD'

        def apply_campaign_consistency_guard(decision, country_details):
            base = str(decision or '').strip().upper()
            if base in ['', 'LEARNING', 'DATA_INCOMPLETE']:
                return decision, {'campaign_total': 0, 'stop_ratio': 0.0, 'pause_ratio': 0.0, 'majority_action': 'HOLD'}
            rows = country_details if isinstance(country_details, list) else []
            agg = {}
            for row in rows:
                camp = str(row.get('meta_campaign') or '').strip().upper()
                if not camp:
                    continue
                if camp not in agg:
                    agg[camp] = {'m': 0.0, 'r': 0.0, 'h': 0.0, 'n': 0, 'tags': {}, 'join_status': ''}
                rec = agg[camp]
                rec['m'] += safe_float(row.get('decision_margin'))
                rec['r'] += safe_float(row.get('ivt_risk_score'))
                rec['h'] += safe_float(row.get('health_score'))
                rec['n'] += 1
                t = str(row.get('final_label') or '').strip().upper()
                if t:
                    rec['tags'][t] = int(rec['tags'].get(t, 0)) + 1
                rec['join_status'] = str(row.get('join_status') or rec.get('join_status') or '').strip().upper()
            if not agg:
                return decision, {'campaign_total': 0, 'stop_ratio': 0.0, 'pause_ratio': 0.0, 'majority_action': 'HOLD'}
            counts = {'STOP': 0, 'PAUSE': 0, 'SCALE DOWN': 0, 'HOLD': 0, 'SCALE UP': 0}
            for _, rec in agg.items():
                n = max(1, int(rec.get('n', 0)))
                m = float(rec.get('m', 0.0)) / n
                r = float(rec.get('r', 0.0)) / n
                h = float(rec.get('h', 0.0)) / n
                tags = rec.get('tags') or {}
                dom_tag = max(tags.items(), key=lambda kv: kv[1])[0] if tags else ''
                act = derive_campaign_action(dom_tag, m, r, h, rec.get('join_status', ''))
                counts[act] = int(counts.get(act, 0)) + 1
            total = max(1, sum(counts.values()))
            stop_ratio = float(counts.get('STOP', 0) / total)
            pause_ratio = float(counts.get('PAUSE', 0) / total)
            majority_action = max(counts.items(), key=lambda kv: kv[1])[0]
            final_decision = base
            if stop_ratio >= 0.80:
                final_decision = 'STOP'
            elif (stop_ratio >= 0.50) and (base != 'STOP'):
                final_decision = 'SCALE DOWN'
            elif (stop_ratio >= 0.30) and (base == 'HOLD'):
                final_decision = 'SCALE DOWN'
            return final_decision, {
                'campaign_total': int(total),
                'stop_ratio': float(round(stop_ratio, 4)),
                'pause_ratio': float(round(pause_ratio, 4)),
                'majority_action': str(majority_action)
            }
        try:
            if pd is None:
                raise RuntimeError('pandas belum tersedia')
            payload = json.loads((req.body or b'').decode('utf-8') or '{}')
            target_date = str(payload.get('date') or '').strip()
            target_date_obj = pd.to_datetime(target_date, errors='coerce').date() if str(target_date).strip() else None
            dim = str(payload.get('dim') or 'domain').strip().lower()
            raw_entities = payload.get('entities') or []
            include_events = bool(payload.get('include_events'))
            include_source = bool(payload.get('include_source', True))
            include_timeline = bool(payload.get('include_timeline', True))
            def normalize_site_entity(v):
                s = str(v or '').strip().lower()
                s = re.sub(r'^https?://', '', s)
                s = s.split('/')[0].split('?')[0].split('#')[0]
                s = re.sub(r'^www\.', '', s)
                return s
            def extract_site_base(v):
                s = normalize_site_entity(v)
                if not s:
                    return ''
                try:
                    ext = tldextract.extract(s)
                    if ext.domain and ext.suffix:
                        return f"{ext.domain}.{ext.suffix}"
                except Exception:
                    pass
                parts = [p for p in s.split('.') if p]
                return '.'.join(parts[-2:]) if len(parts) >= 2 else s
            if dim == 'country':
                entities_raw = [str(x).strip().upper() for x in raw_entities if str(x).strip()]
                entities = []
                for e in entities_raw:
                    if e not in entities:
                        entities.append(e)
                    if e == 'TR' and 'TU' not in entities:
                        entities.append('TU')
                    elif e == 'TU' and 'TR' not in entities:
                        entities.append('TR')
            else:
                entity_set = set()
                for x in raw_entities:
                    raw = str(x).strip()
                    if not raw:
                        continue
                    norm = normalize_site_entity(raw)
                    if norm:
                        entity_set.add(norm)
                    base = extract_site_base(raw)
                    if base:
                        entity_set.add(base)
                entities = sorted(entity_set)
            if not target_date:
                return JsonResponse({'status': False, 'error': 'date wajib diisi'}, status=400)
            if not entities:
                return JsonResponse({'status': True, 'data': {}}, safe=False)
            scoring_module = _get_scoring_concept_module()
            query_df = getattr(scoring_module, 'query_df', None)
            if query_df is None:
                raise RuntimeError('query_df belum tersedia')
            status_table = getattr(scoring_module, 'STATUS_TABLE', 'hris_trendHorizone.fact_site_country_status_history')
            event_table = getattr(scoring_module, 'EVENT_TABLE', 'hris_trendHorizone.fact_change_event_long')
            source_table = getattr(scoring_module, 'SOURCE_TABLE', 'hris_trendHorizone.fact_join_hourly')
            def table_exists(table_name):
                try:
                    q = query_df(f"EXISTS TABLE {table_name}")
                    if q is None or q.empty:
                        return False
                    first_col = q.columns[0]
                    return bool(int(q.iloc[0][first_col]))
                except Exception as ex:
                    logger.warning('DashboardScoringDataView table_exists failed for %s: %s', table_name, ex)
                    return False
            if not table_exists(status_table):
                return JsonResponse({
                    'status': True,
                    'data': {},
                    'scoring_ready': False,
                    'reason': 'Tabel scoring status belum tersedia'
                }, safe=False)
            if include_events and (not table_exists(event_table)):
                return JsonResponse({
                    'status': True,
                    'data': {},
                    'scoring_ready': False,
                    'reason': 'Tabel scoring event belum tersedia'
                }, safe=False)
            def resolve_table_columns(table_name):
                try:
                    schema_df = query_df(f"DESCRIBE TABLE {table_name}")
                    for c in ['name', 'column', 'Field']:
                        if c in schema_df.columns:
                            return set(schema_df[c].astype(str).str.strip().tolist())
                except Exception as ex:
                    logger.warning('DashboardScoringDataView resolve_table_columns failed for %s: %s', table_name, ex)
                return set()
            table_cols = resolve_table_columns(status_table)
            literals = ', '.join("'{}'".format(x.replace("'", "''")) for x in sorted(set(entities)))
            is_country_dim = (dim == 'country')
            entity_key_expr = "upper(country_code)" if is_country_dim else "lower(site)"
            status_filter_expr = f"(upper(country_code) IN ({literals}) OR upper(country_name) IN ({literals}))" if is_country_dim else f"lower(site) IN ({literals})"
            days_active_expr = "toInt32(days_active)" if 'days_active' in table_cols else "toInt32(0)"
            forecast_horizon_expr = "toInt32(forecast_horizon_hours)" if 'forecast_horizon_hours' in table_cols else "toInt32(24)"
            forecast_direction_expr = "toString(forecast_direction)" if 'forecast_direction' in table_cols else "'STABLE_OUTLOOK'"
            forecast_conf_expr = "toFloat64(forecast_confidence)" if 'forecast_confidence' in table_cols else "toFloat64(0)"
            forecast_reason_expr = "toString(forecast_reason)" if 'forecast_reason' in table_cols else "''"
            reco_action_expr = "toString(recommended_action)" if 'recommended_action' in table_cols else "'HOLD'"
            reco_pct_expr = "toFloat64(recommended_budget_change_pct)" if 'recommended_budget_change_pct' in table_cols else "toFloat64(0)"
            reco_target_expr = "toFloat64(recommended_budget_target)" if 'recommended_budget_target' in table_cols else "toFloat64(0)"
            reco_reason_expr = "toString(budget_reco_reason)" if 'budget_reco_reason' in table_cols else "''"
            sql = f"""
            SELECT
                site,
                meta_campaign,
                date AS scoring_date,
                run_time,
                run_hour,
                country_code,
                country_name,
                {entity_key_expr} AS entity_key,
                entity_key AS status_entity_key,
                mapped_revenue_source,
                join_status,
                spend,
                revenue_value,
                final_label,
                root_cause_label,
                health_score,
                adjustment_score,
                ivt_risk_score,
                confidence,
                decision_margin,
                traffic_score,
                delivery_score,
                yield_score,
                quality_score,
                revenue_score,
                efficiency_score,
                engagement_score,
                control_score,
                ivt_click_stress_score,
                ivt_serving_score,
                ivt_attention_score,
                ivt_counter_score,
                ivt_funnel_score,
                positive_signal_count,
                negative_signal_count,
                neutral_signal_count,
                reason_summary,
                {days_active_expr} AS days_active,
                {forecast_horizon_expr} AS forecast_horizon_hours,
                {forecast_direction_expr} AS forecast_direction,
                {forecast_conf_expr} AS forecast_confidence,
                {forecast_reason_expr} AS forecast_reason,
                {reco_action_expr} AS recommended_action,
                {reco_pct_expr} AS recommended_budget_change_pct,
                {reco_target_expr} AS recommended_budget_target,
                {reco_reason_expr} AS budget_reco_reason
            FROM {status_table}
            WHERE toDate(date) = toDate('{target_date}')
              AND {status_filter_expr}
            """
            event_sql = f"""
            SELECT 
            date AS scoring_date,
            run_time,
            run_hour,
            entity_key,
            site,
            meta_campaign,
            country_code, 
            country_name,
            join_status,
            mapped_revenue_source,
            source_scope,
            header_name,
            metric_group,
            metric_type,
            funnel_stage,
            metric_role,
            rule_key,
            score_method,
            expected_direction,
            prev_value,
            cur_value,
            delta_abs,
            delta_pct,
            current_increment,
            baseline_center,
            baseline_scale,
            volume_gate_value,
            volume_gate_pass,
            denominator_value,
            confidence,
            signal_strength,
            health_component,
            adjustment_component,
            change_class,
            event_label,
            event_reason,
            note,
            ivt_component,
            ivt_capacity,
            baseline_source,
            hist_points,
            z_raw,
            directional_z,
            day_type,
            is_composite
            FROM {event_table}
            WHERE toDate(date) = toDate('{target_date}')
              AND lower(site) IN ({literals})
            """
            source_sql = f"""
            SELECT
                site,
                meta_campaign,
                date,
                entity_key,
                country_code,
                argMax(country_name, run_hour) AS country_name,
                argMax(mapped_revenue_source, run_hour) AS mapped_revenue_source,
                argMax(meta_spend, run_hour) AS meta_spend,
                argMax(adx_revenue, run_hour) AS adx_revenue,
                argMax(adsense_estimated_earnings, run_hour) AS adsense_estimated_earnings
            FROM
            (
                SELECT
                    site,
                    lower(meta_campaign) AS meta_campaign,
                    date,
                    lower(entity_key) AS entity_key,
                    upper(country_code) AS country_code,
                    country_name,
                    run_hour,
                    mapped_revenue_source,
                    meta_spend,
                    adx_revenue,
                    adsense_estimated_earnings
                FROM {source_table}
                WHERE toDate(date) = toDate('{target_date}')
                  AND {status_filter_expr}
            )
            GROUP BY
                site,
                date,
                entity_key,
                meta_campaign,
                country_code
            """
            timeline_hour_expr = "toHour(run_time)" if 'run_time' in table_cols else "run_hour"
            timeline_entity_expr = "upper(country_code)" if is_country_dim else "lower(site)"
            timeline_sql = f"""
            SELECT
                {timeline_entity_expr} AS entity_key,
                toString({timeline_hour_expr}) AS run_hour
            FROM {status_table}
            WHERE toDate(date) = toDate('{target_date}')
              AND {status_filter_expr}
            GROUP BY entity_key, run_hour
            ORDER BY entity_key, toInt32OrZero(run_hour) DESC, run_hour DESC
            """
            def load_status_df():
                df = query_df(sql)
                for c in ['scoring_date', 'date']:
                    if c in df.columns:
                        df[c] = pd.to_datetime(df[c], errors='coerce')
                        df = df.dropna(subset=[c])
                        df[c] = df[c].dt.date
                if 'run_time' in df.columns:
                    df['run_time'] = pd.to_datetime(df['run_time'], errors='coerce')
                if 'run_hour' in df.columns:
                    df['run_hour_raw'] = df['run_hour']
                    df['run_hour'] = pd.to_numeric(df['run_hour'], errors='coerce').fillna(0).astype(int)
                return df
            def load_event_df():
                if not include_events:
                    return pd.DataFrame()
                ev = query_df(event_sql)
                if 'run_hour' in ev.columns:
                    ev['run_hour'] = pd.to_numeric(ev['run_hour'], errors='coerce').fillna(0).astype(int)
                return ev
            def load_source_df():
                if not include_source:
                    return pd.DataFrame()
                try:
                    return query_df(source_sql)
                except Exception as ex:
                    logger.warning('DashboardScoringDataView source query failed: %s', ex)
                    return pd.DataFrame()
            def load_timeline_df():
                if not include_timeline:
                    return pd.DataFrame()
                try:
                    tdf = query_df(timeline_sql)
                except Exception as ex:
                    logger.warning('DashboardScoringDataView timeline query failed: %s', ex)
                    return pd.DataFrame()
                if 'entity_key' in tdf.columns:
                    tdf['entity_key'] = tdf['entity_key'].astype(str).map(lambda x: x.strip().upper())
                return tdf
            df = load_status_df()
            event_df = load_event_df()
            source_df = load_source_df()
            timeline_df = load_timeline_df()
            if df.empty:
                return JsonResponse({'status': True, 'data': {}}, safe=False)
            df['entity_key'] = df['entity_key'].astype(str).map(lambda x: x.strip().upper())
            if 'site' in df.columns:
                df['site'] = df['site'].astype(str).map(lambda x: x.strip().upper())
            if 'meta_campaign' in df.columns:
                df['meta_campaign'] = df['meta_campaign'].astype(str).map(lambda x: x.strip().upper())
            if not event_df.empty and 'entity_key' in event_df.columns:
                event_df['entity_key'] = event_df['entity_key'].astype(str).map(lambda x: x.strip().upper())
            df['scoring_date'] = pd.to_datetime(df.get('scoring_date'), errors='coerce').dt.date
            if 'run_hour' in df.columns:
                df['run_hour'] = pd.to_numeric(df.get('run_hour'), errors='coerce').fillna(0).astype(int)
            source_lookup = {}
            source_agg_lookup = {}
            source_site_country_lookup = {}
            source_site_country_campaign_lookup = {}
            source_entity_country_lookup = {}
            def site_key_candidates(raw_key, site_value=''):
                if dim == 'country':
                    k = str(raw_key or '').strip().upper()
                    return [k] if k else []
                keys = []
                for v in [raw_key, site_value]:
                    s = str(v or '').strip().lower()
                    if not s:
                        continue
                    s = s.split('|')[0].strip()
                    s = normalize_site_entity(s)
                    if s:
                        keys.append(s)
                        base = extract_site_base(s)
                        if base:
                            keys.append(base)
                return [k for k in dict.fromkeys(keys) if k]
            def site_country_candidates(raw_key, site_value='', country_code=''):
                base_keys = site_key_candidates(raw_key, site_value)
                cc = str(country_code or '').strip().upper()
                if not cc:
                    return base_keys
                with_cc = [f"{k}|{cc}" for k in base_keys if k]
                return [k for k in dict.fromkeys(with_cc + base_keys) if k]
            if not source_df.empty and 'entity_key' in source_df.columns:
                source_df['entity_key'] = source_df['entity_key'].astype(str).map(lambda x: x.strip().upper())
                if 'site' in source_df.columns:
                    source_df['site'] = source_df['site'].astype(str).map(lambda x: x.strip().upper())
                if 'meta_campaign' in source_df.columns:
                    source_df['meta_campaign'] = source_df['meta_campaign'].astype(str).map(lambda x: x.strip().upper())
                def _src_num(v):
                    try:
                        if pd.isna(v):
                            return 0.0
                    except Exception:
                        pass
                    try:
                        return float(v)
                    except Exception:
                        return 0.0
                def source_row_rank(r):
                    adx = _src_num(r.get('adx_revenue'))
                    ads = _src_num(r.get('adsense_estimated_earnings'))
                    spend = _src_num(r.get('meta_spend'))
                    rev = adx + ads
                    has_src = 1.0 if str(r.get('mapped_revenue_source') or '').strip() else 0.0
                    return (1.0 if rev > 0 else 0.0) * 1_000_000 + rev * 100 + (1.0 if spend > 0 else 0.0) * 10 + has_src
                for _, srow in source_df.iterrows():
                    row_dict = dict(srow)
                    src_key = str(srow.get('entity_key')).strip()
                    src_key_upper = src_key.upper()
                    src_site = str(srow.get('site') or '').strip()
                    src_meta_campaign = str(srow.get('meta_campaign') or '').strip().upper()
                    src_country = str(srow.get('country_code') or srow.get('country_cd') or '').strip().upper()
                    norm_site = normalize_site_entity(src_site)
                    row_rank = source_row_rank(row_dict)
                    if src_key_upper and src_country:
                        sec_key = f"{src_key_upper}|{src_country}"
                        sec = source_entity_country_lookup.get(sec_key) or {
                            'meta_spend': 0.0, 'adx_revenue': 0.0, 'adsense_estimated_earnings': 0.0, 'mapped_revenue_source': ''
                        }
                        sec['meta_spend'] = float(sec.get('meta_spend', 0.0)) + _src_num(srow.get('meta_spend'))
                        sec['adx_revenue'] = float(sec.get('adx_revenue', 0.0)) + _src_num(srow.get('adx_revenue'))
                        sec['adsense_estimated_earnings'] = float(sec.get('adsense_estimated_earnings', 0.0)) + _src_num(srow.get('adsense_estimated_earnings'))
                        if not str(sec.get('mapped_revenue_source') or '').strip():
                            sec['mapped_revenue_source'] = str(srow.get('mapped_revenue_source') or '').strip()
                        source_entity_country_lookup[sec_key] = sec
                    if norm_site and src_country:
                        sc_key = f"{norm_site}|{src_country}"
                        sc = source_site_country_lookup.get(sc_key) or {
                            'site': norm_site, 'meta_campaign': src_meta_campaign, 'country_code': src_country,
                            'meta_spend': 0.0, 'adx_revenue': 0.0, 'adsense_estimated_earnings': 0.0, 'mapped_revenue_source': ''
                        }
                        sc['meta_spend'] = float(sc.get('meta_spend', 0.0)) + _src_num(srow.get('meta_spend'))
                        sc['adx_revenue'] = float(sc.get('adx_revenue', 0.0)) + _src_num(srow.get('adx_revenue'))
                        sc['adsense_estimated_earnings'] = float(sc.get('adsense_estimated_earnings', 0.0)) + _src_num(srow.get('adsense_estimated_earnings'))
                        if not str(sc.get('mapped_revenue_source') or '').strip():
                            sc['mapped_revenue_source'] = str(srow.get('mapped_revenue_source') or '').strip()
                        source_site_country_lookup[sc_key] = sc
                        if src_meta_campaign:
                            scc_key = f"{norm_site}|{src_country}|{src_meta_campaign}"
                            scc = source_site_country_campaign_lookup.get(scc_key) or {
                                'site': norm_site, 'meta_campaign': src_meta_campaign, 'country_code': src_country,
                                'meta_spend': 0.0, 'adx_revenue': 0.0, 'adsense_estimated_earnings': 0.0, 'mapped_revenue_source': ''
                            }
                            scc['meta_spend'] = float(scc.get('meta_spend', 0.0)) + _src_num(srow.get('meta_spend'))
                            scc['adx_revenue'] = float(scc.get('adx_revenue', 0.0)) + _src_num(srow.get('adx_revenue'))
                            scc['adsense_estimated_earnings'] = float(scc.get('adsense_estimated_earnings', 0.0)) + _src_num(srow.get('adsense_estimated_earnings'))
                            if not str(scc.get('mapped_revenue_source') or '').strip():
                                scc['mapped_revenue_source'] = str(srow.get('mapped_revenue_source') or '').strip()
                            source_site_country_campaign_lookup[scc_key] = scc
                    cands = site_country_candidates(src_key, src_site, src_country)
                    for cand in cands:
                        if not cand:
                            continue
                        existing = source_lookup.get(cand)
                        if (existing is None) or (row_rank > source_row_rank(existing)):
                            source_lookup[cand] = row_dict
                        agg = source_agg_lookup.get(cand) or {
                            'meta_spend': 0.0, 'adx_revenue': 0.0, 'adsense_estimated_earnings': 0.0, 'mapped_revenue_source': ''
                        }
                        agg['meta_spend'] = float(agg.get('meta_spend', 0.0)) + _src_num(srow.get('meta_spend'))
                        agg['adx_revenue'] = float(agg.get('adx_revenue', 0.0)) + _src_num(srow.get('adx_revenue'))
                        agg['adsense_estimated_earnings'] = float(agg.get('adsense_estimated_earnings', 0.0)) + _src_num(srow.get('adsense_estimated_earnings'))
                        if not str(agg.get('mapped_revenue_source') or '').strip():
                            agg['mapped_revenue_source'] = str(srow.get('mapped_revenue_source') or '').strip()
                        source_agg_lookup[cand] = agg

            timeline_map = {}
            if not timeline_df.empty and 'entity_key' in timeline_df.columns and 'run_hour' in timeline_df.columns:
                for ekey, tpart in timeline_df.groupby('entity_key', sort=False):
                    hs = tpart['run_hour'].astype(str).str.strip()
                    hs = hs[(hs != '') & (hs.str.lower() != 'nan')]
                    hs = hs.map(lambda x: f"{int(float(x)):02d}" if str(x).replace('.', '', 1).isdigit() else str(x))
                    if not hs.empty:
                        nums = pd.to_numeric(hs, errors='coerce')
                        if nums.notna().any():
                            ordered = hs.to_frame('h').assign(n=nums).sort_values(['n', 'h'], ascending=[False, False])
                            timeline_map[str(ekey).strip().upper()] = ordered['h'].drop_duplicates().tolist()
                        else:
                            timeline_map[str(ekey).strip().upper()] = hs.drop_duplicates().tolist()

            out = {}
            def avg(frame, col, weighted=False):
                if col not in frame.columns:
                    return 0.0

                def to_num(v):
                    if pd.isna(v):
                        return np.nan
                    if isinstance(v, str):
                        s = v.strip().replace('%', '')
                        if s.count(',') == 1 and s.count('.') == 0:
                            s = s.replace(',', '.')
                        else:
                            s = s.replace(',', '')
                        try:
                            return float(s)
                        except Exception:
                            return np.nan
                    try:
                        return float(v)
                    except Exception:
                        return np.nan

                vals = frame[col].map(to_num).dropna()
                if not len(vals):
                    return 0.0
                if weighted and 'signal_total' in frame.columns:
                    w = pd.to_numeric(frame['signal_total'], errors='coerce').fillna(0.0).loc[vals.index]
                    den = float(w.sum())
                    if den > 0:
                        return float((vals * w).sum() / den)
                return float(vals.mean())
            def dominant_text(frame, col, default=''):
                try:
                    if frame is None or frame.empty or col not in frame.columns:
                        return default
                    vals = frame[col].astype(str).str.strip()
                    vals = vals[(vals != '') & (vals.str.lower() != 'nan')]
                    if vals.empty:
                        return default
                    return str(vals.value_counts().index[0])
                except Exception:
                    return default
            for entity_key, part in df.groupby('entity_key', sort=False):
                # Ensure signal_total is available in part for weighted averages
                for c in ['positive_signal_count', 'negative_signal_count', 'neutral_signal_count']:
                    if c not in part.columns:
                        part[c] = 0
                part['signal_total'] = pd.to_numeric(part['positive_signal_count'], errors='coerce').fillna(0) + \
                                       pd.to_numeric(part['negative_signal_count'], errors='coerce').fillna(0) + \
                                       pd.to_numeric(part['neutral_signal_count'], errors='coerce').fillna(0)

                # Score domain selalu dihitung dari seluruh campaign (termasuk yang masih LEARNING)
                # agar agregasi tidak bias ke campaign non-learning saja.
                part_eval = part.copy()

                snap = part_eval.copy()  
                if 'country_code' in snap.columns and 'run_hour' in snap.columns:
                    snap_keys = ['country_code']
                    if 'meta_campaign' in snap.columns:
                        snap_keys.append('meta_campaign')
                    sort_cols = list(snap_keys)
                    if 'scoring_date' in snap.columns:
                        sort_cols.append('scoring_date')
                    elif 'date' in snap.columns:
                        sort_cols.append('date')
                    if 'run_time' in snap.columns:
                        sort_cols.append('run_time')
                    sort_cols.append('run_hour')
                    snap = snap.sort_values(sort_cols).drop_duplicates(subset=snap_keys, keep='last')
                if target_date_obj is not None and 'scoring_date' in snap.columns:
                    snap_on_target = snap[snap['scoring_date'].eq(target_date_obj)].copy()
                    if not snap_on_target.empty:
                        snap = snap_on_target

                join_status_series = snap['join_status'] if 'join_status' in snap.columns else pd.Series('', index=snap.index)
                join_status_clean = join_status_series.astype(str).str.upper().str.strip()
                join_status_summary = ', '.join([f"{k}:{v}" for k, v in join_status_clean.value_counts().to_dict().items() if str(k).strip()])
                
                # Averaged metrics from campaign aktif non-learning (fallback: semua campaign)
                health = avg(part_eval, 'health_score', weighted=True)
                risk = avg(part_eval, 'ivt_risk_score', weighted=True)
                adj = avg(part_eval, 'adjustment_score', weighted=True)
                dm = avg(part_eval, 'decision_margin', weighted=True)
                conf_raw = avg(part_eval, 'confidence', weighted=True)
                conf = normalize_conf(conf_raw)

                labels = []
                for c in ['final_label', 'root_cause_label']:
                    if c in snap.columns:
                        labels.extend([str(x).strip().upper() for x in snap[c].tolist() if str(x).strip()])
                label = pd.Series(labels).value_counts().index[0] if labels else 'STABLE'

                # Totals from evaluation frame + continuity guard from full frame
                total_pos = int(pd.to_numeric(part_eval['positive_signal_count'], errors='coerce').fillna(0).sum()) if 'positive_signal_count' in part_eval.columns else 0
                total_neg = int(pd.to_numeric(part_eval['negative_signal_count'], errors='coerce').fillna(0).sum()) if 'negative_signal_count' in part_eval.columns else 0
                total_neu = int(pd.to_numeric(part_eval['neutral_signal_count'], errors='coerce').fillna(0).sum()) if 'neutral_signal_count' in part_eval.columns else 0
                spend_total = float(pd.to_numeric(part_eval['spend'], errors='coerce').fillna(0).sum()) if 'spend' in part_eval.columns else 0.0
                revenue_total = float(pd.to_numeric(part_eval['revenue_value'], errors='coerce').fillna(0).sum()) if 'revenue_value' in part_eval.columns else 0.0
                roi_total = ((revenue_total - spend_total) / spend_total) if spend_total > 0 else 0.0
                profit_strong = (revenue_total > spend_total) and (roi_total >= 0.30)
                full_spend_total = float(pd.to_numeric(part['spend'], errors='coerce').fillna(0).sum()) if 'spend' in part.columns else spend_total
                full_revenue_total = float(pd.to_numeric(part['revenue_value'], errors='coerce').fillna(0).sum()) if 'revenue_value' in part.columns else revenue_total
                continuity_running = (full_spend_total > 0) or (full_revenue_total > 0)
                has_signal = (total_pos + total_neg + total_neu) > 0
                has_metric_surface = any([abs(safe_float(health)) > 0.0, abs(safe_float(risk)) > 0.0, abs(safe_float(adj)) > 0.0, abs(safe_float(dm)) > 0.0])
                has_scoring = (has_signal and conf >= 0.05) or (continuity_running and has_metric_surface and conf >= 0.03)
                traffic_now = avg(part_eval, 'traffic_score', weighted=True)
                delivery_now = avg(part_eval, 'delivery_score', weighted=True)
                yield_now = avg(part_eval, 'yield_score', weighted=True)
                revenue_now = avg(part_eval, 'revenue_score', weighted=True)
                anomaly_cards = derive_anomaly_cards(traffic_now, delivery_now, yield_now, revenue_now, risk, adj)
                days_series = part_eval['scoring_date'] if 'scoring_date' in part_eval.columns else (part_eval['date'] if 'date' in part_eval.columns else pd.Series([], dtype=object))
                days_hist = int(pd.to_datetime(days_series, errors='coerce').dropna().dt.date.nunique())
                days_flag = int(pd.to_numeric(part_eval.get('days_active', pd.Series([], dtype=float)), errors='coerce').fillna(0).max()) if 'days_active' in part_eval.columns else 0
                active_days_effective = max(days_hist, days_flag)
                maturity_profile = campaign_maturity_profile(part)
                mature_campaign_ratio = float(maturity_profile.get('mature_campaign_ratio', 0.0))
                mature_spend_share = float(maturity_profile.get('mature_spend_share', 0.0))
                mature_campaign_count = int(maturity_profile.get('mature_campaign_count', 0))
                campaign_count = int(maturity_profile.get('campaign_count', 0))
                source_mode = 'BLENDED'
                if 'mapped_revenue_source' in snap.columns:
                    src_vals = snap['mapped_revenue_source'].astype(str).str.strip().str.upper()
                    src_vals = src_vals[src_vals.ne('')]
                    if not src_vals.empty:
                        source_mode = str(src_vals.value_counts().index[0])
                score = None
                decision = ""
                continuity_mature = continuity_running and ((mature_campaign_count > 0) or (mature_campaign_ratio >= 0.35) or (mature_spend_share >= 0.35) or (full_revenue_total > full_spend_total and full_spend_total > 0))
                learning_gate = (active_days_effective < 2) and (mature_campaign_count <= 0) and (mature_campaign_ratio < 0.60) and (mature_spend_share < 0.55) and (not continuity_mature)
                if learning_gate:
                    label = "LEARNING"
                    decision = "LEARNING"
                elif has_scoring:
                    if label == "LEARNING" and (mature_campaign_count > 0 or mature_campaign_ratio >= 0.35 or mature_spend_share >= 0.35 or continuity_mature):
                        label = "STABLE"
                    score, decision = derive_score_decision(health, risk, adj, conf, dm, label, profit_strong, anomaly_cards, roi_total, source_mode)
                else:
                    label = "STABLE" if continuity_mature else "DATA_INCOMPLETE"
                    decision = "HOLD" if continuity_mature else decision
                reason_parts = [str(x).strip() for x in snap.get('reason_summary', pd.Series([], dtype=str)).tolist() if str(x).strip()]
                reason_summary = ' | '.join(list(dict.fromkeys(reason_parts))[:3])

                scoring_timeline = []
                def _hour_key(v):
                    s = str(v or '').strip()
                    if not s or s.lower() == 'nan':
                        return None
                    try:
                        h = int(float(s))
                        if 0 <= h <= 23:
                            return f"{h:02d}"
                    except Exception:
                        pass
                    dt = pd.to_datetime(s, errors='coerce')
                    if pd.notna(dt):
                        return f"{int(dt.hour):02d}"
                    return None
                def _fmt_run_hour_label(v):
                    k = _hour_key(v)
                    return f"{k}:00" if k else ''

                base_hour_series = part['run_hour_raw'] if 'run_hour_raw' in part.columns else part.get('run_hour')
                part_hour_key = pd.Series([None] * len(part), index=part.index, dtype=object)
                if base_hour_series is not None:
                    part_hour_key = base_hour_series.map(_hour_key)
                if 'run_time' in part.columns:
                    rt_key = part['run_time'].map(_hour_key)
                    part_hour_key = part_hour_key.where(part_hour_key.notna(), rt_key)

                run_hours = []
                map_hours = list(timeline_map.get(str(entity_key).strip().upper(), []) or [])
                run_hours.extend([_hour_key(x) for x in map_hours if _hour_key(x) is not None])
                run_hours.extend([x for x in part_hour_key.dropna().astype(str).tolist() if x])
                run_hours = sorted(list(dict.fromkeys(run_hours)), reverse=True)

                if run_hours:
                    for rh in run_hours[:8]:
                        snap_h = part.loc[part_hour_key.astype(str).eq(str(rh))].copy()
                        if target_date_obj is not None and 'scoring_date' in snap_h.columns:
                            snap_h_target = snap_h[snap_h['scoring_date'].eq(target_date_obj)].copy()
                            if not snap_h_target.empty:
                                snap_h = snap_h_target
                        if 'country_code' in snap_h.columns and 'run_hour' in snap_h.columns:
                            sort_cols_h = ['country_code']
                            if 'scoring_date' in snap_h.columns:
                                sort_cols_h.append('scoring_date')
                            if 'run_time' in snap_h.columns:
                                sort_cols_h.append('run_time')
                            sort_cols_h.append('run_hour')
                            snap_h = snap_h.sort_values(sort_cols_h).drop_duplicates(subset=['country_code'], keep='last')
                        for c in ['positive_signal_count', 'negative_signal_count', 'neutral_signal_count']:
                            if c not in snap_h.columns:
                                snap_h[c] = 0
                        snap_h['signal_total'] = pd.to_numeric(snap_h['positive_signal_count'], errors='coerce').fillna(0) + pd.to_numeric(snap_h['negative_signal_count'], errors='coerce').fillna(0) + pd.to_numeric(snap_h['neutral_signal_count'], errors='coerce').fillna(0)
                        h = avg(snap_h, 'health_score', weighted=True)
                        r = avg(snap_h, 'ivt_risk_score', weighted=True)
                        a = avg(snap_h, 'adjustment_score', weighted=True)
                        m = avg(snap_h, 'decision_margin', weighted=True)
                        c_raw = avg(snap_h, 'confidence', weighted=True)
                        c01 = normalize_conf(c_raw)
                        lbl_vals = []
                        for c in ['final_label', 'root_cause_label']:
                            if c in snap_h.columns:
                                lbl_vals.extend([str(x).strip().upper() for x in snap_h[c].tolist() if str(x).strip()])
                        lbl = pd.Series(lbl_vals).value_counts().index[0] if lbl_vals else 'STABLE'
                        pos_h = int(pd.to_numeric(snap_h['positive_signal_count'], errors='coerce').fillna(0).sum()) if 'positive_signal_count' in snap_h.columns else 0
                        neg_h = int(pd.to_numeric(snap_h['negative_signal_count'], errors='coerce').fillna(0).sum()) if 'negative_signal_count' in snap_h.columns else 0
                        neu_h = int(pd.to_numeric(snap_h['neutral_signal_count'], errors='coerce').fillna(0).sum()) if 'neutral_signal_count' in snap_h.columns else 0
                        spend_h = float(pd.to_numeric(snap_h['spend'], errors='coerce').fillna(0).sum()) if 'spend' in snap_h.columns else 0.0
                        revenue_h = float(pd.to_numeric(snap_h['revenue_value'], errors='coerce').fillna(0).sum()) if 'revenue_value' in snap_h.columns else 0.0
                        roi_h = ((revenue_h - spend_h) / spend_h) if spend_h > 0 else 0.0
                        profit_strong_h = (revenue_h > spend_h) and (roi_h >= 0.30)
                        continuity_running_h = (spend_h > 0) or (revenue_h > 0)
                        has_signal_h = (pos_h + neg_h + neu_h) > 0
                        has_metric_surface_h = any([abs(safe_float(h)) > 0.0, abs(safe_float(r)) > 0.0, abs(safe_float(a)) > 0.0, abs(safe_float(m)) > 0.0])
                        has_scoring_h = (has_signal_h and c01 >= 0.05) or (continuity_running_h and has_metric_surface_h and c01 >= 0.03)
                        days_series_h = snap_h['scoring_date'] if 'scoring_date' in snap_h.columns else (snap_h['date'] if 'date' in snap_h.columns else pd.Series([], dtype=object))
                        days_hist_h = int(pd.to_datetime(days_series_h, errors='coerce').dropna().dt.date.nunique())
                        days_flag_h = int(pd.to_numeric(snap_h.get('days_active', pd.Series([], dtype=float)), errors='coerce').fillna(0).max()) if 'days_active' in snap_h.columns else 0
                        active_days_effective_h = max(days_hist_h, days_flag_h)
                        maturity_profile_h = campaign_maturity_profile(part)
                        mature_campaign_ratio_h = float(maturity_profile_h.get('mature_campaign_ratio', 0.0))
                        mature_spend_share_h = float(maturity_profile_h.get('mature_spend_share', 0.0))
                        mature_campaign_count_h = int(maturity_profile_h.get('mature_campaign_count', 0))
                        sc = None
                        dec = ''
                        continuity_mature_h = continuity_running_h and ((mature_campaign_count_h > 0) or (mature_campaign_ratio_h >= 0.35) or (mature_spend_share_h >= 0.35) or (revenue_h > spend_h and spend_h > 0))
                        learning_gate_h = (active_days_effective_h < 2) and (mature_campaign_count_h <= 0) and (mature_campaign_ratio_h < 0.60) and (mature_spend_share_h < 0.55) and (not continuity_mature_h)
                        if learning_gate_h:
                            lbl = 'LEARNING'
                            dec = 'LEARNING'
                        elif has_scoring_h:
                            if lbl == 'LEARNING' and (mature_campaign_count_h > 0 or mature_campaign_ratio_h >= 0.35 or mature_spend_share_h >= 0.35 or continuity_mature_h):
                                lbl = 'STABLE'
                            traffic_h = avg(snap_h, 'traffic_score', weighted=True)
                            delivery_h = avg(snap_h, 'delivery_score', weighted=True)
                            yield_h = avg(snap_h, 'yield_score', weighted=True)
                            revenue_hs = avg(snap_h, 'revenue_score', weighted=True)
                            anomaly_cards_h = derive_anomaly_cards(traffic_h, delivery_h, yield_h, revenue_hs, r, a)
                            source_mode_h = source_mode
                            if 'mapped_revenue_source' in snap_h.columns:
                                src_vals_h = snap_h['mapped_revenue_source'].astype(str).str.strip().str.upper()
                                src_vals_h = src_vals_h[src_vals_h.ne('')]
                                if not src_vals_h.empty:
                                    source_mode_h = str(src_vals_h.value_counts().index[0])
                            sc, dec = derive_score_decision(h, r, a, c01, m, lbl, profit_strong_h, anomaly_cards_h, roi_h, source_mode_h)
                        else:
                            lbl = 'STABLE' if continuity_mature_h else 'DATA_INCOMPLETE'
                            dec = 'HOLD' if continuity_mature_h else dec

                        reason_h_parts = [str(x).strip() for x in snap_h.get('reason_summary', pd.Series([], dtype=str)).tolist() if str(x).strip()]
                        reason_h_summary = ' | '.join(list(dict.fromkeys(reason_h_parts))[:3])
                        country_h = []
                        for _, crow in snap_h.iterrows():
                            country_h.append({
                                'country_code': str(crow.get('country_code') or ''),
                                'country_name': str(crow.get('country_name') or ''),
                                'meta_campaign': str(crow.get('meta_campaign') or ''),
                                'days_active': int(safe_float(crow.get('days_active'))),
                                'health_score': safe_float(crow.get('health_score')),
                                'ivt_risk_score': safe_float(crow.get('ivt_risk_score')),
                                'adjustment_score': safe_float(crow.get('adjustment_score')),
                                'confidence': clip((safe_float(crow.get('confidence')) / 100.0) if safe_float(crow.get('confidence')) > 1.0 else safe_float(crow.get('confidence')), 0.0, 1.0),
                                'decision_margin': safe_float(crow.get('decision_margin')),
                                'final_label': str(crow.get('final_label', '')),
                                'join_status': str(crow.get('join_status', '')),
                                'positive_signals': int(safe_float(crow.get('positive_signal_count'))),
                                'negative_signals': int(safe_float(crow.get('negative_signal_count'))),
                                'neutral_signals': int(safe_float(crow.get('neutral_signal_count'))),
                            })

                        dec_final_h, campaign_guard_h = apply_campaign_consistency_guard(dec, country_h)
                        scoring_timeline.append({
                            'run_hour': str(rh),
                            'run_hour_label': _fmt_run_hour_label(rh),
                            'run_time': '',
                            'snapshot_time': '',
                            'score': sc,
                            'decision': dec_final_h,
                            'label': lbl,
                            'health_score': h,
                            'ivt_risk_score': r,
                            'adjustment_score': a,
                            'confidence': c01,
                            'decision_margin': m,
                            'reason_summary': reason_h_summary,
                            'reasons': [x for x in [reason_h_summary, lbl] if x],
                            'positive_signal_count': pos_h,
                            'negative_signal_count': neg_h,
                            'neutral_signal_count': neu_h,
                            'country_details': country_h,
                            'days_active': int(active_days_effective_h),
                            'forecast_horizon_hours': int(safe_float(avg(snap_h, 'forecast_horizon_hours', weighted=False)) or 24),
                            'forecast_direction': dominant_text(snap_h, 'forecast_direction', 'STABLE_OUTLOOK'),
                            'forecast_confidence': float(round(avg(snap_h, 'forecast_confidence', weighted=True), 4)),
                            'forecast_reason': dominant_text(snap_h, 'forecast_reason', ''),
                            'recommended_action': dominant_text(snap_h, 'recommended_action', 'HOLD'),
                            'recommended_budget_change_pct': float(round(avg(snap_h, 'recommended_budget_change_pct', weighted=True), 2)),
                            'recommended_budget_target': float(round(spend_h * (1.0 + (avg(snap_h, 'recommended_budget_change_pct', weighted=True) / 100.0)), 2)) if spend_h > 0 else 0.0,
                            'budget_reco_reason': dominant_text(snap_h, 'budget_reco_reason', ''),
                            'campaign_stop_ratio': float(campaign_guard_h.get('stop_ratio', 0.0)),
                            'campaign_majority_action': str(campaign_guard_h.get('majority_action', 'HOLD')),
                        })

                latest_run_time = snap['run_time'].dropna().max() if 'run_time' in snap.columns else None
                latest_run_hour = snap['run_hour'].dropna().max() if 'run_hour' in snap.columns else None
                latest_snapshot = latest_run_time if pd.notna(latest_run_time) else latest_run_hour
                run_hour = pd.to_datetime(latest_run_hour, errors='coerce').strftime('%Y-%m-%d %H:%M:%S') if pd.notna(latest_run_hour) else ''
                update_score_time = pd.to_datetime(latest_snapshot, errors='coerce').strftime('%Y-%m-%d %H:%M:%S') if pd.notna(latest_snapshot) else ''

                ev = event_df[event_df['entity_key'].eq(entity_key)].copy() if (include_events and not event_df.empty and 'entity_key' in event_df.columns) else pd.DataFrame()
                if (risk <= 0.0) and (not ev.empty) and ('ivt_component' in ev.columns) and ('ivt_capacity' in ev.columns):
                    ivt_num = pd.to_numeric(ev['ivt_component'], errors='coerce').fillna(0.0).sum()
                    ivt_den = pd.to_numeric(ev['ivt_capacity'], errors='coerce').fillna(0.0).sum()
                    if ivt_den > 0:
                        risk = clip(float(100.0 * ivt_num / ivt_den), 0.0, 100.0)
                if (adj == 0.0) and (not ev.empty) and ('adjustment_component' in ev.columns):
                    adj = float(pd.to_numeric(ev['adjustment_component'], errors='coerce').fillna(0.0).sum())
                if (health == 0.0) and (not ev.empty) and ('health_component' in ev.columns):
                    health = clip(float(50.0 + pd.to_numeric(ev['health_component'], errors='coerce').fillna(0.0).sum()), 0.0, 100.0)
                if (conf <= 0.0) and (not ev.empty) and ('confidence' in ev.columns):
                    conf_evt = float(pd.to_numeric(ev['confidence'], errors='coerce').dropna().mean()) if len(ev.index) else 0.0
                    conf = clip((conf_evt / 100.0) if conf_evt > 1.0 else conf_evt, 0.0, 1.0)
                if dm == 0.0 and ((health != 0.0) or (risk != 0.0) or (adj != 0.0)):
                    dm = float(health - risk + min(0.0, adj))

                dominant_event_label = ''
                if not ev.empty and 'event_label' in ev.columns:
                    s = ev['event_label'].astype(str).str.strip()
                    s = s[s != '']
                    if not s.empty:
                        dominant_event_label = s.value_counts().index[0]
                
                country_details = []
                detail_snap = part.copy()
                if target_date_obj is not None and 'scoring_date' in detail_snap.columns:
                    detail_target = detail_snap[detail_snap['scoring_date'].eq(target_date_obj)].copy()
                    if not detail_target.empty:
                        detail_snap = detail_target
                if 'country_code' in detail_snap.columns and 'run_hour' in detail_snap.columns:
                    detail_keys = ['country_code']
                    if 'meta_campaign' in detail_snap.columns:
                        detail_keys.append('meta_campaign')
                    detail_sort = list(detail_keys)
                    if 'scoring_date' in detail_snap.columns:
                        detail_sort.append('scoring_date')
                    if 'run_time' in detail_snap.columns:
                        detail_sort.append('run_time')
                    detail_sort.append('run_hour')
                    detail_snap = detail_snap.sort_values(detail_sort).drop_duplicates(subset=detail_keys, keep='last')
                for _, row in detail_snap.iterrows():
                    row_entity_key = str(row.get('status_entity_key') or row.get('site') or row.get('entity_key') or '').strip()
                    row_site_key = str(row.get('site') or '').strip()
                    row_meta_campaign = str(row.get('meta_campaign') or '').strip().upper()
                    src = {}
                    matched_cand = ''
                    row_country = str(row.get('country_code') or '').strip().upper()
                    for cand in site_country_candidates(row_entity_key, row_site_key, row_country):
                        if cand in source_lookup:
                            src = dict(source_lookup[cand])
                            matched_cand = cand
                            break
                    row_norm_site = normalize_site_entity(row_site_key)
                    row_country = str(row.get('country_code') or '').strip().upper()
                    campaign_src_applied = False
                    row_entity_upper = str(row_entity_key or '').strip().upper()

                    # Prioritas 1: site+country+campaign (paling spesifik)
                    if row_norm_site and row_country and row_meta_campaign:
                        scc_key = f"{row_norm_site}|{row_country}|{row_meta_campaign}"
                        scc_src = source_site_country_campaign_lookup.get(scc_key)
                        if scc_src:
                            src['meta_spend'] = scc_src.get('meta_spend', src.get('meta_spend'))
                            src['adx_revenue'] = scc_src.get('adx_revenue', src.get('adx_revenue'))
                            src['adsense_estimated_earnings'] = scc_src.get('adsense_estimated_earnings', src.get('adsense_estimated_earnings'))
                            src['mapped_revenue_source'] = scc_src.get('mapped_revenue_source', src.get('mapped_revenue_source'))
                            campaign_src_applied = True

                    # Prioritas 2: site+country
                    if (not campaign_src_applied) and row_norm_site and row_country:
                        sc_key = f"{row_norm_site}|{row_country}"
                        sc_src = source_site_country_lookup.get(sc_key)
                        if sc_src:
                            src['meta_spend'] = sc_src.get('meta_spend', src.get('meta_spend'))
                            src['adx_revenue'] = sc_src.get('adx_revenue', src.get('adx_revenue'))
                            src['adsense_estimated_earnings'] = sc_src.get('adsense_estimated_earnings', src.get('adsense_estimated_earnings'))
                            if not str(src.get('mapped_revenue_source') or '').strip():
                                src['mapped_revenue_source'] = sc_src.get('mapped_revenue_source', src.get('mapped_revenue_source'))
                            campaign_src_applied = True

                    # Prioritas 3: entity+country (lebih agregat)
                    if (not campaign_src_applied) and row_entity_upper and row_country:
                        sec_key = f"{row_entity_upper}|{row_country}"
                        sec_src = source_entity_country_lookup.get(sec_key)
                        if sec_src:
                            src['meta_spend'] = sec_src.get('meta_spend', src.get('meta_spend'))
                            src['adx_revenue'] = sec_src.get('adx_revenue', src.get('adx_revenue'))
                            src['adsense_estimated_earnings'] = sec_src.get('adsense_estimated_earnings', src.get('adsense_estimated_earnings'))
                            src['mapped_revenue_source'] = sec_src.get('mapped_revenue_source', src.get('mapped_revenue_source'))
                            campaign_src_applied = True

                    # Prioritas 4: fallback agregat kandidat
                    if (not campaign_src_applied) and matched_cand and matched_cand in source_agg_lookup:
                        agg_src = source_agg_lookup.get(matched_cand) or {}
                        src['meta_spend'] = agg_src.get('meta_spend', src.get('meta_spend'))
                        src['adx_revenue'] = agg_src.get('adx_revenue', src.get('adx_revenue'))
                        src['adsense_estimated_earnings'] = agg_src.get('adsense_estimated_earnings', src.get('adsense_estimated_earnings'))
                        if not str(src.get('mapped_revenue_source') or '').strip():
                            src['mapped_revenue_source'] = agg_src.get('mapped_revenue_source', src.get('mapped_revenue_source'))
                    def nullable_float(v):
                        if v is None:
                            return None
                        if isinstance(v, str):
                            s = v.strip()
                            if s == '':
                                return None
                            s = s.replace('%', '')
                            if s.count(',') == 1 and s.count('.') == 0:
                                s = s.replace(',', '.')
                            else:
                                s = s.replace(',', '')
                            try:
                                return float(s)
                            except Exception:
                                return None
                        try:
                            if pd.isna(v):
                                return None
                        except Exception:
                            pass
                        try:
                            return float(v)
                        except Exception:
                            return None

                    # Prioritaskan spend dari source table agar tidak undercount saat status row masih parsial
                    status_spend_opt = nullable_float(row.get('spend'))
                    status_meta_spend_opt = nullable_float(row.get('meta_spend'))
                    source_spend_opt = nullable_float(src.get('meta_spend'))

                    if source_spend_opt is not None:
                        meta_spend_opt = source_spend_opt
                        spend_source = 'source_meta_spend'
                    elif status_spend_opt is not None:
                        meta_spend_opt = status_spend_opt
                        spend_source = 'status_spend'
                    elif status_meta_spend_opt is not None:
                        meta_spend_opt = status_meta_spend_opt
                        spend_source = 'status_meta_spend'
                    else:
                        meta_spend_opt = None
                        spend_source = 'none'

                    meta_spend_v = safe_float(meta_spend_opt)
                    status_spend_v = safe_float(status_spend_opt)
                    status_meta_spend_v = safe_float(status_meta_spend_opt)
                    source_spend_v = safe_float(source_spend_opt)

                    adx_revenue_opt = nullable_float(src.get('adx_revenue'))
                    if adx_revenue_opt is None:
                        adx_revenue_opt = nullable_float(row.get('adx_revenue'))
                    adsense_revenue_opt = nullable_float(src.get('adsense_estimated_earnings'))
                    if adsense_revenue_opt is None:
                        adsense_revenue_opt = nullable_float(row.get('adsense_estimated_earnings'))

                    adx_revenue_v = safe_float(adx_revenue_opt)
                    adsense_estimated_earnings_v = safe_float(adsense_revenue_opt)
                    mapped_revenue_source_v = str(src.get('mapped_revenue_source') or row.get('mapped_revenue_source') or '').strip().upper()
                    row_revenue_opt = nullable_float(row.get('revenue_value'))
                    if row_revenue_opt is None:
                        row_revenue_opt = nullable_float(row.get('revenue'))
                    sum_channel_revenue_v = safe_float(adx_revenue_opt) + safe_float(adsense_revenue_opt)
                    row_revenue_v = safe_float(row_revenue_opt)
                    if mapped_revenue_source_v == 'ADX_ONLY':
                        adx_v = safe_float(adx_revenue_opt)
                        revenue_v = adx_v if adx_v > 0 else row_revenue_v
                    elif mapped_revenue_source_v == 'ADSENSE_ONLY':
                        ads_v = safe_float(adsense_revenue_opt)
                        revenue_v = ads_v if ads_v > 0 else row_revenue_v
                    else:
                        revenue_v = sum_channel_revenue_v if sum_channel_revenue_v > 0 else row_revenue_v
                    roi_v = ((revenue_v - meta_spend_v) / meta_spend_v * 100.0) if meta_spend_v > 0 else 0.0
                    country_details.append({
                        'country_code': str(row.get('country_code') or ''),
                        'country_name': str(row.get('country_name') or ''),
                        'meta_campaign': row_meta_campaign,
                        'days_active': int(safe_float(row.get('days_active'))),
                        'health_score': safe_float(row.get('health_score')),
                        'ivt_risk_score': safe_float(row.get('ivt_risk_score')),
                        'adjustment_score': safe_float(row.get('adjustment_score')),
                        'confidence': clip((safe_float(row.get('confidence')) / 100.0) if safe_float(row.get('confidence')) > 1.0 else safe_float(row.get('confidence')), 0.0, 1.0),
                        'decision_margin': safe_float(row.get('decision_margin')),
                        'final_label': str(row.get('final_label', '')),
                        'join_status': str(row.get('join_status', '')),
                        'mapped_revenue_source': mapped_revenue_source_v,
                        'adx_revenue': adx_revenue_v,
                        'adsense_estimated_earnings': adsense_estimated_earnings_v,
                        'spend': meta_spend_v,
                        'spend_source': spend_source,
                        'spend_status_raw': status_spend_v,
                        'spend_status_meta': status_meta_spend_v,
                        'spend_source_meta': source_spend_v,
                        'revenue': revenue_v,
                        'roi': roi_v,
                        'positive_signals': int(safe_float(row.get('positive_signal_count'))),
                        'negative_signals': int(safe_float(row.get('negative_signal_count'))),
                        'neutral_signals': int(safe_float(row.get('neutral_signal_count'))),
                    })
                decision_final, campaign_guard = apply_campaign_consistency_guard(decision, country_details)
                out[entity_key] = {
                    'score': score,
                    'decision': decision_final,
                    'label': label,
                    'reasons': [x for x in [reason_summary, dominant_event_label, label] if x],
                    'breakdown': {'meta': {
                        'join_status_summary': join_status_summary,
                        'reason_summary': reason_summary,
                        'run_hour': run_hour,
                        'run_time': update_score_time,
                        'snapshot_time': update_score_time,
                        'last_update': update_score_time,
                        'final_label': label,
                        'root_cause_label': label,
                        'health_score': health,
                        'ivt_risk_score': risk,
                        'adjustment_score': adj,
                        'confidence': conf,
                        'decision_margin': dm,
                        'days_active': int(active_days_effective),
                        'learning_gate': bool(learning_gate),
                        'campaign_count': int(campaign_count),
                        'mature_campaign_count': int(mature_campaign_count),
                        'mature_campaign_ratio': float(round(mature_campaign_ratio, 4)),
                        'mature_spend_share': float(round(mature_spend_share, 4)),
                        'campaign_total': int(campaign_guard.get('campaign_total', 0)),
                        'campaign_stop_ratio': float(campaign_guard.get('stop_ratio', 0.0)),
                        'campaign_pause_ratio': float(campaign_guard.get('pause_ratio', 0.0)),
                        'campaign_majority_action': str(campaign_guard.get('majority_action', 'HOLD')),
                        'forecast_horizon_hours': int(safe_float(avg(snap, 'forecast_horizon_hours', weighted=False)) or 24),
                        'forecast_direction': dominant_text(snap, 'forecast_direction', 'STABLE_OUTLOOK'),
                        'forecast_confidence': float(round(avg(snap, 'forecast_confidence', weighted=True), 4)),
                        'forecast_reason': dominant_text(snap, 'forecast_reason', ''),
                        'recommended_action': dominant_text(snap, 'recommended_action', 'HOLD'),
                        'recommended_budget_change_pct': float(round(avg(snap, 'recommended_budget_change_pct', weighted=True), 2)),
                        'recommended_budget_target': float(round(spend_total * (1.0 + (avg(snap, 'recommended_budget_change_pct', weighted=True) / 100.0)), 2)) if spend_total > 0 else 0.0,
                        'budget_reco_reason': dominant_text(snap, 'budget_reco_reason', ''),
                        'anomaly_cards': anomaly_cards,
                        'traffic_score': avg(snap, 'traffic_score', weighted=True),
                        'delivery_score': avg(snap, 'delivery_score', weighted=True),
                        'yield_score': avg(snap, 'yield_score', weighted=True),
                        'quality_score': avg(snap, 'quality_score', weighted=True),
                        'revenue_score': avg(snap, 'revenue_score', weighted=True),
                        'efficiency_score': avg(snap, 'efficiency_score', weighted=True),
                        'engagement_score': avg(snap, 'engagement_score', weighted=True),
                        'control_score': avg(snap, 'control_score', weighted=True),
                        'ivt_click_stress_score': avg(snap, 'ivt_click_stress_score', weighted=True),
                        'ivt_serving_score': avg(snap, 'ivt_serving_score', weighted=True),
                        'ivt_attention_score': avg(snap, 'ivt_attention_score', weighted=True),
                        'ivt_counter_score': avg(snap, 'ivt_counter_score', weighted=True),
                        'ivt_funnel_score': avg(snap, 'ivt_funnel_score', weighted=True),
                        'positive_signal_count': total_pos,
                        'negative_signal_count': total_neg,
                        'neutral_signal_count': total_neu,
                        'dominant_event_label': dominant_event_label,
                        'country_details': country_details,
                        'scoring_timeline': scoring_timeline,
                        'scoring_source': 'status_event_aggregate',
                        'scoring_source_label': 'Agregasi fact_site_country_status_history + fact_change_event_long'
                    }}
                }
                if include_events:
                    def _json_scalar(v):
                        try:
                            if pd.isna(v):
                                return None
                        except Exception:
                            pass
                        try:
                            if hasattr(v, 'isoformat'):
                                return v.isoformat(sep=' ')
                        except Exception:
                            pass
                        try:
                            if isinstance(v, np.generic):
                                return v.item()
                        except Exception:
                            pass
                        if isinstance(v, (dict, list, str, int, float, bool)) or v is None:
                            return v
                        return str(v)

                    def _row_to_json_dict(row_obj):
                        out_row = {}
                        for k, v in dict(row_obj).items():
                            out_row[str(k)] = _json_scalar(v)
                        return out_row

                    status_source_rows = detail_snap if 'detail_snap' in locals() else snap
                    status_records_raw = [_row_to_json_dict(srow) for _, srow in status_source_rows.iterrows()]
                    status_records = [_row_to_json_dict(srow) for srow in (country_details if isinstance(country_details, list) else [])]
                    if not status_records:
                        status_records = status_records_raw
                    event_records = [_row_to_json_dict(erow) for _, erow in ev.iterrows()] if not ev.empty else []
                    statuses_spend_sum = float(sum(safe_float((r or {}).get('spend')) for r in status_records))
                    statuses_raw_spend_sum = float(sum(safe_float((r or {}).get('spend')) for r in status_records_raw))
                    country_details_spend_sum = float(sum(safe_float((r or {}).get('spend')) for r in (country_details if isinstance(country_details, list) else [])))

                    out[entity_key]['scoring'] = {
                        'statuses': status_records,
                        'statuses_raw': status_records_raw,
                        'events': event_records,
                        'rows_written': len(status_records),
                        'event_rows_written': len(event_records),
                        'source': 'dashboard_scoring_data_raw',
                        'spend_audit': {
                            'statuses_spend_sum': statuses_spend_sum,
                            'statuses_raw_spend_sum': statuses_raw_spend_sum,
                            'country_details_spend_sum': country_details_spend_sum,
                            'statuses_count': len(status_records),
                            'statuses_raw_count': len(status_records_raw)
                        }
                    }
            return JsonResponse({'status': True, 'data': out}, safe=False)
        except Exception as e:
            logger.exception('DashboardScoringDataView failed')
            return JsonResponse({
                'status': False,
                'error': str(e),
                'traceback': traceback.format_exc()
            }, status=500)


@method_decorator(csrf_exempt, name='dispatch')
class DashboardScoringCompareView(View):
    """
    Endpoint ringan untuk kebutuhan modal "Detail Keputusan":
    - bandingkan snapshot pada jam tertentu (current hour) vs D-1/D-3/D-7
    - bandingkan juga snapshot harian (latest hour pada masing-masing hari)
    Catatan: ini tidak menghitung EWMA (itu tetap di scoring_concept), tapi memakai output tabel status/events yang sudah tersimpan.
    """

    def dispatch(self, request, *args, **kwargs):
        if 'hris_admin' not in request.session:
            return JsonResponse({'status': False, 'error': 'Unauthorized'}, status=401)
        return super().dispatch(request, *args, **kwargs)

    def post(self, req):
        def safe_float(v):
            try:
                return float(v)
            except Exception:
                return 0.0

        def clip(v, lo, hi):
            return max(lo, min(hi, v))

        def normalize_site_entity(v):
            s = str(v or '').strip().lower()
            s = re.sub(r'^https?://', '', s)
            s = s.split('/')[0].split('?')[0].split('#')[0]
            s = re.sub(r'^www\.', '', s)
            return s

        def hour_key(v):
            s = str(v or '').strip()
            if not s or s.lower() == 'nan':
                return None
            try:
                h = int(float(s))
                if 0 <= h <= 23:
                    return h
            except Exception:
                pass
            dt = pd.to_datetime(s, errors='coerce') if pd is not None else None
            if dt is not None and pd.notna(dt):
                return int(dt.hour)
            return None

        def derive_score_decision(health, risk, adj, conf01, dm, label, roi_value=0.0, source_mode='BLENDED'):
            # Samakan dengan DashboardScoringDataView + scxDeriveDecision di template (profit-first)
            source_mode_key = str(source_mode or 'BLENDED').strip().upper()
            single_source = source_mode_key in ["ADX_ONLY", "ADSENSE_ONLY"]
            down_score_cut = 52 if single_source else 55
            down_dm_cut = -12 if single_source else -10
            down_anomaly_cut = 3 if single_source else 2
            profit_component = clip((float(roi_value) + 1.0) * 50.0, 0.0, 100.0)
            score = (((health + 100.0) / 2.0) * 0.25) + ((100.0 - risk) * 0.2) + (conf01 * 100.0 * 0.1) + (clip(dm + 50.0, 0.0, 100.0) * 0.15) + (profit_component * 0.30)
            score = int(round(clip(score, 0.0, 100.0)))
            negative_labels = ["TRAFFIC_DROP", "SERVING_DROP", "YIELD_DROP", "VIEWABILITY_DROP", "EFFICIENCY_DROP", "REVENUE_DROP", "NEGATIVE_MIXED", "NEG_ADJUSTMENT", "WATCH_NEGATIVE", "WATCH_DECAY"]
            label_up = str(label or 'STABLE').strip().upper()
            anomaly_cards = []
            if risk >= 70: anomaly_cards.append('IVT_RISK')
            if adj <= -60: anomaly_cards.append('NEG_ADJUSTMENT')
            if dm <= -50: anomaly_cards.append('MARGIN_CRASH')
            severe_anomaly = label_up.startswith("RED_FLAG") or ('IVT_RISK' in anomaly_cards)
            decision = "HOLD"
            if severe_anomaly or (risk >= 90 and conf01 >= 0.60 and score < 40) or (dm <= -65 and health <= -25 and conf01 >= 0.60):
                decision = "STOP"
            elif (label_up in ["POSITIVE_EXPANSION", "POSITIVE_RECOVERY", "WATCH_POSITIVE"] and score >= 62 and risk < 60 and dm >= 6 and conf01 >= 0.45) or (score >= 72 and risk < 50 and dm >= 10 and conf01 >= 0.50):
                decision = "SCALE UP"
            elif (label_up in negative_labels and ((score < down_score_cut and dm < down_dm_cut) or (health < -12 and adj < -18))) or (score < 40 and dm < -12 and (health < -15 or risk >= 72)) or (len(anomaly_cards) >= down_anomaly_cut and score < 62):
                decision = "SCALE_DOWN"

            if (decision == "SCALE_DOWN") and (label_up in ["WATCH_DECAY", "WATCH_NEGATIVE"]) and (risk < 40) and (score >= 45):
                decision = "HOLD"
            if decision == "SCALE_DOWN":
                decision = "SCALE DOWN"
            return score, decision

        def aggregate_snapshot(frame: pd.DataFrame) -> dict:
            if frame is None or frame.empty:
                return {}
            tmp = frame.copy()
            # dedup per country (ambil yang terbaru berdasarkan run_time lalu run_hour)
            if 'country_code' in tmp.columns:
                tmp['country_code'] = tmp['country_code'].astype(str).str.strip().str.upper()
            if 'run_time' in tmp.columns:
                tmp['run_time_dt'] = pd.to_datetime(tmp['run_time'], errors='coerce')
            else:
                tmp['run_time_dt'] = pd.NaT
            if 'run_hour' in tmp.columns:
                tmp['run_hour_key'] = tmp['run_hour'].map(hour_key)
            else:
                tmp['run_hour_key'] = None
            if 'country_code' in tmp.columns:
                tmp = tmp.sort_values(['country_code', 'run_time_dt', 'run_hour_key']).drop_duplicates(subset=['country_code'], keep='last')

            for c in ['positive_signal_count', 'negative_signal_count', 'neutral_signal_count']:
                if c not in tmp.columns:
                    tmp[c] = 0.0
            tmp['signal_total'] = (
                pd.to_numeric(tmp['positive_signal_count'], errors='coerce').fillna(0.0)
                + pd.to_numeric(tmp['negative_signal_count'], errors='coerce').fillna(0.0)
                + pd.to_numeric(tmp['neutral_signal_count'], errors='coerce').fillna(0.0)
            )

            def avg(col, weighted=True):
                if col not in tmp.columns:
                    return 0.0
                vals = pd.to_numeric(tmp[col], errors='coerce').dropna()
                if not len(vals):
                    return 0.0
                if weighted:
                    w = pd.to_numeric(tmp.loc[vals.index, 'signal_total'], errors='coerce').fillna(0.0)
                    den = float(w.sum())
                    if den > 0:
                        return float((vals * w).sum() / den)
                return float(vals.mean())

            health = avg('health_score', weighted=True)
            risk = avg('ivt_risk_score', weighted=True)
            adj = avg('adjustment_score', weighted=True)
            dm = avg('decision_margin', weighted=True)
            conf_raw = avg('confidence', weighted=True)
            conf01 = clip((conf_raw / 100.0) if conf_raw > 1.0 else conf_raw, 0.0, 1.0)

            labels = []
            for c in ['final_label', 'root_cause_label']:
                if c in tmp.columns:
                    labels.extend([str(x).strip().upper() for x in tmp[c].tolist() if str(x).strip()])
            label = pd.Series(labels).value_counts().index[0] if labels else 'STABLE'

            spend_total = float(pd.to_numeric(tmp['spend'], errors='coerce').fillna(0.0).sum()) if 'spend' in tmp.columns else 0.0
            revenue_total = float(pd.to_numeric(tmp['revenue_value'], errors='coerce').fillna(0.0).sum()) if 'revenue_value' in tmp.columns else 0.0
            roi_total = ((revenue_total - spend_total) / spend_total) if spend_total > 0 else 0.0
            source_mode = 'BLENDED'
            if 'mapped_revenue_source' in tmp.columns:
                src_vals = tmp['mapped_revenue_source'].astype(str).str.strip().str.upper()
                src_vals = src_vals[src_vals.ne('')]
                if not src_vals.empty:
                    source_mode = str(src_vals.value_counts().index[0])
            score, decision = derive_score_decision(health, risk, adj, conf01, dm, label, roi_total, source_mode)

            reason_parts = [str(x).strip() for x in tmp.get('reason_summary', pd.Series([], dtype=str)).tolist() if str(x).strip()]
            reason_summary = ' | '.join(list(dict.fromkeys(reason_parts))[:3])

            last_rt = ''
            if 'run_time_dt' in tmp.columns and tmp['run_time_dt'].notna().any():
                last_rt = tmp['run_time_dt'].max().strftime('%Y-%m-%d %H:%M:%S')

            return {
                'health_score': health,
                'ivt_risk_score': risk,
                'adjustment_score': adj,
                'confidence': conf01,
                'decision_margin': dm,
                'score': score,
                'decision': decision,
                'label': label,
                'reason_summary': reason_summary,
                'updated_at': last_rt,
                'countries': int(len(tmp)),
                'positive_signal_count': int(pd.to_numeric(tmp['positive_signal_count'], errors='coerce').fillna(0).sum()),
                'negative_signal_count': int(pd.to_numeric(tmp['negative_signal_count'], errors='coerce').fillna(0).sum()),
                'neutral_signal_count': int(pd.to_numeric(tmp['neutral_signal_count'], errors='coerce').fillna(0).sum()),
            }

        try:
            if pd is None:
                raise RuntimeError('pandas belum tersedia')
            payload = json.loads((req.body or b'').decode('utf-8') or '{}')
            target_date_str = str(payload.get('date') or '').strip()
            domain_raw = payload.get('domain') or payload.get('site') or payload.get('meta_campaign') or ''
            domain = normalize_site_entity(domain_raw)
            run_hour_req = payload.get('run_hour')
            run_hour_req = hour_key(run_hour_req)
            if not target_date_str or not domain:
                return JsonResponse({'status': False, 'error': 'date dan domain wajib diisi'}, status=400)
            target_dt = pd.to_datetime(target_date_str, errors='coerce')
            if pd.isna(target_dt):
                return JsonResponse({'status': False, 'error': 'format date tidak valid (YYYY-MM-DD)'}, status=400)
            target_date = target_dt.date()
            compare_offsets = {'h1': 1, 'h3': 3, 'h7': 7}
            compare_dates = {k: (target_date - timedelta(days=v)) for k, v in compare_offsets.items()}

            scoring_module = _get_scoring_concept_module()
            query_df_func = getattr(scoring_module, 'query_df', None)
            if query_df_func is None:
                query_df_func = query_df  # fallback import atas
            status_table = getattr(scoring_module, 'STATUS_TABLE', 'hris_trendHorizone.fact_site_country_status_history')
            source_table = getattr(scoring_module, 'SOURCE_TABLE', 'hris_trendHorizone.fact_join_hourly')

            dates_in = [target_date] + list(compare_dates.values())
            literals_dates = ', '.join([f"toDate('{d.isoformat()}')" for d in dates_in])

            status_sql = f"""
            SELECT
                toDate(date) AS date,
                site,
                country_code,
                country_name,
                run_time,
                run_hour,
                mapped_revenue_source,
                join_status,
                final_label,
                root_cause_label,
                health_score,
                adjustment_score,
                ivt_risk_score,
                confidence,
                decision_margin,
                positive_signal_count,
                negative_signal_count,
                neutral_signal_count,
                reason_summary
            FROM {status_table}
            WHERE toDate(date) IN ({literals_dates})
              AND lower(site) = '{domain.replace("'", "''")}'
            """
            sdf = query_df_func(status_sql)
            if sdf is None or sdf.empty:
                return JsonResponse({'status': True, 'data': {'domain': domain, 'date': target_date_str, 'empty': True}}, safe=False)
            sdf = sdf.copy()
            sdf['date'] = pd.to_datetime(sdf['date'], errors='coerce').dt.date

            # Query sumber (spend/revenue) untuk bantu rekomendasi aksi
            src_sql = f"""
            SELECT
                toDate(date) AS date,
                run_hour,
                sum(meta_spend) AS meta_spend,
                sum(adx_revenue) AS adx_revenue,
                sum(adsense_estimated_earnings) AS adsense_estimated_earnings,
                argMax(mapped_revenue_source, mdd) AS mapped_revenue_source
            FROM {source_table}
            WHERE toDate(date) IN ({literals_dates})
              AND lower(site) = '{domain.replace("'", "''")}'
            GROUP BY date, run_hour
            """
            try:
                src_df = query_df_func(src_sql)
            except Exception:
                src_df = pd.DataFrame()

            src_map = {}
            if src_df is not None and not src_df.empty:
                for _, r in src_df.iterrows():
                    d = pd.to_datetime(r.get('date'), errors='coerce')
                    d = d.date() if pd.notna(d) else None
                    h = hour_key(r.get('run_hour'))
                    if d is None or h is None:
                        continue
                    src_map[(d, h)] = {
                        'meta_spend': safe_float(r.get('meta_spend')),
                        'adx_revenue': safe_float(r.get('adx_revenue')),
                        'adsense_estimated_earnings': safe_float(r.get('adsense_estimated_earnings')),
                        'mapped_revenue_source': str(r.get('mapped_revenue_source') or '').strip().upper(),
                    }

            def attach_financial(snap: dict, d: date, h: Optional[int]):
                if not snap:
                    return snap
                if h is None:
                    return snap
                src = src_map.get((d, h))
                if not src:
                    return snap
                spend = safe_float(src.get('meta_spend'))
                revenue = safe_float(src.get('adx_revenue')) + safe_float(src.get('adsense_estimated_earnings'))
                roi = ((revenue - spend) / spend * 100.0) if spend > 0 else 0.0
                snap['meta_spend'] = spend
                snap['revenue'] = revenue
                snap['roi'] = roi
                snap['mapped_revenue_source'] = str(src.get('mapped_revenue_source') or '')
                return snap

            # Tentukan latest hour per tanggal
            sdf['run_hour_key'] = sdf['run_hour'].map(hour_key)
            latest_hour_by_date = {}
            for d, part in sdf.groupby('date', sort=False):
                hrs = pd.to_numeric(part['run_hour_key'], errors='coerce')
                hrs = hrs.dropna()
                if len(hrs):
                    latest_hour_by_date[d] = int(hrs.max())

            # Snapshot current (jam aktif atau latest)
            cur_hour = run_hour_req if run_hour_req is not None else latest_hour_by_date.get(target_date)
            cur_hour = int(cur_hour) if cur_hour is not None else None
            cur_frame = sdf[(sdf['date'] == target_date) & (sdf['run_hour_key'] == cur_hour)].copy() if cur_hour is not None else sdf[sdf['date'] == target_date].copy()
            cur = aggregate_snapshot(cur_frame)
            cur = attach_financial(cur, target_date, cur_hour)
            if cur:
                cur['date'] = target_date.isoformat()
                cur['run_hour'] = cur_hour

            by_day = {}
            by_hour = {}
            for key, dprev in compare_dates.items():
                lh = latest_hour_by_date.get(dprev)
                # daily snapshot = latest hour
                day_frame = sdf[(sdf['date'] == dprev) & (sdf['run_hour_key'] == lh)].copy() if lh is not None else sdf[sdf['date'] == dprev].copy()
                snap_day = aggregate_snapshot(day_frame)
                snap_day = attach_financial(snap_day, dprev, lh)
                if snap_day:
                    snap_day['date'] = dprev.isoformat()
                    snap_day['run_hour'] = lh
                by_day[key] = snap_day or {}

                # hour snapshot = same hour as current (jika ada)
                snap_h = {}
                if cur_hour is not None:
                    h_frame = sdf[(sdf['date'] == dprev) & (sdf['run_hour_key'] == cur_hour)].copy()
                    snap_h = aggregate_snapshot(h_frame)
                    snap_h = attach_financial(snap_h, dprev, cur_hour)
                    if snap_h:
                        snap_h['date'] = dprev.isoformat()
                        snap_h['run_hour'] = cur_hour
                by_hour[key] = snap_h or {}

            # rekomendasi aksi sederhana (bisa disempurnakan)
            rec = {'action': cur.get('decision') if cur else 'HOLD', 'budget_change_pct': 0, 'reason': ''}
            if cur:
                dec = str(cur.get('decision') or '').upper()
                conf = float(cur.get('confidence') or 0.0)
                score = float(cur.get('score') or 0.0)
                if dec == 'SCALE UP':
                    rec['budget_change_pct'] = 15 if (score >= 75 and conf >= 0.55) else 10
                    rec['reason'] = 'Sinyal positif kuat; pertimbangkan naikkan budget bertahap.'
                elif dec == 'SCALE DOWN':
                    rec['budget_change_pct'] = -15 if score < 45 else -10
                    rec['reason'] = 'Sinyal negatif dominan; turunkan budget untuk mitigasi risiko.'
                elif dec == 'STOP':
                    rec['budget_change_pct'] = 0
                    rec['reason'] = 'Red flag / risiko tinggi; pertimbangkan stop campaign sementara.'
                else:
                    rec['reason'] = 'Kondisi relatif stabil; lanjut monitor.'

            return JsonResponse({
                'status': True,
                'data': {
                    'domain': domain,
                    'date': target_date.isoformat(),
                    'current': cur or {},
                    'compare_by_day': by_day,
                    'compare_by_hour': by_hour,
                    'recommendation': rec,
                }
            }, safe=False)
        except Exception as e:
            logger.exception('DashboardScoringCompareView failed')
            return JsonResponse({'status': False, 'error': str(e), 'traceback': traceback.format_exc()}, status=500)

@method_decorator(csrf_exempt, name='dispatch')
class DashboardCreateScoringView(View):
    def dispatch(self, request, *args, **kwargs):
        if 'hris_admin' not in request.session:
            return JsonResponse({
                'status': False,
                'message': 'Unauthorized'
            }, status=401)
        return JsonResponse({
            'status': False,
            'message': 'Dashboard create scoring dinonaktifkan. Gunakan cron terjadwal.'
        }, status=403)

    def post(self, req):
        try:
            payload = json.loads((req.body or b'').decode('utf-8') or '{}')
            batch_id = str(payload.get('batch_id'))
            target_date = str(payload.get('date') or '').strip()
            run_hour_raw = payload.get('run_hour')
            domain = str(payload.get('domain') or payload.get('site') or payload.get('meta_campaign') or '').strip().lower()
            country_cd = str(payload.get('country_cd') or payload.get('country_code') or '').strip().upper()
            mapped_revenue_source = str(payload.get('mapped_revenue_source') or '').strip().lower()

            if not target_date:
                return JsonResponse({
                    'status': False,
                    'message': 'date wajib diisi'
                }, status=400)

            if not domain and not country_cd:
                return JsonResponse({
                    'status': False,
                    'message': 'domain atau country_cd wajib diisi'
                }, status=400)

            try:
                target_dt = datetime.strptime(target_date, '%Y-%m-%d').date()
            except Exception:
                return JsonResponse({
                    'status': False,
                    'message': 'format date harus YYYY-MM-DD'
                }, status=400)

            run_hour = None
            if run_hour_raw not in (None, ''):
                try:
                    run_hour = int(run_hour_raw)
                except Exception:
                    return JsonResponse({
                        'status': False,
                        'message': 'run_hour harus integer'
                    }, status=400)

            allowed_scoring_hours = {3, 6, 9, 12, 15, 18, 21, 23}
            if run_hour is None:
                run_hour = int(datetime.now().hour)
            if run_hour not in allowed_scoring_hours:
                return JsonResponse({
                    'status': False,
                    'message': 'Scoring manual hanya diizinkan pada jam 03,06,09,12,15,18,21,23 (WIB).'
                }, status=400)

            comparison_dates = {
                'h1': (target_dt - timedelta(days=1)).isoformat(),
                'h3': (target_dt - timedelta(days=3)).isoformat(),
                'h7': (target_dt - timedelta(days=7)).isoformat(),
                'h14': (target_dt - timedelta(days=14)).isoformat(),
            }

            scoring_module = _get_scoring_concept_module()
            scoring_module.query_df = query_df
            scoring_module.insert_df = insert_df
            scoring_result = scoring_module.score_site_country(
                target_date=target_dt,
                domain = domain,
                compatibility_mode=False,
                write_results=True,
                run_hour=run_hour,
            )
            return JsonResponse({
                'status': True,
                'message': 'create scoring berhasil',
                'request': {
                    'date': target_date,
                    'run_hour': run_hour,
                    'domain': domain,
                    'country_cd': country_cd,
                    'mapped_revenue_source': mapped_revenue_source,
                },
                'comparison_dates': comparison_dates,
                'scoring': scoring_result,
            }, safe=False, json_dumps_params={'allow_nan': False})

        except Exception as e:
            logger.exception("DashboardCreateScoringView failed")
            return JsonResponse({
                'status': False,
                'message': str(e),
                'traceback': traceback.format_exc(),
            }, status=500)

def _get_scoring_concept_module():
    from . import scoring_concept
    return scoring_concept

@method_decorator(csrf_exempt, name='dispatch')
class DashboardSyncView(View):
    def dispatch(self, request, *args, **kwargs):
        if 'hris_admin' not in request.session:
            return JsonResponse({'status': False, 'error': 'Unauthorized'}, status=401)
        return JsonResponse({
            'status': False,
            'message': 'Dashboard sync/scoring manual dinonaktifkan. Gunakan cron terjadwal.'
        }, status=403)

    def post(self, req):
        try:
            payload = {}
            try:
                payload = json.loads((req.body or b'').decode('utf-8') or '{}')
            except Exception:
                payload = {}

            tanggal = str(payload.get('tanggal') or '').strip()
            if not tanggal:
                tanggal = '%'

            source = str(payload.get('source') or 'all').strip().lower()

            steps = []
            if source == 'adsense':
                commands = [
                    'cron_ads_country_load',
                    'cron_adsense_country_load',
                ]
            elif source == 'adx':
                commands = [
                    'cron_ads_country_load',
                    'cron_adx_country_load',
                ]
            else:
                commands = [
                    'cron_ads_country_load',
                    'cron_adx_country_load',
                    'cron_adsense_country_load',
                ]
            class _LimitedStringIO:
                def __init__(self, max_chars=20000):
                    self.max_chars = int(max_chars or 0)
                    self._parts = []
                    self._len = 0

                def write(self, s):
                    if s is None:
                        return 0
                    ss = str(s)
                    self._parts.append(ss)
                    self._len += len(ss)
                    if self.max_chars > 0 and self._len > self.max_chars:
                        joined = ''.join(self._parts)
                        if len(joined) > self.max_chars:
                            joined = joined[-self.max_chars:]
                        self._parts = [joined]
                        self._len = len(joined)
                    return len(ss)

                def flush(self):
                    return None

                def getvalue(self):
                    return ''.join(self._parts)

            for cmd in commands:
                buf = _LimitedStringIO(max_chars=20000)
                step = {'command': cmd}
                try:
                    if tanggal != '%':
                        call_command(cmd, tanggal=tanggal, stdout=buf)
                    else:
                        call_command(cmd, stdout=buf)
                    step['status'] = True
                except Exception as e:
                    step['status'] = False
                    step['error'] = str(e)

                out = buf.getvalue()
                step['output'] = out

                try:
                    m_ins = re.search(r"Berhasil\s+insert:\s*([0-9]+)", out or '', flags=re.IGNORECASE)
                    if m_ins:
                        step['inserted'] = int(m_ins.group(1))
                    m_fail = re.search(r"gagal:\s*([0-9]+)", out or '', flags=re.IGNORECASE)
                    if m_fail:
                        step['failed_rows'] = int(m_fail.group(1))
                except Exception:
                    pass

                steps.append(step)

            failed_jobs = 0
            for s in steps:
                if s.get('status') is False:
                    failed_jobs += 1

            scoring_step = None
            if tanggal != '%':
                scoring_step = {'command': 'score_site_country'}
                try:
                    if failed_jobs > 0:
                        scoring_step['status'] = True
                        scoring_step['skipped'] = True
                        scoring_step['warning'] = 'Scoring dilewati karena masih ada job sinkronisasi yang gagal.'
                    else:
                        allowed_scoring_hours = {3, 6, 9, 12, 15, 18, 21, 23}
                        run_hour_now = int(datetime.now().hour)
                        if run_hour_now not in allowed_scoring_hours:
                            scoring_step['status'] = True
                            scoring_step['skipped'] = True
                            scoring_step['warning'] = 'Scoring dashboard dilewati karena di luar jam cron resmi (03,06,09,12,15,18,21,23 WIB).'
                        else:
                            target_date = datetime.strptime(tanggal, '%Y-%m-%d').date()
                            score_site_country = _get_scoring_concept_module().score_site_country
                            scoring_result = score_site_country(
                                target_date=target_date,
                                compatibility_mode=False,
                                write_results=True,
                                run_hour=run_hour_now,
                            )
                            scoring_step['status'] = bool(scoring_result.get('ok'))
                            scoring_step['rows_written'] = int(scoring_result.get('rows_written') or 0)
                            scoring_step['event_rows_written'] = int(scoring_result.get('event_rows_written') or 0)
                            if scoring_result.get('warning'):
                                scoring_step['warning'] = str(scoring_result.get('warning'))
                            scoring_step['result'] = scoring_result
                except Exception as e:
                    scoring_step['status'] = False
                    scoring_step['error'] = str(e)
                steps.append(scoring_step)
                        # ...existing code...
            else:
                target_date = datetime.strptime(tanggal, '%Y-%m-%d').date()
                score_site_country = _get_scoring_concept_module().score_site_country
                scoring_result = score_site_country(
                    target_date=target_date,
                    compatibility_mode=False,
                    write_results=True
                )
            # ...existing code...
            parts = []
            for s in steps:
                cmd = str(s.get('command') or '').strip()
                if cmd == 'score_site_country':
                    if s.get('rows_written') is not None:
                        parts.append(f"status={int(s.get('rows_written') or 0)}")
                    if s.get('event_rows_written') is not None:
                        parts.append(f"events={int(s.get('event_rows_written') or 0)}")
                    continue
                ins = s.get('inserted')
                if ins is None:
                    continue
                short = cmd.replace('cron_', '').replace('_load', '')
                parts.append(f"{short}={ins}")

            message = 'Synchronize selesai'
            if parts:
                message = message + ' (' + ', '.join(parts) + ')'

            failed_jobs = 0
            for s in steps:
                if s.get('status') is False:
                    failed_jobs += 1

            return JsonResponse({'status': True, 'message': message, 'failed': failed_jobs, 'steps': steps})
        except Exception as e:
            return JsonResponse({'status': False, 'error': str(e)}, status=500)


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
            # Statistik user/login tidak dipakai lagi di dashboard utama.
            # Tetap kirim struktur default agar frontend lama tetap aman.
            dashboard_data = {
                'user_stats': {
                    'total_users': 0,
                    'active_users': 0,
                    'total_logins_7days': 0,
                    'activity_rate': 0
                },
                'charts': {
                    'login_activity': {
                        'labels': [],
                        'login_counts': [],
                        'unique_users': []
                    }
                },
                'recent_logins': []
            }
            # Tambah statistik akun Ads & AdX + daftar akun AdX untuk filter
            try:
                rs_ads_accounts = data_mysql().master_account_ads()
                ads_accounts_count = len(rs_ads_accounts['data']) if rs_ads_accounts.get('status') else 0
            except Exception:
                ads_accounts_count = 0

            try:
                rs_adx_credentials = data_mysql().get_all_app_credentials()
                adx_accounts_count = len(rs_adx_credentials['data']) if rs_adx_credentials.get('status') else 0
                adx_accounts_list = []
                if rs_adx_credentials.get('status'):
                    for row in rs_adx_credentials['data']:
                        adx_accounts_list.append({
                            'account_id': row.get('account_id') or '',
                            'user_mail': row.get('user_mail') or row.get('account_email') or '',
                            'account_name': row.get('account_name') or (row.get('user_mail') or '')
                        })
                else:
                    adx_accounts_list = []
            except Exception:
                adx_accounts_count = 0
                adx_accounts_list = []

            session_mail = req.session.get('hris_admin', {}).get('user_mail')
            default_selected_account = ''
            if session_mail and adx_accounts_list:
                for acc in adx_accounts_list:
                    if (acc.get('user_mail') or '') == session_mail and (acc.get('account_id') or ''):
                        default_selected_account = acc.get('account_id') or ''
                        break
            if not default_selected_account:
                default_selected_account = (adx_accounts_list[0].get('account_id') or '') if adx_accounts_list else ''

            dashboard_data['account_stats'] = {
                'ads_accounts_count': ads_accounts_count,
                'adx_accounts_count': adx_accounts_count,
                'adx_accounts': adx_accounts_list,
                'default_selected_account': default_selected_account
            }

            try:
                end_dt = datetime.now().date()
                start_date_7 = (end_dt - timedelta(days=6)).strftime('%Y-%m-%d')
                end_date_7 = end_dt.strftime('%Y-%m-%d')

                rs_adx = data_mysql().get_all_adx_traffic_account_by_params(start_date_7, end_date_7, None, None)
                adx_rows = []
                if rs_adx and isinstance(rs_adx, dict):
                    adx_rows = (rs_adx.get('hasil') or {}).get('data') or []

                rs_adsense = data_mysql().get_all_adsense_traffic_account_by_params(start_date_7, end_date_7, None)
                adsense_rows = []
                if rs_adsense and isinstance(rs_adsense, dict):
                    adsense_rows = (rs_adsense.get('hasil') or {}).get('data') or []

                rev_adx_by_date = defaultdict(float)
                for row in adx_rows:
                    dt_key = str((row or {}).get('date') or '')[:10]
                    rev_adx_by_date[dt_key] += float((row or {}).get('revenue') or 0)

                rev_adsense_by_date = defaultdict(float)
                for row in adsense_rows:
                    dt_key = str((row or {}).get('date') or '')[:10]
                    rev_adsense_by_date[dt_key] += float((row or {}).get('revenue') or 0)

                chart_dates = [(end_dt - timedelta(days=6 - i)) for i in range(7)]
                labels = [f"{d.day} {data_bulan.get(d.month, str(d.month))} {d.year}" for d in chart_dates]
                date_keys = [d.strftime('%Y-%m-%d') for d in chart_dates]

                dashboard_data.setdefault('charts', {})
                dashboard_data['charts']['earnings_comparison'] = {
                    'labels': labels,
                    'adx_earnings': [round(rev_adx_by_date.get(k, 0.0), 2) for k in date_keys],
                    'adsense_earnings': [round(rev_adsense_by_date.get(k, 0.0), 2) for k in date_keys]
                }
            except Exception:
                dashboard_data.setdefault('charts', {})
                dashboard_data['charts']['earnings_comparison'] = {
                    'labels': [],
                    'adx_earnings': [],
                    'adsense_earnings': []
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
        return super().dispatch(request, *args, **kwargs)
    def get(self, req):
        data_account = data_mysql().master_account_ads()['data']
        data_domain = data_mysql().master_domain_ads()['data']
        last_update = data_mysql().get_last_update_ads_traffic_per_domain()['data']['last_update']
        data = {
            'title': 'Data Summary Facebook Ads',
            'user': req.session['hris_admin'],  
            'last_update': last_update,
            'data_account': data_account,
            'data_domain': data_domain,
        }
        return render(req, 'admin/facebook_ads/summary/index.html', data)
    
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
        selected_account_list = []
        if data_account:
            selected_account_list = [str(s).strip() for s in data_account.split(',') if s.strip()]
        data_domain = req.GET.get('data_domain')
        selected_domain_list = []
        if data_domain:
            selected_domain_list = [str(s).strip() for s in data_domain.split(',') if s.strip()]
        # Panggil ke database layer dengan argumen positional sesuai definisi fungsi
        db_result = data_mysql().get_all_ads_traffic_campaign_by_params(
            tanggal_dari,
            tanggal_sampai,
            selected_account_list,
            selected_domain_list,
        )
        # Unwrap payload (fungsi mengembalikan {'hasil': {...}})
        payload = db_result.get('hasil', {}) if isinstance(db_result, dict) else {}
        status_ok = bool(payload.get('status', False))
        raw_rows = payload.get('data', []) if status_ok else []
        # Normalisasi kolom agar cocok dengan harapan di management/static/ajax/admin/facebook_ads/campaign.js
        normalized_rows = []
        total_spend = 0.0
        total_impressions = 0
        total_reach = 0
        total_clicks = 0
        for row in raw_rows or []:
            account_name = row.get('account_name')
            domain = row.get('domain')
            campaign = row.get('campaign')

            spend = float(row.get('spend', 0) or 0)
            impressions = int(row.get('impressions', 0) or 0)
            reach = int(row.get('reach', 0) or 0)
            clicks = int(row.get('clicks', 0) or 0)
            cpr = float(row.get('cpr', 0) or 0)
            cpc = float(row.get('cpc', 0) or 0)

            frequency_val = row.get('frequency', None)
            if frequency_val in [None, '']:
                if reach == 0:
                    frequency = 0
                else:
                    frequency = float(format(impressions / reach, '.1f'))
            else:
                try:
                    frequency = float(frequency_val)
                except Exception:
                    frequency = 0

            lpv = float(row.get('lpv', 0) or 0)
            lpv_rate = float(row.get('lpv_rate', 0) or 0)

            normalized_rows.append({
                'date': row.get('date'),
                'account_id': row.get('account_id'),
                'account_name': account_name,
                'domain': domain,
                'campaign': campaign,
                'spend': spend,
                'impressions': impressions,
                'reach': reach,
                'clicks': clicks,
                'frequency': frequency,
                'cpr': cpr,
                'cpc': cpc,
                'lpv': lpv,
                'lpv_rate': lpv_rate,
            })
            # Akumulasi untuk total
            total_spend += spend
            total_impressions += impressions
            total_reach += reach
            total_clicks += clicks
        # Agregasi total: frequency total sebagai (impressions/reach)*100, CPR total sebagai spend/clicks
        if total_reach == 0:
            total_frequency = 0
        else:
            total_frequency = format(total_impressions / total_reach, '.1f')
        rata_cpr = round(sum([row['cpr'] for row in normalized_rows]) / len(normalized_rows), 0) if normalized_rows else 0.0
        rata_cpc = round(sum([row['cpc'] for row in normalized_rows]) / len(normalized_rows), 0) if normalized_rows else 0.0
        monitor_rows = []
        try:
            domain_filter_for_api = str(data_domain or '%') if 'data_domain' in locals() else '%'
            accounts_all = data_mysql().master_account_ads().get('data', [])

            # Pilih account sesuai filter (jika tidak ada -> semua account)
            if selected_account_list:
                selected_set = set([str(x) for x in selected_account_list])
                accounts_target = [a for a in accounts_all if str(a.get('account_id')) in selected_set]
            else:
                accounts_target = accounts_all

            api_rows = []
            if not selected_account_list or len(accounts_target) > 1:
                api_rs = fetch_data_insights_all_accounts_by_subdomain(
                    str(tanggal_dari),
                    accounts_target,
                    domain_filter_for_api,
                    str(tanggal_sampai),
                )
                api_rows = (api_rs or {}).get('data', []) if isinstance(api_rs, dict) else []
            elif accounts_target:
                acc = accounts_target[0]
                api_rs = fetch_data_insights_account(
                    str(tanggal_dari),
                    str(acc.get('access_token', '')),
                    str(acc.get('account_id', '')),
                    domain_filter_for_api,
                    str(acc.get('account_name', '')),
                    str(tanggal_sampai),
                )
                api_rows = (api_rs or {}).get('data', []) if isinstance(api_rs, dict) else []

            for r in api_rows or []:
                spend_v = float(r.get('spend', 0) or 0)
                budget_v = float(r.get('daily_budget', 0) or 0)
                status_v = str(r.get('status', '') or 'UNKNOWN').upper()

                is_paused = (status_v == 'PAUSED')
                is_overspend = (spend_v > budget_v)

                if not (is_paused or is_overspend):
                    continue

                remark_v = 'Paused' if is_paused else 'Overspend'
                monitor_rows.append({
                    'account_name': r.get('account_name') or '-',
                    'campaign': r.get('campaign_name') or '-',
                    'spend': spend_v,
                    'daily_budget': budget_v,
                    'campaign_status': status_v,
                    'remark': remark_v,
                })

            monitor_rows = sorted(monitor_rows, key=lambda x: float(x.get('spend', 0) or 0), reverse=True)
        except Exception:
            # fallback query DB lama jika API gagal
            monitor_result = data_mysql().get_monitoring_campaign_facebook_by_params(
                tanggal_dari,
                tanggal_sampai,
                selected_account_list,
                selected_domain_list,
            )
            monitor_payload = monitor_result.get('hasil', {}) if isinstance(monitor_result, dict) else {}
            monitor_rows = monitor_payload.get('data', []) if bool(monitor_payload.get('status', False)) else []

        response_data = {
            'hasil': "Data Traffic Per Campaign",
            'data_campaign': normalized_rows,
            'total_campaign': [{
                'total_spend': total_spend,
                'total_impressions': total_impressions,
                'total_reach': total_reach,
                'total_click': total_clicks,
                'total_frequency': total_frequency,
                'total_cpr': format(rata_cpr, '.0f'),
                'total_cpc': format(rata_cpc, '.0f'),
            }],
            'monitoring_campaign': monitor_rows,
        }
        # Jika terjadi kegagalan di layer DB, kirimkan respons kosong agar frontend tidak error
        if not status_ok:
            response_data['error'] = payload.get('data') or payload.get('message') or 'Gagal mengambil data campaign'
        return JsonResponse(response_data)

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
        today_dt = datetime.now().date()
        now = (today_dt - timedelta(days=6)).strftime('%Y-%m-%d')
        data_account_ads = data_mysql().data_account_ads_by_params(now)['data']
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
        data_domain = data_mysql().master_domain_ads()['data']
        today = datetime.now().strftime('%Y-%m-%d')
        seven_days_ago = (datetime.now() - timedelta(days=6)).strftime('%Y-%m-%d')
        data = {
            'title': 'Data Traffic Per Account Facebook Ads',
            'user': req.session['hris_admin'],
            'data_account': data_account,
            'data_domain': data_domain,
            'today': today,
            'seven_days_ago': seven_days_ago,
        }
        return render(req, 'admin/facebook_ads/per_account/index.html', data)
    
class page_per_account_facebook(View):
    def dispatch(self, request, *args, **kwargs):
        if 'hris_admin' not in request.session:
            return redirect('admin_login')
        elif 'hris_admin' not in request.session:
            return redirect('user_login')
        return super(page_per_account_facebook, self).dispatch(request, *args, **kwargs)
    def get(self, req):
        tanggal_dari = (req.GET.get('tanggal_dari') or '').strip() or (req.GET.get('tanggal') or '').strip()
        tanggal_sampai = (req.GET.get('tanggal_sampai') or '').strip()
        data_account = req.GET.get('data_account')
        data_domain = req.GET.get('data_domain')

        if not tanggal_dari or tanggal_dari == '%':
            tanggal_dari = datetime.now().strftime('%Y-%m-%d')
        if not tanggal_sampai or tanggal_sampai == '%':
            tanggal_sampai = tanggal_dari

        try:
            if tanggal_dari > tanggal_sampai:
                tanggal_dari, tanggal_sampai = tanggal_sampai, tanggal_dari
        except Exception:
            pass

        if not data_domain or data_domain == '':
            data_domain = '%'

        if not data_account or data_account == '%':
            rs_account = data_mysql().master_account_ads()['data']
            data = fetch_data_insights_all_accounts_by_subdomain(str(tanggal_dari), rs_account, str(data_domain), str(tanggal_sampai))
        else:
            rs_data_account = data_mysql().master_account_ads_by_id({
                'data_account': data_account,
            })['data']
            data = fetch_data_insights_account(
                str(tanggal_dari),
                str(rs_data_account['access_token']),
                str(rs_data_account['account_id']),
                str(data_domain),
                str(rs_data_account['account_name']),
                str(tanggal_sampai),
            )
        
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
        # Ambil data account menggunakan fungsi yang mengembalikan list records
        rs_account = data_mysql().master_account_ads_by_params({
            'data_account': account_ads_id,
        })
        records = rs_account.get('data') or []
        rs_data_account = records[0] if records else None

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
        
        # Cek apakah account exists - gunakan account_ads_id (UUID)
        rs_account = data_mysql().master_account_ads_by_params({
            'data_account': account_ads_id,
        })
        data_list = rs_account.get('data') or []
        existing_account = data_list[0] if data_list else None
        
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

class DeleteAccountFacebookAds(View):
    def dispatch(self, request, *args, **kwargs):
        if 'hris_admin' not in request.session:
            return redirect('admin_login')
        return super(DeleteAccountFacebookAds, self).dispatch(request, *args, **kwargs)

    def post(self, req):
        account_ads_id = req.POST.get('account_ads_id')
        if not account_ads_id:
            return JsonResponse({
                'status': False,
                'message': 'account_ads_id wajib diisi'
            }, status=400)

        rs_account = data_mysql().master_account_ads_by_params({
            'data_account': account_ads_id,
        })
        data_list = rs_account.get('data') or []
        existing_account = data_list[0] if data_list else None

        if existing_account is None:
            return JsonResponse({
                'status': False,
                'message': 'Account tidak ditemukan!'
            }, status=404)

        rs_delete = data_mysql().delete_account_ads(account_ads_id)
        hasil = rs_delete.get('hasil') if isinstance(rs_delete, dict) else None
        if not isinstance(hasil, dict):
            hasil = {
                'status': False,
                'message': 'Gagal menghapus account'
            }

        if hasil.get('status', False):
            try:
                from .utils import invalidate_cache_on_data_update
                invalidate_cache_on_data_update(existing_account.get('account_id'), event_type='account_delete')
            except Exception:
                pass

        return JsonResponse(hasil)

@method_decorator(csrf_exempt, name='dispatch') 
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
        except Exception:
            daily_budget = 0

        before_budget = None
        campaign_name = ''
        try:
            meta = fetch_campaign_meta(str(rs_data_account['access_token']), str(campaign_id))
            mdata = meta.get('data') if isinstance(meta, dict) else None
            if isinstance(meta, dict) and meta.get('status') and mdata is not None:
                getter = getattr(mdata, 'get', None)
                if callable(getter):
                    campaign_name = getter('name') or ''
                    try:
                        before_budget = int(getter('daily_budget') or 0)
                    except Exception:
                        before_budget = None
        except Exception:
            before_budget = None

        data = fetch_daily_budget_per_campaign(
            str(rs_data_account['access_token']),
            str(rs_data_account['account_id']),
            str(campaign_id),
            daily_budget
        )

        try:
            after_budget = int((data or {}).get('daily_budget') or 0)
        except Exception:
            after_budget = None

        try:
            changed = (before_budget is None) or (after_budget is None) or (before_budget != after_budget)
            if changed:
                admin = req.session.get('hris_admin', {})
                user_id = admin.get('user_id')
                chat_id = ''
                try:
                    uresp = data_mysql().get_user_by_id(user_id)
                    urow = (uresp or {}).get('data') if isinstance(uresp, dict) else None
                    chat_id = get_telegram_chat_id_for_user(urow)
                except Exception:
                    chat_id = ''
                if not chat_id:
                    chat_id = (os.getenv('TELEGRAM_DEFAULT_CHAT_ID') or '').strip()
                if chat_id:
                    actor = admin.get('user_alias') or admin.get('user_name') or admin.get('user_mail') or '-'
                    nm = campaign_name or str(campaign_id)
                    acc_name = ''
                    try:
                        if isinstance(rs_data_account, dict):
                            acc_name = rs_data_account.get('account_name') or ''
                    except Exception:
                        acc_name = ''
                    if not acc_name:
                        acc_name = str(account_id or '-')
                    changed_at = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    before_txt = _format_idr_number(before_budget).replace('Rp ', 'Rp. ') if before_budget is not None else '-'
                    after_txt = _format_idr_number(after_budget).replace('Rp ', 'Rp. ') if after_budget is not None else '-'
                    msg = (
                        "Daily budget campaign berubah\n"
                        f"Campaign: {nm}\n"
                        f"Account: {acc_name}\n"
                        f"Sebelum: {before_txt}\n"
                        f"Sesudah: {after_txt}\n"
                        f"Oleh: {actor}\n"
                        f"Waktu: {changed_at}"
                    )
                    send_telegram_message_aiogram(chat_id, msg)
        except Exception:
            pass

        if (data or {}).get('daily_budget') is not None:
            from .utils import invalidate_cache_on_data_update
            invalidate_cache_on_data_update(rs_data_account['account_id'], campaign_id, 'budget_update')

        return JsonResponse({'daily_budget': (data or {}).get('daily_budget')})
    
@method_decorator(csrf_exempt, name='dispatch')    
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
                        before_status = None
                        campaign_name = ''
                        try:
                            meta = fetch_campaign_meta(str(account_data.get('access_token')), str(campaign_id))
                            mdata = meta.get('data') if isinstance(meta, dict) else None
                            getter = getattr(mdata, 'get', None)
                            if isinstance(meta, dict) and meta.get('status') and callable(getter):
                                campaign_name = getter('name') or ''
                                before_status = getter('status')
                        except Exception:
                            before_status = None

                        data = fetch_status_per_campaign(
                            str(account_data['access_token']),
                            str(campaign_id),
                            str(status)
                        )
                        if 'error' not in data:
                            try:
                                admin = req.session.get('hris_admin', {})
                                user_id = admin.get('user_id')
                                chat_id = ''
                                try:
                                    uresp = data_mysql().get_user_by_id(user_id)
                                    urow = (uresp or {}).get('data') if isinstance(uresp, dict) else None
                                    chat_id = get_telegram_chat_id_for_user(urow)
                                except Exception:
                                    chat_id = ''
                                if not chat_id:
                                    chat_id = (os.getenv('TELEGRAM_DEFAULT_CHAT_ID') or '').strip()
                                if chat_id:
                                    actor = admin.get('user_alias') or admin.get('user_name') or admin.get('user_mail') or '-'
                                    changed_at = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                                    nm = (data.get('name') or campaign_name or str(campaign_id))
                                    acc_name = str(account_data.get('account_name') or account_data.get('account_id') or '-')
                                    after_status = data.get('status')
                                    msg = (
                                        "Status campaign berubah\n"
                                        f"Campaign: {nm}\n"
                                        f"Account: {acc_name}\n"
                                        f"Sebelum: {before_status if before_status is not None else '-'}\n"
                                        f"Sesudah: {after_status if after_status is not None else '-'}\n"
                                        f"Oleh: {actor}\n"
                                        f"Waktu: {changed_at}"
                                    )
                                    send_telegram_message_aiogram(chat_id, msg)
                            except Exception:
                                pass

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

                before_status = None
                campaign_name = ''
                try:
                    meta = fetch_campaign_meta(str(rs_data_account.get('access_token')), str(campaign_id))
                    mdata = meta.get('data') if isinstance(meta, dict) else None
                    getter = getattr(mdata, 'get', None)
                    if isinstance(meta, dict) and meta.get('status') and callable(getter):
                        campaign_name = getter('name') or ''
                        before_status = getter('status')
                except Exception:
                    before_status = None

                data = fetch_status_per_campaign(str(rs_data_account['access_token']), str(campaign_id), str(status))

                if 'error' in data:
                    return JsonResponse({
                        'success': False,
                        'message': f'Gagal mengupdate campaign: {data["error"]}'
                    })

                try:
                    admin = req.session.get('hris_admin', {})
                    user_id = admin.get('user_id')
                    chat_id = ''
                    try:
                        uresp = data_mysql().get_user_by_id(user_id)
                        urow = (uresp or {}).get('data') if isinstance(uresp, dict) else None
                        chat_id = get_telegram_chat_id_for_user(urow)
                    except Exception:
                        chat_id = ''
                    if not chat_id:
                        chat_id = (os.getenv('TELEGRAM_DEFAULT_CHAT_ID') or '').strip()
                    if chat_id:
                        actor = admin.get('user_alias') or admin.get('user_name') or admin.get('user_mail') or '-'
                        changed_at = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                        nm = (data.get('name') or campaign_name or str(campaign_id))
                        acc_name = str(rs_data_account.get('account_name') or account_id or '-')
                        after_status = data.get('status')
                        msg = (
                            "Status campaign berubah\n"
                            f"Campaign: {nm}\n"
                            f"Account: {acc_name}\n"
                            f"Sebelum: {before_status if before_status is not None else '-'}\n"
                            f"Sesudah: {after_status if after_status is not None else '-'}\n"
                            f"Oleh: {actor}\n"
                            f"Waktu: {changed_at}"
                        )
                        send_telegram_message_aiogram(chat_id, msg)
                except Exception:
                    pass

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

@method_decorator(csrf_exempt, name='dispatch')
class create_campaign_per_account(View):
    def dispatch(self, request, *args, **kwargs):
        if 'hris_admin' not in request.session:
            return JsonResponse({'success': False, 'message': 'Unauthorized'}, status=401)
        return super(create_campaign_per_account, self).dispatch(request, *args, **kwargs)

    def post(self, req):
        try:
            account_id = str(req.POST.get('account_id') or '').strip()
            campaign_name = str(req.POST.get('campaign_name') or '').strip()
            objective = str(req.POST.get('objective') or 'OUTCOME_TRAFFIC').strip().upper()
            status = str(req.POST.get('status') or 'PAUSED').strip().upper()
            buying_type = str(req.POST.get('buying_type') or 'AUCTION').strip().upper()
            special_ad_category = str(req.POST.get('special_ad_category') or 'NONE').strip().upper()
            campaign_budget_type = str(req.POST.get('campaign_budget_type') or 'none').strip().lower()
            campaign_daily_budget = str(req.POST.get('campaign_daily_budget') or '').strip()
            campaign_lifetime_budget = str(req.POST.get('campaign_lifetime_budget') or '').strip()
            campaign_spend_cap = str(req.POST.get('campaign_spend_cap') or '').strip()

            if not account_id:
                return JsonResponse({'success': False, 'message': 'Account wajib dipilih'})
            if not campaign_name:
                return JsonResponse({'success': False, 'message': 'Nama campaign wajib diisi'})

            allowed_status = {'PAUSED', 'ACTIVE'}
            if status not in allowed_status:
                status = 'PAUSED'

            rs = data_mysql().master_account_ads_by_id({'data_account': account_id})
            acc = (rs or {}).get('data') if isinstance(rs, dict) else None
            if not isinstance(acc, dict):
                return JsonResponse({'success': False, 'message': 'Account tidak ditemukan'})

            token = str(acc.get('access_token') or '').strip()
            real_account_id = str(acc.get('account_id') or account_id).replace('act_', '').strip()
            if not token or not real_account_id:
                return JsonResponse({'success': False, 'message': 'Token atau Account ID tidak valid'})

            url = f"https://graph.facebook.com/v22.0/act_{real_account_id}/campaigns"
            special_ad_categories = [] if special_ad_category in ('', 'NONE') else [special_ad_category]
            payload = {
                'access_token': token,
                'name': campaign_name,
                'objective': objective,
                'status': status,
                'buying_type': buying_type,
                'special_ad_categories': json.dumps(special_ad_categories),
            }
            if campaign_budget_type == 'daily' and campaign_daily_budget:
                payload['daily_budget'] = str(max(1000, int(float(campaign_daily_budget or 0))))
            elif campaign_budget_type == 'lifetime' and campaign_lifetime_budget:
                payload['lifetime_budget'] = str(max(1000, int(float(campaign_lifetime_budget or 0))))
            if campaign_spend_cap:
                payload['spend_cap'] = str(max(0, int(float(campaign_spend_cap or 0))))
            resp = requests.post(url, data=payload, timeout=45)
            body = resp.json() if resp.text else {}

            if resp.status_code >= 400 or (isinstance(body, dict) and body.get('error')):
                err = (body.get('error') or {}).get('message') if isinstance(body, dict) else ''
                return JsonResponse({'success': False, 'message': err or f'Graph API error ({resp.status_code})'})

            campaign_id = str((body or {}).get('id') or '').strip()
            return JsonResponse({
                'success': True,
                'message': 'Campaign berhasil dibuat',
                'campaign_id': campaign_id,
                'campaign_name': campaign_name,
                'objective': objective,
                'status': status,
            })
        except Exception as e:
            return JsonResponse({'success': False, 'message': f'Gagal membuat campaign: {str(e)}'})

@method_decorator(csrf_exempt, name='dispatch')
class create_campaign_fullstack_per_account(View):
    def dispatch(self, request, *args, **kwargs):
        if 'hris_admin' not in request.session:
            return JsonResponse({'success': False, 'message': 'Unauthorized'}, status=401)
        return super(create_campaign_fullstack_per_account, self).dispatch(request, *args, **kwargs)

    def post(self, req):
        try:
            account_id = str(req.POST.get('account_id') or '').strip()
            campaign_name = str(req.POST.get('campaign_name') or '').strip()
            objective = str(req.POST.get('objective') or 'OUTCOME_TRAFFIC').strip().upper()
            status = str(req.POST.get('status') or 'PAUSED').strip().upper()
            buying_type = str(req.POST.get('buying_type') or 'AUCTION').strip().upper()
            adset_name = str(req.POST.get('adset_name') or f'{campaign_name} - ADSET 1').strip()
            ad_name = str(req.POST.get('ad_name') or f'{campaign_name} - AD 1').strip()
            page_id = str(req.POST.get('page_id') or '').strip()
            website_url = str(req.POST.get('website_url') or '').strip()
            primary_text = str(req.POST.get('primary_text') or '').strip()
            headline = str(req.POST.get('headline') or '').strip()
            description = str(req.POST.get('description') or '').strip()
            caption = str(req.POST.get('caption') or '').strip()
            display_link = str(req.POST.get('display_link') or '').strip()
            url_tags = str(req.POST.get('url_tags') or '').strip()
            instagram_actor_id = str(req.POST.get('instagram_actor_id') or '').strip()
            use_existing_post = str(req.POST.get('use_existing_post') or '0').strip()
            existing_post_id = str(req.POST.get('existing_post_id') or '').strip()
            cta_type = str(req.POST.get('cta_type') or 'LEARN_MORE').strip().upper()
            countries_raw = str(req.POST.get('countries') or 'ID').strip()
            pixel_id = str(req.POST.get('pixel_id') or '').strip()

            try:
                daily_budget = int(float(req.POST.get('daily_budget') or 50000))
            except Exception:
                daily_budget = 50000
            try:
                age_min = int(req.POST.get('age_min') or 18)
            except Exception:
                age_min = 18
            try:
                age_max = int(req.POST.get('age_max') or 65)
            except Exception:
                age_max = 65

            if not account_id or not campaign_name:
                return JsonResponse({'success': False, 'message': 'Account dan nama campaign wajib diisi'})
            if not page_id:
                return JsonResponse({'success': False, 'message': 'Page ID wajib diisi untuk membuat ad'})
            if not website_url:
                return JsonResponse({'success': False, 'message': 'Website URL wajib diisi untuk membuat ad'})

            rs = data_mysql().master_account_ads_by_id({'data_account': account_id})
            acc = (rs or {}).get('data') if isinstance(rs, dict) else None
            if not isinstance(acc, dict):
                return JsonResponse({'success': False, 'message': 'Account tidak ditemukan'})

            token = str(acc.get('access_token') or '').strip()
            real_account_id = str(acc.get('account_id') or account_id).replace('act_', '').strip()
            if not token or not real_account_id:
                return JsonResponse({'success': False, 'message': 'Token atau Account ID tidak valid'})

            countries = [str(x).strip().upper() for x in countries_raw.split(',') if str(x).strip()]
            if not countries:
                countries = ['ID']

            def _graph_post(path, payload):
                p = dict(payload or {})
                p['access_token'] = token
                resp = requests.post(f"https://graph.facebook.com/v22.0/{str(path).lstrip('/')}", data=p, timeout=45)
                body = resp.json() if resp.text else {}
                if resp.status_code >= 400 or (isinstance(body, dict) and body.get('error')):
                    err = (body.get('error') or {}).get('message') if isinstance(body, dict) else ''
                    return {'ok': False, 'data': body, 'error': err or f'Graph API error ({resp.status_code})'}
                return {'ok': True, 'data': body, 'error': ''}

            camp_rs = _graph_post(f'act_{real_account_id}/campaigns', {
                'name': campaign_name,
                'objective': objective,
                'status': status,
                'buying_type': buying_type,
                'special_ad_categories': '[]',
            })
            if not camp_rs['ok']:
                return JsonResponse({'success': False, 'step': 'campaign', 'message': camp_rs['error']})
            campaign_id = str((camp_rs['data'] or {}).get('id') or '').strip()

            targeting = {'geo_locations': {'countries': countries}, 'age_min': max(13, age_min), 'age_max': max(age_min, age_max)}
            adset_payload = {
                'name': adset_name,
                'campaign_id': campaign_id,
                'daily_budget': str(max(1000, daily_budget)),
                'billing_event': 'IMPRESSIONS',
                'optimization_goal': 'LINK_CLICKS',
                'bid_strategy': 'LOWEST_COST_WITHOUT_CAP',
                'status': status,
                'targeting': json.dumps(targeting),
            }
            if pixel_id:
                adset_payload['promoted_object'] = json.dumps({'pixel_id': pixel_id, 'custom_event_type': 'PAGE_VIEW'})

            adset_rs = _graph_post(f'act_{real_account_id}/adsets', adset_payload)
            if not adset_rs['ok']:
                return JsonResponse({'success': False, 'step': 'adset', 'campaign_id': campaign_id, 'message': adset_rs['error']})
            adset_id = str((adset_rs['data'] or {}).get('id') or '').strip()

            link_data = {'link': website_url, 'call_to_action': {'type': cta_type, 'value': {'link': website_url}}}
            if primary_text:
                link_data['message'] = primary_text
            if headline:
                link_data['name'] = headline
            creative_rs = _graph_post(f'act_{real_account_id}/adcreatives', {
                'name': f'{ad_name} - CREATIVE',
                'object_story_spec': json.dumps({'page_id': page_id, 'link_data': link_data}),
            })
            if not creative_rs['ok']:
                return JsonResponse({'success': False, 'step': 'creative', 'campaign_id': campaign_id, 'adset_id': adset_id, 'message': creative_rs['error']})
            creative_id = str((creative_rs['data'] or {}).get('id') or '').strip()

            ad_rs = _graph_post(f'act_{real_account_id}/ads', {
                'name': ad_name,
                'adset_id': adset_id,
                'creative': json.dumps({'creative_id': creative_id}),
                'status': status,
            })
            if not ad_rs['ok']:
                return JsonResponse({'success': False, 'step': 'ad', 'campaign_id': campaign_id, 'adset_id': adset_id, 'creative_id': creative_id, 'message': ad_rs['error']})
            ad_id = str((ad_rs['data'] or {}).get('id') or '').strip()

            return JsonResponse({
                'success': True,
                'message': 'Campaign + Ad Set + Ad berhasil dibuat',
                'campaign_id': campaign_id,
                'adset_id': adset_id,
                'creative_id': creative_id,
                'ad_id': ad_id,
            })
        except Exception as e:
            return JsonResponse({'success': False, 'message': f'Gagal create full stack: {str(e)}'})

@method_decorator(csrf_exempt, name='dispatch')
class create_adset_ad_per_account(View):
    def dispatch(self, request, *args, **kwargs):
        if 'hris_admin' not in request.session:
            return JsonResponse({'success': False, 'message': 'Unauthorized'}, status=401)
        return super(create_adset_ad_per_account, self).dispatch(request, *args, **kwargs)

    def post(self, req):
        try:
            account_id = str(req.POST.get('account_id') or '').strip()
            campaign_id = str(req.POST.get('campaign_id') or '').strip()
            status = str(req.POST.get('status') or 'PAUSED').strip().upper()
            adset_name = str(req.POST.get('adset_name') or f'ADSET {campaign_id}').strip()
            ad_name = str(req.POST.get('ad_name') or f'AD {campaign_id}').strip()
            page_id = str(req.POST.get('page_id') or '').strip()
            website_url = str(req.POST.get('website_url') or '').strip()
            primary_text = str(req.POST.get('primary_text') or '').strip()
            headline = str(req.POST.get('headline') or '').strip()
            cta_type = str(req.POST.get('cta_type') or 'LEARN_MORE').strip().upper()
            countries_raw = str(req.POST.get('countries') or 'ID').strip()
            pixel_id = str(req.POST.get('pixel_id') or '').strip()
            daily_budget = int(float(req.POST.get('daily_budget') or 50000))
            lifetime_budget_raw = str(req.POST.get('lifetime_budget') or '').strip()
            budget_type = str(req.POST.get('budget_type') or 'daily').strip().lower()
            start_time = str(req.POST.get('start_time') or '').strip()
            end_time = str(req.POST.get('end_time') or '').strip()
            conversion_location = str(req.POST.get('conversion_location') or 'WEBSITE').strip().upper()
            optimization_goal = str(req.POST.get('optimization_goal') or 'LINK_CLICKS').strip().upper()
            bid_strategy = str(req.POST.get('bid_strategy') or 'LOWEST_COST_WITHOUT_CAP').strip().upper()
            bid_amount_raw = str(req.POST.get('bid_amount') or '').strip()
            attribution_window = str(req.POST.get('attribution_window') or '7d_click_1d_view').strip().lower()
            dynamic_creative = str(req.POST.get('dynamic_creative') or '0').strip()
            gender = str(req.POST.get('gender') or 'all').strip().lower()
            advantage = str(req.POST.get('advantage') or '0').strip()
            placement_mode = str(req.POST.get('placement_mode') or 'auto').strip().lower()
            age_min = int(req.POST.get('age_min') or 18)
            age_max = int(req.POST.get('age_max') or 65)

            if not account_id or not campaign_id:
                return JsonResponse({'success': False, 'message': 'Account dan Campaign ID wajib diisi'})
            if not page_id or not website_url:
                return JsonResponse({'success': False, 'message': 'Page ID dan Website URL wajib diisi'})

            rs = data_mysql().master_account_ads_by_id({'data_account': account_id})
            acc = (rs or {}).get('data') if isinstance(rs, dict) else None
            if not isinstance(acc, dict):
                return JsonResponse({'success': False, 'message': 'Account tidak ditemukan'})
            token = str(acc.get('access_token') or '').strip()
            real_account_id = str(acc.get('account_id') or account_id).replace('act_', '').strip()

            countries = [str(x).strip().upper() for x in countries_raw.split(',') if str(x).strip()] or ['ID']
            targeting = {'geo_locations': {'countries': countries}, 'age_min': max(13, age_min), 'age_max': max(age_min, age_max)}
            if gender == 'male':
                targeting['genders'] = [1]
            elif gender == 'female':
                targeting['genders'] = [2]
            if placement_mode == 'manual':
                targeting['publisher_platforms'] = ['facebook', 'instagram', 'audience_network', 'messenger']
            if advantage == '1':
                targeting['targeting_automation'] = {'advantage_audience': 1}

            def _post(path, payload):
                p = dict(payload or {}); p['access_token'] = token
                r = requests.post(f"https://graph.facebook.com/v22.0/{str(path).lstrip('/')}", data=p, timeout=45)
                b = r.json() if r.text else {}
                if r.status_code >= 400 or (isinstance(b, dict) and b.get('error')):
                    return False, (b.get('error') or {}).get('message') if isinstance(b, dict) else f'Graph API error ({r.status_code})', b
                return True, '', b

            adset_payload = {
                'name': adset_name,
                'campaign_id': campaign_id,
                'billing_event': 'IMPRESSIONS',
                'optimization_goal': optimization_goal,
                'bid_strategy': bid_strategy,
                'status': status,
                'targeting': json.dumps(targeting),
                'destination_type': conversion_location,
            }
            if budget_type == 'lifetime' and lifetime_budget_raw:
                adset_payload['lifetime_budget'] = str(max(1000, int(float(lifetime_budget_raw or 0))))
            else:
                adset_payload['daily_budget'] = str(max(1000, daily_budget))
            if start_time:
                adset_payload['start_time'] = start_time
            if end_time:
                adset_payload['end_time'] = end_time
            if bid_amount_raw:
                adset_payload['bid_amount'] = str(int(float(bid_amount_raw)))
            if attribution_window in ('1d_click', '7d_click', '7d_click_1d_view'):
                if attribution_window == '1d_click':
                    adset_payload['attribution_spec'] = json.dumps([{'event_type': 'CLICK_THROUGH', 'window_days': 1}])
                elif attribution_window == '7d_click':
                    adset_payload['attribution_spec'] = json.dumps([{'event_type': 'CLICK_THROUGH', 'window_days': 7}])
                else:
                    adset_payload['attribution_spec'] = json.dumps([
                        {'event_type': 'CLICK_THROUGH', 'window_days': 7},
                        {'event_type': 'VIEW_THROUGH', 'window_days': 1}
                    ])
            if dynamic_creative == '1':
                adset_payload['is_dynamic_creative'] = 'true'
            if pixel_id:
                adset_payload['promoted_object'] = json.dumps({'pixel_id': pixel_id, 'custom_event_type': 'PAGE_VIEW'})

            ok, err, adset_rs = _post(f'act_{real_account_id}/adsets', adset_payload)
            if not ok:
                return JsonResponse({'success': False, 'step': 'adset', 'message': err})
            adset_id = str((adset_rs or {}).get('id') or '').strip()

            creative_payload = {'name': f'{ad_name} - CREATIVE'}
            if url_tags:
                creative_payload['url_tags'] = url_tags
            if use_existing_post == '1' and existing_post_id:
                creative_payload['object_story_id'] = existing_post_id
            else:
                link_data = {'link': website_url, 'call_to_action': {'type': cta_type, 'value': {'link': website_url}}}
                if primary_text:
                    link_data['message'] = primary_text
                if headline:
                    link_data['name'] = headline
                if description:
                    link_data['description'] = description
                if caption:
                    link_data['caption'] = caption
                if display_link:
                    link_data['display_link'] = display_link
                object_story_spec = {'page_id': page_id, 'link_data': link_data}
                if instagram_actor_id:
                    object_story_spec['instagram_actor_id'] = instagram_actor_id
                creative_payload['object_story_spec'] = json.dumps(object_story_spec)
            ok, err, creative_rs = _post(f'act_{real_account_id}/adcreatives', creative_payload)
            if not ok:
                return JsonResponse({'success': False, 'step': 'creative', 'adset_id': adset_id, 'message': err})
            creative_id = str((creative_rs or {}).get('id') or '').strip()

            ok, err, ad_rs = _post(f'act_{real_account_id}/ads', {
                'name': ad_name, 'adset_id': adset_id, 'creative': json.dumps({'creative_id': creative_id}), 'status': status
            })
            if not ok:
                return JsonResponse({'success': False, 'step': 'ad', 'adset_id': adset_id, 'creative_id': creative_id, 'message': err})

            return JsonResponse({'success': True, 'message': 'Ad Set + Ad berhasil dibuat', 'campaign_id': campaign_id, 'adset_id': adset_id, 'creative_id': creative_id, 'ad_id': str((ad_rs or {}).get('id') or '')})
        except Exception as e:
            return JsonResponse({'success': False, 'message': f'Gagal membuat adset/ad: {str(e)}'})

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
    
class AdsCampaignListView(View):
    """AJAX endpoint untuk mengambil daftar situs dari Facebook Ads Manager"""
    def dispatch(self, request, *args, **kwargs):
        if 'hris_admin' not in request.session:
            return redirect('admin_login')
        return super().dispatch(request, *args, **kwargs)
    def get(self, req):
        selected_accounts = req.GET.get('selected_accounts')
        if selected_accounts:
            ads_id = selected_accounts
        else:
            ads_id = req.session.get('hris_admin', {}).get('ads_id')
        try:
            # Ambil daftar campaign dari Facebook Ads Manager jika cache miss
            result = data_mysql().fetch_ads_campaign_list(
                ads_id
            )
            # Simpan ke cache untuk permintaan berikutnya
            try:
                # Cache selama 6 jam; daftar situs jarang berubah
                set_cached_data(cache_key, result['hasil'], timeout=6 * 60 * 60)
            except Exception as _cache_set_err:
                print(f"[WARNING] failed to cache ads_sites_list: {_cache_set_err}")
            return JsonResponse(result['hasil'], safe=False)
        except Exception as e:
            return JsonResponse({
                'status': False,
                'error': str(e)
            })

class AdsSitesListView(View):
    """AJAX endpoint untuk mengambil daftar situs dari Facebook Ads Manager"""
    def dispatch(self, request, *args, **kwargs):
        if 'hris_admin' not in request.session:
            return redirect('admin_login')
        return super().dispatch(request, *args, **kwargs)
    def get(self, req):
        selected_accounts = req.GET.get('selected_accounts')
        selected_account_list = []
        if selected_accounts:
            selected_account_list = [str(s).strip() for s in selected_accounts.split(',') if s.strip()]
        if selected_account_list:
            ads_id = selected_account_list  
        else:
            ads_id = req.session.get('hris_admin', {}).get('ads_id')
        try:
            # Ambil daftar situs dari Facebook Ads Manager jika cache miss
            result = data_mysql().fetch_ads_sites_list(
                ads_id
            )
            # Simpan ke cache untuk permintaan berikutnya
            try:
                # Cache selama 6 jam; daftar situs jarang berubah
                set_cached_data(cache_key, result['hasil'], timeout=6 * 60 * 60)
            except Exception as _cache_set_err:
                print(f"[WARNING] failed to cache ads_sites_list: {_cache_set_err}")
            return JsonResponse(result['hasil'], safe=False)
        except Exception as e:
            return JsonResponse({
                'status': False,
                'error': str(e)
            })

class AdsAccountListView(View):
    """AJAX endpoint untuk mengambil daftar situs dari Facebook Ads Manager"""
    def dispatch(self, request, *args, **kwargs):
        if 'hris_admin' not in request.session:
            return redirect('admin_login')
        return super().dispatch(request, *args, **kwargs)
    def get(self, req):
        selected_domains = req.GET.get('selected_domains')
        selected_domain_list = []
        if selected_domains:
            selected_domain_list = [str(s).strip() for s in selected_domains.split(',') if s.strip()]
        try:
            # Ambil daftar account dari Facebook Ads Manager jika cache miss
            result = data_mysql().fetch_ads_account_list(
                selected_domain_list
            )
            # Simpan ke cache untuk permintaan berikutnya
            try:
                # Cache selama 6 jam; daftar situs jarang berubah
                set_cached_data(cache_key, result['hasil'], timeout=6 * 60 * 60)
            except Exception as _cache_set_err:
                print(f"[WARNING] failed to cache ads_sites_list: {_cache_set_err}")
            return JsonResponse(result['hasil'], safe=False)
        except Exception as e:
            return JsonResponse({
                'status': False,
                'error': str(e)
            })

class PerCampaignFacebookAds(View):
    def dispatch(self, request, *args, **kwargs):
        if 'hris_admin' not in request.session:
            return redirect('admin_login')
        return super(PerCampaignFacebookAds, self).dispatch(request, *args, **kwargs)
    def get(self, req):
        data_account = data_mysql().master_account_ads()['data']
        data_domain = data_mysql().master_domain_ads()['data']
        last_update = data_mysql().get_last_update_ads_traffic_per_domain()['data']['last_update']
        data = {
            'title': 'Data Traffic Per Campaign Facebook Ads',
            'user': req.session['hris_admin'],  
            'last_update': last_update,
            'data_account': data_account,
            'data_domain': data_domain,
        }
        return render(req, 'admin/facebook_ads/campaign/index.html', data)

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
        selected_account_list = []
        if data_account:
            selected_account_list = [str(s).strip() for s in data_account.split(',') if s.strip()]
        data_domain = req.GET.get('data_domain')
        selected_domain_list = []
        if data_domain:
            selected_domain_list = [str(s).strip() for s in data_domain.split(',') if s.strip()]
        
        # rs_account = data_mysql().master_account_ads()
        # if (data_sub_domain != '%' or data_sub_domain == '%') and data_account != '%':
        #     rs_data_account = data_mysql().master_account_ads_by_id({
        #         'data_account': data_account,
        #     })['data']
        #     data = fetch_data_insights_campaign_filter_account(str(tanggal_dari), str(tanggal_sampai), str(rs_data_account['access_token']), str(rs_data_account['account_id']), str(rs_data_account['account_name']), str(data_sub_domain))
        # else:  
        #     data = fetch_data_insights_campaign_filter_sub_domain(str(tanggal_dari), str(tanggal_sampai), rs_account['data'], str(data_sub_domain))

        # Panggil ke database layer dengan argumen positional sesuai definisi fungsi
        db_result = data_mysql().get_all_ads_traffic_campaign_by_params(
            tanggal_dari,
            tanggal_sampai,
            selected_account_list,
            selected_domain_list,
        )
        # Unwrap payload (fungsi mengembalikan {'hasil': {...}})
        payload = db_result.get('hasil', {}) if isinstance(db_result, dict) else {}
        status_ok = bool(payload.get('status', False))
        raw_rows = payload.get('data', []) if status_ok else []
        # Normalisasi kolom agar cocok dengan harapan di management/static/ajax/admin/facebook_ads/campaign.js
        normalized_rows = []
        total_spend = 0.0
        total_impressions = 0
        total_reach = 0
        total_clicks = 0
        for row in raw_rows or []:
            account_name = row.get('account_name')
            domain = row.get('domain')
            campaign = row.get('campaign')

            spend = float(row.get('spend', 0) or 0)
            impressions = int(row.get('impressions', 0) or 0)
            reach = int(row.get('reach', 0) or 0)
            clicks = int(row.get('clicks', 0) or 0)
            cpr = float(row.get('cpr', 0) or 0)
            cpc = float(row.get('cpc', 0) or 0)

            frequency_val = row.get('frequency', None)
            if frequency_val in [None, '']:
                if reach == 0:
                    frequency = 0
                else:
                    frequency = float(format(impressions / reach, '.1f'))
            else:
                try:
                    frequency = float(frequency_val)
                except Exception:
                    frequency = 0

            lpv = float(row.get('lpv', 0) or 0)
            lpv_rate = float(row.get('lpv_rate', 0) or 0)

            normalized_rows.append({
                'date': row.get('date'),
                'account_id': row.get('account_id'),
                'account_name': account_name,
                'domain': domain,
                'campaign': campaign,
                'spend': spend,
                'impressions': impressions,
                'reach': reach,
                'clicks': clicks,
                'frequency': frequency,
                'cpr': cpr,
                'cpc': cpc,
                'lpv': lpv,
                'lpv_rate': lpv_rate,
            })
            # Akumulasi untuk total
            total_spend += spend
            total_impressions += impressions
            total_reach += reach
            total_clicks += clicks
        # Agregasi total: frequency total sebagai (impressions/reach)*100, CPR total sebagai spend/clicks
        if total_reach == 0:
            total_frequency = 0
        else:
            total_frequency = format(total_impressions / total_reach, '.1f')
        rata_cpr = round(sum([row['cpr'] for row in normalized_rows]) / len(normalized_rows), 0) if normalized_rows else 0.0
        rata_cpc = round(sum([row['cpc'] for row in normalized_rows]) / len(normalized_rows), 0) if normalized_rows else 0.0
        response_data = {
            'hasil': "Data Traffic Per Campaign",
            'data_campaign': normalized_rows,
            'total_campaign': [{
                'total_spend': total_spend,
                'total_impressions': total_impressions,
                'total_reach': total_reach,
                'total_click': total_clicks,
                'total_frequency': total_frequency,
                'total_cpr': format(rata_cpr, '.0f'),
                'total_cpc': format(rata_cpc, '.0f'),
            }],
        }
        # Jika terjadi kegagalan di layer DB, kirimkan respons kosong agar frontend tidak error
        if not status_ok:
            response_data['error'] = payload.get('data') or payload.get('message') or 'Gagal mengambil data campaign'
        return JsonResponse(response_data)

class page_per_campaign_facebook_detail(View):
    def dispatch(self, request, *args, **kwargs):
        if 'hris_admin' not in request.session:
            return redirect('admin_login')
        return super(page_per_campaign_facebook_detail, self).dispatch(request, *args, **kwargs)

    def get(self, req):
        account_id = str(req.GET.get('account_id') or '').strip()
        campaign_name = str(req.GET.get('campaign_name') or '').strip()
        start_date = str(req.GET.get('start_date') or '').strip()
        end_date = str(req.GET.get('end_date') or '').strip()
        if not account_id or not campaign_name:
            return JsonResponse({'status': False, 'error': 'account_id dan campaign_name wajib diisi', 'data': {}})

        acc = data_mysql().master_account_ads_by_id({'data_account': account_id})
        acc_data = (acc or {}).get('data') if isinstance(acc, dict) else None
        if not isinstance(acc_data, dict):
            return JsonResponse({'status': False, 'error': 'Account tidak ditemukan', 'data': {}})

        token = str(acc_data.get('access_token') or '').strip()
        real_account_id = str(acc_data.get('account_id') or account_id).replace('act_', '').strip()
        if not token or not real_account_id:
            return JsonResponse({'status': False, 'error': 'Token atau account_id tidak valid', 'data': {}})

        def _graph(path, params=None):
            q = {'access_token': token}
            if isinstance(params, dict): q.update(params)
            resp = requests.get(f"https://graph.facebook.com/v22.0/{str(path).lstrip('/')}", params=q, timeout=45)
            data = resp.json() if resp.text else {}
            if resp.status_code >= 400 or (isinstance(data, dict) and data.get('error')):
                msg = (data.get('error') or {}).get('message') if isinstance(data, dict) else f'HTTP {resp.status_code}'
                return {'ok': False, 'data': {}, 'error': msg or 'Graph API error'}
            return {'ok': True, 'data': data, 'error': ''}

        camp_rs = _graph(f'act_{real_account_id}/campaigns', {'fields': 'id,name,status,objective,daily_budget,lifetime_budget,buying_type,special_ad_categories', 'limit': 200})
        campaigns = (camp_rs.get('data') or {}).get('data', []) if isinstance(camp_rs.get('data'), dict) else []
        name_lc = campaign_name.lower()
        selected = next((c for c in campaigns if str((c or {}).get('name') or '').strip().lower() == name_lc), None)
        if selected is None:
            selected = next((c for c in campaigns if name_lc in str((c or {}).get('name') or '').strip().lower()), None)
        if not selected:
            return JsonResponse({'status': True, 'data': {'campaign': {'name': campaign_name}, 'platforms': [], 'ad_assets': [], 'adsets': []}, 'warning': 'Campaign tidak ditemukan pada API account ini'})

        campaign_id = str(selected.get('id') or '').strip()
        camp_meta = _graph(campaign_id, {
            'fields': 'id,name,status,effective_status,objective,buying_type,daily_budget,lifetime_budget,budget_remaining,start_time,stop_time,created_time,updated_time,special_ad_categories,special_ad_category_country,smart_promotion_type'
        })
        adsets_rs = _graph(f'{campaign_id}/adsets', {
            'fields': 'id,name,status,effective_status,optimization_goal,billing_event,bid_strategy,bid_amount,daily_budget,lifetime_budget,budget_remaining,start_time,end_time,destination_type,attribution_spec,promoted_object,targeting,frequency_control_specs',
            'limit': 500
        })
        ads_rs = _graph(f'{campaign_id}/ads', {
            'fields': 'id,name,status,effective_status,configured_status,conversion_domain,source_ad_id,tracking_specs,adset{id,name,status,effective_status},creative{id,name,title,body,object_story_spec,asset_feed_spec,url_tags,thumbnail_url,image_url,object_type,call_to_action_type,instagram_actor_id,effective_instagram_story_id}',
            'limit': 500
        })

        insight_params = {'fields': 'publisher_platform,platform_position,impressions,reach,clicks,spend', 'breakdowns': 'publisher_platform,platform_position', 'limit': 500}
        if start_date and end_date:
            insight_params['time_range'] = json.dumps({'since': start_date, 'until': end_date})
        insights_rs = _graph(f'{campaign_id}/insights', insight_params)

        adsets = ((adsets_rs.get('data') or {}).get('data', []) if isinstance(adsets_rs.get('data'), dict) else [])
        ads = ((ads_rs.get('data') or {}).get('data', []) if isinstance(ads_rs.get('data'), dict) else [])
        insights = ((insights_rs.get('data') or {}).get('data', []) if isinstance(insights_rs.get('data'), dict) else [])
        adset_map = {str((x or {}).get('id') or ''): x for x in adsets}

        platforms = []
        seen_platform = set()
        for it in insights:
            pp = str((it or {}).get('publisher_platform') or '').strip()
            pos = str((it or {}).get('platform_position') or '').strip()
            label = ' / '.join([x for x in [pp, pos] if x])
            if label and label not in seen_platform:
                seen_platform.add(label)
                platforms.append(label)

        campaign_full = (camp_meta.get('data') if isinstance(camp_meta.get('data'), dict) else selected) or {}
        ad_assets = []
        for ad in ads:
            adset_obj = (ad or {}).get('adset') or {}
            adset_id = str(adset_obj.get('id') or '')
            adset_meta = adset_map.get(adset_id, {})
            targeting = adset_meta.get('targeting') or {}
            advantage = targeting.get('targeting_automation') if isinstance(targeting, dict) else None
            creative = (ad or {}).get('creative') or {}
            ad_assets.append({
                'ad_id': ad.get('id'),
                'ad_name': ad.get('name'),
                'identity': {'account_id': real_account_id, 'campaign_id': campaign_id, 'adset_id': adset_id, 'adset_name': adset_obj.get('name') or adset_meta.get('name')},
                'platforms': platforms,
                'objective': campaign_full.get('objective') or selected.get('objective'),
                'advantage_audience': advantage,
                'targeting': targeting,
                'campaign': campaign_full,
                'adset': adset_meta,
                'ad': {
                    'id': ad.get('id'),
                    'name': ad.get('name'),
                    'status': ad.get('status'),
                    'effective_status': ad.get('effective_status'),
                    'configured_status': ad.get('configured_status'),
                    'conversion_domain': ad.get('conversion_domain'),
                    'source_ad_id': ad.get('source_ad_id'),
                    'tracking_specs': ad.get('tracking_specs')
                },
                'material': creative
            })

        return JsonResponse({'status': True, 'data': {'campaign': campaign_full, 'platforms': platforms, 'adsets': adsets, 'ad_assets': ad_assets}})
    
class PerCountryFacebookAds(View):
    def dispatch(self, request, *args, **kwargs):
        if 'hris_admin' not in request.session:
            return redirect('admin_login')
        return super(PerCountryFacebookAds, self).dispatch(request, *args, **kwargs)
    def get(self, req):
        data_account = data_mysql().master_account_ads()['data']
        data_domain = data_mysql().master_domain_ads()['data']
        last_update = data_mysql().get_last_update_ads_traffic_country()['data']['last_update']
        data = {
            'title': 'Data Traffic Per Country Facebook Ads',
            'user': req.session['hris_admin'],
            'last_update': last_update,
            'data_account': data_account,
            'data_domain': data_domain,
        }
        return render(req, 'admin/facebook_ads/country/index.html', data)
    
class page_per_country_facebook(View):
    def dispatch(self, request, *args, **kwargs):
        if 'hris_admin' not in request.session:
            return redirect('admin_login')
        elif 'hris_admin' not in request.session:
            return redirect('user_login')
        return super(page_per_country_facebook, self).dispatch(request, *args, **kwargs)
    def post(self, req):
        tanggal_dari = req.POST.get('tanggal_dari') 
        tanggal_sampai = req.POST.get('tanggal_sampai')
        data_account = req.POST.get('data_account')
        selected_account_list = []
        if data_account:
            selected_account_list = [str(s).strip() for s in data_account.split(',') if s.strip()]
        data_domain = req.POST.get('data_domain')
        selected_domain_list = []
        if data_domain:
            selected_domain_list = [str(s).strip() for s in data_domain.split(',') if s.strip()]

        selected_countries_json = req.POST.get('selected_countries', '[]')
        try:
            selected_countries = json.loads(selected_countries_json)
        except Exception:
            selected_countries = []
        db_resp = data_mysql().get_all_ads_traffic_country_by_params(
            tanggal_dari,
            tanggal_sampai,
            selected_account_list,
            selected_domain_list,
            selected_countries
        )
        data_rows = db_resp.get('data') if isinstance(db_resp, dict) else []
        # Normalisasi rows ke format yang diharapkan JS
        normalized = []
        total_spend = 0.0
        total_impressions = 0
        total_reach = 0
        total_clicks = 0
        frequency_total = 0.0
        rata_cpr_ratio = 0.0
        rata_cpc_ratio = 0.0
        for r in (data_rows or []):
            country_name = (r.get('country_name') or '').strip()
            country_code = (r.get('country_code') or '').strip().upper()
            country_label = f"{country_name} ({country_code})" if country_code else country_name
            spend = float(r.get('spend') or 0)
            impressions = int(r.get('impressions') or 0)
            reach = int(r.get('reach') or 0)
            clicks = int(r.get('clicks') or 0)
            if reach:
                frequency = format(impressions / reach, '.1f')
            else:
                frequency = 0
            normalized.append({
                'country': country_label,
                'country_name': country_name,
                'country_code': country_code,
                'spend': spend,
                'impressions': impressions,
                'reach': reach,
                'clicks': clicks,
                'frequency': frequency,
                'cpr': round(float(r.get('cpr') or 0), 0),
                'cpc': round(float(r.get('cpc') or 0), 0),
            })
            total_spend += spend
            total_impressions += impressions
            total_reach += reach
            total_clicks += clicks

        frequency_total = format(total_impressions / total_reach, '.1f') if total_reach else 0
        rata_cpr_ratio = format(sum((row.get('cpr') or 0) for row in normalized) / len(normalized), '.0f') if normalized else '0.0'
        rata_cpc_ratio = format(sum((row.get('cpc') or 0) for row in normalized) / len(normalized), '.0f') if normalized else '0.0'
        data = {
            'data': normalized,
            'total': {
                'impressions': total_impressions,
                'spend': total_spend,
                'clicks': total_clicks,
                'reach': total_reach,
                'frequency': frequency_total,
                'cpr': round(float(rata_cpr_ratio or 0), 0),
                'cpc': round(float(rata_cpc_ratio or 0), 0),
            }
        }
        # Normalize total structure jika berasal dari utils.py (berbentuk list)
        if isinstance(data, dict) and 'total' in data and isinstance(data['total'], list) and len(data['total']) > 0:
            original_total = data['total'][0]
            data['total'] = {
                'impressions': original_total.get('total_impressions', 0),
                'spend': original_total.get('total_spend', 0),
                'clicks': original_total.get('total_click', 0),
                'reach': original_total.get('total_reach', 0),
                'frequency': original_total.get('total_frequency', 0)
            }
        # Filter data berdasarkan negara yang dipilih
        if selected_countries and len(selected_countries) > 0:
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
                data['total'] = {
                    'impressions': total_impressions,
                    'spend': total_spend,
                    'clicks': total_clicks,
                    'reach': total_reach,
                    'frequency': frequency,
                }
            else:
                # Jika tidak ada data setelah filter, set total ke 0
                data['total'] = {
                    'impressions': 0,
                    'spend': 0,
                    'clicks': 0,
                    'reach': 0,
                    'frequency': 0
                }
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
        admin = req.session.get('hris_admin', {})
        if admin.get('super_st') == '0':
            data_account_adx = data_mysql().get_all_adx_account_data_user(admin.get('user_id'))
            data_domain_adx = data_mysql().get_all_adx_domain_data_user(admin.get('user_id'))
        else:
            data_account_adx = data_mysql().get_all_adx_account_data()
            data_domain_adx = data_mysql().get_all_adx_domain_data()
        if not data_domain_adx['status']:
            return JsonResponse({
                'status': False,
                'error': data_domain_adx['data']
            })
        data = {
            'title': 'AdX Summary Dashboard',
            'user': req.session['hris_admin'],
            'data_account_adx': data_account_adx['data'],
            'data_domain_adx': data_domain_adx['data']
        }
        return render(req, 'admin/adx_manager/summary/index.html', data)

class AdxDomainSuggestView(View):
    """AJAX endpoint suggest subdomain AdX (Select2)"""
    def dispatch(self, request, *args, **kwargs):
        if 'hris_admin' not in request.session:
            return redirect('admin_login')
        return super().dispatch(request, *args, **kwargs)

    def get(self, req):
        q = str(req.GET.get('q') or '').strip()
        start_date = str(req.GET.get('start_date') or '').strip()
        end_date = str(req.GET.get('end_date') or '').strip()
        selected_account = str(req.GET.get('selected_account') or '').strip()
        admin = req.session.get('hris_admin', {})

        if not q:
            return JsonResponse({'results': []})

        # Default range jika tidak dikirim
        if not start_date or not end_date:
            today = datetime.now().strftime('%Y-%m-%d')
            start_date = start_date or today
            end_date = end_date or today

        # Jika tidak pilih account, pakai semua account yang boleh diakses user
        if selected_account == '':
            rs_account = data_mysql().get_all_adx_account_data_user(admin.get('user_id')) if admin.get('super_st') == '0' else data_mysql().get_all_adx_account_data()
            rows = (rs_account or {}).get('data') if isinstance(rs_account, dict) else []
            if not isinstance(rows, list):
                rows = []
            account_ids = [str((r or {}).get('account_id') or '').strip() for r in rows if str((r or {}).get('account_id') or '').strip()]
            selected_account = ','.join(account_ids)

        account_list = [s.strip() for s in selected_account.split(',') if s.strip()]
        like = f"%{q.strip()}%"
        limit = 100

        account_tokens = []
        for a in account_list:
            v = str(a or '').strip()
            if not v:
                continue
            account_tokens.append(v)
            if v.lower().startswith('act_'):
                account_tokens.append(v[4:])
            else:
                account_tokens.append(f"act_{v}")
        account_tokens = list(dict.fromkeys([x for x in account_tokens if x]))

        db = data_mysql()
        rows = []

        # ClickHouse-first
        try:
            db._ensure_report_connection()
            db.cur_hris = db.report_cur
            where = [
                "toDate(b.data_adx_country_tanggal) BETWEEN toDate(%s) AND toDate(%s)",
                "lowerUTF8(b.data_adx_country_domain) LIKE lowerUTF8(%s)",
            ]
            params = [start_date, end_date, like]
            if account_tokens:
                acc_like = " OR ".join(["toString(b.account_id) LIKE %s"] * len(account_tokens))
                where.append(f"({acc_like})")
                params.extend([f"%{a}%" for a in account_tokens])
            sql = "\n".join([
                "SELECT DISTINCT b.data_adx_country_domain AS site_name",
                "FROM data_adx_country b",
                "WHERE " + " AND ".join(where),
                "ORDER BY site_name ASC",
                f"LIMIT {limit}",
            ])
            db.cur_hris.execute(sql, tuple(params))
            rows = db.fetch_all()
        except Exception:
            # Fallback MySQL
            try:
                if db.ensure_connection():
                    db.cur_hris = db.mysql_cur
                    where = [
                        "b.data_adx_country_tanggal BETWEEN %s AND %s",
                        "b.data_adx_country_domain LIKE %s",
                    ]
                    params = [start_date, end_date, like]
                    if account_tokens:
                        acc_like = " OR ".join(["b.account_id LIKE %s"] * len(account_tokens))
                        where.append(f"({acc_like})")
                        params.extend([f"%{a}%" for a in account_tokens])
                    sql = "\n".join([
                        "SELECT DISTINCT b.data_adx_country_domain AS site_name",
                        "FROM data_adx_country b",
                        "WHERE " + " AND ".join(where),
                        "ORDER BY site_name ASC",
                        f"LIMIT {limit}",
                    ])
                    db.cur_hris.execute(sql, tuple(params))
                    rows = db.fetch_all()
            except Exception:
                rows = []

        results = []
        seen = set()
        for r in (rows or []):
            site = str((r or {}).get('site_name') or '').strip()
            if not site:
                continue
            k = site.lower()
            if k in seen:
                continue
            seen.add(k)
            results.append({'id': site, 'text': site})
            if len(results) >= limit:
                break

        return JsonResponse({'results': results})


class AdxSummaryDataView(View):
    """AJAX endpoint untuk data AdX Summary"""
    def dispatch(self, request, *args, **kwargs):
        if 'hris_admin' not in request.session:
            return redirect('admin_login')
        return super().dispatch(request, *args, **kwargs)
    def get(self, req):
        start_date = req.GET.get('start_date')
        end_date = req.GET.get('end_date')
        selected_account = req.GET.get('selected_account', '')
        admin = req.session.get('hris_admin', {})
        if selected_account == '':
            rs_account = data_mysql().get_all_adx_account_data_user(admin.get('user_id'))
            if not isinstance(rs_account, dict) or not rs_account.get('status'):
                return JsonResponse({
                    'status': False,
                    'error': (rs_account or {}).get('data') or 'Gagal mengambil data account AdX'
                })
            rows = rs_account.get('data') or []
            if not isinstance(rows, list):
                rows = []
            account_ids = []
            for item in rows:
                aid = None
                try:
                    if isinstance(item, dict):
                        aid = item.get('account_id')
                except Exception:
                    aid = None
                if aid is not None and str(aid).strip():
                    account_ids.append(str(aid))
            selected_account = ",".join(account_ids)
        else:
            selected_account = req.GET.get('selected_account', '')
        account_list = []
        if selected_account:
            account_list = [str(s).strip() for s in selected_account.split(',') if s.strip()]
        selected_domain = req.GET.get('selected_domain')
        selected_domain_list = []
        if selected_domain:
            selected_domain_list = [str(s).strip() for s in selected_domain.split(',') if s.strip()]
        if not start_date or not end_date:      
            return JsonResponse({
                'status': False,
                'error': 'Start date and end date are required'
            })
        try:
            result = data_mysql().get_all_adx_traffic_account_by_params(start_date, end_date, account_list, selected_domain_list, force_clickhouse=True)
            # Unwrap format lama { 'hasil': ... } dan siapkan data
            payload = result['hasil'] if isinstance(result, dict) and 'hasil' in result else result
            data_rows = payload.get('data') if isinstance(payload, dict) else []
            if not isinstance(data_rows, list):
                data_rows = []
            # Agregasi summary untuk periode terpilih
            total_impressions = sum((row.get('impressions_adx') or 0) for row in data_rows) if data_rows else 0
            total_clicks = sum((row.get('clicks_adx') or 0) for row in data_rows) if data_rows else 0
            total_revenue = sum((row.get('revenue') or 0) for row in data_rows) if data_rows else 0.0
            avg_cpc = (float(total_revenue) / float(total_clicks)) if total_clicks else 0.0
            avg_ctr = ((float(total_clicks) / float(total_impressions)) * 100.0) if total_impressions else 0.0
            # Tambahkan data traffic hari ini
            today = datetime.now().strftime('%Y-%m-%d')
            today_result = data_mysql().get_all_adx_traffic_account_by_params(today, today, selected_account, selected_domain_list, force_clickhouse=True)
            today_payload = today_result['hasil'] if isinstance(today_result, dict) and 'hasil' in today_result else today_result
            today_rows = today_payload.get('data') if isinstance(today_payload, dict) else []
            if not isinstance(today_rows, list):
                today_rows = []
            today_impressions = sum((row.get('impressions_adx') or 0) for row in today_rows) if today_rows else 0
            today_clicks = sum((row.get('clicks_adx') or 0) for row in today_rows) if today_rows else 0
            today_revenue = sum((row.get('revenue') or 0) for row in today_rows) if today_rows else 0.0
            today_ctr = ((float(today_clicks) / float(today_impressions)) * 100.0) if today_impressions else 0.0
            domain_suggestions = []
            seen_domains = set()
            for row in data_rows:
                try:
                    site = str((row or {}).get('site_name') or '').strip()
                except Exception:
                    site = ''
                if not site:
                    continue
                key = site.lower()
                if key in seen_domains:
                    continue
                seen_domains.add(key)
                domain_suggestions.append(site)

            # Bangun respons konsisten untuk frontend
            response_data = {
                'status': bool(payload.get('status')) if isinstance(payload, dict) else True,
                'message': payload.get('message', 'Data adx summary berhasil diambil') if isinstance(payload, dict) else 'Data adx summary berhasil diambil',
                'data': data_rows,
                'domain_suggestions': domain_suggestions,
                'summary': {
                    'total_impressions': total_impressions,
                    'total_clicks': total_clicks,
                    'total_revenue': total_revenue,
                    'avg_cpc': avg_cpc,
                    'avg_ctr': avg_ctr
                },
                'today_traffic': {
                    'impressions': today_impressions,
                    'clicks': today_clicks,
                    'revenue': today_revenue,
                    'ctr': today_ctr
                }
            }
            # Jika payload memiliki status False, sertakan error bila ada
            if isinstance(payload, dict) and not payload.get('status'):
                response_data['status'] = False
                if 'data' in payload and isinstance(payload['data'], str):
                    response_data['error'] = payload['data']
            return JsonResponse(response_data)
        except Exception as e:
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
        
        # Handle site_filter - check both array format and string format
        site_filter_list = req.GET.getlist('site_filter[]')  # Array format
        site_filter_string = req.GET.get('site_filter', '')   # String format
        
        if site_filter_list:
            site_filter = ','.join(site_filter_list)
        else:
            site_filter = site_filter_string
        
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
            return JsonResponse(result)
            
        except Exception as e:
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
            return JsonResponse(result)
            
        except Exception as e:
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
        admin = req.session.get('hris_admin', {})
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
        if admin.get('super_st') == '0':
            data_account_adx = data_mysql().get_all_adx_account_data_user(admin.get('user_id'))
            result = data_mysql().get_all_app_credentials_user(admin.get('user_id'))
        else:
            data_account_adx = data_mysql().get_all_adx_account_data()
            result = data_mysql().get_all_app_credentials()
        if not data_account_adx['status']:
            return JsonResponse({
                'status': False,
                'error': data_account_adx['data']
            })
        if result.get('status'):
            credentials_data = result.get('data', [])
        else:
            credentials_data = []
        rs_users = data_mysql().data_user_by_params()
        if rs_users.get('status'):
            rs_users = rs_users.get('data', [])
        else:
            rs_users = []
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
            'rs_users': rs_users,
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
            if not user_mail:
                return JsonResponse({
                    'status': False,
                    'error': 'User Mail tidak ditemukan dalam session'
                })
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
            network_code = req.POST.get('network_code')
            user_mail = req.POST.get('user_mail')
            admin = req.session.get('hris_admin', {})
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
            
            if isinstance(exists, dict) and not exists.get('status', True):
                return JsonResponse({
                    'status': False,
                    'error': 'Gagal mengecek app_credentials di database'
                })

            if isinstance(exists, int) and exists > 0:
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
            else:
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

            if isinstance(result, dict) and result.get('status'):
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
        admin = req.session.get('hris_admin', {})
        if admin.get('super_st') == '0':
            data_account_adx = data_mysql().get_all_adx_account_data_user(admin.get('user_id'))
            data_domain_adx = data_mysql().get_all_adx_domain_data_user(admin.get('user_id'))
        else:
            data_account_adx = data_mysql().get_all_adx_account_data()
            data_domain_adx = data_mysql().get_all_adx_domain_data()
        if not data_domain_adx['status']:
            return JsonResponse({
                'status': False,
                'error': data_domain_adx['data']
            })
        last_update = data_mysql().get_last_update_adx_traffic_per_domain()['data']['last_update']
        data = {
            'title': 'AdX Traffic Per Account',
            'user': req.session['hris_admin'],
            'data_account_adx': data_account_adx['data'],
            'data_domain_adx': data_domain_adx['data'],
            'last_update': last_update
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
            logger.info(f"OAuth Start - GET parameters: {dict(req.GET)}")
            current_user = req.session.get('hris_admin', {})
            # Izinkan target email via query (?email=xxx); jika tidak ada, JANGAN paksa fallback ke email session
            target_mail = req.GET.get('email')
            user_id = current_user.get('user_id')
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
                'https://www.googleapis.com/auth/adsense',
                # Scope untuk sinkronisasi Gmail AdSense policy events
                # 'https://www.googleapis.com/auth/gmail.readonly'
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
                'https://www.googleapis.com/auth/adsense',
                # Scope untuk sinkronisasi Gmail AdSense policy events
                # 'https://www.googleapis.com/auth/gmail.readonly'
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
        selected_account = req.GET.get('selected_account')
        admin = req.session.get('hris_admin', {})
        if not selected_account:
            rs_account = data_mysql().get_all_adx_account_data_user(admin.get('user_id')) if admin.get('super_st') == '0' else data_mysql().get_all_adx_account_data()
            account_ids = [str(item['account_id']) for item in rs_account.get('data', [])]
            selected_account = ",".join(account_ids)
        else:
            selected_account = req.GET.get('selected_account', '')
        selected_account_list = []
        if selected_account:
            selected_account_list = [str(s).strip() for s in selected_account.split(',') if s.strip()]
        selected_domains = req.GET.get('selected_domains')
        selected_domain_list = []
        if selected_domains:
            selected_domain_list = [str(s).strip() for s in selected_domains.split(',') if s.strip()]
        if not start_date or not end_date:      
            return JsonResponse({
                'status': False,
                'error': 'Start date and end date are required'
            })
        try:
            # Format tanggal untuk AdManager API
            start_date_formatted = datetime.strptime(start_date, '%Y-%m-%d').strftime('%Y-%m-%d')
            end_date_formatted = datetime.strptime(end_date, '%Y-%m-%d').strftime('%Y-%m-%d')  
            # Gunakan fungsi baru yang mengambil data berdasarkan kredensial user
            rs_result = data_mysql().get_all_adx_traffic_account_by_params(
                start_date_formatted,
                end_date_formatted,
                selected_account_list,
                selected_domain_list,
                force_clickhouse=True
            )
            rows_map = {}
            if rs_result and rs_result['hasil']['data']:
                for rs in rs_result['hasil']['data']:
                    date_key = str(rs.get('date', '') or '')
                    raw_site = str(rs.get('site_name', '') or '')
                    base_subdomain = extract_base_subdomain(raw_site) if raw_site else ''
                    if not base_subdomain:
                        base_subdomain = raw_site
                    impressions = int(rs.get('impressions_adx', 0) or 0)
                    clicks = int(rs.get('clicks_adx', 0) or 0)
                    revenue = float(rs.get('revenue', 0.0) or 0.0)
                    total_requests = int(rs.get('total_requests', 0) or 0)
                    responses_served = int(rs.get('responses_served', 0) or 0)
                    active_view_pct_viewable = float(rs.get('active_view_pct_viewable', 0.0) or 0.0)
                    active_view_avg_time_sec = float(rs.get('active_view_avg_time_sec', 0.0) or 0.0)

                    key = f"{date_key}|{base_subdomain}"
                    entry = rows_map.get(key)
                    if not entry:
                        entry = {
                            'date': date_key,
                            'site_name': base_subdomain,
                            'site_name_raw': raw_site,
                            'impressions_adx': 0,
                            'clicks_adx': 0,
                            'revenue': 0.0,
                            'total_requests': 0,
                            'responses_served': 0,
                            'active_view_weight': 0,
                            'active_view_pct_viewable_sum': 0.0,
                            'active_view_avg_time_sec_sum': 0.0,
                        }
                        rows_map[key] = entry
                    entry['impressions_adx'] += impressions
                    entry['clicks_adx'] += clicks
                    entry['revenue'] += revenue
                    entry['total_requests'] += total_requests
                    entry['responses_served'] += responses_served
                    if impressions > 0:
                        entry['active_view_weight'] += impressions
                        entry['active_view_pct_viewable_sum'] += active_view_pct_viewable * impressions
                        entry['active_view_avg_time_sec_sum'] += active_view_avg_time_sec * impressions
            result_rows = []
            total_impressions = 0
            total_clicks = 0
            total_revenue = 0.0
            for _, item in rows_map.items():
                imp = int(item.get('impressions_adx') or 0)
                clk = int(item.get('clicks_adx') or 0)
                rev = float(item.get('revenue') or 0.0)
                cpc_adx = (rev / clk) if clk > 0 else 0.0
                ctr = ((clk / imp) * 100) if imp > 0 else 0.0
                ecpm = ((rev / imp) * 1000) if imp > 0 else 0.0
                total_impressions += imp
                total_clicks += clk
                total_revenue += rev
                total_requests = int(item.get('total_requests') or 0)
                responses_served = int(item.get('responses_served') or 0)

                match_rate = (float(responses_served) / float(total_requests) * 100.0) if total_requests > 0 else 0.0
                fill_rate = (float(imp) / float(responses_served) * 100.0) if responses_served > 0 else 0.0

                w = int(item.get('active_view_weight') or 0)
                active_view_pct_viewable = (float(item.get('active_view_pct_viewable_sum') or 0.0) / float(w)) if w > 0 else 0.0
                active_view_avg_time_sec = (float(item.get('active_view_avg_time_sec_sum') or 0.0) / float(w)) if w > 0 else 0.0

                result_rows.append({
                    'date': item['date'],
                    'site_name': item['site_name'] + '.com',
                    'site_name_raw': item.get('site_name_raw') or item.get('site_name') or '',
                    'impressions_adx': imp,
                    'clicks_adx': clk,
                    'cpc_adx': round(cpc_adx, 2),
                    'ecpm': round(ecpm, 2),
                    'ctr': round(ctr, 2),
                    'revenue': round(rev, 2),
                    'total_requests': total_requests,
                    'responses_served': responses_served,
                    'match_rate': round(match_rate, 2),
                    'fill_rate': round(fill_rate, 2),
                    'active_view_pct_viewable': round(active_view_pct_viewable, 2),
                    'active_view_avg_time_sec': round(active_view_avg_time_sec, 2),
                })
            result_rows.sort(key=lambda x: (x['date'] or '', x['site_name'] or ''))
            summary = {
                'total_clicks': total_clicks,
                'total_impressions': total_impressions,
                'total_revenue': round(total_revenue, 2),
                'avg_cpc': round((total_revenue / total_clicks), 2) if total_clicks > 0 else 0.0,
                'avg_ecpm': round(((total_revenue / total_impressions) * 1000), 2) if total_impressions > 0 else 0.0,
                'avg_ctr': round(((total_clicks / total_impressions) * 100), 2) if total_impressions > 0 else 0.0
            }
            return JsonResponse({
                'status': True,
                'message': 'Data adx traffic account berhasil diambil',
                'summary': summary,
                'data': result_rows
            }, safe=False)
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
        selected_account_list = []
        if selected_accounts:
            selected_account_list = [str(s).strip() for s in selected_accounts.split(',') if s.strip()]
        if selected_account_list:
            user_mail = data_mysql().fetch_user_mail_by_account(selected_account_list)    
        else:
            user_mail = req.session.get('hris_admin', {}).get('user_mail')
        try:
            # Cek cache terlebih dahulu untuk mempercepat respons
            try:
                cache_key = generate_cache_key('adx_sites_list', str(user_mail or ''))
                cached_sites = get_cached_data(cache_key)
                if cached_sites is not None:
                    return JsonResponse(cached_sites, safe=False)
            except Exception as _cache_err:
                # Lanjutkan tanpa memblokir jika cache gagal
                print(f"[WARNING] adx_sites_list cache unavailable: {_cache_err}")

            # Ambil daftar situs dari Ad Manager jika cache miss
            # result = fetch_user_sites_list(user_mail)
            end_date = date.today()
            start_date = end_date - timedelta(days=7)
            result = data_mysql().fetch_user_sites_list(
                user_mail, 
                start_date.strftime('%Y-%m-%d'), 
                end_date.strftime('%Y-%m-%d')
            )
            # Simpan ke cache untuk permintaan berikutnya
            try:
                # Cache selama 6 jam; daftar situs jarang berubah
                set_cached_data(cache_key, result['hasil'], timeout=6 * 60 * 60)
            except Exception as _cache_set_err:
                print(f"[WARNING] failed to cache ads_sites_list: {_cache_set_err}")
            return JsonResponse(result['hasil'], safe=False)
        except Exception as e:
            return JsonResponse({
                'status': False,
                'error': str(e)
            })

class AdxAccountListView(View):
    """AJAX endpoint untuk mengambil daftar akun dari Ad Manager"""
    def dispatch(self, request, *args, **kwargs):
        if 'hris_admin' not in request.session:
            return redirect('admin_login')
        return super().dispatch(request, *args, **kwargs)
    def get(self, req):
        selected_domains = req.GET.get('selected_domains')
        selected_domain_list = []
        if selected_domains:
            selected_domain_list = [str(s).strip() for s in selected_domains.split(',') if s.strip()]
        try:
            # Cek cache terlebih dahulu untuk mempercepat respons
            try:
                cache_key = generate_cache_key('adx_accounts_list', str(selected_domains or ''))
                cached_accounts = get_cached_data(cache_key)
                if cached_accounts is not None:
                    return JsonResponse(cached_accounts, safe=False)
            except Exception as _cache_err:
                # Lanjutkan tanpa memblokir jika cache gagal
                print(f"[WARNING] adx_account_list cache unavailable: {_cache_err}")

            # Ambil daftar situs dari Ad Manager jika cache miss
            # result = fetch_user_sites_list(user_mail)
            end_date = date.today()
            start_date = end_date - timedelta(days=7)
            result = data_mysql().fetch_account_list_by_domain(
                selected_domain_list, 
                start_date.strftime('%Y-%m-%d'), 
                end_date.strftime('%Y-%m-%d')
            )
            # Simpan ke cache untuk permintaan berikutnya
            try:
                # Cache selama 6 jam; daftar akun jarang berubah
                set_cached_data(cache_key, result['hasil'], timeout=6 * 60 * 60)
            except Exception as _cache_set_err:
                print(f"[WARNING] failed to cache adx_account_list: {_cache_set_err}")
            return JsonResponse(result['hasil'], safe=False)
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
        admin = req.session.get('hris_admin', {})
        if admin.get('super_st') == '0':
            data_account_adx = data_mysql().get_all_adx_account_data_user(admin.get('user_id'))
            data_domain_adx = data_mysql().get_all_adx_domain_data_user(admin.get('user_id'))
        else:
            data_account_adx = data_mysql().get_all_adx_account_data()
            data_domain_adx = data_mysql().get_all_adx_domain_data()
        if not data_domain_adx['status']:
            return JsonResponse({
                'status': False,
                'error': data_domain_adx['data']
            })
        last_update = data_mysql().get_last_update_adx_traffic_country()['data']['last_update']
        data = {
            'title': 'AdX Traffic Per Country',
            'user': req.session['hris_admin'],
            'data_account_adx': data_account_adx['data'],
            'data_domain_adx': data_domain_adx['data'],
            'last_update': last_update,
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
        selected_account = req.GET.get('selected_account')
        admin = req.session.get('hris_admin', {})
        if not selected_account:
            rs_account = data_mysql().get_all_adx_account_data_user(admin.get('user_id')) if admin.get('super_st') == '0' else data_mysql().get_all_adx_account_data()
            account_ids = [str(item['account_id']) for item in rs_account.get('data', [])]
            selected_account = ",".join(account_ids)
        else:
            selected_account = req.GET.get('selected_account', '')
        selected_account_list = []
        if selected_account:
            selected_account_list = [str(s).strip() for s in selected_account.split(',') if s.strip()]
        selected_domains = req.GET.get('selected_domains')
        selected_domain_list = []
        if selected_domains:
            selected_domain_list = [str(s).strip() for s in selected_domains.split(',') if s.strip()]
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
            # result = fetch_adx_traffic_per_country(start_date_formatted, end_date_formatted, user_mail, selected_sites, countries_list)    
            result = data_mysql().get_all_adx_traffic_country_by_params(start_date_formatted, end_date_formatted, selected_account_list, selected_domain_list, countries_list, force_clickhouse=True)
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
        admin = req.session.get('hris_admin', {})
        if admin.get('super_st') == '0':
            data_account_adx = data_mysql().get_all_adx_account_data_user(admin.get('user_id'))
            data_domain_adx = data_mysql().get_all_adx_domain_data_user(admin.get('user_id'))
        else:
            data_account_adx = data_mysql().get_all_adx_account_data()
            data_domain_adx = data_mysql().get_all_adx_domain_data()
        if not data_domain_adx['status']:
            return JsonResponse({
                'status': False,
                'error': data_domain_adx['data']
            })
        data_account = data_mysql().master_account_ads()['data']
        last_update = data_mysql().get_last_update_adx_traffic_country()['data']['last_update']
        data = {
            'title': 'ROI Per Country',
            'user': req.session['hris_admin'],
            'last_update': last_update,
            'data_account': data_account,
            'data_account_adx': data_account_adx['data'],
            'data_domain_adx': data_domain_adx['data'],
        }
        return render(req, 'admin/report_roi/per_country/index.html', data)

class RoiTrafficPerCountryDataView(View):
    def dispatch(self, request, *args, **kwargs):
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
        selected_account = req.GET.get('selected_account_adx')
        admin = req.session.get('hris_admin', {})
        if selected_account == '':
            rs_account = data_mysql().get_all_adx_account_data_user(admin.get('user_id'))
            account_ids = [str(item['account_id']) for item in rs_account.get('data', [])]
            selected_account = ",".join(account_ids)
        else:
            selected_account = req.GET.get('selected_account_adx', '')
        selected_account_list = []
        if selected_account:
            selected_account_list = [str(s).strip() for s in selected_account.split(',') if s.strip()]
        selected_domain = req.GET.get('selected_domains')
        selected_domain_list = build_domain_filter_terms(selected_domain, include_original=True, include_base=True)
        selected_domain_list_fb = build_domain_filter_terms(selected_domain, include_original=False, include_base=True)
        selected_account_ads = req.GET.get('selected_account_ads', '')
        selected_countries = req.GET.get('selected_countries', '')
        try:
            # Validasi parameter tanggal terlebih dahulu
            if not start_date or not end_date:
                return JsonResponse({
                    'status': False,
                    'error': 'Parameter tanggal tidak lengkap'
                })
            # Parse selected countries dari string yang dipisah koma
            raw_countries_list = []
            if selected_countries and selected_countries.strip():
                raw_countries_list = [country.strip() for country in selected_countries.split(',') if country.strip()]
            else:
                print("[DEBUG] No countries selected, will fetch all countries")

            def normalize_country_code(cc):
                c = (str(cc or '').strip().upper())
                if not c:
                    return ''
                alias = {
                    'TU': 'TR'
                }
                return alias.get(c, c)

            countries_list = []
            for c in (raw_countries_list or []):
                cc = normalize_country_code(c)
                if cc and cc not in countries_list:
                    countries_list.append(cc)

            def expand_country_codes_filter(items):
                out = []
                for it in (items or []):
                    s = normalize_country_code(it)
                    if not s:
                        continue
                    out.append(s)
                    if s == 'TR':
                        out.append('TU')
                seen = set()
                uniq = []
                for x in out:
                    if x and x not in seen:
                        seen.add(x)
                        uniq.append(x)
                return uniq
            countries_list_query = expand_country_codes_filter(countries_list)
            # agar FB mengikuti domain yang ada di akun AdX terpilih
            sites_for_fb = None
            if not selected_domain or not selected_domain.strip():
                try:
                    # Ambil list sites dari database
                    sites_result = data_mysql().fetch_user_sites_id_list(
                        start_date, end_date, selected_account_list
                    )
                    if sites_result['hasil']['data']:
                        # Ambil data sites
                        sites_for_fb = sites_result['hasil']['data']
                        # Hapus semua 'Unknown'
                        sites_for_fb = [site for site in sites_for_fb if site != 'Unknown']
                    else:
                        print(f"[DEBUG ROI] No sites derived for FB filter: {sites_result['hasil']['data']}")
                except Exception as _sites_err:
                    print(f"[DEBUG ROI] Unable to derive sites_for_fb: {_sites_err}")
            # ===== Response-level cache (meng-cache hasil akhir penggabungan) =====
            response_cache_key = generate_cache_key(
                'roi_traffic_country_response_v2',
                start_date,
                end_date,
                selected_account_list,
                selected_domain_list,
                selected_account_ads or '',
                ','.join(countries_list_query) if countries_list_query else ''
            )
            cached_response = get_cached_data(response_cache_key)
            if cached_response is not None:
                return JsonResponse(cached_response, safe=False)
            data_facebook = None
            # Jalankan paralel jika selected_domain sudah ada (menghindari fetch FB yang terlalu lebar)
            if selected_account_list and not selected_domain_list:
                with ThreadPoolExecutor(max_workers=2) as executor:
                    adx_future = executor.submit(
                        data_mysql().get_all_adx_roi_country_detail_by_params,
                        start_date,
                        end_date,
                        selected_account_list,
                        selected_domain_list,
                        countries_list_query
                    )
                    data_adx = adx_future.result()
                    unique_name_site = []
                    if data_adx.get("status") and data_adx.get("data"):
                        unique_sites = set()
                        for row in data_adx["data"]:
                            site_name = (row.get("site_name") or "").strip().lower()
                            if not site_name or site_name == "unknown":
                                continue
                            unique_sites.add(site_name)
                        extracted_names = []
                        for site in unique_sites:
                            if "." not in site:
                                continue

                            parts = site.split(".")

                            if len(parts) >= 2:
                                main_domain = ".".join(parts[:2])   # ✅ ambil depan
                            else:
                                main_domain = site

                            extracted_names.append(main_domain)
                        unique_name_site = list(set(extracted_names))
                    fb_future = executor.submit(
                        data_mysql().get_all_ads_roi_country_detail_by_params,
                        start_date,
                        end_date,
                        unique_name_site,
                        countries_list_query
                    )
                    data_adx = adx_future.result()
                    try:
                        # Hapus timeout: tunggu hingga FB selesai agar data lengkap
                        data_facebook = fb_future.result()
                    except Exception as e:
                        data_facebook = None
            elif selected_domain_list :
                with ThreadPoolExecutor(max_workers=2) as executor:
                    adx_future = executor.submit(
                        data_mysql().get_all_adx_roi_country_detail_by_params,
                        start_date,
                        end_date,
                        selected_account_list,
                        selected_domain_list,
                        countries_list_query
                    )
                    unique_name_site = []
                    if selected_domain_list_fb:
                        seen_sites = set()
                        for site_item in selected_domain_list_fb:
                            site_name = str(site_item or '').strip().strip("\"'")
                            if not site_name or site_name == 'Unknown' or site_name in seen_sites:
                                continue
                            seen_sites.add(site_name)
                            unique_name_site.append(site_name)
                    fb_future = executor.submit(
                        data_mysql().get_all_ads_roi_country_detail_by_params,
                        start_date,
                        end_date,
                        unique_name_site,
                        countries_list_query
                    )
                    data_adx = adx_future.result()
                    try:
                        # Hapus timeout: tunggu hingga FB selesai agar data lengkap
                        data_facebook = fb_future.result()
                    except Exception as e:
                        data_facebook = None
            else:
                # Filter Domain kosong: tampilkan data semua domain dari akun AdX terpilih
                data_adx = data_mysql().get_all_adx_roi_country_detail_by_params(
                    start_date, 
                    end_date, 
                    selected_account_list, 
                    selected_domain_list, 
                    countries_list_query
                )
                try:
                    unique_name_site = []
                    with ThreadPoolExecutor(max_workers=1) as executor:
                        if sites_for_fb:
                            unique_sites = set(site.strip() for site in sites_for_fb if site.strip() and site.strip() != 'Unknown')
                            extracted_names = []
                            for site in unique_sites:
                                main_domain = extract_base_subdomain(site.strip())
                                if main_domain and main_domain != 'Unknown':
                                    extracted_names.append(main_domain)
                            unique_name_site = list(set(extracted_names))

                        if not unique_name_site:
                            adx_payload_tmp = data_adx.get('hasil') if isinstance(data_adx, dict) and data_adx.get('hasil') else data_adx
                            adx_items_tmp = adx_payload_tmp.get('data') if isinstance(adx_payload_tmp, dict) else []
                            if adx_items_tmp:
                                extracted_names = []
                                for adx_item in (adx_items_tmp or []):
                                    site_name = str(adx_item.get('site_name', '') or '')
                                    main_domain = extract_base_subdomain(site_name.strip())
                                    if main_domain and main_domain != 'Unknown':
                                        extracted_names.append(main_domain)
                                unique_name_site = list(set(extracted_names))
                        if unique_name_site:
                            fb_future = executor.submit(
                                data_mysql().get_all_ads_roi_country_detail_by_params,
                                start_date, end_date, unique_name_site, countries_list_query
                            )
                            data_facebook = fb_future.result()
                        else:
                            data_facebook = None
                except Exception as e:
                    data_facebook = None
            # Ringkas data Facebook untuk diagnosa
            try:
                if data_facebook and isinstance(data_facebook, dict) and data_facebook.get('hasil') and data_facebook['hasil'].get('data'):
                    fb_items = data_facebook['hasil'].get('data', []) or []
                    fb_total_spend = 0.0
                    for _it in fb_items:
                        try:
                            fb_total_spend += float(_it.get('spend', 0) or 0)
                        except Exception:
                            pass
                    if fb_items:
                        sample_labels = []
                        for _s in fb_items[:5]:
                            sample_labels.append(_s.get('country'))
                else:
                    print("[DEBUG ROI] No Facebook data returned or fetch failed")
            except Exception as _sum_e:
                print(f"[DEBUG ROI] Unable to summarize FB data: {_sum_e}")
            # Proses penggabungan data AdX dan Facebook
            # Pastikan bentuk payload sesuai: gunakan 'hasil' untuk AdX dan FB jika tersedia
            adx_payload = data_adx.get('hasil') if isinstance(data_adx, dict) and data_adx.get('hasil') else data_adx
            fb_payload = (data_facebook.get('hasil') if isinstance(data_facebook, dict) and data_facebook.get('hasil') else {'status': True, 'data': []})
            result = process_roi_traffic_country_data(adx_payload, fb_payload)
            # Filter hasil berdasarkan negara yang dipilih jika ada
            if countries_list and result.get('status') and result.get('data'):
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
                filtered_data = []
                for item in result['data']:
                    country_code = item.get('country_code', '').lower()
                    country_name = item.get('country', '').lower()
                    # Check if country matches any in the filter list (case insensitive)
                    country_matched = False
                    for filter_country in parsed_filter_countries:
                        if country_name == filter_country or country_code == filter_country:
                            country_matched = True
                            break
                    # Only add to filtered_data if country_matched is True
                    if country_matched:
                        filtered_data.append(item)
                    else:
                        print(f"[DEBUG ROI] ✗ No match found for '{country_name}' - EXCLUDED from results")
                
                result['data'] = filtered_data
                result['total_records'] = len(filtered_data)

            if countries_list and result.get('status'):
                try:
                    allow_codes = set([normalize_country_code(x) for x in (countries_list or []) if normalize_country_code(x)])
                    if allow_codes:
                        result['daily_rows'] = [r for r in (result.get('daily_rows') or []) if normalize_country_code((r or {}).get('country_code')) in allow_codes]
                except Exception:
                    pass
            # Simpan hasil akhir ke cache dengan TTL 15 menit
            try:
                set_cached_data(response_cache_key, result, timeout=900)
            except Exception as _cache_err:
                print(f"[DEBUG] Failed to cache ROI Country final response: {_cache_err}")
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
        adx_map = {}
        fb_map = {}
        country_name_by_code = {}

        def normalize_country_code(cc):
            c = (str(cc or '').strip().upper())
            if not c:
                return ''
            if c == 'TU':
                return 'TR'
            return c

        # Normalisasi AdX: date + base_subdomain + country_code
        adx_items_raw = data_adx.get('data') if isinstance(data_adx, dict) else []
        adx_items = adx_items_raw if isinstance(adx_items_raw, list) else []
        for adx_item in (adx_items or []):
            if not isinstance(adx_item, dict):
                continue
            date_key = str(adx_item.get('date', '') or '')
            site_name = str(adx_item.get('site_name', '') or '')
            base_subdomain = extract_base_subdomain(site_name)
            country_code = normalize_country_code(adx_item.get('country_code', '') or '')
            country_name = adx_item.get('country_name', '') or ''
            impressions_adx = int(adx_item.get('impressions', 0) or 0)
            clicks_adx = int(adx_item.get('clicks', 0) or 0)
            revenue = float(adx_item.get('revenue', 0) or 0)
            # Pastikan date_key, base_subdomain, dan country_code tersedia agar agregasi sesuai pasangan site/tanggal/negara
            if not date_key or not base_subdomain or not country_code:
                continue
            country_name_by_code[country_code] = country_name or country_name_by_code.get(country_code, '')
            var_key = f"{date_key}_{base_subdomain}_{country_code}"
            entry = adx_map.get(var_key) or {'revenue': 0.0, 'impressions_adx': 0, 'clicks_adx': 0}
            entry['revenue'] += revenue
            entry['impressions_adx'] += impressions_adx
            entry['clicks_adx'] += clicks_adx
            adx_map[var_key] = entry

        # Normalisasi FB: date + base_subdomain + country_code
        fb_payload = data_facebook if isinstance(data_facebook, dict) else {'status': True, 'data': []}
        fb_items_raw = fb_payload.get('data') if isinstance(fb_payload, dict) else []
        fb_items = fb_items_raw if isinstance(fb_items_raw, list) else []
        for fb_item in fb_items:
            if not isinstance(fb_item, dict):
                continue
            date_key = str(fb_item.get('date', '') or '')
            domain = str(fb_item.get('domain', '') or '')
            base_subdomain = extract_base_subdomain(domain)
            country_code = normalize_country_code(fb_item.get('country_code', '') or '')
            country_name = fb_item.get('country_name', '') or ''
            spend = float(fb_item.get('spend', 0) or 0)
            clicks_fb = int(fb_item.get('clicks', 0) or 0)
            impressions_fb = int(fb_item.get('impressions', 0) or 0)
            cpr = float(fb_item.get('cpr', 0) or 0)
            # Pastikan date_key, base_subdomain, dan country_code tersedia agar agregasi sesuai pasangan site/tanggal/negara
            if not date_key or not base_subdomain or not country_code:
                continue
            country_name_by_code[country_code] = country_name or country_name_by_code.get(country_code, '')
            var_key = f"{date_key}_{base_subdomain}_{country_code}"
            entry = fb_map.get(var_key) or {'spend': 0.0, 'impressions_fb': 0, 'clicks_fb': 0, 'cpr_sum': 0.0, 'cpr_count': 0}
            entry['spend'] += spend
            entry['impressions_fb'] += impressions_fb
            entry['clicks_fb'] += clicks_fb
            if cpr > 0:
                entry['cpr_sum'] += cpr
                entry['cpr_count'] += 1
            fb_map[var_key] = entry

        # Agregasi per country_code
        agg_all = {}
        agg_filtered = {}
        daily_all = {}
        daily_filtered = {}
        union_keys = set(list(adx_map.keys()) + list(fb_map.keys()))
        for key in union_keys:
            try:
                parts = key.split('_')
                if len(parts) < 3:
                    continue
                date_key = parts[0]
                country_code = parts[-1]
            except Exception:
                continue
            adx_entry = adx_map.get(key) or {'revenue': 0.0, 'impressions_adx': 0, 'clicks_adx': 0}
            fb_entry = fb_map.get(key) or {'spend': 0.0, 'impressions_fb': 0, 'clicks_fb': 0, 'cpr_sum': 0.0, 'cpr_count': 0}
            revenue = float(adx_entry['revenue'])
            spend = float(fb_entry['spend'])
            impressions_fb = int(fb_entry['impressions_fb'])
            clicks_fb = int(fb_entry['clicks_fb'])
            impressions_adx = int(adx_entry['impressions_adx'])
            clicks_adx = int(adx_entry['clicks_adx'])
            name = country_name_by_code.get(country_code, '')
            if country_code not in agg_all:
                agg_all[country_code] = {'country': name, 'country_code': country_code, 'impressions_fb': 0, 'impressions_adx': 0, 'spend': 0.0, 'clicks_fb': 0, 'clicks_adx': 0, 'revenue': 0.0, 'cpr_sum': 0.0, 'cpr_count': 0}
            agg_all[country_code]['spend'] += spend
            agg_all[country_code]['revenue'] += revenue
            agg_all[country_code]['impressions_fb'] += impressions_fb
            agg_all[country_code]['clicks_fb'] += clicks_fb
            agg_all[country_code]['impressions_adx'] += impressions_adx
            agg_all[country_code]['clicks_adx'] += clicks_adx
            agg_all[country_code]['cpr_sum'] += fb_entry.get('cpr_sum', 0.0)
            agg_all[country_code]['cpr_count'] += fb_entry.get('cpr_count', 0)

            per_day_map = daily_all.get(country_code)
            if not per_day_map:
                daily_all[country_code] = {}
                per_day_map = daily_all[country_code]
            day_entry = per_day_map.get(date_key)
            if not day_entry:
                per_day_map[date_key] = {'spend': 0.0, 'clicks_fb': 0, 'revenue': 0.0, 'impressions_adx': 0}
                day_entry = per_day_map[date_key]
            day_entry['spend'] += spend
            day_entry['clicks_fb'] += clicks_fb
            day_entry['revenue'] += revenue
            day_entry['impressions_adx'] += impressions_adx

            if spend > 0:
                if country_code not in agg_filtered:
                    agg_filtered[country_code] = {'country': name, 'country_code': country_code, 'impressions_fb': 0, 'impressions_adx': 0, 'spend': 0.0, 'clicks_fb': 0, 'clicks_adx': 0, 'revenue': 0.0, 'cpr_sum': 0.0, 'cpr_count': 0}
                agg_filtered[country_code]['spend'] += spend
                agg_filtered[country_code]['revenue'] += revenue
                agg_filtered[country_code]['impressions_fb'] += impressions_fb
                agg_filtered[country_code]['clicks_fb'] += clicks_fb
                agg_filtered[country_code]['impressions_adx'] += impressions_adx
                agg_filtered[country_code]['clicks_adx'] += clicks_adx
                agg_filtered[country_code]['cpr_sum'] += fb_entry.get('cpr_sum', 0.0)
                agg_filtered[country_code]['cpr_count'] += fb_entry.get('cpr_count', 0)

                per_day_map_f = daily_filtered.get(country_code)
                if not per_day_map_f:
                    daily_filtered[country_code] = {}
                    per_day_map_f = daily_filtered[country_code]
                day_entry_f = per_day_map_f.get(date_key)
                if not day_entry_f:
                    per_day_map_f[date_key] = {'spend': 0.0, 'clicks_fb': 0, 'revenue': 0.0, 'impressions_adx': 0}
                    day_entry_f = per_day_map_f[date_key]
                day_entry_f['spend'] += spend
                day_entry_f['clicks_fb'] += clicks_fb
                day_entry_f['revenue'] += revenue
                day_entry_f['impressions_adx'] += impressions_adx

        combined_data_all = []
        for code, item in agg_all.items():
            s = float(item['spend'])
            r = float(item['revenue'])
            imp_fb = int(item['impressions_fb'])
            clk_fb = int(item['clicks_fb'])
            imp_adx = int(item['impressions_adx'])
            clk_adx = int(item['clicks_adx'])
            ctr_fb = ((clk_fb / imp_fb) * 100) if imp_fb > 0 else 0
            ctr_adx = ((clk_adx / imp_adx) * 100) if imp_adx > 0 else 0
            cpc_fb = (s / clk_fb) if clk_fb > 0 else 0
            cpc_adx = (r / clk_adx) if clk_adx > 0 else 0
            roi = ((r - s) / s * 100) if s > 0 else 0

            cpr = (s / clk_fb) if clk_fb > 0 else 0.0
            ecpm = ((r / imp_adx) * 1000) if imp_adx > 0 else 0.0

            combined_data_all.append({
                'country': item['country'],
                'country_code': code,
                'impressions_fb': imp_fb,
                'impressions_adx': imp_adx,
                'clicks_fb': clk_fb,
                'clicks_adx': clk_adx,
                'cpr': round(cpr, 2),
                'cpc_fb': round(cpc_fb, 2),
                'ctr_fb': round(ctr_fb, 2),
                'cpc_adx': round(cpc_adx, 2),
                'ctr_adx': round(ctr_adx, 2),
                'ecpm': round(ecpm, 2),
                'spend': round(s, 2),
                'revenue': round(r, 2),
                'roi': round(roi, 2)
            })

        combined_data_filtered = []
        for code, item in agg_filtered.items():
            s = float(item['spend'])
            r = float(item['revenue'])
            imp_fb = int(item['impressions_fb'])
            clk_fb = int(item['clicks_fb'])
            imp_adx = int(item['impressions_adx'])
            clk_adx = int(item['clicks_adx'])
            ctr_fb = ((clk_fb / imp_fb) * 100) if imp_fb > 0 else 0
            ctr_adx = ((clk_adx / imp_adx) * 100) if imp_adx > 0 else 0
            cpc_fb = (s / clk_fb) if clk_fb > 0 else 0
            cpc_adx = (r / clk_adx) if clk_adx > 0 else 0
            roi = ((r - s) / s * 100) if s > 0 else 0

            cpr = (s / clk_fb) if clk_fb > 0 else 0.0
            ecpm = ((r / imp_adx) * 1000) if imp_adx > 0 else 0.0

            combined_data_filtered.append({
                'country': item['country'],
                'country_code': code,
                'impressions_fb': imp_fb,
                'impressions_adx': imp_adx,
                'clicks_fb': clk_fb,
                'clicks_adx': clk_adx,
                'cpr': round(cpr, 2),
                'cpc_fb': round(cpc_fb, 2),
                'ctr_fb': round(ctr_fb, 2),
                'cpc_adx': round(cpc_adx, 2),
                'ctr_adx': round(ctr_adx, 2),
                'ecpm': round(ecpm, 2),
                'spend': round(s, 2),
                'revenue': round(r, 2),
                'roi': round(roi, 2)
            })

        combined_data_all.sort(key=lambda x: x['roi'], reverse=True)
        combined_data_filtered.sort(key=lambda x: x['roi'], reverse=True)
        total_spend_all = sum(d['spend'] for d in combined_data_all) if combined_data_all else 0.0
        total_revenue_all = sum(d['revenue'] for d in combined_data_all) if combined_data_all else 0.0
        total_spend_filtered = sum(d['spend'] for d in combined_data_filtered) if combined_data_filtered else 0.0
        total_revenue_filtered = sum(d['revenue'] for d in combined_data_filtered) if combined_data_filtered else 0.0

        count_all = len(combined_data_all)
        count_filtered = len(combined_data_filtered)

        cpr_values_all = [float(d.get('cpr', 0) or 0) for d in combined_data_all if float(d.get('cpr', 0) or 0) > 0]
        cpr_values_filtered = [float(d.get('cpr', 0) or 0) for d in combined_data_filtered if float(d.get('cpr', 0) or 0) > 0]
        rata_cpr_all = round(sum(cpr_values_all) / len(cpr_values_all), 2) if cpr_values_all else 0
        rata_cpr_filtered = round(sum(cpr_values_filtered) / len(cpr_values_filtered), 2) if cpr_values_filtered else 0

        return {
            'status': True,
            'data': combined_data_all,
            'data_filtered': combined_data_filtered,
            'total_records': count_all,
            'total_records_filtered': count_filtered,
            'summary_all': {
                'total_spend': round(total_spend_all, 2),
                'total_clicks_fb': sum(d['clicks_fb'] for d in combined_data_all),
                'total_impressions_fb': sum(d['impressions_fb'] for d in combined_data_all),
                'total_clicks_adx': sum(d['clicks_adx'] for d in combined_data_all),
                'total_impressions_adx': sum(d['impressions_adx'] for d in combined_data_all),
                'total_ctr_fb': sum(d['ctr_fb'] for d in combined_data_all),
                'total_ctr_adx': sum(d['ctr_adx'] for d in combined_data_all),
                'rata_cpr': rata_cpr_all,
                'total_revenue': round(total_revenue_all, 2),
                'total_roi': round(((total_revenue_all - total_spend_all) / total_spend_all * 100) if total_spend_all > 0 else 0, 2)
            },
            'summary_filtered': {
                'total_spend': round(total_spend_filtered, 2),
                'total_clicks_fb': sum(d['clicks_fb'] for d in combined_data_filtered),
                'total_impressions_fb': sum(d['impressions_fb'] for d in combined_data_filtered),
                'total_clicks_adx': sum(d['clicks_adx'] for d in combined_data_filtered),
                'total_impressions_adx': sum(d['impressions_adx'] for d in combined_data_filtered),
                'total_ctr_fb': sum(d['ctr_fb'] for d in combined_data_filtered),
                'total_ctr_adx': sum(d['ctr_adx'] for d in combined_data_filtered),
                'rata_cpr': rata_cpr_filtered,
                'total_revenue': round(total_revenue_filtered, 2),
                'total_roi': round(((total_revenue_filtered - total_spend_filtered) / total_spend_filtered * 100) if total_spend_filtered > 0 else 0, 2)
            }
        }
        
    except Exception as e:
        return {
            'status': False,
            'error': f'Error processing ROI traffic country data: {str(e)}',
            'data': []
        }

def process_roi_monitoring_country_data(data_adx, data_facebook):
    """Fungsi untuk menggabungkan data AdX dan Facebook berdasarkan date + subdomain + country_code,
    lalu agregasi per negara dan menghitung ROI"""
    try:
        adx_map = {}
        fb_map = {}
        country_name_by_code = {}

        def normalize_country_code(cc):
            c = (str(cc or '').strip().upper())
            if not c:
                return ''
            alias = {
                'TU': 'TR'
            }
            return alias.get(c, c)

        # Normalisasi AdX: date + base_subdomain + country_code
        adx_items_raw = data_adx.get('data') if isinstance(data_adx, dict) else []
        adx_items = adx_items_raw if isinstance(adx_items_raw, list) else []
        for adx_item in (adx_items or []):
            if not isinstance(adx_item, dict):
                continue
            date_key = str(adx_item.get('date', '') or '')
            site_name = str(adx_item.get('site_name', '') or '')
            base_subdomain = extract_base_subdomain(site_name)
            country_code = normalize_country_code(adx_item.get('country_code', '') or '')
            country_name = adx_item.get('country_name', '') or ''
            revenue = float(adx_item.get('revenue', 0) or 0)
            
            impressions_adx = int(adx_item.get('impressions', 0) or 0)
            clicks_adx = int(adx_item.get('clicks', 0) or 0)
            total_requests = int(adx_item.get('total_requests', 0) or 0)
            responses_served = int(adx_item.get('responses_served', 0) or 0)
            match_rate = float(adx_item.get('match_rate', 0) or 0)
            fill_rate = float(adx_item.get('fill_rate', 0) or 0)
            active_view_pct_viewable = float(adx_item.get('active_view_pct_viewable', 0) or 0)
            active_view_avg_time_sec = float(adx_item.get('active_view_avg_time_sec', 0) or 0)
            
            if not date_key or not base_subdomain or not country_code:
                continue
            country_name_by_code[country_code] = country_name or country_name_by_code.get(country_code, '')
            key = f"{date_key}_{base_subdomain}_{country_code}"
            
            entry = adx_map.get(key) or {
                'revenue': 0.0,
                'impressions_adx': 0,
                'clicks_adx': 0,
                'total_requests': 0,
                'responses_served': 0,
                'match_rate_sum': 0.0,
                'fill_rate_sum': 0.0,
                'active_view_pct_viewable_sum': 0.0,
                'active_view_avg_time_sec_sum': 0.0,
                'count': 0
            }
            entry['revenue'] += revenue
            entry['impressions_adx'] += impressions_adx
            entry['clicks_adx'] += clicks_adx
            entry['total_requests'] += total_requests
            entry['responses_served'] += responses_served
            entry['match_rate_sum'] += match_rate
            entry['fill_rate_sum'] += fill_rate
            entry['active_view_pct_viewable_sum'] += active_view_pct_viewable
            entry['active_view_avg_time_sec_sum'] += active_view_avg_time_sec
            entry['count'] += 1
            adx_map[key] = entry

        # Normalisasi FB: date + base_subdomain + country_code
        fb_payload = data_facebook if isinstance(data_facebook, dict) else {'status': True, 'data': []}
        fb_items_raw = fb_payload.get('data') if isinstance(fb_payload, dict) else []
        fb_items = fb_items_raw if isinstance(fb_items_raw, list) else []
        for fb_item in fb_items:
            if not isinstance(fb_item, dict):
                continue
            date_key = str(fb_item.get('date', '') or '')
            domain = str(fb_item.get('domain', '') or '')
            base_subdomain = extract_base_subdomain(domain)
            country_code = normalize_country_code(fb_item.get('country_code', '') or '')
            country_name = fb_item.get('country_name', '') or ''
            spend = float(fb_item.get('spend', 0) or 0)
            if not date_key or not base_subdomain or not country_code:
                continue
            country_name_by_code[country_code] = country_name or country_name_by_code.get(country_code, '')
            key = f"{date_key}_{base_subdomain}_{country_code}"
            fb_map[key] = (fb_map.get(key, 0.0) + spend)

        # Agregasi per country_code
        agg_all = {}
        agg_filtered = {}
        union_keys = set(list(adx_map.keys()) + list(fb_map.keys()))
        for key in union_keys:
            try:
                country_code = key.split('_')[-1]
            except Exception:
                continue
                
            adx_entry = adx_map.get(key) or {
                'revenue': 0.0,
                'impressions_adx': 0,
                'clicks_adx': 0,
                'total_requests': 0,
                'responses_served': 0,
                'match_rate_sum': 0.0,
                'fill_rate_sum': 0.0,
                'active_view_pct_viewable_sum': 0.0,
                'active_view_avg_time_sec_sum': 0.0,
                'count': 0
            }
            revenue = float(adx_entry['revenue'])
            spend = float(fb_map.get(key, 0.0) or 0.0)
            name = country_name_by_code.get(country_code, '')

            if country_code not in agg_all:
                agg_all[country_code] = {
                    'country': name, 'country_code': country_code, 'spend': 0.0, 'revenue': 0.0,
                    'impressions_adx': 0, 'clicks_adx': 0, 'total_requests': 0, 'responses_served': 0,
                    'match_rate_sum': 0.0, 'fill_rate_sum': 0.0, 'active_view_pct_viewable_sum': 0.0, 'active_view_avg_time_sec_sum': 0.0, 'count': 0
                }
            
            curr = agg_all[country_code]
            curr['spend'] += spend
            curr['revenue'] += revenue
            curr['impressions_adx'] += adx_entry['impressions_adx']
            curr['clicks_adx'] += adx_entry['clicks_adx']
            curr['total_requests'] += adx_entry['total_requests']
            curr['responses_served'] += adx_entry['responses_served']
            curr['match_rate_sum'] += adx_entry['match_rate_sum']
            curr['fill_rate_sum'] += adx_entry['fill_rate_sum']
            curr['active_view_pct_viewable_sum'] += adx_entry['active_view_pct_viewable_sum']
            curr['active_view_avg_time_sec_sum'] += adx_entry['active_view_avg_time_sec_sum']
            curr['count'] += adx_entry['count']

            if spend > 0:
                if country_code not in agg_filtered:
                    agg_filtered[country_code] = {
                        'country': name, 'country_code': country_code, 'spend': 0.0, 'revenue': 0.0,
                        'impressions_adx': 0, 'clicks_adx': 0, 'total_requests': 0, 'responses_served': 0,
                        'match_rate_sum': 0.0, 'fill_rate_sum': 0.0, 'active_view_pct_viewable_sum': 0.0, 'active_view_avg_time_sec_sum': 0.0, 'count': 0
                    }
                curr_f = agg_filtered[country_code]
                curr_f['spend'] += spend
                curr_f['revenue'] += revenue
                curr_f['impressions_adx'] += adx_entry['impressions_adx']
                curr_f['clicks_adx'] += adx_entry['clicks_adx']
                curr_f['total_requests'] += adx_entry['total_requests']
                curr_f['responses_served'] += adx_entry['responses_served']
                curr_f['match_rate_sum'] += adx_entry['match_rate_sum']
                curr_f['fill_rate_sum'] += adx_entry['fill_rate_sum']
                curr_f['active_view_pct_viewable_sum'] += adx_entry['active_view_pct_viewable_sum']
                curr_f['active_view_avg_time_sec_sum'] += adx_entry['active_view_avg_time_sec_sum']
                curr_f['count'] += adx_entry['count']

        def build_row(code, item):
            s = item['spend']
            r = item['revenue']
            net_profit = r - s
            roi = ((r - s) / s * 100) if s > 0 else 0
            cnt = max(1, item['count'])
            
            cpc_adx = r / item['clicks_adx'] if item['clicks_adx'] > 0 else 0.0
            ecpm_adx = (r / item['impressions_adx']) * 1000 if item['impressions_adx'] > 0 else 0.0
            
            return {
                'country': item['country'],
                'country_code': code,
                'spend': round(s, 2),
                'revenue': round(r, 2),
                'net_profit': round(net_profit, 2),
                'roi': round(roi, 2),
                'impressions': item['impressions_adx'],
                'clicks': item['clicks_adx'],
                'cpc': round(cpc_adx, 2),
                'ecpm': round(ecpm_adx, 2),
                'total_requests': item['total_requests'],
                'responses_served': item['responses_served'],
                'match_rate': round(item['match_rate_sum'] / cnt, 2),
                'fill_rate': round(item['fill_rate_sum'] / cnt, 2),
                'active_view_pct_viewable': round(item['active_view_pct_viewable_sum'] / cnt, 2),
                'active_view_avg_time_sec': round(item['active_view_avg_time_sec_sum'] / cnt, 2),
            }

        combined_data_all = [build_row(code, item) for code, item in agg_all.items()]
        combined_data_filtered = [build_row(code, item) for code, item in agg_filtered.items()]

        combined_data_all.sort(key=lambda x: x['roi'], reverse=True)
        combined_data_filtered.sort(key=lambda x: x['roi'], reverse=True)

        total_spend_all = sum(d['spend'] for d in combined_data_all) if combined_data_all else 0.0
        total_revenue_all = sum(d['revenue'] for d in combined_data_all) if combined_data_all else 0.0
        total_spend_filtered = sum(d['spend'] for d in combined_data_filtered) if combined_data_filtered else 0.0
        total_revenue_filtered = sum(d['revenue'] for d in combined_data_filtered) if combined_data_filtered else 0.0

        return {
            'status': True,
            'data': combined_data_all,
            'data_filtered': combined_data_filtered,
            'total_records': len(combined_data_all),
            'total_records_filtered': len(combined_data_filtered),
            'summary_all': {
                'total_spend': round(total_spend_all, 2),
                'total_revenue': round(total_revenue_all, 2),
                'total_net_profit': round(total_revenue_all - total_spend_all, 2),
                'roi_nett': round(((total_revenue_all - total_spend_all) / total_spend_all * 100) if total_spend_all > 0 else 0, 2)
            },
            'summary_filtered': {
                'total_spend': round(total_spend_filtered, 2),
                'total_revenue': round(total_revenue_filtered, 2),
                'total_net_profit': round(total_revenue_filtered - total_spend_filtered, 2),
                'roi_nett': round(((total_revenue_filtered - total_spend_filtered) / total_spend_filtered * 100) if total_spend_filtered > 0 else 0, 2)
            }
        }
    except Exception as e:
        return {
            'status': False,
            'error': f'Error processing ROI traffic country data: {str(e)}',
            'data': []
        }

def build_roi_monitoring_country_daily_rows(data_adx, data_facebook):
    try:
        def normalize_country_code(cc):
            c = (str(cc or '').strip().upper())
            if not c:
                return ''
            alias = {
                'TU': 'TR'
            }
            return alias.get(c, c)

        adx_items_raw = data_adx.get('data') if isinstance(data_adx, dict) else []
        adx_items = adx_items_raw if isinstance(adx_items_raw, list) else []
        fb_items_raw = (data_facebook.get('data') if isinstance(data_facebook, dict) else []) or []
        fb_items = fb_items_raw if isinstance(fb_items_raw, list) else []

        country_name_by_code = {}
        rev_map = {}
        spend_map = {}
        adx_metrics_map = {}

        for adx_item in (adx_items or []):
            if not isinstance(adx_item, dict):
                continue
            date_key = str(adx_item.get('date', '') or '')
            site_name = str(adx_item.get('site_name', '') or '')
            base_subdomain = extract_base_subdomain(site_name)
            country_code = normalize_country_code(adx_item.get('country_code', '') or '')
            country_name = adx_item.get('country_name', '') or ''
            revenue = float(adx_item.get('revenue', 0) or 0)
            
            impressions_adx = int(adx_item.get('impressions', 0) or 0)
            clicks_adx = int(adx_item.get('clicks', 0) or 0)
            total_requests = int(adx_item.get('total_requests', 0) or 0)
            responses_served = int(adx_item.get('responses_served', 0) or 0)
            match_rate = float(adx_item.get('match_rate', 0) or 0)
            fill_rate = float(adx_item.get('fill_rate', 0) or 0)
            active_view_pct_viewable = float(adx_item.get('active_view_pct_viewable', 0) or 0)
            active_view_avg_time_sec = float(adx_item.get('active_view_avg_time_sec', 0) or 0)

            if not date_key or not base_subdomain or not country_code:
                continue
            country_name_by_code[country_code] = country_name or country_name_by_code.get(country_code, '')
            k = f"{date_key}|{base_subdomain}|{country_code}"
            rev_map[k] = rev_map.get(k, 0.0) + revenue
            
            entry = adx_metrics_map.get(k) or {
                'impressions_adx': 0, 'clicks_adx': 0, 'total_requests': 0, 'responses_served': 0,
                'match_rate_sum': 0.0, 'fill_rate_sum': 0.0, 'active_view_pct_viewable_sum': 0.0, 'active_view_avg_time_sec_sum': 0.0, 'count': 0
            }
            entry['impressions_adx'] += impressions_adx
            entry['clicks_adx'] += clicks_adx
            entry['total_requests'] += total_requests
            entry['responses_served'] += responses_served
            entry['match_rate_sum'] += match_rate
            entry['fill_rate_sum'] += fill_rate
            entry['active_view_pct_viewable_sum'] += active_view_pct_viewable
            entry['active_view_avg_time_sec_sum'] += active_view_avg_time_sec
            entry['count'] += 1
            adx_metrics_map[k] = entry

        for fb_item in (fb_items or []):
            if not isinstance(fb_item, dict):
                continue
            date_key = str(fb_item.get('date', '') or '')
            domain = str(fb_item.get('domain', '') or '')
            base_subdomain = extract_base_subdomain(domain)
            country_code = normalize_country_code(fb_item.get('country_code', '') or '')
            country_name = fb_item.get('country_name', '') or ''
            spend = float(fb_item.get('spend', 0) or 0)
            if not date_key or not base_subdomain or not country_code:
                continue
            country_name_by_code[country_code] = country_name or country_name_by_code.get(country_code, '')
            k = f"{date_key}|{base_subdomain}|{country_code}"
            spend_map[k] = spend_map.get(k, 0.0) + spend

        union_keys = set(list(rev_map.keys()) + list(spend_map.keys()))
        daily_agg = {}
        for k in union_keys:
            parts = str(k).split('|')
            if len(parts) != 3:
                continue
            date_key, _, country_code = parts
            revenue = float(rev_map.get(k, 0.0) or 0.0)
            spend = float(spend_map.get(k, 0.0) or 0.0)
            adx_metrics = adx_metrics_map.get(k) or {
                'impressions_adx': 0, 'clicks_adx': 0, 'total_requests': 0, 'responses_served': 0,
                'match_rate_sum': 0.0, 'fill_rate_sum': 0.0, 'active_view_pct_viewable_sum': 0.0, 'active_view_avg_time_sec_sum': 0.0, 'count': 0
            }
            dk = (date_key, country_code)
            cur = daily_agg.get(dk)
            if not cur:
                cur = {
                    'date': date_key, 'country_code': country_code, 'country': country_name_by_code.get(country_code, ''), 
                    'spend': 0.0, 'revenue': 0.0,
                    'impressions_adx': 0, 'clicks_adx': 0, 'total_requests': 0, 'responses_served': 0,
                    'match_rate_sum': 0.0, 'fill_rate_sum': 0.0, 'active_view_pct_viewable_sum': 0.0, 'active_view_avg_time_sec_sum': 0.0, 'count': 0
                }
                daily_agg[dk] = cur
            cur['spend'] += spend
            cur['revenue'] += revenue
            cur['impressions_adx'] += adx_metrics['impressions_adx']
            cur['clicks_adx'] += adx_metrics['clicks_adx']
            cur['total_requests'] += adx_metrics['total_requests']
            cur['responses_served'] += adx_metrics['responses_served']
            cur['match_rate_sum'] += adx_metrics['match_rate_sum']
            cur['fill_rate_sum'] += adx_metrics['fill_rate_sum']
            cur['active_view_pct_viewable_sum'] += adx_metrics['active_view_pct_viewable_sum']
            cur['active_view_avg_time_sec_sum'] += adx_metrics['active_view_avg_time_sec_sum']
            cur['count'] += adx_metrics['count']

        rows = []
        for (_, _), item in daily_agg.items():
            s = float(item.get('spend', 0.0) or 0.0)
            r = float(item.get('revenue', 0.0) or 0.0)
            net_profit = r - s
            roi = ((r - s) / s * 100) if s > 0 else 0.0
            cnt = max(1, item['count'])
            
            cpc_adx = r / item['clicks_adx'] if item['clicks_adx'] > 0 else 0.0
            ecpm_adx = (r / item['impressions_adx']) * 1000 if item['impressions_adx'] > 0 else 0.0
            
            rows.append({
                'date': item.get('date'),
                'country': item.get('country') or country_name_by_code.get(item.get('country_code'), ''),
                'country_code': item.get('country_code'),
                'spend': round(s, 2),
                'revenue': round(r, 2),
                'net_profit': round(net_profit, 2),
                'roi': round(roi, 2),
                'impressions': item['impressions_adx'],
                'clicks': item['clicks_adx'],
                'cpc': round(cpc_adx, 2),
                'ecpm': round(ecpm_adx, 2),
                'total_requests': item['total_requests'],
                'responses_served': item['responses_served'],
                'match_rate': round(item['match_rate_sum'] / cnt, 2),
                'fill_rate': round(item['fill_rate_sum'] / cnt, 2),
                'active_view_pct_viewable': round(item['active_view_pct_viewable_sum'] / cnt, 2),
                'active_view_avg_time_sec': round(item['active_view_avg_time_sec_sum'] / cnt, 2),
            })

        rows.sort(key=lambda x: (str(x.get('date') or ''), str(x.get('country_code') or '')))
        return rows
    except Exception:
        return []

class RoiTrafficPerDomainView(View):
    """View untuk ROI Per Domain"""
    def dispatch(self, request, *args, **kwargs):
        if 'hris_admin' not in request.session:
            return redirect('admin_login')
        return super().dispatch(request, *args, **kwargs)
    def get(self, req):
        admin = req.session.get('hris_admin', {})
        if admin.get('super_st') == '0':
            data_account_adx = data_mysql().get_all_adx_account_data_user(admin.get('user_id'))
            data_domain_adx = data_mysql().get_all_adx_domain_data_user(admin.get('user_id'))
        else:
            data_account_adx = data_mysql().get_all_adx_account_data()
            data_domain_adx = data_mysql().get_all_adx_domain_data()
        data_account = data_mysql().master_account_ads()['data']
        last_update = data_mysql().get_last_update_adx_traffic_per_domain()['data']['last_update']
        data = {
            'title': 'ROI Per Domain',
            'user': req.session['hris_admin'],
            'data_account': data_account,
            'data_account_adx': data_account_adx['data'],
            'data_domain_adx': data_domain_adx['data'],
            'last_update': last_update
        }
        return render(req, 'admin/report_roi/per_domain/index.html', data)

class TrafficPerDomainReportView(View):
    """View untuk Traffic Per Domain"""
    def dispatch(self, request, *args, **kwargs):
        if 'hris_admin' not in request.session:
            return redirect('admin_login')
        return super().dispatch(request, *args, **kwargs)

    def get(self, req):
        data = {
            'title': 'Traffic Per Domain',
            'user': req.session['hris_admin']
        }
        return render(req, 'admin/report_traffic/per_domain/index.html', data)

class TrafficPerDomainAdSpendView(View):
    """AJAX endpoint total ad spend for selected domain(s) + report range."""
    def dispatch(self, request, *args, **kwargs):
        if 'hris_admin' not in request.session:
            return redirect('admin_login')
        return super().dispatch(request, *args, **kwargs)

    @staticmethod
    def _to_domain_key(domain_value):
        s = str(domain_value or '').strip().lower()
        if not s:
            return ''
        parts = [p for p in s.split('.') if p]
        if len(parts) >= 2:
            return parts[0] + '.' + parts[1]
        return s

    @staticmethod
    def _range_to_dates(report):
        today = datetime.now().date()
        r = str(report or 'today').strip().lower()
        if r == '7days':
            start = today - timedelta(days=6)
        elif r == '30days':
            start = today - timedelta(days=29)
        elif r == '90days':
            start = today - timedelta(days=89)
        else:
            start = today
        return start.strftime('%Y-%m-%d'), today.strftime('%Y-%m-%d')

    def get(self, req):
        try:
            report = req.GET.get('report', 'today')
            domains_csv = str(req.GET.get('domains') or '').strip()
            raw_domains = [d.strip() for d in domains_csv.split(',') if d.strip()]
            domain_keys = []
            seen = set()
            for raw in raw_domains:
                key = self._to_domain_key(raw)
                if key and key not in seen:
                    seen.add(key)
                    domain_keys.append(key)

            start_date, end_date = self._range_to_dates(report)
            rs_spend = data_mysql().get_total_ads_spend_by_domain_keys_and_date(domain_keys, start_date, end_date)
            if not isinstance(rs_spend, dict) or not rs_spend.get('status'):
                return JsonResponse({
                    'status': False,
                    'error': (rs_spend or {}).get('data') if isinstance(rs_spend, dict) else 'Failed query ad spend'
                }, status=500)

            rs_revenue = data_mysql().get_total_adx_revenue_by_domains_and_date(raw_domains, start_date, end_date)
            if not isinstance(rs_revenue, dict) or not rs_revenue.get('status'):
                return JsonResponse({
                    'status': False,
                    'error': (rs_revenue or {}).get('data') if isinstance(rs_revenue, dict) else 'Failed query revenue'
                }, status=500)

            total_ad_spend = float(((rs_spend.get('data') or {}).get('total_ad_spend')) or 0)
            total_revenue = float(((rs_revenue.get('data') or {}).get('total_revenue')) or 0)
            return JsonResponse({
                'status': True,
                'total_ad_spend': total_ad_spend,
                'total_revenue': total_revenue,
                'start_date': start_date,
                'end_date': end_date
            })
        except Exception as e:
            return JsonResponse({'status': False, 'error': str(e)}, status=500)

class TrafficPerCampaignReportView(View):
    """View untuk Traffic Per Campaign"""
    def dispatch(self, request, *args, **kwargs):
        if 'hris_admin' not in request.session:
            return redirect('admin_login')
        return super().dispatch(request, *args, **kwargs)

    def get(self, req):
        data = {
            'title': 'Traffic Per Campaign',
            'user': req.session['hris_admin']
        }
        return render(req, 'admin/report_traffic/per_campaign/index.html', data)

class TrafficCampaignListView(View):
    """AJAX endpoint campaign list + campaign name Facebook (cached 6 jam)"""
    CACHE_KEY = generate_cache_key('traffic_campaign_list_with_name_v1')
    NAME_MAP_KEY = generate_cache_key('traffic_campaign_name_map_v1')
    CACHE_TTL_SECONDS = 6 * 60 * 60
    JOB_TTL_SECONDS = 10 * 60

    def dispatch(self, request, *args, **kwargs):
        if 'hris_admin' not in request.session:
            return redirect('admin_login')
        return super().dispatch(request, *args, **kwargs)

    @classmethod
    def _job_cache_key(cls, job_id):
        return generate_cache_key('traffic_campaign_refresh_job_v1', job_id)

    @classmethod
    def _set_job_state(cls, job_id, state):
        set_cached_data(cls._job_cache_key(job_id), state, cls.JOB_TTL_SECONDS)

    @classmethod
    def _get_job_state(cls, job_id):
        state = get_cached_data(cls._job_cache_key(job_id))
        return state if isinstance(state, dict) else None

    @staticmethod
    def _is_invalid_campaign_name(value):
        if not isinstance(value, str):
            return True
        cleaned = value.strip()
        if not cleaned:
            return True
        return cleaned.lower() in ('not found', 'failed')

    @classmethod
    def _resolve_campaign_payload(cls, progress_callback=None):
        cached_payload = get_cached_data(cls.CACHE_KEY)
        tracker_resp = None
        tracker_error = None
        try:
            tracker_resp = requests.get(
                "https://api-tracker.kiwipixel.com/v1/campaign?token=tgh-app",
                timeout=30,
                headers={
                    "Accept": "application/json",
                    "User-Agent": "hris-management/1.0"
                }
            )
        except Exception as req_err:
            tracker_error = str(req_err)

        if tracker_resp is None or tracker_resp.status_code != 200:
            if isinstance(cached_payload, dict) and isinstance(cached_payload.get('campaigns'), list):
                cached_copy = dict(cached_payload)
                cached_copy['stale'] = True
                cached_copy['warning'] = tracker_error or f"refresh_failed_status_{getattr(tracker_resp, 'status_code', 'no_response')}"
                return cached_copy
            return {
                'status': True,
                'filters': {},
                'campaigns': [],
                'stale': True,
                'warning': tracker_error or f'source_unavailable_status_{getattr(tracker_resp, "status_code", "no_response")}'
            }

        tracker_json = tracker_resp.json() if tracker_resp.content else {}
        if not isinstance(tracker_json, dict):
            tracker_json = {}
        tracker_campaigns = tracker_json.get('campaigns') or []
        if not isinstance(tracker_campaigns, list):
            tracker_campaigns = []

        campaign_name_map = get_cached_data(cls.NAME_MAP_KEY) or {}
        if not isinstance(campaign_name_map, dict):
            campaign_name_map = {}
        if isinstance(cached_payload, dict) and isinstance(cached_payload.get('campaigns'), list):
            for cached_item in cached_payload.get('campaigns'):
                if not isinstance(cached_item, dict):
                    continue
                cached_id = str(cached_item.get('utm_id') or '').strip()
                cached_name = cached_item.get('campaign_name')
                if cached_id and isinstance(cached_name, str) and cached_name.strip():
                    if cached_name.strip().lower() not in ('not found', 'failed'):
                        campaign_name_map[cached_id] = cached_name.strip()

        # Progress denominator = only campaigns that need DB check/recheck
        # (exclude campaigns that already have valid cached names).
        total_campaign = 0
        ids_to_check = []
        for item in tracker_campaigns:
            row = item if isinstance(item, dict) else {}
            campaign_id = str(row.get('utm_id') or '').strip()
            if not campaign_id:
                continue
            cached_name = str(campaign_name_map.get(campaign_id) or '').strip()
            if cls._is_invalid_campaign_name(cached_name):
                total_campaign += 1
                ids_to_check.append(campaign_id)

        db_name_map = {}
        if ids_to_check:
            rs_name_map = data_mysql().get_master_ads_campaign_name_map(ids_to_check)
            if isinstance(rs_name_map, dict) and rs_name_map.get('status'):
                db_name_map = rs_name_map.get('data') or {}
            if not isinstance(db_name_map, dict):
                db_name_map = {}

        checked_done = 0
        enriched_campaigns = []

        for item in tracker_campaigns:
            row = item if isinstance(item, dict) else {}
            campaign_id = str(row.get('utm_id') or '').strip()
            domains = str(row.get('domains') or '').strip()
            if not campaign_id:
                continue

            campaign_name = str(campaign_name_map.get(campaign_id) or '').strip()
            if cls._is_invalid_campaign_name(campaign_name):
                campaign_name = str(db_name_map.get(campaign_id) or '').strip()
                checked_done += 1
                if not campaign_name:
                    campaign_name = 'Not Found'

            if campaign_name:
                campaign_name_map[campaign_id] = campaign_name

            enriched_campaigns.append({
                'utm_id': campaign_id,
                'domains': domains,
                'campaign_name': campaign_name or campaign_id
            })

            if callable(progress_callback):
                progress_callback(total_campaign, checked_done)

        payload = {
            'filters': tracker_json.get('filters') if isinstance(tracker_json.get('filters'), dict) else {},
            'campaigns': enriched_campaigns,
            'total_campaign': total_campaign,
            'checked_done': checked_done
        }
        set_cached_data(cls.CACHE_KEY, payload, cls.CACHE_TTL_SECONDS)
        set_cached_data(cls.NAME_MAP_KEY, campaign_name_map, cls.CACHE_TTL_SECONDS)
        return payload

    @classmethod
    def _run_refresh_job(cls, job_id):
        try:
            def on_progress(total_campaign, checked_done):
                current = cls._get_job_state(job_id) or {}
                current.update({
                    'status': True,
                    'running': True,
                    'total_campaign': int(total_campaign or 0),
                    'checked_done': int(checked_done or 0),
                })
                cls._set_job_state(job_id, current)

            payload = cls._resolve_campaign_payload(progress_callback=on_progress)
            cls._set_job_state(job_id, {
                'status': True,
                'running': False,
                'total_campaign': int(payload.get('total_campaign') or 0),
                'checked_done': int(payload.get('checked_done') or 0),
                'payload': payload
            })
        except Exception as e:
            cls._set_job_state(job_id, {
                'status': False,
                'running': False,
                'error': str(e)
            })

    def get(self, req):
        try:
            refresh = str(req.GET.get('refresh', '')).strip().lower() in ('1', 'true', 'yes')
            async_mode = str(req.GET.get('async', '')).strip().lower() in ('1', 'true', 'yes')
            job_id = str(req.GET.get('job_id') or '').strip()
            cached_payload = get_cached_data(self.CACHE_KEY)

            if job_id:
                state = self._get_job_state(job_id)
                if not state:
                    return JsonResponse({'status': False, 'error': 'job_not_found'}, status=404)
                return JsonResponse(state)

            if refresh and async_mode:
                new_job_id = uuid.uuid4().hex
                self._set_job_state(new_job_id, {
                    'status': True,
                    'running': True,
                    'total_campaign': 0,
                    'checked_done': 0
                })
                import threading
                worker = threading.Thread(target=self._run_refresh_job, args=(new_job_id,), daemon=True)
                worker.start()
                return JsonResponse({
                    'status': True,
                    'job_id': new_job_id,
                    'running': True,
                    'total_campaign': 0,
                    'checked_done': 0
                })

            if not refresh:
                if isinstance(cached_payload, dict) and isinstance(cached_payload.get('campaigns'), list):
                    return JsonResponse(cached_payload)
            payload = self._resolve_campaign_payload()
            return JsonResponse(payload)
        except Exception as e:
            return JsonResponse({
                'status': False,
                'error': str(e)
            }, status=500)

class RoiTrafficPerDomainDataView(View):
    """AJAX endpoint untuk data ROI Traffic Per Domain"""
    def dispatch(self, request, *args, **kwargs):
        if 'hris_admin' not in request.session:
            return redirect('admin_login')
        return super().dispatch(request, *args, **kwargs)
    def get(self, req):
        try:
            start_date = req.GET.get('start_date') or req.GET.get('tanggal_dari')
            end_date = req.GET.get('end_date') or req.GET.get('tanggal_sampai') or req.GET.get('date')
            selected_accounts = req.GET.get('selected_account_adx')
            today_ymd = datetime.now().strftime('%Y-%m-%d')
            if not start_date:
                start_date = today_ymd
            if not end_date:
                end_date = today_ymd
            admin = req.session.get('hris_admin', {})
            if selected_accounts == '':
                rs_account = data_mysql().get_all_adx_account_data_user(admin.get('user_id'))
                if not isinstance(rs_account, dict) or not rs_account.get('status'):
                    return JsonResponse({
                        'status': False,
                        'error': (rs_account or {}).get('data') or 'Gagal mengambil data account AdX'
                    })
                rows = rs_account.get('data') or []
                if not isinstance(rows, list):
                    rows = []
                account_ids = []
                for item in rows:
                    aid = None
                    try:
                        if isinstance(item, dict):
                            aid = item.get('account_id')
                    except Exception:
                        aid = None
                    if aid is not None and str(aid).strip():
                        account_ids.append(str(aid))
                selected_accounts = ",".join(account_ids)
            else:
                selected_accounts = req.GET.get('selected_account_adx', '')
            selected_domain_filter = str(req.GET.get('selected_domains') or '').strip()
            selected_account_ads = req.GET.get('selected_account_ads')
            domain_terms = build_domain_filter_terms(selected_domain_filter, include_original=True, include_base=True)
            domain_terms_fb = build_domain_filter_terms(selected_domain_filter, include_original=False, include_base=True)
            # --- 1. Parse tanggal aman
            def parse_date(d):
                s = str(d or '').strip()
                try:
                    return datetime.strptime(s, '%Y-%m-%d').strftime('%Y-%m-%d')
                except (ValueError, TypeError):
                    raise ValueError(f"Tanggal tidak valid: {s}")
            start_date_formatted = parse_date(start_date)
            end_date_formatted = parse_date(end_date)
            # --- 2. Normalisasi selected_sites_list
            selected_account_list = []
            if selected_accounts:
                selected_account_list = [str(s).strip() for s in selected_accounts.split(',') if s.strip()]
            # --- 3. Ambil data AdX
            adx_result = data_mysql().get_all_adx_traffic_account_by_params(
                start_date_formatted,
                end_date_formatted,
                selected_account_list,
                domain_terms
            )
            # --- 4. Proses Facebook data
            facebook_data = None
            unique_name_site = []
            if domain_terms_fb:
                seen_sites = set()
                for site in domain_terms_fb:
                    site_name = str(site or '').strip().strip("\"'")
                    if not site_name or site_name == 'Unknown' or site_name in seen_sites:
                        continue
                    seen_sites.add(site_name)
                    unique_name_site.append(site_name)
            elif adx_result:
                # Ambil unique site dari AdX
                extracted_sites = set()
                for adx_item in adx_result['hasil']['data']:
                    site_name = str(adx_item.get('site_name', '')).strip()
                    if site_name and site_name != 'Unknown':
                        extracted_sites.add(site_name)
                for site in extracted_sites:
                    if "." in site:
                        parts = site.split(".")       # pisah berdasarkan titik
                        main_domain = ".".join(parts[:2])
                    else:
                        main_domain = site
                    unique_name_site.append(main_domain)
            unique_name_site = list(set(unique_name_site))
            if unique_name_site:
                facebook_data = data_mysql().get_all_ads_roi_traffic_campaign_by_params(
                    start_date_formatted,
                    end_date_formatted,
                    unique_name_site
                )
            # --- 5. Gabungkan data AdX dan Facebook
            raw_rows_all = []
            combined_data_all = []
            combined_data_filtered = []
            total_spend = 0.0
            total_impressions_fb = 0.0
            total_clicks_fb = 0.0
            total_impressions_adx = 0.0
            total_clicks_adx = 0.0
            total_cpr = 0.0
            total_ctr_fb = 0.0
            total_cpc_fb = 0.0
            total_cpc_adx = 0.0
            total_ctr_adx = 0.0
            total_cpm = 0.0
            total_revenue = 0.0 

            def normalize_country_code(cc):
                c = (str(cc or '').strip().upper())
                if not c:
                    return ''
                if c == 'TU':
                    return 'TR'
                return c

            facebook_map = {}
            if facebook_data and facebook_data['hasil']['data']:
                for fb_item in facebook_data['hasil']['data']:
                    date_key = str(fb_item.get('date', ''))
                    subdomain = str(fb_item.get('domain', ''))
                    country_code = normalize_country_code(fb_item.get('country_code', ''))
                    key = f"{date_key}_{extract_base_subdomain(subdomain)}_{country_code}"
                    facebook_map[key] = fb_item
            if adx_result and adx_result['hasil']['data']:
                 # --- NEW: siapkan raw_rows + dua grup agregasi (per date+domain)
                grouped_all = {}
                grouped_filtered = {}
                seen_fb_keys = set()
                for adx_item in adx_result['hasil']['data']:
                    date_key = str(adx_item.get('date', ''))
                    subdomain = str(adx_item.get('site_name', ''))
                    impressions_adx = float(adx_item.get('impressions_adx', 0))
                    base_subdomain = extract_base_subdomain(subdomain)
                    country_code = normalize_country_code(adx_item.get('country_code', ''))
                    fb_key = f"{date_key}_{base_subdomain}_{country_code}"
                    seen_fb_keys.add(fb_key)
                    fb_data = facebook_map.get(fb_key)
                    spend = float(fb_data.get('spend') or 0) if fb_data else 0
                    impressions_fb = float(fb_data.get('impressions_fb') or 0) if fb_data else 0
                    clicks_fb = float(fb_data.get('clicks_fb') or 0) if fb_data else 0
                    clicks_adx = float(adx_item.get('clicks_adx') or 0)
                    cpr = float(fb_data.get('cpr') or 0) if fb_data else 0
                    cpc = float(fb_data.get('cpc_fb') or 0) if fb_data else 0
                    revenue = float(adx_item.get('revenue', 0))
                    ctr_fb = ((clicks_fb / impressions_fb) * 100) if impressions_fb > 0 else 0
                    cpc_fb = cpc
                    ctr_adx = ((clicks_adx / impressions_adx) * 100) if impressions_adx > 0 else 0
                    cpc_adx = (revenue / clicks_adx) if clicks_adx > 0 else 0
                    cpm = float(adx_item.get('ecpm', 0))
                    raw_rows_all.append({
                        'site_name': base_subdomain or subdomain,
                        'date': date_key,
                        'country_code': country_code,
                        'spend': spend,
                        'impressions_fb': impressions_fb,
                        'clicks_fb': clicks_fb,
                        'impressions_adx': impressions_adx,
                        'clicks_adx': clicks_adx,
                        'cpr': cpr,
                        'ctr_fb': ctr_fb,
                        'cpc_fb': cpc_fb,
                        'ctr_adx': ctr_adx,
                        'cpc_adx': cpc_adx,
                        'cpm': cpm,
                        'revenue': revenue
                    })
                    key = f"{date_key}|{base_subdomain or subdomain}"
                    entry = grouped_all.get(key) or {
                        'site_name': base_subdomain or subdomain,
                        'date': date_key,
                        'spend': 0.0,
                        'revenue': 0.0,
                        'impressions_fb': 0.0,
                        'impressions_adx': 0.0,
                        'clicks_fb': 0.0,
                        'clicks_adx': 0.0,
                        # avg fields
                        'cpr_sum': 0.0,
                        'cpr_cnt': 0,
                        'cpr': 0.0,
                        'ctr_fb': 0.0,
                        'cpc_fb': 0.0,
                        'ctr_adx': 0.0,
                        'cpc_adx': 0.0,
                        'cpm_sum': 0.0,
                        'cpm_cnt': 0,
                        'cpm': 0.0,
                    }
                    entry['spend'] += spend
                    entry['impressions_fb'] += impressions_fb
                    entry['clicks_fb'] += clicks_fb
                    entry['impressions_adx'] += impressions_adx
                    entry['clicks_adx'] += clicks_adx
                    entry['revenue'] += revenue

                    # CPR avg per (date|domain)
                    entry['cpr_sum'] += cpr
                    entry['cpr_cnt'] += 1
                    entry['cpr'] = (entry['cpr_sum'] / entry['cpr_cnt']) if entry['cpr_cnt'] > 0 else 0

                    entry['ctr_fb'] = ((entry['clicks_fb'] / entry['impressions_fb']) * 100) if entry['impressions_fb'] > 0 else 0
                    entry['cpc_fb'] = (entry['revenue'] / entry['clicks_fb']) if entry['clicks_fb'] > 0 else 0
                    entry['ctr_adx'] = ((entry['clicks_adx'] / entry['impressions_adx']) * 100) if entry['impressions_adx'] > 0 else 0
                    entry['cpc_adx'] = (entry['revenue'] / entry['clicks_adx']) if entry['clicks_adx'] > 0 else 0

                    # eCPM avg per (date|domain)
                    entry['cpm_sum'] += cpm
                    entry['cpm_cnt'] += 1
                    entry['cpm'] = (entry['cpm_sum'] / entry['cpm_cnt']) if entry['cpm_cnt'] > 0 else 0

                    grouped_all[key] = entry
                    if spend > 0:
                        f_entry = grouped_filtered.get(key) or {
                            'site_name': base_subdomain or subdomain,
                            'date': date_key,
                            'spend': 0.0,
                            'revenue': 0.0,
                            'impressions_fb': 0.0,
                            'impressions_adx': 0.0,
                            'clicks_fb': 0.0,
                            'clicks_adx': 0.0,
                            # avg fields
                            'cpr_sum': 0.0,
                            'cpr_cnt': 0,
                            'cpr': 0.0,
                            'ctr_fb': 0.0,
                            'cpc_fb': 0.0,
                            'ctr_adx': 0.0,
                            'cpc_adx': 0.0,
                            'cpm_sum': 0.0,
                            'cpm_cnt': 0,
                            'cpm': 0.0,
                        }
                        f_entry['spend'] += spend
                        f_entry['impressions_fb'] += impressions_fb
                        f_entry['clicks_fb'] += clicks_fb
                        f_entry['impressions_adx'] += impressions_adx
                        f_entry['clicks_adx'] += clicks_adx
                        f_entry['revenue'] += revenue

                        # CPR avg per (date|domain)
                        f_entry['cpr_sum'] += cpr
                        f_entry['cpr_cnt'] += 1
                        f_entry['cpr'] = (f_entry['cpr_sum'] / f_entry['cpr_cnt']) if f_entry['cpr_cnt'] > 0 else 0

                        f_entry['ctr_fb'] = ((f_entry['clicks_fb'] / f_entry['impressions_fb']) * 100) if f_entry['impressions_fb'] > 0 else 0
                        f_entry['cpc_fb'] = (f_entry['revenue'] / f_entry['clicks_fb']) if f_entry['clicks_fb'] > 0 else 0
                        f_entry['ctr_adx'] = ((f_entry['clicks_adx'] / f_entry['impressions_adx']) * 100) if f_entry['impressions_adx'] > 0 else 0
                        f_entry['cpc_adx'] = (f_entry['revenue'] / f_entry['clicks_adx']) if f_entry['clicks_adx'] > 0 else 0

                        # eCPM avg per (date|domain)
                        f_entry['cpm_sum'] += cpm
                        f_entry['cpm_cnt'] += 1
                        f_entry['cpm'] = (f_entry['cpm_sum'] / f_entry['cpm_cnt']) if f_entry['cpm_cnt'] > 0 else 0

                        grouped_filtered[key] = f_entry

                # Tambahkan baris FB yang tidak punya pasangan AdX (supaya total spend konsisten)
                for fb_key, fb_item in (facebook_map or {}).items():
                    if fb_key in seen_fb_keys:
                        continue
                    date_key = str(fb_item.get('date', ''))
                    subdomain = str(fb_item.get('domain', ''))
                    base_subdomain = extract_base_subdomain(subdomain)
                    country_code = normalize_country_code(fb_item.get('country_code', ''))
                    spend = float(fb_item.get('spend') or 0)
                    impressions_fb = float(fb_item.get('impressions_fb') or 0)
                    clicks_fb = float(fb_item.get('clicks_fb') or 0)
                    cpr = float(fb_item.get('cpr') or 0)
                    cpc_fb = float(fb_item.get('cpc_fb') or 0)

                    raw_rows_all.append({
                        'site_name': base_subdomain or subdomain,
                        'date': date_key,
                        'country_code': country_code,
                        'spend': spend,
                        'impressions_fb': impressions_fb,
                        'clicks_fb': clicks_fb,
                        'impressions_adx': 0.0,
                        'clicks_adx': 0.0,
                        'cpr': cpr,
                        'ctr_fb': ((clicks_fb / impressions_fb) * 100) if impressions_fb > 0 else 0,
                        'cpc_fb': cpc_fb,
                        'ctr_adx': 0.0,
                        'cpc_adx': 0.0,
                        'cpm': 0.0,
                        'revenue': 0.0
                    })

                    key = f"{date_key}|{base_subdomain or subdomain}"
                    entry = grouped_all.get(key) or {
                        'site_name': base_subdomain or subdomain,
                        'date': date_key,
                        'spend': 0.0,
                        'revenue': 0.0,
                        'impressions_fb': 0.0,
                        'impressions_adx': 0.0,
                        'clicks_fb': 0.0,
                        'clicks_adx': 0.0,
                        'cpr_sum': 0.0,
                        'cpr_cnt': 0,
                        'cpr': 0.0,
                        'ctr_fb': 0.0,
                        'cpc_fb': 0.0,
                        'ctr_adx': 0.0,
                        'cpc_adx': 0.0,
                        'cpm_sum': 0.0,
                        'cpm_cnt': 0,
                        'cpm': 0.0,
                    }

                    entry['spend'] += spend
                    entry['impressions_fb'] += impressions_fb
                    entry['clicks_fb'] += clicks_fb
                    entry['revenue'] += 0.0
                    entry['cpr_sum'] += cpr
                    entry['cpr_cnt'] += 1
                    entry['cpr'] = (entry['cpr_sum'] / entry['cpr_cnt']) if entry['cpr_cnt'] > 0 else 0
                    entry['ctr_fb'] = ((entry['clicks_fb'] / entry['impressions_fb']) * 100) if entry['impressions_fb'] > 0 else 0
                    entry['cpc_fb'] = (entry['revenue'] / entry['clicks_fb']) if entry['clicks_fb'] > 0 else 0
                    grouped_all[key] = entry

                    if spend > 0:
                        f_entry = grouped_filtered.get(key) or {
                            'site_name': base_subdomain or subdomain,
                            'date': date_key,
                            'spend': 0.0,
                            'revenue': 0.0,
                            'impressions_fb': 0.0,
                            'impressions_adx': 0.0,
                            'clicks_fb': 0.0,
                            'clicks_adx': 0.0,
                            'cpr_sum': 0.0,
                            'cpr_cnt': 0,
                            'cpr': 0.0,
                            'ctr_fb': 0.0,
                            'cpc_fb': 0.0,
                            'ctr_adx': 0.0,
                            'cpc_adx': 0.0,
                            'cpm_sum': 0.0,
                            'cpm_cnt': 0,
                            'cpm': 0.0,
                        }
                        f_entry['spend'] += spend
                        f_entry['impressions_fb'] += impressions_fb
                        f_entry['clicks_fb'] += clicks_fb
                        f_entry['revenue'] += 0.0
                        f_entry['cpr_sum'] += cpr
                        f_entry['cpr_cnt'] += 1
                        f_entry['cpr'] = (f_entry['cpr_sum'] / f_entry['cpr_cnt']) if f_entry['cpr_cnt'] > 0 else 0
                        f_entry['ctr_fb'] = ((f_entry['clicks_fb'] / f_entry['impressions_fb']) * 100) if f_entry['impressions_fb'] > 0 else 0
                        f_entry['cpc_fb'] = (f_entry['revenue'] / f_entry['clicks_fb']) if f_entry['clicks_fb'] > 0 else 0
                        grouped_filtered[key] = f_entry

                combined_data_all = []
                total_spend = 0.0
                total_impressions_fb = 0.0
                total_clicks_fb = 0.0
                total_impressions_adx = 0.0
                total_clicks_adx = 0.0
                total_cpr = 0.0
                total_ctr_fb = 0.0
                total_cpc_fb = 0.0
                total_ctr_adx = 0.0
                total_cpc_adx = 0.0
                total_cpm = 0.0
                total_revenue = 0.0
                for _, item in sorted(grouped_all.items(), key=lambda kv: (kv[1]['date'], kv[1]['site_name'])):
                    spend_val = item['spend']
                    impressions_fb_val = item['impressions_fb']
                    impressions_adx_val = item['impressions_adx']
                    clicks_fb_val = item['clicks_fb']
                    clicks_adx_val = item['clicks_adx']
                    cpr_val = item['cpr']
                    ctr_fb_val = item['ctr_fb']
                    cpc_fb_val = item['cpc_fb']
                    ctr_adx_val = item['ctr_adx']
                    cpc_adx_val = item['cpc_adx']
                    cpm_val = item['cpm']
                    revenue_val = item['revenue']
                    roi = ((revenue_val - spend_val) / spend_val * 100) if spend_val > 0 else 0
                    combined_data_all.append({
                        'site_name': item['site_name'] + '.com',
                        'date': item['date'],
                        'spend': spend_val,
                        'impressions_fb': impressions_fb_val,
                        'clicks_fb': clicks_fb_val,
                        'impressions_adx': impressions_adx_val,
                        'clicks_adx': clicks_adx_val,
                        'cpr': cpr_val,
                        'ctr_fb': ctr_fb_val,
                        'cpc_fb': cpc_fb_val,
                        'ctr_adx': ctr_adx_val,
                        'cpc_adx': cpc_adx_val,
                        'cpm': cpm_val,
                        'revenue': revenue_val,
                        'roi': roi
                    })
                    total_spend += spend_val
                    total_impressions_fb += impressions_fb_val
                    total_clicks_fb += clicks_fb_val
                    total_impressions_adx += impressions_adx_val
                    total_clicks_adx += clicks_adx_val
                    total_cpr += cpr_val
                    total_ctr_fb += ctr_fb_val
                    total_cpc_fb += cpc_fb_val
                    total_ctr_adx += ctr_adx_val
                    total_cpc_adx += cpc_adx_val
                    total_cpm += cpm_val
                    total_revenue += revenue_val

                combined_data_filtered = []
                for _, item in sorted(grouped_filtered.items(), key=lambda kv: (kv[1]['date'], kv[1]['site_name'])):
                    spend_val = item['spend']
                    impressions_fb_val = item['impressions_fb']
                    impressions_adx_val = item['impressions_adx']
                    clicks_fb_val = item['clicks_fb']
                    clicks_adx_val = item['clicks_adx']
                    cpr_val = item['cpr']
                    ctr_fb_val = item['ctr_fb']
                    cpc_fb_val = item['cpc_fb']
                    ctr_adx_val = item['ctr_adx']
                    cpc_adx_val = item['cpc_adx']
                    cpm_val = item['cpm']
                    revenue_val = item['revenue']
                    roi = ((revenue_val - spend_val) / spend_val * 100) if spend_val > 0 else 0
                    combined_data_filtered.append({
                        'site_name': item['site_name'] + '.com',
                        'date': item['date'],
                        'spend': spend_val,
                        'impressions_fb': impressions_fb_val,
                        'clicks_fb': clicks_fb_val,
                        'impressions_adx': impressions_adx_val,
                        'clicks_adx': clicks_adx_val,
                        'cpr': cpr_val,
                        'ctr_fb': ctr_fb_val,
                        'cpc_fb': cpc_fb_val,
                        'ctr_adx': ctr_adx_val,
                        'cpc_adx': cpc_adx_val,
                        'cpm': cpm_val,
                        'revenue': revenue_val,
                        'roi': roi
                    })
            roi_nett_summary = ((total_revenue - total_spend) / total_spend * 100) if total_spend > 0 else 0

            result = {
                'status': True,
                'data': combined_data_all,              # semua kontribusi
                'data_filtered': combined_data_filtered, # hanya spend > 0
                'summary': {
                    'total_clicks_fb': total_clicks_fb,
                    'total_clicks_adx': total_clicks_adx,
                    'total_spend': total_spend,
                    'roi_nett': roi_nett_summary,
                    'total_revenue': total_revenue
                }
            }
            return JsonResponse(result, safe=False)
        except Exception as e:
            return JsonResponse({'status': False, 'error': str(e)})

def extract_base_subdomain(full_string):
    parts = full_string.split('.')
    # jika ada minimal 2 bagian (1 titik), ambil dua bagian pertama
    if len(parts) >= 2:
        main_domain = ".".join(parts[:2])
    else:
        main_domain = full_string
    # jika tidak ada titik, kembalikan string asli
    return main_domain


def build_domain_filter_terms(selected_domains, include_original=True, include_base=True):
    """Normalisasi filter domain dari UI (string CSV / list) menjadi list domain unik.
    Mendukung input FQDN dan base-subdomain agar query LIKE tetap match lintas sumber data.
    """
    raw_items = []
    if isinstance(selected_domains, str):
        raw_items = [s for s in selected_domains.split(',')]
    elif isinstance(selected_domains, (list, tuple, set)):
        for item in selected_domains:
            raw_items.extend(str(item or '').split(','))

    terms = []
    seen = set()
    for item in raw_items:
        token = str(item or '').strip().strip("\"'")
        if not token:
            continue

        candidates = []
        if include_original:
            candidates.append(token)
        if include_base:
            base = extract_base_subdomain(token)
            if base:
                candidates.append(base)

        for cand in candidates:
            key = str(cand or '').strip().lower()
            if not key or key in seen:
                continue
            seen.add(key)
            terms.append(str(cand).strip())

    return terms

def normalize_score(val, min_val, max_val):
    try:
        val = float(val or 0)
        min_val = float(min_val or 0)
        max_val = float(max_val or 0)
        if max_val == min_val:
            return 0.0
        return (val - min_val) / (max_val - min_val)
    except Exception:
        return 0.0

def scoring_engine(meta, adx, adsense, weights=None):
    """
    meta, adx, adsense: dict {'1d':..., '3d':..., '7d':..., '14d':...}
    weights: dict, e.g. {'meta':0.4, 'adx':0.3, 'adsense':0.3}
    """
    if weights is None:
        weights = {'meta': 0.4, 'adx': 0.3, 'adsense': 0.3}
    # flatten all values for normalization
    all_vals = []
    for d in [meta, adx, adsense]:
        all_vals += [float(d.get(k, 0) or 0) for k in ['1d','3d','7d','14d']]
    min_v, max_v = min(all_vals), max(all_vals)
    meta_score = sum([normalize_score(meta.get(k, 0), min_v, max_v) for k in ['1d','3d','7d','14d']]) / 4
    adx_score = sum([normalize_score(adx.get(k, 0), min_v, max_v) for k in ['1d','3d','7d','14d']]) / 4
    adsense_score = sum([normalize_score(adsense.get(k, 0), min_v, max_v) for k in ['1d','3d','7d','14d']]) / 4
    final_score = (
        meta_score * weights['meta'] +
        adx_score * weights['adx'] +
        adsense_score * weights['adsense']
    )
    if final_score >= 0.7:
        decision = "scale"
    elif final_score >= 0.4:
        decision = "hold"
    else:
        decision = "stop"
    return {
        "meta_score": round(meta_score, 3),
        "adx_score": round(adx_score, 3),
        "adsense_score": round(adsense_score, 3),
        "final_score": round(final_score, 3),
        "decision": decision
    }

class MonitoringScoringBaselineHourlyView(View):
    def dispatch(self, request, *args, **kwargs):
        if 'hris_admin' not in request.session:
            is_ajax = False
            try:
                is_ajax = request.headers.get('X-Requested-With') == 'XMLHttpRequest'
            except Exception:
                pass
            if not is_ajax:
                is_ajax = request.META.get('HTTP_X_REQUESTED_WITH') == 'XMLHttpRequest'
            if is_ajax:
                return JsonResponse({'status': False, 'error': 'Sesi berakhir atau tidak valid. Silakan login ulang.'})
            return redirect('admin_login')
        return super().dispatch(request, *args, **kwargs)

    def get_metrics_sum(db, table, date_field, days, metrics):
        today = datetime.now().date()
        start = today - timedelta(days=days)
        fields = ', '.join([f"SUM({m}) as {m}" for m in metrics])
        sql = f"""
            SELECT {fields}
            FROM {table}
            WHERE {date_field} >= %s AND {date_field} <= %s
        """
        if db.execute_query(sql, (start, today)):
            row = db.cur_hris.fetchone()
            # Kembalikan dict {metrik: nilai}
            if isinstance(row, dict):
                return {m: float(row.get(m) or 0) for m in metrics}
            else:
                return {m: float(row[i] or 0) for i, m in enumerate(metrics)}
        return {m: 0.0 for m in metrics}
    
    def scoring_engine_multi(meta, adx, adsense, weights=None):
        """
        meta, adx, adsense:         class MonitoringScoringBaselineHourlyView(View):
            def dispatch(self, request, *args, **kwargs):
                if 'hris_admin' not in request.session:
                    is_ajax = False
                    try:
                        is_ajax = request.headers.get('X-Requested-With') == 'XMLHttpRequest'
                    except Exception:
                        pass
                    if not is_ajax:
                        is_ajax = request.META.get('HTTP_X_REQUESTED_WITH') == 'XMLHttpRequest'
                    if is_ajax:
                        return JsonResponse({'status': False, 'error': 'Sesi berakhir atau tidak valid. Silakan login ulang.'})
                    return redirect('admin_login')
                return super().dispatch(request, *args, **kwargs)
        
            def get(self, req):
                try:
                    db = data_mysql()
                    # Daftar metrik numerik utama per tabel
                    meta_metrics = [
                        'log_ads_country_impre­si', 'log_ads_country_click', 'log_ads_country_cpc', 'log_ads_country_ctr',
                        'log_ads_country_cpm', 'log_ads_country_ecpm', 'log_ads_country_revenue'
                    ]
                    adx_metrics = [
                        'log_adx_country_impre­si', 'log_adx_country_click', 'log_adx_country_cpc', 'log_adx_country_ctr',
                        'log_adx_country_cpm', 'log_adx_country_ecpm', 'log_adx_country_total_requests',
                        'log_adx_country_responses_served', 'log_adx_country_match_rate', 'log_adx_country_fill_rate',
                        'log_adx_country_active_view_pct_viewable', 'log_adx_country_active_view_avg_time_sec',
                        'log_adx_country_revenue'
                    ]
                    adsense_metrics = [
                        'log_adsense_country_impre­si', 'log_adsense_country_click', 'log_adsense_country_cpc', 'log_adsense_country_ctr',
                        'log_adsense_country_cpm', 'log_adsense_country_ecpm', 'log_adsense_country_revenue'
                    ]
                    meta = {w: get_metrics_sum(db, 'log_ads_country', 'log_ads_country_tanggal', int(w[:-1]), meta_metrics) for w in ['1d','3d','7d','14d']}
                    adx = {w: get_metrics_sum(db, 'log_adx_country', 'log_adx_country_tanggal', int(w[:-1]), adx_metrics) for w in ['1d','3d','7d','14d']}
                    adsense = {w: get_metrics_sum(db, 'log_adsense_country', 'log_adsense_country_tanggal', int(w[:-1]), adsense_metrics) for w in ['1d','3d','7d','14d']}
                    result = scoring_engine_multi(meta, adx, adsense)
                    return JsonResponse({
                        'status': True,
                        'meta': meta,
                        'adx': adx,
                        'adsense': adsense,
                        'scoring': result
                    })
                except Exception as e:
                    return JsonResponse({'status': False, 'error': str(e)})dict {'1d':{metrik:val,...}, '3d':..., ...}
        """
        if weights is None:
            weights = {'meta': 0.4, 'adx': 0.3, 'adsense': 0.3}
        # Gabungkan semua nilai untuk normalisasi global
        all_vals = []
        for d in [meta, adx, adsense]:
            for window in ['1d','3d','7d','14d']:
                all_vals += list((d.get(window) or {}).values())
        min_v, max_v = min(all_vals), max(all_vals)
        def norm_avg(d):
            vals = []
            for window in ['1d','3d','7d','14d']:
                for v in (d.get(window) or {}).values():
                    vals.append(normalize_score(v, min_v, max_v))
            return sum(vals) / len(vals) if vals else 0.0
        meta_score = norm_avg(meta)
        adx_score = norm_avg(adx)
        adsense_score = norm_avg(adsense)
        final_score = (
            meta_score * weights['meta'] +
            adx_score * weights['adx'] +
            adsense_score * weights['adsense']
        )
        if final_score >= 0.7:
            decision = "scale"
        elif final_score >= 0.4:
            decision = "hold"
        else:
            decision = "stop"
        return {
            "meta_score": round(meta_score, 3),
            "adx_score": round(adx_score, 3),
            "adsense_score": round(adsense_score, 3),
            "final_score": round(final_score, 3),
            "decision": decision
        }

    def get(self, req):
        try:
            # Ambil tanggal hari ini
            today = datetime.now().date()
            db = data_mysql()

            # Helper untuk ambil sum revenue per window hari
            def get_sum(table, date_field, revenue_field, days):
                start = today - timedelta(days=days)
                sql = f"""
                    SELECT SUM({revenue_field}) as total
                    FROM {table}
                    WHERE {date_field} >= %s AND {date_field} <= %s
                """
                if db.execute_query(sql, (start, today)):
                    row = db.cur_hris.fetchone()
                    return float((row.get('total') if isinstance(row, dict) else row[0]) or 0)
                return 0

            meta = {
                '1d': get_sum('log_ads_country', 'log_ads_country_tanggal', 'log_ads_country_revenue', 1),
                '3d': get_sum('log_ads_country', 'log_ads_country_tanggal', 'log_ads_country_revenue', 3),
                '7d': get_sum('log_ads_country', 'log_ads_country_tanggal', 'log_ads_country_revenue', 7),
                '14d': get_sum('log_ads_country', 'log_ads_country_tanggal', 'log_ads_country_revenue', 14),
            }
            adx = { 
                '1d': get_sum('log_adx_country', 'log_adx_country_tanggal', 'log_adx_country_revenue', 1),
                '3d': get_sum('log_adx_country', 'log_adx_country_tanggal', 'log_adx_country_revenue', 3),
                '7d': get_sum('log_adx_country', 'log_adx_country_tanggal', 'log_adx_country_revenue', 7),
                '14d': get_sum('log_adx_country', 'log_adx_country_tanggal', 'log_adx_country_revenue', 14),
            }
            adsense = {
                '1d': get_sum('log_adsense_country', 'log_adsense_country_tanggal', 'log_adsense_country_revenue', 1),
                '3d': get_sum('log_adsense_country', 'log_adsense_country_tanggal', 'log_adsense_country_revenue', 3),
                '7d': get_sum('log_adsense_country', 'log_adsense_country_tanggal', 'log_adsense_country_revenue', 7),
                '14d': get_sum('log_adsense_country', 'log_adsense_country_tanggal', 'log_adsense_country_revenue', 14),
            }

            # Panggil scoring engine (fungsi sudah ada di bawah file ini)
            result = scoring_engine(meta, adx, adsense)
            return JsonResponse({
                'status': True,
                'meta': meta,
                'adx': adx,
                'adsense': adsense,
                'scoring': result
            })
        except Exception as e:
            return JsonResponse({'status': False, 'error': str(e)})

class RoiCountryHourlyDataView(View):
    def dispatch(self, request, *args, **kwargs):
        if 'hris_admin' not in request.session:
            is_ajax = False
            try:
                is_ajax = request.headers.get('X-Requested-With') == 'XMLHttpRequest'
            except Exception:
                pass
            if not is_ajax:
                is_ajax = request.META.get('HTTP_X_REQUESTED_WITH') == 'XMLHttpRequest'
            if is_ajax:
                return JsonResponse({'status': False, 'error': 'Sesi berakhir atau tidak valid. Silakan login ulang.'})
            return redirect('admin_login')
        return super().dispatch(request, *args, **kwargs)
    def get(self, req):
        try:
            target_date = req.GET.get('date')
            if not target_date or not isinstance(target_date, str) or len(target_date) != 10:
                target_date = datetime.now().strftime('%Y-%m-%d')
            selected_domain = req.GET.get('selected_domains', '')
            selected_domain_str = ''
            if selected_domain:
                # gunakan domain pertama jika ada multiple, fokus single domain
                selected_domain_str = str(selected_domain.split(',')[0]).strip()
            cache_key = generate_cache_key(
                'roi_country_hourly_v3',
                target_date,
                selected_domain_str
            )
            cached = get_cached_data(cache_key)
            if cached is not None:
                return JsonResponse(cached, safe=False)
            db = data_mysql()
            adx_resp = db.get_all_adx_roi_country_hourly_logs_by_params(
                target_date,
                selected_domain_str,
            )
            adx_rows = adx_resp.get('data') if isinstance(adx_resp, dict) else []
            ads_resp = db.get_all_ads_roi_country_hourly_logs_by_params(
                target_date,
                selected_domain_str,
            )
            ads_rows = (ads_resp.get('hasil') or {}).get('data') if isinstance(ads_resp, dict) else []
            by_country = {}
            hours_present = set()
            for row in adx_rows or []:
                code = str(row.get('country_code', '') or '').upper()
                name = row.get('country_name', '') or code
                hour = int(row.get('hour', 0) or 0)
                hkey = f"{hour:02d}"
                if code not in by_country:
                    by_country[code] = {'country_code': code, 'country': name, 'revenue': {}, 'spend': {}}
                by_country[code]['revenue'][hkey] = by_country[code]['revenue'].get(hkey, 0.0) + float(row.get('revenue', 0) or 0)
                hours_present.add(hkey)
            for row in ads_rows or []:
                code = str(row.get('country_code', '') or '').upper()
                name = row.get('country_name', '') or code
                hour = int(row.get('hour', 0) or 0)
                hkey = f"{hour:02d}"
                if code not in by_country:
                    by_country[code] = {'country_code': code, 'country': name, 'revenue': {}, 'spend': {}}
                by_country[code]['spend'][hkey] = by_country[code]['spend'].get(hkey, 0.0) + float(row.get('spend', 0) or 0)
                hours_present.add(hkey)
            hours = sorted(list(hours_present), key=lambda x: int(x)) if hours_present else [f"{h:02d}" for h in range(24)]
            countries_series = []
            total_revenue = 0.0
            total_spend = 0.0
            for code, item in by_country.items():
                series = []
                rev_series = []
                spend_series = []
                for h in hours:
                    r = float(item['revenue'].get(h, 0.0))
                    s = float(item['spend'].get(h, 0.0))
                    roi = ((r - s) / s * 100) if s > 0 else 0.0
                    series.append(round(roi, 2))
                    rev_series.append(round(r, 2))
                    spend_series.append(round(s, 2))
                    total_revenue += r
                    total_spend += s
                countries_series.append({
                    'country_code': code,
                    'country': item.get('country', code),
                    'roi': series,
                    'revenue': rev_series,
                    'spend': spend_series
                })
            countries_series.sort(key=lambda x: sum(x['roi']), reverse=True)
            result = {
                'status': True,
                'date': target_date,
                'hours': hours,
                'countries': countries_series,
                'summary': {
                    'total_revenue': round(total_revenue, 2),
                    'total_spend': round(total_spend, 2),
                    'roi_nett': round(((total_revenue - total_spend) / total_spend * 100) if total_spend > 0 else 0, 2),
                    'total_countries': len(countries_series)
                }
            }
            set_cached_data(cache_key, result, timeout=1800)
            return JsonResponse(result, safe=False)
        except Exception as e:
            return JsonResponse({'status': False, 'error': str(e)})




class RoiMonitoringCountryHourlyHeatmapView(View):
    def dispatch(self, request, *args, **kwargs):
        if 'hris_admin' not in request.session:
            is_ajax = False
            try:
                is_ajax = request.headers.get('X-Requested-With') == 'XMLHttpRequest'
            except Exception:
                pass
            if not is_ajax:
                is_ajax = request.META.get('HTTP_X_REQUESTED_WITH') == 'XMLHttpRequest'
            if is_ajax:
                return JsonResponse({'status': False, 'error': 'Sesi berakhir atau tidak valid. Silakan login ulang.'})
            return redirect('admin_login')
        return super().dispatch(request, *args, **kwargs)

    def get(self, req):
        try:
            target_date = req.GET.get('date')
            if not target_date or not isinstance(target_date, str) or len(target_date) != 10:
                target_date = datetime.now().strftime('%Y-%m-%d')

            selected_domains = req.GET.get('selected_domains', '')
            selected_domain_list = []
            if selected_domains:
                selected_domain_list = [str(s).strip() for s in str(selected_domains).split(',') if str(s).strip()]

            cache_key = generate_cache_key(
                'roi_monitoring_country_hourly_heatmap_v1',
                target_date,
                ','.join(selected_domain_list) if selected_domain_list else ''
            )
            cached = get_cached_data(cache_key)
            if cached is not None:
                return JsonResponse(cached, safe=False)

            db = data_mysql()
            adx_resp = db.get_all_adx_roi_country_hourly_logs_by_params(
                target_date,
                selected_domain_list,
            )
            adx_rows = adx_resp.get('data') if isinstance(adx_resp, dict) else []

            ads_resp = db.get_all_ads_roi_country_hourly_logs_by_params(
                target_date,
                selected_domain_list,
            )
            ads_rows = (ads_resp.get('hasil') or {}).get('data') if isinstance(ads_resp, dict) else []

            by_country = {}
            hours_present = set()

            for row in adx_rows or []:
                code = str(row.get('country_code', '') or '').upper()
                name = row.get('country_name', '') or code
                hour = int(row.get('hour', 0) or 0)
                hkey = f"{hour:02d}"
                if code not in by_country:
                    by_country[code] = {'country_code': code, 'country': name, 'revenue': {}, 'spend': {}}
                by_country[code]['revenue'][hkey] = by_country[code]['revenue'].get(hkey, 0.0) + float(row.get('revenue', 0) or 0)
                hours_present.add(hkey)

            for row in ads_rows or []:
                code = str(row.get('country_code', '') or '').upper()
                name = row.get('country_name', '') or code
                hour = int(row.get('hour', 0) or 0)
                hkey = f"{hour:02d}"
                if code not in by_country:
                    by_country[code] = {'country_code': code, 'country': name, 'revenue': {}, 'spend': {}}
                by_country[code]['spend'][hkey] = by_country[code]['spend'].get(hkey, 0.0) + float(row.get('spend', 0) or 0)
                hours_present.add(hkey)

            hours = sorted(list(hours_present), key=lambda x: int(x)) if hours_present else [f"{h:02d}" for h in range(24)]

            countries_series = []
            total_revenue = 0.0
            total_spend = 0.0

            for code, item in by_country.items():
                series = []
                rev_series = []
                spend_series = []
                for h in hours:
                    r = float(item['revenue'].get(h, 0.0))
                    s = float(item['spend'].get(h, 0.0))
                    roi = ((r - s) / s * 100) if s > 0 else 0.0
                    series.append(round(roi, 2))
                    rev_series.append(round(r, 2))
                    spend_series.append(round(s, 2))
                    total_revenue += r
                    total_spend += s
                countries_series.append({
                    'country_code': code,
                    'country': item.get('country', code),
                    'roi': series,
                    'revenue': rev_series,
                    'spend': spend_series
                })

            countries_series.sort(key=lambda x: sum(x['roi']), reverse=True)

            result = {
                'status': True,
                'date': target_date,
                'hours': hours,
                'countries': countries_series,
                'summary': {
                    'total_revenue': round(total_revenue, 2),
                    'total_spend': round(total_spend, 2),
                    'roi_nett': round(((total_revenue - total_spend) / total_spend * 100) if total_spend > 0 else 0, 2),
                    'total_countries': len(countries_series)
                }
            }

            if countries_series:
                set_cached_data(cache_key, result, timeout=1800)

            return JsonResponse(result, safe=False)
        except Exception as e:
            return JsonResponse({'status': False, 'error': str(e)})

class DashboardDomainHourlyHeatmapView(View):
    def dispatch(self, request, *args, **kwargs):
        if 'hris_admin' not in request.session:
            is_ajax = False
            try:
                is_ajax = request.headers.get('X-Requested-With') == 'XMLHttpRequest'
            except Exception:
                pass
            if not is_ajax:
                is_ajax = request.META.get('HTTP_X_REQUESTED_WITH') == 'XMLHttpRequest'
            if is_ajax:
                return JsonResponse({'status': False, 'error': 'Sesi berakhir atau tidak valid. Silakan login ulang.'})
            return redirect('admin_login')
        return super().dispatch(request, *args, **kwargs)

    def get(self, req):
        try:
            tanggal = req.GET.get('tanggal', '')
            source = (req.GET.get('source') or 'adx').strip().lower()
            domains_raw = (req.GET.get('domains') or req.GET.get('selected_domains') or req.GET.get('domain') or '').strip()
            selected_domains = [str(x).strip().lower() for x in domains_raw.split(',') if str(x).strip()]
            selected_domain_set = set(selected_domains)

            def _norm_site(v):
                s = str(v or '').strip().lower()
                s = re.sub(r'^https?://', '', s)
                s = s.split('/')[0].split('?')[0].split('#')[0]
                s = re.sub(r'^www\.', '', s)
                return s

            def _site_match(site_value):
                if not selected_domain_set:
                    return True
                site = _norm_site(site_value)
                if not site:
                    return False
                for d in selected_domain_set:
                    if site == d or site.endswith('.' + d) or d in site:
                        return True
                return False
            # --- 1. Parse tanggal aman
            def parse_date(d):
                try:
                    return datetime.strptime(d, '%Y-%m-%d').strftime('%Y-%m-%d')
                except (ValueError, TypeError):
                    raise ValueError(f"Tanggal tidak valid: {d}")
            tanggal_formatted = parse_date(tanggal)
            db = data_mysql()
            adx_resp = None
            adsense_resp = None
            adx_rows = []
            adsense_rows = []
            if source == 'adsense':
                adsense_resp = db.get_all_adsense_country_hourly_by_params(tanggal_formatted)
                adsense_rows = (adsense_resp.get('data') if isinstance(adsense_resp, dict) else []) or []
                if not isinstance(adsense_rows, list):
                    adsense_rows = []
            elif source == 'all':
                adx_resp = db.get_all_adx_country_hourly_by_params(tanggal_formatted)
                adsense_resp = db.get_all_adsense_country_hourly_by_params(tanggal_formatted)
                adx_rows = (adx_resp.get('data') if isinstance(adx_resp, dict) else []) or []
                adsense_rows = (adsense_resp.get('data') if isinstance(adsense_resp, dict) else []) or []
                if not isinstance(adx_rows, list):
                    adx_rows = []
                if not isinstance(adsense_rows, list):
                    adsense_rows = []
            else:
                adx_resp = db.get_all_adx_country_hourly_by_params(tanggal_formatted)
                adx_rows = (adx_resp.get('data') if isinstance(adx_resp, dict) else []) or []
                if not isinstance(adx_rows, list):
                    adx_rows = []

            if selected_domain_set:
                adx_rows = [r for r in (adx_rows or []) if _site_match((r or {}).get('log_adx_country_domain', ''))]
                adsense_rows = [r for r in (adsense_rows or []) if _site_match((r or {}).get('log_adsense_country_domain', ''))]

            unique_name_site = []
            extracted_sites = set[Any]()
            def _collect_sites(rows, key):
                for item in rows or []:
                    site_name = str((item or {}).get(key, '')).strip()
                    if site_name and site_name != 'Unknown':
                        extracted_sites.add(site_name)
            _collect_sites(adx_rows, 'log_adx_country_domain')
            _collect_sites(adsense_rows, 'log_adsense_country_domain')
            for site in extracted_sites:
                main_domain = site
                if "." in site:
                    parts = site.split(".")
                    if len(parts) >= 2:
                        main_domain = ".".join(parts[:2])
                unique_name_site.append(main_domain)
            unique_name_site = list(set(unique_name_site))
            ads_resp = None
            if unique_name_site:
                ads_resp = db.get_all_ads_country_hourly_by_params(
                    tanggal_formatted,
                    unique_name_site
                )
            ads_rows = ((ads_resp or {}).get('hasil') or {}).get('data') or []
            if not isinstance(ads_rows, list):
                ads_rows = []
            rev_by_hour = {f"{h:02d}": 0.0 for h in range(24)}
            spend_by_hour = {f"{h:02d}": 0.0 for h in range(24)}
            def _acc_rev(rows):
                for row in rows or []:
                    try:
                        hour = int(row.get('hour', 0) or 0)
                    except Exception:
                        hour = 0
                    if hour < 0 or hour > 23:
                        continue
                    hkey = f"{hour:02d}"
                    rev_by_hour[hkey] = rev_by_hour.get(hkey, 0.0) + float(row.get('revenue', 0) or 0)
            _acc_rev(adx_rows)
            _acc_rev(adsense_rows)
            for row in ads_rows or []:
                try:
                    hour = int(row.get('hour', 0) or 0)
                except Exception:
                    hour = 0
                if hour < 0 or hour > 23:
                    continue
                hkey = f"{hour:02d}"
                spend_by_hour[hkey] = spend_by_hour.get(hkey, 0.0) + float(row.get('spend', 0) or 0)
            hours = [f"{h:02d}" for h in range(24)]
            revenue_series = []
            spend_series = []
            roi_series = []
            total_revenue = 0.0
            total_spend = 0.0
            for h in hours:
                r = float(rev_by_hour.get(h, 0.0) or 0.0)
                s = float(spend_by_hour.get(h, 0.0) or 0.0)
                total_revenue += r
                total_spend += s
                revenue_series.append(round(r, 2))
                spend_series.append(round(s, 2))
                roi_series.append(round((((r - s) / s) * 100) if s > 0 else 0.0, 2))
            result = {
                'status': True,
                'tanggal': tanggal_formatted,
                'hours': hours,
                'roi': roi_series,
                'revenue': revenue_series,
                'spend': spend_series,
                'summary': {
                    'total_revenue': round(total_revenue, 2),
                    'total_spend': round(total_spend, 2),
                    'roi_nett': round((((total_revenue - total_spend) / total_spend) * 100) if total_spend > 0 else 0.0, 2)
                }
            }
            return JsonResponse(result, safe=False)
        except Exception as e:
            return JsonResponse({'status': False, 'error': str(e)})

class DashboardPortfolioPulseView(View):
    def dispatch(self, request, *args, **kwargs):
        if 'hris_admin' not in request.session:
            is_ajax = False
            try:
                is_ajax = request.headers.get('X-Requested-With') == 'XMLHttpRequest'
            except Exception:
                pass
            if not is_ajax:
                is_ajax = request.META.get('HTTP_X_REQUESTED_WITH') == 'XMLHttpRequest'
            if is_ajax:
                return JsonResponse({'status': False, 'error': 'Sesi berakhir atau tidak valid. Silakan login ulang.'})
            return redirect('admin_login')
        return super().dispatch(request, *args, **kwargs)

    def get(self, req):
        try:
            end_date = (req.GET.get('end_date') or req.GET.get('tanggal') or '').strip()
            days_raw = req.GET.get('days', '14')
            forecast_raw = req.GET.get('forecast_days', '3')

            def parse_date(d):
                try:
                    return datetime.strptime(d, '%Y-%m-%d').date()
                except (ValueError, TypeError):
                    raise ValueError(f"Tanggal tidak valid: {d}")

            end_dt = parse_date(end_date)

            try:
                days = int(days_raw)
            except Exception:
                days = 14
            if days < 1:
                days = 1
            if days > 60:
                days = 60

            try:
                forecast_days = int(forecast_raw)
            except Exception:
                forecast_days = 3
            if forecast_days < 0:
                forecast_days = 0
            if forecast_days > 7:
                forecast_days = 7

            start_dt = end_dt - timedelta(days=days - 1)
            start_date_formatted = start_dt.strftime('%Y-%m-%d')
            end_date_formatted = end_dt.strftime('%Y-%m-%d')

            db = data_mysql()
            engine = ''
            try:
                engine = (db._report_engine() or '').lower()
            except Exception:
                engine = ''

            if engine in ('clickhouse', 'ch'):
                sql_rev = """
                    SELECT
                        toDate(data_adx_country_tanggal) AS date,
                        SUM(data_adx_country_revenue) AS revenue
                    FROM data_adx_country
                    WHERE toDate(data_adx_country_tanggal) BETWEEN toDate(%s) AND toDate(%s)
                    GROUP BY date
                    ORDER BY date ASC
                """.strip()
                sql_spend = """
                    SELECT
                        toDate(b.data_ads_country_tanggal) AS date,
                        SUM(b.data_ads_country_spend) AS spend
                    FROM data_ads_country b
                    INNER JOIN (
                        SELECT DISTINCT
                            concat(
                                arrayElement(splitByChar('.', data_adx_country_domain), 1),
                                '.',
                                arrayElement(splitByChar('.', data_adx_country_domain), 2),
                                '.com'
                            ) AS domain
                        FROM data_adx_country
                        WHERE toDate(data_adx_country_tanggal) BETWEEN toDate(%s) AND toDate(%s)
                    ) d
                        ON concat(
                            arrayElement(splitByChar('.', b.data_ads_domain), 1),
                            '.',
                            arrayElement(splitByChar('.', b.data_ads_domain), 2),
                            '.com'
                        ) = d.domain
                    WHERE toDate(b.data_ads_country_tanggal) BETWEEN toDate(%s) AND toDate(%s)
                    GROUP BY date
                    ORDER BY date ASC
                """.strip()
            else:
                sql_rev = """
                    SELECT
                        DATE(b.data_adx_country_tanggal) AS date,
                        SUM(b.data_adx_country_revenue) AS revenue
                    FROM data_adx_country b
                    WHERE b.data_adx_country_tanggal BETWEEN %s AND %s
                    GROUP BY DATE(b.data_adx_country_tanggal)
                    ORDER BY DATE(b.data_adx_country_tanggal) ASC
                """.strip()
                sql_spend = """
                    SELECT
                        DATE(b.data_ads_country_tanggal) AS date,
                        SUM(b.data_ads_country_spend) AS spend
                    FROM data_ads_country b
                    INNER JOIN (
                        SELECT DISTINCT
                            CONCAT(SUBSTRING_INDEX(a.data_adx_country_domain, '.', 2), '.com') AS domain
                        FROM data_adx_country a
                        WHERE a.data_adx_country_tanggal BETWEEN %s AND %s
                    ) d
                        ON CONCAT(SUBSTRING_INDEX(b.data_ads_domain, '.', 2), '.com') = d.domain
                    WHERE b.data_ads_country_tanggal BETWEEN %s AND %s
                    GROUP BY DATE(b.data_ads_country_tanggal)
                    ORDER BY DATE(b.data_ads_country_tanggal) ASC
                """.strip()

            if not db.execute_query(sql_rev, (start_date_formatted, end_date_formatted)):
                raise Exception('Gagal mengambil data revenue')
            rev_rows = db.fetch_all() or []
            db.commit()

            if not db.execute_query(sql_spend, (start_date_formatted, end_date_formatted, start_date_formatted, end_date_formatted)):
                raise Exception('Gagal mengambil data spend')
            spend_rows = db.fetch_all() or []
            db.commit()

            rev_by_date = {}
            for r in rev_rows:
                k = str(r.get('date') or '').strip()
                if not k:
                    continue
                rev_by_date[k] = rev_by_date.get(k, 0.0) + float(r.get('revenue', 0) or 0)

            spend_by_date = {}
            for r in spend_rows:
                k = str(r.get('date') or '').strip()
                if not k:
                    continue
                spend_by_date[k] = spend_by_date.get(k, 0.0) + float(r.get('spend', 0) or 0)

            dates = []
            revenue = []
            spend = []
            roi = []
            for i in range(days):
                d = (start_dt + timedelta(days=i)).strftime('%Y-%m-%d')
                dates.append(d)
                r = float(rev_by_date.get(d, 0.0) or 0.0)
                s = float(spend_by_date.get(d, 0.0) or 0.0)
                revenue.append(round(r, 2))
                spend.append(round(s, 2))
                roi.append(round((((r - s) / s) * 100) if s > 0 else 0.0, 2))

            def avg_last(vals, k=3):
                xs = [float(v or 0.0) for v in (vals or []) if v is not None]
                if not xs:
                    return 0.0
                xs = xs[-k:] if len(xs) > k else xs
                return sum(xs) / float(len(xs))

            spend_nonzero = [float(v) for v in spend if (v is not None and float(v) > 0)]
            spend_base = avg_last(spend_nonzero if spend_nonzero else spend, 3)

            roi_nonzero = []
            for i in range(len(roi)):
                try:
                    if float(spend[i] or 0) > 0:
                        roi_nonzero.append(float(roi[i] or 0.0))
                except Exception:
                    pass
            roi_base = avg_last(roi_nonzero if roi_nonzero else roi, 3)

            forecast_spend = [round(float(spend_base or 0.0), 2) for _ in range(forecast_days)]
            forecast_roi = [round(float(roi_base or 0.0), 2) for _ in range(forecast_days)]

            result = {
                'status': True,
                'start_date': start_date_formatted,
                'end_date': end_date_formatted,
                'days': days,
                'forecast_days': forecast_days,
                'dates': dates,
                'revenue': revenue,
                'spend': spend,
                'roi': roi,
                'forecast': {
                    'spend': forecast_spend,
                    'roi': forecast_roi
                }
            }

            return JsonResponse(result, safe=False)
        except Exception as e:
            return JsonResponse({'status': False, 'error': str(e)})

class RoiMonitoringDomainHourlyHeatmapView(View):
    def dispatch(self, request, *args, **kwargs):
        if 'hris_admin' not in request.session:
            is_ajax = False
            try:
                is_ajax = request.headers.get('X-Requested-With') == 'XMLHttpRequest'
            except Exception:
                pass
            if not is_ajax:
                is_ajax = request.META.get('HTTP_X_REQUESTED_WITH') == 'XMLHttpRequest'
            if is_ajax:
                return JsonResponse({'status': False, 'error': 'Sesi berakhir atau tidak valid. Silakan login ulang.'})
            return redirect('admin_login')
        return super().dispatch(request, *args, **kwargs)

    def get(self, req):
        try:
            start_date = req.GET.get('start_date', '')
            end_date = req.GET.get('end_date', '')
            selected_domains = req.GET.get('selected_domains')
            # --- 1. Parse tanggal aman
            def parse_date(d):
                try:
                    return datetime.strptime(d, '%Y-%m-%d').strftime('%Y-%m-%d')
                except (ValueError, TypeError):
                    raise ValueError(f"Tanggal tidak valid: {d}")
            start_date_formatted = parse_date(start_date)
            end_date_formatted = parse_date(end_date)
            # --- 2. Normalisasi selected_sites_list
            selected_domain_list = []
            if selected_domains:
                selected_domain_list = [str(s).strip() for s in selected_domains.split(',') if s.strip()]
            cache_key = generate_cache_key(
                'roi_monitoring_domain_hourly_heatmap',
                start_date_formatted,
                end_date_formatted,
                ','.join(selected_domain_list) if selected_domain_list else '',
            )
            cached = get_cached_data(cache_key)
            if cached is not None:
                return JsonResponse(cached, safe=False)

            db = data_mysql()
            adx_resp = db.get_all_adx_roi_country_hourly_by_params(
                start_date_formatted,
                end_date_formatted,
                selected_domain_list,
            )
            adx_rows = adx_resp.get('data') if isinstance(adx_resp, dict) else []
            # --- 4. Proses Facebook data
            facebook_data = None
            unique_name_site = []
            if selected_domain_list:
                for site in selected_domain_list:
                    site = str(site).strip()
                    main_domain = site
                    if "." in site:
                        parts = site.split(".")       # pisah berdasarkan titik
                        if len(parts) >= 2:
                            main_domain = ".".join(parts[:2])
                    unique_name_site.append(main_domain)
            elif adx_resp:
                # Ambil unique site dari AdX
                extracted_sites = set[Any]()
                for adx_item in adx_resp['data']:
                    site_name = str(adx_item.get('log_adx_country_domain', '')).strip()
                    if site_name and site_name != 'Unknown':
                        extracted_sites.add(site_name)
                for site in extracted_sites:
                    main_domain = site
                    if "." in site:
                        parts = site.split(".")       # pisah berdasarkan titik
                        if len(parts) >= 2:
                            main_domain = ".".join(parts[:2])
                    unique_name_site.append(main_domain)
            unique_name_site = list(set(unique_name_site))
            ads_resp = None
            if unique_name_site:
                ads_resp = db.get_all_ads_roi_country_hourly_by_params(
                    start_date_formatted,
                    end_date_formatted,
                    unique_name_site
                )
            ads_rows = ((ads_resp or {}).get('hasil') or {}).get('data') or []
            if not isinstance(ads_rows, list):
                ads_rows = []
            rev_by_hour = {f"{h:02d}": 0.0 for h in range(24)}
            spend_by_hour = {f"{h:02d}": 0.0 for h in range(24)}
            for row in adx_rows or []:
                try:
                    hour = int(row.get('hour', 0) or 0)
                except Exception:
                    hour = 0
                if hour < 0 or hour > 23:
                    continue
                hkey = f"{hour:02d}"
                rev_by_hour[hkey] = rev_by_hour.get(hkey, 0.0) + float(row.get('revenue', 0) or 0)
            for row in ads_rows or []:
                try:
                    hour = int(row.get('hour', 0) or 0)
                except Exception:
                    hour = 0
                if hour < 0 or hour > 23:
                    continue
                hkey = f"{hour:02d}"
                spend_by_hour[hkey] = spend_by_hour.get(hkey, 0.0) + float(row.get('spend', 0) or 0)
            hours = [f"{h:02d}" for h in range(24)]
            revenue_series = []
            spend_series = []
            roi_series = []
            total_revenue = 0.0
            total_spend = 0.0
            for h in hours:
                r = float(rev_by_hour.get(h, 0.0) or 0.0)
                s = float(spend_by_hour.get(h, 0.0) or 0.0)
                total_revenue += r
                total_spend += s
                revenue_series.append(round(r, 2))
                spend_series.append(round(s, 2))
                roi_series.append(round((((r - s) / s) * 100) if s > 0 else 0.0, 2))
            result = {
                'status': True,
                'start_date': start_date_formatted,
                'end_date': end_date_formatted,
                'hours': hours,
                'roi': roi_series,
                'revenue': revenue_series,
                'spend': spend_series,
                'summary': {
                    'total_revenue': round(total_revenue, 2),
                    'total_spend': round(total_spend, 2),
                    'roi_nett': round((((total_revenue - total_spend) / total_spend) * 100) if total_spend > 0 else 0.0, 2)
                }
            }
            set_cached_data(cache_key, result, timeout=300)
            return JsonResponse(result, safe=False)
        except Exception as e:
            return JsonResponse({'status': False, 'error': str(e)})

class RoiHourlyAdxFilterView(View):
    def dispatch(self, request, *args, **kwargs):
        if 'hris_admin' not in request.session:
            return redirect('admin_login')
        return super().dispatch(request, *args, **kwargs)
    def get(self, req):
        try:
            admin = req.session.get('hris_admin', {})
            user_id = admin.get('user_id')
            super_st = admin.get('super_st')
            try:
                cache_key = generate_cache_key('roi_hourly_adx_filter', str(user_id or ''), str(super_st or ''))
                cached = get_cached_data(cache_key)
                if cached is not None:
                    return JsonResponse(cached, safe=False)
            except Exception:
                pass
            if super_st == '0' and user_id:
                rs = data_mysql().get_all_app_credentials_user(user_id)
            else:
                rs = data_mysql().get_all_app_credentials()
            data_list = []
            if isinstance(rs, dict) and rs.get('status'):
                for row in rs.get('data') or []:
                    data_list.append({
                        'user_mail': row.get('user_mail') or '',
                        'account_name': row.get('account_name') or (row.get('user_mail') or '')
                    })
            try:
                set_cached_data(cache_key, data_list, timeout=6 * 60 * 60)
            except Exception:
                pass
            return JsonResponse(data_list, safe=False)
        except Exception as e:
            return JsonResponse({'status': False, 'error': str(e)})

class RoiHourlyDomainFilterView(View):
    def dispatch(self, request, *args, **kwargs):
        if 'hris_admin' not in request.session:
            return redirect('admin_login')
        return super().dispatch(request, *args, **kwargs)
    def get(self, req):
        try:
            db = data_mysql()
            sql = """
                SELECT DISTINCT log_adx_country_domain AS domain
                FROM log_adx_country
                WHERE log_adx_country_domain IS NOT NULL AND log_adx_country_domain <> ''
                ORDER BY log_adx_country_domain ASC
            """
            if not db.execute_query(sql):
                return JsonResponse([], safe=False)
            rows = db.fetch_all() or []
            # Return as simple list of {domain}
            return JsonResponse(rows, safe=False)
        except Exception as e:
            return JsonResponse([], safe=False)

class RoiSummaryView(View):
    """View untuk ROI Summary - menampilkan ringkasan data ROI"""
    def dispatch(self, request, *args, **kwargs):
        if 'hris_admin' not in request.session:
            return redirect('admin_login')
        return super().dispatch(request, *args, **kwargs)
    def get(self, req):
        admin = req.session.get('hris_admin', {})
        data_account = data_mysql().master_account_ads()['data']
        last_update = data_mysql().get_last_update_adx_traffic_per_domain()['data']['last_update']
        if admin.get('super_st') == '0':
            data_account_adx = data_mysql().get_all_adx_account_data_user(admin.get('user_id'))
            data_domain_adx = data_mysql().get_all_adx_domain_data_user(admin.get('user_id'))
        else:
            data_account_adx = data_mysql().get_all_adx_account_data()
            data_domain_adx = data_mysql().get_all_adx_domain_data()
        if not data_domain_adx['status']:
            return JsonResponse({
                'status': False,
                'error': data_domain_adx['data']
            })
        data = {
            'title': 'ROI Summary Dashboard',
            'user': req.session['hris_admin'],
            'data_account': data_account,
            'data_account_adx': data_account_adx['data'],
            'data_domain_adx': data_domain_adx['data'],
            'last_update': last_update
        }
        return render(req, 'admin/report_roi/all_rekap/index.html', data)

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
class AssignAccountUserView(View):
    def post(self, request):
        try:
            admin = request.session.get('hris_admin', {})
            account_id = request.POST.get('account_id')
            user_akun = request.POST.getlist('user_akun[]') # FIXED
            if not user_akun:
                return JsonResponse({
                    'status': False,
                    'message': 'Tidak ada user dipilih'
                })
            db = data_mysql()
            for user in user_akun:
                params = {
                    'account_id': account_id,
                    'user_id': user,
                    'mdb': admin.get('user_id'),
                    'mdb_name': admin.get('user_alias') or admin.get('user_name')
                }
                result = db.assign_account_user(params)
                if not result['status']:
                    return JsonResponse({
                        'status': False,
                        'message': result.get('message', 'Gagal assign account user')
                    }, status=500)
            return JsonResponse({
                'status': True,
                'message': 'Account user berhasil diassign'
            })
        except Exception as e:
            return JsonResponse({
                'status': False,
                'message': f'Error: {str(e)}'
            }, status=500)

@method_decorator(csrf_exempt, name='dispatch')
class UpdateAccountNameView(View):
    def post(self, request):
        try:
            user_mail = request.POST.get('user_mail')
            new_account_name = request.POST.get('account_name')
            new_mcm_revenue_share = request.POST.get('mcm_revenue_share')

            if not user_mail or not new_account_name or not new_mcm_revenue_share:
                return JsonResponse({
                    'status': False,
                    'message': 'User mail, account name, dan MCM Revenue Share harus diisi'
                }, status=400)
            # Update account name in database
            db = data_mysql()
            result = db.update_account_name(user_mail, new_account_name, new_mcm_revenue_share)
            
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


@method_decorator(csrf_exempt, name='dispatch')
class DeleteAdxAccountCredentialsView(View):
    def post(self, request):
        try:
            admin = request.session.get('hris_admin')
            if not admin:
                return JsonResponse({'status': False, 'message': 'Unauthorized'}, status=401)

            is_superadmin = (admin.get('super_st') == '1') or (admin.get('user_name') == 'superadmin')
            if not is_superadmin:
                return JsonResponse({'status': False, 'message': 'Unauthorized'}, status=403)

            user_mail = (request.POST.get('user_mail') or '').strip()
            if not user_mail:
                return JsonResponse({'status': False, 'message': 'user_mail harus diisi'}, status=400)

            db = data_mysql()
            result = db.delete_adx_account_credentials(user_mail, admin.get('user_id'), admin.get('user_alias') or admin.get('user_name'))
            if result.get('status'):
                return JsonResponse({'status': True, 'message': result.get('message', 'Kredensial berhasil dihapus')})

            return JsonResponse({'status': False, 'message': result.get('message', 'Gagal menghapus kredensial')}, status=500)

        except Exception as e:
            return JsonResponse({'status': False, 'message': f'Error: {str(e)}'}, status=500)

# ===== ROI Monitoring Domain =====

class RoiMonitoringDomainView(View):
    """View untuk ROI Summary - menampilkan ringkasan data ROI"""
    def dispatch(self, request, *args, **kwargs):
        if 'hris_admin' not in request.session:
            return redirect('admin_login')
        return super().dispatch(request, *args, **kwargs)
    def get(self, req):
        admin = req.session.get('hris_admin', {})
        data_account = data_mysql().master_account_ads()['data']
        last_update = data_mysql().get_last_update_adx_traffic_per_domain()['data']['last_update']
        if admin.get('super_st') == '0':
            data_account_adx = data_mysql().get_all_adx_account_data_user(admin.get('user_id'))
            data_domain_adx = data_mysql().get_all_adx_domain_data_user(admin.get('user_id'))
        else:
            data_account_adx = data_mysql().get_all_adx_account_data()
            data_domain_adx = data_mysql().get_all_adx_domain_data()
        if not data_domain_adx['status']:
            return JsonResponse({
                'status': False,
                'error': data_domain_adx['data']
            })
        data = {
            'title': 'ROI Summary Dashboard',
            'user': req.session['hris_admin'],
            'data_account': data_account,
            'data_account_adx': data_account_adx['data'],
            'data_domain_adx': data_domain_adx['data'],
            'last_update': last_update
        }
        return render(req, 'admin/report_roi/monitoring_domain/index.html', data)

class RoiMonitoringDomainDataView(View):
    """AJAX endpoint untuk data ROI Traffic Per Domain"""
    def dispatch(self, request, *args, **kwargs):
        if 'hris_admin' not in request.session:
            return redirect('admin_login')
        return super().dispatch(request, *args, **kwargs)
    def get(self, req):
        try:
            start_date = req.GET.get('start_date')
            end_date = req.GET.get('end_date')
            selected_accounts = req.GET.get('selected_account_adx')
            admin = req.session.get('hris_admin', {})
            if selected_accounts == '':
                rs_account = data_mysql().get_all_adx_account_data_user(admin.get('user_id'))
                account_ids = [str(item['account_id']) for item in rs_account.get('data', [])]
                selected_accounts = ",".join(account_ids)
            else:
                selected_accounts = req.GET.get('selected_account_adx', '')
            selected_domains = req.GET.get('selected_domains')
            # --- 1. Parse tanggal aman
            def parse_date(d):
                try:
                    return datetime.strptime(d, '%Y-%m-%d').strftime('%Y-%m-%d')
                except (ValueError, TypeError):
                    raise ValueError(f"Tanggal tidak valid: {d}")
            start_date_formatted = parse_date(start_date)
            end_date_formatted = parse_date(end_date)
            # --- 2. Normalisasi selected_sites_list
            selected_account_list = []
            if selected_accounts:
                selected_account_list = [str(s).strip() for s in selected_accounts.split(',') if s.strip()]
            selected_domain_list = []
            if selected_domains:
                selected_domain_list = [str(s).strip() for s in selected_domains.split(',') if s.strip()]

            response_cache_key = generate_cache_key(
                'roi_domain_response_v2',
                admin.get('user_id') or '',
                admin.get('super_st') or '',
                start_date_formatted,
                end_date_formatted,
                ','.join(selected_account_list) if selected_account_list else '',
                ','.join(selected_domain_list) if selected_domain_list else '',
            )
            def _attach_active_days(payload):
                try:
                    rows = list((payload or {}).get('data') or [])
                    if not rows:
                        return payload
                    sites_for_age = [str((x or {}).get('site_name') or '').strip() for x in rows]
                    age_result = data_mysql().get_fact_join_hourly_active_days_map(end_date_formatted, sites_for_age)
                    active_days_by_site = (age_result or {}).get('data') if isinstance(age_result, dict) and age_result.get('status') else {}
                    for item in rows:
                        site_key = str((item or {}).get('site_name') or '').strip().lower()
                        item['active_days'] = int((active_days_by_site or {}).get(site_key, 0) or 0)
                    rows_filtered = list((payload or {}).get('data_filtered') or [])
                    for item in rows_filtered:
                        site_key = str((item or {}).get('site_name') or '').strip().lower()
                        item['active_days'] = int((active_days_by_site or {}).get(site_key, 0) or 0)
                    payload['data'] = rows
                    payload['data_filtered'] = rows_filtered
                except Exception:
                    pass
                return payload
            cached_response = get_cached_data(response_cache_key)
            if cached_response is not None:
                return JsonResponse(_attach_active_days(cached_response), safe=False)

            # --- 3. Ambil data AdX
            adx_result = data_mysql().get_all_adx_monitoring_account_by_params(
                start_date_formatted,
                end_date_formatted,
                selected_account_list,
                selected_domain_list
            )

            last_update = ''
            try:
                lu = data_mysql().get_last_update_adx_monitoring_by_params(
                    start_date_formatted,
                    end_date_formatted,
                    selected_account_list
                )
                last_update = ((lu or {}).get('data') or {}).get('last_update') or ''
            except Exception:
                last_update = ''

            last_update_by_site = {}
            try:
                lus = data_mysql().get_last_update_adx_monitoring_by_domain_params(
                    start_date_formatted,
                    end_date_formatted,
                    selected_account_list,
                    selected_domain_list
                )
                for x in ((lus or {}).get('data') or []):
                    k = str((x or {}).get('site_name') or '').strip()
                    if k:
                        last_update_by_site[k] = (x or {}).get('last_update') or ''
            except Exception:
                last_update_by_site = {}

            # --- 4. Proses Facebook data
            facebook_data = None
            unique_name_site = []
            if selected_domain_list:
                for site in selected_domain_list:
                    site = str(site).strip()
                    main_domain = site
                    if "." in site:
                        parts = site.split(".")       # pisah berdasarkan titik
                        if len(parts) >= 2:
                            main_domain = ".".join(parts[:2])
                    unique_name_site.append(main_domain)
            elif adx_result:
                # Ambil unique site dari AdX
                extracted_sites = set[Any]()
                for adx_item in adx_result['hasil']['data']:
                    site_name = str(adx_item.get('site_name', '')).strip()
                    if site_name and site_name != 'Unknown':
                        extracted_sites.add(site_name)
                for site in extracted_sites:
                    main_domain = site
                    if "." in site:
                        parts = site.split(".")       # pisah berdasarkan titik
                        if len(parts) >= 2:
                            main_domain = ".".join(parts[:2])
                    unique_name_site.append(main_domain)
            unique_name_site = list(set(unique_name_site))
            if unique_name_site:
                facebook_data = data_mysql().get_all_ads_roi_monitoring_campaign_by_params(
                    start_date_formatted,
                    end_date_formatted,
                    unique_name_site
                )
            # --- 5. Gabungkan data AdX dan Facebook
            # Siapkan struktur data
            raw_rows_map = {}
            raw_rows_all = []
            combined_data_all = []
            combined_data_filtered = []
            total_spend = 0
            total_revenue = 0

            def normalize_country_code(cc):
                c = (str(cc or '').strip().upper())
                if not c:
                    return ''
                if c == 'TU':
                    return 'TR'
                return c

            facebook_map = {}
            if facebook_data and facebook_data['hasil']['data']:
                for fb_item in facebook_data['hasil']['data']:
                    date_key = str(fb_item.get('date', ''))
                    subdomain = str(fb_item.get('domain', ''))
                    country_code = normalize_country_code(fb_item.get('country_code', ''))
                    key = f"{date_key}_{extract_base_subdomain(subdomain)}_{country_code}"
                    facebook_map[key] = fb_item
            if adx_result and adx_result['hasil']['data']:
                # --- NEW: siapkan raw_rows + dua grup agregasi
                grouped_all = {}
                grouped_filtered = {}
                seen_fb_keys = set()
                for adx_item in adx_result['hasil']['data']:
                    date_key = str(adx_item.get('date', ''))
                    subdomain = str(adx_item.get('site_name', ''))
                    base_subdomain = extract_base_subdomain(subdomain)
                    site_key = base_subdomain or subdomain
                    country_code = normalize_country_code(adx_item.get('country_code', ''))
                    key = f"{date_key}_{base_subdomain}_{country_code}"
                    seen_fb_keys.add(key)
                    fb_data = facebook_map.get(key)
                    account_ads = str((fb_data or {}).get('account_name', ''))
                    spend = float((fb_data or {}).get('spend', 0))
                    revenue = float(adx_item.get('revenue', 0) or 0)
                    impressions = int(adx_item.get('impressions') or 0)
                    clicks = int(adx_item.get('clicks') or 0)
                    total_requests = int(adx_item.get('total_requests') or 0)
                    responses_served = int(adx_item.get('responses_served') or 0)
                    active_view_pct_viewable = float(adx_item.get('active_view_pct_viewable') or 0.0)
                    active_view_avg_time_sec = float(adx_item.get('active_view_avg_time_sec') or 0.0)

                    row_key = f"{date_key}_{site_key}"
                    cur_row = raw_rows_map.get(row_key)
                    if not cur_row:
                        cur_row = {
                            'site_name': site_key,
                            'date': date_key,
                            'spend': 0.0,
                            'revenue': 0.0,
                            'impressions_fb': 0.0,
                            'clicks_fb': 0.0,
                            'impressions_adx': 0.0,
                            'clicks_adx': 0.0,
                            'total_requests': 0,
                            'responses_served': 0,
                            'active_view_weight': 0,
                            'active_view_pct_viewable_sum': 0.0,
                            'active_view_avg_time_sec_sum': 0.0,
                        }
                        raw_rows_map[row_key] = cur_row

                    cur_row['spend'] = float(cur_row.get('spend', 0) or 0) + spend
                    cur_row['revenue'] = float(cur_row.get('revenue', 0) or 0) + revenue
                    cur_row['impressions_fb'] = float(cur_row.get('impressions_fb', 0) or 0) + float((fb_data or {}).get('impressions') or 0)
                    cur_row['clicks_fb'] = float(cur_row.get('clicks_fb', 0) or 0) + float((fb_data or {}).get('clicks') or 0)

                    cur_row['impressions_adx'] = float(cur_row.get('impressions_adx', 0) or 0) + float(impressions)
                    cur_row['clicks_adx'] = float(cur_row.get('clicks_adx', 0) or 0) + float(clicks)
                    cur_row['total_requests'] = int(cur_row.get('total_requests', 0) or 0) + total_requests
                    cur_row['responses_served'] = int(cur_row.get('responses_served', 0) or 0) + responses_served
                    if impressions > 0:
                        cur_row['active_view_weight'] = int(cur_row.get('active_view_weight', 0) or 0) + impressions
                        cur_row['active_view_pct_viewable_sum'] = float(cur_row.get('active_view_pct_viewable_sum', 0) or 0) + (active_view_pct_viewable * impressions)
                        cur_row['active_view_avg_time_sec_sum'] = float(cur_row.get('active_view_avg_time_sec_sum', 0) or 0) + (active_view_avg_time_sec * impressions)

                    if site_key not in grouped_all:
                        grouped_all[site_key] = {
                            'site_name': site_key,
                            'account_ads': account_ads,
                            'spend': 0.0,
                            'revenue': 0.0,
                            'impressions': 0,
                            'clicks': 0,
                            'total_requests': 0,
                            'responses_served': 0,
                            'active_view_weight': 0,
                            'active_view_pct_viewable_sum': 0.0,
                            'active_view_avg_time_sec_sum': 0.0,
                        }
                    grouped_all[site_key]['account_ads'] = account_ads
                    grouped_all[site_key]['spend'] += spend
                    grouped_all[site_key]['revenue'] += revenue
                    grouped_all[site_key]['impressions'] += impressions
                    grouped_all[site_key]['clicks'] += clicks
                    grouped_all[site_key]['total_requests'] += total_requests
                    grouped_all[site_key]['responses_served'] += responses_served
                    if impressions > 0:
                        grouped_all[site_key]['active_view_weight'] += impressions
                        grouped_all[site_key]['active_view_pct_viewable_sum'] += (active_view_pct_viewable * impressions)
                        grouped_all[site_key]['active_view_avg_time_sec_sum'] += (active_view_avg_time_sec * impressions)

                    if spend > 0:
                        if site_key not in grouped_filtered:
                            grouped_filtered[site_key] = {
                                'site_name': site_key,
                                'account_ads': account_ads,
                                'spend': 0.0,
                                'revenue': 0.0,
                                'impressions': 0,
                                'clicks': 0,
                                'total_requests': 0,
                                'responses_served': 0,
                                'active_view_weight': 0,
                                'active_view_pct_viewable_sum': 0.0,
                                'active_view_avg_time_sec_sum': 0.0,
                            }
                        grouped_filtered[site_key]['account_ads'] = account_ads
                        grouped_filtered[site_key]['spend'] += spend
                        grouped_filtered[site_key]['revenue'] += revenue
                        grouped_filtered[site_key]['impressions'] += impressions
                        grouped_filtered[site_key]['clicks'] += clicks
                        grouped_filtered[site_key]['total_requests'] += total_requests
                        grouped_filtered[site_key]['responses_served'] += responses_served
                        if impressions > 0:
                            grouped_filtered[site_key]['active_view_weight'] += impressions
                            grouped_filtered[site_key]['active_view_pct_viewable_sum'] += (active_view_pct_viewable * impressions)
                            grouped_filtered[site_key]['active_view_avg_time_sec_sum'] += (active_view_avg_time_sec * impressions)

                # Tambahkan baris FB yang tidak punya pasangan AdX (supaya total spend konsisten)
                for fb_key, fb_item in (facebook_map or {}).items():
                    if fb_key in seen_fb_keys:
                        continue
                    subdomain = str(fb_item.get('domain', ''))
                    base_subdomain = extract_base_subdomain(subdomain)
                    site_key = base_subdomain or subdomain
                    account_ads = str(fb_item.get('account_name', ''))
                    spend = float(fb_item.get('spend', 0) or 0)

                    date_key = str(fb_item.get('date', ''))
                    row_key = f"{date_key}_{site_key}"
                    cur_row = raw_rows_map.get(row_key)
                    if not cur_row:
                        cur_row = {
                            'site_name': site_key,
                            'date': date_key,
                            'spend': 0.0,
                            'revenue': 0.0,
                            'impressions_fb': 0.0,
                            'clicks_fb': 0.0,
                            'impressions_adx': 0.0,
                            'clicks_adx': 0.0,
                        }
                        raw_rows_map[row_key] = cur_row
                    cur_row['spend'] = float(cur_row.get('spend', 0) or 0) + spend
                    cur_row['impressions_fb'] = float(cur_row.get('impressions_fb', 0) or 0) + float(fb_item.get('impressions') or 0)
                    cur_row['clicks_fb'] = float(cur_row.get('clicks_fb', 0) or 0) + float(fb_item.get('clicks') or 0)

                    if site_key not in grouped_all:
                        grouped_all[site_key] = {'site_name': site_key, 'account_ads': account_ads, 'spend': 0.0, 'revenue': 0.0}
                    grouped_all[site_key]['account_ads'] = account_ads
                    grouped_all[site_key]['spend'] += spend

                    if spend > 0:
                        if site_key not in grouped_filtered:
                            grouped_filtered[site_key] = {'site_name': site_key, 'account_ads': account_ads, 'spend': 0.0, 'revenue': 0.0}
                        grouped_filtered[site_key]['account_ads'] = account_ads
                        grouped_filtered[site_key]['spend'] += spend

                # Bentuk output agregasi + ROI
                combined_data_all = []
                total_spend = 0
                total_revenue = 0
                for item in grouped_all.values():
                    spend_val = item['spend']
                    revenue_val = item['revenue']
                    roi = ((revenue_val - spend_val) / spend_val * 100) if spend_val > 0 else 0
                    impressions = int(item.get('impressions', 0) or 0)
                    clicks = int(item.get('clicks', 0) or 0)
                    total_requests = int(item.get('total_requests', 0) or 0)
                    responses_served = int(item.get('responses_served', 0) or 0)
                    match_rate = (float(responses_served) / float(total_requests) * 100.0) if total_requests > 0 else 0.0
                    fill_rate = (float(impressions) / float(responses_served) * 100.0) if responses_served > 0 else 0.0
                    cpc = (float(revenue_val) / float(clicks)) if clicks > 0 else 0.0
                    ecpm = ((float(revenue_val) / float(impressions)) * 1000.0) if impressions > 0 else 0.0
                    w = int(item.get('active_view_weight', 0) or 0)
                    active_view_pct_viewable = (float(item.get('active_view_pct_viewable_sum', 0.0) or 0.0) / float(w)) if w > 0 else 0.0
                    active_view_avg_time_sec = (float(item.get('active_view_avg_time_sec_sum', 0.0) or 0.0) / float(w)) if w > 0 else 0.0

                    combined_data_all.append({
                        'site_name': item['site_name'],
                        'account_ads': item['account_ads'],
                        'spend': spend_val,
                        'revenue': revenue_val,
                        'roi': roi,
                        'impressions': impressions,
                        'clicks': clicks,
                        'ecpm': round(ecpm, 2),
                        'cpc': round(cpc, 2),
                        'total_requests': total_requests,
                        'responses_served': responses_served,
                        'match_rate': round(match_rate, 2),
                        'fill_rate': round(fill_rate, 2),
                        'active_view_pct_viewable': round(active_view_pct_viewable, 2),
                        'active_view_avg_time_sec': round(active_view_avg_time_sec, 2),
                        'last_update': last_update_by_site.get(item['site_name'], '')
                    })
                    total_spend += spend_val
                    total_revenue += revenue_val

                combined_data_filtered = []
                for item in grouped_filtered.values():
                    spend_val = item['spend']
                    revenue_val = item['revenue']
                    roi = ((revenue_val - spend_val) / spend_val * 100) if spend_val > 0 else 0
                    impressions = int(item.get('impressions', 0) or 0)
                    clicks = int(item.get('clicks', 0) or 0)
                    total_requests = int(item.get('total_requests', 0) or 0)
                    responses_served = int(item.get('responses_served', 0) or 0)
                    match_rate = (float(responses_served) / float(total_requests) * 100.0) if total_requests > 0 else 0.0
                    fill_rate = (float(impressions) / float(responses_served) * 100.0) if responses_served > 0 else 0.0
                    cpc = (float(revenue_val) / float(clicks)) if clicks > 0 else 0.0
                    ecpm = ((float(revenue_val) / float(impressions)) * 1000.0) if impressions > 0 else 0.0
                    w = int(item.get('active_view_weight', 0) or 0)
                    active_view_pct_viewable = (float(item.get('active_view_pct_viewable_sum', 0.0) or 0.0) / float(w)) if w > 0 else 0.0
                    active_view_avg_time_sec = (float(item.get('active_view_avg_time_sec_sum', 0.0) or 0.0) / float(w)) if w > 0 else 0.0

                    combined_data_filtered.append({
                        'site_name': item['site_name'],
                        'account_ads': item['account_ads'],
                        'spend': spend_val,
                        'revenue': revenue_val,
                        'roi': roi,
                        'impressions': impressions,
                        'clicks': clicks,
                        'ecpm': round(ecpm, 2),
                        'cpc': round(cpc, 2),
                        'total_requests': total_requests,
                        'responses_served': responses_served,
                        'match_rate': round(match_rate, 2),
                        'fill_rate': round(fill_rate, 2),
                        'active_view_pct_viewable': round(active_view_pct_viewable, 2),
                        'active_view_avg_time_sec': round(active_view_avg_time_sec, 2),
                        'last_update': last_update_by_site.get(item['site_name'], '')
                    })
            roi_nett_summary = ((total_revenue - total_spend) / total_spend * 100) if total_spend > 0 else 0

            raw_rows_all = list((raw_rows_map or {}).values())
            try:
                raw_rows_all.sort(key=lambda x: (str((x or {}).get('date') or ''), str((x or {}).get('site_name') or '')))
            except Exception:
                pass

            active_days_by_site = {}
            try:
                sites_for_age = [str((x or {}).get('site_name') or '').strip() for x in (combined_data_all or [])]
                age_result = data_mysql().get_fact_join_hourly_active_days_map(end_date_formatted, sites_for_age)
                if isinstance(age_result, dict) and age_result.get('status'):
                    active_days_by_site = (age_result.get('data') or {})
            except Exception:
                active_days_by_site = {}

            for item in (combined_data_all or []):
                site_key = str((item or {}).get('site_name') or '').strip().lower()
                item['active_days'] = int(active_days_by_site.get(site_key, 0) or 0)
            for item in (combined_data_filtered or []):
                site_key = str((item or {}).get('site_name') or '').strip().lower()
                item['active_days'] = int(active_days_by_site.get(site_key, 0) or 0)

            def _clamp(v, lo, hi):
                try:
                    x = float(v)
                except Exception:
                    x = 0.0
                if x < lo:
                    return lo
                if x > hi:
                    return hi
                return x

            def _stdev(values):
                xs = []
                for v in (values or []):
                    try:
                        xs.append(float(v))
                    except Exception:
                        xs.append(0.0)
                if len(xs) < 2:
                    return 0.0
                mean = sum(xs) / float(len(xs))
                var = sum((x - mean) ** 2 for x in xs) / float(len(xs) - 1)
                return math.sqrt(var)

            def _decide_action(m):
                roi = float(m.get('roi') or 0.0)
                spend = float(m.get('spend') or 0.0)
                clicks_fb = float(m.get('clicks_fb') or 0.0)
                clicks_adx = float(m.get('clicks_adx') or 0.0)
                impressions_fb = float(m.get('impressions_fb') or 0.0)
                impressions_adx = float(m.get('impressions_adx') or 0.0)
                stability_idx = float(m.get('stability_idx') or 0.0)
                last3_avg_roi = float(m.get('last3_avg_roi') or 0.0)
                days = int(m.get('days') or 0)

                ctr_fb = (clicks_fb / impressions_fb * 100.0) if impressions_fb > 0 else 0.0
                ctr_adx = (clicks_adx / impressions_adx * 100.0) if impressions_adx > 0 else 0.0
                cpc_fb = (spend / clicks_fb) if clicks_fb > 0 else 0.0
                cpc_adx = (float(m.get('revenue') or 0.0) / clicks_adx) if clicks_adx > 0 else 0.0
                cpm_fb = (spend / impressions_fb * 1000.0) if impressions_fb > 0 else 0.0
                ecpm_adx = (float(m.get('revenue') or 0.0) / impressions_adx * 1000.0) if impressions_adx > 0 else 0.0
                conv_rate = (clicks_adx / clicks_fb * 100.0) if clicks_fb > 0 else 0.0

                low_signal = (spend < 25000.0) or ((clicks_fb + clicks_adx) < 30.0) or (days < 2)

                reasons = []
                action = 'HOLD'

                if low_signal:
                    action = 'HOLD'
                    reasons.append('Data belum cukup kuat untuk keputusan agresif')
                else:
                    if spend >= 100000.0 and (roi <= -30.0 or last3_avg_roi <= -20.0):
                        action = 'STOP'
                        reasons.append('ROI negatif berat pada spend signifikan')
                    elif roi < 0.0:
                        action = 'CUT'
                        reasons.append('ROI negatif')
                    elif roi >= 50.0 and stability_idx >= 60.0 and conv_rate >= 2.0:
                        action = 'SCALE_UP'
                        reasons.append('ROI tinggi dan stabil')
                    else:
                        action = 'HOLD'
                        reasons.append('ROI belum memenuhi syarat scale atau cut')

                roi_score = _clamp(((roi + 50.0) / 150.0) * 100.0, 0.0, 100.0)
                conv_score = _clamp((conv_rate / 10.0) * 100.0, 0.0, 100.0)
                score = _clamp((roi_score * 0.55) + (stability_idx * 0.25) + (conv_score * 0.20), 0.0, 100.0)

                return {
                    'action': action,
                    'score': round(score, 0),
                    'reasons': reasons[:3],
                    'ctr_fb': ctr_fb,
                    'ctr_adx': ctr_adx,
                    'cpc_fb': cpc_fb,
                    'cpc_adx': cpc_adx,
                    'cpm_fb': cpm_fb,
                    'ecpm_adx': ecpm_adx,
                    'conv_rate': conv_rate,
                }

            by_site = defaultdict(list)
            for r in (raw_rows_all or []):
                site_key = str((r or {}).get('site_name') or '').strip()
                if not site_key:
                    continue
                by_site[site_key].append(r)

            ml = {}
            for site_key, rows in (by_site or {}).items():
                try:
                    rows_sorted = sorted(rows, key=lambda x: str((x or {}).get('date') or ''))
                except Exception:
                    rows_sorted = list(rows)

                spend_total = sum(float((x or {}).get('spend') or 0.0) for x in rows_sorted)
                revenue_total = sum(float((x or {}).get('revenue') or 0.0) for x in rows_sorted)
                impressions_fb_total = sum(float((x or {}).get('impressions_fb') or 0.0) for x in rows_sorted)
                clicks_fb_total = sum(float((x or {}).get('clicks_fb') or 0.0) for x in rows_sorted)
                impressions_adx_total = sum(float((x or {}).get('impressions_adx') or 0.0) for x in rows_sorted)
                clicks_adx_total = sum(float((x or {}).get('clicks_adx') or 0.0) for x in rows_sorted)

                roi_total = ((revenue_total - spend_total) / spend_total * 100.0) if spend_total > 0 else 0.0

                daily_roi = []
                for x in rows_sorted:
                    s = float((x or {}).get('spend') or 0.0)
                    rrev = float((x or {}).get('revenue') or 0.0)
                    if s > 0:
                        daily_roi.append(((rrev - s) / s) * 100.0)

                last7 = daily_roi[-7:] if daily_roi else []
                roi_stab = _stdev(last7)
                stability_idx = _clamp(100.0 - (roi_stab * 4.0), 0.0, 100.0)

                last3 = daily_roi[-3:] if daily_roi else []
                last3_avg_roi = (sum(last3) / float(len(last3))) if last3 else 0.0

                ml_payload = {
                    'roi': roi_total,
                    'spend': spend_total,
                    'revenue': revenue_total,
                    'impressions_fb': impressions_fb_total,
                    'clicks_fb': clicks_fb_total,
                    'impressions_adx': impressions_adx_total,
                    'clicks_adx': clicks_adx_total,
                    'days': len(rows_sorted),
                    'roi_stability': roi_stab,
                    'stability_idx': stability_idx,
                    'last3_avg_roi': last3_avg_roi,
                }
                ml_payload.update(_decide_action(ml_payload))
                ml[site_key] = ml_payload

            result = {
                'status': True,
                'last_update': last_update,
                'data': combined_data_all,
                'data_filtered': combined_data_filtered,
                'raw_rows': raw_rows_all,
                'ml': ml,
                'summary': {
                    'total_spend': total_spend,
                    'roi_nett': roi_nett_summary,
                    'total_revenue': total_revenue
                }
            }
            set_cached_data(response_cache_key, result, timeout=300)
            return JsonResponse(result, safe=False)
        except Exception as e:
            return JsonResponse({'status': False, 'error': str(e)})

class RoiMonitoringDomainCampaignsView(View):
    def dispatch(self, request, *args, **kwargs):
        if 'hris_admin' not in request.session:
            return redirect('admin_login')
        return super().dispatch(request, *args, **kwargs)

    def get(self, req):
        try:
            account_ads = (req.GET.get('account_ads') or '').strip()
            site_name = (req.GET.get('site_name') or '').strip()
            start_date = (req.GET.get('start_date') or '').strip()
            end_date = (req.GET.get('end_date') or '').strip()

            if not end_date:
                end_date = datetime.now().strftime('%Y-%m-%d')
            if not start_date:
                try:
                    end_dt = datetime.strptime(end_date, '%Y-%m-%d').date()
                except Exception:
                    end_dt = datetime.now().date()
                    end_date = end_dt.strftime('%Y-%m-%d')
                start_date = (end_dt - timedelta(days=6)).strftime('%Y-%m-%d')

            if not account_ads or not site_name:
                return JsonResponse({'status': False, 'error': 'account_ads dan site_name wajib diisi', 'campaigns': []})

            accounts = (data_mysql().master_account_ads() or {}).get('data') or []
            match = None
            for a in (accounts or []):
                if str((a or {}).get('account_name') or '').strip() == account_ads:
                    match = a
                    break
            if match is None:
                key = account_ads.lower()
                for a in (accounts or []):
                    if str((a or {}).get('account_name') or '').strip().lower() == key:
                        match = a
                        break
            if match is None:
                return JsonResponse({'status': False, 'error': 'Account Ads tidak ditemukan', 'campaigns': []})

            access_token = str((match or {}).get('access_token') or '')
            account_id = str((match or {}).get('account_id') or '')
            account_name = str((match or {}).get('account_name') or account_ads)
            if not access_token or not account_id:
                return JsonResponse({'status': False, 'error': 'Access token / account_id kosong', 'campaigns': []})

            rs = fetch_data_insights_account(
                str(start_date),
                access_token,
                account_id,
                str(site_name),
                account_name,
                str(end_date),
            )
            items = (rs or {}).get('data') or []
            by_id = {}
            for it in (items or []):
                cid = str((it or {}).get('campaign_id') or '').strip()
                if not cid:
                    continue
                try:
                    spend = float((it or {}).get('spend') or 0.0)
                except Exception:
                    spend = 0.0
                cur = by_id.get(cid)
                cur_spend = 0.0
                if isinstance(cur, dict):
                    try:
                        cur_spend = float(cur.get('spend') or 0.0)
                    except Exception:
                        cur_spend = 0.0
                if (cur is None) or (spend > cur_spend):
                    try:
                        dbgt = float((it or {}).get('daily_budget') or 0.0)
                    except Exception:
                        dbgt = 0.0
                    by_id[cid] = {
                        'campaign_id': cid,
                        'campaign_name': str((it or {}).get('campaign_name') or cid),
                        'daily_budget': dbgt,
                        'spend': spend,
                    }

            campaigns = list(by_id.values())
            try:
                campaigns.sort(key=lambda x: float((x or {}).get('spend') or 0.0), reverse=True)
            except Exception:
                pass

            return JsonResponse({'status': True, 'campaigns': campaigns, 'start_date': start_date, 'end_date': end_date}, safe=False)
        except Exception as e:
            return JsonResponse({'status': False, 'error': str(e), 'campaigns': []}, safe=False)

class RoiMonitoringDomainCampaignBreakdownView(View):
    def dispatch(self, request, *args, **kwargs):
        if 'hris_admin' not in request.session:
            return redirect('admin_login')
        return super().dispatch(request, *args, **kwargs)

    def get(self, req):
        try:
            site_name = (req.GET.get('site_name') or '').strip().lower()
            start_date = (req.GET.get('start_date') or '').strip()
            end_date = (req.GET.get('end_date') or '').strip()

            if not site_name:
                return JsonResponse({'status': False, 'error': 'site_name wajib diisi', 'data': []}, safe=False)

            if not end_date:
                end_date = datetime.now().strftime('%Y-%m-%d')
            if not start_date:
                end_dt = datetime.strptime(end_date, '%Y-%m-%d').date()
                start_date = (end_dt - timedelta(days=6)).strftime('%Y-%m-%d')

            parts = [p for p in site_name.split('.') if p]
            if len(parts) >= 2:
                site_name = '.'.join(parts[:2])

            rs = data_mysql().get_monitoring_domain_campaign_breakdown_by_params(start_date, end_date, site_name)
            if not (isinstance(rs, dict) and rs.get('status')):
                return JsonResponse({'status': False, 'error': (rs or {}).get('error', 'Gagal mengambil data'), 'data': []}, safe=False)

            return JsonResponse({'status': True, 'site_name': site_name, 'start_date': start_date, 'end_date': end_date, 'data': rs.get('data', [])}, safe=False)
        except Exception as e:
            return JsonResponse({'status': False, 'error': str(e), 'data': []}, safe=False)

@method_decorator(csrf_exempt, name='dispatch')
class RoiMonitoringDomainUpdateDailyBudgetCampaignView(View):
    def dispatch(self, request, *args, **kwargs):
        if 'hris_admin' not in request.session:
            return redirect('admin_login')
        return super().dispatch(request, *args, **kwargs)

    def post(self, req):
        try:
            account_ads = (req.POST.get('account_ads') or '').strip()
            campaign_id = (req.POST.get('campaign_id') or '').strip()
            raw = req.POST.get('daily_budget', '0')
            cleaned = raw.replace('Rp', '').replace('.', '').replace(',', '').strip()
            try:
                daily_budget = int(cleaned)
            except Exception:
                daily_budget = 0

            if not account_ads or not campaign_id:
                return JsonResponse({'status': False, 'error': 'account_ads dan campaign_id wajib diisi'})
            if daily_budget <= 0:
                return JsonResponse({'status': False, 'error': 'daily_budget tidak valid'})

            accounts = (data_mysql().master_account_ads() or {}).get('data') or []
            match = None
            for a in (accounts or []):
                if str((a or {}).get('account_name') or '').strip() == account_ads:
                    match = a
                    break
            if match is None:
                key = account_ads.lower()
                for a in (accounts or []):
                    if str((a or {}).get('account_name') or '').strip().lower() == key:
                        match = a
                        break
            if match is None:
                return JsonResponse({'status': False, 'error': 'Account Ads tidak ditemukan'})

            access_token = str((match or {}).get('access_token') or '')
            account_id = str((match or {}).get('account_id') or '')
            if not access_token or not account_id:
                return JsonResponse({'status': False, 'error': 'Access token / account_id kosong'})

            data = fetch_daily_budget_per_campaign(
                access_token,
                account_id,
                str(campaign_id),
                int(daily_budget),
            )

            if (data or {}).get('daily_budget') is not None:
                from .utils import invalidate_cache_on_data_update
                invalidate_cache_on_data_update(account_id, campaign_id, 'budget_update')

            return JsonResponse({'status': True, 'daily_budget': (data or {}).get('daily_budget')})
        except Exception as e:
            return JsonResponse({'status': False, 'error': str(e)})

@method_decorator(csrf_exempt, name='dispatch')
class RoiMonitoringDomainUpdateCampaignStatusCampaignView(View):
    def dispatch(self, request, *args, **kwargs):
        if 'hris_admin' not in request.session:
            return redirect('admin_login')
        return super().dispatch(request, *args, **kwargs)

    def post(self, req):
        try:
            account_ads = (req.POST.get('account_ads') or '').strip()
            status = (req.POST.get('status') or 'PAUSED').strip().upper() or 'PAUSED'

            campaign_id = (req.POST.get('campaign_id') or '').strip()
            campaign_ids_json = (req.POST.get('campaign_ids') or '').strip()

            campaign_ids = []
            if campaign_ids_json:
                try:
                    import json
                    parsed = json.loads(campaign_ids_json)
                    if isinstance(parsed, list):
                        campaign_ids = [str(x).strip() for x in parsed if str(x).strip()]
                except Exception:
                    campaign_ids = []

            if campaign_id:
                campaign_ids = [campaign_id]

            if not account_ads:
                return JsonResponse({'status': False, 'error': 'account_ads wajib diisi'})
            if not campaign_ids:
                return JsonResponse({'status': False, 'error': 'campaign_id(s) wajib diisi'})
            if status not in ['ACTIVE', 'PAUSED']:
                return JsonResponse({'status': False, 'error': 'status tidak valid'})

            accounts = (data_mysql().master_account_ads() or {}).get('data') or []
            match = None
            for a in (accounts or []):
                if str((a or {}).get('account_name') or '').strip() == account_ads:
                    match = a
                    break
            if match is None:
                key = account_ads.lower()
                for a in (accounts or []):
                    if str((a or {}).get('account_name') or '').strip().lower() == key:
                        match = a
                        break
            if match is None:
                return JsonResponse({'status': False, 'error': 'Account Ads tidak ditemukan'})

            access_token = str((match or {}).get('access_token') or '')
            account_id = str((match or {}).get('account_id') or '')
            if not access_token or not account_id:
                return JsonResponse({'status': False, 'error': 'Access token / account_id kosong'})

            success_count = 0
            failed = []
            last_status = None

            for cid in campaign_ids:
                try:
                    data = fetch_status_per_campaign(str(access_token), str(cid), str(status))
                    if isinstance(data, dict) and ('error' not in data):
                        success_count += 1
                        last_status = data.get('status')
                        try:
                            from .utils import invalidate_cache_on_data_update
                            invalidate_cache_on_data_update(account_id, cid, 'status_update')
                        except Exception:
                            pass
                    else:
                        failed.append({'campaign_id': cid, 'error': (data or {}).get('error') if isinstance(data, dict) else 'unknown_error'})
                except Exception as e:
                    failed.append({'campaign_id': cid, 'error': str(e)})

            if success_count <= 0:
                return JsonResponse({'status': False, 'error': 'Gagal mengupdate status campaign', 'failed': failed}, safe=False)

            return JsonResponse({
                'status': True,
                'updated': success_count,
                'failed': failed,
                'campaign_status': last_status or status,
            }, safe=False)
        except Exception as e:
            return JsonResponse({'status': False, 'error': str(e)})

# ===== ROI Monitoring Country =====

class RoiMonitoringCountryView(View):
    """View untuk ROI Summary - menampilkan ringkasan data ROI"""
    def dispatch(self, request, *args, **kwargs):
        if 'hris_admin' not in request.session:
            return redirect('admin_login')
        return super().dispatch(request, *args, **kwargs)
    def get(self, req):
        admin = req.session.get('hris_admin', {})
        data_account = data_mysql().master_account_ads()['data']
        last_update = data_mysql().get_last_update_adx_traffic_per_domain()['data']['last_update']
        if admin.get('super_st') == '0':
            data_account_adx = data_mysql().get_all_adx_account_data_user(admin.get('user_id'))
            data_domain_adx = data_mysql().get_all_adx_domain_data_user(admin.get('user_id'))
        else:
            data_account_adx = data_mysql().get_all_adx_account_data()
            data_domain_adx = data_mysql().get_all_adx_domain_data()
        if not data_domain_adx['status']:
            return JsonResponse({
                'status': False,
                'error': data_domain_adx['data']
            })
        data = {
            'title': 'ROI Summary Dashboard',
            'user': req.session['hris_admin'],
            'data_account': data_account,
            'data_account_adx': data_account_adx['data'],
            'data_domain_adx': data_domain_adx['data'],
            'last_update': last_update
        }
        return render(req, 'admin/report_roi/monitoring_country/index.html', data)

class RoiMonitoringCountryDataView(View):
    def dispatch(self, request, *args, **kwargs):
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
        selected_account = req.GET.get('selected_account_adx', '')
        admin = req.session.get('hris_admin', {})
        if selected_account == '':
            rs_account = data_mysql().get_all_adx_account_data_user(admin.get('user_id'))
            account_ids = [str(item['account_id']) for item in rs_account.get('data', [])]
            selected_account = ",".join(account_ids)
        else:
            selected_account = req.GET.get('selected_account_adx', '')
        selected_domain = req.GET.get('selected_domains', '')
        selected_account_list = []
        if selected_account:
            selected_account_list = [str(a).strip() for a in selected_account.split(',') if a.strip()]
        selected_domain_list = build_domain_filter_terms(selected_domain, include_original=True, include_base=True)
        selected_domain_list_fb = build_domain_filter_terms(selected_domain, include_original=False, include_base=True)
        selected_countries = req.GET.get('selected_countries', '')
        include_subdomains = str(req.GET.get('include_subdomains') or '').strip().lower() in ('1', 'true', 'yes')
        try:
            # Validasi parameter tanggal terlebih dahulu
            if not start_date or not end_date:
                return JsonResponse({
                    'status': False,
                    'error': 'Parameter tanggal tidak lengkap'
                })
            # Parse selected countries dari string yang dipisah koma
            raw_countries_list = []
            if selected_countries and selected_countries.strip():
                raw_countries_list = [country.strip() for country in selected_countries.split(',') if country.strip()]
            else:
                print("[DEBUG] No countries selected, will fetch all countries")

            def normalize_country_code(cc):
                c = (str(cc or '').strip().upper())
                if not c:
                    return ''
                if c == 'TU':
                    return 'TR'
                return c

            countries_list = []
            for c in (raw_countries_list or []):
                cc = normalize_country_code(c)
                if cc and cc not in countries_list:
                    countries_list.append(cc)

            def expand_country_codes_filter(items):
                out = []
                for it in (items or []):
                    s = normalize_country_code(it)
                    if not s:
                        continue
                    out.append(s)
                    if s == 'TR':
                        out.append('TU')
                seen = set()
                uniq = []
                for x in out:
                    if x and x not in seen:
                        seen.add(x)
                        uniq.append(x)
                return uniq

            countries_list_query = expand_country_codes_filter(countries_list)

            # agar FB mengikuti domain yang ada di akun AdX terpilih
            sites_for_fb = None
            if not selected_domain or not selected_domain.strip():
                try:
                    # Ambil list sites dari database
                    sites_result = data_mysql().fetch_user_sites_id_list(
                        start_date, end_date, selected_account or '%'
                    )
                    if sites_result['hasil']['data']:
                        # Ambil data sites
                        sites_for_fb = sites_result['hasil']['data']
                        # Hapus semua 'Unknown'
                        sites_for_fb = [site for site in sites_for_fb if site != 'Unknown']
                    else:
                        print(f"[DEBUG ROI] No sites derived for FB filter: {sites_result['hasil']['data']}")
                except Exception as _sites_err:
                    print(f"[DEBUG ROI] Unable to derive sites_for_fb: {_sites_err}")
            # ===== Response-level cache (meng-cache hasil akhir penggabungan) =====
            response_cache_key = generate_cache_key(
                'roi_country_response_v3',
                start_date,
                end_date,
                selected_account or '',
                selected_domain_list or '',
                ','.join(countries_list_query) if countries_list_query else '',
                'subdomains:1' if include_subdomains else ''
            )
            cached_response = get_cached_data(response_cache_key)
            if cached_response is not None:
                return JsonResponse(cached_response, safe=False)
            data_facebook = None
            # Jalankan paralel jika selected_domain sudah ada (menghindari fetch FB yang terlalu lebar)
            if selected_account_list and not selected_domain_list:
                with ThreadPoolExecutor(max_workers=2) as executor:
                    adx_future = executor.submit(
                        data_mysql().get_all_adx_roi_country_detail_by_params,
                        start_date,
                        end_date,
                        selected_account_list,
                        selected_domain_list,
                        countries_list_query
                    )
                    data_adx = adx_future.result()
                    unique_name_site = []
                    if data_adx.get("status") and data_adx.get("data"):
                        unique_sites = set()
                        for row in data_adx["data"]:
                            site_name = (row.get("site_name") or "").strip().lower()
                            if not site_name or site_name == "unknown":
                                continue
                            unique_sites.add(site_name)
                        extracted_names = []
                        for site in unique_sites:
                            if "." not in site:
                                continue

                            parts = site.split(".")

                            if len(parts) >= 2:
                                main_domain = ".".join(parts[:2])   # ✅ ambil depan
                            else:
                                main_domain = site

                            extracted_names.append(main_domain)
                        unique_name_site = list(set(extracted_names))
                    fb_future = executor.submit(
                        data_mysql().get_all_ads_roi_country_detail_by_params,
                        start_date,
                        end_date,
                        unique_name_site,
                        countries_list_query
                    )
                    data_adx = adx_future.result()
                    try:
                        # Hapus timeout: tunggu hingga FB selesai agar data lengkap
                        data_facebook = fb_future.result()
                    except Exception as e:
                        data_facebook = None
            elif selected_domain_list :
                with ThreadPoolExecutor(max_workers=2) as executor:
                    adx_future = executor.submit(
                        data_mysql().get_all_adx_roi_country_detail_by_params,
                        start_date,
                        end_date,
                        selected_account_list,
                        selected_domain_list,
                        countries_list_query
                    )
                    unique_name_site = []
                    if selected_domain_list_fb:
                        seen_sites = set()
                        for site_item in selected_domain_list_fb:
                            site_name = str(site_item or '').strip().strip("\"'")
                            if not site_name or site_name == 'Unknown' or site_name in seen_sites:
                                continue
                            seen_sites.add(site_name)
                            unique_name_site.append(site_name)
                    fb_future = executor.submit(
                        data_mysql().get_all_ads_roi_country_detail_by_params,
                        start_date,
                        end_date,
                        unique_name_site,
                        countries_list_query
                    )
                    data_adx = adx_future.result()
                    try:
                        # Hapus timeout: tunggu hingga FB selesai agar data lengkap
                        data_facebook = fb_future.result()
                    except Exception as e:
                        data_facebook = None
            else:
                # Filter Domain kosong: tampilkan data semua domain dari akun AdX terpilih
                data_adx = data_mysql().get_all_adx_roi_country_detail_by_params(
                    start_date, 
                    end_date, 
                    selected_account_list, 
                    selected_domain_list, 
                    countries_list_query
                )
                try:
                    unique_name_site = []
                    with ThreadPoolExecutor(max_workers=1) as executor:
                        if sites_for_fb:
                            unique_sites = set(site.strip() for site in sites_for_fb if site.strip() and site.strip() != 'Unknown')
                            extracted_names = []
                            for site in unique_sites:
                                main_domain = extract_base_subdomain(site.strip())
                                if main_domain and main_domain != 'Unknown':
                                    extracted_names.append(main_domain)
                            unique_name_site = list(set(extracted_names))

                        if not unique_name_site:
                            adx_payload_tmp = data_adx.get('hasil') if isinstance(data_adx, dict) and data_adx.get('hasil') else data_adx
                            adx_items_tmp = adx_payload_tmp.get('data') if isinstance(adx_payload_tmp, dict) else []
                            if adx_items_tmp:
                                extracted_names = []
                                for adx_item in (adx_items_tmp or []):
                                    site_name = str(adx_item.get('site_name', '') or '')
                                    main_domain = extract_base_subdomain(site_name.strip())
                                    if main_domain and main_domain != 'Unknown':
                                        extracted_names.append(main_domain)
                                unique_name_site = list(set(extracted_names))
                        if unique_name_site:
                            fb_future = executor.submit(
                                data_mysql().get_all_ads_roi_country_detail_by_params,
                                start_date, end_date, unique_name_site, countries_list_query
                            )
                            data_facebook = fb_future.result()
                        else:
                            data_facebook = None
                except Exception as e:
                    data_facebook = None
            # Ringkas data Facebook untuk diagnosa
            try:
                if data_facebook and isinstance(data_facebook, dict) and data_facebook.get('hasil') and data_facebook['hasil'].get('data'):
                    fb_items = data_facebook['hasil'].get('data', []) or []
                    fb_total_spend = 0.0
                    for _it in fb_items:
                        try:
                            fb_total_spend += float(_it.get('spend', 0) or 0)
                        except Exception:
                            pass
                    if fb_items:
                        sample_labels = []
                        for _s in fb_items[:5]:
                            sample_labels.append(_s.get('country'))
                else:
                    print("[DEBUG ROI] No Facebook data returned or fetch failed")
            except Exception as _sum_e:
                print(f"[DEBUG ROI] Unable to summarize FB data: {_sum_e}")
            # Proses penggabungan data AdX dan Facebook
            # Pastikan bentuk payload sesuai: gunakan 'hasil' untuk AdX dan FB jika tersedia
            adx_payload = data_adx.get('hasil') if isinstance(data_adx, dict) and data_adx.get('hasil') else data_adx
            fb_payload = (data_facebook.get('hasil') if isinstance(data_facebook, dict) and data_facebook.get('hasil') else {'status': True, 'data': []})
            result = process_roi_monitoring_country_data(adx_payload, fb_payload)

            # Konsistensi summary spend dengan menu monitoring_domain (campaign-level FB)
            campaign_total_spend = None
            try:
                # Ikuti logika monitoring_domain: ambil domain dari payload AdX pada periode aktif
                campaign_sites = []
                if selected_domain_list:
                    campaign_sites = [extract_base_subdomain(s) for s in selected_domain_list if str(s or '').strip()]
                else:
                    adx_items_for_sites = adx_payload.get('data') if isinstance(adx_payload, dict) else []
                    if isinstance(adx_items_for_sites, list):
                        campaign_sites = [extract_base_subdomain((it or {}).get('site_name', '')) for it in adx_items_for_sites if isinstance(it, dict)]

                campaign_sites = sorted(set([str(s).strip() for s in campaign_sites if str(s).strip() and str(s).strip() != 'Unknown']))

                if campaign_sites:
                    fb_campaign = data_mysql().get_all_ads_roi_monitoring_campaign_by_params(
                        start_date,
                        end_date,
                        campaign_sites
                    )
                    fb_campaign_items = (((fb_campaign or {}).get('hasil') or {}).get('data') or [])
                    campaign_total_spend = round(sum(float((it or {}).get('spend', 0) or 0) for it in fb_campaign_items if isinstance(it, dict)), 2)
            except Exception:
                campaign_total_spend = None

            if campaign_total_spend is not None and not countries_list_query:
                def _override_summary_spend(sum_obj):
                    if not isinstance(sum_obj, dict):
                        return sum_obj
                    try:
                        total_revenue = float(sum_obj.get('total_revenue', 0) or 0)
                    except Exception:
                        total_revenue = 0.0
                    total_spend = float(campaign_total_spend or 0)
                    total_net_profit = total_revenue - total_spend
                    roi_nett = ((total_net_profit / total_spend) * 100) if total_spend > 0 else 0.0
                    sum_obj['total_spend'] = round(total_spend, 2)
                    sum_obj['total_net_profit'] = round(total_net_profit, 2)
                    sum_obj['roi_nett'] = round(roi_nett, 2)
                    sum_obj['total_roi'] = round(roi_nett, 2)
                    sum_obj['spend_source'] = 'facebook_campaign_level'
                    return sum_obj

                result['summary_all'] = _override_summary_spend(result.get('summary_all'))
                result['summary_filtered'] = _override_summary_spend(result.get('summary_filtered'))

            # Tambahkan daily_rows
            try:
                result['daily_rows'] = build_roi_monitoring_country_daily_rows(adx_payload, fb_payload)
            except Exception:
                result['daily_rows'] = []

            # ===== H-1 compare (previous day) untuk single-day request =====
            compare = None
            try:
                if start_date == end_date:
                    cur_dt = datetime.strptime(str(start_date), '%Y-%m-%d')
                    prev_dt = cur_dt - timedelta(days=1)
                    prev_start = prev_dt.strftime('%Y-%m-%d')
                    prev_end = prev_start

                    unique_name_site_local = []
                    try:
                        unique_name_site_local = locals().get('unique_name_site') or []
                    except Exception:
                        unique_name_site_local = []

                    prev_adx = data_mysql().get_all_log_adx_country_detail_by_params(
                        prev_start,
                        prev_end,
                        selected_account_list,
                        selected_domain_list,
                        countries_list_query
                    )

                    prev_fb = None
                    try:
                        if unique_name_site_local:
                            prev_fb = data_mysql().get_all_log_ads_country_detail_by_params(
                                prev_start,
                                prev_end,
                                unique_name_site_local,
                                countries_list_query
                            )
                    except Exception:
                        prev_fb = None

                    prev_adx_payload = prev_adx.get('hasil') if isinstance(prev_adx, dict) and prev_adx.get('hasil') else prev_adx
                    prev_fb_payload = (prev_fb.get('hasil') if isinstance(prev_fb, dict) and prev_fb.get('hasil') else {'status': True, 'data': []})

                    prev_res = process_roi_monitoring_country_data(
                        prev_adx_payload or {'status': True, 'data': []},
                        prev_fb_payload
                    )

                    def pick_summary(res):
                        if not isinstance(res, dict):
                            return {}
                        return res.get('summary_filtered') or res.get('summary_all') or res.get('summary') or {}

                    def delta_summary(cur, prev):
                        try:
                            cs = float(cur.get('total_spend') or 0)
                            ps = float(prev.get('total_spend') or 0)
                            cr = float(cur.get('total_revenue') or 0)
                            pr = float(prev.get('total_revenue') or 0)
                            cn = float(cur.get('total_net_profit') or 0)
                            pn = float(prev.get('total_net_profit') or 0)
                            croi = float(cur.get('roi_nett') or cur.get('total_roi') or 0)
                            proi = float(prev.get('roi_nett') or prev.get('total_roi') or 0)
                        except Exception:
                            cs = ps = cr = pr = cn = pn = croi = proi = 0.0
                        return {
                            'total_spend': cs - ps,
                            'total_revenue': cr - pr,
                            'total_net_profit': cn - pn,
                            'roi_nett': croi - proi
                        }

                    sum_cur = pick_summary(result)
                    sum_prev = pick_summary(prev_res)
                    compare = {
                        'mode': 'h-1',
                        'current_start': start_date,
                        'current_end': end_date,
                        'prev_start': prev_start,
                        'prev_end': prev_end,
                        'summary_current': sum_cur,
                        'summary_prev': sum_prev,
                        'summary_delta': delta_summary(sum_cur, sum_prev)
                    }

                    prev_map = {}
                    for it in (prev_res.get('data') or []):
                        cc = normalize_country_code((it or {}).get('country_code', ''))
                        if cc and cc not in prev_map:
                            prev_map[cc] = it

                    def attach_prev(rows):
                        for it in (rows or []):
                            cc = normalize_country_code((it or {}).get('country_code', ''))
                            p = prev_map.get(cc) or {}
                            try:
                                ps = float(p.get('spend') or 0)
                                pr = float(p.get('revenue') or 0)
                                pn = float(p.get('net_profit') or (pr - ps))
                                proi = float(p.get('roi') or (((pr - ps) / ps * 100) if ps > 0 else 0))
                            except Exception:
                                ps = pr = pn = proi = 0.0
                            try:
                                cs = float(it.get('spend') or 0)
                                cr = float(it.get('revenue') or 0)
                                cn = float(it.get('net_profit') or (cr - cs))
                                croi = float(it.get('roi') or (((cr - cs) / cs * 100) if cs > 0 else 0))
                            except Exception:
                                cs = cr = cn = croi = 0.0
                            it['prev_spend'] = ps
                            it['prev_revenue'] = pr
                            it['prev_net_profit'] = pn
                            it['prev_roi'] = proi
                            it['delta_spend'] = cs - ps
                            it['delta_revenue'] = cr - pr
                            it['delta_net_profit'] = cn - pn
                            it['delta_roi'] = croi - proi

                    attach_prev(result.get('data'))
                    attach_prev(result.get('data_filtered'))
            except Exception:
                compare = None

            if compare:
                result['compare'] = compare

            if include_subdomains:
                subdomains_by_country = {}
                try:
                    want = set([normalize_country_code(c) for c in (countries_list or []) if normalize_country_code(c)])

                    def _add(cc, site):
                        if not cc:
                            return
                        if want and cc not in want:
                            return
                        s = str(site or '').strip()
                        if not s or s == 'Unknown':
                            return
                        b = extract_base_subdomain(s)
                        if not b:
                            return
                        cur = subdomains_by_country.get(cc)
                        if cur is None:
                            cur = set()
                            subdomains_by_country[cc] = cur
                        cur.add(b)

                    adx_items = adx_payload.get('data') if isinstance(adx_payload, dict) else []
                    for it in (adx_items or []):
                        cc = normalize_country_code((it or {}).get('country_code'))
                        _add(cc, (it or {}).get('site_name'))

                    fb_items = fb_payload.get('data') if isinstance(fb_payload, dict) else []
                    for it in (fb_items or []):
                        cc = normalize_country_code((it or {}).get('country_code'))
                        _add(cc, (it or {}).get('domain'))

                    result['subdomains_by_country'] = {k: sorted(list(v)) for k, v in subdomains_by_country.items()}
                    if len(want) == 1:
                        only = next(iter(want))
                        result['subdomains'] = result['subdomains_by_country'].get(only, [])
                except Exception:
                    result['subdomains_by_country'] = {}
                    if countries_list and len(countries_list) == 1:
                        result['subdomains'] = []

            # Filter hasil berdasarkan negara yang dipilih jika ada
            if countries_list and result.get('status') and result.get('data'):
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
                filtered_data = []
                for item in result['data']:
                    country_code = item.get('country_code', '').lower()
                    country_name = item.get('country', '').lower()
                    # Check if country matches any in the filter list (case insensitive)
                    country_matched = False
                    for filter_country in parsed_filter_countries:
                        if country_name == filter_country or country_code == filter_country:
                            country_matched = True
                            break
                    # Only add to filtered_data if country_matched is True
                    if country_matched:
                        filtered_data.append(item)
                    else:
                        print(f"[DEBUG ROI] ✗ No match found for '{country_name}' - EXCLUDED from results")
                
                result['data'] = filtered_data
                result['total_records'] = len(filtered_data)
            # Simpan hasil akhir ke cache dengan TTL 15 menit
            try:
                set_cached_data(response_cache_key, result, timeout=900)
            except Exception as _cache_err:
                print(f"[DEBUG] Failed to cache ROI Country final response: {_cache_err}")
            return JsonResponse(result, safe=False)
        except Exception as e:
            return JsonResponse({
                'status': False,
                'error': str(e)
            })


class RoiMonitoringCountryBreakdownView(View):
    def dispatch(self, request, *args, **kwargs):
        if 'hris_admin' not in request.session:
            return redirect('admin_login')
        return super().dispatch(request, *args, **kwargs)

    def get(self, req):
        try:
            country_code = str(req.GET.get('country_code') or '').strip().upper()
            start_date = str(req.GET.get('start_date') or '').strip()
            end_date = str(req.GET.get('end_date') or '').strip()
            selected_domains = str(req.GET.get('selected_domains') or '').strip()

            if not country_code:
                return JsonResponse({'status': False, 'error': 'country_code wajib diisi', 'data': []}, safe=False)

            if not end_date:
                end_date = datetime.now().strftime('%Y-%m-%d')
            if not start_date:
                end_dt = datetime.strptime(end_date, '%Y-%m-%d').date()
                start_date = (end_dt - timedelta(days=6)).strftime('%Y-%m-%d')

            selected_domain_list = [s.strip() for s in selected_domains.split(',') if s.strip()] if selected_domains else []
            rs = data_mysql().get_monitoring_country_subdomain_campaign_breakdown_by_params(
                start_date,
                end_date,
                country_code,
                selected_domain_list
            )

            if not (isinstance(rs, dict) and rs.get('status')):
                return JsonResponse({
                    'status': False,
                    'error': (rs or {}).get('error', 'Gagal mengambil data breakdown'),
                    'data': []
                }, safe=False)

            return JsonResponse({
                'status': True,
                'country_code': country_code,
                'start_date': start_date,
                'end_date': end_date,
                'data': rs.get('data', [])
            }, safe=False)
        except Exception as e:
            return JsonResponse({'status': False, 'error': str(e), 'data': []}, safe=False)

# ===== ROI Rekapitulasi =====

class RoiRekapitulasiView(View):
    """View untuk ROI Summary - menampilkan ringkasan data ROI"""
    def dispatch(self, request, *args, **kwargs):
        if 'hris_admin' not in request.session:
            return redirect('admin_login')
        return super().dispatch(request, *args, **kwargs)
    def get(self, req):
        admin = req.session.get('hris_admin', {})
        data_account = data_mysql().master_account_ads()['data']
        last_update = data_mysql().get_last_update_adx_traffic_per_domain()['data']['last_update']
        if admin.get('super_st') == '0':
            data_account_adx = data_mysql().get_all_adx_account_data_user(admin.get('user_id'))
            data_domain_adx = data_mysql().get_all_adx_domain_data_user(admin.get('user_id'))
        else:
            data_account_adx = data_mysql().get_all_adx_account_data()
            data_domain_adx = data_mysql().get_all_adx_domain_data()
        if not data_domain_adx['status']:
            return JsonResponse({
                'status': False,
                'error': data_domain_adx['data']
            })
        data = {
            'title': 'ROI Summary Dashboard',
            'user': req.session['hris_admin'],
            'data_account': data_account,
            'data_account_adx': data_account_adx['data'],
            'data_domain_adx': data_domain_adx['data'],
            'last_update': last_update
        }
        return render(req, 'admin/report_roi/rekapitulasi_roi/index.html', data)

class RoiRekapitulasiDataView(View):
    def dispatch(self, request, *args, **kwargs):
        if 'hris_admin' not in request.session:
            return redirect('admin_login')
        return super().dispatch(request, *args, **kwargs)
    def get(self, req):
        try:
            periode_mode = (req.GET.get('periode_mode') or 'harian').strip().lower()

            MONTH_ID = {
                1: "Jan", 2: "Feb", 3: "Mar", 4: "Apr",
                5: "Mei", 6: "Jun", 7: "Jul", 8: "Agu",
                9: "Sep", 10: "Okt", 11: "Nov", 12: "Des"
            }

            def format_tanggal_id(dt):
                return f"{dt.day} {MONTH_ID[dt.month]} {dt.year}"

            def parse_month(ym):
                s = (ym or '').strip()
                parts = s.split('-')
                if len(parts) != 2:
                    raise ValueError(f"Format bulan tidak valid: {ym}")
                y = int(parts[0])
                m = int(parts[1])
                if m < 1 or m > 12:
                    raise ValueError(f"Bulan tidak valid: {ym}")
                return y, m

            def add_months(y, m, delta):
                idx = (y * 12 + (m - 1)) + int(delta)
                ny = idx // 12
                nm = (idx % 12) + 1
                return ny, nm

            if periode_mode == 'bulanan':
                import calendar

                month_from = req.GET.get('month_from')
                month_to = req.GET.get('month_to')
                if not month_from or not month_to:
                    raise ValueError("month_from dan month_to harus diisi untuk mode bulanan")

                y1, m1 = parse_month(month_from)
                y2, m2 = parse_month(month_to)
                months_count = (y2 - y1) * 12 + (m2 - m1) + 1
                if months_count <= 0:
                    raise ValueError("month_from > month_to")

                start_date = datetime(y1, m1, 1).date()
                end_date = datetime(y2, m2, calendar.monthrange(y2, m2)[1]).date()

                py2, pm2 = add_months(y1, m1, -1)
                py1, pm1 = add_months(py2, pm2, -(months_count - 1))

                past_start_date = datetime(py1, pm1, 1).date()
                past_end_date = datetime(py2, pm2, calendar.monthrange(py2, pm2)[1]).date()

                periode_now = (
                    f"Periode <br> "
                    f"{format_tanggal_id(start_date)} s/d {format_tanggal_id(end_date)}"
                )
                periode_past = (
                    f"Periode <br> "
                    f"{format_tanggal_id(past_start_date)} s/d {format_tanggal_id(past_end_date)}"
                )
            else:
                start_date = req.GET.get('start_date')
                end_date = req.GET.get('end_date')
                if not start_date or not end_date:
                    raise ValueError("start_date dan end_date harus diisi")
                start_date = datetime.strptime(start_date, '%Y-%m-%d').date()
                end_date = datetime.strptime(end_date, '%Y-%m-%d').date()
                if start_date > end_date:
                    raise ValueError("start_date > end_date")

                total_days = (end_date - start_date).days + 1
                past_end_date = start_date - timedelta(days=1)
                past_start_date = past_end_date - timedelta(days=total_days - 1)

                periode_now = (
                    f"Periode <br> "
                    f"{format_tanggal_id(start_date)} s/d {format_tanggal_id(end_date)}"
                )
                periode_past = (
                    f"Periode <br> "
                    f"{format_tanggal_id(past_start_date)} s/d {format_tanggal_id(past_end_date)}"
                )
            selected_account = req.GET.get('selected_account_adx')
            admin = req.session.get('hris_admin', {})
            if selected_account == '':
                rs_account = data_mysql().get_all_adx_account_data_user(admin.get('user_id'))
                account_ids = [str(item['account_id']) for item in rs_account.get('data', [])]
                selected_account = ",".join(account_ids)
            else:
                selected_account = req.GET.get('selected_account_adx', '')
            selected_account_list = []
            if selected_account:
                selected_account_list = [
                    s.strip() for s in selected_account.split(',') if s.strip()
                ]
            selected_domain = req.GET.get('selected_domains')
            selected_domain_list = []
            if selected_domain:
                selected_domain_list = [
                    s.strip() for s in selected_domain.split(',') if s.strip()  
                ]
            adx_result = data_mysql().get_all_rekapitulasi_adx_monitoring_account_by_params(
                start_date,
                end_date,
                past_start_date,
                past_end_date,
                selected_account_list,
                selected_domain_list
            )
            return JsonResponse({
                'status': True,
                'periode_now': periode_now,
                'periode_past': periode_past,
                'data': adx_result['hasil']['data'],
            })
        except Exception as e:
            return JsonResponse({'status': False, 'error': str(e)})

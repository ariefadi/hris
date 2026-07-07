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
from datetime import datetime, date, timedelta, timezone
from django.http import HttpResponse, JsonResponse, QueryDict, HttpResponseRedirect, StreamingHttpResponse
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

META_ADSET_TZ = timezone(timedelta(hours=7))


def fmt_meta_adset_dt_local(raw, tz=META_ADSET_TZ):
    s = str(raw or '').strip()
    if not s:
        return ''
    try:
        normalized = re.sub(r'([+-]\d{2})(\d{2})$', r'\1:\2', s.replace('Z', '+00:00'))
        if 'T' in normalized:
            dt = datetime.fromisoformat(normalized)
            if dt.tzinfo is not None:
                dt = dt.astimezone(tz)
            return dt.strftime('%Y-%m-%dT%H:%M')
    except Exception:
        pass
    m = re.match(r'(\d{4}-\d{2}-\d{2})T(\d{2}:\d{2})', s)
    if m:
        return m.group(1) + 'T' + m.group(2)
    return s


def normalize_meta_adset_dt_for_api(raw, tz=META_ADSET_TZ):
    s = str(raw or '').strip()
    if not s:
        return ''
    if re.search(r'[+-]\d{2}:?\d{2}$', s) or s.endswith('Z'):
        return re.sub(r'([+-]\d{2}):(\d{2})$', r'\1\2', s.replace('Z', '+0000'))
    if len(s) == 16 and 'T' in s:
        s = s + ':00'
    elif re.match(r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}$', s):
        s = s + ':00'
    offset = tz.utcoffset(datetime.now()) or timedelta(0)
    total_minutes = int(offset.total_seconds() // 60)
    sign = '+' if total_minutes >= 0 else '-'
    total_minutes = abs(total_minutes)
    return s + f'{sign}{total_minutes // 60:02d}{total_minutes % 60:02d}'


def _parse_csv_values(raw, upper=False):
    vals = [str(x).strip() for x in str(raw or '').split(',') if str(x).strip()]
    out = []
    for v in vals:
        vv = v.upper() if upper else v
        if vv not in out:
            out.append(vv)
    return out


def _parse_custom_audience_refs(raw):
    if not raw:
        return []
    try:
        parsed = json.loads(raw) if isinstance(raw, str) else raw
    except Exception:
        return []
    if not isinstance(parsed, list):
        parsed = [parsed]
    out = []
    for item in parsed:
        if isinstance(item, dict):
            iid = str(item.get('id') or '').strip()
        else:
            iid = str(item or '').strip()
        if iid:
            out.append({'id': iid})
    return out


def _normalize_facebook_placement_positions(vals):
    cleaned = [str(v).strip() for v in (vals or []) if str(v).strip()]
    fb_map = {'video_feeds': 'facebook_reels', 'suggested_video': 'facebook_reels'}
    normalized = []
    for v in cleaned:
        v = fb_map.get(v, v)
        if v in ('video_feeds', 'suggested_video'):
            continue
        if v not in normalized:
            normalized.append(v)
    return normalized


def build_adset_targeting_from_post(post):
    countries_raw = str(post.get('countries') or 'ID').strip()
    location_include_countries_raw = str(post.get('location_include_countries') or countries_raw or 'ID').strip()
    location_exclude_countries_raw = str(post.get('location_exclude_countries') or '').strip()
    location_include_regions_raw = str(post.get('location_include_regions') or '').strip()
    location_include_cities_raw = str(post.get('location_include_cities') or '').strip()
    location_exclude_regions_raw = str(post.get('location_exclude_regions') or '').strip()
    location_exclude_cities_raw = str(post.get('location_exclude_cities') or '').strip()
    languages_raw = str(post.get('languages') or '').strip()
    detailed_targeting_raw = str(post.get('detailed_targeting') or '').strip()
    gender = str(post.get('gender') or 'all').strip().lower()
    advantage = str(post.get('advantage') or '0').strip()
    placement_mode = str(post.get('placement_mode') or 'auto').strip().lower()
    placement_device_mode = str(post.get('placement_device_mode') or 'all').strip().lower()
    placement_platforms_raw = str(post.get('placement_platforms') or '').strip()
    placement_positions_raw = str(post.get('placement_positions') or '').strip()
    try:
        age_min = int(post.get('age_min') or 18)
    except Exception:
        age_min = 18
    try:
        age_max = int(post.get('age_max') or 65)
    except Exception:
        age_max = 65

    include_countries = _parse_csv_values(location_include_countries_raw or countries_raw or 'ID', upper=True) or ['ID']
    exclude_countries = _parse_csv_values(location_exclude_countries_raw, upper=True)
    include_regions = [{'key': v} for v in _parse_csv_values(location_include_regions_raw)]
    include_cities = [{'key': v} for v in _parse_csv_values(location_include_cities_raw)]
    exclude_regions = [{'key': v} for v in _parse_csv_values(location_exclude_regions_raw)]
    exclude_cities = [{'key': v} for v in _parse_csv_values(location_exclude_cities_raw)]

    if (include_regions or include_cities) and len(include_countries) <= 1:
        include_countries = []

    geo_locations = {}
    if include_countries:
        geo_locations['countries'] = include_countries
    if include_regions:
        geo_locations['regions'] = include_regions
    if include_cities:
        geo_locations['cities'] = include_cities

    targeting = {'geo_locations': geo_locations}
    age_min = max(13, age_min)
    age_max = max(age_min, min(65, age_max))
    if advantage == '1':
        adv_min = max(18, min(25, age_min))
        adv_range_max = max(adv_min, min(65, age_max))
        targeting['age_min'] = adv_min
        targeting['age_max'] = 65
        targeting['age_range'] = [adv_min, adv_range_max]
    else:
        targeting['age_min'] = age_min
        targeting['age_max'] = age_max

    excluded_geo_locations = {}
    if exclude_countries:
        excluded_geo_locations['countries'] = exclude_countries
    if exclude_regions:
        excluded_geo_locations['regions'] = exclude_regions
    if exclude_cities:
        excluded_geo_locations['cities'] = exclude_cities
    if excluded_geo_locations:
        targeting['excluded_geo_locations'] = excluded_geo_locations

    language_keys = []
    if languages_raw:
        try:
            parsed_lang = json.loads(languages_raw)
            if not isinstance(parsed_lang, list):
                parsed_lang = [parsed_lang]
        except Exception:
            parsed_lang = [x.strip() for x in languages_raw.split(',') if x.strip()]
        for lv in parsed_lang:
            sv = str(lv or '').strip()
            if sv.isdigit():
                iv = int(sv)
                if iv not in language_keys:
                    language_keys.append(iv)
    if language_keys:
        targeting['locales'] = language_keys

    flexible_spec = []
    if detailed_targeting_raw:
        try:
            parsed_dt = json.loads(detailed_targeting_raw)
        except Exception:
            parsed_dt = []
        if isinstance(parsed_dt, dict):
            parsed_groups = [parsed_dt.get('include') or [], parsed_dt.get('narrow') or []]
        elif isinstance(parsed_dt, list):
            parsed_groups = [parsed_dt]
        else:
            parsed_groups = []
        for group in parsed_groups:
            interests = []
            for item in (group or []):
                if isinstance(item, dict):
                    iid = str(item.get('id') or '').strip()
                    iname = str(item.get('name') or '').strip()
                else:
                    iid = str(item or '').strip()
                    iname = ''
                if not iid:
                    continue
                row = {'id': iid}
                if iname:
                    row['name'] = iname
                interests.append(row)
            if interests:
                flexible_spec.append({'interests': interests})
        if flexible_spec:
            targeting['flexible_spec'] = flexible_spec

    custom_audiences = _parse_custom_audience_refs(post.get('advantage_custom_audiences'))
    if custom_audiences:
        targeting['custom_audiences'] = custom_audiences
    excluded_custom_audiences = _parse_custom_audience_refs(post.get('excluded_custom_audiences'))
    if excluded_custom_audiences:
        targeting['excluded_custom_audiences'] = excluded_custom_audiences

    if gender == 'male':
        targeting['genders'] = [1]
    elif gender == 'female':
        targeting['genders'] = [2]

    if placement_mode == 'manual':
        try:
            parsed_platforms = json.loads(placement_platforms_raw) if placement_platforms_raw else []
            if not isinstance(parsed_platforms, list):
                parsed_platforms = []
        except Exception:
            parsed_platforms = []
        allowed_platforms = ['facebook', 'instagram', 'audience_network', 'messenger', 'threads']
        selected_platforms = [p for p in [str(x).strip().lower() for x in parsed_platforms] if p in allowed_platforms]
        if not selected_platforms:
            selected_platforms = ['facebook', 'instagram', 'audience_network', 'messenger']
        if 'threads' in selected_platforms and 'instagram' not in selected_platforms:
            selected_platforms.append('instagram')
        targeting['publisher_platforms'] = selected_platforms

        if placement_device_mode == 'mobile':
            targeting['device_platforms'] = ['mobile']
        elif placement_device_mode == 'desktop':
            targeting['device_platforms'] = ['desktop']
        else:
            targeting['device_platforms'] = ['mobile', 'desktop']

        try:
            parsed_positions = json.loads(placement_positions_raw) if placement_positions_raw else {}
            if not isinstance(parsed_positions, dict):
                parsed_positions = {}
        except Exception:
            parsed_positions = {}

        if 'threads' in selected_platforms:
            tvals = parsed_positions.get('threads') or []
            if 'threads_stream' in tvals:
                ivals = parsed_positions.get('instagram') or []
                if 'stream' not in ivals:
                    parsed_positions['instagram'] = list(ivals) + ['stream']

        pos_field_map = {
            'facebook': 'facebook_positions',
            'instagram': 'instagram_positions',
            'audience_network': 'audience_network_positions',
            'messenger': 'messenger_positions',
            'threads': 'threads_positions',
        }
        allowed_positions = {'messenger': ['story', 'sponsored_messages'], 'threads': ['threads_stream']}
        for p in selected_platforms:
            vals = parsed_positions.get(p) or []
            if not isinstance(vals, list):
                vals = []
            if p == 'facebook':
                vals = _normalize_facebook_placement_positions(vals)
            elif p in allowed_positions:
                vals = [v for v in [str(x).strip() for x in vals if str(x).strip()] if v in allowed_positions[p]]
            else:
                vals = [str(v).strip() for v in vals if str(v).strip()]
            if vals and p in pos_field_map:
                targeting[pos_field_map[p]] = vals

    if advantage == '1':
        targeting['targeting_automation'] = {'advantage_audience': 1}

    return targeting


def build_adset_targeting_for_audience_estimate(post):
    """Targeting spec for audience size widget — mirrors Ads Manager (excludes Advantage+ expansion)."""
    targeting = build_adset_targeting_from_post(post)
    targeting.pop('targeting_automation', None)
    return targeting


def _extract_meta_audience_size_bounds(row):
    if not isinstance(row, dict):
        return None, None
    for lo_key, hi_key in (
        ('estimate_mau_lower_bound', 'estimate_mau_upper_bound'),
        ('users_lower_bound', 'users_upper_bound'),
    ):
        lower = row.get(lo_key)
        upper = row.get(hi_key)
        if lower in (None, -1) or upper in (None, -1):
            continue
        try:
            lower = int(lower)
            upper = int(upper)
        except Exception:
            continue
        if lower >= 0 and upper >= 0:
            if upper < lower:
                upper = lower
            return lower, upper
    return None, None


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

class FacebookLanguageSuggestView(View):
    """AJAX endpoint suggest bahasa (adlocale) dari Meta Ads"""
    def dispatch(self, request, *args, **kwargs):
        if 'hris_admin' not in request.session:
            return redirect('admin_login')
        return super().dispatch(request, *args, **kwargs)

    def get(self, req):
        q = str(req.GET.get('q') or '').strip()
        selected_account = str(req.GET.get('selected_account') or '').strip()
        if not q or not selected_account:
            return JsonResponse({'results': []})
        try:
            rs = data_mysql().master_account_ads_by_id({'data_account': selected_account})
            acc = (rs or {}).get('data') if isinstance(rs, dict) else None
            token = str((acc or {}).get('access_token') or '').strip()
            if not token:
                return JsonResponse({'results': []})
            resp = requests.get('https://graph.facebook.com/v22.0/search', params={
                'type': 'adlocale', 'q': q, 'limit': 50, 'access_token': token,
            }, timeout=20)
            body = resp.json() if resp.text else {}
            rows = (body or {}).get('data') if isinstance(body, dict) else []
            results, seen = [], set()
            for r in (rows or []):
                key = str((r or {}).get('key') or (r or {}).get('id') or '').strip()
                name = str((r or {}).get('name') or '').strip()
                if not key:
                    continue
                d = f"{key}|{name}".lower()
                if d in seen:
                    continue
                seen.add(d)
                results.append({'id': key, 'text': (f'{name} (key: {key})' if name else f'key: {key}')})
            return JsonResponse({'results': results})
        except Exception:
            return JsonResponse({'results': []})

class FacebookLocationSuggestView(View):
    """AJAX endpoint suggest lokasi (negara/wilayah/kota) dari Meta Ads"""
    def dispatch(self, request, *args, **kwargs):
        if 'hris_admin' not in request.session:
            return redirect('admin_login')
        return super().dispatch(request, *args, **kwargs)

    def get(self, req):
        if str(req.GET.get('geocode') or '').strip() == '1':
            q = str(req.GET.get('q') or '').strip()
            if not q:
                return JsonResponse({'lat': None, 'lng': None})
            try:
                resp = requests.get('https://nominatim.openstreetmap.org/search', params={
                    'q': q,
                    'format': 'json',
                    'limit': 1,
                }, headers={'User-Agent': 'HRIS-AdsBuilder/1.0 (location-ui)'}, timeout=10)
                rows = resp.json() if resp.text else []
                if rows:
                    return JsonResponse({'lat': float(rows[0]['lat']), 'lng': float(rows[0]['lon'])})
            except Exception:
                pass
            return JsonResponse({'lat': None, 'lng': None})

        q = str(req.GET.get('q') or '').strip()
        selected_account = str(req.GET.get('selected_account') or '').strip()
        location_type = str(req.GET.get('location_type') or 'country').strip().lower()
        resolve_key = str(req.GET.get('resolve_key') or '').strip()
        if resolve_key and selected_account:
            try:
                rs = data_mysql().master_account_ads_by_id({'data_account': selected_account})
                acc = (rs or {}).get('data') if isinstance(rs, dict) else None
                token = str((acc or {}).get('access_token') or '').strip()
                if not token:
                    return JsonResponse({'results': []})
                resp = requests.get(
                    'https://graph.facebook.com/v22.0/search',
                    params={
                        'type': 'adgeolocation',
                        'location_ids': json.dumps([resolve_key]),
                        'access_token': token,
                    },
                    timeout=20,
                )
                body = resp.json() if resp.text else {}
                rows = (body or {}).get('data') if isinstance(body, dict) else []
                row = rows[0] if rows else None
                if isinstance(row, dict):
                    name = str(row.get('name') or '').strip()
                    key = str(row.get('key') or resolve_key).strip()
                    row_type = str(row.get('type') or location_type or 'region').strip().lower()
                    if name:
                        return JsonResponse({'results': [{'id': key, 'token': key, 'text': name, 'type': row_type}]})
            except Exception:
                pass
            return JsonResponse({'results': []})
        if len(q) < 1 or not selected_account:
            return JsonResponse({'results': []})
        type_map = {'country': 'country', 'region': 'region', 'city': 'city'}
        kind_labels = {
            'country': 'Negara',
            'region': 'Provinsi',
            'city': 'Kota',
            'subcity': 'Subdistrict',
            'neighborhood': 'Lingkungan',
            'zip': 'Kode Pos',
            'geo_market': 'Market',
            'electoral_district': 'Distrik',
        }
        search_types = ['country', 'region', 'city'] if location_type in ('all', 'any', '') else [type_map.get(location_type, 'country')]
        try:
            rs = data_mysql().master_account_ads_by_id({'data_account': selected_account})
            acc = (rs or {}).get('data') if isinstance(rs, dict) else None
            token = str((acc or {}).get('access_token') or '').strip()
            if not token:
                return JsonResponse({'results': []})
            results = []
            seen = set()
            for api_loc in search_types:
                resp = requests.get('https://graph.facebook.com/v22.0/search', params={
                    'type': 'adgeolocation',
                    'q': q,
                    'location_types': json.dumps([api_loc]),
                    'limit': 30,
                    'access_token': token,
                }, timeout=20)
                body = resp.json() if resp.text else {}
                rows = (body or {}).get('data') if isinstance(body, dict) else []
                for r in (rows or []):
                    key = str((r or {}).get('key') or '').strip()
                    name = str((r or {}).get('name') or '').strip()
                    cc = str((r or {}).get('country_code') or '').strip().upper()
                    region = str((r or {}).get('region') or '').strip()
                    row_type = str((r or {}).get('type') or api_loc).strip().lower()
                    token_val = cc if api_loc == 'country' and cc else key
                    if not token_val:
                        continue
                    if api_loc != 'country' and len(name) <= 3 and name.upper() == cc:
                        continue
                    dedup = f'{token_val}|{name}|{cc}|{region}|{row_type}'.lower()
                    if dedup in seen:
                        continue
                    seen.add(dedup)
                    parts = [name]
                    if region and region.lower() != name.lower():
                        parts.append(region)
                    if cc:
                        parts.append(cc)
                    display = ', '.join(dict.fromkeys([p for p in parts if p]))
                    kind = kind_labels.get(row_type) or kind_labels.get(api_loc) or 'Lokasi'
                    results.append({
                        'id': token_val,
                        'token': token_val,
                        'text': display,
                        'name': name,
                        'short_name': name.split(',')[0].strip() if name else '',
                        'kind': kind,
                        'location_type': row_type if row_type else api_loc,
                        'country_code': cc,
                        'region': region,
                        'key': key or token_val,
                    })
            return JsonResponse({'results': results[:60]})
        except Exception:
            return JsonResponse({'results': []})

class FacebookLanguageSuggestView(View):
    def dispatch(self, request, *args, **kwargs):
        if 'hris_admin' not in request.session:
            return redirect('admin_login')
        return super().dispatch(request, *args, **kwargs)

    def get(self, req):
        q = str(req.GET.get('q') or '').strip().lower()
        selected_account = str(req.GET.get('selected_account') or '').strip()

        fallback_locales = [
            ('42', 'Bahasa Indonesia'), ('6', 'English (All)'), ('1002', 'English (US)'),
            ('1001', 'English (UK)'), ('3', 'Spanish'), ('10', 'Arabic'), ('35', 'Malay'),
            ('52', 'Thai'), ('53', 'Vietnamese'), ('7', 'French'), ('5', 'German'), ('8', 'Italian'),
            ('9', 'Portuguese'), ('11', 'Japanese'), ('12', 'Korean'), ('13', 'Chinese (Simplified)'),
            ('14', 'Chinese (Traditional)')
        ]

        def _filter_fallback(query_text):
            rows = []
            for key, name in fallback_locales:
                raw = f"{key} {name}".lower()
                if query_text and query_text not in raw:
                    continue
                rows.append({'id': key, 'text': f'{name} (key: {key})'})
            return rows[:50]

        token = ''
        try:
            if selected_account:
                rs = data_mysql().master_account_ads_by_id({'data_account': selected_account})
                acc = (rs or {}).get('data') if isinstance(rs, dict) else None
                token = str((acc or {}).get('access_token') or '').strip()
            if not token:
                rs_all = data_mysql().master_account_ads()
                first = ((rs_all or {}).get('data') or [{}])[0]
                token = str((first or {}).get('access_token') or '').strip()
        except Exception:
            token = ''

        if not token:
            return JsonResponse({'results': _filter_fallback(q)})

        try:
            params = {'type': 'adlocale', 'q': (q or 'english'), 'limit': 50, 'access_token': token}
            resp = requests.get('https://graph.facebook.com/v22.0/search', params=params, timeout=20)
            body = resp.json() if resp.text else {}
            rows = (body or {}).get('data') if isinstance(body, dict) else []
            results = []
            seen = set()
            for r in (rows or []):
                key = str((r or {}).get('key') or (r or {}).get('id') or '').strip()
                name = str((r or {}).get('name') or '').strip()
                if not key:
                    continue
                dedup = f"{key}|{name}".lower()
                if dedup in seen:
                    continue
                seen.add(dedup)
                results.append({'id': key, 'text': (f'{name} (key: {key})' if name else f'key: {key}')})
            if results:
                if q:
                    results = [x for x in results if q in str(x.get('text', '')).lower()]
                return JsonResponse({'results': results[:50]})
            return JsonResponse({'results': _filter_fallback(q)})
        except Exception:
            return JsonResponse({'results': _filter_fallback(q)})

class FacebookDetailedTargetingSuggestView(View):
    def dispatch(self, request, *args, **kwargs):
        if 'hris_admin' not in request.session:
            return redirect('admin_login')
        return super().dispatch(request, *args, **kwargs)

    @staticmethod
    def _fmt_size_num(val):
        try:
            return f"{int(val):,}".replace(',', '.')
        except (TypeError, ValueError):
            return ''

    @classmethod
    def _size_label(cls, row):
        lo = (row or {}).get('audience_size_lower_bound')
        hi = (row or {}).get('audience_size_upper_bound')
        single = (row or {}).get('audience_size') or (row or {}).get('coverage')
        if lo and hi:
            return f"{cls._fmt_size_num(lo)} - {cls._fmt_size_num(hi)}"
        if single:
            return cls._fmt_size_num(single)
        return ''

    @staticmethod
    def _category_label(search_type, path, row_type):
        type_map = {
            'adinterest': 'Minat',
            'adeducationschool': 'Pendidikan',
            'adeducationmajor': 'Pendidikan',
            'adworkemployer': 'Pekerjaan',
            'adworkposition': 'Pekerjaan',
        }
        class_map = {
            'interests': 'Minat',
            'behaviors': 'Perilaku',
            'demographics': 'Demografi',
            'life_events': 'Peristiwa Penting',
            'industries': 'Industri',
            'income': 'Financial',
            'family_statuses': 'Orang Tua',
            'relationship_statuses': 'Hubungan',
            'education_statuses': 'Pendidikan',
            'college_years': 'Pendidikan',
        }
        if search_type in type_map:
            return type_map[search_type]
        rt = str(row_type or '').strip().lower()
        if rt in class_map:
            return class_map[rt]
        path = path or []
        if path:
            return str(path[0])
        return 'Minat'

    def get(self, req):
        q = str(req.GET.get('q') or '').strip()
        selected_account = str(req.GET.get('selected_account') or '').strip()
        search_type = str(req.GET.get('search_type') or 'adinterest').strip()
        if len(q) < 2 or not selected_account:
            return JsonResponse({'results': []})
        try:
            rs = data_mysql().master_account_ads_by_id({'data_account': selected_account})
            acc = (rs or {}).get('data') if isinstance(rs, dict) else None
            token = str((acc or {}).get('access_token') or '').strip()
            if not token:
                return JsonResponse({'results': []})
            params = {
                'type': search_type,
                'q': q,
                'limit': 25,
                'access_token': token,
            }
            resp = requests.get('https://graph.facebook.com/v22.0/search', params=params, timeout=20)
            body = resp.json() if resp.text else {}
            rows = (body or {}).get('data') if isinstance(body, dict) else []
            results = []
            seen = set()
            for r in (rows or []):
                rid = str((r or {}).get('id') or '').strip()
                name = str((r or {}).get('name') or '').strip()
                if not rid or not name:
                    continue
                key = f"{rid}|{name}".lower()
                if key in seen:
                    continue
                seen.add(key)
                path = (r or {}).get('path') or []
                category = self._category_label(search_type, path, (r or {}).get('type'))
                size_label = self._size_label(r)
                subtext = str((r or {}).get('subtext') or '').strip()
                display = name + (f" ({subtext})" if subtext else '')
                results.append({
                    'id': rid,
                    'text': display,
                    'name': name,
                    'category': category,
                    'size_label': size_label,
                    'audience_size_lower': (r or {}).get('audience_size_lower_bound') or (r or {}).get('audience_size') or (r or {}).get('coverage'),
                    'audience_size_upper': (r or {}).get('audience_size_upper_bound'),
                    'path': path,
                    'type': search_type,
                })
            return JsonResponse({'results': results})
        except Exception:
            return JsonResponse({'results': []})


class FacebookDetailedTargetingBrowseView(View):
    def dispatch(self, request, *args, **kwargs):
        if 'hris_admin' not in request.session:
            return redirect('admin_login')
        return super().dispatch(request, *args, **kwargs)

    @staticmethod
    def _fmt_size_num(val):
        try:
            return f"{int(val):,}".replace(',', '.')
        except (TypeError, ValueError):
            return ''

    @classmethod
    def _size_label(cls, row):
        lo = (row or {}).get('audience_size_lower_bound')
        hi = (row or {}).get('audience_size_upper_bound')
        single = (row or {}).get('audience_size')
        if lo and hi:
            return f"{cls._fmt_size_num(lo)} - {cls._fmt_size_num(hi)}"
        if single:
            return cls._fmt_size_num(single)
        return ''

    @staticmethod
    def _category_label(class_name, path, row_type):
        class_map = {
            'interests': 'Minat',
            'behaviors': 'Perilaku',
            'demographics': 'Demografi',
            'life_events': 'Peristiwa Penting',
            'industries': 'Industri',
            'income': 'Financial',
            'net_worth': 'Financial',
            'family_statuses': 'Orang Tua',
            'moms': 'Orang Tua',
            'relationship_statuses': 'Hubungan',
            'interested_in': 'Hubungan',
            'education_statuses': 'Pendidikan',
            'college_years': 'Pendidikan',
        }
        rt = str(row_type or class_name or '').strip().lower()
        if rt in class_map:
            return class_map[rt]
        path = path or []
        if path:
            return str(path[0])
        return class_map.get(str(class_name or '').lower(), 'Minat')

    def get(self, req):
        selected_account = str(req.GET.get('selected_account') or '').strip()
        class_name = str(req.GET.get('class') or '').strip()
        group_path = str(req.GET.get('group_path') or '').strip()
        mode = str(req.GET.get('mode') or 'items').strip().lower()
        if not selected_account or not class_name:
            return JsonResponse({'results': []})
        try:
            rs = data_mysql().master_account_ads_by_id({'data_account': selected_account})
            acc = (rs or {}).get('data') if isinstance(rs, dict) else None
            token = str((acc or {}).get('access_token') or '').strip()
            if not token:
                return JsonResponse({'results': []})
            resp = requests.get('https://graph.facebook.com/v22.0/search', params={
                'type': 'adTargetingCategory',
                'class': class_name,
                'limit': 1000,
                'access_token': token,
            }, timeout=25)
            body = resp.json() if resp.text else {}
            rows = (body or {}).get('data') if isinstance(body, dict) else []
            if mode == 'groups':
                groups, seen = [], set()
                for r in (rows or []):
                    path = (r or {}).get('path') or []
                    if not path:
                        continue
                    g = str(path[0] or '').strip()
                    if not g or g.lower() in seen:
                        continue
                    seen.add(g.lower())
                    groups.append({'id': g, 'name': g, 'type': 'group'})
                groups.sort(key=lambda x: x['name'].lower())
                return JsonResponse({'results': groups})
            results, seen = [], set()
            for r in (rows or []):
                rid = str((r or {}).get('id') or '').strip()
                name = str((r or {}).get('name') or '').strip()
                if not rid or not name:
                    continue
                path = (r or {}).get('path') or []
                if group_path:
                    if not path or str(path[0]) != group_path:
                        continue
                key = rid.lower()
                if key in seen:
                    continue
                seen.add(key)
                category = self._category_label(class_name, path, (r or {}).get('type'))
                size_label = self._size_label(r)
                results.append({
                    'id': rid,
                    'text': name,
                    'name': name,
                    'category': category,
                    'size_label': size_label,
                    'audience_size_lower': (r or {}).get('audience_size_lower_bound') or (r or {}).get('audience_size'),
                    'audience_size_upper': (r or {}).get('audience_size_upper_bound'),
                    'path': path,
                    'description': str((r or {}).get('description') or '').strip(),
                    'type': str((r or {}).get('type') or class_name),
                })
            results.sort(key=lambda x: x['name'].lower())
            return JsonResponse({'results': results[:120]})
        except Exception:
            return JsonResponse({'results': []})

class FacebookCustomAudienceSuggestView(View):
    """AJAX endpoint daftar audiens kustom / serupa dari akun Meta Ads."""
    def dispatch(self, request, *args, **kwargs):
        if 'hris_admin' not in request.session:
            return redirect('admin_login')
        return super().dispatch(request, *args, **kwargs)

    def get(self, req):
        q = str(req.GET.get('q') or '').strip().lower()
        selected_account = str(req.GET.get('selected_account') or '').strip()
        tab = str(req.GET.get('tab') or 'all').strip().lower()
        if not selected_account or not q:
            return JsonResponse({'results': []})
        try:
            rs = data_mysql().master_account_ads_by_id({'data_account': selected_account})
            acc = (rs or {}).get('data') if isinstance(rs, dict) else None
            token = str((acc or {}).get('access_token') or '').strip()
            if not token:
                return JsonResponse({'results': []})
            ad_act = str((acc or {}).get('account_ads_id') or (acc or {}).get('account_id') or '').strip()
            if not ad_act.lower().startswith('act_'):
                ad_act = 'act_' + ad_act.replace('act_', '')
            resp = requests.get(
                f'https://graph.facebook.com/v22.0/{ad_act}/customaudiences',
                params={
                    'fields': 'id,name,subtype,description',
                    'limit': 100,
                    'access_token': token,
                },
                timeout=20,
            )
            body = resp.json() if resp.text else {}
            rows = (body or {}).get('data') if isinstance(body, dict) else []
            results, seen = [], set()
            for r in (rows or []):
                rid = str((r or {}).get('id') or '').strip()
                name = str((r or {}).get('name') or '').strip()
                subtype = str((r or {}).get('subtype') or '').strip().upper()
                if not rid or not name:
                    continue
                is_lookalike = subtype == 'LOOKALIKE'
                if tab == 'lookalike' and not is_lookalike:
                    continue
                if tab == 'custom' and is_lookalike:
                    continue
                hay = f'{name} {rid} {subtype}'.lower()
                if q not in hay:
                    continue
                if rid.lower() in seen:
                    continue
                seen.add(rid.lower())
                suffix = 'Audiens serupa' if is_lookalike else 'Audiens kustom'
                results.append({'id': rid, 'text': f'{name} • {suffix}', 'subtype': 'lookalike' if is_lookalike else 'custom'})
            return JsonResponse({'results': results[:50]})
        except Exception:
            return JsonResponse({'results': []})

def _facebook_page_access_token(page_id, user_token):
    page_id = str(page_id or '').strip()
    user_token = str(user_token or '').strip()
    if not page_id or not user_token:
        return user_token
    try:
        resp = requests.get(
            f'https://graph.facebook.com/v22.0/{page_id}',
            params={'fields': 'access_token', 'access_token': user_token},
            timeout=20,
        )
        data = (resp.json() if resp.text else {}) or {}
        return str(data.get('access_token') or user_token).strip() or user_token
    except Exception:
        return user_token

def _facebook_page_post_ad_ready(post_id, page_id, tokens):
    """Return (ok, message, meta) for using object_story_id in dev-mode ads."""
    post_id = str(post_id or '').strip()
    page_id = str(page_id or '').strip()
    if not post_id:
        return False, 'Existing Post ID wajib diisi.', {}
    if '_' not in post_id:
        return False, 'Existing Post ID harus format Facebook Page Post (pageid_postid), bukan ID Instagram/media.', {}
    post_page_id = post_id.split('_', 1)[0].strip()
    if page_id and post_page_id and post_page_id != page_id:
        return False, f'Postingan ({post_id}) bukan milik halaman yang dipilih ({page_id}).', {}
    meta = {}
    lookup_id = post_id
    for tok in [t for t in (tokens or []) if t]:
        try:
            resp = requests.get(
                f'https://graph.facebook.com/v22.0/{lookup_id}',
                params={
                    'fields': 'id,is_published,is_eligible_for_promotion,promotable_id,application{id,name},status_type',
                    'access_token': tok,
                },
                timeout=20,
            )
            body = (resp.json() if resp.text else {}) or {}
            if isinstance(body, dict) and body.get('error'):
                continue
            meta = body if isinstance(body, dict) else {}
            break
        except Exception:
            continue
    if not meta.get('id'):
        return False, 'Postingan tidak ditemukan atau token tidak punya akses ke posting ini.', meta
    is_published = meta.get('is_published')
    if is_published is False:
        return False, (
            'Postingan ini belum dipublish di Halaman Facebook. '
            'Mode development Meta hanya menerima posting PUBLIK yang sudah tayang di halaman. '
            'Buat posting langsung di facebook.com/Page Anda, lalu pilih di sini.'
        ), meta
    if meta.get('is_eligible_for_promotion') is False:
        return False, (
            'Postingan ini tidak eligible untuk iklan (is_eligible_for_promotion=false). '
            'Pilih posting lain yang dibuat langsung di Halaman Facebook, bukan via app/iklan sebelumnya.'
        ), meta
    usable_id = str(meta.get('promotable_id') or meta.get('id') or post_id).strip()
    if '_' not in usable_id:
        return False, 'ID posting tidak valid untuk iklan. Pilih posting Facebook Page (format pageid_postid).', meta
    meta['usable_object_story_id'] = usable_id
    return True, '', meta

def _facebook_graph_data_rows(url, params, access_token, timeout=20):
    access_token = str(access_token or '').strip()
    if not access_token:
        return []
    try:
        resp = requests.get(url, params={**(params or {}), 'access_token': access_token}, timeout=timeout)
        data = (resp.json() if resp.text else {}) or {}
        if isinstance(data, dict) and data.get('error'):
            return []
        if isinstance(data, dict) and data.get('data') is not None:
            return [x for x in (data.get('data') or []) if isinstance(x, dict)]
        if isinstance(data, dict) and data.get('id'):
            return [data]
        return []
    except Exception:
        return []

def _facebook_normalize_ig_node(ig):
    if not isinstance(ig, dict):
        return {}
    out = dict(ig)
    if not out.get('profile_picture_url'):
        pic = out.get('profile_pic')
        if isinstance(pic, dict):
            pic = str(((pic.get('data') or {}).get('url') or pic.get('url') or '')).strip()
        out['profile_picture_url'] = str(pic or '').strip()
    graph_id = str(out.get('id') or '').strip()
    legacy_id = str(out.get('legacy_instagram_user_id') or '').strip()
    if graph_id:
        out['graph_id'] = graph_id
    if legacy_id:
        out['legacy_instagram_user_id'] = legacy_id
    # Ads API instagram_user_id expects Graph IG account id (e.g. 178414...), not legacy id.
    if graph_id:
        out['id'] = graph_id
    elif legacy_id:
        out['id'] = legacy_id
    return out

def _facebook_fetch_ig_profile(iid, tokens, graph_id=''):
    ids = []
    for raw in (iid, graph_id):
        val = str(raw or '').strip()
        if val and val not in ids:
            ids.append(val)
    fields = 'id,username,name,profile_picture_url,legacy_instagram_user_id'
    for token in tokens:
        token = str(token or '').strip()
        if not token:
            continue
        for fid in ids:
            try:
                resp = requests.get(
                    f'https://graph.facebook.com/v22.0/{fid}',
                    params={'fields': fields, 'access_token': token},
                    timeout=20,
                )
                detail = (resp.json() if resp.text else {}) or {}
                if isinstance(detail, dict) and detail.get('id') and not detail.get('error'):
                    return detail
            except Exception:
                pass
    return {}

def _facebook_resolve_instagram_user_id(ig_id, tokens, page_id='', graph_id='', ad_account_id=''):
    """Resolve instagram_user_id for object_story_spec (Graph API IG account id linked to page)."""
    ig_id = str(ig_id or '').strip()
    graph_id = str(graph_id or '').strip()
    page_id = str(page_id or '').strip()
    ad_account_id = str(ad_account_id or '').strip()
    token_list = []
    for t in (tokens if isinstance(tokens, (list, tuple)) else [tokens]):
        t = str(t or '').strip()
        if t and t not in token_list:
            token_list.append(t)
    if not token_list:
        return ''

    wanted = {x for x in [ig_id, graph_id] if x}

    def _graph_id_from_raw(raw):
        if not isinstance(raw, dict):
            return ''
        gid = str(raw.get('id') or '').strip()
        legacy = str(raw.get('legacy_instagram_user_id') or '').strip()
        if gid and (not legacy or gid != legacy):
            return gid
        if gid:
            return gid
        return ''

    def _matches(raw):
        if not wanted:
            return True
        if not isinstance(raw, dict):
            return False
        gid = _graph_id_from_raw(raw)
        legacy = str(raw.get('legacy_instagram_user_id') or '').strip()
        norm_id = str(_facebook_normalize_ig_node(raw).get('id') or '').strip()
        return bool(wanted.intersection({gid, legacy, norm_id}))

    if page_id:
        page_token = _facebook_page_access_token(page_id, token_list[0])
        lookup_tokens = [t for t in [page_token, *token_list] if t]
        for access in lookup_tokens:
            for ig in _facebook_page_instagram_candidates(
                page_id,
                token_list[0],
                ensure_pbia=False,
                page_name='',
                ad_account_id=ad_account_id,
            ):
                if not _matches(ig):
                    continue
                resolved = _graph_id_from_raw(ig)
                if resolved:
                    return resolved

    if ad_account_id:
        for ig in _facebook_ad_account_instagram_candidates(ad_account_id, token_list[0]):
            if not _matches(ig):
                continue
            resolved = _graph_id_from_raw(ig)
            if resolved:
                return resolved

    if ig_id or graph_id:
        detail = _facebook_fetch_ig_profile(ig_id or graph_id, token_list, graph_id)
        if isinstance(detail, dict) and detail:
            resolved = str(detail.get('id') or '').strip()
            if resolved:
                return resolved

    if ig_id.isdigit() and len(ig_id) >= 15:
        return ig_id
    return ''

def _facebook_ig_actor_row(ig, page_name='', access_token='', extra_tokens=None):
    ig = _facebook_normalize_ig_node(ig)
    if not isinstance(ig, dict):
        return None
    iid = str((ig or {}).get('id') or '').strip()
    if not iid:
        return None
    uname = str((ig or {}).get('username') or '').strip()
    iname = str((ig or {}).get('name') or '').strip()
    pic = str((ig or {}).get('profile_picture_url') or '').strip()
    tokens = []
    for t in [access_token, *(extra_tokens or [])]:
        t = str(t or '').strip()
        if t and t not in tokens:
            tokens.append(t)
    if tokens and (not uname or not iname or not pic):
        detail = _facebook_fetch_ig_profile(iid, tokens, str((ig or {}).get('graph_id') or '').strip())
        if detail:
            uname = uname or str(detail.get('username') or '').strip()
            iname = iname or str(detail.get('name') or '').strip()
            pic = pic or str(detail.get('profile_picture_url') or '').strip()
            iid = str(detail.get('id') or detail.get('legacy_instagram_user_id') or iid).strip() or iid
    page_name = str(page_name or '').strip()
    label = uname if uname else (iname if iname and not iname.isdigit() else '')
    if not label:
        label = f'Instagram {page_name}' if page_name else 'Profil Instagram'
    return {
        'id': iid,
        'username': uname,
        'name': iname or uname or label,
        'label': label,
        'display_name': label,
        'picture_url': pic,
        'page_name': page_name,
    }

def _facebook_ig_has_username(ig):
    ig = _facebook_normalize_ig_node(ig)
    return bool(str((ig or {}).get('username') or '').strip())

def _facebook_enrich_ig_node(ig, access_token='', page_name='', extra_tokens=None):
    ig = _facebook_normalize_ig_node(ig)
    if not ig.get('id'):
        return ig
    if _facebook_ig_has_username(ig):
        return ig
    row = _facebook_ig_actor_row(ig, page_name, access_token, extra_tokens=extra_tokens)
    if not row:
        return ig
    ig = dict(ig)
    if row.get('username'):
        ig['username'] = row['username']
    if row.get('name'):
        ig['name'] = row['name']
    if row.get('picture_url'):
        ig['profile_picture_url'] = row['picture_url']
    if row.get('label'):
        ig['label'] = row['label']
        ig['display_name'] = row['label']
    if row.get('id'):
        ig['id'] = row['id']
    return ig

def _facebook_merge_ig_candidates(*buckets):
    merged, seen = [], set()
    for bucket in buckets:
        for ig in (bucket or []):
            ig = _facebook_normalize_ig_node(ig)
            iid = str((ig or {}).get('id') or '').strip()
            if not iid or iid in seen:
                continue
            seen.add(iid)
            merged.append(ig)
    merged.sort(key=lambda ig: (0 if _facebook_ig_has_username(ig) else 1))
    return merged

def _facebook_page_instagram_candidates(page_id, token, ensure_pbia=False, page_name='', ad_account_id=''):
    page_id = str(page_id or '').strip()
    token = str(token or '').strip()
    page_name = str(page_name or '').strip()
    if not page_id or not token:
        return []
    page_token = _facebook_page_access_token(page_id, token)
    access_tokens = [t for t in [page_token, token] if t]
    linked, edge_named, edge_other, pbia, ad_named = [], [], [], [], []
    seen = set()

    def append_bucket(bucket, ig):
        ig = _facebook_enrich_ig_node(ig, page_token or token, page_name, extra_tokens=access_tokens)
        iid = str((ig or {}).get('id') or '').strip()
        if not iid or iid in seen:
            return
        seen.add(iid)
        bucket.append(ig)

    page_fields = 'name,instagram_business_account{id,username,name,profile_picture_url,legacy_instagram_user_id},connected_instagram_account{id,username,name,profile_picture_url,legacy_instagram_user_id}'
    page_data = {}
    for access in access_tokens:
        try:
            page_resp = requests.get(
                f'https://graph.facebook.com/v22.0/{page_id}',
                params={'fields': page_fields, 'access_token': access},
                timeout=20,
            )
            data = (page_resp.json() if page_resp.text else {}) or {}
            if isinstance(data, dict) and not data.get('error'):
                page_data = data
                if not page_name:
                    page_name = str(data.get('name') or '').strip()
                break
        except Exception:
            pass
    if isinstance(page_data, dict):
        for key in ('instagram_business_account', 'connected_instagram_account'):
            ig = page_data.get(key)
            if ig:
                append_bucket(linked, ig)

    access = page_token or token
    for ig in _facebook_graph_data_rows(
        f'https://graph.facebook.com/v22.0/{page_id}/instagram_accounts',
        {'fields': 'id,username,name,profile_picture_url,legacy_instagram_user_id', 'limit': 50},
        access,
    ):
        if _facebook_ig_has_username(ig):
            append_bucket(edge_named, ig)
        else:
            append_bucket(edge_other, ig)

    for ig in _facebook_graph_data_rows(
        f'https://graph.facebook.com/v22.0/{page_id}/page_backed_instagram_accounts',
        {'fields': 'id,username,name,profile_pic,legacy_instagram_user_id', 'limit': 50},
        access,
    ):
        append_bucket(pbia, ig)

    if ad_account_id:
        for ig in _facebook_ad_account_instagram_candidates(ad_account_id, token):
            if _facebook_ig_has_username(ig):
                append_bucket(ad_named, ig)

    candidates = _facebook_merge_ig_candidates(linked, edge_named, ad_named, edge_other, pbia)

    if not candidates and ensure_pbia:
        try:
            create_resp = requests.post(
                f'https://graph.facebook.com/v22.0/{page_id}/page_backed_instagram_accounts',
                params={'access_token': access},
                timeout=20,
            )
            created = (create_resp.json() if create_resp.text else {}) or {}
            if isinstance(created, dict) and created.get('id'):
                append_bucket(pbia, created)
                candidates = _facebook_merge_ig_candidates(linked, edge_named, ad_named, edge_other, pbia)
        except Exception:
            pass
    return candidates

def _facebook_ad_account_instagram_candidates(account_id, token):
    real_id = str(account_id or '').replace('act_', '').strip()
    token = str(token or '').strip()
    if not real_id or not token:
        return []
    rows = _facebook_graph_data_rows(
        f'https://graph.facebook.com/v22.0/act_{real_id}/instagram_accounts',
        {'fields': 'id,username,name,profile_picture_url,legacy_instagram_user_id', 'limit': 200},
        token,
    )
    return [_facebook_normalize_ig_node(x) for x in rows]

def _facebook_instagram_suggest_results(token, selected_account='', page_id='', q='', ensure_pbia=False):
    q = str(q or '').strip().lower()
    page_id = str(page_id or '').strip()
    token = str(token or '').strip()
    results, seen = [], set()

    def add_row(ig, page_name='', source='', access_token=''):
        row = _facebook_ig_actor_row(ig, page_name, access_token or token)
        if not row:
            return
        raw = f"{row.get('id','')} {row.get('username','')} {row.get('name','')} {row.get('label','')} {page_name}".lower()
        if q and q not in raw:
            return
        key = f"ig|{row['id']}".lower()
        if key in seen:
            return
        seen.add(key)
        display = row.get('display_name') or row.get('label') or 'Profil Instagram'
        text = display
        meta = source or (f'Tertaut ke {page_name}' if page_name else 'Profil Instagram')
        if page_name and display == f'Instagram {page_name}':
            meta = f'Page-backed account · {page_name}'
        results.append({'id': row['id'], 'text': text, 'meta': meta, **row})

    page_name_cache = {}

    def page_name_for(pid):
        pid = str(pid or '').strip()
        if not pid:
            return ''
        if pid in page_name_cache:
            return page_name_cache[pid]
        name = pid
        try:
            page_token = _facebook_page_access_token(pid, token)
            resp = requests.get(
                f'https://graph.facebook.com/v22.0/{pid}',
                params={'fields': 'name', 'access_token': page_token or token},
                timeout=20,
            )
            data = (resp.json() if resp.text else {}) or {}
            if isinstance(data, dict) and not data.get('error'):
                name = str(data.get('name') or pid).strip() or pid
        except Exception:
            pass
        page_name_cache[pid] = name
        return name

    if page_id:
        pname = page_name_for(page_id)
        page_token = _facebook_page_access_token(page_id, token)
        for ig in _facebook_page_instagram_candidates(page_id, token, ensure_pbia=ensure_pbia, page_name=pname, ad_account_id=selected_account):
            add_row(ig, pname, 'Profil Instagram untuk Halaman ini', page_token)

    try:
        pages_resp = requests.get(
            'https://graph.facebook.com/v22.0/me/accounts',
            params={'fields': 'id,name,access_token', 'limit': 200, 'access_token': token},
            timeout=20,
        )
        pages = ((pages_resp.json() if pages_resp.text else {}) or {}).get('data') or []
    except Exception:
        pages = []

    for p in pages:
        pid = str((p or {}).get('id') or '').strip()
        if not pid or (page_id and pid != page_id):
            continue
        pname = str((p or {}).get('name') or page_name_for(pid)).strip()
        page_token = str((p or {}).get('access_token') or '').strip() or _facebook_page_access_token(pid, token)
        for ig in _facebook_page_instagram_candidates(pid, token, ensure_pbia=False, page_name=pname, ad_account_id=selected_account):
            add_row(ig, pname, 'Profil Instagram untuk Halaman ini', page_token or token)

    for ig in _facebook_ad_account_instagram_candidates(selected_account, token):
        add_row(ig, '', 'Profil Instagram pada akun iklan')

    return results[:50]

class FacebookIdentitySuggestView(View):
    def dispatch(self, request, *args, **kwargs):
        if 'hris_admin' not in request.session:
            return redirect('admin_login')
        return super().dispatch(request, *args, **kwargs)

    def get(self, req):
        q = str(req.GET.get('q') or '').strip().lower()
        selected_account = str(req.GET.get('selected_account') or '').strip()
        identity_type = str(req.GET.get('identity_type') or 'page').strip().lower()
        page_id = str(req.GET.get('page_id') or '').strip()
        if not selected_account:
            return JsonResponse({'results': []})
        try:
            rs = data_mysql().master_account_ads_by_id({'data_account': selected_account})
            acc = (rs or {}).get('data') if isinstance(rs, dict) else None
            token = str((acc or {}).get('access_token') or '').strip()
            if not token:
                return JsonResponse({'results': []})
            if identity_type == 'instagram':
                ensure_pbia = str(req.GET.get('ensure_pbia') or '').strip().lower() in ('1', 'true', 'yes')
                if not ensure_pbia and page_id and not q:
                    ensure_pbia = True
                results = _facebook_instagram_suggest_results(
                    token,
                    selected_account=selected_account,
                    page_id=page_id,
                    q=q,
                    ensure_pbia=ensure_pbia,
                )
                return JsonResponse({'results': results})
            resp = requests.get('https://graph.facebook.com/v22.0/me/accounts', params={'fields': 'id,name,instagram_business_account{id,username,name},connected_instagram_account{id,username,name}', 'limit': 200, 'access_token': token}, timeout=20)
            rows = ((resp.json() if resp.text else {}) or {}).get('data') or []
            results, seen = [], set()
            for p in rows:
                pid, pname = str((p or {}).get('id') or '').strip(), str((p or {}).get('name') or '').strip()
                if identity_type == 'instagram':
                    continue
                raw = f"{pid} {pname}".lower()
                if q and q not in raw:
                    continue
                key = f"pg|{pid}".lower()
                if key in seen or not pid:
                    continue
                seen.add(key); results.append({'id': pid, 'text': f'{pname} (ID: {pid})'})
            return JsonResponse({'results': results[:50]})
        except Exception:
            return JsonResponse({'results': []})

class FacebookPageMessagingAssetsView(View):
    def dispatch(self, request, *args, **kwargs):
        if 'hris_admin' not in request.session:
            return redirect('admin_login')
        return super().dispatch(request, *args, **kwargs)

    def _token(self, selected_account):
        rs = data_mysql().master_account_ads_by_id({'data_account': selected_account})
        acc = (rs or {}).get('data') if isinstance(rs, dict) else None
        return str((acc or {}).get('access_token') or '').strip()

    def _ig_row(self, ig, page_name, access_token='', extra_tokens=None):
        return _facebook_ig_actor_row(ig, page_name, access_token, extra_tokens=extra_tokens)

    def _page_picture_url(self, page_data):
        return str((((page_data or {}).get('picture') or {}).get('data') or {}).get('url') or '').strip()

    def _page_assets(self, page_data, access_token='', page_id='', token='', ensure_pbia=False, selected_account=''):
        page_name = str((page_data or {}).get('name') or '').strip()
        pic = self._page_picture_url(page_data)
        page_token = str(access_token or '').strip()
        extra_tokens = [t for t in [page_token, token] if t]
        pid = page_id or str((page_data or {}).get('id') or '').strip()
        ig_candidates = _facebook_page_instagram_candidates(
            pid,
            token,
            ensure_pbia=ensure_pbia,
            page_name=page_name,
            ad_account_id=selected_account,
        ) if pid and token else []
        if not ig_candidates:
            ig = self._ig_row((page_data or {}).get('instagram_business_account'), page_name, page_token, extra_tokens=extra_tokens)
            if not ig:
                ig = self._ig_row((page_data or {}).get('connected_instagram_account'), page_name, page_token, extra_tokens=extra_tokens)
        else:
            ig = self._ig_row(ig_candidates[0], page_name, page_token, extra_tokens=extra_tokens)
        ig_list = []
        seen = set()
        for raw in ig_candidates:
            row = self._ig_row(raw, page_name, page_token, extra_tokens=extra_tokens)
            if row and row.get('id') and row['id'] not in seen:
                seen.add(row['id'])
                ig_list.append(row)
        if ig and ig.get('id') and ig['id'] not in seen:
            ig_list.insert(0, ig)
            seen.add(ig['id'])
        ig = ig or (ig_list[0] if ig_list else None)
        wa_raw = str((page_data or {}).get('whatsapp_number') or '').strip()
        wa_numbers = []
        if wa_raw:
            wa_numbers.append({'id': wa_raw, 'label': wa_raw})
        assets = {
            'messenger': {
                'available': True,
                'label': page_name,
                'picture_url': pic,
            },
            'instagram': ig,
            'instagram_accounts': ig_list,
            'whatsapp': {
                'available': bool(wa_numbers),
                'numbers': wa_numbers,
                'picture_url': pic,
            },
            'threads': {
                'available': bool(ig),
                'label': ig.get('label') if ig else '',
                'picture_url': (ig or {}).get('picture_url') or '',
                'instagram_id': (ig or {}).get('id') if ig else '',
            },
        }
        return assets

    def get(self, req):
        selected_account = str(req.GET.get('selected_account') or '').strip()
        page_id = str(req.GET.get('page_id') or '').strip()
        q = str(req.GET.get('q') or '').strip().lower()
        if not selected_account:
            return JsonResponse({'pages': [], 'assets': None})
        token = self._token(selected_account)
        if not token:
            return JsonResponse({'pages': [], 'assets': None})
        page_fields = 'id,name,picture{url},instagram_business_account{id,username,name,profile_picture_url},connected_instagram_account{id,username,name,profile_picture_url},whatsapp_number'
        try:
            if page_id:
                page_token = _facebook_page_access_token(page_id, token)
                page_resp = requests.get(
                    f'https://graph.facebook.com/v22.0/{page_id}',
                    params={'fields': page_fields, 'access_token': page_token},
                    timeout=20,
                )
                page_data = (page_resp.json() if page_resp.text else {}) or {}
                if isinstance(page_data, dict) and page_data.get('error'):
                    page_resp = requests.get(
                        f'https://graph.facebook.com/v22.0/{page_id}',
                        params={'fields': page_fields, 'access_token': token},
                        timeout=20,
                    )
                    page_data = (page_resp.json() if page_resp.text else {}) or {}
                if isinstance(page_data, dict) and page_data.get('error'):
                    return JsonResponse({'pages': [], 'assets': None, 'message': str(((page_data.get('error') or {}).get('message') or 'Gagal memuat halaman'))})
                pid = str(page_data.get('id') or page_id).strip()
                pname = str(page_data.get('name') or pid).strip()
                assets = self._page_assets(page_data, page_token, page_id=pid, token=token, ensure_pbia=not bool((page_data.get('instagram_business_account') or page_data.get('connected_instagram_account'))), selected_account=selected_account)
                page_row = {
                    'id': pid,
                    'name': pname,
                    'picture_url': self._page_picture_url(page_data),
                    'instagram_label': ((assets.get('instagram') or {}).get('label') or ''),
                }
                return JsonResponse({
                    'page': page_row,
                    'assets': assets,
                })
            resp = requests.get(
                'https://graph.facebook.com/v22.0/me/accounts',
                params={'fields': page_fields, 'limit': 200, 'access_token': token},
                timeout=20,
            )
            rows = ((resp.json() if resp.text else {}) or {}).get('data') or []
            pages, seen = [], set()
            for p in rows:
                pid = str((p or {}).get('id') or '').strip()
                pname = str((p or {}).get('name') or '').strip()
                if not pid or not pname:
                    continue
                raw = f'{pid} {pname}'.lower()
                if q and q not in raw:
                    continue
                key = f'pg|{pid}'.lower()
                if key in seen:
                    continue
                seen.add(key)
                ig = self._ig_row((p or {}).get('instagram_business_account'), pname, token) or self._ig_row((p or {}).get('connected_instagram_account'), pname, token)
                pages.append({
                    'id': pid,
                    'name': pname,
                    'picture_url': self._page_picture_url(p),
                    'instagram_label': (ig or {}).get('label') or '',
                })
            return JsonResponse({'pages': pages[:50], 'assets': None})
        except Exception:
            return JsonResponse({'pages': [], 'assets': None})


def _creative_media_aspect_bucket(width, height):
    try:
        w = float(width or 0)
        h = float(height or 0)
    except (TypeError, ValueError):
        return ''
    if w <= 0 or h <= 0:
        return ''
    ratio = w / h
    if abs(ratio - (16 / 9)) <= 0.12:
        return '16:9'
    if abs(ratio - (9 / 16)) <= 0.12:
        return '9:16'
    if abs(ratio - 0.8) <= 0.12:
        return '4:5'
    return ''


class FacebookCreativeMediaLibraryView(View):
    def dispatch(self, request, *args, **kwargs):
        if 'hris_admin' not in request.session:
            return redirect('admin_login')
        return super().dispatch(request, *args, **kwargs)

    def _token(self, account_id):
        account_id = str(account_id or '').strip()
        stripped = account_id.replace('act_', '')
        candidates = []
        for val in (account_id, stripped, f'act_{stripped}' if stripped else ''):
            val = str(val or '').strip()
            if val and val not in candidates:
                candidates.append(val)
        for candidate in candidates:
            rs = data_mysql().master_account_ads_by_id({'data_account': candidate})
            acc = (rs or {}).get('data') if isinstance(rs, dict) else None
            tok = str((acc or {}).get('access_token') or '').strip()
            if tok:
                return tok
        return ''

    def _real_account_id(self, account_id):
        return str(account_id or '').replace('act_', '').strip()

    def _graph_rows(self, url, token, params=None):
        try:
            p = dict(params or {})
            p['access_token'] = token
            resp = requests.get(url, params=p, timeout=25)
            body = resp.json() if resp.text else {}
            if isinstance(body, dict) and body.get('error'):
                return []
            return (body or {}).get('data') or []
        except Exception:
            return []

    def _resolve_video_thumb(self, row, video_id, token, allow_fetch=True):
        row = row or {}
        for key in ('thumbnail_url', 'picture'):
            val = str((row or {}).get(key) or '').strip()
            if val:
                return val
        fmt = (row or {}).get('format') or []
        if isinstance(fmt, list):
            for entry in fmt:
                pic = str((entry or {}).get('picture') or '').strip()
                if pic:
                    return pic
        if not allow_fetch:
            return ''
        video_id = str(video_id or '').strip()
        if not video_id or not token:
            return ''
        try:
            resp = requests.get(
                f'https://graph.facebook.com/v22.0/{video_id}/thumbnails',
                params={'access_token': token, 'fields': 'uri,is_preferred', 'limit': 8},
                timeout=15,
            )
            body = resp.json() if resp.text else {}
            rows = (body or {}).get('data') or []
            preferred = [r for r in rows if (r or {}).get('is_preferred')]
            for entry in (preferred or rows):
                uri = str((entry or {}).get('uri') or '').strip()
                if uri:
                    return uri
        except Exception:
            pass
        return ''

    def _media_item(self, *, media_id, media_type, title, width, height, thumb_url, source, image_hash='', video_id='', length=0, video_url=''):
        title = str(title or '').strip() or 'untitled'
        try:
            width = int(width or 0)
        except (TypeError, ValueError):
            width = 0
        try:
            height = int(height or 0)
        except (TypeError, ValueError):
            height = 0
        try:
            length = float(length or 0)
        except (TypeError, ValueError):
            length = 0
        thumb_url = str(thumb_url or '').strip()
        video_url = str(video_url or '').strip()
        return {
            'id': str(media_id or image_hash or video_id or '').strip(),
            'type': 'video' if media_type == 'video' else 'image',
            'title': title,
            'width': width,
            'height': height,
            'length': length,
            'aspect': _creative_media_aspect_bucket(width, height),
            'thumb_url': thumb_url,
            'video_url': video_url,
            'source': source,
            'image_hash': str(image_hash or '').strip(),
            'video_id': str(video_id or '').strip(),
        }

    def _fetch_account_media(self, real_account_id, token, kind):
        items = []
        if kind in ('all', 'image'):
            rows = self._graph_rows(
                f'https://graph.facebook.com/v22.0/act_{real_account_id}/adimages',
                token,
                {'fields': 'hash,url,name,width,height,created_time,status', 'limit': 50},
            )
            for row in rows:
                image_hash = str((row or {}).get('hash') or '').strip()
                if not image_hash:
                    continue
                thumb = str((row or {}).get('url') or '').strip()
                items.append(self._media_item(
                    media_id=image_hash,
                    media_type='image',
                    title=(row or {}).get('name') or image_hash[:12],
                    width=(row or {}).get('width'),
                    height=(row or {}).get('height'),
                    thumb_url=thumb,
                    source='account',
                    image_hash=image_hash,
                ))
        if kind in ('all', 'video'):
            rows = self._graph_rows(
                f'https://graph.facebook.com/v22.0/act_{real_account_id}/advideos',
                token,
                {'fields': 'id,title,thumbnail_url,picture,length,width,height,created_time,source,format{width,height,picture}', 'limit': 50},
            )
            for row in rows:
                vid = str((row or {}).get('id') or '').strip()
                if not vid:
                    continue
                fmt = ((row or {}).get('format') or [{}])
                first_fmt = fmt[0] if isinstance(fmt, list) and fmt else {}
                thumb = self._resolve_video_thumb(row, vid, token, allow_fetch=False)
                items.append(self._media_item(
                    media_id=vid,
                    media_type='video',
                    title=(row or {}).get('title') or vid,
                    width=(row or {}).get('width') or (first_fmt or {}).get('width'),
                    height=(row or {}).get('height') or (first_fmt or {}).get('height'),
                    thumb_url=thumb,
                    source='account',
                    video_id=vid,
                    length=(row or {}).get('length'),
                    video_url=(row or {}).get('source'),
                ))
        return items

    def _fetch_instagram_media(self, ig_id, token, kind):
        if not ig_id:
            return []
        fields = 'id,caption,media_type,media_url,thumbnail_url,timestamp'
        rows = self._graph_rows(
            f'https://graph.facebook.com/v22.0/{ig_id}/media',
            token,
            {'fields': fields, 'limit': 50},
        )
        items = []
        for row in rows:
            media_type = str((row or {}).get('media_type') or '').upper()
            is_video = 'VIDEO' in media_type
            if kind == 'image' and is_video:
                continue
            if kind == 'video' and not is_video:
                continue
            mid = str((row or {}).get('id') or '').strip()
            if not mid:
                continue
            caption = str((row or {}).get('caption') or '').strip()
            thumb = str((row or {}).get('thumbnail_url') or (row or {}).get('media_url') or '').strip()
            media_url = str((row or {}).get('media_url') or '').strip()
            items.append(self._media_item(
                media_id=mid,
                media_type='video' if is_video else 'image',
                title=caption[:80] if caption else mid,
                width=0,
                height=0,
                thumb_url=thumb,
                source='instagram',
                video_id=mid if is_video else '',
                video_url=media_url if is_video else '',
            ))
        return items

    def _fetch_page_media(self, page_id, token, page_name, kind):
        if not page_id:
            return []
        page_token = _facebook_page_access_token(page_id, token) or token
        items = []
        if kind in ('all', 'image'):
            rows = self._graph_rows(
                f'https://graph.facebook.com/v22.0/{page_id}/photos',
                page_token,
                {'fields': 'images,name,created_time', 'type': 'uploaded', 'limit': 40},
            )
            for row in rows:
                pid = str((row or {}).get('id') or '').strip()
                imgs = (row or {}).get('images') or []
                best = imgs[0] if imgs else {}
                if isinstance(imgs, list) and imgs:
                    best = max(imgs, key=lambda x: int((x or {}).get('width') or 0))
                thumb = str((best or {}).get('source') or '').strip()
                if not pid or not thumb:
                    continue
                items.append(self._media_item(
                    media_id=pid,
                    media_type='image',
                    title=(row or {}).get('name') or page_name or pid,
                    width=(best or {}).get('width'),
                    height=(best or {}).get('height'),
                    thumb_url=thumb,
                    source='page',
                ))
        if kind in ('all', 'video'):
            rows = self._graph_rows(
                f'https://graph.facebook.com/v22.0/{page_id}/videos',
                page_token,
                {'fields': 'picture,source,title,length,created_time,format', 'limit': 40},
            )
            for row in rows:
                vid = str((row or {}).get('id') or '').strip()
                if not vid:
                    continue
                fmt = ((row or {}).get('format') or [{}])
                first_fmt = fmt[0] if isinstance(fmt, list) and fmt else {}
                thumb = self._resolve_video_thumb(row, vid, page_token, allow_fetch=False)
                items.append(self._media_item(
                    media_id=vid,
                    media_type='video',
                    title=(row or {}).get('title') or page_name or vid,
                    width=(first_fmt or {}).get('width'),
                    height=(first_fmt or {}).get('height'),
                    thumb_url=thumb,
                    source='page',
                    video_id=vid,
                    length=(row or {}).get('length'),
                    video_url=(row or {}).get('source'),
                ))
        return items

    @staticmethod
    def _is_meta_video_id(video_id):
        return bool(re.match(r'^\d+$', str(video_id or '').strip()))

    def _graph_video_source_direct(self, token, video_id):
        try:
            resp = requests.get(
                f'https://graph.facebook.com/v22.0/{video_id}',
                params={'access_token': token, 'fields': 'source,permalink_url,embed_html,format'},
                timeout=20,
            )
            body = resp.json() if resp.text else {}
            if isinstance(body, dict) and not body.get('error'):
                source = str(body.get('source') or '').strip()
                if source:
                    return source
                source = self._extract_video_url_from_embed(body.get('embed_html'))
                if source:
                    return source
        except Exception:
            pass
        return ''

    @staticmethod
    def _extract_video_url_from_embed(embed_html):
        html = str(embed_html or '')
        if not html:
            return ''
        for pattern in (
            r'src="(https?://[^"]+)"',
            r"src='(https?://[^']+)'",
            r'href="(https?://[^"]+)"',
        ):
            match = re.search(pattern, html, re.I)
            if not match:
                continue
            url = str(match.group(1) or '').strip()
            if url and ('fbcdn' in url or '.mp4' in url.lower() or '/video/' in url):
                return url
        return ''

    def _graph_video_source_from_post(self, post_id, tokens):
        post_id = str(post_id or '').strip()
        if not post_id:
            return ''
        for token in tokens:
            token = str(token or '').strip()
            if not token:
                continue
            try:
                resp = requests.get(
                    f'https://graph.facebook.com/v22.0/{post_id}',
                    params={
                        'access_token': token,
                        'fields': 'attachments{media_type,media{source,id},subattachments{data{media_type,media{source,id}}}}',
                    },
                    timeout=20,
                )
                body = resp.json() if resp.text else {}
                if isinstance(body, dict) and body.get('error'):
                    continue
                attach = GetCampaignMetaDetailView._parse_post_attachments(body if isinstance(body, dict) else {})
                source = str(attach.get('video_source') or '').strip()
                if source:
                    return source
            except Exception:
                continue
        return ''

    def _graph_video_source_batch(self, token, video_id):
        try:
            resp = requests.get(
                'https://graph.facebook.com/v22.0/',
                params={'access_token': token, 'ids': video_id, 'fields': 'source'},
                timeout=20,
            )
            body = resp.json() if resp.text else {}
            if isinstance(body, dict):
                row = body.get(str(video_id)) or {}
                if isinstance(row, dict) and not row.get('error'):
                    return str(row.get('source') or '').strip()
        except Exception:
            pass
        return ''

    def _graph_video_source_from_advideos(self, real_account_id, token, video_id):
        if not real_account_id or not token or not video_id:
            return ''
        try:
            resp = requests.get(
                f'https://graph.facebook.com/v22.0/act_{real_account_id}/advideos',
                params={
                    'access_token': token,
                    'fields': 'id,source',
                    'limit': 50,
                    'filtering': json.dumps([{'field': 'id', 'operator': 'IN', 'value': [str(video_id)]}]),
                },
                timeout=25,
            )
            body = resp.json() if resp.text else {}
            if isinstance(body, dict) and not body.get('error'):
                for row in (body.get('data') or []):
                    if str((row or {}).get('id') or '') == str(video_id):
                        src = str((row or {}).get('source') or '').strip()
                        if src:
                            return src
        except Exception:
            pass
        url = f'https://graph.facebook.com/v22.0/act_{real_account_id}/advideos'
        params = {'access_token': token, 'fields': 'id,source', 'limit': 100}
        for _ in range(5):
            try:
                resp = requests.get(url, params=params, timeout=25)
                body = resp.json() if resp.text else {}
                if isinstance(body, dict) and body.get('error'):
                    break
                for row in (body.get('data') or []):
                    if str((row or {}).get('id') or '') == str(video_id):
                        src = str((row or {}).get('source') or '').strip()
                        if src:
                            return src
                next_url = ((body.get('paging') or {}) if isinstance(body, dict) else {}).get('next')
                if not next_url:
                    break
                url = next_url
                params = None
            except Exception:
                break
        return ''

    def _graph_video_source_from_page(self, page_id, page_token, video_id):
        if not page_id or not page_token or not video_id:
            return ''
        try:
            resp = requests.get(
                f'https://graph.facebook.com/v22.0/{page_id}/videos',
                params={'access_token': page_token, 'fields': 'id,source', 'limit': 100},
                timeout=25,
            )
            body = resp.json() if resp.text else {}
            if isinstance(body, dict) and not body.get('error'):
                for row in (body.get('data') or []):
                    if str((row or {}).get('id') or '') == str(video_id):
                        src = str((row or {}).get('source') or '').strip()
                        if src:
                            return src
        except Exception:
            pass
        return ''

    def _resolve_video_source(self, token, video_id, real_account_id='', page_id='', extra_tokens=None, post_id=''):
        video_id = str(video_id or '').strip()
        if not self._is_meta_video_id(video_id):
            return ''
        tokens = []
        page_token = _facebook_page_access_token(page_id, token) if page_id and token else ''
        if page_token:
            tokens.append(page_token)
        for t in list(extra_tokens or []):
            t = str(t or '').strip()
            if t and t not in tokens:
                tokens.append(t)
        if token and token not in tokens:
            tokens.append(token)
        if post_id:
            source = self._graph_video_source_from_post(post_id, tokens)
            if source:
                return source
        for t in tokens:
            source = self._graph_video_source_direct(t, video_id)
            if source:
                return source
        for t in tokens:
            source = self._graph_video_source_batch(t, video_id)
            if source:
                return source
        if real_account_id and token:
            source = self._graph_video_source_from_advideos(real_account_id, token, video_id)
            if source:
                return source
        if page_id:
            for t in tokens:
                source = self._graph_video_source_from_page(page_id, t, video_id)
                if source:
                    return source
        return ''

    def get(self, req):
        account_id = str(req.GET.get('account_id') or '').strip()
        source = str(req.GET.get('source') or 'all').strip().lower()
        kind = str(req.GET.get('kind') or 'all').strip().lower()
        page_id = str(req.GET.get('page_id') or '').strip()
        ig_id = str(req.GET.get('instagram_actor_id') or '').strip()
        page_name = str(req.GET.get('page_name') or '').strip()
        if kind not in ('all', 'image', 'video'):
            kind = 'all'
        token = self._token(account_id)
        real_account_id = self._real_account_id(account_id)
        if not token or not real_account_id:
            return JsonResponse({'sections': [], 'message': 'Token atau akun iklan belum tersedia.'})

        sections = []
        try:
            if source in ('all', 'account'):
                acc_items = self._fetch_account_media(real_account_id, token, kind)
                if acc_items:
                    label = 'Video akun' if kind == 'video' else ('Gambar Akun' if kind == 'image' else 'Media Akun')
                    sections.append({'key': 'account', 'title': label, 'items': acc_items[:50]})

            if source in ('all', 'business'):
                biz_items = self._fetch_account_media(real_account_id, token, kind)
                if biz_items:
                    label = 'Video Bisnis' if kind == 'video' else ('Gambar Bisnis' if kind == 'image' else 'Media Bisnis')
                    sections.append({'key': 'business', 'title': label, 'items': biz_items[:50]})

            if source in ('all', 'instagram'):
                ig_items = self._fetch_instagram_media(ig_id, token, kind)
                if ig_items:
                    label = 'Video Instagram' if kind == 'video' else ('Gambar Instagram' if kind == 'image' else 'Media Instagram')
                    sections.append({'key': 'instagram', 'title': label, 'items': ig_items[:50]})

            if source in ('all', 'page'):
                if not page_name and page_id:
                    try:
                        pr = requests.get(
                            f'https://graph.facebook.com/v22.0/{page_id}',
                            params={'fields': 'name', 'access_token': _facebook_page_access_token(page_id, token) or token},
                            timeout=15,
                        )
                        page_name = str(((pr.json() if pr.text else {}) or {}).get('name') or page_id)
                    except Exception:
                        page_name = page_id
                page_items = self._fetch_page_media(page_id, token, page_name, kind)
                if page_items:
                    title = page_name or 'Halaman Facebook'
                    if kind == 'video':
                        title = f'Video · {title}'
                    elif kind == 'image':
                        title = title
                    sections.append({'key': 'page', 'title': title, 'items': page_items[:50]})
        except Exception:
            pass

        return JsonResponse({'sections': sections})


class FacebookCreativeMediaThumbView(View):
    def dispatch(self, request, *args, **kwargs):
        if 'hris_admin' not in request.session:
            return redirect('admin_login')
        return super().dispatch(request, *args, **kwargs)

    def get(self, req):
        account_id = str(req.GET.get('account_id') or '').strip()
        video_id = str(req.GET.get('video_id') or '').strip()
        if not account_id or not video_id:
            return HttpResponse(status=404)
        lib = FacebookCreativeMediaLibraryView()
        token = lib._token(account_id)
        if not token:
            return HttpResponse(status=404)
        thumb = lib._resolve_video_thumb({}, video_id, token)
        if not thumb:
            return HttpResponse(status=404)
        return HttpResponseRedirect(thumb)


class FacebookCreativeVideoSourceView(View):
    def dispatch(self, request, *args, **kwargs):
        if 'hris_admin' not in request.session:
            return redirect('admin_login')
        return super().dispatch(request, *args, **kwargs)

    def get(self, req):
        account_id = str(req.GET.get('account_id') or '').strip()
        video_id = str(req.GET.get('video_id') or '').strip()
        page_id = str(req.GET.get('page_id') or '').strip()
        post_id = str(req.GET.get('post_id') or '').strip()
        wants_json = str(req.GET.get('format') or '').strip().lower() == 'json'
        if not account_id or not video_id:
            if wants_json:
                return JsonResponse({'success': False, 'message': 'account_id dan video_id wajib diisi'}, status=404)
            return HttpResponse(status=404)
        lib = FacebookCreativeMediaLibraryView()
        if not lib._is_meta_video_id(video_id):
            if wants_json:
                return JsonResponse({'success': False, 'message': 'video_id tidak valid', 'video_id': video_id}, status=404)
            return HttpResponse(status=404)
        token = lib._token(account_id)
        if not token:
            if wants_json:
                return JsonResponse({'success': False, 'message': 'Token akun tidak ditemukan'}, status=404)
            return HttpResponse(status=404)
        extra_tokens = []
        if page_id:
            page_token = _facebook_page_access_token(page_id, token)
            if page_token:
                extra_tokens.append(page_token)
        source = lib._resolve_video_source(
            token,
            video_id,
            real_account_id=lib._real_account_id(account_id),
            page_id=page_id,
            extra_tokens=extra_tokens,
            post_id=post_id,
        )
        if not source:
            if wants_json:
                return JsonResponse({'success': False, 'message': 'Sumber video tidak tersedia dari Meta API', 'video_id': video_id}, status=404)
            return HttpResponse(status=404)
        if str(req.GET.get('format') or '').strip().lower() == 'json':
            return JsonResponse({'video_url': source, 'source': source})
        use_redirect = str(req.GET.get('redirect') or '').strip() == '1'
        use_stream = not use_redirect and (
            str(req.GET.get('stream') or '').strip() == '1'
            or 'video' in str(req.META.get('HTTP_ACCEPT') or '').lower()
            or str(req.GET.get('format') or '').strip().lower() != 'json'
        )
        if use_stream:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
                'Accept': '*/*',
                'Referer': 'https://www.facebook.com/',
            }
            range_header = str(req.META.get('HTTP_RANGE') or '').strip()
            if range_header:
                headers['Range'] = range_header
            try:
                upstream = requests.get(source, headers=headers, stream=True, timeout=90)
            except Exception:
                return HttpResponse(status=502)
            if upstream.status_code >= 400:
                return HttpResponse(status=upstream.status_code)
            content_type = str(upstream.headers.get('Content-Type') or 'video/mp4')
            response = StreamingHttpResponse(
                upstream.iter_content(chunk_size=65536),
                status=upstream.status_code,
                content_type=content_type,
            )
            for key in ('Content-Length', 'Content-Range', 'Accept-Ranges'):
                val = upstream.headers.get(key)
                if val:
                    response[key] = val
            response['Cache-Control'] = 'private, max-age=300'
            response['Accept-Ranges'] = response.get('Accept-Ranges') or 'bytes'
            return response
        return HttpResponseRedirect(source)


def _discovery_normalize_url(url):
    url = str(url or '').strip()
    if not url:
        return ''
    if not url.startswith(('http://', 'https://')):
        url = 'https://' + url
    return url


def _discovery_host(url):
    try:
        from urllib.parse import urlparse
        host = urlparse(url).netloc.lower()
        return host[4:] if host.startswith('www.') else host
    except Exception:
        return ''


def _discovery_same_site(base_url, candidate_url):
    base_host = _discovery_host(base_url)
    cand_host = _discovery_host(candidate_url)
    if not base_host or not cand_host:
        return False
    if base_host == cand_host:
        return True
    return base_host.split('.')[-2:] == cand_host.split('.')[-2:]


def _discovery_label_from_url(url):
    try:
        from urllib.parse import urlparse, unquote
        path = unquote(urlparse(url).path or '').strip('/')
        if not path:
            return 'Beranda'
        slug = path.split('/')[-1]
        slug = re.sub(r'[-_]+', ' ', slug)
        slug = re.sub(r'\s+', ' ', slug).strip()
        if slug:
            return slug[:80].title()
    except Exception:
        pass
    return 'Produk'


def _discovery_skip_href(href):
    h = str(href or '').strip().lower()
    if not h or h.startswith('#') or h.startswith('javascript:') or h.startswith('mailto:'):
        return True
    skip_words = (
        '/login', '/signin', '/signup', '/register', '/cart', '/checkout',
        '/privacy', '/terms', '/policy', '/help', '/account', '/settings',
        '/search', '/wishlist', 'facebook.com', 'instagram.com', 'twitter.com',
    )
    return any(w in h for w in skip_words)


def _discovery_extract_links(html, base_url, limit=24):
    from html.parser import HTMLParser
    from urllib.parse import urljoin, urlparse

    class _Parser(HTMLParser):
        def __init__(self):
            super().__init__()
            self.links = []
            self._href = ''
            self._text_parts = []

        def handle_starttag(self, tag, attrs):
            if tag != 'a':
                return
            ad = dict(attrs)
            self._href = str(ad.get('href') or '').strip()
            self._text_parts = []

        def handle_data(self, data):
            if self._href:
                self._text_parts.append(str(data or ''))

        def handle_endtag(self, tag):
            if tag != 'a' or not self._href:
                return
            text = re.sub(r'\s+', ' ', ''.join(self._text_parts)).strip()
            self.links.append((self._href, text))
            self._href = ''
            self._text_parts = []

    parser = _Parser()
    try:
        parser.feed(str(html or ''))
    except Exception:
        pass

    seen = set()
    out = []
    base_norm = _discovery_normalize_url(base_url)
    for href, text in parser.links:
        if _discovery_skip_href(href):
            continue
        abs_url = urljoin(base_norm, href)
        abs_url = abs_url.split('#')[0].strip()
        if not abs_url.startswith(('http://', 'https://')):
            continue
        if not _discovery_same_site(base_norm, abs_url):
            continue
        key = abs_url.rstrip('/').lower()
        if key in seen:
            continue
        seen.add(key)
        label = text[:80] if text and len(text) >= 3 else _discovery_label_from_url(abs_url)
        if len(label) < 2:
            continue
        score = 0
        low = abs_url.lower()
        if any(x in low for x in ('/product', '/products', '/item', '/p/', '/shop', '/collections', 'shopee', 'tokopedia', 'lazada')):
            score += 3
        if text:
            score += 2
        out.append({'url': abs_url, 'label': label, 'score': score})
    out.sort(key=lambda x: (-x.get('score', 0), x.get('label', '')))
    return [{'url': x['url'], 'label': x['label']} for x in out[:limit]]


def _discovery_meta_scrape(url, token):
    url = _discovery_normalize_url(url)
    token = str(token or '').strip()
    if not url or not token:
        return {}
    try:
        resp = requests.post(
            'https://graph.facebook.com/v22.0/',
            params={'id': url, 'scrape': 'true', 'access_token': token},
            timeout=15,
        )
        body = resp.json() if resp.text else {}
        if isinstance(body, dict) and body.get('error'):
            return {}
        title = str(body.get('title') or '').strip()
        images = body.get('image') or []
        thumb = ''
        if isinstance(images, list) and images:
            thumb = str((images[0] or {}).get('url') or '').strip()
        if not thumb:
            thumb = str(body.get('thumbnail_url') or '').strip()
        return {'title': title, 'thumb_url': thumb}
    except Exception:
        return {}


class FacebookCreativeDiscoveryLinksView(View):
    def dispatch(self, request, *args, **kwargs):
        if 'hris_admin' not in request.session:
            return redirect('admin_login')
        return super().dispatch(request, *args, **kwargs)

    def get(self, req):
        source_url = _discovery_normalize_url(req.GET.get('source_url') or req.GET.get('url') or '')
        account_id = str(req.GET.get('account_id') or '').strip()
        if not source_url:
            return JsonResponse({'links': [], 'message': 'URL sumber belum diisi.'})

        lib = FacebookCreativeMediaLibraryView()
        token = lib._token(account_id) if account_id else ''

        final_url = source_url
        html = ''
        try:
            resp = requests.get(
                source_url,
                timeout=18,
                headers={'User-Agent': 'facebookexternalhit/1.1 (+http://www.facebook.com/externalhit_uatext.php)'},
                allow_redirects=True,
            )
            final_url = str(resp.url or source_url).strip() or source_url
            html = resp.text or ''
        except Exception:
            html = ''

        suggestions = []
        seen_urls = set()

        def _append(url, label, thumb_url=''):
            url = _discovery_normalize_url(url)
            if not url:
                return
            key = url.rstrip('/').lower()
            if key in seen_urls:
                return
            seen_urls.add(key)
            suggestions.append({
                'url': url,
                'label': str(label or _discovery_label_from_url(url))[:80],
                'thumb_url': str(thumb_url or '').strip(),
                'recommended': True,
            })

        meta_main = _discovery_meta_scrape(final_url, token)
        main_label = meta_main.get('title') or _discovery_label_from_url(final_url)
        _append(final_url, main_label, meta_main.get('thumb_url') or '')

        for row in _discovery_extract_links(html, final_url, limit=20):
            thumb = ''
            if token and len(suggestions) < 8:
                meta_row = _discovery_meta_scrape(row['url'], token)
                thumb = meta_row.get('thumb_url') or ''
                if meta_row.get('title') and (not row.get('label') or row.get('label') == _discovery_label_from_url(row['url'])):
                    row['label'] = meta_row['title']
            _append(row['url'], row['label'], thumb)
            if len(suggestions) >= 15:
                break

        return JsonResponse({
            'links': suggestions[:15],
            'source_url': final_url,
            'count': len(suggestions[:15]),
        })


class FacebookExistingPostLibraryView(View):
    def dispatch(self, request, *args, **kwargs):
        if 'hris_admin' not in request.session:
            return redirect('admin_login')
        return super().dispatch(request, *args, **kwargs)
    def _token(self, account_id):
        rs = data_mysql().master_account_ads_by_id({'data_account': account_id}); acc = (rs or {}).get('data') if isinstance(rs, dict) else None
        return str((acc or {}).get('access_token') or '').strip()
    def _page_token(self, page_id, user_token):
        r = requests.get(f'https://graph.facebook.com/v22.0/{page_id}', params={'fields': 'access_token,name', 'access_token': user_token}, timeout=20)
        b = r.json() if r.text else {}
        return str((b or {}).get('access_token') or '').strip(), str((b or {}).get('name') or page_id)
    def get(self, req):
        account_id = str(req.GET.get('account_id') or '').strip(); source = str(req.GET.get('source') or 'facebook').strip(); q = str(req.GET.get('q') or '').strip().lower(); flt = str(req.GET.get('filter') or 'all').strip().lower()
        post_scope = str(req.GET.get('post_scope') or 'published').strip().lower()
        eligible_only = str(req.GET.get('eligible_only') or '1').strip().lower() in ('1', 'true', 'yes', 'on')
        page_id = str(req.GET.get('page_id') or '').strip(); ig_id = str(req.GET.get('instagram_actor_id') or '').strip(); partner_page_id = str(req.GET.get('partner_page_id') or '').strip(); partner_ig_id = str(req.GET.get('partner_instagram_actor_id') or '').strip()
        token = self._token(account_id)
        if not token: return JsonResponse({'results': []})
        results = []
        try:
            def _media_label(item):
                atts = (((item or {}).get('attachments') or {}).get('data') or [])
                first = (atts[0] or {}) if atts else {}
                raw = str((first.get('media_type') or first.get('type') or (item or {}).get('media_type') or (item or {}).get('status_type') or '')).upper()
                if 'VIDEO' in raw:
                    return 'Video'
                if any(x in raw for x in ['PHOTO','IMAGE','ALBUM']):
                    return 'Gambar'
                return 'Teks'
            if source == 'instagram' and ig_id:
                r = requests.get(f'https://graph.facebook.com/v22.0/{ig_id}/media', params={'fields': 'id,caption,media_type,media_url,thumbnail_url,timestamp,permalink', 'limit': 50, 'access_token': token}, timeout=20)
                rows = ((r.json() if r.text else {}) or {}).get('data') or []
                for x in rows:
                    txt = str((x or {}).get('caption') or 'Postingan Instagram').strip() or 'Postingan Instagram'; mt = str((x or {}).get('media_type') or '-').strip(); created = str((x or {}).get('timestamp') or '').replace('T', ' ')[:16]
                    media_label = 'Video' if 'VIDEO' in mt.upper() else ('Gambar' if any(v in mt.upper() for v in ['IMAGE','CAROUSEL']) else 'Teks')
                    raw = f"{txt} {x.get('id','')} {mt}".lower()
                    if q and q not in raw: continue
                    if flt != 'all' and flt != media_label.lower(): continue
                    results.append({
                        'id': str((x or {}).get('id') or ''), 'text': txt[:120], 'source_label': 'Instagram',
                        'media_label': media_label, 'created_label': created or '-',
                        'is_published': False, 'promotable_for_dev': False,
                        'publish_label': 'Instagram (tidak didukung dev mode)',
                    })
            else:
                use_page = partner_page_id if source == 'partner' and partner_page_id else page_id
                if use_page:
                    page_token, page_name = self._page_token(use_page, token)
                    fields = 'id,message,created_time,status_type,is_published,is_eligible_for_promotion,promotable_id,application{id,name},attachments{media_type,type}'
                    rows = []
                    seen = set()
                    if post_scope in ('published', 'all'):
                        resp_pub = requests.get(f'https://graph.facebook.com/v22.0/{use_page}/published_posts', params={'fields': fields, 'limit': 50, 'access_token': page_token or token}, timeout=20)
                        for x in (((resp_pub.json() if resp_pub.text else {}) or {}).get('data') or []):
                            xid = str((x or {}).get('id') or '').strip()
                            if xid and xid not in seen:
                                seen.add(xid)
                                x = dict(x or {})
                                x['_from'] = 'published'
                                rows.append(x)
                    if post_scope in ('promotable', 'all'):
                        resp_prom = requests.get(f'https://graph.facebook.com/v22.0/{use_page}/promotable_posts', params={'fields': fields, 'limit': 50, 'access_token': page_token or token}, timeout=20)
                        for x in (((resp_prom.json() if resp_prom.text else {}) or {}).get('data') or []):
                            xid = str((x or {}).get('id') or '').strip()
                            if xid and xid not in seen:
                                seen.add(xid)
                                x = dict(x or {})
                                x['_from'] = 'promotable'
                                rows.append(x)
                    for x in rows:
                        xid = str((x or {}).get('id') or '').strip()
                        if not xid:
                            continue
                        is_published = (x or {}).get('is_published')
                        if is_published is None and str((x or {}).get('_from') or '') == 'published':
                            is_published = True
                        is_eligible = (x or {}).get('is_eligible_for_promotion')
                        promotable_id = str((x or {}).get('promotable_id') or xid).strip()
                        app_obj = (x or {}).get('application') if isinstance((x or {}).get('application'), dict) else {}
                        app_name = str((app_obj or {}).get('name') or '').strip()
                        promotable_for_dev = bool(
                            is_published is not False
                            and '_' in promotable_id
                            and is_eligible is not False
                        )
                        txt = str((x or {}).get('message') or 'Postingan Facebook').strip() or 'Postingan Facebook'
                        media_label = _media_label(x)
                        created = str((x or {}).get('created_time') or '').replace('T', ' ')[:16]
                        raw = f"{txt} {xid} {promotable_id} {media_label}".lower()
                        if q and q not in raw:
                            continue
                        if flt != 'all' and flt != media_label.lower():
                            continue
                        if post_scope == 'published' and is_published is False:
                            continue
                        if eligible_only and is_eligible is False:
                            continue
                        if promotable_for_dev:
                            publish_label = 'Siap untuk iklan'
                        elif is_eligible is False:
                            publish_label = 'Tidak eligible iklan'
                        elif is_published is False:
                            publish_label = 'Unpublished'
                        else:
                            publish_label = 'Perlu verifikasi'
                        if app_name:
                            publish_label += f' · {app_name[:24]}'
                        results.append({
                            'id': promotable_id,
                            'post_id': xid,
                            'promotable_id': promotable_id,
                            'text': txt[:120],
                            'source_label': ('Konten Mitra' if source == 'partner' else page_name),
                            'media_label': media_label, 'created_label': created or '-',
                            'is_published': bool(is_published is not False),
                            'is_eligible_for_promotion': is_eligible is not False,
                            'promotable_for_dev': promotable_for_dev,
                            'publish_label': publish_label,
                            'application_name': app_name,
                        })
            return JsonResponse({'results': results[:50]})
        except Exception:
            return JsonResponse({'results': results[:50]})

class FacebookCreatePagePostView(View):
    def dispatch(self, request, *args, **kwargs):
        if 'hris_admin' not in request.session:
            return redirect('admin_login')
        return super().dispatch(request, *args, **kwargs)
    def post(self, req):
        account_id = str(req.POST.get('account_id') or '').strip(); page_id = str(req.POST.get('page_id') or '').strip(); message = str(req.POST.get('message') or '').strip(); link = str(req.POST.get('link') or '').strip()
        publish_raw = str(req.POST.get('published') or 'true').strip().lower()
        publish = publish_raw in ('1', 'true', 'yes', 'on')
        if not account_id or not page_id or not message:
            return JsonResponse({'success': False, 'message': 'Account, halaman, dan isi postingan wajib diisi.'})
        try:
            rs = data_mysql().master_account_ads_by_id({'data_account': account_id}); acc = (rs or {}).get('data') if isinstance(rs, dict) else None
            token = str((acc or {}).get('access_token') or '').strip()
            page_meta = requests.get(f'https://graph.facebook.com/v22.0/{page_id}', params={'fields': 'access_token', 'access_token': token}, timeout=20).json()
            page_token = str((page_meta or {}).get('access_token') or token).strip()
            payload = {'message': message, 'published': 'true' if publish else 'false', 'access_token': page_token}
            if link: payload['link'] = link
            resp = requests.post(f'https://graph.facebook.com/v22.0/{page_id}/feed', data=payload, timeout=20)
            body = resp.json() if resp.text else {}
            post_id = str((body or {}).get('id') or '').strip()
            if not post_id:
                err_obj = (body or {}).get('error') or {}
                err_msg = str(err_obj.get('message') or err_obj or '').strip() or 'Gagal membuat postingan.'
                return JsonResponse({'success': False, 'message': err_msg})
            return JsonResponse({
                'success': True,
                'post_id': post_id,
                'post_text': message[:120],
                'is_published': publish,
                'publish_label': 'Publik' if publish else 'Unpublished',
            })
        except Exception:
            return JsonResponse({'success': False, 'message': 'Gagal membuat postingan.'})

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
            negative_labels = ["TRAFFIC_DROP","SERVING_DROP","YIELD_DROP","VIEWABILITY_DROP","EFFICIENCY_DROP","REVENUE_DROP","NEGATIVE_MIXED","NEG_ADJUSTMENT","WATCH_NEGATIVE","WATCH_DECAY","WATCH_IVT","IVT_DOMINANT_RISK","PROFIT_OK_BUT_UNSAFE","TRAFFIC_QUALITY_MISMATCH"]
            hard_pause_labels = ["NEG_ADJUSTMENT", "WATCH_IVT", "IVT_DOMINANT_RISK", "PROFIT_OK_BUT_UNSAFE"]
            soft_pause_labels = ["WATCH_NEGATIVE", "WATCH_DECAY", "TRAFFIC_QUALITY_MISMATCH"]
            label_up = str(label or '').strip().upper()
            source_mode_key = str(source_mode or 'BLENDED').strip().upper()
            single_source = source_mode_key in ["ADX_ONLY", "ADSENSE_ONLY"]
            down_score_cut = 56 if single_source else 58
            down_dm_cut = -10 if single_source else -8
            down_anomaly_cut = 2
            profit_component = clip((float(roi_value) + 1.0) * 50.0, 0.0, 100.0)
            score_raw = (((health + 100.0) / 2.0) * 0.27) + ((100.0 - risk) * 0.33) + (conf * 100.0 * 0.12) + (clip(dm + 50.0, 0.0, 100.0) * 0.18) + (profit_component * 0.10)
            score = int(round(clip(score_raw, 0.0, 100.0)))
            decision = "HOLD"
            anomaly_pressure = len(anomaly_cards)
            red_flag_stop = label_up.startswith("RED_FLAG")
            hard_pause_signal = (label_up in hard_pause_labels) and (
                (risk >= 60) or (dm <= -25) or (adj <= -30) or (anomaly_pressure > 0)
            )
            soft_pause_signal = (label_up in soft_pause_labels) and (
                (risk >= 68) or (dm <= -35) or ((adj <= -35) and (health <= -15)) or (anomaly_pressure > 0)
            )
            pause_signal = hard_pause_signal or soft_pause_signal
            if red_flag_stop:
                decision = "STOP"
            elif pause_signal or ((risk >= 82) and (conf >= 0.50)) or ((dm <= -55) and (health <= -18) and (conf >= 0.50)):
                decision = "PAUSE"
            elif risk >= 68 or (anomaly_pressure >= down_anomaly_cut and risk >= 58):
                decision = "SCALE DOWN"
            elif (label_up in ["POSITIVE_EXPANSION", "POSITIVE_RECOVERY"] and score >= 66 and risk < 42 and dm >= 8 and conf >= 0.50 and health >= 8) or (score >= 76 and risk < 38 and dm >= 12 and conf >= 0.55 and health >= 12):
                decision = "SCALE UP"
            elif (label_up in negative_labels and ((score < down_score_cut and dm < down_dm_cut) or (health < -10 and adj < -15))) or (score < 48 and (health < -10 or risk >= 60)):
                decision = "SCALE DOWN"

            profit_guard_hold = profit_strong and risk < 35 and anomaly_pressure == 0 and (label_up in ["WATCH_DECAY", "WATCH_NEGATIVE"]) and (decision in ["SCALE_DOWN", "SCALE DOWN", "PAUSE"])
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
            light = bool(payload.get('light'))
            include_events = bool(payload.get('include_events')) and (not light)
            include_source = bool(payload.get('include_source', True))
            include_timeline = bool(payload.get('include_timeline', True)) and (not light)
            history_lookback_days = 3 if light else 35
            history_start_obj = (target_date_obj - timedelta(days=history_lookback_days)) if target_date_obj else None
            history_start_sql = history_start_obj.isoformat() if history_start_obj else target_date
            target_date_sql = target_date_obj.isoformat() if target_date_obj else target_date
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
            WHERE toDate(date) >= toDate('{history_start_sql}')
              AND toDate(date) <= toDate('{target_date_sql}')
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
            def compute_decision_margin_value(health_score, ivt_risk_score, adjustment_score):
                ivt_penalty = max(0.0, safe_float(ivt_risk_score) - 45.0)
                return float(round(safe_float(health_score) + safe_float(adjustment_score) - ivt_penalty, 4))
            def dedupe_status_snapshot(frame):
                tmp = frame.copy() if frame is not None else pd.DataFrame()
                if tmp.empty:
                    return tmp
                if 'country_code' in tmp.columns:
                    tmp['country_code'] = tmp['country_code'].astype(str).str.strip().str.upper()
                dedupe_keys = []
                if 'country_code' in tmp.columns:
                    dedupe_keys.append('country_code')
                if 'meta_campaign' in tmp.columns:
                    dedupe_keys.append('meta_campaign')
                if not dedupe_keys:
                    return tmp
                sort_cols = list(dedupe_keys)
                if 'scoring_date' in tmp.columns:
                    sort_cols.append('scoring_date')
                elif 'date' in tmp.columns:
                    sort_cols.append('date')
                if 'run_time' in tmp.columns:
                    sort_cols.append('run_time')
                if 'run_hour' in tmp.columns:
                    sort_cols.append('run_hour')
                tmp = tmp.sort_values(sort_cols).drop_duplicates(subset=dedupe_keys, keep='last')
                return tmp
            def summarize_status_snapshot(frame):
                tmp = dedupe_status_snapshot(frame)
                if tmp.empty:
                    return {}
                for c in ['positive_signal_count', 'negative_signal_count', 'neutral_signal_count']:
                    if c not in tmp.columns:
                        tmp[c] = 0
                tmp['signal_total'] = (
                    pd.to_numeric(tmp['positive_signal_count'], errors='coerce').fillna(0)
                    + pd.to_numeric(tmp['negative_signal_count'], errors='coerce').fillna(0)
                    + pd.to_numeric(tmp['neutral_signal_count'], errors='coerce').fillna(0)
                )
                health_v = avg(tmp, 'health_score', weighted=True)
                risk_v = avg(tmp, 'ivt_risk_score', weighted=True)
                adj_v = avg(tmp, 'adjustment_score', weighted=True)
                conf_v = normalize_conf(avg(tmp, 'confidence', weighted=True))
                dm_v = compute_decision_margin_value(health_v, risk_v, adj_v)
                labels_v = []
                for c in ['final_label', 'root_cause_label']:
                    if c in tmp.columns:
                        labels_v.extend([str(x).strip().upper() for x in tmp[c].tolist() if str(x).strip()])
                label_v = pd.Series(labels_v).value_counts().index[0] if labels_v else 'STABLE'
                spend_v = float(pd.to_numeric(tmp['spend'], errors='coerce').fillna(0).sum()) if 'spend' in tmp.columns else 0.0
                revenue_v = float(pd.to_numeric(tmp['revenue_value'], errors='coerce').fillna(0).sum()) if 'revenue_value' in tmp.columns else 0.0
                roi_v = ((revenue_v - spend_v) / spend_v) if spend_v > 0 else 0.0
                return {
                    'health_score': float(health_v),
                    'ivt_risk_score': float(risk_v),
                    'adjustment_score': float(adj_v),
                    'confidence': float(conf_v),
                    'decision_margin': float(dm_v),
                    'traffic_score': float(avg(tmp, 'traffic_score', weighted=True)),
                    'delivery_score': float(avg(tmp, 'delivery_score', weighted=True)),
                    'yield_score': float(avg(tmp, 'yield_score', weighted=True)),
                    'quality_score': float(avg(tmp, 'quality_score', weighted=True)),
                    'revenue_score': float(avg(tmp, 'revenue_score', weighted=True)),
                    'efficiency_score': float(avg(tmp, 'efficiency_score', weighted=True)),
                    'engagement_score': float(avg(tmp, 'engagement_score', weighted=True)),
                    'control_score': float(avg(tmp, 'control_score', weighted=True)),
                    'positive_signal_count': int(pd.to_numeric(tmp['positive_signal_count'], errors='coerce').fillna(0).sum()),
                    'negative_signal_count': int(pd.to_numeric(tmp['negative_signal_count'], errors='coerce').fillna(0).sum()),
                    'neutral_signal_count': int(pd.to_numeric(tmp['neutral_signal_count'], errors='coerce').fillna(0).sum()),
                    'label': str(label_v),
                    'roi': float(roi_v),
                    'spend': float(spend_v),
                    'revenue_value': float(revenue_v),
                    'days_active': int(pd.to_numeric(tmp.get('days_active', pd.Series([], dtype=float)), errors='coerce').fillna(0).max()) if 'days_active' in tmp.columns else 0,
                    'source_mode': dominant_text(tmp, 'mapped_revenue_source', 'BLENDED'),
                    'reason_summary': ' | '.join(list(dict.fromkeys([str(x).strip() for x in tmp.get('reason_summary', pd.Series([], dtype=str)).tolist() if str(x).strip()]))[:3]),
                    'row_count': int(len(tmp.index)),
                }
            def blend_snapshot_metrics(current_snapshot, historical_windows):
                components = [('current', 0.35, current_snapshot or {})]
                components.extend([
                    ('h1', 0.20, (historical_windows or {}).get('h1') or {}),
                    ('h3', 0.15, (historical_windows or {}).get('h3') or {}),
                    ('h7', 0.12, (historical_windows or {}).get('h7') or {}),
                    ('h14', 0.08, (historical_windows or {}).get('h14') or {}),
                    ('h28', 0.06, (historical_windows or {}).get('h28') or {}),
                    ('h35', 0.04, (historical_windows or {}).get('h35') or {}),
                ])
                usable = [(name, weight, snap) for name, weight, snap in components if snap]
                if not usable:
                    return {}
                total_weight = sum(weight for _, weight, _ in usable) or 1.0
                metric_keys = [
                    'health_score', 'ivt_risk_score', 'adjustment_score', 'confidence', 'roi',
                    'traffic_score', 'delivery_score', 'yield_score', 'quality_score', 'revenue_score',
                    'efficiency_score', 'engagement_score', 'control_score'
                ]
                blended = {}
                for key in metric_keys:
                    blended[key] = float(sum((weight / total_weight) * safe_float(snap.get(key)) for _, weight, snap in usable))
                blended['decision_margin'] = compute_decision_margin_value(
                    blended.get('health_score', 0.0),
                    blended.get('ivt_risk_score', 0.0),
                    blended.get('adjustment_score', 0.0),
                )
                label_scores = {}
                for _, weight, snap in usable:
                    lbl = str(snap.get('label') or '').strip().upper()
                    if lbl:
                        label_scores[lbl] = float(label_scores.get(lbl, 0.0)) + float(weight / total_weight)
                blended['label'] = max(label_scores.items(), key=lambda kv: kv[1])[0] if label_scores else str((current_snapshot or {}).get('label') or 'STABLE').upper()
                blended['reason_summary'] = ' | '.join([
                    f"{name.upper()}={str(snap.get('label') or '-').upper()} ({safe_float(snap.get('health_score')):.1f}/{safe_float(snap.get('ivt_risk_score')):.1f}/{safe_float(snap.get('adjustment_score')):.1f})"
                    for name, _, snap in usable
                    if snap
                ])
                blended['weights_used'] = {
                    str(name): float(round(weight / total_weight, 4))
                    for name, weight, _ in usable
                }
                return blended
            for entity_key, part in df.groupby('entity_key', sort=False):
                # Ensure signal_total is available in part for weighted averages
                for c in ['positive_signal_count', 'negative_signal_count', 'neutral_signal_count']:
                    if c not in part.columns:
                        part[c] = 0
                part['signal_total'] = pd.to_numeric(part['positive_signal_count'], errors='coerce').fillna(0) + \
                                       pd.to_numeric(part['negative_signal_count'], errors='coerce').fillna(0) + \
                                       pd.to_numeric(part['neutral_signal_count'], errors='coerce').fillna(0)

                part_eval = part.copy()
                if target_date_obj is not None and 'scoring_date' in part_eval.columns:
                    part_on_target = part_eval[part_eval['scoring_date'].eq(target_date_obj)].copy()
                    if not part_on_target.empty:
                        part_eval = part_on_target

                snap = dedupe_status_snapshot(part_eval)
                current_snapshot = summarize_status_snapshot(part_eval)
                historical_windows = {}
                available_hist_dates = []
                if light:
                    blended_snapshot = current_snapshot if current_snapshot else {}
                else:
                    if target_date_obj is not None and 'scoring_date' in part.columns:
                        scoring_dates_all = pd.to_datetime(part['scoring_date'], errors='coerce').dt.date
                        for d in sorted([x for x in scoring_dates_all.dropna().tolist() if x < target_date_obj]):
                            if d not in available_hist_dates:
                                available_hist_dates.append(d)
                        for days_back, key in [(1, 'h1'), (3, 'h3'), (7, 'h7'), (14, 'h14'), (28, 'h28'), (35, 'h35')]:
                            start_d = target_date_obj - timedelta(days=days_back)
                            hist_slice = part[(part['scoring_date'] >= start_d) & (part['scoring_date'] < target_date_obj)].copy()
                            window_snapshot = summarize_status_snapshot(hist_slice)
                            if window_snapshot:
                                historical_windows[key] = window_snapshot
                    blended_snapshot = blend_snapshot_metrics(current_snapshot, historical_windows)

                join_status_series = snap['join_status'] if 'join_status' in snap.columns else pd.Series('', index=snap.index)
                join_status_clean = join_status_series.astype(str).str.upper().str.strip()
                join_status_summary = ', '.join([f"{k}:{v}" for k, v in join_status_clean.value_counts().to_dict().items() if str(k).strip()])
                
                health = float(blended_snapshot.get('health_score', current_snapshot.get('health_score', 0.0)))
                risk = float(blended_snapshot.get('ivt_risk_score', current_snapshot.get('ivt_risk_score', 0.0)))
                adj = float(blended_snapshot.get('adjustment_score', current_snapshot.get('adjustment_score', 0.0)))
                dm = float(blended_snapshot.get('decision_margin', current_snapshot.get('decision_margin', 0.0)))
                conf = float(blended_snapshot.get('confidence', current_snapshot.get('confidence', 0.0)))

                labels = []
                for c in ['final_label', 'root_cause_label']:
                    if c in snap.columns:
                        labels.extend([str(x).strip().upper() for x in snap[c].tolist() if str(x).strip()])
                current_label = pd.Series(labels).value_counts().index[0] if labels else 'STABLE'
                label = str(blended_snapshot.get('label') or current_label or 'STABLE').upper()

                # Totals from evaluation frame + continuity guard from full frame
                total_pos = int(pd.to_numeric(part_eval['positive_signal_count'], errors='coerce').fillna(0).sum()) if 'positive_signal_count' in part_eval.columns else 0
                total_neg = int(pd.to_numeric(part_eval['negative_signal_count'], errors='coerce').fillna(0).sum()) if 'negative_signal_count' in part_eval.columns else 0
                total_neu = int(pd.to_numeric(part_eval['neutral_signal_count'], errors='coerce').fillna(0).sum()) if 'neutral_signal_count' in part_eval.columns else 0
                spend_total = float(pd.to_numeric(part_eval['spend'], errors='coerce').fillna(0).sum()) if 'spend' in part_eval.columns else 0.0
                revenue_total = float(pd.to_numeric(part_eval['revenue_value'], errors='coerce').fillna(0).sum()) if 'revenue_value' in part_eval.columns else 0.0
                roi_total_raw = ((revenue_total - spend_total) / spend_total) if spend_total > 0 else 0.0
                roi_total = float(blended_snapshot.get('roi', roi_total_raw))
                history_has_spend = any(safe_float((historical_windows.get(k) or {}).get('spend')) > 0 for k in ['h1', 'h3', 'h7', 'h14', 'h28', 'h35'])
                profit_strong = (roi_total >= 0.30) and ((revenue_total > 0) or history_has_spend)
                full_spend_total = spend_total
                full_revenue_total = revenue_total
                continuity_running = (full_spend_total > 0) or (full_revenue_total > 0)
                has_signal = (total_pos + total_neg + total_neu) > 0
                has_metric_surface = any([abs(safe_float(health)) > 0.0, abs(safe_float(risk)) > 0.0, abs(safe_float(adj)) > 0.0, abs(safe_float(dm)) > 0.0])
                has_scoring = (has_signal and conf >= 0.05) or (continuity_running and has_metric_surface and conf >= 0.03)
                traffic_now = float(blended_snapshot.get('traffic_score', current_snapshot.get('traffic_score', avg(part_eval, 'traffic_score', weighted=True))))
                delivery_now = float(blended_snapshot.get('delivery_score', current_snapshot.get('delivery_score', avg(part_eval, 'delivery_score', weighted=True))))
                yield_now = float(blended_snapshot.get('yield_score', current_snapshot.get('yield_score', avg(part_eval, 'yield_score', weighted=True))))
                revenue_now = float(blended_snapshot.get('revenue_score', current_snapshot.get('revenue_score', avg(part_eval, 'revenue_score', weighted=True))))
                anomaly_cards = derive_anomaly_cards(traffic_now, delivery_now, yield_now, revenue_now, risk, adj)
                days_series = part_eval['scoring_date'] if 'scoring_date' in part_eval.columns else (part_eval['date'] if 'date' in part_eval.columns else pd.Series([], dtype=object))
                days_hist = int(pd.to_datetime(days_series, errors='coerce').dropna().dt.date.nunique())
                days_flag = int(pd.to_numeric(part_eval.get('days_active', pd.Series([], dtype=float)), errors='coerce').fillna(0).max()) if 'days_active' in part_eval.columns else 0
                active_days_effective = max(days_hist, days_flag)
                maturity_profile = campaign_maturity_profile(part_eval if light else part)
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
                reason_summary = str(blended_snapshot.get('reason_summary') or ' | '.join(list(dict.fromkeys(reason_parts))[:3]))

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

                timeline_hours_limit = 1 if light else (8 if include_timeline else 0)
                if timeline_hours_limit > 0 and run_hours:
                    for rh in run_hours[:timeline_hours_limit]:
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
                        maturity_profile_h = campaign_maturity_profile(part_eval if light else part)
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
                                'score': safe_float(crow.get('score')),
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

                if light and scoring_timeline:
                    tl_head = scoring_timeline[0] or {}
                    tl_score = tl_head.get('score')
                    tl_decision = str(tl_head.get('decision') or '').strip().upper()
                    tl_label = str(tl_head.get('label') or '').strip().upper()
                    if tl_score is not None:
                        score = tl_score
                    if tl_decision:
                        decision = tl_decision
                    if tl_label:
                        label = tl_label

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
                detail_snap = part_eval.copy() if light else part.copy()
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
                if light and len(detail_snap.index) > 120:
                    detail_snap = detail_snap.head(120)
                for _, row in detail_snap.iterrows():
                    row_entity_key = str(row.get('status_entity_key') or row.get('site') or row.get('entity_key') or '').strip()
                    row_site_key = str(row.get('site') or '').strip()
                    row_meta_campaign = str(row.get('meta_campaign') or '').strip().upper()
                    if light:
                        src = {}
                        matched_cand = ''
                        meta_spend_v = safe_float(row.get('spend'))
                        spend_source = 'status_spend'
                        status_spend_v = meta_spend_v
                        status_meta_spend_v = meta_spend_v
                        source_spend_v = 0.0
                        adx_revenue_v = safe_float(row.get('adx_revenue'))
                        adsense_estimated_earnings_v = safe_float(row.get('adsense_estimated_earnings'))
                        mapped_revenue_source_v = str(row.get('mapped_revenue_source') or '').strip().upper()
                        revenue_v = safe_float(row.get('revenue_value'))
                        roi_v = ((revenue_v - meta_spend_v) / meta_spend_v * 100.0) if meta_spend_v > 0 else 0.0
                    else:
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
                        'site': str(row.get('site') or ''),
                        'score': safe_float(row.get('score')),
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
                        'traffic_score': traffic_now,
                        'delivery_score': delivery_now,
                        'yield_score': yield_now,
                        'quality_score': float(blended_snapshot.get('quality_score', current_snapshot.get('quality_score', avg(snap, 'quality_score', weighted=True)))),
                        'revenue_score': revenue_now,
                        'efficiency_score': float(blended_snapshot.get('efficiency_score', current_snapshot.get('efficiency_score', avg(snap, 'efficiency_score', weighted=True)))),
                        'engagement_score': float(blended_snapshot.get('engagement_score', current_snapshot.get('engagement_score', avg(snap, 'engagement_score', weighted=True)))),
                        'control_score': float(blended_snapshot.get('control_score', current_snapshot.get('control_score', avg(snap, 'control_score', weighted=True)))),
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
                        'historical_blend': None if light else {
                            'window_mode': 'trailing_days_excluding_today',
                            'lookback_days': int(history_lookback_days),
                            'available_history_dates': [d.isoformat() for d in available_hist_dates],
                            'weights_used': blended_snapshot.get('weights_used', {}),
                            'current_snapshot': current_snapshot,
                            'windows': historical_windows,
                            'blended_metrics': {
                                'health_score': health,
                                'ivt_risk_score': risk,
                                'adjustment_score': adj,
                                'confidence': conf,
                                'decision_margin': dm,
                                'roi': roi_total,
                                'label': label,
                                'score': score,
                                'decision': decision_final,
                            },
                        },
                        'scoring_source': 'status_light_aggregate' if light else 'status_event_aggregate',
                        'scoring_source_label': 'Scoring ringan (dashboard recap)' if light else 'Agregasi historical fact_site_country_status_history + fact_change_event_long'
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
            return JsonResponse({'status': True, 'data': out, 'debug': {'target_date': target_date, 'requested_entities': entities, 'returned_entity_keys': list(out.keys())}}, safe=False)
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
            # Safety / IVT first, profit sebagai optimizer sekunder
            source_mode_key = str(source_mode or 'BLENDED').strip().upper()
            single_source = source_mode_key in ["ADX_ONLY", "ADSENSE_ONLY"]
            down_score_cut = 56 if single_source else 58
            down_dm_cut = -10 if single_source else -8
            down_anomaly_cut = 2
            profit_component = clip((float(roi_value) + 1.0) * 50.0, 0.0, 100.0)
            score = (((health + 100.0) / 2.0) * 0.27) + ((100.0 - risk) * 0.33) + (conf01 * 100.0 * 0.12) + (clip(dm + 50.0, 0.0, 100.0) * 0.18) + (profit_component * 0.10)
            score = int(round(clip(score, 0.0, 100.0)))
            negative_labels = ["TRAFFIC_DROP", "SERVING_DROP", "YIELD_DROP", "VIEWABILITY_DROP", "EFFICIENCY_DROP", "REVENUE_DROP", "NEGATIVE_MIXED", "NEG_ADJUSTMENT", "WATCH_NEGATIVE", "WATCH_DECAY", "WATCH_IVT", "IVT_DOMINANT_RISK", "PROFIT_OK_BUT_UNSAFE", "TRAFFIC_QUALITY_MISMATCH"]
            hard_pause_labels = ["NEG_ADJUSTMENT", "WATCH_IVT", "IVT_DOMINANT_RISK", "PROFIT_OK_BUT_UNSAFE"]
            soft_pause_labels = ["WATCH_NEGATIVE", "WATCH_DECAY", "TRAFFIC_QUALITY_MISMATCH"]
            label_up = str(label or 'STABLE').strip().upper()
            anomaly_cards = []
            if risk >= 70: anomaly_cards.append('IVT_RISK')
            if adj <= -60: anomaly_cards.append('NEG_ADJUSTMENT')
            if dm <= -50: anomaly_cards.append('MARGIN_CRASH')
            anomaly_pressure = len(anomaly_cards)
            red_flag_stop = label_up.startswith("RED_FLAG")
            hard_pause_signal = (label_up in hard_pause_labels) and (
                (risk >= 60) or (dm <= -25) or (adj <= -30) or (anomaly_pressure > 0)
            )
            soft_pause_signal = (label_up in soft_pause_labels) and (
                (risk >= 68) or (dm <= -35) or ((adj <= -35) and (health <= -15)) or (anomaly_pressure > 0)
            )
            pause_signal = hard_pause_signal or soft_pause_signal
            decision = "HOLD"
            if red_flag_stop:
                decision = "STOP"
            elif pause_signal or ((risk >= 82) and (conf01 >= 0.50)) or ((dm <= -55) and (health <= -18) and (conf01 >= 0.50)):
                decision = "PAUSE"
            elif risk >= 68 or ((anomaly_pressure >= down_anomaly_cut) and risk >= 58):
                decision = "SCALE_DOWN"
            elif (label_up in ["POSITIVE_EXPANSION", "POSITIVE_RECOVERY", "WATCH_POSITIVE"] and score >= 66 and risk < 42 and dm >= 8 and conf01 >= 0.50 and health >= 8) or (score >= 76 and risk < 38 and dm >= 12 and conf01 >= 0.55 and health >= 12):
                decision = "SCALE UP"
            elif (label_up in negative_labels and ((score < down_score_cut and dm < down_dm_cut) or (health < -10 and adj < -15))) or (score < 48 and (health < -10 or risk >= 60)):
                decision = "SCALE_DOWN"

            if (decision in ["SCALE_DOWN", "PAUSE"]) and (label_up in ["WATCH_DECAY", "WATCH_NEGATIVE"]) and (risk < 35) and (score >= 45) and (anomaly_pressure == 0):
                decision = "HOLD"
            if decision == "SCALE_DOWN":
                decision = "SCALE DOWN"
            return score, decision

        def aggregate_snapshot(frame: pd.DataFrame, country_mode: bool = False) -> dict:
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
            if (not country_mode) and 'country_code' in tmp.columns:
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
                'health_score': float(health),
                'ivt_risk_score': float(risk),
                'adjustment_score': float(adj),
                'confidence': float(conf01),
                'decision_margin': float(dm),
                'score': int(score),
                'decision': str(decision),
                'label': str(label),
                'reason_summary': str(reason_summary),
                'updated_at': str(last_rt),
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
            country_code_raw = str(payload.get('country_code') or payload.get('country_cd') or '').strip().upper()
            if country_code_raw == 'TU':
                country_code_raw = 'TR'
            run_hour_req = payload.get('run_hour')
            run_hour_req = hour_key(run_hour_req)
            if not target_date_str or (not domain and not country_code_raw):
                return JsonResponse({'status': False, 'error': 'date dan domain/country_code wajib diisi'}, status=400)
            target_dt = pd.to_datetime(target_date_str, errors='coerce')
            if pd.isna(target_dt):
                return JsonResponse({'status': False, 'error': 'format date tidak valid (YYYY-MM-DD)'}, status=400)
            target_date = target_dt.date()
            compare_offsets = {'h1': 1, 'h3': 3, 'h7': 7, 'h14': 14, 'h28': 28, 'h35': 35}
            compare_dates = {k: (target_date - timedelta(days=v)) for k, v in compare_offsets.items()}
            days_back = max(0, min(int(payload.get('days_back') or 0), 69))

            scoring_module = _get_scoring_concept_module()
            query_df_func = getattr(scoring_module, 'query_df', None)
            if query_df_func is None:
                query_df_func = query_df  # fallback import atas
            status_table = getattr(scoring_module, 'STATUS_TABLE', 'hris_trendHorizone.fact_site_country_status_history')
            source_table = getattr(scoring_module, 'SOURCE_TABLE', 'hris_trendHorizone.fact_join_hourly')

            dates_in = [target_date] + list(compare_dates.values())
            history_dates = [(target_date - timedelta(days=i)) for i in range(days_back, -1, -1)] if days_back > 0 else []
            all_dates = []
            for d in (dates_in + history_dates):
                if d not in all_dates:
                    all_dates.append(d)
            literals_dates = ', '.join([f"toDate('{d.isoformat()}')" for d in all_dates])

            domain_sql = domain.replace("'", "''")
            where_parts = [f"toDate(date) IN ({literals_dates})"]
            if domain:
                where_parts.append(f"lower(site) = '{domain_sql}'")
            if country_code_raw:
                cc_lit = country_code_raw.replace("'", "''")
                if country_code_raw == 'TR':
                    where_parts.append("(upper(country_code) IN ('TR', 'TU'))")
                else:
                    where_parts.append(f"upper(country_code) = '{cc_lit}'")
            where_sql = ' AND '.join(where_parts)
            country_mode = bool(country_code_raw)

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
            WHERE {where_sql}
            """
            sdf = query_df_func(status_sql)
            if sdf is None or sdf.empty:
                return JsonResponse({'status': True, 'data': {'domain': domain, 'country_code': country_code_raw, 'date': target_date_str, 'empty': True}}, safe=False)
            sdf = sdf.copy()
            sdf['date'] = pd.to_datetime(sdf['date'], errors='coerce').dt.date

            # Query sumber (spend/revenue) untuk bantu rekomendasi aksi
            src_where_parts = [f"toDate(date) IN ({literals_dates})"]
            if domain:
                src_where_parts.append(f"lower(site) = '{domain_sql}'")
            src_where_sql = ' AND '.join(src_where_parts)
            src_order_col = 'run_time'
            try:
                src_cols_df = query_df_func(f"DESCRIBE TABLE {source_table}")
                src_cols = set(str(c).strip() for c in (src_cols_df.get('name') or []).tolist())
                if 'mdd' in src_cols:
                    src_order_col = 'mdd'
                elif 'run_time' in src_cols:
                    src_order_col = 'run_time'
            except Exception:
                src_order_col = 'run_time'
            src_sql = f"""
            SELECT
                toDate(date) AS date,
                run_hour,
                sum(meta_spend) AS meta_spend,
                sum(adx_revenue) AS adx_revenue,
                sum(adsense_estimated_earnings) AS adsense_estimated_earnings,
                argMax(mapped_revenue_source, {src_order_col}) AS mapped_revenue_source
            FROM {source_table}
            WHERE {src_where_sql}
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
            cur = aggregate_snapshot(cur_frame, country_mode)
            cur = attach_financial(cur, target_date, cur_hour)
            if cur:
                cur['date'] = target_date.isoformat()
                cur['run_hour'] = cur_hour

            by_day = {}
            by_hour = {}
            hourly_series = {}

            def build_hourly_series(day_value):
                day_frame_all = sdf[sdf['date'] == day_value].copy()
                if day_frame_all.empty:
                    return []
                hours = pd.to_numeric(day_frame_all['run_hour_key'], errors='coerce').dropna().astype(int).tolist()
                seen_hours = []
                series = []
                for hr in sorted(hours):
                    if hr in seen_hours:
                        continue
                    seen_hours.append(hr)
                    hour_frame = day_frame_all[day_frame_all['run_hour_key'] == hr].copy()
                    snap_hour = aggregate_snapshot(hour_frame, country_mode)
                    snap_hour = attach_financial(snap_hour, day_value, hr)
                    if not snap_hour:
                        continue
                    snap_hour['date'] = day_value.isoformat()
                    snap_hour['run_hour'] = int(hr)
                    snap_hour['run_hour_label'] = f"{str(int(hr)).zfill(2)}:00"
                    series.append(snap_hour)
                return series

            hourly_series['current'] = build_hourly_series(target_date)
            for key, dprev in compare_dates.items():
                lh = latest_hour_by_date.get(dprev)
                # daily snapshot = latest hour
                day_frame = sdf[(sdf['date'] == dprev) & (sdf['run_hour_key'] == lh)].copy() if lh is not None else sdf[sdf['date'] == dprev].copy()
                snap_day = aggregate_snapshot(day_frame, country_mode)
                snap_day = attach_financial(snap_day, dprev, lh)
                if snap_day:
                    snap_day['date'] = dprev.isoformat()
                    snap_day['run_hour'] = lh
                by_day[key] = snap_day or {}

                # hour snapshot = same hour as current (jika ada)
                snap_h = {}
                if cur_hour is not None:
                    h_frame = sdf[(sdf['date'] == dprev) & (sdf['run_hour_key'] == cur_hour)].copy()
                    snap_h = aggregate_snapshot(h_frame, country_mode)
                    snap_h = attach_financial(snap_h, dprev, cur_hour)
                    if snap_h:
                        snap_h['date'] = dprev.isoformat()
                        snap_h['run_hour'] = cur_hour
                by_hour[key] = snap_h or {}
                hourly_series[key] = build_hourly_series(dprev)

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

            history_by_day = []
            if days_back > 0:
                for dh in history_dates:
                    lh = latest_hour_by_date.get(dh)
                    day_frame = sdf[(sdf['date'] == dh) & (sdf['run_hour_key'] == lh)].copy() if lh is not None else sdf[sdf['date'] == dh].copy()
                    snap_day = aggregate_snapshot(day_frame, country_mode)
                    snap_day = attach_financial(snap_day, dh, lh)
                    row_hist = {'date': dh.isoformat(), 'run_hour': lh}
                    if snap_day:
                        row_hist.update(snap_day)
                    history_by_day.append(row_hist)
            try:
                from management.scoring_concept import _json_safe_value
            except Exception:
                def _json_safe_value(value):
                    if isinstance(value, dict):
                        return {str(k): _json_safe_value(v) for k, v in value.items()}
                    if isinstance(value, (list, tuple, set)):
                        return [_json_safe_value(v) for v in value]
                    if hasattr(value, 'item'):
                        try:
                            return value.item()
                        except Exception:
                            pass
                    if isinstance(value, (date, datetime)):
                        return value.isoformat()
                    return value

            payload_out = _json_safe_value({
                'domain': domain,
                'country_code': country_code_raw,
                'date': target_date.isoformat(),
                'current': cur or {},
                'compare_offsets': compare_offsets,
                'compare_dates': {k: v.isoformat() for k, v in compare_dates.items()},
                'compare_by_day': by_day,
                'compare_by_hour': by_hour,
                'hourly_series': hourly_series,
                'history_by_day': history_by_day,
                'recommendation': rec,
            })
            return JsonResponse({'status': True, 'data': payload_out}, safe=False)
        except Exception as e:
            logger.exception('DashboardScoringCompareView failed')
            return JsonResponse({'status': False, 'error': str(e), 'traceback': traceback.format_exc()}, status=500)


@method_decorator(csrf_exempt, name='dispatch')
class DashboardScoringRekapCompareView(View):
    """Compare monthly recap pulls vs aggregated daily data for Detail Scoring Invalid data tab."""

    def dispatch(self, request, *args, **kwargs):
        if 'hris_admin' not in request.session:
            return JsonResponse({'status': False, 'error': 'Unauthorized'}, status=401)
        return super().dispatch(request, *args, **kwargs)

    def post(self, req):
        try:
            payload = json.loads((req.body or b'').decode('utf-8') or '{}')
            domain = str(payload.get('domain') or payload.get('site') or '').strip()
            year = payload.get('year') or payload.get('tahun')
            month = payload.get('month') or payload.get('bulan')
            tanggal_tarik = payload.get('tanggal_tarik') or payload.get('tanggal')

            if not domain:
                return JsonResponse({'status': False, 'error': 'domain wajib diisi'}, status=400)
            if not year or not month:
                return JsonResponse({'status': False, 'error': 'year dan month wajib diisi'}, status=400)

            db = data_mysql()
            result = db.get_rekap_vs_daily_compare(domain, year, month, tanggal_tarik)
            if not result.get('status'):
                return JsonResponse({'status': False, 'error': result.get('data') or 'Gagal memuat perbandingan'}, status=400)
            return JsonResponse({'status': True, 'data': result.get('data')}, safe=False)
        except Exception as e:
            logger.exception('DashboardScoringRekapCompareView failed')
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


@method_decorator(csrf_exempt, name='dispatch')
class DashboardTrafficMetricsView(View):
    def dispatch(self, request, *args, **kwargs):
        if 'hris_admin' not in request.session:
            return JsonResponse({'status': False, 'error': 'Unauthorized'}, status=401)
        return super().dispatch(request, *args, **kwargs)

    def post(self, req):
        try:
            payload = json.loads(req.body.decode('utf-8') or '{}')
        except Exception:
            payload = {}

        start_date = str(payload.get('start_date') or payload.get('date') or '').strip()
        end_date = str(payload.get('end_date') or payload.get('date') or start_date).strip()
        domains = payload.get('domains') or []
        subdomain_filter = str(payload.get('subdomain') or '').strip()

        requested_domains = []
        if isinstance(domains, (list, tuple, set)):
            requested_domains.extend([str(item or '').strip() for item in domains if str(item or '').strip()])
        elif isinstance(domains, str):
            requested_domains.extend([str(item or '').strip() for item in domains.split(',') if str(item or '').strip()])
        if subdomain_filter:
            requested_domains.append(subdomain_filter)

        requested_domains = build_domain_filter_terms(requested_domains, include_original=True, include_base=True)
        indexes = _fetch_kiwipixel_campaign_traffic(start_date, end_date)
        by_domain = (indexes or {}).get('by_domain') or {}

        if requested_domains:
            out = {}
            for domain in requested_domains:
                dkey = _normalize_kiwipixel_domain_key(domain)
                if not dkey:
                    continue
                out[dkey] = dict(by_domain.get(dkey) or _zero_kiwipixel_traffic_metrics())
            by_domain = out
        else:
            by_domain = {
                str(k or '').strip(): dict(v or _zero_kiwipixel_traffic_metrics())
                for k, v in by_domain.items()
                if str(k or '').strip()
            }

        return JsonResponse({
            'status': True,
            'data': {
                'start_date': start_date,
                'end_date': end_date,
                'by_domain': by_domain,
            }
        })


@method_decorator(csrf_exempt, name='dispatch')
class DashboardCountryTrafficMetricsView(View):
    def dispatch(self, request, *args, **kwargs):
        if 'hris_admin' not in request.session:
            return JsonResponse({'status': False, 'error': 'Unauthorized'}, status=401)
        return super().dispatch(request, *args, **kwargs)

    def post(self, req):
        try:
            payload = json.loads(req.body.decode('utf-8') or '{}')
        except Exception:
            payload = {}

        start_date = str(payload.get('start_date') or payload.get('date') or '').strip()
        end_date = str(payload.get('end_date') or payload.get('date') or start_date).strip()
        countries = payload.get('countries') or []
        subdomain_filter = str(payload.get('subdomain') or '').strip()
        if subdomain_filter and not _looks_like_kiwipixel_domain(subdomain_filter):
            subdomain_filter = ''
        domains = payload.get('domains') or []

        requested_countries = []
        if isinstance(countries, (list, tuple, set)):
            requested_countries.extend([str(item or '').strip().upper() for item in countries if str(item or '').strip()])
        elif isinstance(countries, str):
            requested_countries.extend([str(item or '').strip().upper() for item in countries.split(',') if str(item or '').strip()])
        requested_countries = [
            _normalize_kiwipixel_country_code(item)
            for item in dict.fromkeys(requested_countries)
            if _normalize_kiwipixel_country_code(item)
        ]

        requested_domains = []
        if isinstance(domains, (list, tuple, set)):
            requested_domains.extend([str(item or '').strip() for item in domains if str(item or '').strip()])
        elif isinstance(domains, str):
            requested_domains.extend([str(item or '').strip() for item in domains.split(',') if str(item or '').strip()])
        if subdomain_filter:
            requested_domains.append(subdomain_filter)
        requested_domains = [
            item for item in build_domain_filter_terms(requested_domains, include_original=True, include_base=True)
            if _looks_like_kiwipixel_domain(item)
        ]

        metrics_by_country = _fetch_kiwipixel_country_metrics(
            requested_countries,
            start_date,
            end_date,
            requested_domains,
        )

        by_country = {
            str(code or '').strip().upper(): dict(metrics or _zero_kiwipixel_traffic_metrics())
            for code, metrics in (metrics_by_country or {}).items()
            if str(code or '').strip()
        }

        if requested_countries:
            out = {}
            for code in requested_countries:
                out[code] = dict(by_country.get(code) or _zero_kiwipixel_traffic_metrics())
            by_country = out

        return JsonResponse({
            'status': True,
            'data': {
                'start_date': start_date,
                'end_date': end_date,
                'by_country': by_country,
            }
        })

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
            selected_domain_list = [
                str(s).strip() for s in data_domain.split(',')
                if str(s).strip() and str(s).strip() != '%'
            ]
        selected_account_list = _resolve_fb_account_filters(selected_account_list)
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
        # Normalisasi kolom agar cocok dengan harapan di management/static/ajax/admin/facebook_ads/summary.js
        normalized_rows = []
        total_spend = 0.0
        total_impressions = 0
        total_reach = 0
        total_clicks = 0
        for row in raw_rows or []:
            account_name = str(row.get('account_name') or '').strip()
            if not account_name:
                account_name = str(row.get('account_id') or row.get('account_email') or '-').strip()
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

        kiwi_traffic_indexes = _fetch_kiwipixel_campaign_traffic(tanggal_dari, tanggal_sampai)
        unique_domains = list({
            str(row.get('domain') or '').strip()
            for row in normalized_rows
            if str(row.get('domain') or '').strip()
        })
        if selected_domain_list:
            unique_domains.extend(selected_domain_list)
        campaign_ids_by_domain = _fetch_fb_campaign_ids_by_domain(
            tanggal_dari,
            tanggal_sampai,
            unique_domains,
        )
        total_visits_sum = 0
        total_unique_sum = 0
        total_pageviews_sum = 0
        seen_domain_keys = set()
        for row in normalized_rows:
            domain_key = _normalize_kiwipixel_domain_key(row.get('domain'))
            campaign_ids = campaign_ids_by_domain.get(domain_key, [])
            metrics = _resolve_kiwipixel_campaign_visitors(
                row.get('domain'),
                campaign_ids,
                kiwi_traffic_indexes,
            )
            row['total_visits'] = metrics.get('total_visits', 0)
            row['unique_visitor'] = metrics.get('unique_visitor', 0)
            row['total_pageviews'] = metrics.get('total_pageviews', 0)
            row['total_visitors'] = metrics.get('total_visits', 0)
            if domain_key and domain_key not in seen_domain_keys:
                seen_domain_keys.add(domain_key)
                total_visits_sum += int(metrics.get('total_visits') or 0)
                total_unique_sum += int(metrics.get('unique_visitor') or 0)
                total_pageviews_sum += int(metrics.get('total_pageviews') or 0)

        monitor_rows = []
        try:
            accounts_all = data_mysql().master_account_ads().get('data', [])

            # Pilih account sesuai filter (jika tidak ada -> semua account)
            if selected_account_list:
                selected_set = set([str(x) for x in selected_account_list])
                accounts_target = [
                    a for a in accounts_all
                    if str(a.get('account_id')) in selected_set
                    or str(a.get('account_ads_id')) in selected_set
                ]
            else:
                accounts_target = accounts_all

            domain_filter_for_api = str(data_domain or '').strip()
            if domain_filter_for_api in ('', '%'):
                domain_filter_for_api = '%'

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
                'total_visitors': total_visits_sum,
                'total_visits': total_visits_sum,
                'unique_visitor': total_unique_sum,
                'total_pageviews': total_pageviews_sum,
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

class CreateCampaignFacebookAds(View):
    def dispatch(self, request, *args, **kwargs):
        if 'hris_admin' not in request.session:
            return redirect('admin_login')
        return super(CreateCampaignFacebookAds, self).dispatch(request, *args, **kwargs)
    def get(self, req):
        rs_accounts = data_mysql().master_account_ads()
        account_rows = (rs_accounts or {}).get('data') if isinstance(rs_accounts, dict) else []
        if not isinstance(account_rows, list):
            account_rows = []
        data = {
            'title': 'Create Campaign Facebook Ads',
            'user': req.session['hris_admin'],
            'data_account': account_rows,
            'account_rows': account_rows,
            'today': datetime.now().strftime('%Y-%m-%d'),
            'seven_days_ago': (datetime.now() - timedelta(days=6)).strftime('%Y-%m-%d'),
        }
        return render(req, 'admin/facebook_ads/create_campaign/index.html', data)

class CreateCampaignMetaListView(View):
    def dispatch(self, request, *args, **kwargs):
        if 'hris_admin' not in request.session:
            return JsonResponse({'status': False, 'error': 'Unauthorized'}, status=401)
        return super(CreateCampaignMetaListView, self).dispatch(request, *args, **kwargs)
    def get(self, req):
        selected = [str(x).strip() for x in str(req.GET.get('selected_accounts') or '').split(',') if str(x).strip()]
        keyword = str(req.GET.get('keyword') or '').strip().lower()
        tanggal_dari = str(req.GET.get('tanggal_dari') or '').strip() or datetime.now().strftime('%Y-%m-%d')
        tanggal_sampai = str(req.GET.get('tanggal_sampai') or '').strip() or tanggal_dari
        ignore_date = str(req.GET.get('ignore_date') or '0').strip() == '1'
        if tanggal_dari > tanggal_sampai:
            tanggal_dari, tanggal_sampai = tanggal_sampai, tanggal_dari
        if not selected:
            return JsonResponse({'status': True, 'data': [], 'tanggal_dari': tanggal_dari, 'tanggal_sampai': tanggal_sampai, 'message': 'Pilih account terlebih dahulu'})
        rows = []
        accounts = data_mysql().master_account_ads()['data'] or []
        accounts = [a for a in accounts if str((a or {}).get('account_id') or '').strip() in selected]
        fields = 'id,name,status,effective_status,objective,buying_type,daily_budget,lifetime_budget,created_time,updated_time'
        for acc in accounts:
            token = str((acc or {}).get('access_token') or '').strip()
            real_id = str((acc or {}).get('account_id') or '').replace('act_', '').strip()
            if not token or not real_id:
                continue
            try:
                resp = requests.get(f'https://graph.facebook.com/v22.0/act_{real_id}/campaigns', params={'access_token': token, 'fields': fields, 'limit': 50}, timeout=12)
                body = resp.json() if resp.text else {}
            except Exception:
                continue
            if resp.status_code >= 400 or (isinstance(body, dict) and body.get('error')):
                continue
            for item in ((body or {}).get('data') or []):
                name = str((item or {}).get('name') or '').strip(); created_at = str((item or {}).get('created_time') or '').strip(); updated_at = str((item or {}).get('updated_time') or '').strip()
                created_date = created_at[:10] if len(created_at) >= 10 else ''; updated_date = updated_at[:10] if len(updated_at) >= 10 else ''
                date_match = ignore_date or ((created_date and tanggal_dari <= created_date <= tanggal_sampai) or (updated_date and tanggal_dari <= updated_date <= tanggal_sampai))
                if not (date_match and (not keyword or keyword in name.lower())):
                    continue
                rows.append({'account_id': str((acc or {}).get('account_id') or '').strip(), 'account_name': str((acc or {}).get('account_name') or '').strip(), 'campaign_id': str((item or {}).get('id') or '').strip(), 'campaign_name': name, 'status': str((item or {}).get('status') or '').strip(), 'effective_status': str((item or {}).get('effective_status') or '').strip(), 'objective': str((item or {}).get('objective') or '').strip(), 'buying_type': str((item or {}).get('buying_type') or '').strip(), 'daily_budget': (item or {}).get('daily_budget'), 'lifetime_budget': (item or {}).get('lifetime_budget'), 'created_time': created_at, 'updated_time': updated_at})
        rows.sort(key=lambda x: ((x.get('updated_time') or ''), (x.get('created_time') or '')), reverse=True)
        return JsonResponse({'status': True, 'data': rows[:50], 'tanggal_dari': tanggal_dari, 'tanggal_sampai': tanggal_sampai, 'message': f'{len(rows[:50])} campaign terbaru ditampilkan'})

class GetCampaignMetaDetailView(View):
    def dispatch(self, request, *args, **kwargs):
        if 'hris_admin' not in request.session:
            return JsonResponse({'success': False, 'message': 'Unauthorized'}, status=401)
        return super(GetCampaignMetaDetailView, self).dispatch(request, *args, **kwargs)

    @staticmethod
    def _parse_meta_targeting(raw):
        if isinstance(raw, dict):
            return raw
        if isinstance(raw, str):
            s = raw.strip()
            if not s:
                return {}
            try:
                parsed = json.loads(s)
                return parsed if isinstance(parsed, dict) else {}
            except Exception:
                return {}
        return {}

    @staticmethod
    def _parse_custom_audience_items(raw_list):
        items = []
        for x in (raw_list if isinstance(raw_list, list) else []):
            if isinstance(x, dict):
                iid = str(x.get('id') or '').strip()
                name = str(x.get('name') or iid).strip()
            else:
                iid = str(x or '').strip()
                name = iid
            if iid:
                items.append({'id': iid, 'name': name})
        return items

    @staticmethod
    def _parse_flexible_spec_group(spec):
        if not isinstance(spec, dict):
            return []
        type_labels = {
            'interests': 'Minat',
            'behaviors': 'Perilaku',
            'life_events': 'Peristiwa Penting',
            'industries': 'Industri',
            'income': 'Pendapatan',
            'family_statuses': 'Orang Tua',
            'relationship_statuses': 'Status hubungan',
            'education_statuses': 'Tingkat Pendidikan',
            'work_positions': 'Jabatan',
            'work_employers': 'Pengusaha',
            'education_majors': 'Bidang Studi',
            'education_schools': 'Sekolah',
            'college_years': 'Tahun Kuliah',
        }
        items = []
        for field, label in type_labels.items():
            for x in (spec.get(field) if isinstance(spec.get(field), list) else []):
                if isinstance(x, dict):
                    iid = str(x.get('id') or x.get('key') or '').strip()
                    name = str(x.get('name') or x.get('text') or iid).strip()
                else:
                    iid = str(x or '').strip()
                    name = iid
                if not iid:
                    continue
                items.append({'id': iid, 'name': name, 'category': label, 'path': [label]})
        return items

    @staticmethod
    def _dedupe_targeting_items(items):
        out = []
        seen = set()
        for it in (items or []):
            iid = str((it or {}).get('id') or '').strip()
            if not iid or iid in seen:
                continue
            seen.add(iid)
            out.append(it)
        return out

    @staticmethod
    def _parse_detailed_targeting(tg):
        specs = tg.get('flexible_spec') if isinstance(tg.get('flexible_spec'), list) else []
        include_items = []
        narrow_items = []
        if len(specs) >= 2:
            include_items.extend(GetCampaignMetaDetailView._parse_flexible_spec_group(specs[0]))
            for spec in specs[1:]:
                narrow_items.extend(GetCampaignMetaDetailView._parse_flexible_spec_group(spec))
        else:
            for spec in specs:
                include_items.extend(GetCampaignMetaDetailView._parse_flexible_spec_group(spec))
        top_interests = tg.get('interests') if isinstance(tg.get('interests'), list) else []
        for x in top_interests:
            if isinstance(x, dict):
                iid = str(x.get('id') or x.get('key') or '').strip()
                name = str(x.get('name') or iid).strip()
            else:
                iid = str(x or '').strip()
                name = iid
            if iid:
                include_items.append({'id': iid, 'name': name, 'category': 'Minat', 'path': ['Minat']})
        return {
            'include': GetCampaignMetaDetailView._dedupe_targeting_items(include_items),
            'narrow': GetCampaignMetaDetailView._dedupe_targeting_items(narrow_items),
        }

    @staticmethod
    def _resolve_targeting_item_names(token, items):
        rows = list(items or [])
        if not token or not rows:
            return rows
        needs_resolve = []
        for it in rows:
            iid = str(it.get('id') or '').strip()
            if not iid:
                continue
            name = str(it.get('name') or '').strip()
            path = it.get('path') if isinstance(it.get('path'), list) else []
            if (not name or name == iid) or len(path) < 2:
                needs_resolve.append(it)
        if not needs_resolve:
            return rows
        ids = [str(it.get('id') or '').strip() for it in needs_resolve if str(it.get('id') or '').strip()]
        if not ids:
            return rows
        try:
            resp = requests.get(
                'https://graph.facebook.com/v22.0/',
                params={'access_token': token, 'ids': ','.join(ids[:50]), 'fields': 'name,path'},
                timeout=20,
            )
            body = resp.json() if resp.text else {}
            if not isinstance(body, dict):
                return rows
            for it in needs_resolve:
                node = body.get(str(it.get('id') or '').strip())
                if not isinstance(node, dict):
                    continue
                name = str(node.get('name') or '').strip()
                if name:
                    it['name'] = name
                path = node.get('path') if isinstance(node.get('path'), list) else []
                if path:
                    it['path'] = [str(p) for p in path if str(p or '').strip()]
                    it['category'] = str(path[0])
        except Exception:
            pass
        return rows

    @staticmethod
    def _merge_targeting_dicts(*layers):
        merged = {}
        flex_specs = []
        for layer in layers:
            if not isinstance(layer, dict):
                continue
            cur = dict(layer)
            fs = cur.pop('flexible_spec', None)
            if isinstance(fs, list):
                flex_specs.extend([x for x in fs if isinstance(x, dict)])
            for key, val in cur.items():
                if val in (None, '', [], {}):
                    continue
                if key not in merged or merged.get(key) in (None, '', [], {}):
                    merged[key] = val
        if flex_specs:
            merged['flexible_spec'] = flex_specs
        return merged

    @staticmethod
    def _fetch_targeting_sentence_lines(token, adset_id):
        if not token or not adset_id:
            return []
        try:
            resp = requests.get(
                f'https://graph.facebook.com/v22.0/{adset_id}/targetingsentencelines',
                params={'access_token': token, 'fields': 'id,params,targetingsentencelines,content'},
                timeout=20,
            )
            body = resp.json() if resp.text else {}
            if isinstance(body, dict):
                lines = body.get('targetingsentencelines')
                if isinstance(lines, list):
                    return lines
                data = body.get('data')
                if isinstance(data, list):
                    return data
        except Exception:
            pass
        return []

    @staticmethod
    def _targeting_richness_score(targeting):
        tg = GetCampaignMetaDetailView._parse_meta_targeting(targeting)
        score = 0
        flex_specs = tg.get('flexible_spec') if isinstance(tg.get('flexible_spec'), list) else []
        for spec in flex_specs:
            if not isinstance(spec, dict):
                continue
            for val in spec.values():
                if isinstance(val, list):
                    score += 10 * len(val)
        for key in ('custom_audiences', 'excluded_custom_audiences', 'interests', 'behaviors'):
            vals = tg.get(key) if isinstance(tg.get(key), list) else []
            score += 5 * len(vals)
        if tg.get('locales'):
            score += 2
        geo = tg.get('geo_locations') if isinstance(tg.get('geo_locations'), dict) else {}
        if geo.get('regions') or geo.get('cities'):
            score += 3
        return score

    @staticmethod
    def _pick_adset_row(rows, preferred_id=None):
        items = [r for r in (rows or []) if isinstance(r, dict)]
        if not items:
            return None
        pref = str(preferred_id or '').strip()
        if pref:
            for row in items:
                if str(row.get('id') or '').strip() == pref:
                    return row
        return max(items, key=lambda r: GetCampaignMetaDetailView._targeting_richness_score(r.get('targeting')))

    @staticmethod
    def _load_adset_targeting(token, adset_id, fallback_targeting=None):
        layers = [GetCampaignMetaDetailView._parse_meta_targeting(fallback_targeting)]
        if not adset_id:
            return GetCampaignMetaDetailView._merge_targeting_dicts(*layers)
        try:
            adset_resp = requests.get(
                f'https://graph.facebook.com/v22.0/{adset_id}',
                params={'access_token': token, 'fields': 'targeting,targetingsentencelines{params,targetingsentencelines}'},
                timeout=20,
            )
            adset_body = adset_resp.json() if adset_resp.text else {}
            if isinstance(adset_body, dict):
                if adset_body.get('targeting'):
                    layers.append(GetCampaignMetaDetailView._parse_meta_targeting(adset_body.get('targeting')))
                nested = adset_body.get('targetingsentencelines')
                nested_lines = []
                if isinstance(nested, dict):
                    nested_lines = nested.get('targetingsentencelines') or nested.get('data') or []
                elif isinstance(nested, list):
                    nested_lines = nested
                if isinstance(nested_lines, list):
                    for line in nested_lines:
                        if isinstance(line, dict) and line.get('params'):
                            layers.append(GetCampaignMetaDetailView._parse_meta_targeting(line.get('params')))
        except Exception:
            pass
        for line in GetCampaignMetaDetailView._fetch_targeting_sentence_lines(token, adset_id):
            if isinstance(line, dict):
                layers.append(GetCampaignMetaDetailView._parse_meta_targeting(line.get('params')))
        return GetCampaignMetaDetailView._merge_targeting_dicts(*layers)

    @staticmethod
    def _interest_labels_from_sentence_lines(sentence_rows):
        labels = []
        seen = set()
        interest_headers = (
            'minat', 'interest', 'penargetan terperinci', 'detailed targeting',
            'perilaku', 'behavior', 'demograf', 'demographic', 'shopping', 'beauty', 'fashion',
        )

        def add_label(raw):
            label = str(raw or '').strip()
            if not label:
                return
            if '>' in label:
                label = label.split('>')[-1].strip()
            label = re.sub(r'\s*\([^)]*\)\s*$', '', label).strip()
            key = label.lower()
            if label and key not in seen:
                seen.add(key)
                labels.append(label)

        def walk(node):
            if not isinstance(node, dict):
                return
            content = str(node.get('content') or '').strip()
            children = node.get('children') if isinstance(node.get('children'), list) else []
            header = content.lower().rstrip(':').strip()
            if header and any(marker in header for marker in interest_headers):
                for child in children:
                    text = str(child or '').strip()
                    if not text:
                        continue
                    for part in re.split(r'[;,]', text):
                        add_label(part)
            for child in children:
                if isinstance(child, dict):
                    walk(child)

        for row in (sentence_rows or []):
            if isinstance(row, dict):
                walk(row)
        return labels

    @staticmethod
    def _resolve_interest_labels(token, labels):
        items = []
        seen = set()
        for label in (labels or []):
            name = str(label or '').strip()
            if not name:
                continue
            try:
                resp = requests.get(
                    'https://graph.facebook.com/v22.0/search',
                    params={'access_token': token, 'type': 'adinterest', 'q': name, 'limit': 8},
                    timeout=20,
                )
                body = resp.json() if resp.text else {}
                rows = (body.get('data') or []) if isinstance(body, dict) else []
                picked = None
                for row in rows:
                    if not isinstance(row, dict):
                        continue
                    rid = str(row.get('id') or '').strip()
                    rname = str(row.get('name') or '').strip()
                    if not rid:
                        continue
                    if rname.lower() == name.lower():
                        picked = row
                        break
                    if not picked:
                        picked = row
                if picked:
                    rid = str(picked.get('id') or '').strip()
                    if rid and rid not in seen:
                        seen.add(rid)
                        path = picked.get('path') if isinstance(picked.get('path'), list) else []
                        items.append({
                            'id': rid,
                            'name': str(picked.get('name') or name).strip(),
                            'category': str(path[0] if path else 'Minat'),
                            'path': [str(p) for p in path if str(p or '').strip()] or ['Minat'],
                        })
            except Exception:
                continue
        return items

    @staticmethod
    def _parse_language_items(raw_locales):
        locale_names = {
            '42': 'Bahasa Indonesia', '6': 'English (All)', '1002': 'English (US)', '1001': 'English (UK)',
            '3': 'Spanish', '10': 'Arabic', '35': 'Malay', '52': 'Thai', '53': 'Vietnamese', '7': 'French',
            '5': 'German', '8': 'Italian', '9': 'Portuguese', '11': 'Japanese', '12': 'Korean',
            '13': 'Chinese (Simplified)', '14': 'Chinese (Traditional)',
        }
        items = []
        for x in (raw_locales if isinstance(raw_locales, list) else []):
            key = str(x or '').strip()
            if not key:
                continue
            items.append({'id': key, 'name': locale_names.get(key, key)})
        return items

    @staticmethod
    def _story_block_from_spec(spec):
        if not isinstance(spec, dict):
            return {}, ''
        for key in ('link_data', 'video_data', 'photo_data', 'template_data'):
            block = spec.get(key)
            if isinstance(block, dict) and block:
                return block, key
        return {}, ''

    @staticmethod
    def _first_asset_feed_value(rows, *keys):
        for row in (rows if isinstance(rows, list) else []):
            if not isinstance(row, dict):
                continue
            for key in keys:
                val = str(row.get(key) or '').strip()
                if val:
                    return val
        return ''

    @staticmethod
    def _parse_post_attachments(post_data):
        out = {'video_id': '', 'image_url': '', 'link': '', 'video_source': ''}
        attachments = post_data.get('attachments') if isinstance(post_data.get('attachments'), dict) else {}
        rows = attachments.get('data') if isinstance(attachments.get('data'), list) else []
        for att in rows:
            if not isinstance(att, dict):
                continue
            media = att.get('media') if isinstance(att.get('media'), dict) else {}
            media_type = str(att.get('media_type') or '').strip().lower()
            if media_type == 'video':
                out['video_id'] = out['video_id'] or str(media.get('id') or '').strip()
                out['video_source'] = out['video_source'] or str(media.get('source') or '').strip()
                image = media.get('image') if isinstance(media.get('image'), dict) else {}
                out['image_url'] = out['image_url'] or str(image.get('src') or '').strip()
            else:
                image = media.get('image') if isinstance(media.get('image'), dict) else {}
                out['image_url'] = out['image_url'] or str(image.get('src') or '').strip()
            out['link'] = out['link'] or str(att.get('url') or media.get('source') or '').strip()
            sub = att.get('subattachments') if isinstance(att.get('subattachments'), dict) else {}
            for sub_att in (sub.get('data') if isinstance(sub.get('data'), list) else []):
                if not isinstance(sub_att, dict):
                    continue
                sub_media = sub_att.get('media') if isinstance(sub_att.get('media'), dict) else {}
                if str(sub_att.get('media_type') or '').strip().lower() == 'video':
                    out['video_id'] = out['video_id'] or str(sub_media.get('id') or '').strip()
                    out['video_source'] = out['video_source'] or str(sub_media.get('source') or '').strip()
                sub_image = sub_media.get('image') if isinstance(sub_media.get('image'), dict) else {}
                out['image_url'] = out['image_url'] or str(sub_image.get('src') or '').strip()
        return out

    @staticmethod
    def _fetch_video_thumb(token, video_id):
        if not token or not video_id:
            return ''
        try:
            resp = requests.get(
                f'https://graph.facebook.com/v22.0/{video_id}',
                params={'access_token': token, 'fields': 'picture,thumbnails{uri,is_preferred}'},
                timeout=15,
            )
            body = resp.json() if resp.text else {}
            if not isinstance(body, dict):
                return ''
            thumbs = body.get('thumbnails') if isinstance(body.get('thumbnails'), dict) else {}
            rows = thumbs.get('data') if isinstance(thumbs.get('data'), list) else []
            preferred = next((x for x in rows if isinstance(x, dict) and x.get('is_preferred')), None)
            pick = preferred or (rows[0] if rows else None)
            if isinstance(pick, dict) and pick.get('uri'):
                return str(pick.get('uri') or '')
            return str(body.get('picture') or '')
        except Exception:
            return ''

    @staticmethod
    def _fetch_graph_name(token, node_id):
        if not token or not node_id:
            return ''
        try:
            resp = requests.get(
                f'https://graph.facebook.com/v22.0/{node_id}',
                params={'access_token': token, 'fields': 'name,username'},
                timeout=15,
            )
            body = resp.json() if resp.text else {}
            if isinstance(body, dict):
                return str(body.get('name') or body.get('username') or '').strip()
        except Exception:
            pass
        return ''

    def get(self, req):
        account_id = str(req.GET.get('account_id') or '').strip(); campaign_id = str(req.GET.get('campaign_id') or '').strip()
        preferred_adset_id = str(req.GET.get('adset_id') or '').strip()
        if not account_id or not campaign_id:
            return JsonResponse({'success': False, 'message': 'Account dan campaign wajib diisi'})
        rs = data_mysql().master_account_ads_by_id({'data_account': account_id}); acc = (rs or {}).get('data') if isinstance(rs, dict) else None
        if not isinstance(acc, dict):
            return JsonResponse({'success': False, 'message': 'Account tidak ditemukan'})
        token = str(acc.get('access_token') or '').strip()
        fields = 'id,name,objective,status,buying_type,special_ad_categories,daily_budget,lifetime_budget,spend_cap'
        resp = requests.get(f'https://graph.facebook.com/v22.0/{campaign_id}', params={'access_token': token, 'fields': fields}, timeout=20)
        body = resp.json() if resp.text else {}
        if resp.status_code >= 400 or (isinstance(body, dict) and body.get('error')):
            return JsonResponse({'success': False, 'message': str(((body or {}).get('error') or {}).get('message') or 'Gagal mengambil detail campaign')})
        data = body if isinstance(body, dict) else {}
        cats = data.get('special_ad_categories') if isinstance(data.get('special_ad_categories'), list) else []
        adset_data = {}
        ad_data = {}
        try:
            aresp = requests.get(
                f'https://graph.facebook.com/v22.0/{campaign_id}/adsets',
                params={'access_token': token, 'fields': 'id,name,daily_budget,lifetime_budget,start_time,end_time,optimization_goal,bid_strategy,bid_amount,is_dynamic_creative,attribution_spec,targeting', 'limit': 25},
                timeout=20
            )
            abody = aresp.json() if aresp.text else {}
            adset_rows = (abody or {}).get('data') or [] if isinstance(abody, dict) else []
            row = self._pick_adset_row(adset_rows, preferred_adset_id)
            if isinstance(row, dict):
                adset_id = str(row.get('id') or '').strip()
                tg = self._load_adset_targeting(token, adset_id, row.get('targeting'))
                sentence_rows = self._fetch_targeting_sentence_lines(token, adset_id)
                geo = tg.get('geo_locations') if isinstance(tg.get('geo_locations'), dict) else {}
                exg = tg.get('excluded_geo_locations') if isinstance(tg.get('excluded_geo_locations'), dict) else {}
                gl = {}
                region_countries = set()
                def keys(xs, kind):
                    out = []
                    for x in (xs if isinstance(xs, list) else []):
                        if isinstance(x, dict):
                            k = str(x.get('key') or x.get('id') or x.get('country') or '').strip()
                            n = str(x.get('name') or x.get('region_name') or x.get('city_name') or k).strip()
                            if kind == 'region':
                                cc = str(x.get('country') or '').strip().upper()
                                if cc:
                                    region_countries.add(cc)
                            if k:
                                out.append(k)
                                gl[f'{kind}:{k}'] = n
                        elif x is not None and str(x).strip():
                            k = str(x).strip()
                            out.append(k)
                            gl[f'{kind}:{k}'] = k
                    return out
                genders = tg.get('genders') if isinstance(tg.get('genders'), list) else []
                age_range = tg.get('age_range') if isinstance(tg.get('age_range'), list) else []
                ui_age_min = tg.get('age_min') or 18
                ui_age_max = tg.get('age_max') or 65
                if len(age_range) >= 2:
                    try:
                        ui_age_min = int(age_range[0])
                        ui_age_max = int(age_range[1])
                    except Exception:
                        pass
                attrs = row.get('attribution_spec') if isinstance(row.get('attribution_spec'), list) else []
                attr = '7d_click_1d_view'
                if any(str(x.get('event_type') or '').upper() == 'CLICK_THROUGH' and int(x.get('window_days') or 0) == 1 for x in attrs): attr = '1d_click'
                elif any(str(x.get('event_type') or '').upper() == 'CLICK_THROUGH' and int(x.get('window_days') or 0) == 7 for x in attrs): attr = '7d_click'
                def _fmt_meta_dt(raw):
                    return fmt_meta_adset_dt_local(raw)
                pub_platforms = tg.get('publisher_platforms') if isinstance(tg.get('publisher_platforms'), list) else []
                placement_mode = 'manual' if pub_platforms else 'auto'
                placement_positions = {}
                for plat, field in (
                    ('facebook', 'facebook_positions'),
                    ('instagram', 'instagram_positions'),
                    ('audience_network', 'audience_network_positions'),
                    ('messenger', 'messenger_positions'),
                    ('threads', 'threads_positions'),
                ):
                    vals = tg.get(field) if isinstance(tg.get(field), list) else []
                    if vals:
                        cleaned = [str(v).strip() for v in vals if str(v).strip()]
                        if plat == 'facebook':
                            fb_map = {'video_feeds': 'facebook_reels', 'suggested_video': 'facebook_reels'}
                            mapped = []
                            for v in cleaned:
                                v = fb_map.get(v, v)
                                if v in ('video_feeds', 'suggested_video'):
                                    continue
                                if v not in mapped:
                                    mapped.append(v)
                            cleaned = mapped
                        placement_positions[plat] = cleaned
                device_platforms = tg.get('device_platforms') if isinstance(tg.get('device_platforms'), list) else []
                if device_platforms == ['mobile']:
                    placement_device_mode = 'mobile'
                elif device_platforms == ['desktop']:
                    placement_device_mode = 'desktop'
                else:
                    placement_device_mode = 'all'
                targeting_automation = tg.get('targeting_automation') if isinstance(tg.get('targeting_automation'), dict) else {}
                advantage_audience = targeting_automation.get('advantage_audience')
                advantage = '1' if str(advantage_audience).lower() in ('1', 'true') else '0'
                detailed_targeting = self._parse_detailed_targeting(tg)
                if not (detailed_targeting.get('include') or detailed_targeting.get('narrow')):
                    fallback_labels = self._interest_labels_from_sentence_lines(sentence_rows)
                    resolved = self._resolve_interest_labels(token, fallback_labels)
                    if resolved:
                        detailed_targeting['include'] = self._dedupe_targeting_items(
                            (detailed_targeting.get('include') or []) + resolved
                        )
                detailed_targeting['include'] = self._resolve_targeting_item_names(token, detailed_targeting.get('include') or [])
                detailed_targeting['narrow'] = self._resolve_targeting_item_names(token, detailed_targeting.get('narrow') or [])
                advantage_custom_audiences = self._parse_custom_audience_items(tg.get('custom_audiences'))
                excluded_custom_audiences = self._parse_custom_audience_items(tg.get('excluded_custom_audiences'))
                languages = self._parse_language_items(tg.get('locales'))
                inc_regions = keys(geo.get('regions'), 'region')
                inc_cities = keys(geo.get('cities'), 'city')
                inc_countries = [str(x).strip().upper() for x in (geo.get('countries') if isinstance(geo.get('countries'), list) else []) if str(x).strip()]
                if (inc_regions or inc_cities) and len(inc_countries) <= 1:
                    inc_countries = []
                country_names = {'ID': 'Indonesia', 'SG': 'Singapore', 'MY': 'Malaysia', 'US': 'United States', 'AU': 'Australia', 'GB': 'United Kingdom'}
                display_cc = (list(region_countries)[0] if region_countries else (inc_countries[0] if inc_countries else 'ID'))
                adset_data = {
                    'adset_id': str(row.get('id') or ''), 'adset_name': str(row.get('name') or ''),
                    'budget_type': 'daily' if row.get('daily_budget') else ('lifetime' if row.get('lifetime_budget') else 'daily'),
                    'daily_budget': str(row.get('daily_budget') or ''), 'lifetime_budget': str(row.get('lifetime_budget') or ''),
                    'start_time': _fmt_meta_dt(row.get('start_time')), 'end_time': _fmt_meta_dt(row.get('end_time')),
                    'optimization_goal': str(row.get('optimization_goal') or 'LINK_CLICKS'), 'bid_strategy': str(row.get('bid_strategy') or 'LOWEST_COST_WITHOUT_CAP'),
                    'bid_amount': str(row.get('bid_amount') or ''), 'dynamic_creative': '1' if str(row.get('is_dynamic_creative') or '').lower() == 'true' else '0',
                    'attribution_window': attr, 'age_min': str(ui_age_min), 'age_max': str(ui_age_max),
                    'gender': 'male' if genders == [1] else ('female' if genders == [2] else 'all'),
                    'location_include_countries': ','.join(inc_countries),
                    'location_exclude_countries': ','.join([str(x).strip().upper() for x in (exg.get('countries') if isinstance(exg.get('countries'), list) else []) if str(x).strip()]),
                    'location_include_regions': ','.join(inc_regions), 'location_include_cities': ','.join(inc_cities),
                    'location_exclude_regions': ','.join(keys(exg.get('regions'), 'region')), 'location_exclude_cities': ','.join(keys(exg.get('cities'), 'city')),
                    'location_display_country': country_names.get(display_cc, display_cc),
                    'location_labels': dict(gl, **{
                        ('country:' + str(c).strip().upper()): country_names.get(str(c).strip().upper(), str(c).strip().upper())
                        for c in inc_countries
                        if str(c).strip()
                    }),
                    'advantage': advantage,
                    'detailed_targeting': json.dumps(detailed_targeting),
                    'advantage_custom_audiences': json.dumps(advantage_custom_audiences),
                    'excluded_custom_audiences': json.dumps(excluded_custom_audiences),
                    'languages': json.dumps(languages),
                    'placement_mode': placement_mode,
                    'placement_platforms': json.dumps(pub_platforms),
                    'placement_positions': json.dumps(placement_positions),
                    'placement_device_mode': placement_device_mode,
                }
        except Exception:
            adset_data = {}
        try:
            dresp = requests.get(
                f'https://graph.facebook.com/v22.0/{campaign_id}/ads',
                params={'access_token': token, 'fields': 'id,name,effective_object_story_id,creative{id}', 'limit': 25},
                timeout=20
            )
            dbody = dresp.json() if dresp.text else {}
            rows = (dbody or {}).get('data') or [] if isinstance(dbody, dict) else []
            drow = next((x for x in rows if isinstance((x or {}).get('creative'), dict) or str((x or {}).get('effective_object_story_id') or '').strip()), None)
            if isinstance(drow, dict):
                creative_ref = drow.get('creative') if isinstance(drow.get('creative'), dict) else {}
                creative_id = str(creative_ref.get('id') or '').strip()
                creative = {}
                if creative_id:
                    cresp = requests.get(
                        f'https://graph.facebook.com/v22.0/{creative_id}',
                        params={'access_token': token, 'fields': 'id,title,body,object_story_id,object_story_spec,asset_feed_spec,call_to_action_type,instagram_actor_id,url_tags,thumbnail_url,image_url,object_type,effective_object_story_id'},
                        timeout=20
                    )
                    creative = cresp.json() if cresp.text else {}
                spec = creative.get('object_story_spec') if isinstance(creative.get('object_story_spec'), dict) else {}
                story, story_kind = GetCampaignMetaDetailView._story_block_from_spec(spec)
                asset_feed = creative.get('asset_feed_spec') if isinstance(creative.get('asset_feed_spec'), dict) else {}
                cta = story.get('call_to_action') if isinstance(story.get('call_to_action'), dict) else {}
                cta_val = cta.get('value') if isinstance(cta.get('value'), dict) else {}
                existing_post_id = str(creative.get('object_story_id') or drow.get('effective_object_story_id') or '').strip()
                page_id = str(spec.get('page_id') or (existing_post_id.split('_')[0] if '_' in existing_post_id else ''))
                post_data = {}
                post_attach = {'video_id': '', 'image_url': '', 'link': '', 'video_source': ''}
                if existing_post_id:
                    post_tokens = []
                    if page_id:
                        page_token_early = _facebook_page_access_token(page_id, token)
                        if page_token_early:
                            post_tokens.append(page_token_early)
                    if token and token not in post_tokens:
                        post_tokens.append(token)
                    for post_token in post_tokens:
                        presp = requests.get(
                            f'https://graph.facebook.com/v22.0/{existing_post_id}',
                            params={'access_token': post_token, 'fields': 'id,message,caption,description,link,from{id,name},call_to_action,attachments{media_type,url,media{source,id,image{src}},subattachments{data{media_type,url,media{source,id,image{src}}}}}'},
                            timeout=20
                        )
                        post_data = presp.json() if presp.text else {}
                        if isinstance(post_data, dict) and not post_data.get('error'):
                            break
                        post_data = {}
                    post_attach = GetCampaignMetaDetailView._parse_post_attachments(post_data if isinstance(post_data, dict) else {})
                post_from = post_data.get('from') if isinstance(post_data.get('from'), dict) else {}
                post_cta = post_data.get('call_to_action') if isinstance(post_data.get('call_to_action'), dict) else {}
                post_cta_val = post_cta.get('value') if isinstance(post_cta.get('value'), dict) else {}
                website_url = str(
                    story.get('link') or cta_val.get('link') or post_data.get('link') or post_cta_val.get('link') or post_attach.get('link')
                    or GetCampaignMetaDetailView._first_asset_feed_value(asset_feed.get('link_urls'), 'website_url')
                    or ''
                )
                primary_text = str(
                    story.get('message') or post_data.get('message') or creative.get('body')
                    or GetCampaignMetaDetailView._first_asset_feed_value(asset_feed.get('bodies'), 'text')
                    or ''
                )
                headline = str(
                    story.get('name') or story.get('title') or creative.get('title')
                    or GetCampaignMetaDetailView._first_asset_feed_value(asset_feed.get('titles'), 'text')
                    or ''
                )
                description = str(
                    story.get('description') or story.get('link_description') or post_data.get('description')
                    or GetCampaignMetaDetailView._first_asset_feed_value(asset_feed.get('descriptions'), 'text')
                    or ''
                )
                caption = str(story.get('caption') or post_data.get('caption') or '')
                display_link = str(story.get('caption') or cta_val.get('link_caption') or post_cta_val.get('link_caption') or '')
                video_id = str(
                    story.get('video_id') or post_attach.get('video_id')
                    or GetCampaignMetaDetailView._first_asset_feed_value(asset_feed.get('videos'), 'video_id')
                    or ''
                )
                image_hash = str(story.get('image_hash') or GetCampaignMetaDetailView._first_asset_feed_value(asset_feed.get('images'), 'hash') or '')
                video_thumbnail_url = str(
                    story.get('image_url') or creative.get('thumbnail_url') or creative.get('image_url')
                    or post_attach.get('image_url') or GetCampaignMetaDetailView._first_asset_feed_value(asset_feed.get('videos'), 'thumbnail_url')
                    or ''
                )
                if video_id and not video_thumbnail_url:
                    video_thumbnail_url = GetCampaignMetaDetailView._fetch_video_thumb(token, video_id)
                if not page_id:
                    post_from = post_data.get('from') if isinstance(post_data.get('from'), dict) else {}
                    page_id = str(post_from.get('id') or (existing_post_id.split('_')[0] if '_' in existing_post_id else ''))
                video_source_url = str(post_attach.get('video_source') or '').strip()
                if video_id and not video_source_url:
                    media_lib = FacebookCreativeMediaLibraryView()
                    page_token = _facebook_page_access_token(page_id, token) if page_id else ''
                    extra_tokens = [page_token] if page_token else []
                    video_source_url = media_lib._resolve_video_source(
                        token,
                        video_id,
                        real_account_id=media_lib._real_account_id(account_id),
                        page_id=page_id,
                        extra_tokens=extra_tokens,
                        post_id=existing_post_id,
                    )
                page_id_label = str(post_from.get('name') or '')
                if page_id and not page_id_label:
                    page_id_label = GetCampaignMetaDetailView._fetch_graph_name(token, page_id)
                instagram_actor_id = str(spec.get('instagram_user_id') or spec.get('instagram_actor_id') or creative.get('instagram_actor_id') or '')
                instagram_actor_label = ''
                if instagram_actor_id:
                    instagram_actor_label = GetCampaignMetaDetailView._fetch_graph_name(token, instagram_actor_id)
                cta_type = str(
                    cta.get('type') or post_cta.get('type') or creative.get('call_to_action_type') or 'LEARN_MORE'
                ).strip().upper()
                ad_data = {
                    'ad_id': str(drow.get('id') or ''),
                    'ad_name': str(drow.get('name') or ''),
                    'page_id': page_id,
                    'page_id_label': page_id_label,
                    'instagram_actor_id': instagram_actor_id,
                    'instagram_actor_label': instagram_actor_label,
                    'use_existing_post': '1' if existing_post_id else '0',
                    'existing_post_id': existing_post_id,
                    'existing_post_text': primary_text,
                    'website_url': website_url,
                    'display_link': display_link,
                    'primary_text': primary_text,
                    'headline': headline,
                    'description': description,
                    'caption': caption,
                    'cta_type': cta_type,
                    'url_tags': str(creative.get('url_tags') or ''),
                    'video_id': video_id,
                    'video_thumbnail_url': video_thumbnail_url,
                    'video_source_url': video_source_url,
                    'video_playback_url': (
                        (
                            f'/management/admin/facebook_creative_video_source?account_id={account_id}&video_id={video_id}&stream=1'
                            + (f'&page_id={page_id}' if page_id else '')
                            + (f'&post_id={existing_post_id}' if existing_post_id else '')
                        )
                        if FacebookCreativeMediaLibraryView._is_meta_video_id(video_id) else ''
                    ),
                    'image_hash': image_hash,
                    'creative_type': story_kind or str(creative.get('object_type') or ''),
                }
        except Exception:
            ad_data = {}
        return JsonResponse({'success': True, 'data': {'account_id': account_id, 'campaign_id': str(data.get('id') or campaign_id), 'campaign_name': str(data.get('name') or ''), 'objective': str(data.get('objective') or 'OUTCOME_TRAFFIC'), 'status': str(data.get('status') or 'PAUSED'), 'buying_type': str(data.get('buying_type') or 'AUCTION'), 'special_ad_category': str(cats[0] if cats else 'NONE'), 'campaign_budget_type': 'daily' if data.get('daily_budget') else ('lifetime' if data.get('lifetime_budget') else 'none'), 'campaign_daily_budget': str(data.get('daily_budget') or ''), 'campaign_lifetime_budget': str(data.get('lifetime_budget') or ''), 'campaign_spend_cap': str(data.get('spend_cap') or ''), 'adset': adset_data, 'ad': ad_data}})

@method_decorator(csrf_exempt, name='dispatch')
class UpdateCampaignMetaView(View):
    def dispatch(self, request, *args, **kwargs):
        if 'hris_admin' not in request.session:
            return JsonResponse({'success': False, 'message': 'Unauthorized'}, status=401)
        return super(UpdateCampaignMetaView, self).dispatch(request, *args, **kwargs)
    def post(self, req):
        account_id = str(req.POST.get('account_id') or '').strip(); campaign_id = str(req.POST.get('campaign_id') or '').strip()
        if not account_id or not campaign_id:
            return JsonResponse({'success': False, 'message': 'Account dan campaign wajib diisi'})
        rs = data_mysql().master_account_ads_by_id({'data_account': account_id}); acc = (rs or {}).get('data') if isinstance(rs, dict) else None
        if not isinstance(acc, dict):
            return JsonResponse({'success': False, 'message': 'Account tidak ditemukan'})
        token = str(acc.get('access_token') or '').strip(); special_ad_category = str(req.POST.get('special_ad_category') or 'NONE').strip().upper(); budget_type = str(req.POST.get('campaign_budget_type') or 'none').strip().lower()
        payload = {'access_token': token, 'name': str(req.POST.get('campaign_name') or '').strip(), 'objective': str(req.POST.get('objective') or 'OUTCOME_TRAFFIC').strip().upper(), 'status': str(req.POST.get('status') or 'PAUSED').strip().upper(), 'buying_type': str(req.POST.get('buying_type') or 'AUCTION').strip().upper(), 'special_ad_categories': json.dumps([] if special_ad_category in ('', 'NONE') else [special_ad_category])}
        if budget_type == 'daily' and str(req.POST.get('campaign_daily_budget') or '').strip(): payload['daily_budget'] = str(max(1000, int(float(req.POST.get('campaign_daily_budget') or 0))))
        if budget_type == 'lifetime' and str(req.POST.get('campaign_lifetime_budget') or '').strip(): payload['lifetime_budget'] = str(max(1000, int(float(req.POST.get('campaign_lifetime_budget') or 0))))
        if str(req.POST.get('campaign_spend_cap') or '').strip(): payload['spend_cap'] = str(max(0, int(float(req.POST.get('campaign_spend_cap') or 0))))
        resp = requests.post(f'https://graph.facebook.com/v22.0/{campaign_id}', data=payload, timeout=30); body = resp.json() if resp.text else {}
        if resp.status_code >= 400 or (isinstance(body, dict) and body.get('error')):
            return JsonResponse({'success': False, 'message': str(((body or {}).get('error') or {}).get('message') or 'Gagal update campaign')})
        return JsonResponse({'success': True, 'message': 'Campaign berhasil diperbarui', 'campaign_id': campaign_id})
    
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

            if campaign_budget_type == 'daily' and not campaign_daily_budget:
                return JsonResponse({'success': False, 'message': 'Anggaran Harian Kampanye wajib diisi jika tipe anggaran Harian'})

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
                try:
                    payload['daily_budget'] = str(max(1000, int(float(campaign_daily_budget or 0))))
                except Exception:
                    return JsonResponse({'success': False, 'message': 'Anggaran Harian Kampanye tidak valid'})
            elif campaign_budget_type == 'lifetime' and campaign_lifetime_budget:
                payload['lifetime_budget'] = str(max(1000, int(float(campaign_lifetime_budget or 0))))
            if campaign_spend_cap:
                payload['spend_cap'] = str(max(0, int(float(campaign_spend_cap or 0))))
            resp = requests.post(url, data=payload, timeout=45)
            try:
                body = resp.json() if resp.text else {}
            except Exception:
                body = {}

            if resp.status_code >= 400 or (isinstance(body, dict) and body.get('error')):
                error_obj = (body.get('error') or {}) if isinstance(body, dict) else {}
                err_msg = str(error_obj.get('message') or '').strip()
                err_code = error_obj.get('code')
                err_subcode = error_obj.get('error_subcode')
                fbtrace_id = error_obj.get('fbtrace_id')
                user_title = str(error_obj.get('error_user_title') or '').strip()
                user_msg = str(error_obj.get('error_user_msg') or '').strip()

                detail_parts = []
                if err_code is not None:
                    detail_parts.append(f"code={err_code}")
                if err_subcode is not None:
                    detail_parts.append(f"subcode={err_subcode}")
                if fbtrace_id:
                    detail_parts.append(f"fbtrace_id={fbtrace_id}")

                base_msg = user_title or err_msg or f'Graph API error ({resp.status_code})'
                if user_msg:
                    base_msg = f"{base_msg}. {user_msg}"
                if detail_parts:
                    base_msg = f"{base_msg} [{' | '.join(detail_parts)}]"

                return JsonResponse({
                    'success': False,
                    'message': base_msg,
                    'error': {
                        'code': err_code,
                        'subcode': err_subcode,
                        'fbtrace_id': fbtrace_id,
                        'raw_message': err_msg,
                        'user_title': user_title,
                        'user_message': user_msg,
                    },
                    'account_debug': {
                        'selected_account_id': account_id,
                        'resolved_account_id': real_account_id,
                        'graph_act_id': f'act_{real_account_id}',
                    }
                })

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
            threads_user_id = str(req.POST.get('threads_user_id') or '').strip()
            use_existing_post = str(req.POST.get('use_existing_post') or '0').strip()
            existing_post_id = str(req.POST.get('existing_post_id') or '').strip()
            cta_type = str(req.POST.get('cta_type') or 'LEARN_MORE').strip().upper()
            countries_raw = str(req.POST.get('countries') or 'ID').strip()
            location_include_countries_raw = str(req.POST.get('location_include_countries') or countries_raw or 'ID').strip()
            location_exclude_countries_raw = str(req.POST.get('location_exclude_countries') or '').strip()
            location_include_regions_raw = str(req.POST.get('location_include_regions') or '').strip()
            location_include_cities_raw = str(req.POST.get('location_include_cities') or '').strip()
            location_exclude_regions_raw = str(req.POST.get('location_exclude_regions') or '').strip()
            location_exclude_cities_raw = str(req.POST.get('location_exclude_cities') or '').strip()
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
class FacebookAdsetReachEstimateView(View):
    def dispatch(self, request, *args, **kwargs):
        if 'hris_admin' not in request.session:
            return JsonResponse({'success': False, 'message': 'Unauthorized'}, status=401)
        return super(FacebookAdsetReachEstimateView, self).dispatch(request, *args, **kwargs)

    def post(self, req):
        account_id = str(req.POST.get('account_id') or '').strip()
        if not account_id:
            return JsonResponse({'success': False, 'message': 'Account wajib dipilih'})

        rs = data_mysql().master_account_ads_by_id({'data_account': account_id})
        acc = (rs or {}).get('data') if isinstance(rs, dict) else None
        if not isinstance(acc, dict):
            return JsonResponse({'success': False, 'message': 'Account tidak ditemukan'})

        token = str(acc.get('access_token') or '').strip()
        real_account_id = str(acc.get('account_id') or account_id).replace('act_', '').strip()
        if not token or not real_account_id:
            return JsonResponse({'success': False, 'message': 'Token atau Account ID tidak valid'})

        targeting = build_adset_targeting_for_audience_estimate(req.POST)
        if not (targeting.get('geo_locations') or {}).get('countries') and not (targeting.get('geo_locations') or {}).get('regions') and not (targeting.get('geo_locations') or {}).get('cities'):
            targeting.setdefault('geo_locations', {})['countries'] = ['ID']

        optimization_goal = str(req.POST.get('optimization_goal') or 'LANDING_PAGE_VIEWS').strip().upper()
        allowed_delivery_goals = {
            'IMPRESSIONS', 'REACH', 'LINK_CLICKS', 'LANDING_PAGE_VIEWS', 'POST_ENGAGEMENT',
            'CONVERSATIONS', 'LEAD_GENERATION', 'OFFSITE_CONVERSIONS', 'PAGE_LIKES',
            'APP_INSTALLS', 'VIDEO_VIEWS', 'THRUPLAY', 'PROFILE_VISIT', 'PROFILE_VISITS',
            'VALUE', 'QUALITY_LEAD',
        }
        if optimization_goal not in allowed_delivery_goals:
            optimization_goal = 'LANDING_PAGE_VIEWS'
        optimize_for = optimization_goal if optimization_goal in ('IMPRESSIONS', 'REACH', 'LINK_CLICKS', 'LANDING_PAGE_VIEWS', 'POST_ENGAGEMENT', 'CONVERSATIONS') else 'IMPRESSIONS'
        targeting_json = json.dumps(targeting, separators=(',', ':'))

        def _meta_estimate_row(body):
            if not isinstance(body, dict):
                return None
            data = body.get('data')
            if isinstance(data, list) and data:
                return data[0]
            if any(body.get(k) is not None for k in ('estimate_mau_lower_bound', 'users_lower_bound')):
                return body
            return None

        def _meta_estimate_error(body):
            err = (body.get('error') or {}) if isinstance(body, dict) else {}
            return str(err.get('error_user_msg') or err.get('message') or 'Gagal mengambil estimasi audiens dari Meta')

        try:
            row = None
            source = 'meta_delivery'
            resp = requests.get(
                f'https://graph.facebook.com/v22.0/act_{real_account_id}/delivery_estimate',
                params={
                    'access_token': token,
                    'targeting_spec': targeting_json,
                    'optimization_goal': optimization_goal,
                },
                timeout=25,
            )
            body = resp.json() if resp.text else {}
            if resp.status_code < 400 and not (isinstance(body, dict) and body.get('error')):
                row = _meta_estimate_row(body)

            lower, upper = _extract_meta_audience_size_bounds(row)
            if lower is None:
                source = 'meta_reach'
                resp = requests.get(
                    f'https://graph.facebook.com/v22.0/act_{real_account_id}/reachestimate',
                    params={
                        'access_token': token,
                        'targeting_spec': targeting_json,
                        'optimize_for': optimize_for,
                    },
                    timeout=25,
                )
                body = resp.json() if resp.text else {}
                if resp.status_code >= 400 or (isinstance(body, dict) and body.get('error')):
                    return JsonResponse({'success': False, 'message': _meta_estimate_error(body)})
                row = _meta_estimate_row(body)
                lower, upper = _extract_meta_audience_size_bounds(row)

            if lower is None:
                if isinstance(body, dict) and body.get('error'):
                    return JsonResponse({'success': False, 'message': _meta_estimate_error(body)})
                return JsonResponse({'success': False, 'message': 'Estimasi audiens tidak tersedia dari Meta'})

            return JsonResponse({
                'success': True,
                'lower': lower,
                'upper': upper,
                'source': source,
                'estimate_ready': bool((row or {}).get('estimate_ready', True)),
            })
        except requests.Timeout:
            return JsonResponse({'success': False, 'message': 'Estimasi audiens Meta timeout, coba lagi.'})
        except Exception as exc:
            return JsonResponse({'success': False, 'message': str(exc) or 'Gagal mengambil estimasi audiens'})


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
            adset_id = str(req.POST.get('adset_id') or '').strip()
            save_adset_only = str(req.POST.get('save_adset_only') or '0').strip() == '1'
            force_new_adset = str(req.POST.get('force_new_adset') or '0').strip() == '1'
            status = str(req.POST.get('status') or 'PAUSED').strip().upper()
            adset_name = str(req.POST.get('adset_name') or f'ADSET {campaign_id}').strip()
            ad_name = str(req.POST.get('ad_name') or f'AD {campaign_id}').strip()
            page_id = str(req.POST.get('page_id') or '').strip()
            website_url = str(req.POST.get('website_url') or '').strip()
            primary_text = str(req.POST.get('primary_text') or '').strip()
            headline = str(req.POST.get('headline') or '').strip()
            description = str(req.POST.get('description') or '').strip()
            caption = str(req.POST.get('caption') or '').strip()
            display_link = str(req.POST.get('display_link') or '').strip()
            url_tags = str(req.POST.get('url_tags') or '').strip()
            instagram_actor_id = str(req.POST.get('instagram_actor_id') or '').strip()
            threads_user_id = str(req.POST.get('threads_user_id') or '').strip()
            use_existing_post = str(req.POST.get('use_existing_post') or '0').strip()
            existing_post_id = str(req.POST.get('existing_post_id') or '').strip()
            cta_type = str(req.POST.get('cta_type') or 'LEARN_MORE').strip().upper()
            countries_raw = str(req.POST.get('countries') or 'ID').strip()
            location_include_countries_raw = str(req.POST.get('location_include_countries') or countries_raw or 'ID').strip()
            location_exclude_countries_raw = str(req.POST.get('location_exclude_countries') or '').strip()
            location_include_regions_raw = str(req.POST.get('location_include_regions') or '').strip()
            location_include_cities_raw = str(req.POST.get('location_include_cities') or '').strip()
            location_exclude_regions_raw = str(req.POST.get('location_exclude_regions') or '').strip()
            location_exclude_cities_raw = str(req.POST.get('location_exclude_cities') or '').strip()
            languages_raw = str(req.POST.get('languages') or '').strip()
            detailed_targeting_raw = str(req.POST.get('detailed_targeting') or '').strip()
            pixel_id = str(req.POST.get('pixel_id') or '').strip()
            daily_budget = int(float(req.POST.get('daily_budget') or 50000))
            lifetime_budget_raw = str(req.POST.get('lifetime_budget') or '').strip()
            budget_type = str(req.POST.get('budget_type') or 'daily').strip().lower()
            campaign_budget_type = str(req.POST.get('campaign_budget_type') or 'none').strip().lower()
            start_time = str(req.POST.get('start_time') or '').strip()
            end_time = str(req.POST.get('end_time') or '').strip()
            conversion_location = str(req.POST.get('conversion_location') or 'WEBSITE').strip().upper()
            optimization_goal = str(req.POST.get('optimization_goal') or 'LINK_CLICKS').strip().upper()
            billing_event = str(req.POST.get('billing_event') or 'IMPRESSIONS').strip().upper()
            bid_strategy = str(req.POST.get('bid_strategy') or 'LOWEST_COST_WITHOUT_CAP').strip().upper()
            bid_amount_raw = str(req.POST.get('bid_amount') or '').strip()
            bid_requires_amount = bid_strategy in ('LOWEST_COST_WITH_BID_CAP', 'COST_CAP', 'TARGET_COST')
            if bid_requires_amount and not bid_amount_raw:
                bid_strategy = 'LOWEST_COST_WITHOUT_CAP'
                bid_requires_amount = False
            attribution_window = str(req.POST.get('attribution_window') or '7d_click_1d_view').strip().lower()
            if conversion_location == 'WEBSITE' and optimization_goal in ('LINK_CLICKS', 'LANDING_PAGE_VIEWS'):
                attribution_window = '1d_click'
            dynamic_creative = str(req.POST.get('dynamic_creative') or '0').strip()
            gender = str(req.POST.get('gender') or 'all').strip().lower()
            advantage = str(req.POST.get('advantage') or '0').strip()
            placement_mode = str(req.POST.get('placement_mode') or 'auto').strip().lower()
            placement_device_mode = str(req.POST.get('placement_device_mode') or 'all').strip().lower()
            placement_platforms_raw = str(req.POST.get('placement_platforms') or '').strip()
            placement_positions_raw = str(req.POST.get('placement_positions') or '').strip()
            asset_customization = str(req.POST.get('asset_customization') or '0').strip()
            age_min = int(req.POST.get('age_min') or 18)
            age_max = int(req.POST.get('age_max') or 65)

            if not account_id or not campaign_id:
                return JsonResponse({'success': False, 'message': 'Account dan Campaign ID wajib diisi'})
            if not save_adset_only and (not page_id or not website_url):
                return JsonResponse({'success': False, 'message': 'Page ID dan Website URL wajib diisi'})

            rs = data_mysql().master_account_ads_by_id({'data_account': account_id})
            acc = (rs or {}).get('data') if isinstance(rs, dict) else None
            if not isinstance(acc, dict):
                return JsonResponse({'success': False, 'message': 'Account tidak ditemukan'})
            token = str(acc.get('access_token') or '').strip()
            real_account_id = str(acc.get('account_id') or account_id).replace('act_', '').strip()

            actual_campaign_budget_type = campaign_budget_type
            existing_campaign_bid_strategy = ''
            try:
                camp_resp = requests.get(
                    f'https://graph.facebook.com/v22.0/{campaign_id}',
                    params={'access_token': token, 'fields': 'daily_budget,lifetime_budget,bid_strategy'},
                    timeout=20
                )
                camp_body = camp_resp.json() if camp_resp.text else {}
                if camp_resp.status_code < 400 and not (isinstance(camp_body, dict) and camp_body.get('error')):
                    has_daily = bool((camp_body or {}).get('daily_budget'))
                    has_lifetime = bool((camp_body or {}).get('lifetime_budget'))
                    existing_campaign_bid_strategy = str((camp_body or {}).get('bid_strategy') or '').strip().upper()
                    if has_daily:
                        actual_campaign_budget_type = 'daily'
                    elif has_lifetime:
                        actual_campaign_budget_type = 'lifetime'
                    else:
                        actual_campaign_budget_type = 'none'
            except Exception:
                actual_campaign_budget_type = campaign_budget_type

            if save_adset_only:
                bid_strategy = 'LOWEST_COST_WITHOUT_CAP'
                bid_amount_raw = ''
                bid_requires_amount = False
            elif force_new_adset:
                adset_id = ''

            if (not bid_amount_raw) and existing_campaign_bid_strategy in ('LOWEST_COST_WITH_BID_CAP', 'COST_CAP', 'TARGET_COST'):
                try:
                    camp_fix_resp = requests.post(
                        f'https://graph.facebook.com/v22.0/{campaign_id}',
                        data={'access_token': token, 'bid_strategy': 'LOWEST_COST_WITHOUT_CAP'},
                        timeout=30
                    )
                    camp_fix_body = camp_fix_resp.json() if camp_fix_resp.text else {}
                    if camp_fix_resp.status_code < 400 and not (isinstance(camp_fix_body, dict) and camp_fix_body.get('error')):
                        existing_campaign_bid_strategy = 'LOWEST_COST_WITHOUT_CAP'
                except Exception:
                    pass

            if adset_id and not save_adset_only and not bid_amount_raw:
                try:
                    adset_resp = requests.get(
                        f'https://graph.facebook.com/v22.0/{adset_id}',
                        params={'access_token': token, 'fields': 'bid_strategy'},
                        timeout=20
                    )
                    adset_body = adset_resp.json() if adset_resp.text else {}
                    existing_bid_strategy = str((adset_body or {}).get('bid_strategy') or '').strip().upper()
                    if existing_bid_strategy in ('LOWEST_COST_WITH_BID_CAP', 'COST_CAP', 'TARGET_COST'):
                        adset_id = ''
                        bid_strategy = 'LOWEST_COST_WITHOUT_CAP'
                        bid_requires_amount = False
                except Exception:
                    pass

            def _parse_csv(raw, upper=False):
                vals = [str(x).strip() for x in str(raw or '').split(',') if str(x).strip()]
                out = []
                for v in vals:
                    vv = v.upper() if upper else v
                    if vv not in out:
                        out.append(vv)
                return out

            include_countries = _parse_csv(location_include_countries_raw or countries_raw or 'ID', upper=True) or ['ID']
            exclude_countries = _parse_csv(location_exclude_countries_raw, upper=True)
            include_regions = [{'key': v} for v in _parse_csv(location_include_regions_raw)]
            include_cities = [{'key': v} for v in _parse_csv(location_include_cities_raw)]
            exclude_regions = [{'key': v} for v in _parse_csv(location_exclude_regions_raw)]
            exclude_cities = [{'key': v} for v in _parse_csv(location_exclude_cities_raw)]

            if (include_regions or include_cities) and len(include_countries) <= 1:
                include_countries = []

            geo_locations = {}
            if include_countries:
                geo_locations['countries'] = include_countries
            if include_regions:
                geo_locations['regions'] = include_regions
            if include_cities:
                geo_locations['cities'] = include_cities

            targeting = {'geo_locations': geo_locations}
            age_min = max(13, age_min)
            age_max = max(age_min, min(65, age_max))
            if advantage == '1':
                adv_min = max(18, min(25, age_min))
                adv_range_max = max(adv_min, min(65, age_max))
                targeting['age_min'] = adv_min
                targeting['age_max'] = 65
                targeting['age_range'] = [adv_min, adv_range_max]
            else:
                targeting['age_min'] = age_min
                targeting['age_max'] = age_max
            excluded_geo_locations = {}
            if exclude_countries:
                excluded_geo_locations['countries'] = exclude_countries
            if exclude_regions:
                excluded_geo_locations['regions'] = exclude_regions
            if exclude_cities:
                excluded_geo_locations['cities'] = exclude_cities
            if excluded_geo_locations:
                targeting['excluded_geo_locations'] = excluded_geo_locations

            language_keys = []
            if languages_raw:
                try:
                    parsed_lang = json.loads(languages_raw)
                    if not isinstance(parsed_lang, list):
                        parsed_lang = [parsed_lang]
                except Exception:
                    parsed_lang = [x.strip() for x in languages_raw.split(',') if x.strip()]
                for lv in parsed_lang:
                    sv = str(lv or '').strip()
                    if sv.isdigit():
                        iv = int(sv)
                        if iv not in language_keys:
                            language_keys.append(iv)
            if language_keys:
                targeting['locales'] = language_keys

            flexible_spec = []
            if detailed_targeting_raw:
                try:
                    parsed_dt = json.loads(detailed_targeting_raw)
                except Exception:
                    parsed_dt = []
                if isinstance(parsed_dt, dict):
                    parsed_groups = [parsed_dt.get('include') or [], parsed_dt.get('narrow') or []]
                elif isinstance(parsed_dt, list):
                    parsed_groups = [parsed_dt]
                else:
                    parsed_groups = []
                for group in parsed_groups:
                    interests = []
                    for item in (group or []):
                        if isinstance(item, dict):
                            iid = str(item.get('id') or '').strip()
                            iname = str(item.get('name') or '').strip()
                        else:
                            iid = str(item or '').strip()
                            iname = ''
                        if not iid:
                            continue
                        row = {'id': iid}
                        if iname:
                            row['name'] = iname
                        interests.append(row)
                    if interests:
                        flexible_spec.append({'interests': interests})
                if flexible_spec:
                    targeting['flexible_spec'] = flexible_spec

            if gender == 'male':
                targeting['genders'] = [1]
            elif gender == 'female':
                targeting['genders'] = [2]

            selected_platforms = []
            if placement_mode == 'manual':
                try:
                    parsed_platforms = json.loads(placement_platforms_raw) if placement_platforms_raw else []
                    if not isinstance(parsed_platforms, list):
                        parsed_platforms = []
                except Exception:
                    parsed_platforms = []
                allowed_platforms = ['facebook', 'instagram', 'audience_network', 'messenger', 'threads']
                selected_platforms = [p for p in [str(x).strip().lower() for x in parsed_platforms] if p in allowed_platforms]
                if not selected_platforms:
                    selected_platforms = ['facebook', 'instagram', 'audience_network', 'messenger']
                if 'threads' in selected_platforms and 'instagram' not in selected_platforms:
                    selected_platforms.append('instagram')
                targeting['publisher_platforms'] = selected_platforms

                if placement_device_mode == 'mobile':
                    targeting['device_platforms'] = ['mobile']
                elif placement_device_mode == 'desktop':
                    targeting['device_platforms'] = ['desktop']
                else:
                    targeting['device_platforms'] = ['mobile', 'desktop']

                try:
                    parsed_positions = json.loads(placement_positions_raw) if placement_positions_raw else {}
                    if not isinstance(parsed_positions, dict):
                        parsed_positions = {}
                except Exception:
                    parsed_positions = {}

                if 'threads' in selected_platforms:
                    tvals = parsed_positions.get('threads') or []
                    if 'threads_stream' in tvals:
                        ivals = parsed_positions.get('instagram') or []
                        if 'stream' not in ivals:
                            parsed_positions['instagram'] = list(ivals) + ['stream']
                pos_field_map = {
                    'facebook': 'facebook_positions',
                    'instagram': 'instagram_positions',
                    'audience_network': 'audience_network_positions',
                    'messenger': 'messenger_positions',
                    'threads': 'threads_positions',
                }
                allowed_positions = {'messenger': ['story', 'sponsored_messages'], 'threads': ['threads_stream']}
                def _normalize_platform_positions(platform, vals):
                    cleaned = [str(v).strip() for v in (vals or []) if str(v).strip()]
                    if platform == 'facebook':
                        fb_map = {'video_feeds': 'facebook_reels', 'suggested_video': 'facebook_reels'}
                        normalized = []
                        for v in cleaned:
                            v = fb_map.get(v, v)
                            if v in ('video_feeds', 'suggested_video'):
                                continue
                            if v not in normalized:
                                normalized.append(v)
                        return normalized
                    if platform in allowed_positions:
                        return [v for v in cleaned if v in allowed_positions[platform]]
                    return cleaned

                for p in selected_platforms:
                    vals = parsed_positions.get(p) or []
                    if not isinstance(vals, list):
                        vals = []
                    vals = _normalize_platform_positions(p, vals)
                    if vals and p in pos_field_map:
                        targeting[pos_field_map[p]] = vals

            if advantage == '1':
                targeting['targeting_automation'] = {'advantage_audience': 1}

            def _post(path, payload):
                p = dict(payload or {}); p['access_token'] = token
                r = requests.post(f"https://graph.facebook.com/v22.0/{str(path).lstrip('/')}", data=p, timeout=45)
                b = r.json() if r.text else {}
                if r.status_code >= 400 or (isinstance(b, dict) and b.get('error')):
                    err = (b.get('error') or {}) if isinstance(b, dict) else {}
                    msg = str(err.get('error_user_msg') or err.get('message') or f'Graph API error ({r.status_code})')
                    data = err.get('error_data') or {}
                    blame = data.get('blame_field_specs') if isinstance(data, dict) else None
                    if blame:
                        msg += ' | Field: ' + ', '.join([str(x) for x in blame if x])
                    return False, msg, b
                return True, '', b

            def _post_file(path, files):
                r = requests.post(
                    f"https://graph.facebook.com/v22.0/{str(path).lstrip('/')}",
                    data={'access_token': token},
                    files=files,
                    timeout=120
                )
                b = r.json() if r.text else {}
                if r.status_code >= 400 or (isinstance(b, dict) and b.get('error')):
                    err = (b.get('error') or {}) if isinstance(b, dict) else {}
                    msg = str(err.get('error_user_msg') or err.get('message') or f'Graph API error ({r.status_code})')
                    data = err.get('error_data') or {}
                    blame = data.get('blame_field_specs') if isinstance(data, dict) else None
                    if blame:
                        msg += ' | Field: ' + ', '.join([str(x) for x in blame if x])
                    return False, msg, b
                return True, '', b

            def _normalize_meta_dt(raw):
                return normalize_meta_adset_dt_for_api(raw)

            adset_payload = {
                'name': adset_name,
                'billing_event': billing_event or 'IMPRESSIONS',
                'optimization_goal': optimization_goal,
                'status': status,
                'targeting': json.dumps(targeting),
            }
            if not save_adset_only:
                adset_payload['bid_strategy'] = bid_strategy or 'LOWEST_COST_WITHOUT_CAP'
            if not save_adset_only and conversion_location:
                adset_payload['destination_type'] = conversion_location
            if not adset_id:
                adset_payload['campaign_id'] = campaign_id
            use_adset_budget = actual_campaign_budget_type in ('', 'none')
            if use_adset_budget:
                if budget_type == 'lifetime' and lifetime_budget_raw:
                    adset_payload['lifetime_budget'] = str(max(1000, int(float(lifetime_budget_raw or 0))))
                else:
                    adset_payload['daily_budget'] = str(max(1000, daily_budget))
            start_time = _normalize_meta_dt(start_time)
            end_time = _normalize_meta_dt(end_time)
            if start_time:
                adset_payload['start_time'] = start_time
            if end_time:
                adset_payload['end_time'] = end_time
            if not save_adset_only and bid_amount_raw:
                adset_payload['bid_amount'] = str(int(float(bid_amount_raw)))
            if not save_adset_only and attribution_window in ('1d_click', '7d_click', '7d_click_1d_view'):
                if attribution_window == '1d_click':
                    adset_payload['attribution_spec'] = json.dumps([{'event_type': 'CLICK_THROUGH', 'window_days': 1}])
                elif attribution_window == '7d_click':
                    adset_payload['attribution_spec'] = json.dumps([{'event_type': 'CLICK_THROUGH', 'window_days': 7}])
                else:
                    adset_payload['attribution_spec'] = json.dumps([{'event_type': 'CLICK_THROUGH', 'window_days': 7}, {'event_type': 'VIEW_THROUGH', 'window_days': 1}])
            if not save_adset_only and dynamic_creative == '1':
                adset_payload['is_dynamic_creative'] = 'true'
            if not save_adset_only and pixel_id:
                adset_payload['promoted_object'] = json.dumps({'pixel_id': pixel_id, 'custom_event_type': 'PAGE_VIEW'})

            if adset_id:
                ok, err, adset_rs = _post(adset_id, adset_payload)
            else:
                ok, err, adset_rs = _post(f'act_{real_account_id}/adsets', adset_payload)
            if not ok:
                return JsonResponse({'success': False, 'step': 'adset', 'message': err, 'debug_bid_strategy': adset_payload.get('bid_strategy', ''), 'debug_bid_amount': bid_amount_raw, 'debug_adset_id': adset_id, 'debug_campaign_bid_strategy': existing_campaign_bid_strategy, 'debug_attribution_window': attribution_window, 'debug_advantage': advantage})
            adset_id = str((adset_rs or {}).get('id') or adset_id or '').strip()
            if save_adset_only:
                was_update = bool(str(req.POST.get('adset_id') or '').strip())
                return JsonResponse({
                    'success': True,
                    'message': 'Ad Set berhasil diperbarui' if was_update else 'Ad Set berhasil dibuat',
                    'campaign_id': campaign_id,
                    'adset_id': adset_id,
                    'updated': was_update,
                })

            creative_payload = {'name': f'{ad_name} - CREATIVE'}
            if url_tags:
                creative_payload['url_tags'] = url_tags
            if existing_post_id and use_existing_post != '1':
                use_existing_post = '1'
            if use_existing_post == '1':
                if not existing_post_id:
                    return JsonResponse({
                        'success': False,
                        'step': 'creative',
                        'adset_id': adset_id,
                        'message': 'Pilih postingan yang ada terlebih dahulu (Existing Post ID wajib diisi).',
                        'suggest_existing_post': True,
                    })
                page_token_for_post = _facebook_page_access_token(page_id, token) if page_id else token
                post_ok, post_err, _post_meta = _facebook_page_post_ad_ready(
                    existing_post_id, page_id, [page_token_for_post, token],
                )
                if not post_ok:
                    return JsonResponse({
                        'success': False,
                        'step': 'creative',
                        'adset_id': adset_id,
                        'message': post_err,
                        'suggest_existing_post': True,
                        'needs_published_post': True,
                    })
                object_story_id = str(_post_meta.get('usable_object_story_id') or existing_post_id).strip()
                creative_payload['object_story_id'] = object_story_id
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
                media_story_key = 'link_data'
                media_story_data = link_data
                image_file = req.FILES.get('image_file')
                video_file = req.FILES.get('video_file')
                image_hash_existing = str(req.POST.get('image_hash') or '').strip()
                video_id_existing = str(req.POST.get('video_id') or '').strip()
                video_thumb_existing = str(req.POST.get('video_thumbnail_url') or '').strip()
                if image_file:
                    ok, err, img_rs = _post_file(f'act_{real_account_id}/adimages', files={'filename': (image_file.name, image_file.file, image_file.content_type or 'application/octet-stream')})
                    if not ok:
                        return JsonResponse({'success': False, 'step': 'creative_media', 'adset_id': adset_id, 'message': err})
                    img_data = ((img_rs or {}).get('images') or {}).get(image_file.name) or {}
                    image_hash = str(img_data.get('hash') or '').strip()
                    if image_hash:
                        media_story_data['image_hash'] = image_hash
                elif image_hash_existing:
                    media_story_data['image_hash'] = image_hash_existing
                elif video_file:
                    ok, err, vid_rs = _post_file(f'act_{real_account_id}/advideos', files={'source': (video_file.name, video_file.file, video_file.content_type or 'application/octet-stream')})
                    if not ok:
                        return JsonResponse({'success': False, 'step': 'creative_media', 'adset_id': adset_id, 'message': err})
                    video_id = str((vid_rs or {}).get('id') or '').strip()
                    thumb_url = ''
                    try:
                        thumb_resp = requests.get(
                            f'https://graph.facebook.com/v22.0/{video_id}',
                            params={'access_token': token, 'fields': 'thumbnails'},
                            timeout=20
                        )
                        thumb_body = thumb_resp.json() if thumb_resp.text else {}
                        thumb_rows = (((thumb_body or {}).get('thumbnails') or {}).get('data') or []) if isinstance(thumb_body, dict) else []
                        thumb_url = str(((thumb_rows[0] or {}).get('uri') if thumb_rows else '') or '').strip()
                    except Exception:
                        thumb_url = ''
                    if not thumb_url:
                        return JsonResponse({'success': False, 'step': 'creative_media', 'adset_id': adset_id, 'message': 'Thumbnail video tidak ditemukan. Coba upload ulang video atau gunakan gambar untuk materi iklan.'})
                    media_story_key = 'video_data'
                    media_story_data = {'video_id': video_id, 'image_url': thumb_url, 'call_to_action': {'type': cta_type, 'value': {'link': website_url}}}
                    if primary_text:
                        media_story_data['message'] = primary_text
                    if headline:
                        media_story_data['title'] = headline
                elif video_id_existing:
                    thumb_url = video_thumb_existing
                    if not thumb_url:
                        try:
                            thumb_resp = requests.get(
                                f'https://graph.facebook.com/v22.0/{video_id_existing}',
                                params={'access_token': token, 'fields': 'thumbnails'},
                                timeout=20,
                            )
                            thumb_body = thumb_resp.json() if thumb_resp.text else {}
                            thumb_rows = (((thumb_body or {}).get('thumbnails') or {}).get('data') or []) if isinstance(thumb_body, dict) else []
                            thumb_url = str(((thumb_rows[0] or {}).get('uri') if thumb_rows else '') or '').strip()
                        except Exception:
                            thumb_url = ''
                    if not thumb_url:
                        return JsonResponse({'success': False, 'step': 'creative_media', 'adset_id': adset_id, 'message': 'Thumbnail video library tidak ditemukan. Pilih media lain atau unggah ulang.'})
                    media_story_key = 'video_data'
                    media_story_data = {'video_id': video_id_existing, 'image_url': thumb_url, 'call_to_action': {'type': cta_type, 'value': {'link': website_url}}}
                    if primary_text:
                        media_story_data['message'] = primary_text
                    if headline:
                        media_story_data['title'] = headline
                object_story_spec = {'page_id': page_id, media_story_key: media_story_data}
                needs_instagram_identity = 'instagram' in selected_platforms or 'threads' in selected_platforms
                page_token = _facebook_page_access_token(page_id, token) if page_id else ''
                resolve_tokens = [t for t in [page_token, token] if t]
                instagram_user_id = ''
                if needs_instagram_identity or instagram_actor_id:
                    instagram_user_id = _facebook_resolve_instagram_user_id(
                        instagram_actor_id,
                        resolve_tokens,
                        page_id=page_id,
                        ad_account_id=real_account_id,
                    )
                if needs_instagram_identity and not instagram_user_id:
                    return JsonResponse({
                        'success': False,
                        'step': 'creative',
                        'adset_id': adset_id,
                        'message': 'Profil Instagram tidak valid untuk halaman ini. Buka Identitas, pilih ulang Profil Instagram yang tertaut ke halaman Facebook, lalu simpan lagi.',
                    })
                if 'threads' in selected_platforms:
                    if not instagram_user_id or not threads_user_id:
                        return JsonResponse({'success': False, 'step': 'creative', 'adset_id': adset_id, 'message': 'Threads memerlukan Instagram User ID dan Threads User ID.'})
                    object_story_spec['instagram_user_id'] = instagram_user_id
                    object_story_spec['threads_user_id'] = threads_user_id
                elif instagram_user_id and needs_instagram_identity:
                    object_story_spec['instagram_user_id'] = instagram_user_id
                creative_payload['object_story_spec'] = json.dumps(object_story_spec)
            creative_mode = 'object_story_id' if use_existing_post == '1' else 'object_story_spec'
            ok, err, creative_rs = _post(f'act_{real_account_id}/adcreatives', creative_payload)
            if not ok:
                err_msg = str(err or '').strip()
                low = err_msg.lower()
                if any(x in low for x in (
                    'mode perkembangan', 'development mode', 'harus bersifat publik', 'must be public',
                    'must be made public', 'in development', 'apps in development', 'development apps',
                )):
                    hint = (
                        ' Posting yang dipilih kemungkinan dibuat via app/API (bukan langsung di facebook.com). '
                        'Buat posting baru langsung di Halaman Facebook (bukan lewat HRIS), tunggu tayang, lalu pilih di sini. '
                        'Atau ubah app Meta ke mode Live.'
                    ) if use_existing_post == '1' else (
                        ' Gunakan Penyiapan iklan → Gunakan postingan yang ada → pilih posting dengan badge "Siap untuk iklan", '
                        'lalu klik Lanjutkan sebelum Simpan.'
                    )
                    return JsonResponse({
                        'success': False, 'step': 'creative', 'adset_id': adset_id,
                        'message': err_msg + hint,
                        'app_mode_blocked': True, 'suggest_existing_post': True,
                        'needs_published_post': use_existing_post == '1',
                        'debug_creative_mode': creative_mode,
                        'debug_object_story_id': str(creative_payload.get('object_story_id') or ''),
                        'debug_use_existing_post': use_existing_post,
                    })
                return JsonResponse({'success': False, 'step': 'creative', 'adset_id': adset_id, 'message': err_msg})
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
        
        selected_account_list = _resolve_fb_account_filters(selected_account_list)

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
            account_name = str(row.get('account_name') or '').strip()
            if not account_name:
                account_name = str(row.get('account_id') or row.get('account_email') or '-').strip()
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

        metrics_by_domain = _fetch_kiwipixel_metrics_by_domain(
            tanggal_dari,
            tanggal_sampai,
            selected_domain_list,
        )
        total_visits_sum = 0
        total_unique_sum = 0
        total_pageviews_sum = 0
        seen_domain_keys = set()
        for row in normalized_rows:
            domain_key = _normalize_kiwipixel_domain_key(row.get('domain'))
            metrics = dict(metrics_by_domain.get(domain_key) or _zero_kiwipixel_traffic_metrics())
            row['total_visits'] = metrics.get('total_visits', 0)
            row['unique_visitor'] = metrics.get('unique_visitor', 0)
            row['total_pageviews'] = metrics.get('total_pageviews', 0)
            row['total_visitors'] = metrics.get('total_visits', 0)
            if domain_key and domain_key not in seen_domain_keys:
                seen_domain_keys.add(domain_key)
                total_visits_sum += int(metrics.get('total_visits') or 0)
                total_unique_sum += int(metrics.get('unique_visitor') or 0)
                total_pageviews_sum += int(metrics.get('total_pageviews') or 0)

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
                'total_visitors': total_visits_sum,
                'total_visits': total_visits_sum,
                'unique_visitor': total_unique_sum,
                'total_pageviews': total_pageviews_sum,
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


KIWIPIXEL_COUNTRY_API = 'https://api-tracker.kiwipixel.com/v1/country'
KIWIPIXEL_CAMPAIGN_TRAFFIC_API = 'https://api-tracker.kiwipixel.com/v1/campaign'


def _zero_kiwipixel_traffic_metrics():
    return {'total_visits': 0, 'unique_visitor': 0, 'total_pageviews': 0}


def _add_kiwipixel_traffic_metrics(dst, src):
    for key in ('total_visits', 'unique_visitor', 'total_pageviews'):
        dst[key] = int(dst.get(key) or 0) + int((src or {}).get(key) or 0)


def _build_kiwipixel_campaign_traffic_indexes(payload):
    by_utm_domain = {}
    by_domain = {}
    for camp in (payload.get('campaigns') or []):
        if not isinstance(camp, dict):
            continue
        utm_id = str(camp.get('utm_id') or '').strip()
        for drow in (camp.get('domains') or []):
            if not isinstance(drow, dict):
                continue
            metrics = {
                'total_visits': int(drow.get('total_visits') or 0),
                'unique_visitor': int(drow.get('unique_visitor') or 0),
                'total_pageviews': int(drow.get('total_pageviews') or 0),
            }
            dkey = _normalize_kiwipixel_domain_key(drow.get('domain'))
            if not dkey:
                continue
            domain_acc = by_domain.setdefault(dkey, _zero_kiwipixel_traffic_metrics())
            _add_kiwipixel_traffic_metrics(domain_acc, metrics)
            if utm_id:
                utm_key = (utm_id, dkey)
                utm_acc = by_utm_domain.setdefault(utm_key, _zero_kiwipixel_traffic_metrics())
                _add_kiwipixel_traffic_metrics(utm_acc, metrics)
    return {'by_utm_domain': by_utm_domain, 'by_domain': by_domain}


def _fetch_kiwipixel_campaign_traffic(start_date, end_date):
    start_fmt = _normalize_kiwipixel_date(start_date)
    end_fmt = _normalize_kiwipixel_date(end_date)
    if not start_fmt or not end_fmt:
        return _build_kiwipixel_campaign_traffic_indexes({})
    try:
        response = requests.get(
            KIWIPIXEL_CAMPAIGN_TRAFFIC_API,
            params={'show': 'traffic', 'start-date': start_fmt, 'end-date': end_fmt},
            timeout=45,
            headers={
                'Accept': 'application/json',
                'User-Agent': 'hris-management/1.0',
            },
        )
        if response.status_code != 200:
            return _build_kiwipixel_campaign_traffic_indexes({})
        payload = response.json() if response.content else {}
    except Exception:
        payload = {}
    if not isinstance(payload, dict):
        payload = {}
    return _build_kiwipixel_campaign_traffic_indexes(payload)


def _resolve_kiwipixel_campaign_visitors(site_name, campaign_ids, indexes):
    dkey = _normalize_kiwipixel_domain_key(site_name)
    if not dkey:
        return _zero_kiwipixel_traffic_metrics()
    by_utm_domain = (indexes or {}).get('by_utm_domain') or {}
    by_domain = (indexes or {}).get('by_domain') or {}
    out = _zero_kiwipixel_traffic_metrics()
    matched_campaign = False
    for cid in (campaign_ids or []):
        cid_s = str(cid or '').strip()
        if not cid_s:
            continue
        metrics = by_utm_domain.get((cid_s, dkey))
        if metrics:
            matched_campaign = True
            _add_kiwipixel_traffic_metrics(out, metrics)
    if matched_campaign:
        return out
    fallback = by_domain.get(dkey)
    return dict(fallback) if fallback else _zero_kiwipixel_traffic_metrics()


def _fetch_fb_campaign_ids_by_domain(start_date, end_date, domain_terms):
    terms = build_domain_filter_terms(domain_terms or [], include_original=False, include_base=True)
    if not start_date or not end_date or not terms:
        return {}
    out = {}
    try:
        db = data_mysql()
        like_conditions = " OR ".join([
            "(CONCAT(SUBSTRING_INDEX(b.data_ads_domain, '.', 2), '.com') LIKE %s OR b.data_ads_domain LIKE %s)"
        ] * len(terms))
        like_params = []
        for term in terms:
            token = str(term or '').strip()
            like_params.extend([f"%{token}%", f"%{token}%"])
        sql = f"""
            SELECT DISTINCT
                CONCAT(SUBSTRING_INDEX(b.data_ads_domain, '.', 2), '.com') AS base_domain,
                CAST(b.data_ads_campaign_id AS CHAR) AS campaign_id
            FROM data_ads_country b
            WHERE b.data_ads_country_tanggal BETWEEN %s AND %s
              AND ({like_conditions})
              AND b.data_ads_campaign_id IS NOT NULL
              AND CAST(b.data_ads_campaign_id AS CHAR) <> ''
        """
        params = [start_date, end_date] + like_params
        if not db.execute_query(sql, tuple(params)):
            return {}
        rows = db.fetch_all() or []
        if hasattr(db, 'commit'):
            db.commit()
        for row in rows:
            base_domain = str((row or {}).get('base_domain') or '').strip()
            campaign_id = str((row or {}).get('campaign_id') or '').strip()
            if not base_domain or not campaign_id:
                continue
            dkey = _normalize_kiwipixel_domain_key(base_domain)
            if not dkey:
                continue
            out.setdefault(dkey, set()).add(campaign_id)
    except Exception:
        return {}
    return {k: sorted(v) for k, v in out.items()}


def _resolve_fb_account_filters(selected_account_list):
    """Expand account filter agar cocok dengan account_id / account_ads_id di DB."""
    raw = [str(x).strip() for x in (selected_account_list or []) if str(x).strip() and str(x).strip() != '%']
    if not raw:
        return []
    selected_set = set(raw)
    resolved = set(raw)
    try:
        for acc in data_mysql().master_account_ads().get('data', []) or []:
            aid = str(acc.get('account_id') or '').strip()
            ads_id = str(acc.get('account_ads_id') or '').strip()
            if aid in selected_set or ads_id in selected_set:
                if aid:
                    resolved.add(aid)
                if ads_id:
                    resolved.add(ads_id)
    except Exception:
        pass
    return list(resolved)


def _normalize_kiwipixel_date(value):
    if not value:
        return ''
    cleaned = str(value).strip().replace('-', '').replace('/', '')
    if len(cleaned) == 8 and cleaned.isdigit():
        return cleaned
    try:
        return datetime.strptime(str(value).strip()[:10], '%Y-%m-%d').strftime('%Y%m%d')
    except Exception:
        return ''


def _normalize_kiwipixel_domain_key(domain):
    value = str(domain or '').lower().strip()
    if not value or value == '%':
        return ''
    if '://' in value:
        value = value.split('://', 1)[1]
    value = value.split('/', 1)[0].split('?', 1)[0].split('#', 1)[0]
    value = value.split(':', 1)[0]
    if value.startswith('www.'):
        value = value[4:]
    for suffix in ('.adx', '.ads'):
        if value.endswith(suffix):
            value = value[: -len(suffix)]
    tlds = (
        '.co.id', '.web.id', '.my.id', '.or.id', '.ac.id', '.go.id',
        '.com', '.net', '.org', '.id', '.top', '.io',
    )
    for tld in sorted(tlds, key=len, reverse=True):
        if value.endswith(tld):
            value = value[: -len(tld)]
            break
    return value.strip('.')


def _normalize_kiwipixel_country_code(code):
    value = str(code or '').strip().upper()
    if not value:
        return ''
    if value == 'TU':
        return 'TR'
    return value


def _looks_like_kiwipixel_domain(value):
    text = str(value or '').strip().lower()
    if not text or text in {'-', 'all', '%'}:
        return False
    if len(text) <= 3 and text.isalpha():
        return False
    return ('.' in text) or ('/' in text)


def _expand_kiwipixel_country_code_aliases(codes):
    out = []
    seen = set()
    for item in (codes or []):
        value = str(item or '').strip().upper()
        if not value:
            continue
        aliases = [value]
        normalized = _normalize_kiwipixel_country_code(value)
        if normalized and normalized not in aliases:
            aliases.insert(0, normalized)
        if normalized == 'TR' and 'TU' not in aliases:
            aliases.append('TU')
        for alias in aliases:
            if alias and alias not in seen:
                seen.add(alias)
                out.append(alias)
    return out


def _kiwipixel_domain_matches(api_domain, filter_domains):
    api_key = _normalize_kiwipixel_domain_key(api_domain)
    if not api_key:
        return False
    if not filter_domains:
        return True
    normalized_filters = [
        str(item or '').lower().strip()
        for item in filter_domains
        if str(item or '').strip() and str(item or '').strip() != '%'
    ]
    if not normalized_filters:
        return True
    for domain_filter in normalized_filters:
        filter_key = _normalize_kiwipixel_domain_key(domain_filter)
        if not filter_key:
            continue
        if (
            filter_key == api_key
            or filter_key in api_key
            or api_key in filter_key
            or filter_key in (api_domain or '').lower()
            or (api_domain or '').lower() in filter_key
        ):
            return True
    return False


def _kiwipixel_domain_row_matches(domain_row, filter_domains):
    if not filter_domains:
        return True
    if not isinstance(domain_row, dict):
        return False
    candidates = []
    for key in ('domain', 'site', 'site_name', 'subdomain', 'hostname', 'host', 'url'):
        value = str(domain_row.get(key) or '').strip()
        if value:
            candidates.append(value)
    for candidate in candidates:
        if _kiwipixel_domain_matches(candidate, filter_domains):
            return True
    return False


def _fetch_kiwipixel_country_metrics(country_codes, start_date, end_date, domain_filters):
    start_fmt = _normalize_kiwipixel_date(start_date)
    end_fmt = _normalize_kiwipixel_date(end_date)
    if not start_fmt or not end_fmt:
        return {}

    raw_codes = [str(code or '').strip().upper() for code in (country_codes or []) if str(code or '').strip()]
    codes = _expand_kiwipixel_country_code_aliases(raw_codes)
    requested_code_set = {
        _normalize_kiwipixel_country_code(code)
        for code in raw_codes
        if _normalize_kiwipixel_country_code(code)
    }
    filter_payload = {
        'start_date': int(start_fmt),
        'end_date': int(end_fmt),
    }
    if codes:
        filter_payload['country'] = ','.join(codes)

    try:
        response = requests.get(
            KIWIPIXEL_COUNTRY_API,
            params={'filter': json.dumps(filter_payload, separators=(',', ':'))},
            timeout=30,
            headers={
                'Accept': 'application/json',
                'User-Agent': 'hris-management/1.0',
            },
        )
        if response.status_code != 200:
            return {}
        payload = response.json() if response.content else {}
    except Exception:
        return {}

    normalized_filters = [
        str(item or '').strip()
        for item in (domain_filters or [])
        if str(item or '').strip() and str(item or '').strip() != '%'
    ]
    metrics_by_country = {}
    for country_row in (payload.get('countries') or []):
        if not isinstance(country_row, dict):
            continue
        country_code = _normalize_kiwipixel_country_code(country_row.get('country_code'))
        if not country_code:
            continue
        if requested_code_set and country_code not in requested_code_set:
            continue
        domain_rows = country_row.get('domains') or []
        if not isinstance(domain_rows, list):
            domain_rows = []
        out = metrics_by_country.setdefault(country_code, _zero_kiwipixel_traffic_metrics())
        if not normalized_filters:
            _add_kiwipixel_traffic_metrics(out, country_row)
        else:
            for domain_row in domain_rows:
                if _kiwipixel_domain_row_matches(domain_row, normalized_filters):
                    _add_kiwipixel_traffic_metrics(out, domain_row)
    return metrics_by_country


def _fetch_kiwipixel_country_visits(country_codes, start_date, end_date, domain_filters):
    metrics_by_country = _fetch_kiwipixel_country_metrics(
        country_codes, start_date, end_date, domain_filters
    )
    return {
        code: int((metrics or {}).get('total_visits') or 0)
        for code, metrics in (metrics_by_country or {}).items()
    }


def _fetch_kiwipixel_metrics_by_domain(start_date, end_date, domain_filters=None):
    start_fmt = _normalize_kiwipixel_date(start_date)
    end_fmt = _normalize_kiwipixel_date(end_date)
    if not start_fmt or not end_fmt:
        return {}

    filter_payload = {
        'start_date': int(start_fmt),
        'end_date': int(end_fmt),
    }
    try:
        response = requests.get(
            KIWIPIXEL_COUNTRY_API,
            params={'filter': json.dumps(filter_payload, separators=(',', ':'))},
            timeout=30,
            headers={
                'Accept': 'application/json',
                'User-Agent': 'hris-management/1.0',
            },
        )
        if response.status_code != 200:
            return {}
        payload = response.json() if response.content else {}
    except Exception:
        return {}

    normalized_filters = [
        str(item or '').strip()
        for item in (domain_filters or [])
        if str(item or '').strip() and str(item or '').strip() != '%'
    ]
    metrics_by_domain = {}
    for country_row in (payload.get('countries') or []):
        if not isinstance(country_row, dict):
            continue
        for domain_row in (country_row.get('domains') or []):
            if not isinstance(domain_row, dict):
                continue
            api_domain = domain_row.get('domain')
            if normalized_filters and not _kiwipixel_domain_matches(api_domain, normalized_filters):
                continue
            domain_key = _normalize_kiwipixel_domain_key(api_domain)
            if not domain_key:
                continue
            acc = metrics_by_domain.setdefault(domain_key, _zero_kiwipixel_traffic_metrics())
            _add_kiwipixel_traffic_metrics(acc, domain_row)
    return metrics_by_domain


def _fetch_kiwipixel_visits_by_domain(start_date, end_date, domain_filters=None):
    metrics_by_domain = _fetch_kiwipixel_metrics_by_domain(start_date, end_date, domain_filters)
    return {
        dkey: int((metrics or {}).get('total_visits') or 0)
        for dkey, metrics in (metrics_by_domain or {}).items()
    }


def attach_kiwipixel_visitors_to_country_result(result, start_date, end_date, domain_filters=None):
    if not isinstance(result, dict) or not result.get('status'):
        return result

    def normalize_cc(cc):
        c = (str(cc or '').strip().upper())
        if c == 'TU':
            return 'TR'
        return c

    def lookup_metrics(code, metrics_map):
        cc = normalize_cc(code)
        if not cc:
            return _zero_kiwipixel_traffic_metrics()
        metrics = metrics_map.get(cc)
        if not metrics and cc == 'TR':
            metrics = metrics_map.get('TU')
        return dict(metrics) if metrics else _zero_kiwipixel_traffic_metrics()

    country_codes = []
    for key in ('data', 'data_filtered'):
        for row in (result.get(key) or []):
            if not isinstance(row, dict):
                continue
            cc = normalize_cc(row.get('country_code'))
            if cc and cc not in country_codes:
                country_codes.append(cc)

    domain_list = []
    if domain_filters:
        if isinstance(domain_filters, str):
            domain_list = [s.strip() for s in domain_filters.split(',') if s.strip() and s.strip() != '%']
        else:
            domain_list = [
                str(s).strip()
                for s in domain_filters
                if str(s).strip() and str(s).strip() != '%'
            ]

    if not country_codes:
        return result

    metrics_by_country = _fetch_kiwipixel_country_metrics(
        country_codes, start_date, end_date, domain_list
    )
    for key in ('data', 'data_filtered'):
        for row in (result.get(key) or []):
            if isinstance(row, dict):
                metrics = lookup_metrics(row.get('country_code'), metrics_by_country)
                row['total_visits'] = metrics.get('total_visits', 0)
                row['unique_visitor'] = metrics.get('unique_visitor', 0)
                row['total_pageviews'] = metrics.get('total_pageviews', 0)
                row['total_visitors'] = metrics.get('total_visits', 0)

    def sum_visitors(rows):
        return sum(int((r or {}).get('total_visits') or (r or {}).get('total_visitors') or 0) for r in (rows or []) if isinstance(r, dict))

    if isinstance(result.get('summary_all'), dict):
        result['summary_all']['total_visitors'] = sum_visitors(result.get('data'))
    if isinstance(result.get('summary_filtered'), dict):
        result['summary_filtered']['total_visitors'] = sum_visitors(result.get('data_filtered'))
    if isinstance(result.get('summary'), dict):
        result['summary']['total_visitors'] = sum_visitors(result.get('data'))

    return result


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

        country_codes_for_api = selected_countries[:] if selected_countries else []
        if not country_codes_for_api:
            country_codes_for_api = sorted({
                str(row.get('country_code') or '').strip().upper()
                for row in (data.get('data') or [])
                if str(row.get('country_code') or '').strip()
            })
        metrics_by_country = _fetch_kiwipixel_country_metrics(
            country_codes_for_api,
            tanggal_dari,
            tanggal_sampai,
            selected_domain_list,
        )

        def lookup_country_metrics(code):
            cc = str(code or '').strip().upper()
            if not cc:
                return _zero_kiwipixel_traffic_metrics()
            metrics = metrics_by_country.get(cc)
            if not metrics and cc == 'TR':
                metrics = metrics_by_country.get('TU')
            return dict(metrics) if metrics else _zero_kiwipixel_traffic_metrics()

        total_visits_sum = 0
        total_unique_sum = 0
        total_pageviews_sum = 0
        for row in (data.get('data') or []):
            metrics = lookup_country_metrics(row.get('country_code'))
            row['total_visits'] = metrics.get('total_visits', 0)
            row['unique_visitor'] = metrics.get('unique_visitor', 0)
            row['total_pageviews'] = metrics.get('total_pageviews', 0)
            row['total_visitors'] = metrics.get('total_visits', 0)
            total_visits_sum += int(metrics.get('total_visits') or 0)
            total_unique_sum += int(metrics.get('unique_visitor') or 0)
            total_pageviews_sum += int(metrics.get('total_pageviews') or 0)
        if isinstance(data.get('total'), dict):
            data['total']['total_visitors'] = total_visits_sum
            data['total']['total_visits'] = total_visits_sum
            data['total']['unique_visitor'] = total_unique_sum
            data['total']['total_pageviews'] = total_pageviews_sum

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


class InvalidReportAdxView(View):
    """Halaman laporan perbandingan rekap bulanan vs harian AdX."""

    def dispatch(self, request, *args, **kwargs):
        if 'hris_admin' not in request.session:
            return redirect('admin_login')
        return super().dispatch(request, *args, **kwargs)

    def get(self, req):
        today = datetime.now().date()
        prev_month = today.replace(day=1) - timedelta(days=1)
        month_names = ['Jan', 'Feb', 'Mar', 'Apr', 'Mei', 'Jun', 'Jul', 'Agu', 'Sep', 'Okt', 'Nov', 'Des']
        months = [(f'{i:02d}', month_names[i - 1]) for i in range(1, 13)]
        data = {
            'title': 'Invalid Report AdX',
            'user': req.session['hris_admin'],
            'default_year': prev_month.year,
            'default_month': f'{prev_month.month:02d}',
            'months': months,
        }
        return render(req, 'admin/adx_manager/invalid_report/index.html', data)


class InvalidReportAdxDataView(View):
    """API data laporan invalid AdX."""

    def dispatch(self, request, *args, **kwargs):
        if 'hris_admin' not in request.session:
            return JsonResponse({'status': False, 'error': 'Unauthorized'}, status=401)
        return super().dispatch(request, *args, **kwargs)

    def get(self, req):
        try:
            result = data_mysql().list_adx_rekap_invalid_report(
                year=req.GET.get('year'),
                month=req.GET.get('month'),
                tanggal_tarik=req.GET.get('tanggal_tarik'),
                status_filter=req.GET.get('status') or 'all',
                domain_q=req.GET.get('q') or '',
                hide_zero_spend=req.GET.get('hide_zero_spend'),
            )
            if not result.get('status'):
                return JsonResponse({'status': False, 'error': result.get('data') or 'Gagal memuat data'}, status=400)
            return JsonResponse({'status': True, 'data': result.get('data')}, safe=False)
        except Exception as e:
            logger.exception('InvalidReportAdxDataView failed')
            return JsonResponse({'status': False, 'error': str(e)}, status=500)


class InvalidReportAdxDetailView(View):
    """Detail harian per domain untuk laporan invalid AdX."""

    def dispatch(self, request, *args, **kwargs):
        if 'hris_admin' not in request.session:
            return JsonResponse({'status': False, 'error': 'Unauthorized'}, status=401)
        return super().dispatch(request, *args, **kwargs)

    def get(self, req):
        domain = str(req.GET.get('domain') or '').strip()
        if not domain:
            return JsonResponse({'status': False, 'error': 'domain wajib diisi'}, status=400)
        try:
            result = data_mysql().get_adx_invalid_report_domain_detail(
                domain=domain,
                year=req.GET.get('year'),
                month=req.GET.get('month'),
                tanggal_tarik=req.GET.get('tanggal_tarik'),
            )
            if not result.get('status'):
                return JsonResponse({'status': False, 'error': result.get('data') or 'Gagal memuat detail'}, status=400)
            return JsonResponse({'status': True, 'data': result.get('data')}, safe=False)
        except Exception as e:
            logger.exception('InvalidReportAdxDetailView failed')
            return JsonResponse({'status': False, 'error': str(e)}, status=500)


class InvalidReportAdsView(View):
    """Halaman laporan perbandingan rekap bulanan vs harian Facebook Ads."""

    def dispatch(self, request, *args, **kwargs):
        if 'hris_admin' not in request.session:
            return redirect('admin_login')
        return super().dispatch(request, *args, **kwargs)

    def get(self, req):
        today = datetime.now().date()
        prev_month = today.replace(day=1) - timedelta(days=1)
        month_names = ['Jan', 'Feb', 'Mar', 'Apr', 'Mei', 'Jun', 'Jul', 'Agu', 'Sep', 'Okt', 'Nov', 'Des']
        months = [(f'{i:02d}', month_names[i - 1]) for i in range(1, 13)]
        data = {
            'title': 'Invalid Report Facebook Ads',
            'user': req.session['hris_admin'],
            'default_year': prev_month.year,
            'default_month': f'{prev_month.month:02d}',
            'months': months,
        }
        return render(req, 'admin/facebook_ads/invalid_report/index.html', data)


class InvalidReportAdsDataView(View):
    """API data laporan invalid Facebook Ads."""

    def dispatch(self, request, *args, **kwargs):
        if 'hris_admin' not in request.session:
            return JsonResponse({'status': False, 'error': 'Unauthorized'}, status=401)
        return super().dispatch(request, *args, **kwargs)

    def get(self, req):
        try:
            result = data_mysql().list_ads_rekap_invalid_report(
                year=req.GET.get('year'),
                month=req.GET.get('month'),
                tanggal_tarik=req.GET.get('tanggal_tarik'),
                status_filter=req.GET.get('status') or 'all',
                domain_q=req.GET.get('q') or '',
            )
            if not result.get('status'):
                return JsonResponse({'status': False, 'error': result.get('data') or 'Gagal memuat data'}, status=400)
            return JsonResponse({'status': True, 'data': result.get('data')}, safe=False)
        except Exception as e:
            logger.exception('InvalidReportAdsDataView failed')
            return JsonResponse({'status': False, 'error': str(e)}, status=500)


class InvalidReportAdsDetailView(View):
    """Detail harian per domain untuk laporan invalid Facebook Ads."""

    def dispatch(self, request, *args, **kwargs):
        if 'hris_admin' not in request.session:
            return JsonResponse({'status': False, 'error': 'Unauthorized'}, status=401)
        return super().dispatch(request, *args, **kwargs)

    def get(self, req):
        domain = str(req.GET.get('domain') or '').strip()
        if not domain:
            return JsonResponse({'status': False, 'error': 'domain wajib diisi'}, status=400)
        try:
            result = data_mysql().get_ads_invalid_report_domain_detail(
                domain=domain,
                year=req.GET.get('year'),
                month=req.GET.get('month'),
                tanggal_tarik=req.GET.get('tanggal_tarik'),
            )
            if not result.get('status'):
                return JsonResponse({'status': False, 'error': result.get('data') or 'Gagal memuat detail'}, status=400)
            return JsonResponse({'status': True, 'data': result.get('data')}, safe=False)
        except Exception as e:
            logger.exception('InvalidReportAdsDetailView failed')
            return JsonResponse({'status': False, 'error': str(e)}, status=500)


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
            return_to = str(req.GET.get('return_to') or '').strip().lower()
            if return_to in ('policy_events', 'adsense_policy_events'):
                req.session['oauth_return_to'] = 'adsense_policy_events'
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
                'https://www.googleapis.com/auth/gmail.readonly',
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
                'include_granted_scopes': 'true',
                'prompt': 'consent select_account',
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
                'https://www.googleapis.com/auth/gmail.readonly',
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
                attach_kiwipixel_visitors_to_country_result(
                    cached_response, start_date, end_date, selected_domain_list
                )
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
            attach_kiwipixel_visitors_to_country_result(
                result, start_date, end_date, selected_domain_list
            )
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

class KomparasiTrafficReportView(View):
    """View untuk Komparasi Traffic (unique overlap)"""
    def dispatch(self, request, *args, **kwargs):
        if 'hris_admin' not in request.session:
            return redirect('admin_login')
        return super().dispatch(request, *args, **kwargs)

    def get(self, req):
        data = {
            'title': 'Komparasi Traffic',
            'user': req.session['hris_admin']
        }
        return render(req, 'admin/report_traffic/komparasi_traffic/index.html', data)

class TrafficOverlapProxyView(View):
    """Proxy untuk api-tracker traffic-overlap dengan header Origin dari hostname server."""
    def dispatch(self, request, *args, **kwargs):
        if 'hris_admin' not in request.session:
            return redirect('admin_login')
        return super().dispatch(request, *args, **kwargs)

    def get(self, req):
        try:
            filter_raw = str(req.GET.get('filter') or '').strip()
            if filter_raw:
                try:
                    filter_obj = json.loads(filter_raw)
                except Exception:
                    return JsonResponse({'status': False, 'error': 'Invalid filter JSON'}, status=400)
            else:
                domains = str(req.GET.get('domains') or '').strip()
                identity = str(req.GET.get('identity') or 'device_fingerprint').strip()
                start_date = req.GET.get('start_date')
                end_date = req.GET.get('end_date')
                filter_obj = {
                    'domains': domains,
                    'identity': identity,
                    'start_date': int(start_date) if start_date is not None and str(start_date).strip() else None,
                    'end_date': int(end_date) if end_date is not None and str(end_date).strip() else None
                }

            origin = req.get_host()
            url = "https://api-tracker.kiwipixel.com/v1/traffic-overlap"
            tracker_resp = requests.get(
                url,
                params={'filter': json.dumps(filter_obj)},
                timeout=60,
                headers={
                    'Accept': 'application/json',
                    'User-Agent': 'hris-management/1.0',
                    'Origin': origin
                }
            )
            if tracker_resp.status_code != 200:
                return JsonResponse({
                    'status': False,
                    'error': f'Upstream error status={tracker_resp.status_code}'
                }, status=502)

            try:
                payload = tracker_resp.json() if tracker_resp.content else {}
            except Exception:
                payload = {}
            return JsonResponse(payload, safe=False)
        except Exception as e:
            return JsonResponse({'status': False, 'error': str(e)}, status=500)

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

            rs_spend_daily = data_mysql().get_daily_ads_spend_by_domain_keys_and_date(domain_keys, start_date, end_date)
            if not isinstance(rs_spend_daily, dict) or not rs_spend_daily.get('status'):
                return JsonResponse({
                    'status': False,
                    'error': (rs_spend_daily or {}).get('data') if isinstance(rs_spend_daily, dict) else 'Failed query daily ad spend'
                }, status=500)

            rs_revenue_daily = data_mysql().get_daily_adx_revenue_by_domains_and_date(raw_domains, start_date, end_date)
            if not isinstance(rs_revenue_daily, dict) or not rs_revenue_daily.get('status'):
                return JsonResponse({
                    'status': False,
                    'error': (rs_revenue_daily or {}).get('data') if isinstance(rs_revenue_daily, dict) else 'Failed query daily revenue'
                }, status=500)

            daily_ad_spend = (rs_spend_daily.get('data') or {}) if isinstance(rs_spend_daily.get('data'), dict) else {}
            daily_revenue = (rs_revenue_daily.get('data') or {}) if isinstance(rs_revenue_daily.get('data'), dict) else {}
            total_ad_spend = float(((rs_spend.get('data') or {}).get('total_ad_spend')) or 0)
            total_revenue = float(((rs_revenue.get('data') or {}).get('total_revenue')) or 0)
            return JsonResponse({
                'status': True,
                'total_ad_spend': total_ad_spend,
                'total_revenue': total_revenue,
                'daily_ad_spend': daily_ad_spend,
                'daily_revenue': daily_revenue,
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
            kiwi_traffic_indexes = _fetch_kiwipixel_campaign_traffic(start_date_formatted, end_date_formatted)
            campaign_ids_by_domain = _fetch_fb_campaign_ids_by_domain(
                start_date_formatted,
                end_date_formatted,
                unique_name_site or domain_terms,
            )

            def visitor_metrics_for_site(site_display_name):
                dkey = _normalize_kiwipixel_domain_key(site_display_name)
                campaign_ids = campaign_ids_by_domain.get(dkey, [])
                return _resolve_kiwipixel_campaign_visitors(site_display_name, campaign_ids, kiwi_traffic_indexes)

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
                    site_display = item['site_name'] + '.com'
                    visitors = visitor_metrics_for_site(site_display)
                    combined_data_all.append({
                        'site_name': site_display,
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
                        'total_visits': visitors.get('total_visits', 0),
                        'unique_visitor': visitors.get('unique_visitor', 0),
                        'total_pageviews': visitors.get('total_pageviews', 0),
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
                    site_display = item['site_name'] + '.com'
                    visitors = visitor_metrics_for_site(site_display)
                    combined_data_filtered.append({
                        'site_name': site_display,
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
                        'total_visits': visitors.get('total_visits', 0),
                        'unique_visitor': visitors.get('unique_visitor', 0),
                        'total_pageviews': visitors.get('total_pageviews', 0),
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
                selected_domain_str = str(selected_domain.split(',')[0]).strip()
            selected_domain_list = build_domain_filter_terms(
                selected_domain_str,
                include_original=True,
                include_base=True,
            ) if selected_domain_str else []
            cache_key = generate_cache_key(
                'roi_country_hourly_v4',
                target_date,
                ','.join(selected_domain_list) if selected_domain_list else selected_domain_str,
            )
            cached = get_cached_data(cache_key)
            if cached is not None:
                return JsonResponse(cached, safe=False)
            db = data_mysql()
            adx_resp = db.get_all_adx_roi_country_hourly_logs_by_params(
                target_date,
                selected_domain_list if selected_domain_list else selected_domain_str,
            )
            adx_rows = adx_resp.get('data') if isinstance(adx_resp, dict) else []
            ads_resp = db.get_all_ads_roi_country_hourly_logs_by_params(
                target_date,
                selected_domain_list if selected_domain_list else selected_domain_str,
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
            countries_raw = (req.GET.get('country_code') or req.GET.get('selected_countries') or '').strip()
            selected_country_set = set()
            if countries_raw:
                for part in countries_raw.split(','):
                    cc = str(part or '').strip().upper()
                    if cc == 'TU':
                        cc = 'TR'
                    if cc:
                        selected_country_set.add(cc)
                if 'TR' in selected_country_set:
                    selected_country_set.add('TU')

            def _country_match(row):
                if not selected_country_set:
                    return True
                cc = str((row or {}).get('country_code', '') or '').strip().upper()
                if cc == 'TU':
                    cc = 'TR'
                return cc in selected_country_set

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

            if selected_country_set:
                adx_rows = [r for r in (adx_rows or []) if _country_match(r)]
                adsense_rows = [r for r in (adsense_rows or []) if _country_match(r)]

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
            ads_rows = []
            if selected_country_set:
                ads_resp = db.get_all_ads_roi_country_hourly_logs_by_params(
                    tanggal_formatted,
                    unique_name_site if unique_name_site else None
                )
                ads_rows_raw = ((ads_resp or {}).get('hasil') or {}).get('data') or []
                if not isinstance(ads_rows_raw, list):
                    ads_rows_raw = []
                spend_by_country_hour = {f"{h:02d}": 0.0 for h in range(24)}
                for row in ads_rows_raw:
                    if not _country_match(row):
                        continue
                    try:
                        hour = int(row.get('hour', 0) or 0)
                    except Exception:
                        hour = 0
                    if hour < 0 or hour > 23:
                        continue
                    hkey = f"{hour:02d}"
                    spend_by_country_hour[hkey] = spend_by_country_hour.get(hkey, 0.0) + float(row.get('spend', 0) or 0)
                ads_rows = [{'hour': int(h), 'spend': spend_by_country_hour[h]} for h in spend_by_country_hour]
            elif unique_name_site:
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
                revenue_series.append(round(r, 2))
                spend_series.append(round(s, 2))
                roi_series.append(round((((r - s) / s) * 100) if s > 0 else 0.0, 2))
                if r > 0 or s > 0:
                    total_revenue = r
                    total_spend = s
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

                    row_key = f"{date_key}_{site_key}_{country_code}"
                    cur_row = raw_rows_map.get(row_key)
                    if not cur_row:
                        cur_row = {
                            'site_name': site_key,
                            'date': date_key,
                            'country_code': country_code,
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
                    row_key = f"{date_key}_{site_key}_{country_code}"
                    cur_row = raw_rows_map.get(row_key)
                    if not cur_row:
                        cur_row = {
                            'site_name': site_key,
                            'date': date_key,
                            'country_code': country_code,
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
                raw_rows_all.sort(key=lambda x: (
                    str((x or {}).get('date') or ''),
                    str((x or {}).get('site_name') or ''),
                    str((x or {}).get('country_code') or '')
                ))
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


def _is_fb_token_session_invalid(message):
    msg = str(message or '').lower()
    return (
        'oauthexception' in msg
        or 'error validating access token' in msg
        or 'session has been invalidated' in msg
        or 'changed their password' in msg
        or '"code": 190' in msg
        or "'code': 190" in msg
        or 'error_subcode' in msg and '460' in msg
    )


def _friendly_fb_campaign_error(message):
    raw_message = str(message or '').strip()
    if _is_fb_token_session_invalid(raw_message):
        return (
            'Sesi Facebook untuk account ads ini sudah tidak valid atau sudah expired. '
            'Silakan login ulang / refresh access token Meta pada account tersebut, lalu coba muat campaign lagi.'
        )
    return raw_message or 'Gagal memuat data campaign dari Facebook.'

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
            return JsonResponse({'status': False, 'error': _friendly_fb_campaign_error(e), 'campaigns': []}, safe=False)

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
            return JsonResponse({'status': False, 'error': _friendly_fb_campaign_error(e)})

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
                error_text = 'Gagal mengupdate status campaign'
                if failed:
                    error_text = _friendly_fb_campaign_error((failed[0] or {}).get('error') or error_text)
                return JsonResponse({'status': False, 'error': error_text, 'failed': failed}, safe=False)

            return JsonResponse({
                'status': True,
                'updated': success_count,
                'failed': failed,
                'campaign_status': last_status or status,
            }, safe=False)
        except Exception as e:
            return JsonResponse({'status': False, 'error': _friendly_fb_campaign_error(e)})

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
                attach_kiwipixel_visitors_to_country_result(
                    cached_response, start_date, end_date, selected_domain_list
                )
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
            attach_kiwipixel_visitors_to_country_result(
                result, start_date, end_date, selected_domain_list
            )
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

import os
import json
import calendar
from typing import Any
# Guard pandas import to avoid crash when numpy binaries are incompatible
try:
    import pandas as pd
except Exception:
    pd = None
from django.shortcuts import redirect, render
from django.http import JsonResponse
from django.views import View
from datetime import datetime, date, timedelta
from django.views.decorators.csrf import csrf_exempt
from django.utils.decorators import method_decorator
from google_auth_oauthlib.flow import Flow
from django.core.cache import cache
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeoutError
# Guard googleapiclient import to prevent module-level crash when not installed
try:
    from googleapiclient.discovery import build
except Exception:
    build = None
from google.oauth2.credentials import Credentials
try:
    from .database import data_mysql
except Exception:
    try:
        from management.database import data_mysql
    except Exception:
        from settings.database import data_mysql
from .utils_adsense import (
    fetch_adsense_traffic_account_data,
    fetch_adsense_summary_data,
    fetch_adsense_traffic_per_country,
    fetch_adsense_account_info_and_units,
    set_cached_data_adsense,
    generate_cache_key_adsense,
    get_cached_data_adsense,
)

def get_adsense_data(request):
    if 'credentials' not in request.session:
        return redirect('authorize')
    creds_data = request.session['credentials']
    creds = Credentials(**creds_data)
    service = build('adsense', 'v2', credentials=creds)
    accounts = service.accounts().list().execute()
    return render(request, 'adsense_manager/traffic_per_account/index.html', {'accounts': accounts})

class AdsenseTrafficAccountView(View):
    """View untuk menampilkan halaman AdSense Traffic Account"""
    def dispatch(self, request, *args, **kwargs):
        # Check if user is logged in as admin
        if 'hris_admin' not in request.session:
            return redirect('/management/admin/login')
        return super().dispatch(request, *args, **kwargs)
    
    def get(self, req):
        admin = req.session.get('hris_admin', {})
        if admin.get('super_st') == '0':
            data_account_adsense = data_mysql().get_all_adsense_account_data_user(admin.get('user_id'))
        else:
            data_account_adsense = data_mysql().get_all_adsense_account_data()
        if not data_account_adsense['status']:
            return JsonResponse({
                'status': False,
                'error': data_account_adsense['data']
            })
        data = {
            'title': 'AdSense Traffic Per Account',
            'user': req.session['hris_admin'],
            'data_account_adsense': data_account_adsense['data']
        }
        return render(req, 'admin/adsense_manager/traffic_account/index.html', data)

@method_decorator(csrf_exempt, name='dispatch')
class AdsenseTrafficAccountDataView(View):
    """API endpoint untuk mengambil data AdSense Traffic Account"""
    def dispatch(self, request, *args, **kwargs):
        # Check if user is logged in as admin
        if 'hris_admin' not in request.session:
            return JsonResponse({'error': 'Unauthorized'}, status=401)
        return super().dispatch(request, *args, **kwargs)

    def get(self, req):
        start_date = req.GET.get('start_date')
        end_date = req.GET.get('end_date')
        selected_account = req.GET.get('selected_account')
        selected_account_list = []
        if selected_account:
            selected_account_list = [str(s).strip() for s in selected_account.split(',') if s.strip()]

        selected_countries = req.GET.get('selected_countries', '')
        countries_list = []
        if selected_countries and selected_countries.strip():
            countries_list = [c.strip().upper() for c in selected_countries.split(',') if c.strip()]

        if not start_date or not end_date:
            return JsonResponse({
                'status': False,
                'error': 'Start date and end date are required'
            })
        try:
            start_date_formatted = datetime.strptime(start_date, '%Y-%m-%d').strftime('%Y-%m-%d')
            end_date_formatted = datetime.strptime(end_date, '%Y-%m-%d').strftime('%Y-%m-%d')

            rs_result = data_mysql().get_all_adsense_traffic_account_by_params(start_date_formatted, end_date_formatted, selected_account_list)
            raw_rows = []
            if rs_result and isinstance(rs_result, dict):
                raw_rows = (rs_result.get('hasil') or {}).get('data') or []

            if countries_list:
                raw_rows = [r for r in raw_rows if str((r or {}).get('country_code') or '').strip().upper() in countries_list]

            rows_map = {}
            if raw_rows:
                for rs in raw_rows:
                    date_key = str(rs.get('date', '') or '')
                    raw_site = str(rs.get('site_name', '') or '')
                    base_subdomain = extract_base_subdomain(raw_site) if raw_site else ''
                    if not base_subdomain:
                        base_subdomain = raw_site
                    account_name = str(rs.get('account_name', '') or '')
                    impressions = int(rs.get('impressions_adsense', 0) or 0)
                    clicks = int(rs.get('clicks_adsense', 0) or 0)
                    revenue = float(rs.get('revenue', 0.0) or 0.0)
                    key = f"{date_key}|{base_subdomain}"
                    entry = rows_map.get(key) or {'date': date_key, 'account_name': account_name, 'site_name': base_subdomain, 'impressions_adsense': 0, 'clicks_adsense': 0, 'revenue': 0.0}
                    entry['account_name'] = account_name
                    entry['impressions_adsense'] += impressions
                    entry['clicks_adsense'] += clicks
                    entry['revenue'] += revenue
                    rows_map[key] = entry
            result_rows = []
            total_impressions = 0
            total_clicks = 0
            total_revenue = 0.0
            for _, item in rows_map.items():
                imp = int(item.get('impressions_adsense') or 0)
                clk = int(item.get('clicks_adsense') or 0)
                rev = float(item.get('revenue') or 0.0)
                cpc_adsense = (rev / clk) if clk > 0 else 0.0
                ctr = ((clk / imp) * 100) if imp > 0 else 0.0
                ecpm = ((rev / imp) * 1000) if imp > 0 else 0.0
                total_impressions += imp
                total_clicks += clk
                total_revenue += rev
                result_rows.append({
                    'date': item['date'],
                    'account_name': item['account_name'],
                    'site_name': item['site_name'] + '.com',
                    'impressions_adsense': item['impressions_adsense'],
                    'clicks_adsense': item['clicks_adsense'],
                    'cpc_adsense': round(cpc_adsense, 2),
                    'ecpm': round(ecpm, 2),
                    'ctr': round(ctr, 2),
                    'revenue': round(rev, 2)
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
                'message': 'Data adsense traffic account berhasil diambil',
                'summary': summary,
                'data': result_rows
            }, safe=False)
        except Exception as e:
            return JsonResponse({
                'status': False,
                'error': str(e)
            })

def extract_base_subdomain(full_string):
    parts = full_string.split('.')
    # jika ada minimal 2 bagian (1 titik), ambil dua bagian pertama
    if len(parts) >= 2:
        main_domain = ".".join(parts[:2])
    else:
        main_domain = full_string
    # jika tidak ada titik, kembalikan string asli
    return main_domain

class AdsenseSitesListView(View):
    """AJAX endpoint untuk mengambil daftar situs dari Ad Manager"""
    def dispatch(self, request, *args, **kwargs):
        if 'hris_admin' not in request.session:
            return redirect('admin_login')
        return super().dispatch(request, *args, **kwargs)
    def get(self, req):
        selected_accounts = req.GET.get('selected_accounts')
        print(f"selected_accounts: {selected_accounts}")
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
                cache_key = generate_cache_key_adsense('adsense_sites_list', str(user_mail or ''), str(selected_accounts or ''))
                cached_sites = get_cached_data_adsense(cache_key)
                if isinstance(cached_sites, dict) and cached_sites.get('status') and cached_sites.get('data'):
                    return JsonResponse(cached_sites, safe=False)
            except Exception as _cache_err:
                # Lanjutkan tanpa memblokir jika cache gagal
                print(f"[WARNING] adsense_sites_list cache unavailable: {_cache_err}")

            # Ambil daftar situs dari Ad Manager jika cache miss
            # result = fetch_user_sites_list(user_mail)
            end_date = date.today()
            start_date = end_date - timedelta(days=7)
            result = data_mysql().fetch_user_adsense_sites_list(
                start_date.strftime('%Y-%m-%d'),
                end_date.strftime('%Y-%m-%d'),
                selected_account_list,
            )
            # Simpan ke cache untuk permintaan berikutnya
            try:
                # Cache selama 6 jam; daftar situs jarang berubah
                set_cached_data_adsense(cache_key, result['hasil'], timeout=6 * 60 * 60)
            except Exception as _cache_set_err:
                print(f"[WARNING] failed to cache ads_sites_list: {_cache_set_err}")
            return JsonResponse(result['hasil'], safe=False)
        except Exception as e:
            return JsonResponse({
                'status': False,
                'error': str(e)
            })

class AdsenseAccountListView(View):
    """AJAX endpoint untuk mengambil daftar akun dari AdSense"""
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
                cache_key = generate_cache_key_adsense('adsense_accounts_list', str(selected_domains or ''))
                cached_accounts = get_cached_data_adsense(cache_key)
                if isinstance(cached_accounts, dict) and cached_accounts.get('status') and cached_accounts.get('data'):
                    return JsonResponse(cached_accounts, safe=False)
            except Exception as _cache_err:
                # Lanjutkan tanpa memblokir jika cache gagal
                print(f"[WARNING] adsense_account_list cache unavailable: {_cache_err}")
            # Ambil daftar situs dari Ad Manager jika cache miss
            # result = fetch_user_sites_list(user_mail)
            end_date = date.today()
            start_date = end_date - timedelta(days=7)
            result = data_mysql().fetch_adsense_account_list_by_domain(
                selected_domain_list,
                start_date,
                end_date,
            )
            print(f"[DEBUG] AdsenseAccountListView - result: {result}")
            # Simpan ke cache untuk permintaan berikutnya
            try:
                # Cache selama 6 jam; daftar akun jarang berubah
                set_cached_data_adsense(cache_key, result['hasil'], timeout=6 * 60 * 60)
            except Exception as _cache_set_err:
                print(f"[WARNING] failed to cache adsense_account_list: {_cache_set_err}")
            return JsonResponse(result['hasil'], safe=False)
        except Exception as e:
            return JsonResponse({
                'status': False,
                'error': str(e)
            })

@csrf_exempt
def get_countries_adsense(request):
    """Endpoint untuk mendapatkan daftar negara yang tersedia"""
    if 'hris_admin' not in request.session:
        return redirect('admin_login')
    try:
        # Ambil data negara dari AdX untuk periode 30 hari terakhir
        end_date = datetime.now().date()
        start_date = end_date - timedelta(days=7)
        selected_account = request.GET.get('selected_accounts')
        # Gunakan cache untuk menghindari pemanggilan API berulang
        print(f"[DEBUG] Request params: start_date={start_date}, end_date={end_date}, selected_account={selected_account}")
        try:
            cache_key = generate_cache_key_adsense(
                'countries_adsense',
                start_date.strftime('%Y-%m-%d'),
                end_date.strftime('%Y-%m-%d'),
                selected_account or '',
            )
            cached_countries = get_cached_data_adsense(cache_key)
            if cached_countries is not None:
                return JsonResponse({
                    'status': 'success',
                    'countries': cached_countries
                })
        except Exception as _cache_err:
            # Jika cache bermasalah, lanjutkan tanpa memblokir proses
            print(f"[WARNING] countries_adsense cache unavailable: {_cache_err}")
        result = data_mysql().fetch_country_list_adsense(
            start_date.strftime('%Y-%m-%d'),
            end_date.strftime('%Y-%m-%d'),
            selected_account,
        )
        print(f"[DEBUG] Raw adsense countries result: {result}")
        # Validasi struktur result
        if not result['hasil']['data']:
            print("[WARNING] Adsense countries result is None or empty")
            return JsonResponse({
                'status': 'error',
                'message': 'Tidak ada data adsense country yang tersedia.',
                'countries': []
            })
        
        if not isinstance(result['hasil'], dict):
            print(f"[WARNING] Adsense countries result['hasil'] is not a dict: {type(result['hasil'])}")
            return JsonResponse({
                'status': 'error',
                'message': 'Format data adsense country tidak valid.',
                'countries': []
            })
        
        # Periksa apakah ada key 'data' dalam result['hasil']
        if 'data' not in result['hasil']:
            print(f"[WARNING] Adsense countries result['hasil'] has no 'data' key. Available keys: {list(result['hasil'].keys())}")
            return JsonResponse({
                'status': 'error',
                'message': 'Data adsense country tidak tersedia.',
                'countries': []
            })
        
        # Periksa apakah data adalah list
        if not isinstance(result['hasil']['data'], list):
            print(f"[WARNING] Adsense countries result['hasil']['data'] is not a list: {type(result['hasil']['data'])}")
            return JsonResponse({
                'status': 'error',
                'message': 'Format data adsense country tidak valid.',
                'countries': []
            })
        
        # Ekstrak daftar negara dari data yang tersedia dan hilangkan duplikasi
        countries = []
        seen = set()
        for country_data in result['hasil']['data']:
            if not isinstance(country_data, dict):
                print(f"[WARNING] Adsense countries result['hasil']['data'] country data is not a dict: {type(country_data)}")
                continue
            country_name = (country_data.get('country_name') or '').strip()
            country_code = (country_data.get('country_code') or '').strip().upper()
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
            set_cached_data_adsense(cache_key, countries, timeout=6 * 60 * 60)  # 6 jam
        except Exception as _cache_set_err:
            print(f"[WARNING] Adsense countries failed to cache countries_adsense: {_cache_set_err}")
        return JsonResponse({
            'status': 'success',
            'countries': countries
        })
        
    except Exception as e:
        print(f"[ERROR] Adsense countries failed to fetch countries_adsense: {e}")
        import traceback
        print(f"[ERROR] Traceback: {traceback.format_exc()}")
        return JsonResponse({
            'status': 'error',
            'message': 'Gagal mengambil data adsense country.',
            'error': str(e),
            'countries': []
        }, status=500)

class AdsenseSummaryView(View):
    """View untuk halaman Summary AdSense"""
    def dispatch(self, request, *args, **kwargs):
        if 'hris_admin' not in request.session:
            return redirect('admin_login')
        return super().dispatch(request, *args, **kwargs)

    def get(self, req):
        admin = req.session.get('hris_admin', {})
        if admin.get('super_st') == '0':
            data_account_adsense = data_mysql().get_all_adsense_account_data_user(admin.get('user_id'))
        else:
            data_account_adsense = data_mysql().get_all_adsense_account_data()

        if not data_account_adsense.get('status'):
            return JsonResponse({
                'status': False,
                'error': data_account_adsense.get('data')
            })

        data = {
            'title': 'AdSense Summary Dashboard',
            'user': req.session['hris_admin'],
            'data_account_adsense': data_account_adsense.get('data', [])
        }
        return render(req, 'admin/adsense_manager/summary/index.html', data)

class AdsenseSummaryDataView(View):
    """AJAX endpoint untuk data Summary AdSense (dibuka untuk preview)."""
    def dispatch(self, request, *args, **kwargs):
        # Izinkan akses untuk preview/API; otentikasi akan ditangani di dalam get()
        return super().dispatch(request, *args, **kwargs)

    def get(self, request):
        try:
            print(f"[DEBUG] AdsenseSummaryDataView called with params: {request.GET}")
            
            # Ambil user_mail dari app_credentials yang aktif
            db = data_mysql()
            sql = """
                SELECT user_mail 
                FROM app_credentials 
                WHERE is_active = '1' 
                ORDER BY mdd DESC 
                LIMIT 1
            """
            db.cur_hris.execute(sql)
            credential_data = db.cur_hris.fetchone()
            
            print(f"[DEBUG] Credential data found: {credential_data}")
            
            # Gunakan account_filter jika tersedia, fallback ke kredensial aktif
            account_filter = request.GET.get('account_filter')
            if not credential_data and not account_filter:
                print("[DEBUG] No active credentials found and no account_filter provided")
                return JsonResponse({'status': False, 'error': 'Tidak ada kredensial aktif yang ditemukan'})
            
            user_mail = account_filter if account_filter else credential_data['user_mail']
            print(f"[DEBUG] Using user_mail: {user_mail} (from {'account_filter' if account_filter else 'active credential'})")

            # Ambil parameter
            start_date = request.GET.get('start_date')
            end_date = request.GET.get('end_date')
            site_filter = request.GET.get('selected_sites') or '%'
            
            print(f"[DEBUG] Parameters - start_date: {start_date}, end_date: {end_date}, site_filter: {site_filter}")

            if not start_date or not end_date:
                print("[DEBUG] Missing start_date or end_date")
                return JsonResponse({'status': False, 'error': 'Start date dan end date wajib diisi'})

            # Ambil data summary
            print(f"[DEBUG] Calling fetch_adsense_summary_data with user_mail: {user_mail}")
            result = fetch_adsense_summary_data(user_mail, start_date, end_date, site_filter)
            
            print(f"[DEBUG] fetch_adsense_summary_data result: {result}")
            
            if not result.get('status'):
                print(f"[DEBUG] fetch_adsense_summary_data failed: {result.get('error')}")
                return JsonResponse({'status': False, 'error': result.get('error', 'Gagal mengambil data summary')})

            response_data = {
                'status': True,
                'summary': {
                    'total_impressions': result['data'].get('total_impressions', 0),
                    'total_clicks': result['data'].get('total_clicks', 0),
                    'total_revenue': result['data'].get('total_revenue', 0),
                    'ctr': result['data'].get('avg_ctr', 0),
                    'ecpm': result['data'].get('avg_ecpm', 0),
                    'cpc': result['data'].get('avg_cpc', 0)
                },
                'daily': result['data'].get('daily', []),
                'currency': result['data'].get('currency', 'USD')
            }
            
            print(f"[DEBUG] Returning response: {response_data}")
            return JsonResponse(response_data)

        except Exception as e:
            print(f"[DEBUG] Exception in AdsenseSummaryDataView: {str(e)}")
            import traceback
            traceback.print_exc()
            return JsonResponse({'status': False, 'error': str(e)})

class AdsenseTrafficPerCountryView(View):
    """View untuk halaman AdSense Traffic Per Country"""
    def dispatch(self, request, *args, **kwargs):
        if 'hris_admin' not in request.session:
            return redirect('admin_login')
        return super().dispatch(request, *args, **kwargs)

    def get(self, req):
        admin = req.session.get('hris_admin', {})
        if admin.get('super_st') == '0':
            data_account_adsense = data_mysql().get_all_adsense_account_data_user(admin.get('user_id'))
        else:
            data_account_adsense = data_mysql().get_all_adsense_account_data()
        if not data_account_adsense['status']:
            return JsonResponse({
                'status': False,
                'error': data_account_adsense['data']
            })
        last_update = data_mysql().get_last_update_adsense_traffic_country()['data']['last_update']
        data = {
            'title': 'AdSense Traffic Per Country',
            'user': req.session['hris_admin'],
            'data_account_adsense': data_account_adsense['data'],
            'last_update': last_update,
        }
        return render(req, 'admin/adsense_manager/traffic_country/index.html', data)

@method_decorator(csrf_exempt, name='dispatch')
class AdsenseTrafficPerCountryDataView(View):
    """AJAX endpoint untuk data AdSense Traffic Per Country"""
    def dispatch(self, request, *args, **kwargs):
        if 'hris_admin' not in request.session:
            return redirect('admin_login')
        return super().dispatch(request, *args, **kwargs)
    def get(self, request):
        try:
            # Get form data (same as Traffic Account)
            start_date = request.GET.get('start_date')
            end_date = request.GET.get('end_date')
            if not start_date or not end_date:
                return JsonResponse({
                    'error': 'Start date dan end date wajib diisi'
                }, status=400)
            selected_account = request.GET.get('selected_account')
            selected_account_list = []
            if selected_account:
                selected_account_list = [str(s).strip() for s in selected_account.split(',') if s.strip()]
            print(f"[DEBUG] selected_account_list: {selected_account_list}")
            country_filter = request.GET.get('selected_countries', '')
            # Parse countries list
            countries_list = []
            if country_filter and country_filter.strip():
                countries_list = [c.strip() for c in country_filter.split(',') if c.strip()]
            # Fetch AdSense traffic data per country
            result = data_mysql().get_all_adsense_traffic_country_by_params(start_date, end_date, selected_account_list, countries_list)
            print(f"[DEBUG] get_all_adsense_traffic_country_by_params result: {result}")
            if isinstance(result, dict):
                if 'data' in result:
                    if result['data']:
                        print(f"[DEBUG] First data item: {result['data'][0]}")
                if 'summary' in result:
                    print(f"[DEBUG] Summary: {result['summary']}")
            return JsonResponse(result, safe=False)
        except Exception as e:
            print(f"[ERROR] Exception in AdsenseTrafficPerCountryDataView: {str(e)}")
            import traceback
            traceback.print_exc()
            return JsonResponse({
                'error': f'Server error: {str(e)}'
            }, status=500)

class AdsenseAccountView(View):
    """View untuk halaman AdSense Account"""
    def dispatch(self, request, *args, **kwargs):
        # Untuk preview, izinkan akses; produksi sebaiknya cek session
        return super().dispatch(request, *args, **kwargs)

    def get(self, request):
        return render(request, 'admin/adsense_manager/account/index.html')

class AdsenseAccountDataView(View):
    """AJAX endpoint untuk data AdSense Account dan unit iklan berbasis data real."""
    def get(self, request):
        try:
            # Ambil kredensial yang dipilih (user_mail). Jika tidak ada, gunakan kredensial aktif terbaru
            selected_user_mail = request.GET.get('user_mail')
            if not selected_user_mail:
                db = data_mysql()
                sql = """
                    SELECT user_mail
                    FROM app_credentials
                    WHERE is_active = '1'
                    ORDER BY mdd DESC
                    LIMIT 1
                """
                db.cur_hris.execute(sql)
                row = db.cur_hris.fetchone()
                if not row:
                    return JsonResponse({'status': False, 'error': 'Tidak ada kredensial aktif ditemukan'})
                selected_user_mail = row['user_mail']

            # Ambil data akun dan unit iklan dari AdSense API menggunakan utilitas
            result = fetch_adsense_account_info_and_units(selected_user_mail)
            if not result.get('status'):
                return JsonResponse({'status': False, 'error': result.get('error', 'Gagal mengambil data AdSense')})

            # Kembalikan data real
            return JsonResponse({
                'status': True,
                'data': result.get('accounts', []),
                'ad_units': result.get('ad_units', []),
                'user_mail': selected_user_mail
            })
        except Exception as e:
            return JsonResponse({'status': False, 'error': str(e)})

class AdsenseCredentialsListView(View):
    """AJAX endpoint: daftar kredensial AdSense aktif untuk dipilih di UI."""
    def get(self, request):
        try:
            db = data_mysql()
            sql = """
                SELECT account_name, user_mail 
                FROM app_credentials 
                WHERE is_active = '1'
                ORDER BY account_name ASC
            """
            db.cur_hris.execute(sql)
            rows = db.cur_hris.fetchall() or []
            data = [{'user_mail': r['user_mail'], 'account_name': r['account_name']} for r in rows]
            return JsonResponse({'status': True, 'data': data})
        except Exception as e:
            return JsonResponse({'status': False, 'error': str(e)})

class RekapAdsenseSummaryView(View):
    """View untuk Rekap Adsense Summary - menampilkan ringkasan data Adsense"""
    def dispatch(self, request, *args, **kwargs):
        if 'hris_admin' not in request.session:
            return redirect('admin_login')
        return super().dispatch(request, *args, **kwargs)
    def get(self, req):
        admin = req.session.get('hris_admin', {})
        data_account = data_mysql().master_account_ads()['data']
        last_update = data_mysql().get_last_update_adsense_traffic_per_domain()['data']['last_update']
        if admin.get('super_st') == '0':
            data_account_adsense = data_mysql().get_all_adsense_account_data_user(admin.get('user_id'))
        else:
            data_account_adsense = data_mysql().get_all_adsense_account_data()
        data = {
            'title': 'Rekap Adsense Summary Dashboard',
            'user': req.session['hris_admin'],
            'data_account': data_account,
            'data_account_adsense': data_account_adsense['data'],
            'last_update': last_update
        }
        return render(req, 'admin/report_adsense/all_rekap/index.html', data)

class RekapTrafficPerDomainView(View):
    """View untuk Rekap Traffic Per Domain"""
    def dispatch(self, request, *args, **kwargs):
        if 'hris_admin' not in request.session:
            return redirect('admin_login')
        return super().dispatch(request, *args, **kwargs)
    def get(self, req):
        admin = req.session.get('hris_admin', {})
        if admin.get('super_st') == '0':
            data_account_adsense = data_mysql().get_all_adsense_account_data_user(admin.get('user_id'))['data']
            data_domain_adsense = data_mysql().get_all_adsense_domain_data_user(admin.get('user_id'))['data']
        else:
            data_account_adsense = data_mysql().get_all_adsense_account_data()['data']
            data_domain_adsense = data_mysql().get_all_adsense_domain_data()['data']
        data_account = data_mysql().master_account_ads()['data']
        last_update = data_mysql().get_last_update_adsense_traffic_per_domain()['data']['last_update']
        data = {
            'title': 'ROI Per Domain',
            'user': req.session['hris_admin'],
            'data_account_adsense': data_account_adsense,
            'data_domain_adsense': data_domain_adsense,
            'data_account': data_account,
            'last_update': last_update
        }
        return render(req, 'admin/report_adsense/per_domain/index.html', data)

class RekapTrafficPerDomainDataView(View):
    """AJAX endpoint untuk data ROI Traffic Per Domain"""
    def dispatch(self, request, *args, **kwargs):
        if 'hris_admin' not in request.session:
            return redirect('admin_login')
        return super().dispatch(request, *args, **kwargs)
    def get(self, req):
        try:
            start_date = req.GET.get('start_date')
            end_date = req.GET.get('end_date')
            selected_accounts = req.GET.get('selected_account_adsense')
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
            # --- 3. Ambil data AdX
            adsense_result = data_mysql().get_all_rekap_adsense_traffic_account_by_params(
                start_date_formatted,
                end_date_formatted,
                selected_account_list,
                selected_domain_list
            )
            print(f"[DEBUG ROI] Adsense result: {adsense_result}")
            # --- 4. Proses Facebook data
            facebook_data = None
            unique_name_site = []
            if selected_domain_list:
                for site in selected_domain_list:
                    site = str(site).strip()
                    if "." in site:
                        parts = site.split(".")       # pisah berdasarkan titik
                        if len(parts) >= 1:
                            main_domain = ".".join(parts[:1])
                        else:
                            main_domain = site
                        unique_name_site.append(main_domain)
            elif adsense_result:
                # Ambil unique site dari AdX
                extracted_sites = set()
                for adsense_item in adsense_result['hasil']['data']:
                    site_name = str(adsense_item.get('site_name', '')).strip()
                    if site_name and site_name != 'Unknown':
                        extracted_sites.add(site_name)
                for site in extracted_sites:
                    if "." in site:
                        parts = site.split(".")       # pisah berdasarkan titik
                        main_domain = ".".join(parts[:1])
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
            total_impressions_adsense = 0.0
            total_clicks_adsense = 0.0
            total_cpr = 0.0
            total_ctr_fb = 0.0
            total_cpc_fb = 0.0
            total_cpc_adsense = 0.0
            total_ctr_adsense = 0.0
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
            if adsense_result and adsense_result['hasil']['data']:
                 # --- NEW: siapkan raw_rows + dua grup agregasi (per date+domain)
                grouped_all = {}
                grouped_filtered = {}
                seen_fb_keys = set()
                for adsense_item in adsense_result['hasil']['data']:
                    date_key = str(adsense_item.get('date', ''))
                    subdomain = str(adsense_item.get('site_name', ''))
                    impressions_adsense = float(adsense_item.get('impressions_adsense', 0))
                    base_subdomain = extract_base_subdomain(subdomain)
                    country_code = normalize_country_code(adsense_item.get('country_code', ''))
                    fb_key = f"{date_key}_{base_subdomain}_{country_code}"
                    seen_fb_keys.add(fb_key)
                    fb_data = facebook_map.get(fb_key)
                    spend = float(fb_data.get('spend') or 0) if fb_data else 0
                    impressions_fb = float(fb_data.get('impressions_fb') or 0) if fb_data else 0
                    clicks_fb = float(fb_data.get('clicks_fb') or 0) if fb_data else 0
                    clicks_adsense = float(adsense_item.get('clicks_adsense') or 0)
                    cpr = float(fb_data.get('cpr') or 0) if fb_data else 0
                    cpc = float(fb_data.get('cpc_fb') or 0) if fb_data else 0
                    revenue = float(adsense_item.get('revenue', 0))
                    ctr_fb = ((clicks_fb / impressions_fb) * 100) if impressions_fb > 0 else 0
                    cpc_fb = cpc
                    ctr_adsense = ((clicks_adsense / impressions_adsense) * 100) if impressions_adsense > 0 else 0
                    cpc_adsense = (revenue / clicks_adsense) if clicks_adsense > 0 else 0
                    cpm = float(adsense_item.get('ecpm', 0))
                    raw_rows_all.append({
                        'site_name': base_subdomain or subdomain,
                        'date': date_key,
                        'country_code': country_code,
                        'spend': spend,
                        'impressions_fb': impressions_fb,
                        'clicks_fb': clicks_fb,
                        'impressions_adsense': impressions_adsense,
                        'clicks_adsense': clicks_adsense,
                        'cpr': cpr,
                        'ctr_fb': ctr_fb,
                        'cpc_fb': cpc_fb,
                        'ctr_adsense': ctr_adsense,
                        'cpc_adsense': cpc_adsense,
                        'cpm': cpm,
                        'revenue': revenue
                    })
                    print(f"[DEBUG ROI] raw_rows_all: {raw_rows_all[-1]}")
                    key = f"{date_key}|{base_subdomain or subdomain}"
                    entry = grouped_all.get(key) or {
                        'site_name': base_subdomain or subdomain,
                        'date': date_key,
                        'spend': 0.0,
                        'revenue': 0.0,
                        'impressions_fb': 0.0,
                        'impressions_adsense': 0.0,
                        'clicks_fb': 0.0,
                        'clicks_adsense': 0.0,
                        # avg fields
                        'cpr_sum': 0.0,
                        'cpr_cnt': 0,
                        'cpr': 0.0,
                        'ctr_fb': 0.0,
                        'cpc_fb': 0.0,
                        'ctr_adsense': 0.0,
                        'cpc_adsense': 0.0,
                        'cpm_sum': 0.0,
                        'cpm_cnt': 0,
                        'cpm': 0.0,
                    }
                    entry['spend'] += spend
                    entry['impressions_fb'] += impressions_fb
                    entry['clicks_fb'] += clicks_fb
                    entry['impressions_adsense'] += impressions_adsense
                    entry['clicks_adsense'] += clicks_adsense
                    entry['revenue'] += revenue

                    # CPR avg per (date|domain)
                    entry['cpr_sum'] += cpr
                    entry['cpr_cnt'] += 1
                    entry['cpr'] = (entry['cpr_sum'] / entry['cpr_cnt']) if entry['cpr_cnt'] > 0 else 0

                    entry['ctr_fb'] = ((entry['clicks_fb'] / entry['impressions_fb']) * 100) if entry['impressions_fb'] > 0 else 0
                    entry['cpc_fb'] = (entry['revenue'] / entry['clicks_fb']) if entry['clicks_fb'] > 0 else 0
                    entry['ctr_adsense'] = ((entry['clicks_adsense'] / entry['impressions_adsense']) * 100) if entry['impressions_adsense'] > 0 else 0
                    entry['cpc_adsense'] = (entry['revenue'] / entry['clicks_adsense']) if entry['clicks_adsense'] > 0 else 0

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
                            'impressions_adsense': 0.0,
                            'clicks_fb': 0.0,
                            'clicks_adsense': 0.0,
                            # avg fields
                            'cpr_sum': 0.0,
                            'cpr_cnt': 0,
                            'cpr': 0.0,
                            'ctr_fb': 0.0,
                            'cpc_fb': 0.0,
                            'ctr_adsense': 0.0,
                            'cpc_adsense': 0.0,
                            'cpm_sum': 0.0,
                            'cpm_cnt': 0,
                            'cpm': 0.0,
                        }
                        f_entry['spend'] += spend
                        f_entry['impressions_fb'] += impressions_fb
                        f_entry['clicks_fb'] += clicks_fb
                        f_entry['impressions_adsense'] += impressions_adsense
                        f_entry['clicks_adsense'] += clicks_adsense
                        f_entry['revenue'] += revenue

                        # CPR avg per (date|domain)
                        f_entry['cpr_sum'] += cpr
                        f_entry['cpr_cnt'] += 1
                        f_entry['cpr'] = (f_entry['cpr_sum'] / f_entry['cpr_cnt']) if f_entry['cpr_cnt'] > 0 else 0

                        f_entry['ctr_fb'] = ((f_entry['clicks_fb'] / f_entry['impressions_fb']) * 100) if f_entry['impressions_fb'] > 0 else 0
                        f_entry['cpc_fb'] = (f_entry['revenue'] / f_entry['clicks_fb']) if f_entry['clicks_fb'] > 0 else 0
                        f_entry['ctr_adsense'] = ((f_entry['clicks_adsense'] / f_entry['impressions_adsense']) * 100) if f_entry['impressions_adsense'] > 0 else 0
                        f_entry['cpc_adsense'] = (f_entry['revenue'] / f_entry['clicks_adsense']) if f_entry['clicks_adsense'] > 0 else 0

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
                    base_subdomain = extract_base_subdomain_fb_adsense(subdomain)
                    print(f"[DEBUG ROI] Base subdomain fb_adsense: {base_subdomain}")
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
                        'impressions_adsense': 0.0,
                        'clicks_adsense': 0.0,
                        'cpr': cpr,
                        'ctr_fb': ((clicks_fb / impressions_fb) * 100) if impressions_fb > 0 else 0,
                        'cpc_fb': cpc_fb,
                        'ctr_adsense': 0.0,
                        'cpc_adsense': 0.0,
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
                        'impressions_adsense': 0.0,
                        'clicks_fb': 0.0,
                        'clicks_adsense': 0.0,
                        'cpr_sum': 0.0,
                        'cpr_cnt': 0,
                        'cpr': 0.0,
                        'ctr_fb': 0.0,
                        'cpc_fb': 0.0,
                        'ctr_adsense': 0.0,
                        'cpc_adsense': 0.0,
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
                            'impressions_adsense': 0.0,
                            'clicks_fb': 0.0,
                            'clicks_adsense': 0.0,
                            'cpr_sum': 0.0,
                            'cpr_cnt': 0,
                            'cpr': 0.0,
                            'ctr_fb': 0.0,
                            'cpc_fb': 0.0,
                            'ctr_adsense': 0.0,
                            'cpc_adsense': 0.0,
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
                total_impressions_adsense = 0.0
                total_clicks_adsense = 0.0
                total_cpr = 0.0
                total_ctr_fb = 0.0
                total_cpc_fb = 0.0
                total_ctr_adsense = 0.0
                total_cpc_adsense = 0.0
                total_cpm = 0.0
                total_revenue = 0.0
                for _, item in sorted(grouped_all.items(), key=lambda kv: (kv[1]['date'], kv[1]['site_name'])):
                    spend_val = item['spend']
                    impressions_fb_val = item['impressions_fb']
                    impressions_adsense_val = item['impressions_adsense']
                    clicks_fb_val = item['clicks_fb']
                    clicks_adsense_val = item['clicks_adsense']
                    cpr_val = item['cpr']
                    ctr_fb_val = item['ctr_fb']
                    cpc_fb_val = item['cpc_fb']
                    ctr_adsense_val = item['ctr_adsense']
                    cpc_adsense_val = item['cpc_adsense']
                    cpm_val = item['cpm']
                    revenue_val = item['revenue']
                    roi = ((revenue_val - spend_val) / spend_val * 100) if spend_val > 0 else 0
                    combined_data_all.append({
                        'site_name': item['site_name'] + '.com',
                        'date': item['date'],
                        'spend': spend_val,
                        'impressions_fb': impressions_fb_val,
                        'clicks_fb': clicks_fb_val,
                        'impressions_adsense': impressions_adsense_val,
                        'clicks_adsense': clicks_adsense_val,
                        'cpr': cpr_val,
                        'ctr_fb': ctr_fb_val,
                        'cpc_fb': cpc_fb_val,
                        'ctr_adsense': ctr_adsense_val,
                        'cpc_adsense': cpc_adsense_val,
                        'cpm': cpm_val,
                        'revenue': revenue_val,
                        'roi': roi
                    })
                    total_spend += spend_val
                    total_impressions_fb += impressions_fb_val
                    total_clicks_fb += clicks_fb_val
                    total_impressions_adsense += impressions_adsense_val
                    total_clicks_adsense += clicks_adsense_val
                    total_cpr += cpr_val
                    total_ctr_fb += ctr_fb_val
                    total_cpc_fb += cpc_fb_val
                    total_ctr_adsense += ctr_adsense_val
                    total_cpc_adsense += cpc_adsense_val
                    total_cpm += cpm_val
                    total_revenue += revenue_val

                combined_data_filtered = []
                for _, item in sorted(grouped_filtered.items(), key=lambda kv: (kv[1]['date'], kv[1]['site_name'])):
                    spend_val = item['spend']
                    impressions_fb_val = item['impressions_fb']
                    impressions_adsense_val = item['impressions_adsense']
                    clicks_fb_val = item['clicks_fb']
                    clicks_adsense_val = item['clicks_adsense']
                    cpr_val = item['cpr']
                    ctr_fb_val = item['ctr_fb']
                    cpc_fb_val = item['cpc_fb']
                    ctr_adsense_val = item['ctr_adsense']
                    cpc_adsense_val = item['cpc_adsense']
                    cpm_val = item['cpm']
                    revenue_val = item['revenue']
                    roi = ((revenue_val - spend_val) / spend_val * 100) if spend_val > 0 else 0
                    combined_data_filtered.append({
                        'site_name': item['site_name'] + '.com',
                        'date': item['date'],
                        'spend': spend_val,
                        'impressions_fb': impressions_fb_val,
                        'clicks_fb': clicks_fb_val,
                        'impressions_adsense': impressions_adsense_val,
                        'clicks_adsense': clicks_adsense_val,
                        'cpr': cpr_val,
                        'ctr_fb': ctr_fb_val,
                        'cpc_fb': cpc_fb_val,
                        'ctr_adsense': ctr_adsense_val,
                        'cpc_adsense': cpc_adsense_val,
                        'cpm': cpm_val,
                        'revenue': revenue_val,
                        'roi': roi
                    })
            roi_nett_summary = ((total_revenue - total_spend) / total_spend * 100) if total_spend > 0 else 0
            result = {
                'status': True,
                'data': combined_data_all,              # semua kontribusi
                'data_filtered': combined_data_filtered, # hanya spend > 0
                'raw_rows': raw_rows_all,                # baris mentah (opsional, untuk analisis)
                'summary': {
                    'total_clicks_fb': total_clicks_fb,
                    'total_clicks_adsense': total_clicks_adsense,
                    'total_spend': total_spend,
                    'roi_nett': roi_nett_summary,
                    'total_revenue': total_revenue
                }
            }
            return JsonResponse(result, safe=False)
        except Exception as e:
            return JsonResponse({'status': False, 'error': str(e)})

def extract_base_subdomain_fb_adsense(full_string):
    parts = full_string.split('.')
    if len(parts) >= 2:
        return ".".join(parts[-2:])
    return full_string

# ===== Rekap Adsense Per Country =====
class RekapTrafficPerCountryAdsenseView(View):
    def dispatch(self, request, *args, **kwargs):
        if 'hris_admin' not in request.session:
            return redirect('admin_login')
        return super().dispatch(request, *args, **kwargs)
    def get(self, req):
        admin = req.session.get('hris_admin', {})
        if admin.get('super_st') == '0':
            data_account_adsense = data_mysql().get_all_adsense_account_data_user(admin.get('user_id'))
            data_domain_adsense = data_mysql().get_all_adsense_domain_data_user(admin.get('user_id'))
        else:
            data_account_adsense = data_mysql().get_all_adsense_account_data()
            data_domain_adsense = data_mysql().get_all_adsense_domain_data()
        if not data_account_adsense['status']:
            return JsonResponse({
                'status': False,
                'error': data_account_adsense['data']
            })
        data_account = data_mysql().master_account_ads()['data']
        last_update = data_mysql().get_last_update_adsense_traffic_country()['data']['last_update']
        data = {
            'title': 'ROI Per Country',
            'user': req.session['hris_admin'],
            'last_update': last_update,
            'data_account': data_account,
            'data_account_adsense': data_account_adsense['data'],
            'data_domain_adsense': data_domain_adsense['data'],
        }
        return render(req, 'admin/report_adsense/per_country/index.html', data)

class RekapTrafficPerCountryAdsenseDataView(View):
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
        selected_account = req.GET.get('selected_account_adsense')
        selected_account_list = []
        if selected_account:
            selected_account_list = [str(s).strip() for s in selected_account.split(',') if s.strip()]
        selected_domain = req.GET.get('selected_domains')
        selected_domain_list = []
        if selected_domain:
            selected_domain_list = [str(s).strip() for s in selected_domain.split(',') if s.strip()]
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
                    sites_result = data_mysql().fetch_user_sites_adsense_id_list(
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
            response_cache_key = generate_cache_key_adsense(
                'roi_traffic_country_response_v2',
                start_date,
                end_date,
                selected_account_list,
                selected_domain_list,
                selected_account_ads or '',
                ','.join(countries_list_query) if countries_list_query else ''
            )
            cached_response = get_cached_data_adsense(response_cache_key)
            if cached_response is not None:
                return JsonResponse(cached_response, safe=False)
            data_facebook = None
            # Jalankan paralel jika selected_domain sudah ada (menghindari fetch FB yang terlalu lebar)
            if selected_domain_list:
                with ThreadPoolExecutor(max_workers=2) as executor:
                    adsense_future = executor.submit(
                        data_mysql().get_all_adsense_rekap_country_detail_by_params,
                        start_date,
                        end_date,
                        selected_account_list,
                        selected_domain_list,
                        countries_list_query
                    )
                    # Pastikan selected_sites adalah list
                    if isinstance(selected_domain_list, str):
                        selected_domain_list = [s.strip() for s in selected_domain_list.split(",") if s.strip()]
                    if selected_domain_list:
                        unique_sites = set()
                        for site_item in selected_domain_list:
                            site_name = site_item.strip()
                            if site_name and site_name != 'Unknown':
                                unique_sites.add(site_name)
                        extracted_names = []
                        for site in unique_sites:
                            if "." not in site:
                                continue
                            parts = site.split(".")       # pisah berdasarkan titik
                            if len(parts) >= 1:
                                main_domain = ".".join(parts[:1])
                            else:
                                main_domain = site
                            extracted_names.append(main_domain)
                        unique_name_site = list(set(extracted_names))
                    fb_future = executor.submit(
                        data_mysql().get_all_ads_roi_country_detail_adsense_by_params,
                        start_date,
                        end_date,
                        unique_name_site,
                        countries_list_query
                    )
                    print(f"[DEBUG ROI] Facebook future: {fb_future}")
                    data_adsense = adsense_future.result()
                    try:
                        # Hapus timeout: tunggu hingga FB selesai agar data lengkap
                        data_facebook = fb_future.result()
                    except Exception as e:
                        data_facebook = None
            else:
                # Filter Domain kosong: tampilkan data semua domain dari akun AdSense terpilih
                data_adsense = data_mysql().get_all_adsense_rekap_country_detail_by_params(
                    start_date, 
                    end_date, 
                    selected_account_list, 
                    selected_domain_list, 
                    countries_list_query
                )
                try:
                    unique_name_site = []
                    with ThreadPoolExecutor(max_workers=2) as executor:
                        if sites_for_fb:
                            unique_sites = set(site.strip() for site in sites_for_fb if site.strip() and site.strip() != 'Unknown')
                            extracted_names = []
                            for site in unique_sites:
                                main_domain = extract_base_subdomain(site.strip())
                                if main_domain and main_domain != 'Unknown':
                                    extracted_names.append(main_domain)
                            unique_name_site = list(set(extracted_names))
                        if not unique_name_site:
                            adsense_payload_tmp = data_adsense.get('hasil') if isinstance(data_adsense, dict) and data_adsense.get('hasil') else None
                            adsense_items_tmp = adsense_payload_tmp.get('data') if isinstance(adsense_payload_tmp, dict) else []
                            if adsense_items_tmp:
                                extracted_names = []
                                for adsense_item in (adsense_items_tmp or []):
                                    site_name = str(adsense_item.get('site_name', '') or '')
                                    main_domain = extract_base_subdomain(site_name.strip())
                                    if main_domain and main_domain != 'Unknown':
                                        extracted_names.append(main_domain)
                                unique_name_site = list(set(extracted_names))
                        print(f"[DEBUG ROI] Unique name site (from AdSense): {unique_name_site}")
                        if unique_name_site:
                            fb_future = executor.submit(
                                data_mysql().get_all_ads_roi_country_detail_adsense_by_params,
                                start_date, 
                                end_date, 
                                unique_name_site, 
                                countries_list_query
                            )
                            data_facebook = fb_future.result()
                        else:
                            data_facebook = None
                except Exception as e:
                    print(f"[DEBUG] Facebook fetch (all domains) failed: {e}; continue without FB data")
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
            # Pastikan bentuk payload sesuai: gunakan 'hasil' untuk AdSense dan FB jika tersedia
            adsense_payload = data_adsense.get('hasil') if isinstance(data_adsense, dict) and data_adsense.get('hasil') else data_adsense
            fb_payload = (data_facebook.get('hasil') if isinstance(data_facebook, dict) and data_facebook.get('hasil') else {'status': True, 'data': []})
            print(f"[DEBUG ROI] Facebook payload: {fb_payload}")
            result = process_roi_traffic_country_data(adsense_payload, fb_payload)
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
                            print(f"[DEBUG ROI] ✓ MATCH FOUND: '{country_name}' matches '{filter_country}'")
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
                set_cached_data_adsense(response_cache_key, result, timeout=900)
            except Exception as _cache_err:
                print(f"[DEBUG] Failed to cache ROI Country final response: {_cache_err}")
            return JsonResponse(result, safe=False)
        except Exception as e:
            return JsonResponse({
                'status': False,
                'error': str(e)
            })

def process_roi_traffic_country_data(data_adsense, data_facebook):
    """Fungsi untuk menggabungkan data AdSense dan Facebook berdasarkan kode negara dan menghitung ROI"""
    try:
        # Inisialisasi hasil
        adsense_map = {}
        fb_map = {}
        country_name_by_code = {}
        def normalize_country_code(cc):
            c = (str(cc or '').strip().upper())
            if not c:
                return ''
            if c == 'TU':
                return 'TR'
            return c
        # Normalisasi AdSense: date + base_subdomain + country_code
        adsense_items = data_adsense.get('data') if isinstance(data_adsense, dict) else []
        for adsense_item in (adsense_items or []):
            date_key = str(adsense_item.get('date', '') or '')
            site_name = str(adsense_item.get('site_name', '') or '')
            base_subdomain = site_name
            country_code = normalize_country_code(adsense_item.get('country_code', '') or '')
            country_name = adsense_item.get('country_name', '') or ''
            impressions_adsense = int(adsense_item.get('impressions', 0) or 0)
            clicks_adsense = int(adsense_item.get('clicks', 0) or 0)
            revenue = float(adsense_item.get('revenue', 0) or 0)
            # Pastikan date_key, base_subdomain, dan country_code tersedia agar agregasi sesuai pasangan site/tanggal/negara
            if not date_key or not base_subdomain or not country_code:
                continue
            country_name_by_code[country_code] = country_name or country_name_by_code.get(country_code, '')
            var_key = f"{date_key}_{base_subdomain}_{country_code}"
            entry = adsense_map.get(var_key) or {'revenue': 0.0, 'impressions_adsense': 0, 'clicks_adsense': 0}
            entry['revenue'] += revenue
            entry['impressions_adsense'] += impressions_adsense
            entry['clicks_adsense'] += clicks_adsense
            adsense_map[var_key] = entry
        # Normalisasi FB: date + base_subdomain + country_code
        fb_payload = data_facebook if isinstance(data_facebook, dict) else {'status': True, 'data': []}
        fb_items = fb_payload.get('data') or []
        for fb_item in fb_items:
            date_key = str(fb_item.get('date', '') or '')
            domain = str(fb_item.get('domain', '') or '')
            base_subdomain = extract_base_subdomain_fb_adsense(domain)
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
        union_keys = set(list(adsense_map.keys()) + list(fb_map.keys()))
        for key in union_keys:
            try:
                parts = key.split('_')
                if len(parts) < 3:
                    continue
                date_key = parts[0]
                country_code = parts[-1]
            except Exception:
                continue
            adsense_entry = adsense_map.get(key) or {'revenue': 0.0, 'impressions_adsense': 0, 'clicks_adsense': 0}
            fb_entry = fb_map.get(key) or {'spend': 0.0, 'impressions_fb': 0, 'clicks_fb': 0, 'cpr_sum': 0.0, 'cpr_count': 0}
            revenue = float(adsense_entry['revenue'])
            spend = float(fb_entry['spend'])
            impressions_fb = int(fb_entry['impressions_fb'])
            clicks_fb = int(fb_entry['clicks_fb'])
            impressions_adsense = int(adsense_entry['impressions_adsense'])
            clicks_adsense = int(adsense_entry['clicks_adsense'])
            name = country_name_by_code.get(country_code, '')
            if country_code not in agg_all:
                agg_all[country_code] = {'country': name, 'country_code': country_code, 'impressions_fb': 0, 'impressions_adsense': 0, 'spend': 0.0, 'clicks_fb': 0, 'clicks_adsense': 0, 'revenue': 0.0, 'cpr_sum': 0.0, 'cpr_count': 0}
            agg_all[country_code]['spend'] += spend
            agg_all[country_code]['revenue'] += revenue
            agg_all[country_code]['impressions_fb'] += impressions_fb
            agg_all[country_code]['clicks_fb'] += clicks_fb
            agg_all[country_code]['impressions_adsense'] += impressions_adsense
            agg_all[country_code]['clicks_adsense'] += clicks_adsense
            agg_all[country_code]['cpr_sum'] += fb_entry.get('cpr_sum', 0.0)
            agg_all[country_code]['cpr_count'] += fb_entry.get('cpr_count', 0)
            per_day_map = daily_all.get(country_code)
            if not per_day_map:
                daily_all[country_code] = {}
                per_day_map = daily_all[country_code]
            day_entry = per_day_map.get(date_key)
            if not day_entry:
                per_day_map[date_key] = {'spend': 0.0, 'clicks_fb': 0, 'revenue': 0.0, 'impressions_adsense': 0}
                day_entry = per_day_map[date_key]
            day_entry['spend'] += spend
            day_entry['clicks_fb'] += clicks_fb
            day_entry['revenue'] += revenue
            day_entry['impressions_adsense'] += impressions_adsense
            if spend > 0:
                if country_code not in agg_filtered:
                    agg_filtered[country_code] = {'country': name, 'country_code': country_code, 'impressions_fb': 0, 'impressions_adsense': 0, 'spend': 0.0, 'clicks_fb': 0, 'clicks_adsense': 0, 'revenue': 0.0, 'cpr_sum': 0.0, 'cpr_count': 0}
                agg_filtered[country_code]['spend'] += spend
                agg_filtered[country_code]['revenue'] += revenue
                agg_filtered[country_code]['impressions_fb'] += impressions_fb
                agg_filtered[country_code]['clicks_fb'] += clicks_fb
                agg_filtered[country_code]['impressions_adsense'] += impressions_adsense
                agg_filtered[country_code]['clicks_adsense'] += clicks_adsense
                agg_filtered[country_code]['cpr_sum'] += fb_entry.get('cpr_sum', 0.0)
                agg_filtered[country_code]['cpr_count'] += fb_entry.get('cpr_count', 0)
                per_day_map_f = daily_filtered.get(country_code)
                if not per_day_map_f:
                    daily_filtered[country_code] = {}
                    per_day_map_f = daily_filtered[country_code]
                day_entry_f = per_day_map_f.get(date_key)
                if not day_entry_f:
                    per_day_map_f[date_key] = {'spend': 0.0, 'clicks_fb': 0, 'revenue': 0.0, 'impressions_adsense': 0}
                    day_entry_f = per_day_map_f[date_key]
                day_entry_f['spend'] += spend
                day_entry_f['clicks_fb'] += clicks_fb
                day_entry_f['revenue'] += revenue
                day_entry_f['impressions_adsense'] += impressions_adsense
        combined_data_all = []
        for code, item in agg_all.items():
            s = float(item['spend'])
            r = float(item['revenue'])
            imp_fb = int(item['impressions_fb'])
            clk_fb = int(item['clicks_fb'])
            imp_adsense = int(item['impressions_adsense'])
            clk_adsense = int(item['clicks_adsense'])
            ctr_fb = ((clk_fb / imp_fb) * 100) if imp_fb > 0 else 0
            ctr_adsense = ((clk_adsense / imp_adsense) * 100) if imp_adsense > 0 else 0
            cpc_fb = (s / clk_fb) if clk_fb > 0 else 0
            cpc_adsense = (r / clk_adsense) if clk_adsense > 0 else 0
            roi = ((r - s) / s * 100) if s > 0 else 0
            cpr = (s / clk_fb) if clk_fb > 0 else 0.0
            ecpm = ((r / imp_adsense) * 1000) if imp_adsense > 0 else 0.0
            combined_data_all.append({
                'country': item['country'],
                'country_code': code,
                'impressions_fb': imp_fb,
                'impressions_adsense': imp_adsense,
                'clicks_fb': clk_fb,
                'clicks_adsense': clk_adsense,
                'cpr': round(cpr, 2),
                'cpc_fb': round(cpc_fb, 2),
                'ctr_fb': round(ctr_fb, 2),
                'cpc_adsense': round(cpc_adsense, 2),
                'ctr_adsense': round(ctr_adsense, 2),
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
            imp_adsense = int(item['impressions_adsense'])
            clk_adsense = int(item['clicks_adsense'])
            ctr_fb = ((clk_fb / imp_fb) * 100) if imp_fb > 0 else 0
            ctr_adsense = ((clk_adsense / imp_adsense) * 100) if imp_adsense > 0 else 0
            cpc_fb = (s / clk_fb) if clk_fb > 0 else 0
            cpc_adsense = (r / clk_adsense) if clk_adsense > 0 else 0
            roi = ((r - s) / s * 100) if s > 0 else 0
            cpr = (s / clk_fb) if clk_fb > 0 else 0.0
            ecpm = ((r / imp_adsense) * 1000) if imp_adsense > 0 else 0.0
            combined_data_filtered.append({
                'country': item['country'],
                'country_code': code,
                'impressions_fb': imp_fb,
                'impressions_adsense': imp_adsense,
                'clicks_fb': clk_fb,
                'clicks_adsense': clk_adsense,
                'cpr': round(cpr, 2),
                'cpc_fb': round(cpc_fb, 2),
                'ctr_fb': round(ctr_fb, 2),
                'cpc_adsense': round(cpc_adsense, 2),
                'ctr_adsense': round(ctr_adsense, 2),
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
                'total_clicks_adsense': sum(d['clicks_adsense'] for d in combined_data_all),
                'total_impressions_adsense': sum(d['impressions_adsense'] for d in combined_data_all),
                'total_ctr_fb': sum(d['ctr_fb'] for d in combined_data_all),
                'total_ctr_adsense': sum(d['ctr_adsense'] for d in combined_data_all),
                'rata_cpr': rata_cpr_all,
                'total_revenue': round(total_revenue_all, 2),
                'total_roi': round(((total_revenue_all - total_spend_all) / total_spend_all * 100) if total_spend_all > 0 else 0, 2)
            },
            'summary_filtered': {
                'total_spend': round(total_spend_filtered, 2),
                'total_clicks_fb': sum(d['clicks_fb'] for d in combined_data_filtered),
                'total_impressions_fb': sum(d['impressions_fb'] for d in combined_data_filtered),
                'total_clicks_adsense': sum(d['clicks_adsense'] for d in combined_data_filtered),
                'total_impressions_adsense': sum(d['impressions_adsense'] for d in combined_data_filtered),
                'total_ctr_fb': sum(d['ctr_fb'] for d in combined_data_filtered),
                'total_ctr_adsense': sum(d['ctr_adsense'] for d in combined_data_filtered),
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

# ===== ROI Monitoring Domain =====
class RoiMonitoringDomainAdsenseView(View):
    """View untuk ROI Summary - menampilkan ringkasan data ROI AdSense"""
    def dispatch(self, request, *args, **kwargs):
        if 'hris_admin' not in request.session:
            return redirect('admin_login')
        return super().dispatch(request, *args, **kwargs)
    def get(self, req):
        admin = req.session.get('hris_admin', {})
        data_account = data_mysql().master_account_ads()['data']
        last_update = data_mysql().get_last_update_adsense_traffic_per_domain()['data']['last_update']
        if admin.get('super_st') == '0':
            data_account_adsense = data_mysql().get_all_adsense_account_data_user(admin.get('user_id'))
            data_domain_adsense = data_mysql().get_all_adsense_domain_data_user(admin.get('user_id'))
        else:
            data_account_adsense = data_mysql().get_all_adsense_account_data()
            data_domain_adsense = data_mysql().get_all_adsense_domain_data()
        data = {
            'title': 'Rekap Monitoring Domain AdSense',
            'user': req.session['hris_admin'],
            'data_account': data_account,
            'data_account_adsense': data_account_adsense['data'],
            'data_domain_adsense': data_domain_adsense['data'],
            'last_update': last_update
        }
        return render(req, 'admin/report_adsense/monitoring_domain/index.html', data)

class RoiMonitoringDomainAdsenseDataView(View):
    """AJAX endpoint untuk data Adsense Traffic Per Domain"""
    def dispatch(self, request, *args, **kwargs):
        if 'hris_admin' not in request.session:
            return redirect('admin_login')
        return super().dispatch(request, *args, **kwargs)
    def get(self, req):
        try:
            start_date = req.GET.get('start_date')
            end_date = req.GET.get('end_date')
            selected_accounts = req.GET.get('selected_account_adsense')
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
            # --- 3. Ambil data AdSense
            adsense_result = data_mysql().get_all_adsense_monitoring_account_by_params(
                start_date_formatted,
                end_date_formatted,
                selected_account_list,
                selected_domain_list
            )
            # --- 4. Proses Facebook data
            facebook_data = None
            unique_name_site = []
            if selected_domain_list:
                for site in selected_domain_list:
                    site = str(site).strip()
                    if "." in site:
                        parts = site.split(".")       # pisah berdasarkan titik
                        if len(parts) >= 1:
                            main_domain = ".".join(parts[:1])
                        else:
                            main_domain = site
                    unique_name_site.append(main_domain)
            elif adsense_result:
                # Ambil unique site dari AdSense
                extracted_sites = set()     
                for adsense_item in adsense_result['hasil']['data']:
                    site_name = str(adsense_item.get('site_name', '')).strip()
                    if site_name and site_name != 'Unknown':
                        extracted_sites.add(site_name)
                for site in extracted_sites:
                    if "." in site:
                        parts = site.split(".")       # pisah berdasarkan titik
                        if len(parts) >= 1:
                            main_domain = ".".join(parts[:1])
                        else:
                            main_domain = site
                    unique_name_site.append(main_domain)
            unique_name_site = list(set(unique_name_site))
            if unique_name_site:
                facebook_data = data_mysql().get_all_adsense_roi_monitoring_campaign_by_params(
                    start_date_formatted,
                    end_date_formatted,
                    unique_name_site
                )
            # --- 5. Gabungkan data AdX dan Facebook
            # Siapkan struktur data
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
            if adsense_result and adsense_result['hasil']['data']:
                # --- NEW: siapkan raw_rows + dua grup agregasi
                grouped_all = {}
                grouped_filtered = {}
                seen_fb_keys = set()
                for adsense_item in adsense_result['hasil']['data']:
                    date_key = str(adsense_item.get('date', ''))
                    subdomain = str(adsense_item.get('site_name', ''))
                    base_subdomain = extract_base_subdomain(subdomain)
                    site_key = base_subdomain or subdomain
                    country_code = normalize_country_code(adsense_item.get('country_code', ''))
                    key = f"{date_key}_{base_subdomain}_{country_code}"
                    seen_fb_keys.add(key)
                    fb_data = facebook_map.get(key)
                    account_ads = str((fb_data or {}).get('account_name', ''))
                    spend = float((fb_data or {}).get('spend', 0))
                    revenue = float(adsense_item.get('revenue', 0))
                    raw_rows_all.append({
                        'site_name': site_key,
                        'date': date_key,
                        'account_ads': account_ads,
                        'country_code': country_code,
                        'spend': spend,
                        'revenue': revenue
                    })
                    if site_key not in grouped_all:
                        grouped_all[site_key] = {'site_name': site_key, 'account_ads': account_ads, 'spend': 0.0, 'revenue': 0.0}
                    grouped_all[site_key]['account_ads'] = account_ads
                    grouped_all[site_key]['spend'] += spend
                    grouped_all[site_key]['revenue'] += revenue
                    if spend > 0:
                        if site_key not in grouped_filtered:
                            grouped_filtered[site_key] = {'site_name': site_key, 'account_ads': account_ads, 'spend': 0.0, 'revenue': 0.0}
                        grouped_filtered[site_key]['account_ads'] = account_ads
                        grouped_filtered[site_key]['spend'] += spend
                        grouped_filtered[site_key]['revenue'] += revenue
                # Tambahkan baris FB yang tidak punya pasangan AdX (supaya total spend konsisten)
                for fb_key, fb_item in (facebook_map or {}).items():
                    if fb_key in seen_fb_keys:
                        continue
                    subdomain = str(fb_item.get('domain', ''))
                    base_subdomain = extract_base_subdomain(subdomain)
                    site_key = base_subdomain or subdomain
                    account_ads = str(fb_item.get('account_name', ''))
                    spend = float(fb_item.get('spend', 0) or 0)
                    raw_rows_all.append({
                        'site_name': site_key,
                        'date': str(fb_item.get('date', '')),
                        'account_ads': account_ads,
                        'country_code': normalize_country_code(fb_item.get('country_code', '')),
                        'spend': spend,
                        'revenue': 0.0
                    })
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
                    combined_data_all.append({
                        'site_name': item['site_name'],
                        'account_ads': item['account_ads'],
                        'spend': spend_val,
                        'revenue': revenue_val,
                        'roi': roi
                    })
                    total_spend += spend_val
                    total_revenue += revenue_val

                combined_data_filtered = []
                for item in grouped_filtered.values():
                    spend_val = item['spend']
                    revenue_val = item['revenue']
                    roi = ((revenue_val - spend_val) / spend_val * 100) if spend_val > 0 else 0
                    combined_data_filtered.append({
                        'site_name': item['site_name'],
                        'account_ads': item['account_ads'],
                        'spend': spend_val,
                        'revenue': revenue_val,
                        'roi': roi
                    })
            roi_nett_summary = ((total_revenue - total_spend) / total_spend * 100) if total_spend > 0 else 0
            result = {
                'status': True,
                'data': combined_data_all,              # semua kontribusi
                'data_filtered': combined_data_filtered, # hanya spend > 0
                'raw_rows': raw_rows_all,                # baris mentah (opsional, untuk analisis)
                'summary': {
                    'total_spend': total_spend,
                    'roi_nett': roi_nett_summary,
                    'total_revenue': total_revenue
                }
            }
            print(f"[DEBUG ROI] result: {result.get('summary')}")
            return JsonResponse(result, safe=False)
        except Exception as e:
            return JsonResponse({'status': False, 'error': str(e)})

# ===== ROI Monitoring Country =====
class RoiMonitoringCountryAdsenseView(View):
    """View untuk ROI Summary - menampilkan ringkasan data ROI"""
    def dispatch(self, request, *args, **kwargs):
        if 'hris_admin' not in request.session:
            return redirect('admin_login')
        return super().dispatch(request, *args, **kwargs)
    def get(self, req):
        admin = req.session.get('hris_admin', {})
        data_account = data_mysql().master_account_ads()['data']
        last_update = data_mysql().get_last_update_adsense_traffic_per_domain()['data']['last_update']
        if admin.get('super_st') == '0':
            data_account_adsense = data_mysql().get_all_adsense_account_data_user(admin.get('user_id'))
            data_domain_adsense = data_mysql().get_all_adsense_domain_data_user(admin.get('user_id'))
        else:
            data_account_adsense = data_mysql().get_all_adsense_account_data()
            data_domain_adsense = data_mysql().get_all_adsense_domain_data()
        data = {
            'title': 'Rekap Monitoring Country AdSense',
            'user': req.session['hris_admin'],
            'data_account': data_account,
            'data_account_adsense': data_account_adsense['data'],
            'data_domain_adsense': data_domain_adsense['data'],
            'last_update': last_update
        }
        return render(req, 'admin/report_adsense/monitoring_country/index.html', data)

class RoiMonitoringCountryAdsenseDataView(View):
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
        selected_account = req.GET.get('selected_account_adsense', '')
        selected_domain = req.GET.get('selected_domains', '')
        selected_account_list = []
        if selected_account:
            selected_account_list = [str(a).strip() for a in selected_account.split(',') if a.strip()]
        selected_domain_list = []
        if selected_domain:
            selected_domain_list = [str(s).strip() for s in selected_domain.split(',') if s.strip()]
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
                    sites_result = data_mysql().fetch_user_sites_adsense_id_list(
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
            response_cache_key = generate_cache_key_adsense(
                'roi_country_adsense_response_v2',
                start_date,
                end_date,
                selected_account or '',
                selected_domain_list or '',
                ','.join(countries_list_query) if countries_list_query else ''
            )
            cached_response = get_cached_data_adsense(response_cache_key)
            if cached_response is not None:
                return JsonResponse(cached_response, safe=False)
            data_facebook = None
            # Jalankan paralel jika selected_domain sudah ada (menghindari fetch FB yang terlalu lebar)
            if selected_domain_list:
                with ThreadPoolExecutor(max_workers=2) as executor:
                    adsense_future = executor.submit(
                        data_mysql().get_all_adsense_country_detail_by_params,
                        start_date,
                        end_date,
                        selected_account_list,
                        selected_domain_list,
                        countries_list_query
                    )
                    # Normalisasi domain FB
                    if isinstance(selected_domain_list, str):
                        selected_domain_list = [s.strip() for s in selected_domain_list.split(",") if s.strip()]
                    unique_sites = set()
                    for site_item in selected_domain_list:
                        site_name = site_item.strip()
                        if site_name and site_name != 'Unknown':
                            unique_sites.add(site_name)
                    extracted_names = []
                    for site in unique_sites:
                        if "." not in site:
                            continue
                        parts = site.split(".")
                        main_domain = ".".join(parts[:1]) if len(parts) >= 1 else site
                        extracted_names.append(main_domain)
                    unique_name_site = list(set(extracted_names))

                    fb_future = executor.submit(
                        data_mysql().get_all_ads_adsense_country_detail_by_params,
                        start_date,
                        end_date,
                        unique_name_site,
                        countries_list_query
                    )
                    data_adsense = adsense_future.result()
                    try:
                        data_facebook = fb_future.result()
                    except Exception:
                        data_facebook = None
            else:
                data_adsense = data_mysql().get_all_adsense_country_detail_by_params(
                    start_date,
                    end_date,
                    selected_account_list,
                    selected_domain_list,
                    countries_list_query
                )
                try:
                    unique_name_site = []
                    if sites_for_fb:
                        unique_sites = set(site.strip() for site in sites_for_fb if site.strip() and site.strip() != 'Unknown')
                        extracted_names = []
                        for site in unique_sites:
                            main_domain = ".".join(site.split(".")[:1]) if "." in site else site
                            extracted_names.append(main_domain)
                        unique_name_site = list(set(extracted_names))
                    if unique_name_site:
                        data_facebook = data_mysql().get_all_ads_adsense_country_detail_by_params(
                            start_date,
                            end_date,
                            unique_name_site,
                            countries_list_query
                        )
                    else:
                        data_facebook = None
                except Exception as e:
                    print(f"[DEBUG ROI] Facebook fetch (all domains) failed: {e}; continue without FB data")
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
            adsense_payload = data_adsense.get('hasil') if isinstance(data_adsense, dict) and data_adsense.get('hasil') else data_adsense
            fb_payload = (data_facebook.get('hasil') if isinstance(data_facebook, dict) and data_facebook.get('hasil') else {'status': True, 'data': []})
            result = process_roi_monitoring_country_data(adsense_payload, fb_payload)
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
                            print(f"[DEBUG ROI] ✓ MATCH FOUND: '{country_name}' matches '{filter_country}'")
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
                set_cached_data_adsense(response_cache_key, result, timeout=900)
            except Exception as _cache_err:
                print(f"[DEBUG] Failed to cache ROI Country final response: {_cache_err}")
            return JsonResponse(result, safe=False)
        except Exception as e:
            return JsonResponse({
                'status': False,
                'error': str(e)
            })

def process_roi_monitoring_country_data(data_adsense, data_facebook):
    """Fungsi untuk menggabungkan data AdX dan Facebook berdasarkan date + subdomain + country_code,
    lalu agregasi per negara dan menghitung ROI"""
    try:
        adsense_map = {}
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
        # Normalisasi AdSense: date + base_subdomain + country_code
        adsense_items = data_adsense.get('data') if isinstance(data_adsense, dict) else []
        for adsense_item in (adsense_items or []):
            date_key = str(adsense_item.get('date', '') or '')
            site_name = str(adsense_item.get('site_name', '') or '')
            base_subdomain = site_name
            country_code = normalize_country_code(adsense_item.get('country_code', '') or '')
            country_name = adsense_item.get('country_name', '') or ''
            revenue = float(adsense_item.get('revenue', 0) or 0)
            if not date_key or not base_subdomain or not country_code:
                continue
            country_name_by_code[country_code] = country_name or country_name_by_code.get(country_code, '')
            key = f"{date_key}_{base_subdomain}_{country_code}"
            adsense_map[key] = (adsense_map.get(key, 0.0) + revenue)

        # Normalisasi FB: date + base_subdomain + country_code
        fb_payload = data_facebook if isinstance(data_facebook, dict) else {'status': True, 'data': []}
        fb_items = fb_payload.get('data') or []
        for fb_item in fb_items:
            date_key = str(fb_item.get('date', '') or '')
            domain = str(fb_item.get('domain', '') or '')
            base_subdomain = extract_base_subdomain_fb_adsense(domain)
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
        union_keys = set(list[Any](adsense_map.keys()) + list(fb_map.keys()))
        for key in union_keys:
            try:
                country_code = key.split('_')[-1]
            except Exception:
                continue
            revenue = float(adsense_map.get(key, 0.0) or 0.0)
            spend = float(fb_map.get(key, 0.0) or 0.0)
            name = country_name_by_code.get(country_code, '')

            if country_code not in agg_all:
                agg_all[country_code] = {'country': name, 'country_code': country_code, 'spend': 0.0, 'revenue': 0.0}
            agg_all[country_code]['spend'] += spend
            agg_all[country_code]['revenue'] += revenue

            if spend > 0:
                if country_code not in agg_filtered:
                    agg_filtered[country_code] = {'country': name, 'country_code': country_code, 'spend': 0.0, 'revenue': 0.0}
                agg_filtered[country_code]['spend'] += spend
                agg_filtered[country_code]['revenue'] += revenue

        combined_data_all = []
        for code, item in agg_all.items():
            s = item['spend']
            r = item['revenue']
            roi = ((r - s) / s * 100) if s > 0 else 0
            combined_data_all.append({
                'country': item['country'],
                'country_code': code,
                'spend': round(s, 2),
                'revenue': round(r, 2),
                'roi': round(roi, 2)
            })

        combined_data_filtered = []
        for code, item in agg_filtered.items():
            s = item['spend']
            r = item['revenue']
            roi = ((r - s) / s * 100) if s > 0 else 0
            combined_data_filtered.append({
                'country': item['country'],
                'country_code': code,
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

        return {
            'status': True,
            'data': combined_data_all,
            'data_filtered': combined_data_filtered,
            'total_records': len(combined_data_all),
            'total_records_filtered': len(combined_data_filtered),
            'summary_all': {
                'total_spend': round(total_spend_all, 2),
                'total_revenue': round(total_revenue_all, 2),
                'roi_nett': round(((total_revenue_all - total_spend_all) / total_spend_all * 100) if total_spend_all > 0 else 0, 2)
            },
            'summary_filtered': {
                'total_spend': round(total_spend_filtered, 2),
                'total_revenue': round(total_revenue_filtered, 2),
                'roi_nett': round(((total_revenue_filtered - total_spend_filtered) / total_spend_filtered * 100) if total_spend_filtered > 0 else 0, 2)
            }
        }
    except Exception as e:
        return {
            'status': False,
            'error': f'Error processing ROI traffic country data: {str(e)}',
            'data': []
        }

# ===== Rekapitulasi AdSense =====
class AdsenseRekapitulasiView(View):
    """View untuk AdSense Rekapitulasi - menampilkan ringkasan data AdSense"""
    def dispatch(self, request, *args, **kwargs):
        if 'hris_admin' not in request.session:
            return redirect('admin_login')
        return super().dispatch(request, *args, **kwargs)
    def get(self, req):
        admin = req.session.get('hris_admin', {})
        data_account = data_mysql().master_account_ads()['data']
        last_update = data_mysql().get_last_update_adsense_traffic_per_domain()['data']['last_update']
        if admin.get('super_st') == '0':
            data_account_adsense = data_mysql().get_all_adsense_account_data_user(admin.get('user_id'))
            data_domain_adsense = data_mysql().get_all_adsense_domain_data_user(admin.get('user_id'))
        else:
            data_account_adsense = data_mysql().get_all_adsense_account_data()
            data_domain_adsense = data_mysql().get_all_adsense_domain_data()
        if not data_domain_adsense['status']:
            return JsonResponse({
                'status': False,
                'error': data_domain_adsense['data']
            })
        data = {
            'title': 'AdSense Rekapitulasi Dashboard',
            'user': req.session['hris_admin'],
            'data_account': data_account,
            'data_account_adsense': data_account_adsense['data'],
            'data_domain_adsense': data_domain_adsense['data'],
            'last_update': last_update
        }
        return render(req, 'admin/report_adsense/rekapitulasi_adsense/index.html', data)

class AdsenseRekapitulasiDataView(View):
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
            selected_account_list = []
            if req.GET.get('selected_account_adsense'):
                selected_account_list = [
                    s.strip() for s in req.GET.get('selected_account_adsense').split(',') if s.strip()
                ]
            adsense_result = data_mysql().get_all_rekapitulasi_adsense_monitoring_account_by_params(
                start_date,
                end_date,
                past_start_date,
                past_end_date,
                selected_account_list
            )
            return JsonResponse({
                'status': True,
                'periode_now': periode_now,
                'periode_past': periode_past,
                'data': adsense_result['hasil']['data'],
            })
        except Exception as e:
            return JsonResponse({'status': False, 'error': str(e)})

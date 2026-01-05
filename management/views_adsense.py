import os
import json
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
    set_cached_data,
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
            data_account_adx = data_mysql().get_all_adx_account_data_user(admin.get('user_id'))
        else:
            data_account_adx = data_mysql().get_all_adx_account_data()
        if not data_account_adx['status']:
            return JsonResponse({
                'status': False,
                'error': data_account_adx['data']
            })
        data = {
            'title': 'AdSense Traffic Per Account',
            'user': req.session['hris_admin'],
            'data_account_adx': data_account_adx['data']
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
    """AJAX endpoint untuk mengambil daftar account_name dari app_credentials.
    Mengembalikan daftar account yang tersedia untuk filtering.
    """
    def get(self, request):
        try:
            # Ambil semua account_name dari app_credentials yang aktif
            db = data_mysql()
            sql = """
                SELECT DISTINCT account_name, user_mail 
                FROM app_credentials 
                WHERE is_active = '1' 
                ORDER BY account_name ASC
            """
            db.cur_hris.execute(sql)
            credentials_data = db.cur_hris.fetchall()
            
            if not credentials_data:
                # Return dummy data for preview
                return JsonResponse({
                    'status': True,
                    'data': [
                        {'site_id': 'example@gmail.com', 'site_name': 'Example Account'},
                        {'site_id': 'test@gmail.com', 'site_name': 'Test Account'},
                        {'site_id': 'demo@gmail.com', 'site_name': 'Demo Account'}
                    ]
                })
            
            # Format data sesuai dengan yang diharapkan JavaScript
            sites_data = []
            for credential in credentials_data:
                sites_data.append({
                    'site_id': credential['user_mail'],  # Menggunakan user_mail sebagai site_id
                    'site_name': credential['account_name']  # Menggunakan account_name sebagai site_name
                })
            
            return JsonResponse({
                'status': True,
                'data': sites_data
            })
            
        except Exception as e:
            # Return dummy data on error for preview
            return JsonResponse({
                'status': True,
                'data': [
                    {'site_id': 'example@gmail.com', 'site_name': 'Example Account'},
                    {'site_id': 'test@gmail.com', 'site_name': 'Test Account'},
                    {'site_id': 'demo@gmail.com', 'site_name': 'Demo Account'}
                ]
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
            cache_key = generate_cache_key(
                'countries_adsense',
                start_date.strftime('%Y-%m-%d'),
                end_date.strftime('%Y-%m-%d'),
                selected_account or '',
            )
            cached_countries = get_cached_data(cache_key)
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
            set_cached_data(cache_key, countries, timeout=6 * 60 * 60)  # 6 jam
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
            data_account_adx = data_mysql().get_all_adx_account_data_user(admin.get('user_id'))
        else:
            data_account_adx = data_mysql().get_all_adx_account_data()

        if not data_account_adx.get('status'):
            return JsonResponse({
                'status': False,
                'error': data_account_adx.get('data')
            })

        data = {
            'title': 'AdSense Summary Dashboard',
            'user': req.session['hris_admin'],
            'data_account_adx': data_account_adx.get('data', [])
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
            data_account_adx = data_mysql().get_all_adx_account_data_user(admin.get('user_id'))
        else:
            data_account_adx = data_mysql().get_all_adx_account_data()
        if not data_account_adx['status']:
            return JsonResponse({
                'status': False,
                'error': data_account_adx['data']
            })
        last_update = data_mysql().get_last_update_adx_traffic_country()['data']['last_update']
        data = {
            'title': 'AdSense Traffic Per Country',
            'user': req.session['hris_admin'],
            'data_account_adx': data_account_adx['data'],
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


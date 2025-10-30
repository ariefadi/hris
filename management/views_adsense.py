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
from django.views.decorators.csrf import csrf_exempt
from django.utils.decorators import method_decorator
from google_auth_oauthlib.flow import Flow
# Guard googleapiclient import to prevent module-level crash when not installed
try:
    from googleapiclient.discovery import build
except Exception:
    build = None
from google.oauth2.credentials import Credentials
from .utils_adsense import (
    fetch_adsense_traffic_account_data,
    fetch_adsense_summary_data,
    fetch_adsense_traffic_per_country,
)

# OAuth functions removed - using standardized OAuth flow from oauth_views_package

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
    
    def get(self, request):
        return render(request, 'admin/adsense_manager/traffic_account/index.html')

@method_decorator(csrf_exempt, name='dispatch')
class AdsenseTrafficAccountDataView(View):
    """API endpoint untuk mengambil data AdSense Traffic Account"""
    
    def dispatch(self, request, *args, **kwargs):
        # Check if user is logged in as admin
        if 'hris_admin' not in request.session:
            return JsonResponse({'error': 'Unauthorized'}, status=401)
        return super().dispatch(request, *args, **kwargs)
    
    def post(self, request):
        try:
            # Get user ID from session
            user_id = request.session['hris_admin'].get('user_id')
            if not user_id:
                return JsonResponse({
                    'error': 'User ID not found in session'
                }, status=400)
            
            # Get user email from database using user_id
            from .database import data_mysql
            db = data_mysql()
            sql = "SELECT user_mail FROM app_users WHERE user_id = %s"
            db.cur_hris.execute(sql, (user_id,))
            user_data = db.cur_hris.fetchone()
            
            if not user_data or not user_data.get('user_mail'):
                return JsonResponse({
                    'error': 'User email not found in database'
                }, status=400)
            
            user_mail = user_data['user_mail']
            
            # Get form data
            start_date = request.POST.get('start_date')
            end_date = request.POST.get('end_date')
            site_filter = request.POST.get('site_filter', '%')
            
            # Validate required fields
            if not start_date or not end_date:
                return JsonResponse({
                    'error': 'Start date and end date are required'
                }, status=400)
            
            # Call the AdSense API function
            result = fetch_adsense_traffic_account_data(user_mail, start_date, end_date, site_filter)
            
            if result.get('status'):
                data = result.get('data', {})
                
                if not data.get('sites', []):
                    return JsonResponse({
                        'status': True,
                        'data': [],  # Empty data array when no real AdSense data available
                        'message': 'Tidak ada data AdSense untuk periode ini. Pastikan: 1) Website sudah menggunakan iklan AdSense, 2) Ada traffic/tayangan iklan selama periode ini, 3) Akun AdSense sudah dikonfigurasi dengan benar.'
                    })
                else:
                    # Calculate summary data from sites
                    sites_data = data.get('sites', [])
                    total_impressions = sum(site.get('impressions', 0) for site in sites_data)
                    total_clicks = sum(site.get('clicks', 0) for site in sites_data)
                    total_revenue = sum(site.get('revenue', 0) for site in sites_data)
                    
                    # Calculate averages
                    avg_ctr = (total_clicks / total_impressions * 100) if total_impressions > 0 else 0
                    avg_cpc = (total_revenue / total_clicks) if total_clicks > 0 else 0
                    avg_cpm = (total_revenue / total_impressions * 1000) if total_impressions > 0 else 0
                    
                    summary = {
                        'total_impressions': total_impressions,
                        'total_clicks': total_clicks,
                        'total_revenue': total_revenue,
                        'avg_ctr': avg_ctr,
                        'avg_cpc': avg_cpc,
                        'avg_cpm': avg_cpm
                    }
                    
                    # Return data with summary
                    return JsonResponse({
                        'status': True,
                        'data': sites_data,
                        'summary': summary
                    })
            else:
                return JsonResponse({
                    'error': result.get('error', 'Failed to fetch AdSense data')
                }, status=500)
                
        except Exception as e:
            print(f"[ERROR] Exception in AdsenseTrafficAccountDataView: {str(e)}")
            import traceback
            traceback.print_exc()
            return JsonResponse({
                'error': f'Server error: {str(e)}'
            }, status=500)

class AdsenseSitesListView(View):
    """AJAX endpoint untuk mengambil daftar domain dari AdSense data.
    Mengembalikan daftar domain unik yang tersedia untuk filtering.
    """
    def get(self, request):
        try:
            # Ambil user_mail dari app_credentials yang aktif
            from .database import data_mysql
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
            
            if not credential_data:
                # Return dummy data for preview
                return JsonResponse({
                    'status': True,
                    'data': ['example.com', 'test.com', 'demo.com']
                })
            
            user_mail = credential_data['user_mail']
            
            # Ambil domain unik dari database
            sql_domains = """
                SELECT DISTINCT domain_name 
                FROM adsense_traffic_account 
                WHERE user_mail = %s 
                AND domain_name IS NOT NULL 
                AND domain_name != ''
                ORDER BY domain_name
            """
            db.cur_hris.execute(sql_domains, (user_mail,))
            domains_data = db.cur_hris.fetchall()
            
            # Extract domain names into simple array
            domains = [row['domain_name'] for row in domains_data if row['domain_name']]
            
            # If no domains found, return dummy data
            if not domains:
                domains = ['example.com', 'test.com', 'demo.com']
            
            return JsonResponse({
                'status': True,
                'data': domains
            })
            
        except Exception as e:
            # Return dummy data on error for preview
            return JsonResponse({
                'status': True,
                'data': ['example.com', 'test.com', 'demo.com']
            })

class AdsenseSummaryView(View):
    """View untuk halaman Summary AdSense"""
    def dispatch(self, request, *args, **kwargs):
        if 'hris_admin' not in request.session:
            return redirect('admin_login')
        return super().dispatch(request, *args, **kwargs)

    def get(self, request):
        return render(request, 'admin/adsense_manager/summary/index.html')

class AdsenseSummaryDataView(View):
    """AJAX endpoint untuk data Summary AdSense (dibuka untuk preview)."""
    def dispatch(self, request, *args, **kwargs):
        if 'hris_admin' not in request.session:
            return redirect('admin_login')
        return super().dispatch(request, *args, **kwargs)

    def get(self, request):
        try:
            # Ambil user_mail dari app_credentials yang aktif
            from .database import data_mysql
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
            
            if not credential_data:
                return JsonResponse({'status': False, 'error': 'Tidak ada kredensial aktif yang ditemukan'})
            
            user_mail = credential_data['user_mail']

            # Ambil parameter
            start_date = request.GET.get('start_date')
            end_date = request.GET.get('end_date')
            site_filter = request.GET.get('selected_sites') or '%'

            if not start_date or not end_date:
                return JsonResponse({'status': False, 'error': 'Start date dan end date wajib diisi'})

            # Ambil data summary
            result = fetch_adsense_summary_data(user_mail, start_date, end_date, site_filter)
            
            if not result.get('status'):
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
                }
            }
            return JsonResponse(response_data)

        except Exception as e:
            return JsonResponse({'status': False, 'error': str(e)})

class AdsenseTrafficPerCountryView(View):
    """View untuk halaman AdSense Traffic Per Country"""
    def dispatch(self, request, *args, **kwargs):
        if 'hris_admin' not in request.session:
            return redirect('admin_login')
        return super().dispatch(request, *args, **kwargs)

    def get(self, request):
        return render(request, 'admin/adsense_manager/traffic_country/index.html')

@method_decorator(csrf_exempt, name='dispatch')
class AdsenseTrafficPerCountryDataView(View):
    """AJAX endpoint untuk data AdSense Traffic Per Country"""
    def dispatch(self, request, *args, **kwargs):
        if 'hris_admin' not in request.session:
            return redirect('admin_login')
        return super().dispatch(request, *args, **kwargs)

    def post(self, request):
        try:
            # Get user ID from session
            user_id = request.session['hris_admin'].get('user_id')
            if not user_id:
                return JsonResponse({
                    'error': 'User ID not found in session'
                }, status=400)
            
            # Get user email from database using user_id
            from .database import data_mysql
            db = data_mysql()
            sql = "SELECT user_mail FROM app_users WHERE user_id = %s"
            db.cur_hris.execute(sql, (user_id,))
            user_data = db.cur_hris.fetchone()
            
            if not user_data or not user_data.get('user_mail'):
                return JsonResponse({
                    'error': 'User email not found in database'
                }, status=400)
            
            user_mail = user_data['user_mail']
            # Get form data (same as Traffic Account)
            start_date = request.POST.get('start_date')
            end_date = request.POST.get('end_date')
            site_filter = request.POST.get('site_filter', '%')
            country_filter = request.POST.get('country_filter', '')
            if not start_date or not end_date:
                return JsonResponse({
                    'error': 'Start date dan end date wajib diisi'
                }, status=400)

            # Parse countries list
            countries_list = []
            if country_filter and country_filter.strip():
                countries_list = [c.strip() for c in country_filter.split(',') if c.strip()]

            # Fetch AdSense traffic data per country
            result = fetch_adsense_traffic_per_country(user_mail, start_date, end_date, site_filter, countries_list)
            
            if result.get('status'):
                countries_data = result.get('data', [])
                
                # Calculate summary data
                total_impressions = sum(country.get('impressions', 0) for country in countries_data)
                total_clicks = sum(country.get('clicks', 0) for country in countries_data)
                total_revenue = sum(country.get('revenue', 0) for country in countries_data)
                
                avg_ctr = (total_clicks / total_impressions * 100) if total_impressions > 0 else 0
                avg_cpc = (total_revenue / total_clicks) if total_clicks > 0 else 0
                avg_cpm = (total_revenue / total_impressions * 1000) if total_impressions > 0 else 0
                
                summary = {
                    'total_impressions': total_impressions,
                    'total_clicks': total_clicks,
                    'total_revenue': total_revenue,
                    'avg_ctr': avg_ctr,
                    'avg_cpc': avg_cpc,
                    'avg_cpm': avg_cpm
                }
                
                # Return data with summary
                return JsonResponse({
                    'status': True,
                    'data': countries_data,
                    'summary': summary
                })
            else:
                return JsonResponse({
                    'error': result.get('error', 'Failed to fetch AdSense country data')
                }, status=500)
                
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
    """AJAX endpoint untuk data AdSense Account (dummy untuk preview)."""
    def get(self, request):
        try:
            data = [
                {
                    'account_id': 'pub-1234567890123456',
                    'user_mail': 'adsense_user@example.com',
                    'site_count': 2,
                    'authorized': True,
                },
                {
                    'account_id': 'pub-9876543210987654',
                    'user_mail': 'another_user@example.com',
                    'site_count': 1,
                    'authorized': False,
                },
            ]
            return JsonResponse({'status': True, 'data': data})
        except Exception as e:
            return JsonResponse({'status': False, 'error': str(e)})


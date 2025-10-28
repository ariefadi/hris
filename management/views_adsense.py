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
from .utils_adsense import fetch_adsense_traffic_account_data

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
            
            print(f"[DEBUG] Calling fetch_adsense_traffic_account_data with: {user_mail}, {start_date}, {end_date}, {site_filter}")
            
            # Fetch AdSense traffic account data
            response = fetch_adsense_traffic_account_data(user_mail, start_date, end_date, site_filter)
            
            print(f"[DEBUG] Response status: {response.get('status')}, Error: {response.get('error')}")
            
            if response['status']:
                data = response['data']
                # Check if data is empty
                if not data.get('sites', []) and not data.get('campaigns', []) and data.get('total_impressions', 0) == 0:
                    return JsonResponse({
                        'success': True,
                        'summary': {
                            'total_impressions': data.get('total_impressions', 0),
                            'total_clicks': data.get('total_clicks', 0),
                            'total_revenue': data.get('total_revenue', 0.0),
                            'average_ctr': data.get('overall_ctr', 0.0)
                        },
                        'sites': data.get('sites', []),
                        'campaigns': data.get('campaigns', []),
                        'message': 'No AdSense data found for the selected period. This could be because: 1) No websites are actively using AdSense ads, 2) No traffic or ad impressions during this period, 3) AdSense account needs to be properly configured with active websites.'
                    })
                else:
                    return JsonResponse({
                        'success': True,
                        'summary': {
                            'total_impressions': data.get('total_impressions', 0),
                            'total_clicks': data.get('total_clicks', 0),
                            'total_revenue': data.get('total_revenue', 0.0),
                            'average_ctr': data.get('overall_ctr', 0.0)
                        },
                        'sites': data.get('sites', []),
                        'campaigns': data.get('campaigns', [])
                    })
            else:
                print(f"[ERROR] AdSense API failed: {response.get('error')}")
                return JsonResponse({
                    'error': response['error']
                }, status=500)
                
        except Exception as e:
            print(f"[ERROR] Exception in AdsenseTrafficAccountDataView: {str(e)}")
            import traceback
            traceback.print_exc()
            return JsonResponse({
                'error': f'Server error: {str(e)}'
            }, status=500)

class AdsenseSitesListView(View):
    """AJAX endpoint untuk mengambil daftar situs dari AdSense"""
    def dispatch(self, request, *args, **kwargs):
        if 'hris_admin' not in request.session:
            return redirect('admin_login')
        return super().dispatch(request, *args, **kwargs)
    
    def get(self, request):
        try:
            # Ambil user_id dari session
            user_id = request.session.get('hris_admin', {}).get('user_id')
            if not user_id:
                return JsonResponse({
                    'status': False,
                    'error': 'User ID tidak ditemukan dalam session'
                })
            
            # Ambil email user dari database berdasarkan user_id
            from .database import data_mysql
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
            
            # Return dummy data untuk sementara karena masalah OAuth scope
            return JsonResponse({
                'status': True,
                'data': [
                    {'site_id': 'ca-pub-1234567890123456', 'site_name': 'example.com'},
                    {'site_id': 'ca-pub-1234567890123457', 'site_name': 'test.com'}
                ]
            })
            
        except Exception as e:
            return JsonResponse({
                'status': False,
                'error': str(e)
            })


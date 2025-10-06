"""
OAuth Utilities for Dynamic Refresh Token Management
Utility functions yang dapat diintegrasikan ke dalam Django views
"""

from django.conf import settings
from django.http import JsonResponse
from django.contrib import messages
from management.database import data_mysql
from management.googleads_patch_v2 import apply_googleads_patches
from management.jsonfield_patch import patch_social_django_jsonfield
import requests
import urllib.parse
import logging

logger = logging.getLogger(__name__)

def get_current_user_from_request(request):
    """
    Mendapatkan user yang sedang login dari request Django
    """
    if hasattr(request, 'session') and request.session.get('hris_admin'):
        hris_admin = request.session.get('hris_admin', {})
        return {
            'user_id': hris_admin.get('user_id'),
            'user_mail': hris_admin.get('user_mail'),
            'user_name': hris_admin.get('user_name'),
            'user_alias': hris_admin.get('user_alias')
        }
    return None

def get_oauth_credentials():
    """
    Mendapatkan OAuth credentials dari settings Django
    """
    client_id = getattr(settings, 'SOCIAL_AUTH_GOOGLE_OAUTH2_KEY', None)
    client_secret = getattr(settings, 'SOCIAL_AUTH_GOOGLE_OAUTH2_SECRET', None)
    
    return client_id, client_secret

def generate_oauth_url_for_user(user_mail, scopes=None):
    """
    Generate OAuth authorization URL untuk user tertentu
    """
    if not scopes:
        scopes = ['https://www.googleapis.com/auth/admanager']
    
    client_id, client_secret = get_oauth_credentials()
    if not client_id:
        return None, "OAuth credentials tidak ditemukan"
    
    # Gunakan localhost redirect URI yang lebih mudah dikonfigurasi
    redirect_uri = 'http://127.0.0.1:8000/accounts/complete/google-oauth2/'
    
    params = {
        'client_id': client_id,
        'redirect_uri': redirect_uri,
        'scope': ' '.join(scopes),
        'response_type': 'code',
        'access_type': 'offline',
        'prompt': 'consent',
        'login_hint': user_mail  # Hint untuk login dengan email tertentu
    }
    
    base_url = 'https://accounts.google.com/o/oauth2/auth'
    oauth_url = f"{base_url}?{urllib.parse.urlencode(params)}"
    
    return oauth_url, None

def exchange_code_for_refresh_token(auth_code):
    """
    Exchange authorization code untuk refresh token
    """
    client_id, client_secret = get_oauth_credentials()
    if not client_id or not client_secret:
        return None, None, "OAuth credentials tidak ditemukan"
    
    token_url = 'https://oauth2.googleapis.com/token'
    # Gunakan redirect URI yang sama dengan yang digunakan saat generate URL
    redirect_uri = 'http://127.0.0.1:8000/accounts/complete/google-oauth2/'
    
    data = {
        'client_id': client_id,
        'client_secret': client_secret,
        'code': auth_code,
        'grant_type': 'authorization_code',
        'redirect_uri': redirect_uri
    }
    
    try:
        response = requests.post(token_url, data=data)
        response.raise_for_status()
        
        token_data = response.json()
        refresh_token = token_data.get('refresh_token')
        
        if not refresh_token:
            return None, token_data, "Refresh token tidak ditemukan dalam response"
        
        return refresh_token, token_data, None
        
    except requests.exceptions.RequestException as e:
        logger.error(f"Error exchanging code for token: {str(e)}")
        return None, None, f"Error: {str(e)}"

def save_refresh_token_for_current_user(request, refresh_token):
    """
    Menyimpan refresh token untuk user yang sedang login
    """
    # Apply patches
    patch_social_django_jsonfield()
    apply_googleads_patches()
    
    # Get current user
    current_user = get_current_user_from_request(request)
    if not current_user or not current_user.get('user_mail'):
        return {
            'status': False,
            'message': 'User tidak ditemukan dalam session'
        }
    
    user_mail = current_user['user_mail']
    
    # Save to database
    db = data_mysql()
    try:
        sql = """
            UPDATE app_oauth_credentials 
            SET google_ads_refresh_token = %s,
                updated_at = NOW()
            WHERE user_mail = %s
        """
        
        if db.execute_query(sql, (refresh_token, user_mail)):
            db.db_hris.commit()
            
            if db.cur_hris.rowcount > 0:
                logger.info(f"Refresh token saved for user: {user_mail}")
                return {
                    'status': True,
                    'message': f'Refresh token berhasil disimpan untuk {user_mail}',
                    'user_mail': user_mail,
                    'user_name': current_user.get('user_alias', 'Unknown')
                }
            else:
                return {
                    'status': False,
                    'message': f'User {user_mail} tidak ditemukan di tabel app_oauth_credentials'
                }
        else:
            return {
                'status': False,
                'message': 'Gagal mengeksekusi query database'
            }
            
    except Exception as e:
        logger.error(f"Error saving refresh token: {str(e)}")
        return {
            'status': False,
            'message': f'Error: {str(e)}'
        }

def get_user_oauth_status(user_mail):
    """
    Mendapatkan status OAuth untuk user tertentu
    """
    db = data_mysql()
    try:
        sql = """
            SELECT user_mail, 
                   CASE WHEN google_ads_refresh_token IS NOT NULL AND google_ads_refresh_token != ''
                        THEN 'active' ELSE 'inactive' END as token_status,
                   CASE WHEN google_ads_refresh_token IS NOT NULL 
                        THEN LENGTH(google_ads_refresh_token) ELSE 0 END as token_length,
                   updated_at
            FROM app_oauth_credentials 
            WHERE user_mail = %s
        """
        
        if db.execute_query(sql, (user_mail,)):
            result = db.cur_hris.fetchone()
            if result:
                return {
                    'status': True,
                    'data': {
                        'user_mail': result['user_mail'],
                        'token_status': result['token_status'],
                        'token_length': result['token_length'],
                        'updated_at': result['updated_at'],
                        'has_token': result['token_status'] == 'active'
                    }
                }
            else:
                return {
                    'status': False,
                    'message': f'User {user_mail} tidak ditemukan'
                }
        else:
            return {
                'status': False,
                'message': 'Gagal mengeksekusi query'
            }
            
    except Exception as e:
        logger.error(f"Error getting OAuth status: {str(e)}")
        return {
            'status': False,
            'message': f'Error: {str(e)}'
        }

def handle_oauth_callback(request, auth_code):
    """
    Handle OAuth callback dan simpan refresh token untuk current user
    """
    try:
        # Exchange code for refresh token
        refresh_token, token_data, error = exchange_code_for_refresh_token(auth_code)
        
        if error:
            return JsonResponse({
                'status': False,
                'message': error
            })
        
        if not refresh_token:
            return JsonResponse({
                'status': False,
                'message': 'Refresh token tidak ditemukan',
                'token_data': token_data
            })
        
        # Save refresh token for current user
        save_result = save_refresh_token_for_current_user(request, refresh_token)
        
        if save_result['status']:
            messages.success(request, f"✅ Refresh token berhasil disimpan untuk {save_result['user_name']}")
            return JsonResponse({
                'status': True,
                'message': save_result['message'],
                'user_mail': save_result['user_mail'],
                'user_name': save_result['user_name'],
                'token_length': len(refresh_token)
            })
        else:
            messages.error(request, f"❌ {save_result['message']}")
            return JsonResponse(save_result)
            
    except Exception as e:
        logger.error(f"Error handling OAuth callback: {str(e)}")
        messages.error(request, f"❌ Error: {str(e)}")
        return JsonResponse({
            'status': False,
            'message': f'Error: {str(e)}'
        })

def generate_oauth_flow_for_current_user(request):
    """
    Generate OAuth flow untuk user yang sedang login
    """
    current_user = get_current_user_from_request(request)
    if not current_user or not current_user.get('user_mail'):
        return {
            'status': False,
            'message': 'User tidak ditemukan dalam session'
        }
    
    user_mail = current_user['user_mail']
    oauth_url, error = generate_oauth_url_for_user(user_mail)
    
    if error:
        return {
            'status': False,
            'message': error
        }
    
    return {
        'status': True,
        'oauth_url': oauth_url,
        'user_mail': user_mail,
        'user_name': current_user.get('user_alias', 'Unknown'),
        'instructions': [
            f"1. Buka URL OAuth di browser",
            f"2. Login dengan akun: {user_mail}",
            f"3. Berikan izin akses untuk Google Ad Manager",
            f"4. Copy 'code' parameter dari URL redirect",
            f"5. Submit code melalui form atau API"
        ]
    }
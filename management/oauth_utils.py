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
from management.credential_loader import get_credentials_for_oauth_login, update_settings_for_user
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

def get_oauth_credentials(user_mail=None):
    """
    Mendapatkan OAuth credentials dari settings Django atau database untuk user tertentu
    
    Args:
        user_mail (str, optional): Email user untuk mengambil kredensial spesifik
    """
    if user_mail:
        # Ambil kredensial spesifik untuk user
        credentials = get_credentials_for_oauth_login(user_mail)
        if credentials and credentials.get('google_oauth2_client_id'):
            return credentials['google_oauth2_client_id'], credentials['google_oauth2_client_secret']
    
    # Fallback ke settings default
    client_id = getattr(settings, 'SOCIAL_AUTH_GOOGLE_OAUTH2_KEY', None)
    client_secret = getattr(settings, 'SOCIAL_AUTH_GOOGLE_OAUTH2_SECRET', None)
    
    return client_id, client_secret

def generate_oauth_url_for_user(user_mail, scopes=None):
    """
    Generate OAuth authorization URL untuk user tertentu dengan kredensial yang sesuai
    """
    if not scopes:
        scopes = ['https://www.googleapis.com/auth/admanager']
    
    # Ambil kredensial untuk user spesifik
    client_id, client_secret = get_oauth_credentials(user_mail)
    if not client_id:
        return None, f"OAuth credentials tidak ditemukan untuk user: {user_mail}"
    
    # Update settings untuk memastikan OAuth menggunakan kredensial yang benar
    update_settings_for_user(user_mail)
    
    # Generate OAuth URL
    base_url = "https://accounts.google.com/o/oauth2/v2/auth"
    redirect_uri = getattr(settings, 'OAUTH_REDIRECT_URI', 'urn:ietf:wg:oauth:2.0:oob')
    
    params = {
        'client_id': client_id,
        'redirect_uri': redirect_uri,
        'scope': ' '.join(scopes),
        'response_type': 'code',
        'access_type': 'offline',
        'prompt': 'consent',
        'state': f'user:{user_mail}'  # Tambahkan state untuk tracking user
    }
    
    oauth_url = f"{base_url}?{urllib.parse.urlencode(params)}"
    logger.info(f"Generated OAuth URL for user {user_mail} with client_id: {client_id[:10]}...")
    
    return oauth_url, None

def exchange_code_for_refresh_token(auth_code, user_mail=None):
    """
    Exchange authorization code untuk refresh token dengan kredensial yang sesuai
    
    Args:
        auth_code (str): Authorization code dari OAuth callback
        user_mail (str, optional): Email user untuk menggunakan kredensial yang sesuai
    """
    try:
        # Ambil kredensial untuk user spesifik
        client_id, client_secret = get_oauth_credentials(user_mail)
        if not client_id or not client_secret:
            return None, f"OAuth credentials tidak lengkap untuk user: {user_mail}"
        
        token_url = "https://oauth2.googleapis.com/token"
        redirect_uri = getattr(settings, 'OAUTH_REDIRECT_URI', 'urn:ietf:wg:oauth:2.0:oob')
        
        data = {
            'client_id': client_id,
            'client_secret': client_secret,
            'code': auth_code,
            'grant_type': 'authorization_code',
            'redirect_uri': redirect_uri
        }
        
        response = requests.post(token_url, data=data)
        
        if response.status_code == 200:
            token_data = response.json()
            refresh_token = token_data.get('refresh_token')
            
            if refresh_token:
                logger.info(f"Successfully obtained refresh token for user: {user_mail}")
                return refresh_token, None
            else:
                return None, "Refresh token tidak ditemukan dalam response"
        else:
            logger.error(f"Token exchange failed for user {user_mail}: {response.text}")
            return None, f"Gagal menukar code: {response.text}"
            
    except Exception as e:
        logger.error(f"Error exchanging code for user {user_mail}: {str(e)}")
        return None, f"Error: {str(e)}"

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
        # Coba UPDATE terlebih dahulu
        sql_update = """
            UPDATE app_oauth_credentials 
            SET google_ads_refresh_token = %s,
                updated_at = NOW()
            WHERE user_mail = %s
        """
        
        if db.execute_query(sql_update, (refresh_token, user_mail)):
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
                # Jika tidak ada baris yang ter-update, coba INSERT (upsert fallback)
                try:
                    user_id = current_user.get('user_id')
                    sql_insert = """
                        INSERT INTO app_oauth_credentials (user_id, user_mail, google_ads_refresh_token)
                        VALUES (%s, %s, %s)
                    """
                    if db.execute_query(sql_insert, (user_id, user_mail, refresh_token)):
                        db.db_hris.commit()
                        logger.info(f"Refresh token inserted for user: {user_mail}")
                        return {
                            'status': True,
                            'message': f'Refresh token berhasil dibuat dan disimpan untuk {user_mail}',
                            'user_mail': user_mail,
                            'user_name': current_user.get('user_alias', 'Unknown')
                        }
                    else:
                        return {
                            'status': False,
                            'message': 'Gagal mengeksekusi INSERT ke app_oauth_credentials'
                        }
                except Exception as insert_err:
                    logger.error(f"Error inserting refresh token row: {str(insert_err)}")
                    return {
                        'status': False,
                        'message': f'Gagal membuat row app_oauth_credentials: {str(insert_err)}'
                    }
        else:
            return {
                'status': False,
                'message': 'Gagal mengeksekusi query UPDATE app_oauth_credentials'
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
    Handle OAuth callback dengan dynamic credential loading
    """
    try:
        current_user = get_current_user_from_request(request)
        if not current_user or not current_user.get('user_mail'):
            return {
                'status': False,
                'message': 'User tidak ditemukan dalam session'
            }
        
        user_mail = current_user['user_mail']
        logger.info(f"Processing OAuth callback for user: {user_mail}")
        
        # Exchange code untuk refresh token dengan kredensial user yang sesuai
        refresh_token, error = exchange_code_for_refresh_token(auth_code, user_mail)
        
        if error:
            logger.error(f"OAuth callback failed for {user_mail}: {error}")
            return {
                'status': False,
                'message': error
            }
        
        # Simpan refresh token
        save_result = save_refresh_token_for_current_user(request, refresh_token)
        
        if save_result['status']:
            logger.info(f"OAuth callback completed successfully for {user_mail}")
            return {
                'status': True,
                'message': f'OAuth berhasil untuk {user_mail}',
                'user_mail': user_mail,
                'refresh_token_saved': True
            }
        else:
            return {
                'status': False,
                'message': save_result['message']
            }
            
    except Exception as e:
        logger.error(f"Error in OAuth callback: {str(e)}")
        return {
            'status': False,
            'message': f'Error processing callback: {str(e)}'
        }

def generate_oauth_flow_for_current_user(request):
    """
    Generate OAuth flow untuk user yang sedang login dengan kredensial dinamis
    """
    current_user = get_current_user_from_request(request)
    if not current_user or not current_user.get('user_mail'):
        return {
            'status': False,
            'message': 'User tidak ditemukan dalam session'
        }
    
    user_mail = current_user['user_mail']
    logger.info(f"Generating OAuth flow for user: {user_mail}")
    
    # Generate OAuth URL dengan kredensial user yang sesuai
    oauth_url, error = generate_oauth_url_for_user(user_mail)
    
    if error:
        logger.error(f"Failed to generate OAuth URL for {user_mail}: {error}")
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
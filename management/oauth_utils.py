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

def get_oauth_credentials(user_mail=None, scopes=None):
    """
    Mendapatkan OAuth credentials dari settings Django atau database untuk user tertentu
    
    Args:
        user_mail (str, optional): Email user untuk mengambil kredensial spesifik
        scopes (list[str], optional): Daftar scopes yang diminta; gunakan Ads client untuk scope Ad Manager/DFP
    """
    if user_mail:
        # Ambil kredensial spesifik untuk user
        credentials = get_credentials_for_oauth_login(user_mail)
        if credentials:
            # Jika scope mengandung admanager/dfp/adsense, gunakan client Ads
            use_ads_client = False
            if scopes:
                joined = ' '.join(scopes).lower()
                use_ads_client = any(s in joined for s in ['admanager', 'dfp', 'adsense'])
            if use_ads_client and credentials.get('google_ads_client_id') and credentials.get('google_ads_client_secret'):
                return credentials['google_ads_client_id'], credentials['google_ads_client_secret']
            # Fallback ke OAuth2 Web client
            if credentials.get('google_oauth2_client_id') and credentials.get('google_oauth2_client_secret'):
                return credentials['google_oauth2_client_id'], credentials['google_oauth2_client_secret']
    
    # Fallback ke settings default
    # Prefer Ads client dari settings jika scope Ad Manager
    use_ads_client = False
    if scopes:
        joined = ' '.join(scopes).lower()
        use_ads_client = any(s in joined for s in ['admanager', 'dfp', 'adsense'])
    if use_ads_client:
        client_id = getattr(settings, 'GOOGLE_ADS_CLIENT_ID', None)
        client_secret = getattr(settings, 'GOOGLE_ADS_CLIENT_SECRET', None)
    else:
        client_id = getattr(settings, 'SOCIAL_AUTH_GOOGLE_OAUTH2_KEY', None)
        client_secret = getattr(settings, 'SOCIAL_AUTH_GOOGLE_OAUTH2_SECRET', None)
    
    return client_id, client_secret

def generate_oauth_url_for_user(user_mail, scopes=None, redirect_uri=None):
    """
    Generate OAuth authorization URL untuk user tertentu dengan kredensial yang sesuai
    """
    if not scopes:
        scopes = ['https://www.googleapis.com/auth/admanager']
    
    # Ambil kredensial untuk user spesifik (pilih Ads client jika scope Ad Manager)
    client_id, client_secret = get_oauth_credentials(user_mail, scopes=scopes)
    if not client_id:
        return None, f"OAuth credentials tidak ditemukan untuk user: {user_mail}"
    
    # Update settings untuk memastikan OAuth menggunakan kredensial yang benar
    update_settings_for_user(user_mail)
    
    # Gunakan redirect_uri dari parameter jika tersedia, fallback ke settings
    if not redirect_uri:
        redirect_uri = getattr(settings, 'OAUTH_REDIRECT_URI', 'urn:ietf:wg:oauth:2.0:oob')
    
    # Generate OAuth URL
    base_url = "https://accounts.google.com/o/oauth2/v2/auth"
    
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
    logger.info(
        f"Generated OAuth URL for user={user_mail}, client_id={client_id[:10]}..., redirect_uri={redirect_uri}, scopes={' '.join(scopes)}"
    )
    
    return oauth_url, None

def exchange_code_for_refresh_token(auth_code, user_mail=None, redirect_uri=None, scopes=None):
    """
    Exchange authorization code untuk refresh token dengan kredensial yang sesuai
    
    Args:
        auth_code (str): Authorization code dari OAuth callback
        user_mail (str, optional): Email user untuk menggunakan kredensial yang sesuai
        redirect_uri (str, optional): Redirect URI yang sama persis dengan yang dipakai saat authorization
    """
    try:
        # Ambil kredensial untuk user spesifik (pilih Ads client jika scope Ad Manager)
        client_id, client_secret = get_oauth_credentials(user_mail, scopes=scopes)
        if not client_id or not client_secret:
            return None, f"OAuth credentials tidak lengkap untuk user: {user_mail}"
        
        token_url = "https://oauth2.googleapis.com/token"
        # Gunakan redirect_uri yang SAMA dengan saat generate URL
        if not redirect_uri:
            redirect_uri = getattr(settings, 'OAUTH_REDIRECT_URI', 'urn:ietf:wg:oauth:2.0:oob')
        
        data = {
            'client_id': client_id,
            'client_secret': client_secret,
            'code': auth_code,
            'grant_type': 'authorization_code',
            'redirect_uri': redirect_uri
        }
        logger.info(
            f"Exchanging code for user={user_mail}, client_id={client_id[:10]}..., redirect_uri={redirect_uri}, scopes={scopes or ['https://www.googleapis.com/auth/admanager']}"
        )

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
            # Coba parse error JSON untuk detail
            try:
                err_json = response.json()
            except Exception:
                err_json = {'raw': response.text}
            logger.error(
                f"Token exchange failed for user {user_mail} (status={response.status_code}): {err_json}"
            )
            # Berikan pesan yang menjelaskan kemungkinan mismatch redirect URI
            hint = (
                "Kemungkinan penyebab: redirect_uri tidak cocok dengan yang terdaftar di Google Console atau "
                "redirect_uri saat authorization berbeda dengan saat token exchange. Pastikan keduanya IDENTIK."
            )
            return None, f"Gagal menukar code: {err_json}. {hint}"
            
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

def save_refresh_token_for_user(user_mail, refresh_token):
    """
    Menyimpan refresh token untuk user tertentu (tanpa bergantung pada session)
    Mengembalikan error yang jelas jika baris kredensial belum ada.
    """
    try:
        patch_social_django_jsonfield()
        apply_googleads_patches()

        if not user_mail:
            return {
                'status': False,
                'message': 'Email user tidak boleh kosong'
            }

        db = data_mysql()
        # Pastikan kredensial user ada terlebih dahulu
        creds = db.get_user_oauth_credentials(user_mail)
        if not creds.get('status'):
            return {
                'status': False,
                'message': f'Gagal mengambil kredensial untuk {user_mail}: {creds.get("error", "Unknown error")}'
            }
        if not creds.get('data'):
            return {
                'status': False,
                'message': f'Kredensial OAuth belum dikonfigurasi untuk {user_mail}. Isi client_id/client_secret terlebih dahulu.'
            }

        upd = db.update_refresh_token(user_mail, refresh_token)
        if upd.get('status'):
            logger.info(f"Refresh token saved for user: {user_mail}")
            return {
                'status': True,
                'message': f'Refresh token berhasil disimpan untuk {user_mail}',
                'user_mail': user_mail
            }
        else:
            return {
                'status': False,
                'message': upd.get('error', 'Gagal update refresh token')
            }
    except Exception as e:
        logger.error(f"Error saving refresh token for {user_mail}: {str(e)}")
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

def handle_oauth_callback(request, auth_code, target_user_mail=None):
    """
    Handle OAuth callback dengan dynamic credential loading
    Mendukung target user lain melalui parameter `target_user_mail` atau fallback ke session.
    """
    try:
        current_user = get_current_user_from_request(request)
        # Tentukan email target: prioritas parameter, lalu session
        user_mail = target_user_mail or (current_user.get('user_mail') if current_user else None)
        if not user_mail:
            return {
                'status': False,
                'message': 'Email user tidak ditemukan (session kosong dan parameter tidak diberikan)'
            }
        logger.info(f"Processing OAuth callback for user: {user_mail}")
        
        # Pastikan redirect_uri yang dipakai saat exchange SAMA dengan yang dipakai saat authorization
        callback_url = request.build_absolute_uri('/management/admin/oauth/callback/')
        # Scope sensitif untuk Ad Manager
        scopes = ['https://www.googleapis.com/auth/admanager']
        # Exchange code untuk refresh token dengan kredensial user yang sesuai
        refresh_token, error = exchange_code_for_refresh_token(
            auth_code, user_mail, redirect_uri=callback_url, scopes=scopes
        )
        
        if error:
            logger.error(f"OAuth callback failed for {user_mail}: {error}")
            return {
                'status': False,
                'message': error
            }
        
        # Simpan refresh token
        # Simpan ke user target
        save_result = save_refresh_token_for_user(user_mail, refresh_token)
        
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
    
    # Generate absolute redirect URL ke endpoint callback (GET)
    callback_url = request.build_absolute_uri('/management/admin/oauth/callback/')
    logger.info(
        f"Generating OAuth URL with redirect_uri={callback_url} for user={user_mail}"
    )

    # Scope sensitif untuk Ad Manager
    scopes = ['https://www.googleapis.com/auth/admanager']
    # Generate OAuth URL dengan kredensial user yang sesuai dan redirect web
    oauth_url, error = generate_oauth_url_for_user(user_mail, scopes=scopes, redirect_uri=callback_url)
    
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
            f"1. Klik OAuth URL untuk membuka consent screen",
            f"2. Login dengan akun: {user_mail}",
            f"3. Berikan izin akses untuk Google Ad Manager",
            f"4. Setelah sukses, kamu akan dialihkan kembali ke dashboard",
            f"5. Token akan otomatis disimpan; jika gagal, gunakan form manual"
        ]
    }

def generate_oauth_flow_for_selected_user(request, target_user_mail):
    """
    Generate OAuth flow untuk email yang dipilih admin.
    Menggunakan kredensial user tersebut dan mengembalikan URL dengan state berisi email.
    """
    if not target_user_mail:
        return {
            'status': False,
            'message': 'Email target tidak boleh kosong'
        }

    logger.info(f"Generating OAuth flow for selected user: {target_user_mail}")

    # Redirect URL absolut untuk callback
    callback_url = request.build_absolute_uri('/management/admin/oauth/callback/')

    # Scope Ad Manager
    scopes = ['https://www.googleapis.com/auth/admanager']

    # Buat URL OAuth untuk email target
    oauth_url, error = generate_oauth_url_for_user(target_user_mail, scopes=scopes, redirect_uri=callback_url)
    if error:
        logger.error(f"Failed to generate OAuth URL for {target_user_mail}: {error}")
        return {
            'status': False,
            'message': error
        }

    # Ambil nama alias dari app_users jika tersedia
    try:
        db = data_mysql()
        info = db.get_user_by_email(target_user_mail)
        user_alias = None
        if info and info.get('status') and info.get('data'):
            user_alias = info['data'].get('user_alias') or info['data'].get('user_name')
    except Exception:
        user_alias = None

    return {
        'status': True,
        'oauth_url': oauth_url,
        'user_mail': target_user_mail,
        'user_name': user_alias or target_user_mail,
        'instructions': [
            f"1. Klik OAuth URL untuk membuka consent screen",
            f"2. Login dengan akun: {target_user_mail}",
            f"3. Berikan izin akses untuk Google Ad Manager",
            f"4. Setelah sukses, kamu akan dialihkan kembali ke dashboard",
            f"5. Token akan otomatis disimpan; jika gagal, gunakan form manual"
        ]
    }
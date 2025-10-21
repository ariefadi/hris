from datetime import datetime
import uuid
from geopy.geocoders import Nominatim
import requests

def load_user_credentials(backend, user, response, request, *args, **kwargs):
    """
    Load kredensial OAuth yang sesuai untuk user yang sedang login
    Pipeline ini dipanggil sebelum proses OAuth dimulai
    """
    from management.credential_loader import update_settings_for_user
    import logging
    
    logger = logging.getLogger(__name__)
    
    # Ambil email dari user atau dari request
    user_email = None
    if user and hasattr(user, 'email'):
        user_email = user.email
    elif hasattr(request, 'session') and request.session.get('oauth_login_email'):
        user_email = request.session.get('oauth_login_email')
    
    if user_email:
        logger.info(f"[PIPELINE] Loading credentials for user: {user_email}")
        # Update Django settings dengan kredensial user yang sesuai
        success = update_settings_for_user(user_email)
        if success:
            logger.info(f"[PIPELINE] Successfully loaded credentials for: {user_email}")
        else:
            logger.warning(f"[PIPELINE] Failed to load credentials for: {user_email}")
    else:
        logger.warning("[PIPELINE] No user email found, using default credentials")
    
    return {}

def validate_email_access(backend, user, response, request, *args, **kwargs):
    """
    Validasi email sebelum login OAuth berhasil.
    Mengecek apakah email terdaftar di Ad Manager dan database.
    """
    from management.utils import validate_oauth_email
    from social_core.exceptions import AuthForbidden
    
    print(f"[DEBUG] Pipeline validate_email_access called for user: {user.email}")
    
    # Set user_mail attribute
    user.user_mail = user.email
    
    # Validasi email
    validation_result = validate_oauth_email(user.email)
    
    if not validation_result['valid']:
        error_msg = validation_result.get('error', 'Email validation failed')
        print(f"[DEBUG] Email validation failed: {error_msg}")
        request.session['oauth_error'] = error_msg
        raise AuthForbidden(backend, error_msg)
    
    print(f"[DEBUG] Email validation passed for: {user.email}")
    return {'user': user}

def set_hris_session(backend, user, response, request, *args, **kwargs):
    """
    Set HRIS session setelah user berhasil login melalui OAuth
    """
    from management.database import data_mysql
    from management.utils import check_email_in_database
    
    print(f"[DEBUG] Pipeline set_hris_session called for user: {user.email}")
    print(f"[DEBUG] Response keys: {list(response.keys()) if isinstance(response, dict) else 'Not a dict'}")
    print(f"[DEBUG] Response content: {response}")
    
    # Ambil data user dari database berdasarkan email
    db_check = check_email_in_database(user.email)
    
    print(f"[DEBUG] Database query result: {db_check}")

    if db_check['status'] and db_check['exists']:
        user_data = db_check['data']
        # Coba simpan refresh_token jika tersedia dari response
    try:
        print(f"[DEBUG] Full OAuth response: {response}")
        print(f"[DEBUG] Response type: {type(response)}")
        print(f"[DEBUG] Response keys: {list(response.keys()) if isinstance(response, dict) else 'Not a dict'}")
        
        refresh_token = response.get('refresh_token')
        print(f"[DEBUG] Extracted refresh_token: {refresh_token}")
        
        if refresh_token:
            print(f"[DEBUG] Found refresh_token in response, saving to database...")
            db = data_mysql()
            
            # Cek apakah user sudah ada di tabel oauth credentials
            check_sql = '''
                SELECT user_id, user_mail FROM app_oauth_credentials 
                WHERE user_mail = %s AND is_active = 1
            '''
            existing_user = db.execute_query(check_sql, (user.email,))
            print(f"[DEBUG] Existing OAuth credentials check: {existing_user}")
            
            update_sql = '''
                UPDATE app_oauth_credentials 
                SET google_ads_refresh_token = %s, updated_at = NOW()
                WHERE user_mail = %s AND is_active = 1
            '''
            if db.execute_query(update_sql, (refresh_token, user.email)):
                print(f"[DEBUG] Refresh token successfully saved for {user.email}")
                
                # Verifikasi penyimpanan
                verify_sql = '''
                    SELECT google_ads_refresh_token FROM app_oauth_credentials 
                    WHERE user_mail = %s AND is_active = 1
                '''
                saved_token = db.execute_query(verify_sql, (user.email,))
                print(f"[DEBUG] Verification - saved token: {saved_token}")
            else:
                print(f"[DEBUG] Failed to save refresh token for {user.email}")
        else:
            print(f"[DEBUG] No refresh_token found in OAuth response")
            print(f"[DEBUG] This might be because:")
            print(f"[DEBUG] 1. User already granted permission before (refresh_token only given on first consent)")
            print(f"[DEBUG] 2. OAuth settings not configured for offline access")
            print(f"[DEBUG] 3. User needs to revoke app permissions and re-authenticate")
            
    except Exception as e:
        print(f"[DEBUG] Error saving refresh token: {str(e)}")
        import traceback
        print(f"[DEBUG] Full traceback: {traceback.format_exc()}")
        
        # Set session HRIS
        request.session['hris_admin'] = {
            'user_id': user_data.get('user_id'),
            'user_mail': user_data.get('user_mail'),
            'user_name': user_data.get('user_name'),
            'user_alias': user_data.get('user_alias'),
            'login_time': datetime.now().isoformat(),
            'oauth_login': True
        }
        
        print(f"[DEBUG] HRIS session set for user: {user.email}")
        
        return {
            'user': user,
            'hris_session_set': True,
            'user_data': user_data
        }
    else:
        print(f"[DEBUG] User {user.email} not found in database")
        request.session['oauth_error'] = f'User {user.email} tidak terdaftar dalam sistem'
        return {
            'user': user,
            'hris_session_set': False,
            'error': 'User not found in database'
        }

def save_profile(backend, user, response, *args, **kwargs):
    """
    Simpan atau update profile user dari OAuth response
    """
    print(f"[DEBUG] Pipeline save_profile called for user: {user.email}")
    
    # Update user profile jika diperlukan
    if hasattr(user, 'first_name') and not user.first_name:
        user.first_name = response.get('given_name', '')
    
    if hasattr(user, 'last_name') and not user.last_name:
        user.last_name = response.get('family_name', '')
    
    user.save()
    
    return {'user': user}
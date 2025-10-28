from management.database import data_mysql
import logging

logger = logging.getLogger(__name__)

def save_refresh_token(backend, user, response, request, *args, **kwargs):
    """
    Pipeline function untuk menyimpan refresh token ke database
    """
    try:
        print(f"[DEBUG] Pipeline save_refresh_token called for user: {user.email}")
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
            
            if existing_user:
                # Update existing record
                update_sql = '''
                    UPDATE app_oauth_credentials 
                    SET google_ads_refresh_token = %s, updated_at = NOW()
                    WHERE user_mail = %s AND is_active = 1
                '''
                if db.execute_query(update_sql, (refresh_token, user.email)):
                    db.db_hris.commit()
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
                print(f"[DEBUG] No existing OAuth credentials found for {user.email}")
                print(f"[DEBUG] User needs to be added to app_oauth_credentials table first")
        else:
            print(f"[DEBUG] No refresh_token found in OAuth response")
            print(f"[DEBUG] This might be because:")
            print(f"[DEBUG] 1. User already granted permission before (refresh_token only given on first consent)")
            print(f"[DEBUG] 2. OAuth settings not configured for offline access")
            print(f"[DEBUG] 3. User needs to revoke app permissions and re-authenticate")
            
            # Cek apakah user sudah memiliki refresh token di database
            db = data_mysql()
            check_existing_token_sql = '''
                SELECT google_ads_refresh_token FROM app_oauth_credentials 
                WHERE user_mail = %s AND is_active = 1
            '''
            existing_token = db.execute_query(check_existing_token_sql, (user.email,))
            if existing_token and existing_token[0] and existing_token[0][0]:
                print(f"[DEBUG] User already has refresh token in database: {existing_token[0][0][:20]}...")
            else:
                print(f"[DEBUG] User has no refresh token in database")
            
    except Exception as e:
        print(f"[DEBUG] Error saving refresh token: {str(e)}")
        import traceback
        print(f"[DEBUG] Full traceback: {traceback.format_exc()}")
        
    return {}
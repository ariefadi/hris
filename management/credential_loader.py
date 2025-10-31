"""
Credential Loader untuk mengambil kredensial OAuth dari database
"""
import logging
from django.conf import settings
from .database import data_mysql

logger = logging.getLogger(__name__)

def get_credentials_from_db(user_mail=None):
    """
    Mengambil kredensial OAuth dari database.
    Prioritas baca dari tabel baru `app_credentials` (client_id, client_secret, refresh_token, network_code, developer_token)
    Fallback ke tabel lama `app_credentials` jika tidak ditemukan.
    """
    try:
        db = data_mysql()

        if user_mail:
            # Coba ambil dari tabel baru app_credentials
            sql_new = '''
                SELECT user_mail, client_id, client_secret, refresh_token, network_code, developer_token
                FROM app_credentials
                WHERE user_mail = %s
                LIMIT 1
            '''
            if db.execute_query(sql_new, (user_mail,)):
                row = db.cur_hris.fetchone()
                if row:
                    logger.info(f"Loaded app_credentials for user: {user_mail}")
                    return {
                        'client_id': row.get('client_id', ''),
                        'client_secret': row.get('client_secret', ''),
                        'refresh_token': row.get('refresh_token', ''),
                        'network_code': row.get('network_code', ''),
                        'developer_token': row.get('developer_token', ''),
                        'user_mail': row.get('user_mail', ''),
                        'user_id': ''
                    }

            # Fallback: ambil dari tabel lama app_credentials
            result = db.get_user_credentials(user_mail)
            if isinstance(result, dict) and result.get('status'):
                credentials = result['data']
                logger.info(f"Loaded legacy credentials for user: {user_mail}")
                return {
                    'client_id': credentials.get('client_id', ''),
                    'client_secret': credentials.get('client_secret', ''),
                    'refresh_token': credentials.get('refresh_token', ''),
                    'network_code': credentials.get('network_code', ''),
                    'user_mail': credentials.get('user_mail', ''),
                    'user_id': credentials.get('user_id', '')
                }

            logger.warning(f"No credentials found for user: {user_mail}")
        else:
            # Default: ambil kredensial aktif dari app_credentials
            sql_new_default = '''
                SELECT user_mail, client_id, client_secret, refresh_token, network_code, developer_token
                FROM app_credentials
                WHERE is_active = '1'
                ORDER BY mdd DESC
                LIMIT 1
            '''
            if db.execute_query(sql_new_default):
                row = db.cur_hris.fetchone()
                if row:
                    logger.info("Loaded default credentials from app_credentials")
                    return {
                        'client_id': row.get('client_id', ''),
                        'client_secret': row.get('client_secret', ''),
                        'refresh_token': row.get('refresh_token', ''),
                        'network_code': row.get('network_code', ''),
                        'developer_token': row.get('developer_token', ''),
                        'user_mail': row.get('user_mail', ''),
                        'user_id': ''
                    }

    except Exception as e:
        logger.error(f"Error loading credentials from database: {str(e)}")

def load_credentials_to_settings(user_mail=None):
    """
    Load kredensial dari database dan set ke Django settings
    
    Args:
        user_mail (str, optional): Email user untuk mengambil kredensial spesifik
    """
    credentials = get_credentials_from_db(user_mail)
    
    if credentials:
        # Update Django settings dengan kredensial dari database
        settings.GOOGLE_OAUTH2_CLIENT_ID = credentials['client_id']
        settings.GOOGLE_OAUTH2_CLIENT_SECRET = credentials['client_secret']
        settings.GOOGLE_ADS_CLIENT_ID = credentials['client_id']
        settings.GOOGLE_ADS_CLIENT_SECRET = credentials['client_secret']
        settings.GOOGLE_ADS_REFRESH_TOKEN = credentials['refresh_token']
        
        # Update Social Auth settings
        settings.SOCIAL_AUTH_GOOGLE_OAUTH2_KEY = credentials['client_id']
        settings.SOCIAL_AUTH_GOOGLE_OAUTH2_SECRET = credentials['client_secret']
        
        logger.info(f"Settings updated with credentials for user: {credentials.get('user_mail', 'default')}")
        return True
    
    logger.error("Failed to load credentials to settings")
    return False

def get_current_user_credentials():
    """
    Mengambil kredensial untuk user yang sedang login
    Menggunakan session atau context untuk menentukan user
    """
    try:
        from .middleware import get_current_user_mail
        user_mail = get_current_user_mail()
        
        if user_mail:
            return get_credentials_from_db(user_mail)
        else:
            # Jika tidak ada user dalam session, gunakan kredensial default
            return get_credentials_from_db()
            
    except Exception as e:
        logger.error(f"Error getting current user credentials: {str(e)}")
        return get_credentials_from_db()  # Fallback ke default

def get_credentials_for_oauth_login(email):
    """
    Mengambil kredensial OAuth khusus untuk proses login
    Digunakan saat user akan melakukan OAuth login dengan email tertentu
    
    Args:
        email (str): Email user yang akan login
        
    Returns:
        dict: Kredensial OAuth untuk email tersebut
    """
    try:
        logger.info(f"Loading OAuth credentials for login attempt: {email}")
        credentials = get_credentials_from_db(email)
        
        if credentials and credentials.get('google_oauth2_client_id'):
            logger.info(f"Found OAuth credentials for {email}")
            return credentials
        else:
            logger.warning(f"No OAuth credentials found for {email}, using default")
            return get_credentials_from_db()  # Fallback ke default
            
    except Exception as e:
        logger.error(f"Error loading OAuth credentials for {email}: {str(e)}")
        return get_credentials_from_db()  # Fallback ke default

def update_settings_for_user(user_mail):
    """
    Update Django settings dengan kredensial untuk user tertentu
    Digunakan untuk memastikan OAuth menggunakan kredensial yang benar
    
    Args:
        user_mail (str): Email user
    """
    try:
        credentials = get_credentials_from_db(user_mail)
        
        if credentials and credentials.get('google_oauth2_client_id'):
            # Update settings secara real-time
            settings.SOCIAL_AUTH_GOOGLE_OAUTH2_KEY = credentials['client_id']
            settings.SOCIAL_AUTH_GOOGLE_OAUTH2_SECRET = credentials['client_secret']
            settings.GOOGLE_OAUTH2_CLIENT_ID = credentials['client_id']
            settings.GOOGLE_OAUTH2_CLIENT_SECRET = credentials['client_secret']
            
            logger.info(f"Updated settings for OAuth login: {user_mail}")
            return True
        else:
            logger.warning(f"Could not update settings for {user_mail}, credentials not found")
            return False
            
    except Exception as e:
        logger.error(f"Error updating settings for {user_mail}: {str(e)}")
        return False
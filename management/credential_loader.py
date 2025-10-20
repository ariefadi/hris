"""
Credential Loader untuk mengambil kredensial OAuth dari database
"""
import logging
from django.conf import settings
from .database import data_mysql

logger = logging.getLogger(__name__)

def get_credentials_from_db(user_mail=None):
    """
    Mengambil kredensial OAuth dari database berdasarkan user_mail
    Jika user_mail tidak diberikan, ambil kredensial default (user pertama yang aktif)
    
    Args:
        user_mail (str, optional): Email user untuk mengambil kredensial spesifik
        
    Returns:
        dict: Kredensial OAuth atau None jika tidak ditemukan
    """
    try:
        db = data_mysql()
        
        if user_mail:
            # Ambil kredensial untuk user spesifik
            result = db.get_user_oauth_credentials(user_mail)
            if result['status']:
                credentials = result['data']
                logger.info(f"Loaded credentials for user: {user_mail}")
                return {
                    'google_oauth2_client_id': credentials.get('google_oauth2_client_id', ''),
                    'google_oauth2_client_secret': credentials.get('google_oauth2_client_secret', ''),
                    'google_ads_client_id': credentials.get('google_ads_client_id', ''),
                    'google_ads_client_secret': credentials.get('google_ads_client_secret', ''),
                    'google_ads_refresh_token': credentials.get('google_ads_refresh_token', ''),
                    'google_ad_manager_network_code': credentials.get('google_ad_manager_network_code', ''),
                    'user_mail': credentials.get('user_mail', ''),
                    'user_id': credentials.get('user_id', '')
                }
            else:
                logger.warning(f"No credentials found for user: {user_mail}")
        else:
            # Ambil kredensial default (user pertama yang aktif)
            sql = '''
                SELECT user_id, user_mail, google_oauth2_client_id, google_oauth2_client_secret,
                       google_ads_client_id, google_ads_client_secret, google_ads_refresh_token,
                       google_ad_manager_network_code
                FROM app_oauth_credentials
                WHERE is_active = 1
                ORDER BY created_at ASC
                LIMIT 1
            '''
            
            if db.execute_query(sql):
                credentials = db.cur_hris.fetchone()
                if credentials:
                    logger.info("Loaded default credentials from database")
                    return {
                        'google_oauth2_client_id': credentials.get('google_oauth2_client_id', ''),
                        'google_oauth2_client_secret': credentials.get('google_oauth2_client_secret', ''),
                        'google_ads_client_id': credentials.get('google_ads_client_id', ''),
                        'google_ads_client_secret': credentials.get('google_ads_client_secret', ''),
                        'google_ads_refresh_token': credentials.get('google_ads_refresh_token', ''),
                        'google_ad_manager_network_code': credentials.get('google_ad_manager_network_code', ''),
                        'user_mail': credentials.get('user_mail', ''),
                        'user_id': credentials.get('user_id', '')
                    }
                else:
                    logger.warning("No active credentials found in database")
            else:
                logger.error("Failed to execute query for default credentials")
                
    except Exception as e:
        logger.error(f"Error loading credentials from database: {str(e)}")
    
    # Fallback ke environment variables jika database tidak tersedia
    logger.info("Falling back to environment variables")
    return {
        'google_oauth2_client_id': settings.GOOGLE_OAUTH2_CLIENT_ID,
        'google_oauth2_client_secret': settings.GOOGLE_OAUTH2_CLIENT_SECRET,
        'google_ads_client_id': settings.GOOGLE_ADS_CLIENT_ID,
        'google_ads_client_secret': settings.GOOGLE_ADS_CLIENT_SECRET,
        'google_ads_refresh_token': settings.GOOGLE_ADS_REFRESH_TOKEN,
        'google_ad_manager_network_code': getattr(settings, 'GOOGLE_AD_MANAGER_NETWORK_CODE', ''),
        'user_mail': '',
        'user_id': ''
    }

def load_credentials_to_settings(user_mail=None):
    """
    Load kredensial dari database dan set ke Django settings
    
    Args:
        user_mail (str, optional): Email user untuk mengambil kredensial spesifik
    """
    credentials = get_credentials_from_db(user_mail)
    
    if credentials:
        # Update Django settings dengan kredensial dari database
        settings.GOOGLE_OAUTH2_CLIENT_ID = credentials['google_oauth2_client_id']
        settings.GOOGLE_OAUTH2_CLIENT_SECRET = credentials['google_oauth2_client_secret']
        settings.GOOGLE_ADS_CLIENT_ID = credentials['google_ads_client_id']
        settings.GOOGLE_ADS_CLIENT_SECRET = credentials['google_ads_client_secret']
        settings.GOOGLE_ADS_REFRESH_TOKEN = credentials['google_ads_refresh_token']
        
        # Update Social Auth settings
        settings.SOCIAL_AUTH_GOOGLE_OAUTH2_KEY = credentials['google_oauth2_client_id']
        settings.SOCIAL_AUTH_GOOGLE_OAUTH2_SECRET = credentials['google_oauth2_client_secret']
        
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
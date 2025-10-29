"""
Django settings for hris project.
"""
import os
from pathlib import Path
from pickle import FALSE
from dotenv import load_dotenv
import pymysql

# Install PyMySQL as MySQLdb
pymysql.install_as_MySQLdb()

# Apply JSONField compatibility patch for social_django
try:
    from management.jsonfield_patch import patch_social_django_jsonfield
    patch_social_django_jsonfield()
except ImportError:
    pass

# Build paths inside the project like this: BASE_DIR / 'subdir'.
BASE_DIR = Path(__file__).resolve().parent.parent

# Load environment variables from .env file
load_dotenv(BASE_DIR / '.env')

# Provide compatibility alias for deprecated force_text used by older packages
try:
    from django.utils.encoding import force_str
    import django.utils.encoding as _enc
    if not hasattr(_enc, 'force_text'):
        _enc.force_text = force_str
except Exception:
    pass

# Provide compatibility aliases for deprecated urlquote/urlquote_plus
try:
    from urllib.parse import quote as _quote, quote_plus as _quote_plus
    import django.utils.http as _http
    if not hasattr(_http, 'urlquote'):
        _http.urlquote = _quote
    if not hasattr(_http, 'urlquote_plus'):
        _http.urlquote_plus = _quote_plus
except Exception:
    pass

# Function to get credentials from database
def get_credentials_from_db(request=None):
    """
    Mengambil kredensial dari tabel app_oauth_credentials berdasarkan user_id dan user_mail dari session.
    """
    from management.database import data_mysql
    
    # Ambil user_id dan user_mail dari request.oauth_user jika tersedia
    user_id = None
    user_mail = None
    if request and hasattr(request, 'oauth_user'):
        user_id = request.oauth_user.get('user_id')
        user_mail = request.oauth_user.get('user_mail')
    
    # Ambil kredensial dari database
    db = data_mysql()
    credentials = db.get_user_oauth_credentials(user_mail=user_mail)
    
    if credentials['status'] and credentials['data']:
        creds = credentials['data']
        return {
            'google_oauth2_client_id': creds['google_oauth2_client_id'],
            'google_oauth2_client_secret': creds['google_oauth2_client_secret'],
            'google_ads_client_id': creds['google_ads_client_id'],
            'google_ads_client_secret': creds['google_ads_client_secret'],
            'google_ads_refresh_token': creds['google_ads_refresh_token'],
            'google_ad_manager_network_code': creds['google_ad_manager_network_code']
        }
    return None

# Quick-start development settings - unsuitable for production
SECRET_KEY = os.getenv('DJANGO_SECRET_KEY', 'your-secret-key-here')
DEBUG = True
ALLOWED_HOSTS = ['127.0.0.1', 'localhost', 'kiwipixel.com', 'www.kiwipixel.com']

# Application definition
INSTALLED_APPS = [
    'management',
    'django.contrib.admin',
    'django.contrib.auth',
    'django.contrib.contenttypes',
    'django.contrib.sessions',
    'django.contrib.messages',
    'django.contrib.staticfiles',
    'social_django',
    'django_extensions',
]

AUTHENTICATION_BACKENDS = (
    'management.custom_oauth.CustomGoogleOAuth2',  # Custom backend
    'django.contrib.auth.backends.ModelBackend',
)

# Middleware Configuration
MIDDLEWARE = [
    'django.middleware.security.SecurityMiddleware',
    'django.contrib.sessions.middleware.SessionMiddleware',
    'django.middleware.common.CommonMiddleware',
    'django.middleware.csrf.CsrfViewMiddleware',
    'django.contrib.auth.middleware.AuthenticationMiddleware',
    'django.contrib.messages.middleware.MessageMiddleware',
    'django.middleware.clickjacking.XFrameOptionsMiddleware',
    'social_django.middleware.SocialAuthExceptionMiddleware',
    'management.middleware.AuthMiddleware',
    'management.middleware.RequestMiddleware',
    'management.middleware.OAuthCredentialsMiddleware',
]

ROOT_URLCONF = 'hris.urls'

TEMPLATES = [
    {
        'BACKEND': 'django.template.backends.django.DjangoTemplates',
        'DIRS': [BASE_DIR / 'templates'],
        'APP_DIRS': True,
        'OPTIONS': {
            'context_processors': [
                'django.template.context_processors.debug',
                'django.template.context_processors.request',
                'django.contrib.auth.context_processors.auth',
                'django.contrib.messages.context_processors.messages',
                'social_django.context_processors.backends',
                'social_django.context_processors.login_redirect',
            ],
        },
    },
]

WSGI_APPLICATION = 'hris.wsgi.application'

# Database Configuration - MySQL
DATABASES = {
    'default': {
        'ENGINE': 'django.db.backends.mysql',
        'NAME': 'hris_trendHorizone',
        'USER': 'root',
        'PASSWORD': '',
        'HOST': '127.0.0.1',
        'PORT': '3307',
        'OPTIONS': {
            'sql_mode': 'STRICT_TRANS_TABLES',
            'init_command': "SET sql_mode='STRICT_TRANS_TABLES'",
        }
    }
}

# Override database version check for MariaDB compatibility
import django.db.backends.mysql.base
import django.db.backends.mysql.features

# Override mysql_server_info property
def get_mysql_server_info(self):
    return '10.5.0-MariaDB'

# Override mysql_is_mariadb property  
def get_mysql_is_mariadb(self):
    return True

# Override minimum_database_version property
def get_minimum_database_version(self):
    return (10, 4)

django.db.backends.mysql.base.DatabaseWrapper.mysql_server_info = property(get_mysql_server_info)
django.db.backends.mysql.base.DatabaseWrapper.mysql_is_mariadb = property(get_mysql_is_mariadb)
django.db.backends.mysql.features.DatabaseFeatures.minimum_database_version = property(get_minimum_database_version)

# SQLite Configuration (backup/fallback)
# DATABASES = {
#     'default': {
#         'ENGINE': 'django.db.backends.sqlite3',
#         'NAME': BASE_DIR / 'db.sqlite3',
#     }
# }

# Static files Configuration
STATIC_URL = '/static/'
STATIC_ROOT = os.path.join(BASE_DIR, 'staticfiles')
STATICFILES_DIRS = [
    os.path.join(BASE_DIR, 'static'),
]

DEFAULT_AUTO_FIELD = 'django.db.models.BigAutoField'
# Session Configuration for OAuth State Fix
SESSION_ENGINE = 'django.contrib.sessions.backends.db'
SESSION_COOKIE_AGE = 3600  # 1 hour (shorter for OAuth)
SESSION_COOKIE_SECURE = False  # Set to False for HTTP development
SESSION_COOKIE_HTTPONLY = True
SESSION_COOKIE_SAMESITE = 'Lax'
SESSION_SAVE_EVERY_REQUEST = True  # Important for OAuth state

# CSRF Configuration
CSRF_COOKIE_SECURE = False  # Set to False for HTTP development
CSRF_COOKIE_SAMESITE = 'Lax'
CSRF_USE_SESSIONS = True  # Use sessions for CSRF tokens
CSRF_TRUSTED_ORIGINS = [
    'https://kiwipixel.com',
    'https://www.kiwipixel.com',
]

# Social Auth Storage and State Configuration
SOCIAL_AUTH_STORAGE = 'social_django.models.DjangoStorage'
SOCIAL_AUTH_USE_UNIQUE_USER_ID = True
SOCIAL_AUTH_SESSION_EXPIRATION = True

# Social Auth state configuration
SOCIAL_AUTH_FIELDS_STORED_IN_SESSION = ['state']
SOCIAL_AUTH_PROTECTED_USER_FIELDS = ['email']

# Social Auth Google OAuth2 Settings - SSO only (basic scopes)
SOCIAL_AUTH_GOOGLE_OAUTH2_SCOPE = [
    'openid',
    'email',
    'profile'
]

# Google API Scopes for Ad Manager
GOOGLE_SCOPES = [
    'https://www.googleapis.com/auth/dfp',
    'https://www.googleapis.com/auth/admanager',
]

# Sensitive scopes moved to separate configuration for manual authorization when needed
SENSITIVE_SCOPES = [
    'adwords',
    'dfp', 
    'admanager'
]

SOCIAL_AUTH_GOOGLE_OAUTH2_AUTH_EXTRA_ARGUMENTS = {
    'access_type': 'offline',
    'prompt': 'consent',
    'include_granted_scopes': 'true'
}

# Explicitly include refresh_token in extra data
SOCIAL_AUTH_GOOGLE_OAUTH2_EXTRA_DATA = [
    ('refresh_token', 'refresh_token', True),
    ('expires_in', 'expires'),
    ('token_type', 'token_type', True),
    ('access_token', 'access_token'),
]

# Google Scopes - Basic scopes only to avoid verification warning
GOOGLE_SCOPES = [
    'openid',
    'email', 
    'profile'
]

# Sensitive Google Scopes for Ad Manager/AdSense - to be requested separately
SENSITIVE_GOOGLE_SCOPES = [
    'https://www.googleapis.com/auth/dfp',
    'https://www.googleapis.com/auth/admanager',
    'https://www.googleapis.com/auth/adsense.readonly',
]

# OAuth Redirect URLs - harus sesuai dengan yang terdaftar di Google Console
# Dapat di-override lewat environment agar tidak terikat DEBUG
SOCIAL_AUTH_GOOGLE_OAUTH2_REDIRECT_URI = os.getenv(
    'SOCIAL_AUTH_GOOGLE_OAUTH2_REDIRECT_URI',
    'http://127.0.0.1:8000/accounts/complete/google-oauth2/' if DEBUG else 'https://kiwipixel.com/accounts/complete/google-oauth2/'
)

# Redirect URI untuk flow OAuth kustom (oauth_utils) agar penukaran code cocok
OAUTH_REDIRECT_URI = os.getenv(
    'OAUTH_REDIRECT_URI',
    'http://127.0.0.1:8000/management/admin/oauth/callback/' if DEBUG else 'https://kiwipixel.com/management/admin/oauth/callback/'
)

# Social Auth Error Handling
SOCIAL_AUTH_RAISE_EXCEPTIONS = False
SOCIAL_AUTH_LOGIN_ERROR_URL = '/management/admin/login'
SOCIAL_AUTH_LOGIN_REDIRECT_URL = '/management/admin/oauth_redirect'
SOCIAL_AUTH_NEW_USER_REDIRECT_URL = '/management/admin/oauth_redirect'

# Social Auth Pipeline - Simplified with Dynamic Credential Loading
SOCIAL_AUTH_PIPELINE = (
    'social_core.pipeline.social_auth.social_details',
    'social_core.pipeline.social_auth.social_uid',
    'social_core.pipeline.social_auth.auth_allowed',
    'social_core.pipeline.social_auth.social_user',
    'social_core.pipeline.user.get_username',
    'social_core.pipeline.user.create_user',
    'social_core.pipeline.social_auth.associate_user',
    'social_core.pipeline.social_auth.load_extra_data',
    'management.pipeline.set_hris_session',
    'social_core.pipeline.user.user_details',
    'management.pipeline.save_profile',
)

# Login URLs
LOGIN_URL = '/management/admin/login'
LOGIN_REDIRECT_URL = '/management/admin/oauth_redirect'
LOGOUT_REDIRECT_URL = '/management/admin/login'



# Default OAuth credentials from environment variables
GOOGLE_OAUTH2_CLIENT_ID = os.getenv('GOOGLE_OAUTH2_CLIENT_ID', '')
GOOGLE_OAUTH2_CLIENT_SECRET = os.getenv('GOOGLE_OAUTH2_CLIENT_SECRET', '')
GOOGLE_ADS_CLIENT_ID = os.getenv('GOOGLE_ADS_CLIENT_ID', '')
GOOGLE_ADS_CLIENT_SECRET = os.getenv('GOOGLE_ADS_CLIENT_SECRET', '')
GOOGLE_ADS_REFRESH_TOKEN = os.getenv('GOOGLE_ADS_REFRESH_TOKEN', '')
GOOGLE_AD_MANAGER_NETWORK_CODE = os.getenv('GOOGLE_AD_MANAGER_NETWORK_CODE', '')

# Load credentials from database
try:
    from management.credential_loader import get_credentials_from_db
    db_credentials = get_credentials_from_db()
    if db_credentials and db_credentials.get('google_oauth2_client_id'):
        GOOGLE_OAUTH2_CLIENT_ID = db_credentials['google_oauth2_client_id']
        GOOGLE_OAUTH2_CLIENT_SECRET = db_credentials['google_oauth2_client_secret']
        GOOGLE_ADS_CLIENT_ID = db_credentials['google_ads_client_id']
        GOOGLE_ADS_CLIENT_SECRET = db_credentials['google_ads_client_secret']
        GOOGLE_ADS_REFRESH_TOKEN = db_credentials['google_ads_refresh_token']
        GOOGLE_AD_MANAGER_NETWORK_CODE = db_credentials['google_ad_manager_network_code']
        print(f"[SETTINGS] Loaded credentials from database for user: {db_credentials.get('user_mail', 'default')}")
except Exception as e:
    print(f"[SETTINGS] Could not load credentials from database: {str(e)}")
    print("[SETTINGS] Using environment variables as fallback")

# Social Auth Configuration
SOCIAL_AUTH_GOOGLE_OAUTH2_KEY = GOOGLE_OAUTH2_CLIENT_ID
SOCIAL_AUTH_GOOGLE_OAUTH2_SECRET = GOOGLE_OAUTH2_CLIENT_SECRET
# Set HTTPS redirect based on environment
if DEBUG:
    SOCIAL_AUTH_REDIRECT_IS_HTTPS = False   # Development now uses HTTP
else:
    SOCIAL_AUTH_REDIRECT_IS_HTTPS = True   # Production uses HTTPS

# Function to get user-specific credentials
def get_user_credentials():
    from management.middleware import get_current_user_mail
    user_mail = get_current_user_mail()
    if user_mail:
        return get_credentials_from_db(user_mail)
    return get_credentials_from_db()  # Fallback to default credentials

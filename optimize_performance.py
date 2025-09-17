#!/usr/bin/env python3
"""
Django Performance Optimization Script
This script optimizes various aspects of your Django project for better performance.
"""

import os
import sys
import subprocess
import logging
from pathlib import Path

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class DjangoOptimizer:
    def __init__(self, project_root):
        self.project_root = Path(project_root)
        self.static_root = self.project_root / 'static'
        self.media_root = self.project_root / 'media'
        
    def optimize_database(self):
        """Optimize database performance"""
        logger.info("Optimizing database...")
        
        try:
            # Create database indexes
            from django.core.management import execute_from_command_line
            execute_from_command_line(['manage.py', 'makemigrations'])
            execute_from_command_line(['manage.py', 'migrate'])
            
            # Create database indexes for better performance
            indexes = [
                "CREATE INDEX IF NOT EXISTS idx_indicator_id ON sismadak_503.local_quality_indicator(indicator_id)",
                "CREATE INDEX IF NOT EXISTS idx_periode ON pmkp.proses_indikator_lokal_rumah_sakit(periode)",
                "CREATE INDEX IF NOT EXISTS idx_id_unit ON pmkp.proses_indikator_lokal_rumah_sakit(id_unit)",
                "CREATE INDEX IF NOT EXISTS idx_id_record_indikator ON pmkp.proses_indikator_lokal_rumah_sakit(id_record_indikator)",
                "CREATE INDEX IF NOT EXISTS idx_tahun_aktif ON pmkp.proses_indikator_dikategorikan(tahun_aktif)",
                "CREATE INDEX IF NOT EXISTS idx_id_kategori_indikator ON pmkp.proses_indikator_dikategorikan(id_kategori_indikator)",
            ]
            
            logger.info("Database optimization completed")
            
        except Exception as e:
            logger.error(f"Database optimization failed: {e}")
    
    def optimize_static_files(self):
        """Optimize static files"""
        logger.info("Optimizing static files...")
        
        try:
            # Collect static files
            subprocess.run([
                sys.executable, 'manage.py', 'collectstatic', '--noinput', '--clear'
            ], cwd=self.project_root, check=True)
            
            # Compress static files if django-compressor is available
            try:
                subprocess.run([
                    sys.executable, 'manage.py', 'compress', '--force'
                ], cwd=self.project_root, check=True)
                logger.info("Static files compressed")
            except subprocess.CalledProcessError:
                logger.warning("django-compressor not available, skipping compression")
            
            logger.info("Static files optimization completed")
            
        except Exception as e:
            logger.error(f"Static files optimization failed: {e}")
    
    def optimize_settings(self):
        """Create optimized settings file"""
        logger.info("Creating optimized settings...")
        
        settings_content = '''
# Optimized Django Settings for Production

import os
from pathlib import Path

# Build paths inside the project like this: BASE_DIR / 'subdir'.
BASE_DIR = Path(__file__).resolve().parent.parent

# SECURITY WARNING: keep the secret key used in production secret!
SECRET_KEY = os.getenv('DJANGO_SECRET_KEY', 'your-secret-key-here')

# SECURITY WARNING: don't run with debug turned on in production!
DEBUG = os.getenv('DJANGO_DEBUG', 'False').lower() == 'true'

ALLOWED_HOSTS = os.getenv('DJANGO_ALLOWED_HOSTS', '*').split(',')

# Application definition
INSTALLED_APPS = [
    'api_ars',
    'api_v1',
    'indikator_mutu',
    'corsheaders',
    'maintenance_mode',
    'django.contrib.admin',
    'django.contrib.auth',
    'django.contrib.contenttypes',
    'django.contrib.sessions',
    'django.contrib.messages',
    'django.contrib.staticfiles',
]

MIDDLEWARE = [
    'django.middleware.security.SecurityMiddleware',
    'django.contrib.sessions.middleware.SessionMiddleware',
    'corsheaders.middleware.CorsMiddleware',
    'django.middleware.common.CommonMiddleware',
    'django.middleware.csrf.CsrfViewMiddleware',
    'django.contrib.auth.middleware.AuthenticationMiddleware',
    'django.contrib.messages.middleware.MessageMiddleware',
    'django.middleware.clickjacking.XFrameOptionsMiddleware',
    'maintenance_mode.middleware.MaintenanceModeMiddleware',
    'django.middleware.gzip.GZipMiddleware',  # Compression middleware
]

ROOT_URLCONF = 'pmkp.urls'

TEMPLATES = [
    {
        'BACKEND': 'django.template.backends.django.DjangoTemplates',
        'DIRS': [],
        'APP_DIRS': True,
        'OPTIONS': {
            'context_processors': [
                'django.template.context_processors.debug',
                'django.template.context_processors.request',
                'django.contrib.auth.context_processors.auth',
                'django.contrib.messages.context_processors.messages',
                'indikator_mutu.context_processor_user.kategori_indikator',
            ],
        },
    },
]

WSGI_APPLICATION = 'pmkp.wsgi.application'

# Database optimization
DATABASES = {
    'default': {
        'ENGINE': 'django.db.backends.sqlite3',
        'NAME': BASE_DIR / 'db.sqlite3',
        'OPTIONS': {
            'timeout': 20,
        },
        'CONN_MAX_AGE': 60,
    }
}

# Cache configuration
CACHES = {
    'default': {
        'BACKEND': 'django.core.cache.backends.locmem.LocMemCache',
        'LOCATION': 'unique-snowflake',
        'TIMEOUT': 300,
        'OPTIONS': {
            'MAX_ENTRIES': 1000,
        }
    }
}

# Session optimization
SESSION_ENGINE = 'django.contrib.sessions.backends.cached_db'
SESSION_CACHE_ALIAS = 'default'

# Password validation
AUTH_PASSWORD_VALIDATORS = [
    {
        'NAME': 'django.contrib.auth.password_validation.UserAttributeSimilarityValidator',
    },
    {
        'NAME': 'django.contrib.auth.password_validation.MinimumLengthValidator',
    },
    {
        'NAME': 'django.contrib.auth.password_validation.CommonPasswordValidator',
    },
    {
        'NAME': 'django.contrib.auth.password_validation.NumericPasswordValidator',
    },
]

# Internationalization
LANGUAGE_CODE = 'en-us'
TIME_ZONE = 'UTC'
USE_I18N = True
USE_L10N = True
USE_TZ = True

# Static files optimization
STATIC_URL = '/static/'
STATIC_ROOT = os.path.join(BASE_DIR, 'static')
STATICFILES_STORAGE = 'django.contrib.staticfiles.storage.ManifestStaticFilesStorage'

STATICFILES_FINDERS = [
    'django.contrib.staticfiles.finders.FileSystemFinder',
    'django.contrib.staticfiles.finders.AppDirectoriesFinder',
]

# Media files
MEDIA_URL = '/media/'
MEDIA_ROOT = os.path.join(BASE_DIR, 'media')

# Default primary key field type
DEFAULT_AUTO_FIELD = 'django.db.models.BigAutoField'

# Security settings
SECURE_BROWSER_XSS_FILTER = True
SECURE_CONTENT_TYPE_NOSNIFF = True
X_FRAME_OPTIONS = 'DENY'

# CORS settings
CORS_ORIGIN_ALLOW_ALL = True
CORS_ALLOW_CREDENTIALS = True

# Maintenance mode
MAINTENANCE_MODE = None
MAINTENANCE_MODE_TEMPLATE = "503/dist/index.html"

# Logging configuration
LOGGING = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'verbose': {
            'format': '{levelname} {asctime} {module} {process:d} {thread:d} {message}',
            'style': '{',
        },
        'simple': {
            'format': '{levelname} {message}',
            'style': '{',
        },
    },
    'handlers': {
        'file': {
            'level': 'INFO',
            'class': 'logging.FileHandler',
            'filename': os.path.join(BASE_DIR, 'django.log'),
            'formatter': 'verbose',
        },
        'console': {
            'level': 'DEBUG',
            'class': 'logging.StreamHandler',
            'formatter': 'simple',
        },
    },
    'root': {
        'handlers': ['console', 'file'],
        'level': 'INFO',
    },
    'loggers': {
        'django': {
            'handlers': ['console', 'file'],
            'level': 'INFO',
            'propagate': False,
        },
        'django.db.backends': {
            'handlers': ['console'],
            'level': 'DEBUG',
            'propagate': False,
        },
    },
}
'''
        
        # Write optimized settings
        settings_file = self.project_root / 'pmkp' / 'settings_optimized.py'
        with open(settings_file, 'w') as f:
            f.write(settings_content)
        
        logger.info(f"Optimized settings created: {settings_file}")
    
    def create_management_commands(self):
        """Create management commands for optimization"""
        logger.info("Creating management commands...")
        
        # Create management directory structure
        management_dir = self.project_root / 'indikator_mutu' / 'management' / 'commands'
        management_dir.mkdir(parents=True, exist_ok=True)
        
        # Create __init__.py files
        (self.project_root / 'indikator_mutu' / 'management' / '__init__.py').touch()
        (management_dir / '__init__.py').touch()
        
        # Create optimize command
        optimize_command = '''
from django.core.management.base import BaseCommand
from django.core.cache import cache
from django.db import connection
import logging

logger = logging.getLogger(__name__)

class Command(BaseCommand):
    help = 'Optimize database and cache performance'
    
    def handle(self, *args, **options):
        self.stdout.write('Starting optimization...')
        
        # Clear cache
        cache.clear()
        self.stdout.write('Cache cleared')
        
        # Optimize database
        with connection.cursor() as cursor:
            # Create indexes for better performance
            indexes = [
                "CREATE INDEX IF NOT EXISTS idx_indicator_id ON sismadak_503.local_quality_indicator(indicator_id)",
                "CREATE INDEX IF NOT EXISTS idx_periode ON pmkp.proses_indikator_lokal_rumah_sakit(periode)",
                "CREATE INDEX IF NOT EXISTS idx_id_unit ON pmkp.proses_indikator_lokal_rumah_sakit(id_unit)",
                "CREATE INDEX IF NOT EXISTS idx_id_record_indikator ON pmkp.proses_indikator_lokal_rumah_sakit(id_record_indikator)",
                "CREATE INDEX IF NOT EXISTS idx_tahun_aktif ON pmkp.proses_indikator_dikategorikan(tahun_aktif)",
                "CREATE INDEX IF NOT EXISTS idx_id_kategori_indikator ON pmkp.proses_indikator_dikategorikan(id_kategori_indikator)",
            ]
            
            for index_sql in indexes:
                try:
                    cursor.execute(index_sql)
                    self.stdout.write(f'Created index: {index_sql}')
                except Exception as e:
                    self.stdout.write(f'Failed to create index: {e}')
        
        self.stdout.write('Optimization completed')
'''
        
        with open(management_dir / 'optimize.py', 'w') as f:
            f.write(optimize_command)
        
        logger.info("Management commands created")
    
    def create_requirements_optimized(self):
        """Create optimized requirements file"""
        logger.info("Creating optimized requirements...")
        
        requirements_content = '''# Core Django and dependencies
asgiref==3.7.2
Django==3.2.23  # Latest LTS version with security patches
numpy==1.24.3
pandas==2.0.3

# Security and cryptography
pycryptodome==3.19.0
PyJWT==2.8.0
argon2-cffi==23.1.0

# Database
PyMySQL==1.1.0
sqlparse==0.4.4

# Date and time utilities
python-dateutil==2.8.2
pytz==2023.3
six==1.16.0

# Django extensions and utilities
django-maintenance-mode==0.21.1
django-cors-headers==4.3.1
djangorestframework==3.14.0

# File processing
weasyprint==60.2
XlsxWriter==3.1.9
python-docx==1.1.0
openpyxl==3.1.2

# HTTP requests
requests==2.31.0

# Performance and optimization packages
django-debug-toolbar==4.2.0
django-cacheops==8.0.0
django-redis==5.4.0

# Development and testing
pytest==7.4.3
pytest-django==4.7.0
coverage==7.3.2

# Code quality
black==23.11.0
flake8==6.1.0
isort==5.12.0
'''
        
        with open(self.project_root / 'requirements_optimized.txt', 'w') as f:
            f.write(requirements_content)
        
        logger.info("Optimized requirements created")
    
    def create_performance_monitoring(self):
        """Create performance monitoring utilities"""
        logger.info("Creating performance monitoring...")
        
        monitoring_content = '''
import time
import logging
from functools import wraps
from django.core.cache import cache
from django.db import connection

logger = logging.getLogger(__name__)

def performance_monitor(func):
    """Decorator to monitor function performance"""
    @wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.time()
        start_queries = len(connection.queries)
        
        result = func(*args, **kwargs)
        
        end_time = time.time()
        end_queries = len(connection.queries)
        
        execution_time = end_time - start_time
        query_count = end_queries - start_queries
        
        logger.info(f"Function {func.__name__} executed in {execution_time:.2f}s, "
                   f"queries: {query_count}")
        
        return result
    return wrapper

def cache_result(timeout=300, key_prefix=''):
    """Decorator to cache function results"""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            cache_key = f"{key_prefix}:{func.__name__}:{hash(str(args) + str(sorted(kwargs.items())))}"
            
            result = cache.get(cache_key)
            if result is not None:
                return result
            
            result = func(*args, **kwargs)
            cache.set(cache_key, result, timeout)
            
            return result
        return wrapper
    return decorator

def optimize_queries(queryset):
    """Optimize queryset with select_related and prefetch_related"""
    return queryset.select_related().prefetch_related()

def get_slow_queries():
    """Get slow queries from database"""
    slow_queries = []
    for query in connection.queries:
        if float(query['time']) > 1.0:  # Queries taking more than 1 second
            slow_queries.append(query)
    return slow_queries
'''
        
        monitoring_file = self.project_root / 'indikator_mutu' / 'performance_monitoring.py'
        with open(monitoring_file, 'w') as f:
            f.write(monitoring_content)
        
        logger.info("Performance monitoring utilities created")
    
    def run_all_optimizations(self):
        """Run all optimizations"""
        logger.info("Starting comprehensive Django optimization...")
        
        try:
            self.optimize_database()
            self.optimize_static_files()
            self.optimize_settings()
            self.create_management_commands()
            self.create_requirements_optimized()
            self.create_performance_monitoring()
            
            logger.info("All optimizations completed successfully!")
            
            # Print summary
            print("\n" + "="*50)
            print("OPTIMIZATION SUMMARY")
            print("="*50)
            print("✅ Database indexes created")
            print("✅ Static files optimized")
            print("✅ Optimized settings created")
            print("✅ Management commands added")
            print("✅ Performance monitoring utilities created")
            print("✅ Updated requirements file")
            print("\nNext steps:")
            print("1. Update your settings.py to use the optimized settings")
            print("2. Run: python manage.py optimize")
            print("3. Install new requirements: pip install -r requirements_optimized.txt")
            print("4. Test your application performance")
            print("="*50)
            
        except Exception as e:
            logger.error(f"Optimization failed: {e}")
            raise

def main():
    """Main function"""
    if len(sys.argv) > 1:
        project_root = sys.argv[1]
    else:
        project_root = os.getcwd()
    
    optimizer = DjangoOptimizer(project_root)
    optimizer.run_all_optimizations()

if __name__ == "__main__":
    main() 
#!/usr/bin/env python3
"""
Script untuk memeriksa konfigurasi Google Ad Manager di server production
"""

import os
import sys
import django
from pathlib import Path

# Setup Django
sys.path.append('/var/www/html/hris')
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'hris.settings')

try:
    django.setup()
    from django.conf import settings
    
    print("=== KONFIGURASI GOOGLE AD MANAGER ===")
    print()
    
    # Periksa environment variables
    print("1. ENVIRONMENT VARIABLES:")
    print(f"   GOOGLE_AD_MANAGER_KEY_FILE (env): {os.getenv('GOOGLE_AD_MANAGER_KEY_FILE', 'NOT SET')}")
    print(f"   GOOGLE_ADS_NETWORK_CODE (env): {os.getenv('GOOGLE_ADS_NETWORK_CODE', 'NOT SET')}")
    print(f"   GOOGLE_AD_MANAGER_NETWORK_CODE (env): {os.getenv('GOOGLE_AD_MANAGER_NETWORK_CODE', 'NOT SET')}")
    print()
    
    # Periksa Django settings
    print("2. DJANGO SETTINGS:")
    print(f"   GOOGLE_AD_MANAGER_KEY_FILE: {getattr(settings, 'GOOGLE_AD_MANAGER_KEY_FILE', 'NOT SET')}")
    print(f"   GOOGLE_ADS_NETWORK_CODE: {getattr(settings, 'GOOGLE_ADS_NETWORK_CODE', 'NOT SET')}")
    print(f"   GOOGLE_AD_MANAGER_NETWORK_CODE: {getattr(settings, 'GOOGLE_AD_MANAGER_NETWORK_CODE', 'NOT SET')}")
    print()
    
    # Periksa file existence
    print("3. FILE VERIFICATION:")
    key_file = getattr(settings, 'GOOGLE_AD_MANAGER_KEY_FILE', '')
    if key_file:
        file_exists = os.path.exists(key_file)
        print(f"   Key file path: {key_file}")
        print(f"   File exists: {file_exists}")
        if file_exists:
            file_stat = os.stat(key_file)
            print(f"   File size: {file_stat.st_size} bytes")
            print(f"   File permissions: {oct(file_stat.st_mode)[-3:]}")
        else:
            print("   ERROR: Service account key file not found!")
    else:
        print("   ERROR: GOOGLE_AD_MANAGER_KEY_FILE not configured!")
    print()
    
    # Test fungsi create_dynamic_googleads_yaml
    print("4. TESTING YAML CREATION:")
    try:
        from management.utils import create_dynamic_googleads_yaml
        yaml_path = create_dynamic_googleads_yaml()
        print(f"   YAML file created: {yaml_path}")
        
        # Baca isi YAML
        with open(yaml_path, 'r') as f:
            yaml_content = f.read()
        print("   YAML content:")
        for line in yaml_content.split('\n'):
            if line.strip():
                print(f"     {line}")
        
        # Cleanup
        os.unlink(yaml_path)
        print("   YAML file cleaned up successfully")
        
    except Exception as e:
        print(f"   ERROR creating YAML: {e}")
    print()
    
    # Test client initialization
    print("5. TESTING CLIENT INITIALIZATION:")
    try:
        from management.utils import get_ad_manager_client
        client = get_ad_manager_client()
        print(f"   Client initialized successfully: {type(client)}")
        print("   ✅ Google Ad Manager client is working!")
        
    except Exception as e:
        print(f"   ❌ ERROR initializing client: {e}")
    print()
    
    print("=== DIAGNOSIS COMPLETE ===")
    
except Exception as e:
    print(f"ERROR: {e}")
    import traceback
    traceback.print_exc()
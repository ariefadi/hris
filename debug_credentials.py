#!/usr/bin/env python3
"""
Debug Google Ad Manager credentials and permissions
"""

import os
import sys
import django
from pathlib import Path
import json

# Add the project directory to Python path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

# Setup Django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'hris.settings')
django.setup()

def debug_credentials():
    """Debug Google Ad Manager credentials step by step"""
    
    print("[DEBUG] Checking environment variables...")
    required_vars = [
        'GOOGLE_ADS_DEVELOPER_TOKEN',
        'GOOGLE_ADS_CLIENT_ID', 
        'GOOGLE_ADS_CLIENT_SECRET',
        'GOOGLE_ADS_REFRESH_TOKEN',
        'GOOGLE_AD_MANAGER_NETWORK_CODE',
        'GOOGLE_AD_MANAGER_KEY_FILE'
    ]
    
    for var in required_vars:
        value = os.getenv(var)
        if value:
            if 'TOKEN' in var or 'SECRET' in var:
                print(f"[DEBUG] {var}: {'*' * 20}...{value[-10:]}")
            else:
                print(f"[DEBUG] {var}: {value}")
        else:
            print(f"[ERROR] {var}: NOT SET")
    
    print("\n[DEBUG] Checking service account key file...")
    key_file = os.getenv('GOOGLE_AD_MANAGER_KEY_FILE')
    if key_file and os.path.exists(key_file):
        try:
            with open(key_file, 'r') as f:
                key_data = json.load(f)
            print(f"[DEBUG] Service account key file exists")
            print(f"[DEBUG] Service account email: {key_data.get('client_email', 'NOT FOUND')}")
            print(f"[DEBUG] Project ID: {key_data.get('project_id', 'NOT FOUND')}")
        except Exception as e:
            print(f"[ERROR] Failed to read service account key: {e}")
    else:
        print(f"[ERROR] Service account key file not found: {key_file}")
    
    print("\n[DEBUG] Testing OAuth2 credentials...")
    try:
        from google.oauth2.credentials import Credentials
        from google.auth.transport.requests import Request
        
        # Create OAuth2 credentials
        creds = Credentials(
            token=None,
            refresh_token=os.getenv('GOOGLE_ADS_REFRESH_TOKEN'),
            token_uri='https://oauth2.googleapis.com/token',
            client_id=os.getenv('GOOGLE_ADS_CLIENT_ID'),
            client_secret=os.getenv('GOOGLE_ADS_CLIENT_SECRET')
        )
        
        # Try to refresh the token
        print(f"[DEBUG] Attempting to refresh OAuth2 token...")
        creds.refresh(Request())
        print(f"[DEBUG] OAuth2 token refreshed successfully")
        print(f"[DEBUG] Access token: {'*' * 20}...{creds.token[-10:] if creds.token else 'NONE'}")
        
    except Exception as e:
        print(f"[ERROR] OAuth2 credential test failed: {e}")
    
    print("\n[DEBUG] Testing Google Ad Manager API access...")
    try:
        # Import and apply patches
        from management.googleads_patch_v2 import apply_googleads_patches
        apply_googleads_patches()
        
        from management.utils import get_ad_manager_client
        
        print(f"[DEBUG] Creating Ad Manager client...")
        client = get_ad_manager_client()
        if not client:
            print(f"[ERROR] Failed to create Ad Manager client")
            return
        
        print(f"[DEBUG] Getting NetworkService...")
        network_service = client.GetService('NetworkService', version='v202408')
        
        print(f"[DEBUG] Attempting getCurrentNetwork (this will likely fail)...")
        try:
            network = network_service.getCurrentNetwork()
            print(f"[SUCCESS] Network retrieved: {network.displayName} ({network.networkCode})")
        except Exception as e:
            print(f"[ERROR] getCurrentNetwork failed: {e}")
            
            # Check if it's an authentication error
            error_str = str(e).lower()
            if 'authentication' in error_str:
                print(f"[DIAGNOSIS] This appears to be an authentication error")
                print(f"[SUGGESTION] Check if:")
                print(f"  1. OAuth2 refresh token is valid and not expired")
                print(f"  2. Client ID and secret are correct")
                print(f"  3. Developer token is valid for Ad Manager API")
            elif 'permission' in error_str:
                print(f"[DIAGNOSIS] This appears to be a permission error")
                print(f"[SUGGESTION] Check if:")
                print(f"  1. The user has access to the specified network code")
                print(f"  2. The network code {os.getenv('GOOGLE_AD_MANAGER_NETWORK_CODE')} is correct")
                print(f"  3. The user has appropriate permissions in Google Ad Manager")
            else:
                print(f"[DIAGNOSIS] Unknown error type")
        
    except Exception as e:
        print(f"[ERROR] Ad Manager API test failed: {e}")

if __name__ == "__main__":
    debug_credentials()
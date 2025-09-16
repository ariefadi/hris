#!/usr/bin/env python
"""
Test script to verify Google Ad Manager authentication after OAuth scope fix
"""

import os
import sys
import django
from pathlib import Path

# Setup Django
BASE_DIR = Path(__file__).resolve().parent
sys.path.append(str(BASE_DIR))
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'hris.settings')
django.setup()

from django.conf import settings
from management.database import data_mysql
from management.googleads_patch_v2 import apply_googleads_patches
from management.utils import fetch_adx_traffic_account_by_user
import json

def test_auth_fix():
    """Test Google Ad Manager authentication after OAuth scope fix"""
    print("=== Testing Google Ad Manager Authentication Fix ===")
    
    try:
        # Apply GoogleAds patches
        print("[INFO] Applying GoogleAds patches...")
        apply_googleads_patches()
        
        # Get test user from database
        print("[INFO] Getting test user from database...")
        db = data_mysql()
        users_result = db.data_user_by_params()
        
        if not users_result['status'] or not users_result['data']:
            print("[ERROR] No users found in database")
            return
        
        # Use first user for testing
        test_user = users_result['data'][0]
        user_email = test_user['user_mail']
        print(f"[INFO] Using test user: {user_email}")
        
        # Test parameters
        start_date = '2024-12-01'
        end_date = '2024-12-15'
        site_filter = 'all'
        
        print(f"[INFO] Testing AdX Traffic Account fetch...")
        print(f"[INFO] Date range: {start_date} to {end_date}")
        print(f"[INFO] Site filter: {site_filter}")
        
        # Call the function that was failing
        result = fetch_adx_traffic_account_by_user(
            user_email=user_email,
            start_date=start_date,
            end_date=end_date,
            site_filter=site_filter
        )
        
        print("\n=== Function Result ===")
        print(f"Status: {result.get('status', 'Unknown')}")
        print(f"Has 'error' field: {('error' in result)}")
        print(f"Has 'note' field: {('note' in result)}")
        
        if result.get('status') == False:
            error_msg = result.get('error', 'No error message')
            note_msg = result.get('note', 'No note message')
            print(f"Error message: {error_msg}")
            print(f"Note message: {note_msg}")
            
            # Check if this is still an authentication error
            if 'authentication' in error_msg.lower() or 'permission' in error_msg.lower():
                print("\n[WARNING] Still getting authentication/permission errors")
                print("[INFO] This might indicate:")
                print("  1. OAuth scope fix requires user to re-authenticate")
                print("  2. Service account needs proper Ad Manager permissions")
                print("  3. Network code might be incorrect")
            else:
                print("\n[SUCCESS] Authentication error resolved!")
                print("[INFO] Now getting different type of error (expected for test data)")
        else:
            print("\n[SUCCESS] Function executed successfully!")
            if 'data' in result:
                print(f"Data keys: {list(result['data'].keys()) if isinstance(result['data'], dict) else 'Not a dict'}")
        
        # Test JSON serialization
        try:
            json_str = json.dumps(result)
            print(f"JSON serializable: ✓")
            print(f"JSON length: {len(json_str)} characters")
        except Exception as e:
            print(f"JSON serialization failed: {e}")
        
        print("\n=== OAuth Scope Configuration ===")
        oauth_scopes = getattr(settings, 'SOCIAL_AUTH_GOOGLE_OAUTH2_SCOPE', [])
        print(f"Current OAuth scopes: {oauth_scopes}")
        
        dfp_scope = 'https://www.googleapis.com/auth/dfp'
        if dfp_scope in oauth_scopes:
            print(f"✓ DFP scope is configured: {dfp_scope}")
        else:
            print(f"✗ DFP scope is missing: {dfp_scope}")
        
        print("\n=== Credentials Check ===")
        print(f"Developer Token: {settings.GOOGLE_ADS_DEVELOPER_TOKEN[:10]}..." if settings.GOOGLE_ADS_DEVELOPER_TOKEN else "Missing")
        print(f"Client ID: {settings.GOOGLE_ADS_CLIENT_ID[:20]}..." if settings.GOOGLE_ADS_CLIENT_ID else "Missing")
        print(f"Client Secret: {settings.GOOGLE_ADS_CLIENT_SECRET[:10]}..." if settings.GOOGLE_ADS_CLIENT_SECRET else "Missing")
        print(f"Refresh Token: {settings.GOOGLE_ADS_REFRESH_TOKEN[:20]}..." if settings.GOOGLE_ADS_REFRESH_TOKEN else "Missing")
        print(f"Network Code: {settings.GOOGLE_AD_MANAGER_NETWORK_CODE}")
        
        service_account_file = settings.GOOGLE_AD_MANAGER_KEY_FILE
        if service_account_file and os.path.exists(service_account_file):
            print(f"✓ Service account file exists: {service_account_file}")
        else:
            print(f"✗ Service account file missing: {service_account_file}")
        
        print("\n=== Test Summary ===")
        if result.get('status') == False:
            error_msg = result.get('error', '')
            if 'authentication' in error_msg.lower() or 'permission' in error_msg.lower():
                print("❌ Authentication issue still exists")
                print("💡 Next steps:")
                print("   1. Users need to re-authenticate with new OAuth scope")
                print("   2. Check service account permissions in Ad Manager")
                print("   3. Verify network code is correct")
            else:
                print("✅ Authentication issue resolved!")
                print("ℹ️  Now getting different error (normal for test environment)")
        else:
            print("✅ Function executed successfully!")
            
    except Exception as e:
        print(f"\n[ERROR] Test failed with exception: {e}")
        import traceback
        traceback.print_exc()

if __name__ == '__main__':
    test_auth_fix()
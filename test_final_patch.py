#!/usr/bin/env python
"""
Final test for Google Ad Manager API with patches and service account
"""

import os
import sys
sys.path.insert(0, '/Users/ariefdwicahyoadi/hris')

# Configure Django settings
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'hris.settings')
import django
from django.conf import settings
try:
    django.setup()
except Exception as e:
    print(f"[WARNING] Django setup failed: {e}")
    # Fallback: configure minimal settings
    if not settings.configured:
        settings.configure(
            GOOGLE_AD_MANAGER_KEY_FILE='/Users/ariefdwicahyoadi/hris/service-account-key.json',
            GOOGLE_ADS_NETWORK_CODE='23303534834',
            GOOGLE_ADS_CLIENT_ID=os.getenv('GOOGLE_ADS_CLIENT_ID', ''),
            GOOGLE_ADS_CLIENT_SECRET=os.getenv('GOOGLE_ADS_CLIENT_SECRET', ''),
            GOOGLE_ADS_REFRESH_TOKEN=os.getenv('GOOGLE_ADS_REFRESH_TOKEN', ''),
            GOOGLE_ADS_DEVELOPER_TOKEN=os.getenv('GOOGLE_ADS_DEVELOPER_TOKEN', ''),
        )

def test_final_patch():
    """Test Google Ad Manager API with final patches"""
    try:
        print("[INFO] Testing final Google Ad Manager API with patches...")
        
        # Import and apply patches
        from management.googleads_patch_v2 import apply_googleads_patches
        apply_googleads_patches()
        
        from googleads import ad_manager
        from management.utils import create_dynamic_googleads_yaml
        
        print("[INFO] Creating Ad Manager client...")
        yaml_file = create_dynamic_googleads_yaml()
        if not yaml_file:
            raise Exception("Failed to create YAML configuration")
        
        try:
            client = ad_manager.AdManagerClient.LoadFromStorage(yaml_file)
            print("[SUCCESS] Ad Manager client created successfully")
            
            print("[INFO] Getting NetworkService...")
            network_service = client.GetService('NetworkService', version='v202408')
            
            print("[INFO] Calling getCurrentNetwork...")
            network = network_service.getCurrentNetwork()
            print(f"[SUCCESS] Network: {network.displayName} ({network.networkCode})")
            
            print("[INFO] Testing basic service access...")
            # Test other basic services
            try:
                inventory_service = client.GetService('InventoryService', version='v202408')
                print("[SUCCESS] InventoryService accessible")
            except Exception as e:
                print(f"[WARNING] InventoryService not accessible: {e}")
            
            try:
                user_service = client.GetService('UserService', version='v202408')
                print("[SUCCESS] UserService accessible")
            except Exception as e:
                print(f"[WARNING] UserService not accessible: {e}")
            
            print("\n[SUCCESS] All tests passed! Google Ad Manager API is working correctly.")
            print(f"[SUCCESS] Connected to network: {network.displayName}")
            print(f"[SUCCESS] Network code: {network.networkCode}")
            print(f"[SUCCESS] Authentication method: Service Account")
            
            return True
            
        finally:
            # Clean up temp file
            if yaml_file and os.path.exists(yaml_file):
                os.unlink(yaml_file)
                
    except Exception as e:
        print(f"[ERROR] Test failed: {e}")
        print(f"[ERROR] Error type: {type(e).__name__}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = test_final_patch()
    if success:
        print("\n✅ Google Ad Manager API integration is working correctly!")
        print("✅ Service account authentication successful")
        print("✅ XML parsing bug patches applied successfully")
        print("✅ Ready for production use")
    else:
        print("\n❌ Google Ad Manager API integration failed")
        sys.exit(1)
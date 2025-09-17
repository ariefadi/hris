#!/usr/bin/env python
import os
import sys
import django

# Setup Django environment
sys.path.append('/Users/ariefdwicahyoadi/hris')
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'hris.settings')
django.setup()

# Import after Django setup
from management.utils import fetch_adx_traffic_account_by_user
from management.googleads_patch_v2 import apply_googleads_patches

def test_adx_data_fetch():
    """Test function to fetch AdX data directly"""
    print("[TEST] Starting AdX data fetch test...")
    
    # Apply patches
    apply_googleads_patches()
    
    # Test parameters
    user_email = "adiarief463@gmail.com"
    start_date = "2025-08-22"
    end_date = "2025-08-29"
    site_filter = None
    
    try:
        print(f"[TEST] Calling fetch_adx_traffic_account_by_user with:")
        print(f"  - user_email: {user_email}")
        print(f"  - start_date: {start_date}")
        print(f"  - end_date: {end_date}")
        print(f"  - site_filter: {site_filter}")
        
        result = fetch_adx_traffic_account_by_user(
            user_email, 
            start_date, 
            end_date, 
            site_filter
        )
        
        print(f"[TEST] Result: {result}")
        
        # Check if we got real data
        if result.get('status') and result.get('data'):
            print(f"[TEST] Success! Got {len(result['data'])} records")
            if result['data']:
                print(f"[TEST] Sample record: {result['data'][0]}")
        else:
            print("[TEST] No data returned or error occurred")
            
        # Test simpler API call to see if basic connectivity works
        print("\n[TEST] Testing basic Ad Manager connectivity...")
        try:
            from management.utils import get_user_ad_manager_client
            client_result = get_user_ad_manager_client(user_email)
            if client_result['status']:
                client = client_result['client']
                print(f"[TEST] Client created successfully")
                
                # Try to get network service (simpler than report service)
                try:
                    network_service = client.GetService('NetworkService')
                    print(f"[TEST] NetworkService obtained successfully")
                    
                    # Try to get current network (simplest call)
                    try:
                        current_network = network_service.getCurrentNetwork()
                        print(f"[TEST] getCurrentNetwork successful: {current_network.get('displayName', 'Unknown')}")
                    except Exception as e:
                        print(f"[TEST] getCurrentNetwork failed: {e}")
                        
                except Exception as e:
                    print(f"[TEST] Failed to get NetworkService: {e}")
            else:
                print(f"[TEST] Failed to create client: {client_result.get('error', 'Unknown error')}")
        except Exception as e:
            print(f"[TEST] Basic connectivity test failed: {e}")
            
    except Exception as e:
        print(f"[TEST] Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_adx_data_fetch()
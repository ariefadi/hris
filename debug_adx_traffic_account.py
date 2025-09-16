#!/usr/bin/env python3
"""
Script untuk men-debug error "Unknown error occurred" di menu AdX Traffic Account
"""

import os
import sys
import django
from datetime import datetime, timedelta

# Setup Django
sys.path.append('/Users/ariefdwicahyoadi/hris')
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'hris.settings')
django.setup()

from management.database import data_mysql
from management.utils import fetch_adx_traffic_account_by_user
from management.googleads_patch_v2 import apply_googleads_patches

def debug_adx_traffic_account():
    print("=== Debugging AdX Traffic Account Error ===\n")
    
    # Apply patches first
    print("1. Applying GoogleAds patches...")
    try:
        apply_googleads_patches()
        print("✓ Patches applied successfully")
    except Exception as e:
        print(f"✗ Error applying patches: {e}")
        return
    
    # Get a test user from database
    print("\n2. Getting test user from database...")
    try:
        users_result = data_mysql().data_user_by_params()
        if not users_result['status'] or not users_result['data']:
            print("✗ No users found in database")
            return
        
        # Find first user with email
        test_user = None
        for user in users_result['data']:
            if user.get('user_mail'):
                test_user = user
                break
        
        if not test_user:
            print("✗ No user with email found")
            return
        
        user_email = test_user['user_mail']
        user_id = test_user['user_id']
        print(f"✓ Using test user: {user_email} (ID: {user_id})")
        
    except Exception as e:
        print(f"✗ Error getting user data: {e}")
        return
    
    # Test date range (last 7 days)
    end_date = datetime.now().date()
    start_date = end_date - timedelta(days=7)
    
    print(f"\n3. Testing AdX traffic account data fetch...")
    print(f"   Date range: {start_date} to {end_date}")
    print(f"   User email: {user_email}")
    
    try:
        # Test the exact same function called by the view
        result = fetch_adx_traffic_account_by_user(
            user_email=user_email,
            start_date=start_date.strftime('%Y-%m-%d'),
            end_date=end_date.strftime('%Y-%m-%d'),
            site_filter=None
        )
        
        print(f"\n4. Results Analysis:")
        print(f"   Status: {result.get('status', 'Unknown')}")
        print(f"   API Method: {result.get('api_method', 'Unknown')}")
        
        if result.get('status'):
            print(f"   ✓ Success! Data retrieved")
            print(f"   Data keys: {list(result.keys())}")
            
            if 'data' in result:
                data = result['data']
                print(f"   Number of records: {len(data) if isinstance(data, list) else 'N/A'}")
                
                if isinstance(data, list) and len(data) > 0:
                    print(f"   Sample record: {data[0]}")
                elif isinstance(data, list) and len(data) == 0:
                    print(f"   ⚠ No data records found (empty list)")
                else:
                    print(f"   Data type: {type(data)}")
            
            if 'summary' in result:
                summary = result['summary']
                print(f"   Summary: {summary}")
            
            if 'note' in result:
                print(f"   Note: {result['note']}")
                
        else:
            print(f"   ✗ Failed to fetch data")
            error_msg = result.get('error', result.get('note', 'Unknown error'))
            print(f"   Error: {error_msg}")
            print(f"   Method: {result.get('api_method', 'Unknown method')}")
            
            if 'details' in result:
                print(f"   Details: {result['details']}")
            
            # Check for specific error patterns
            if 'credentials' in error_msg.lower():
                print(f"   → Credential issue detected")
            elif 'client' in error_msg.lower():
                print(f"   → Ad Manager client issue detected")
            elif 'permission' in error_msg.lower() or 'unauthorized' in error_msg.lower():
                print(f"   → Permission/authorization issue detected")
            elif 'scope' in error_msg.lower():
                print(f"   → OAuth scope issue detected")
            elif 'network' in error_msg.lower():
                print(f"   → Network connectivity issue detected")
            else:
                print(f"   → Unknown error pattern")
    
    except Exception as e:
        print(f"   ✗ Exception occurred: {e}")
        import traceback
        print(f"   Traceback: {traceback.format_exc()}")
    
    print(f"\n5. Simulating View Response...")
    
    # Simulate what the view would return
    try:
        # This mimics the exact logic in AdxTrafficPerAccountDataView
        from django.http import JsonResponse
        import json
        
        # Test with the same parameters that would come from the frontend
        test_params = {
            'start_date': start_date.strftime('%Y-%m-%d'),
            'end_date': end_date.strftime('%Y-%m-%d'),
            'site_filter': ''
        }
        
        print(f"   Test parameters: {test_params}")
        
        # Format dates like the view does
        start_date_formatted = datetime.strptime(test_params['start_date'], '%Y-%m-%d').strftime('%Y-%m-%d')
        end_date_formatted = datetime.strptime(test_params['end_date'], '%Y-%m-%d').strftime('%Y-%m-%d')
        
        # Filter value like the view does
        filter_value = test_params['site_filter'] if test_params['site_filter'] and test_params['site_filter'] != '%' else None
        
        print(f"   Formatted start_date: {start_date_formatted}")
        print(f"   Formatted end_date: {end_date_formatted}")
        print(f"   Filter value: {filter_value}")
        
        # Call the function exactly like the view does
        view_result = fetch_adx_traffic_account_by_user(
            user_email, 
            start_date_formatted, 
            end_date_formatted, 
            filter_value
        )
        
        print(f"   View result status: {view_result.get('status')}")
        
        # Convert to JSON like JsonResponse would
        json_result = json.dumps(view_result, default=str)
        print(f"   JSON serializable: ✓")
        print(f"   JSON length: {len(json_result)} characters")
        
        # Check what the frontend would see
        if view_result.get('status'):
            print(f"   ✓ Frontend would show success")
        else:
            error_msg = view_result.get('error', 'Unknown error occurred')
            print(f"   ✗ Frontend would show error: {error_msg}")
            
            # This is exactly what the JavaScript shows
            if not error_msg or error_msg == '':
                print(f"   → JavaScript would show: 'Unknown error occurred'")
            else:
                print(f"   → JavaScript would show: 'Error: {error_msg}'")
        
    except Exception as e:
        print(f"   ✗ Error in view simulation: {e}")
        import traceback
        print(f"   Traceback: {traceback.format_exc()}")
    
    print(f"\n=== Debug Summary ===\n")
    print("Possible causes of 'Unknown error occurred':")
    print("1. fetch_adx_traffic_account_by_user returns status=False with empty/missing error message")
    print("2. Exception in view that gets caught and converted to str(e) but e is empty")
    print("3. JSON serialization issue causing empty response")
    print("4. Network/timeout issue causing incomplete response")
    print("5. Ad Manager API authentication/permission issue")
    print("\nNext steps:")
    print("- Check server logs for detailed error messages")
    print("- Verify user credentials in database")
    print("- Test with different date ranges")
    print("- Check Ad Manager API access and permissions")

if __name__ == "__main__":
    debug_adx_traffic_account()
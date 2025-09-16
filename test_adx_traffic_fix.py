#!/usr/bin/env python3
"""
Test script to verify that the "Unknown error occurred" issue in AdX Traffic Account is fixed
"""

import os
import sys
import django
from pathlib import Path

# Add the project directory to Python path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

# Setup Django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'hris.settings')
django.setup()

# Import and apply GoogleAds patches
from management.googleads_patch_v2 import apply_googleads_patches
from management.database import data_mysql
from management.utils import fetch_adx_traffic_account_by_user
import json

def test_adx_traffic_fix():
    """
    Test that AdX Traffic Account error handling is fixed
    """
    try:
        print("[INFO] Testing AdX Traffic Account error handling fix...")
        
        # Apply patches
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
        print(f"[INFO] Testing with user: {user_email}")
        
        # Test with date range that should trigger error
        start_date = '2024-01-01'
        end_date = '2024-01-07'
        site_filter = None
        
        print(f"[INFO] Calling fetch_adx_traffic_account_by_user...")
        print(f"[INFO] Parameters: user_email={user_email}, start_date={start_date}, end_date={end_date}")
        
        # Call the function that was causing "Unknown error occurred"
        result = fetch_adx_traffic_account_by_user(
            user_email=user_email,
            start_date=start_date,
            end_date=end_date,
            site_filter=site_filter
        )
        
        print(f"\n=== Function Result ===")
        print(f"Status: {result.get('status')}")
        print(f"Has 'error' field: {'error' in result}")
        print(f"Has 'note' field: {'note' in result}")
        
        if 'error' in result:
            print(f"Error message: {result['error']}")
        if 'note' in result:
            print(f"Note message: {result['note']}")
        
        # Test JSON serialization
        try:
            json_result = json.dumps(result)
            print(f"JSON serializable: ✓")
            print(f"JSON length: {len(json_result)} characters")
        except Exception as json_error:
            print(f"JSON serialization failed: {json_error}")
            return
        
        # Simulate frontend behavior
        print(f"\n=== Frontend Simulation ===")
        if result.get('status') == False:
            if 'error' in result and result['error']:
                print(f"✓ Frontend would show error: {result['error']}")
                print(f"→ JavaScript would show: 'Error: {result['error']}'")
            else:
                print(f"✗ Frontend would show: 'Unknown error occurred'")
                print(f"→ JavaScript would show: 'Error: Unknown error occurred'")
        else:
            print(f"✓ Request would succeed with data")
        
        # Test view simulation
        print(f"\n=== View Response Simulation ===")
        # Simulate what AdxTrafficPerAccountDataView would return
        from django.http import JsonResponse
        
        try:
            # This simulates the view returning the result
            view_response = JsonResponse(result, safe=False)
            print(f"✓ View would return valid JsonResponse")
            print(f"Response status: {result.get('status')}")
            
            # Check if error field exists for frontend
            if not result.get('status') and 'error' in result:
                print(f"✓ Error field available for frontend: {result['error']}")
            elif not result.get('status'):
                print(f"✗ No error field - frontend would show 'Unknown error occurred'")
                
        except Exception as view_error:
            print(f"✗ View would fail: {view_error}")
        
        print(f"\n=== Test Summary ===")
        if not result.get('status') and 'error' in result and result['error']:
            print(f"✓ Fix successful: Error message properly returned in 'error' field")
            print(f"✓ Frontend will show: '{result['error']}' instead of 'Unknown error occurred'")
        elif result.get('status'):
            print(f"✓ Request succeeded - no error to display")
        else:
            print(f"✗ Fix incomplete: 'error' field missing or empty")
            
    except Exception as e:
        print(f"[ERROR] Test failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == '__main__':
    test_adx_traffic_fix()
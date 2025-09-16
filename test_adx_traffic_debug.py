#!/usr/bin/env python
import os
import sys
import django
from datetime import datetime, timedelta

# Setup Django environment
sys.path.append('/Users/ariefdwicahyoadi/hris')
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'hris.settings')
django.setup()

# Import after Django setup
from management.utils import fetch_adx_traffic_account_by_user
from management.googleads_patch_v2 import apply_googleads_patches
from management.database import data_mysql

def test_adx_traffic_debug():
    print("=== Testing AdX Traffic Account Data Fetch ===")
    
    # Apply patches first
    print("\n1. Applying GoogleAds patches...")
    try:
        apply_googleads_patches()
        print("✓ Patches applied successfully")
    except Exception as e:
        print(f"✗ Error applying patches: {e}")
        return
    
    # Get a real user email from database
    print("\n2. Getting user data from database...")
    try:
        # Get all users to find one with email
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
        print(f"✓ Using test user email: {user_email}")
        
    except Exception as e:
        print(f"✗ Error getting user data: {e}")
        # Fallback to environment variable if available
        user_email = os.getenv('TEST_USER_EMAIL')
        if not user_email:
            print("Please set TEST_USER_EMAIL environment variable or ensure database has users with email")
            return
        print(f"Using fallback email: {user_email}")
    
    # Test date range (last 7 days)
    end_date = datetime.now().date()
    start_date = end_date - timedelta(days=7)
    
    print(f"\n3. Testing AdX traffic data fetch...")
    print(f"   Date range: {start_date} to {end_date}")
    print(f"   User email: {user_email}")
    
    try:
        result = fetch_adx_traffic_account_by_user(
            user_email=user_email,
            start_date=start_date.strftime('%Y-%m-%d'),
            end_date=end_date.strftime('%Y-%m-%d'),
            site_filter=None
        )
        
        print(f"\n4. Results:")
        print(f"   Status: {result.get('status', 'Unknown')}")
        
        if result.get('status'):
            print(f"   ✓ Success! Data retrieved")
            print(f"   Data keys: {list(result.keys())}")
            
            if 'data' in result:
                data = result['data']
                print(f"   Number of records: {len(data) if isinstance(data, list) else 'N/A'}")
                
                if isinstance(data, list) and len(data) > 0:
                    print(f"   Sample record: {data[0]}")
                elif isinstance(data, dict):
                    print(f"   Data structure: {data}")
                else:
                    print(f"   Data: {data}")
            
            if 'summary' in result:
                summary = result['summary']
                print(f"   Summary: {summary}")
                
        else:
            print(f"   ✗ Failed to fetch data")
            print(f"   Error: {result.get('error', 'Unknown error')}")
            print(f"   Method: {result.get('method', 'Unknown method')}")
            
            if 'details' in result:
                print(f"   Details: {result['details']}")
    
    except Exception as e:
        print(f"   ✗ Exception occurred: {e}")
        import traceback
        print(f"   Traceback: {traceback.format_exc()}")

if __name__ == "__main__":
    test_adx_traffic_debug()
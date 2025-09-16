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

def test_adx_extended_range():
    print("=== Testing AdX Traffic with Extended Date Range ===")
    
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
        return
    
    # Test multiple date ranges
    date_ranges = [
        # Last 30 days
        (datetime.now().date() - timedelta(days=30), datetime.now().date()),
        # Last 60 days
        (datetime.now().date() - timedelta(days=60), datetime.now().date() - timedelta(days=30)),
        # Last 90 days
        (datetime.now().date() - timedelta(days=90), datetime.now().date() - timedelta(days=60)),
        # Specific date range (January 2024)
        (datetime(2024, 1, 1).date(), datetime(2024, 1, 31).date()),
        # Specific date range (December 2023)
        (datetime(2023, 12, 1).date(), datetime(2023, 12, 31).date()),
    ]
    
    for i, (start_date, end_date) in enumerate(date_ranges, 1):
        print(f"\n{i+2}. Testing date range: {start_date} to {end_date}")
        
        try:
            result = fetch_adx_traffic_account_by_user(
                user_email=user_email,
                start_date=start_date.strftime('%Y-%m-%d'),
                end_date=end_date.strftime('%Y-%m-%d'),
                site_filter=None
            )
            
            print(f"   Status: {result.get('status', 'Unknown')}")
            
            if result.get('status'):
                if 'data' in result:
                    data = result['data']
                    num_records = len(data) if isinstance(data, list) else 'N/A'
                    print(f"   ✓ Success! Records found: {num_records}")
                    
                    if isinstance(data, list) and len(data) > 0:
                        print(f"   Sample record: {data[0]}")
                        break  # Found data, stop testing
                    
                if 'summary' in result:
                    summary = result['summary']
                    total_impressions = summary.get('total_impressions', 0)
                    total_clicks = summary.get('total_clicks', 0)
                    total_revenue = summary.get('total_revenue', 0)
                    print(f"   Summary - Impressions: {total_impressions}, Clicks: {total_clicks}, Revenue: {total_revenue}")
                    
                    if total_impressions > 0 or total_clicks > 0 or total_revenue > 0:
                        print(f"   ✓ Found non-zero data in summary!")
                        break
                        
            else:
                print(f"   ✗ Failed: {result.get('error', 'Unknown error')}")
                
        except Exception as e:
            print(f"   ✗ Exception: {e}")
    
    print("\n=== Conclusion ===")
    print("If all date ranges return empty data, it could mean:")
    print("1. The Google Ad Manager account doesn't have AdX data for these periods")
    print("2. AdX reporting might not be enabled for this account")
    print("3. The account might not have any AdX traffic")
    print("4. There might be permission issues with AdX reporting")
    print("\nTo resolve this, check:")
    print("- Google Ad Manager account has AdX enabled")
    print("- Account has actual AdX traffic/revenue")
    print("- User has proper permissions for AdX reporting")

if __name__ == "__main__":
    test_adx_extended_range()
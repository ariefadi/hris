#!/usr/bin/env python3
"""
Direct test of fetch_adx_traffic_account_by_user function
"""

import os
import sys
import django
from datetime import datetime, timedelta

# Add the project directory to Python path
sys.path.append('/Users/ariefdwicahyoadi/hris')

# Setup Django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'hris.settings')
django.setup()

from management.utils import fetch_adx_traffic_account_by_user

def test_direct_fetch():
    """Test direct fetch of AdX traffic data"""
    print("=== Direct Test of AdX Traffic Data Fetch ===")
    
    # Use the user with complete credentials
    test_email = "adiarief463@gmail.com"
    
    # Test with last 7 days
    end_date = datetime.now().date()
    start_date = end_date - timedelta(days=7)
    
    print(f"\nTesting fetch_adx_traffic_account_by_user:")
    print(f"  - User: {test_email}")
    print(f"  - Date range: {start_date} to {end_date}")
    
    try:
        result = fetch_adx_traffic_account_by_user(
            test_email,
            start_date.strftime('%Y-%m-%d'),
            end_date.strftime('%Y-%m-%d')
        )
        
        print(f"\nResult status: {result.get('status', False)}")
        
        if result.get('status'):
            print("✓ Successfully fetched AdX traffic data!")
            print(f"  - API Method: {result.get('api_method')}")
            print(f"  - Records count: {len(result.get('data', []))}")
            print(f"  - User email: {result.get('user_email')}")
            print(f"  - Note: {result.get('note')}")
            
            # Show sample data if available
            data = result.get('data', [])
            if data:
                print(f"\nSample data (first 3 records):")
                for i, record in enumerate(data[:3]):
                    print(f"  Record {i+1}: {record}")
                    
            # Show summary if available
            summary = result.get('summary', {})
            if summary:
                print(f"\nSummary:")
                print(f"  - Total Impressions: {summary.get('total_impressions', 0)}")
                print(f"  - Total Clicks: {summary.get('total_clicks', 0)}")
                print(f"  - Total Earnings: {summary.get('total_earnings', 0)}")
                
        else:
            print("✗ Failed to fetch AdX traffic data")
            print(f"  - Error: {result.get('error', 'Unknown error')}")
            
    except Exception as e:
        print(f"✗ Exception occurred: {str(e)}")
        import traceback
        print(f"Traceback: {traceback.format_exc()}")

if __name__ == "__main__":
    test_direct_fetch()
    print("\n=== Test Complete ===")
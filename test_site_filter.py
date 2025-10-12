#!/usr/bin/env python
"""
Test script to verify site filtering functionality
"""
import os
import sys
import django

# Add the project directory to Python path
sys.path.append('/Users/ariefdwicahyoadi/hris')

# Set up Django environment
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'hris.settings')
django.setup()

from management.utils import fetch_adx_traffic_per_country
from datetime import datetime, timedelta

def test_site_filter():
    """Test the site filtering functionality"""
    print("="*60)
    print("TESTING SITE FILTER FUNCTIONALITY")
    print("="*60)
    
    # Test parameters
    user_email = "aksarabrita470@gmail.com"  # Using user with credentials
    start_date = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
    end_date = datetime.now().strftime('%Y-%m-%d')
    site_filter = "site1,site2,site3"  # Test with multiple sites
    selected_countries = ["ID", "US"]  # Test countries
    
    print(f"Test parameters:")
    print(f"  User email: {user_email}")
    print(f"  Start date: {start_date}")
    print(f"  End date: {end_date}")
    print(f"  Site filter: {site_filter}")
    print(f"  Selected countries: {selected_countries}")
    print()
    
    try:
        print("Calling fetch_adx_traffic_per_country...")
        result = fetch_adx_traffic_per_country(
            start_date=start_date,
            end_date=end_date,
            user_mail=user_email,
            countries_list=selected_countries,
            site_filter=site_filter
        )
        
        print("="*60)
        print("RESULT:")
        print("="*60)
        print(f"Type: {type(result)}")
        if isinstance(result, dict):
            print(f"Keys: {list(result.keys())}")
            if 'data' in result:
                print(f"Data length: {len(result['data']) if result['data'] else 0}")
        elif isinstance(result, list):
            print(f"List length: {len(result)}")
        else:
            print(f"Result: {result}")
            
    except Exception as e:
        print("="*60)
        print("ERROR OCCURRED:")
        print("="*60)
        print(f"Error: {e}")
        print(f"Error type: {type(e)}")
        import traceback
        print(f"Traceback:\n{traceback.format_exc()}")

if __name__ == "__main__":
    test_site_filter()
#!/usr/bin/env python3
"""
Test script to verify the fixed site filtering functionality
"""
import os
import sys
import django
from datetime import datetime, timedelta

# Add the project root to Python path
sys.path.insert(0, '/Users/ariefdwicahyoadi/hris')

# Set up Django environment
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'hris.settings')
django.setup()

from management.utils import fetch_adx_traffic_per_country

def test_site_filtering():
    """Test the site filtering functionality"""
    print("Testing site filtering functionality...")
    
    # Test parameters
    user_mail = "aksarabrita470@gmail.com"  # Using actual user from database
    end_date = datetime.now().date()
    start_date = end_date - timedelta(days=7)  # Last 7 days
    
    print(f"Date range: {start_date} to {end_date}")
    print(f"User: {user_mail}")
    
    # Test 1: No site filter (should return all sites)
    print("\n=== Test 1: No site filter ===")
    try:
        result_no_filter = fetch_adx_traffic_per_country(
            start_date=start_date,
            end_date=end_date,
            user_mail=user_mail,
            site_filter=None
        )
        
        if result_no_filter.get('status'):
            print(f"✓ Success - Retrieved {len(result_no_filter.get('data', []))} countries")
            # Show available sites
            all_sites = set()
            for country_data in result_no_filter.get('data', []):
                for site_data in country_data.get('sites', []):
                    all_sites.add(site_data.get('site_name', 'Unknown'))
            print(f"Available sites: {sorted(list(all_sites))}")
        else:
            print(f"✗ Failed: {result_no_filter.get('error', 'Unknown error')}")
            
    except Exception as e:
        print(f"✗ Exception: {e}")
    
    # Test 2: With site filter (if we found sites in test 1)
    print("\n=== Test 2: With site filter ===")
    try:
        # Use a common site name for testing
        test_site_filter = "example.com"  # Replace with actual site from your data
        
        result_with_filter = fetch_adx_traffic_per_country(
            start_date=start_date,
            end_date=end_date,
            user_mail=user_mail,
            site_filter=test_site_filter
        )
        
        if result_with_filter.get('status'):
            print(f"✓ Success - Retrieved {len(result_with_filter.get('data', []))} countries with site filter")
            # Verify filtering worked
            filtered_sites = set()
            for country_data in result_with_filter.get('data', []):
                for site_data in country_data.get('sites', []):
                    filtered_sites.add(site_data.get('site_name', 'Unknown'))
            print(f"Filtered sites: {sorted(list(filtered_sites))}")
            
            if test_site_filter in filtered_sites or len(filtered_sites) == 0:
                print("✓ Site filtering appears to be working correctly")
            else:
                print(f"⚠ Warning: Expected site '{test_site_filter}' not found in results")
        else:
            print(f"✗ Failed: {result_with_filter.get('error', 'Unknown error')}")
            
    except Exception as e:
        print(f"✗ Exception: {e}")
    
    # Test 3: Multiple site filter
    print("\n=== Test 3: Multiple site filter ===")
    try:
        test_multiple_sites = "site1.com,site2.com"  # Replace with actual sites
        
        result_multiple = fetch_adx_traffic_per_country(
            start_date=start_date,
            end_date=end_date,
            user_mail=user_mail,
            site_filter=test_multiple_sites
        )
        
        if result_multiple.get('status'):
            print(f"✓ Success - Retrieved {len(result_multiple.get('data', []))} countries with multiple site filter")
            # Show what sites were found
            multiple_sites = set()
            for country_data in result_multiple.get('data', []):
                for site_data in country_data.get('sites', []):
                    multiple_sites.add(site_data.get('site_name', 'Unknown'))
            print(f"Sites found: {sorted(list(multiple_sites))}")
        else:
            print(f"✗ Failed: {result_multiple.get('error', 'Unknown error')}")
            
    except Exception as e:
        print(f"✗ Exception: {e}")

if __name__ == "__main__":
    test_site_filtering()
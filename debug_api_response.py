#!/usr/bin/env python
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

def debug_api_response():
    """Debug the actual API response structure"""
    print("="*80)
    print("DEBUGGING API RESPONSE STRUCTURE")
    print("="*80)
    
    # Use a real user email with credentials
    user_mail = "aksarabrita470@gmail.com"
    
    # Set date range (last 7 days)
    end_date = datetime.now().date()
    start_date = end_date - timedelta(days=7)
    
    print(f"User: {user_mail}")
    print(f"Date range: {start_date} to {end_date}")
    print(f"Countries: ['US', 'ID']")
    print(f"Site filter: 'example.com,test.com'")
    print("-"*80)
    
    # Test 1: Without site filter
    print("\n1. TESTING WITHOUT SITE FILTER:")
    print("-"*40)
    result1 = fetch_adx_traffic_per_country(
        user_mail=user_mail,
        start_date=start_date,
        end_date=end_date,
        countries_list=['US', 'ID'],
        site_filter=None
    )
    
    if result1.get('status'):
        print(f"✓ Success: Got {len(result1.get('data', []))} rows")
        if result1.get('data'):
            sample_row = result1['data'][0]
            print(f"Sample row keys: {list(sample_row.keys())}")
            print(f"Sample row: {sample_row}")
            
            # Check if any row has site_name
            has_site_data = any('site_name' in row for row in result1['data'])
            print(f"Has site data: {has_site_data}")
    else:
        print(f"✗ Failed: {result1.get('error')}")
    
    # Test 2: With site filter
    print("\n2. TESTING WITH SITE FILTER:")
    print("-"*40)
    result2 = fetch_adx_traffic_per_country(
        user_mail=user_mail,
        start_date=start_date,
        end_date=end_date,
        countries_list=['US', 'ID'],
        site_filter='example.com,test.com'
    )
    
    if result2.get('status'):
        print(f"✓ Success: Got {len(result2.get('data', []))} rows")
        if result2.get('data'):
            sample_row = result2['data'][0]
            print(f"Sample row keys: {list(sample_row.keys())}")
            print(f"Sample row: {sample_row}")
            
            # Check if any row has site_name
            has_site_data = any('site_name' in row for row in result2['data'])
            print(f"Has site data: {has_site_data}")
            
            # Check for site names in data
            site_names = [row.get('site_name', 'N/A') for row in result2['data']]
            unique_sites = set(site_names)
            print(f"Unique site names found: {unique_sites}")
    else:
        print(f"✗ Failed: {result2.get('error')}")
    
    print("\n" + "="*80)
    print("DEBUG COMPLETE")
    print("="*80)

if __name__ == "__main__":
    debug_api_response()
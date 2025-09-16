#!/usr/bin/env python3
"""
Debug script extended untuk memeriksa data AdSense dengan periode yang lebih luas
"""

import os
import sys
import django
from datetime import datetime, timedelta

# Setup Django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'hris.settings')
django.setup()

from management.utils_adsense import get_user_adsense_client

def debug_adsense_extended(user_email):
    """
    Debug AdSense dengan periode yang lebih luas dan berbagai konfigurasi
    """
    print(f"\n=== Extended AdSense Debug for {user_email} ===")
    
    try:
        # Get AdSense client
        client_result = get_user_adsense_client(user_email)
        if not client_result['status']:
            print(f"❌ Failed to get AdSense client: {client_result.get('error')}")
            return
        
        service = client_result['service']
        accounts = service.accounts().list().execute()
        account_id = accounts['accounts'][0]['name']
        
        print(f"✅ Using account: {account_id}")
        
        # Test dengan periode yang sangat luas (1 tahun terakhir)
        print("\n1. Testing with very wide date range (1 year)...")
        
        end_date = datetime.now()
        start_date = datetime(2024, 1, 1)  # Mulai dari awal 2024
        
        try:
            report_request = service.accounts().reports().generate(
                account=account_id,
                dateRange='CUSTOM',
                startDate_year=start_date.year,
                startDate_month=start_date.month,
                startDate_day=start_date.day,
                endDate_year=end_date.year,
                endDate_month=end_date.month,
                endDate_day=end_date.day,
                metrics=['IMPRESSIONS', 'CLICKS', 'ESTIMATED_EARNINGS']
            )
            
            report = report_request.execute()
            
            if 'rows' in report and report['rows']:
                print(f"✅ Found data in wide range: {len(report['rows'])} rows")
                
                total_impressions = 0
                total_clicks = 0
                total_earnings = 0.0
                
                for row in report['rows']:
                    if len(row['cells']) >= 3:
                        total_impressions += int(float(row['cells'][0]['value']))
                        total_clicks += int(float(row['cells'][1]['value']))
                        total_earnings += float(row['cells'][2]['value'])
                
                print(f"   - Total Impressions: {total_impressions:,}")
                print(f"   - Total Clicks: {total_clicks:,}")
                print(f"   - Total Earnings: ${total_earnings:.2f}")
            else:
                print("❌ No data found even in wide range")
                
        except Exception as e:
            print(f"⚠️  Error with wide range: {str(e)}")
        
        # Test dengan preset date ranges
        print("\n2. Testing with preset date ranges...")
        
        preset_ranges = ['LAST_7_DAYS', 'LAST_30_DAYS', 'THIS_MONTH', 'LAST_MONTH']
        
        for preset in preset_ranges:
            try:
                print(f"   Testing {preset}...")
                report_request = service.accounts().reports().generate(
                    account=account_id,
                    dateRange=preset,
                    metrics=['IMPRESSIONS', 'CLICKS', 'ESTIMATED_EARNINGS']
                )
                
                report = report_request.execute()
                
                if 'rows' in report and report['rows']:
                    total_impressions = sum(int(float(row['cells'][0]['value'])) for row in report['rows'])
                    total_clicks = sum(int(float(row['cells'][1]['value'])) for row in report['rows'])
                    total_earnings = sum(float(row['cells'][2]['value']) for row in report['rows'])
                    
                    print(f"     ✅ {preset}: {total_impressions:,} impressions, {total_clicks:,} clicks, ${total_earnings:.2f}")
                else:
                    print(f"     ❌ {preset}: No data")
                    
            except Exception as e:
                print(f"     ⚠️  {preset}: Error - {str(e)}")
        
        # Test available dimensions
        print("\n3. Testing available dimensions...")
        
        dimensions_to_test = [
            ['DATE'],
            ['COUNTRY_NAME'],
            ['PLATFORM_TYPE_NAME'],
            ['AD_UNIT_SIZE_NAME']
        ]
        
        for dims in dimensions_to_test:
            try:
                print(f"   Testing dimensions {dims}...")
                report_request = service.accounts().reports().generate(
                    account=account_id,
                    dateRange='LAST_30_DAYS',
                    dimensions=dims,
                    metrics=['IMPRESSIONS', 'CLICKS', 'ESTIMATED_EARNINGS']
                )
                
                report = report_request.execute()
                
                if 'rows' in report and report['rows']:
                    print(f"     ✅ Found {len(report['rows'])} rows with dimensions {dims}")
                    
                    # Show first few rows
                    for i, row in enumerate(report['rows'][:3]):
                        dim_values = [cell['value'] for cell in row['cells'][:len(dims)]]
                        metrics_values = [cell['value'] for cell in row['cells'][len(dims):]]
                        print(f"       {i+1}. {dim_values} -> {metrics_values}")
                else:
                    print(f"     ❌ No data with dimensions {dims}")
                    
            except Exception as e:
                print(f"     ⚠️  Dimensions {dims}: Error - {str(e)}")
        
        # Test account status and settings
        print("\n4. Checking account details...")
        
        try:
            account_details = service.accounts().get(name=account_id).execute()
            print(f"   Account State: {account_details.get('state', 'Unknown')}")
            print(f"   Premium: {account_details.get('premium', 'Unknown')}")
            print(f"   Creation Time: {account_details.get('createTime', 'Unknown')}")
            
            # Check if account is ready for payments
            if 'state' in account_details:
                if account_details['state'] == 'READY':
                    print("   ✅ Account is READY for serving ads")
                elif account_details['state'] == 'GETTING_READY':
                    print("   ⚠️  Account is GETTING_READY (may not have data yet)")
                elif account_details['state'] == 'REQUIRES_REVIEW':
                    print("   ⚠️  Account REQUIRES_REVIEW")
                else:
                    print(f"   ⚠️  Account state: {account_details['state']}")
                    
        except Exception as e:
            print(f"   ⚠️  Error getting account details: {str(e)}")
        
        # Final recommendation
        print("\n" + "="*60)
        print("DIAGNOSIS:")
        print("- AdSense account is found and accessible")
        print("- Account state is READY")
        print("- However, no traffic data is available for any tested period")
        print("\nPOSSIBLE REASONS:")
        print("1. Account is new and hasn't generated any ad impressions yet")
        print("2. Website(s) linked to this AdSense account have no traffic")
        print("3. Ad codes are not properly implemented on websites")
        print("4. Websites are not approved or ads are not showing")
        print("5. Data might be available in AdSense web interface but not via API")
        print("\nRECOMMENDATIONS:")
        print("1. Check AdSense web interface at https://www.google.com/adsense/")
        print("2. Verify ad codes are properly implemented on websites")
        print("3. Check if websites have actual traffic and ad impressions")
        print("4. Wait for data to accumulate if account is very new")
        print("="*60)
        
    except Exception as e:
        print(f"❌ General error: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == '__main__':
    user_email = 'adiarief463@gmail.com'
    debug_adsense_extended(user_email)
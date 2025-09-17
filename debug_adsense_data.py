#!/usr/bin/env python3
"""
Debug script untuk memeriksa data AdSense dan mengapa tidak ada data yang muncul
"""

import os
import sys
import django
from datetime import datetime, timedelta

# Setup Django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'hris.settings')
django.setup()

from management.utils_adsense import get_user_adsense_client

def debug_adsense_account(user_email):
    """
    Debug AdSense account untuk melihat detail akun dan data yang tersedia
    """
    print(f"\n=== Debugging AdSense Account for {user_email} ===")
    
    try:
        # Get AdSense client
        print("1. Getting AdSense client...")
        client_result = get_user_adsense_client(user_email)
        if not client_result['status']:
            print(f"❌ Failed to get AdSense client: {client_result.get('error')}")
            return
        
        service = client_result['service']
        print("✅ AdSense client initialized successfully")
        
        # Get accounts list
        print("\n2. Getting AdSense accounts...")
        accounts = service.accounts().list().execute()
        
        if not accounts.get('accounts'):
            print("❌ No AdSense accounts found")
            return
        
        print(f"✅ Found {len(accounts['accounts'])} AdSense account(s)")
        
        for i, account in enumerate(accounts['accounts']):
            print(f"   Account {i+1}:")
            print(f"     - Name: {account.get('name', 'N/A')}")
            print(f"     - Display Name: {account.get('displayName', 'N/A')}")
            print(f"     - State: {account.get('state', 'N/A')}")
            print(f"     - Premium: {account.get('premium', 'N/A')}")
            print(f"     - Time Zone: {account.get('timeZone', {}).get('id', 'N/A')}")
        
        # Use first account for detailed analysis
        account_id = accounts['accounts'][0]['name']
        print(f"\n3. Using account: {account_id}")
        
        # Get ad units
        print("\n4. Getting ad units...")
        try:
            ad_units = service.accounts().adunits().list(parent=account_id).execute()
            if ad_units.get('adUnits'):
                print(f"✅ Found {len(ad_units['adUnits'])} ad unit(s)")
                for i, unit in enumerate(ad_units['adUnits'][:5]):  # Show first 5
                    print(f"   Ad Unit {i+1}:")
                    print(f"     - Name: {unit.get('name', 'N/A')}")
                    print(f"     - Display Name: {unit.get('displayName', 'N/A')}")
                    print(f"     - State: {unit.get('state', 'N/A')}")
                    print(f"     - Content Ads Settings: {unit.get('contentAdsSettings', {}).get('type', 'N/A')}")
            else:
                print("❌ No ad units found")
        except Exception as e:
            print(f"⚠️  Error getting ad units: {str(e)}")
        
        # Test different date ranges
        print("\n5. Testing different date ranges...")
        
        date_ranges = [
            ('Last 7 days', 7),
            ('Last 30 days', 30),
            ('Last 90 days', 90)
        ]
        
        for range_name, days in date_ranges:
            end_date = datetime.now()
            start_date = end_date - timedelta(days=days)
            
            print(f"\n   Testing {range_name} ({start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')})...")
            
            try:
                # Simple report request
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
                    print(f"     ✅ Found {len(report['rows'])} data rows")
                    
                    # Show summary
                    total_impressions = 0
                    total_clicks = 0
                    total_earnings = 0.0
                    
                    for row in report['rows']:
                        if len(row['cells']) >= 3:
                            total_impressions += int(float(row['cells'][0]['value']))
                            total_clicks += int(float(row['cells'][1]['value']))
                            total_earnings += float(row['cells'][2]['value'])
                    
                    print(f"     - Total Impressions: {total_impressions:,}")
                    print(f"     - Total Clicks: {total_clicks:,}")
                    print(f"     - Total Earnings: ${total_earnings:.2f}")
                    
                    if total_impressions > 0:
                        print(f"     🎉 Found data for this period!")
                        break
                else:
                    print(f"     ❌ No data found for this period")
                    
            except Exception as e:
                print(f"     ⚠️  Error testing {range_name}: {str(e)}")
        
        # Test with dimensions
        print("\n6. Testing report with dimensions...")
        try:
            end_date = datetime.now()
            start_date = end_date - timedelta(days=30)
            
            report_request = service.accounts().reports().generate(
                account=account_id,
                dateRange='CUSTOM',
                startDate_year=start_date.year,
                startDate_month=start_date.month,
                startDate_day=start_date.day,
                endDate_year=end_date.year,
                endDate_month=end_date.month,
                endDate_day=end_date.day,
                dimensions=['AD_UNIT_NAME'],
                metrics=['IMPRESSIONS', 'CLICKS', 'ESTIMATED_EARNINGS']
            )
            
            report = report_request.execute()
            
            if 'rows' in report and report['rows']:
                print(f"✅ Found {len(report['rows'])} ad units with data")
                for i, row in enumerate(report['rows'][:5]):  # Show first 5
                    if len(row['cells']) >= 4:
                        ad_unit = row['cells'][0]['value']
                        impressions = int(float(row['cells'][1]['value']))
                        clicks = int(float(row['cells'][2]['value']))
                        earnings = float(row['cells'][3]['value'])
                        print(f"   {i+1}. {ad_unit}: {impressions:,} impressions, {clicks:,} clicks, ${earnings:.2f}")
            else:
                print("❌ No ad unit data found")
                
        except Exception as e:
            print(f"⚠️  Error testing with dimensions: {str(e)}")
        
    except Exception as e:
        print(f"❌ General error: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == '__main__':
    user_email = 'adiarief463@gmail.com'
    debug_adsense_account(user_email)
    
    print("\n" + "="*60)
    print("Debug completed. Check the output above for issues.")
    print("="*60)
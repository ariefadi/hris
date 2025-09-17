#!/usr/bin/env python3
"""
Script untuk menguji dan mendiagnosis masalah AdX secara komprehensif
"""

import os
import sys
import django
from datetime import datetime, timedelta

# Add the project directory to Python path
sys.path.insert(0, '/Users/ariefdwicahyoadi/hris')

# Set Django settings
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'hris.settings')
django.setup()

def test_adx_comprehensive():
    """
    Test AdX functionality comprehensively
    """
    print("🔍 Testing AdX Comprehensive Diagnosis")
    print("=" * 50)
    
    # Apply patches
    try:
        from management.googleads_patch_v2 import apply_googleads_patches
        apply_googleads_patches()
        print("✓ GoogleAds patches applied successfully")
    except Exception as e:
        print(f"⚠ Warning: Could not apply patches: {e}")
    
    # Test 1: Check user and database
    print("\n1. Checking user and database...")
    try:
        from django.contrib.auth.models import User
        # Try to import models - may not exist
        try:
            from management.models import GoogleAdsCredentials
        except ImportError:
            print("⚠ GoogleAdsCredentials model not found, checking user only")
            GoogleAdsCredentials = None
        
        user = User.objects.filter(email='adiarief463@gmail.com').first()
        if user:
            print(f"✓ User found: {user.email}")
            
            if GoogleAdsCredentials:
                creds = GoogleAdsCredentials.objects.filter(user=user).first()
                if creds:
                    print(f"✓ Credentials found for user")
                else:
                    print(f"✗ No credentials found for user")
                    return
            else:
                print(f"⚠ Skipping credentials check (model not available)")
        else:
            print(f"✗ User not found")
            return
    except Exception as e:
        print(f"✗ Database error: {e}")
        return
    
    # Test 2: Test Ad Manager Client
    print("\n2. Testing Ad Manager Client...")
    try:
        from management.utils import get_user_ad_manager_client
        
        client_result = get_user_ad_manager_client(user.email)
        if client_result['status']:
            client = client_result['client']
            print(f"✓ Ad Manager client created successfully")
            
            # Test network access
            try:
                network_service = client.GetService('NetworkService', version='v202408')
                network = network_service.getCurrentNetwork()
                print(f"✓ Network access successful: {network['displayName']} (ID: {network['networkCode']})")
                
                # Check if network has AdX enabled
                if hasattr(network, 'adExchangeEnabled'):
                    print(f"✓ AdX enabled status: {network.get('adExchangeEnabled', 'Unknown')}")
                else:
                    print(f"⚠ AdX enabled status not available in network info")
                    
            except Exception as e:
                print(f"✗ Network access failed: {e}")
                return
        else:
            print(f"✗ Failed to create Ad Manager client: {client_result.get('error', 'Unknown error')}")
            return
    except Exception as e:
        print(f"✗ Ad Manager client error: {e}")
        return
    
    # Test 3: Test Report Service
    print("\n3. Testing Report Service...")
    try:
        report_service = client.GetService('ReportService', version='v202408')
        print(f"✓ Report service created successfully")
        
        # Test 4: Try different column combinations
        print("\n4. Testing different column combinations...")
        
        test_combinations = [
            {
                'name': 'Only Impressions',
                'dimensions': ['DATE'],
                'columns': ['AD_EXCHANGE_IMPRESSIONS']
            },
            {
                'name': 'Only Revenue', 
                'dimensions': ['DATE'],
                'columns': ['AD_EXCHANGE_TOTAL_EARNINGS']
            },
            {
                'name': 'Only Clicks',
                'dimensions': ['DATE'], 
                'columns': ['AD_EXCHANGE_CLICKS']
            },
            {
                'name': 'Impressions + Revenue',
                'dimensions': ['DATE'],
                'columns': ['AD_EXCHANGE_IMPRESSIONS', 'AD_EXCHANGE_TOTAL_EARNINGS']
            },
            {
                'name': 'All AdX Columns',
                'dimensions': ['DATE'],
                'columns': ['AD_EXCHANGE_IMPRESSIONS', 'AD_EXCHANGE_CLICKS', 'AD_EXCHANGE_TOTAL_EARNINGS']
            },
            {
                'name': 'With Site Dimension',
                'dimensions': ['DATE', 'AD_EXCHANGE_SITE_NAME'],
                'columns': ['AD_EXCHANGE_IMPRESSIONS']
            }
        ]
        
        end_date = datetime.now().date()
        start_date = end_date - timedelta(days=7)
        
        for i, combo in enumerate(test_combinations, 1):
            print(f"\n   Test {i}: {combo['name']}")
            print(f"   Dimensions: {combo['dimensions']}")
            print(f"   Columns: {combo['columns']}")
            
            try:
                # Create report query
                report_query = {
                    'reportQuery': {
                        'dimensions': combo['dimensions'],
                        'columns': combo['columns'],
                        'dateRangeType': 'CUSTOM_DATE',
                        'startDate': {
                            'year': start_date.year,
                            'month': start_date.month,
                            'day': start_date.day
                        },
                        'endDate': {
                            'year': end_date.year,
                            'month': end_date.month,
                            'day': end_date.day
                        }
                    }
                }
                
                print(f"   Attempting to create report...")
                report_job = report_service.runReportJob(report_query)
                print(f"   ✓ Report created successfully! ID: {report_job['id']}")
                
                # Check report status
                import time
                max_attempts = 10
                for attempt in range(max_attempts):
                    try:
                        status = report_service.getReportJobStatus(report_job['id'])
                        print(f"   Status check {attempt + 1}: {status}")
                        
                        if status == 'COMPLETED':
                            print(f"   ✓ Report completed successfully!")
                            
                            # Try to download report
                            try:
                                downloader = client.GetDataDownloader(version='v202408')
                                report_data = downloader.DownloadReportToString(
                                    report_job['id'], 'CSV_DUMP'
                                )
                                lines = report_data.strip().split('\n')
                                print(f"   ✓ Report downloaded: {len(lines)} lines")
                                if len(lines) > 1:
                                    print(f"   ✓ Has data rows: {len(lines) - 1} data rows")
                                else:
                                    print(f"   ⚠ No data rows (header only)")
                            except Exception as download_error:
                                print(f"   ✗ Download failed: {download_error}")
                            break
                        elif status == 'FAILED':
                            print(f"   ✗ Report failed")
                            break
                        else:
                            time.sleep(2)
                    except Exception as status_error:
                        print(f"   ✗ Status check failed: {status_error}")
                        break
                
                # This combination works!
                print(f"   🎉 SUCCESS: This combination works!")
                break
                
            except Exception as e:
                error_msg = str(e)
                print(f"   ✗ Failed: {error_msg}")
                
                if 'NOT_NULL' in error_msg:
                    print(f"   → NOT_NULL constraint violation")
                elif 'REPORT_NOT_FOUND' in error_msg:
                    print(f"   → Report not found error")
                elif 'PERMISSION' in error_msg.upper():
                    print(f"   → Permission denied")
                elif 'INVALID' in error_msg.upper():
                    print(f"   → Invalid parameter")
                else:
                    print(f"   → Unknown error type")
    
    except Exception as e:
        print(f"✗ Report service error: {e}")
        return
    
    print("\n" + "=" * 50)
    print("🏁 Comprehensive AdX Test Complete")
    
    print("\n💡 Recommendations:")
    print("1. If all tests fail with NOT_NULL: Network may not have AdX data")
    print("2. If PERMISSION errors: Check AdX access in Google Ad Manager")
    print("3. If REPORT_NOT_FOUND: Check network configuration")
    print("4. Try different date ranges (last 30 days, last 90 days)")
    print("5. Verify AdX is properly configured in Google Ad Manager")
    
    print("\n🔧 Next steps:")
    print("1. Check Google Ad Manager → Admin → Global Settings → Network Settings")
    print("2. Verify AdX is enabled and configured")
    print("3. Check if there's actual AdX traffic in the date range")
    print("4. Contact Google support if AdX should be available but isn't working")

if __name__ == '__main__':
    test_adx_comprehensive()
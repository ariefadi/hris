#!/usr/bin/env python3
"""
Debug masalah site_name yang menampilkan 'Ad Exchange Display'
alih-alih nama domain yang sebenarnya
"""

import os
import sys
import django
from datetime import datetime, timedelta

# Setup Django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'hris.settings')
sys.path.append('/Users/ariefdwicahyoadi/hris')
django.setup()

from management.utils import get_user_ad_manager_client

def debug_site_name_issue():
    """Debug masalah site_name"""
    
    print("=" * 60)
    print("🔍 DEBUG: Site Name Issue Analysis")
    print("=" * 60)
    
    user_email = "adiarief463@gmail.com"
    end_date = datetime.now().date()
    start_date = end_date - timedelta(days=7)
    
    print(f"📅 Date range: {start_date} to {end_date}")
    print(f"👤 User email: {user_email}")
    
    try:
        # Get client
        client_result = get_user_ad_manager_client(user_email)
        if not client_result['status']:
            print(f"❌ Failed to get client: {client_result['error']}")
            return
        
        client = client_result['client']
        report_service = client.GetService('ReportService', version='v202408')
        
        print("\n🔍 Testing different dimension combinations...")
        
        # Test different dimension combinations to find the right one for site names
        dimension_tests = [
            {
                'name': 'AD_UNIT_NAME only',
                'dimensions': ['DATE', 'AD_UNIT_NAME'],
                'columns': ['TOTAL_LINE_ITEM_LEVEL_IMPRESSIONS']
            },
            {
                'name': 'AD_EXCHANGE_SITE_NAME (if available)',
                'dimensions': ['DATE', 'AD_EXCHANGE_SITE_NAME'],
                'columns': ['AD_EXCHANGE_IMPRESSIONS']
            },
            {
                'name': 'CUSTOM_TARGETING_VALUE_ID',
                'dimensions': ['DATE', 'CUSTOM_TARGETING_VALUE_ID'],
                'columns': ['TOTAL_LINE_ITEM_LEVEL_IMPRESSIONS']
            },
            {
                'name': 'AD_UNIT_NAME + CUSTOM_TARGETING',
                'dimensions': ['DATE', 'AD_UNIT_NAME', 'CUSTOM_TARGETING_VALUE_ID'],
                'columns': ['TOTAL_LINE_ITEM_LEVEL_IMPRESSIONS']
            },
            {
                'name': 'ADVERTISER_NAME',
                'dimensions': ['DATE', 'ADVERTISER_NAME'],
                'columns': ['TOTAL_LINE_ITEM_LEVEL_IMPRESSIONS']
            },
            {
                'name': 'LINE_ITEM_NAME',
                'dimensions': ['DATE', 'LINE_ITEM_NAME'],
                'columns': ['TOTAL_LINE_ITEM_LEVEL_IMPRESSIONS']
            }
        ]
        
        for i, test in enumerate(dimension_tests, 1):
            print(f"\n📊 Test {i}/{len(dimension_tests)}: {test['name']}")
            print(f"   Dimensions: {test['dimensions']}")
            print(f"   Columns: {test['columns']}")
            
            try:
                report_query = {
                    'reportQuery': {
                        'dimensions': test['dimensions'],
                        'columns': test['columns'],
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
                
                # Try to create report
                report_job = report_service.runReportJob(report_query)
                print(f"   ✅ Report created successfully (ID: {report_job['id']})")
                
                # Wait for completion
                max_attempts = 10
                for attempt in range(max_attempts):
                    status = report_service.getReportJobStatus(report_job['id'])
                    if status == 'COMPLETED':
                        print(f"   ✅ Report completed")
                        
                        # Download and show sample data
                        downloader = client.GetDataDownloader(version='v202408')
                        report_data = downloader.DownloadReportToString(report_job['id'], 'CSV_DUMP')
                        
                        lines = report_data.strip().split('\n')
                        if len(lines) > 1:
                            print(f"   📋 Sample data (first 3 rows):")
                            print(f"      Header: {lines[0]}")
                            for j, line in enumerate(lines[1:4]):
                                if line.strip():
                                    print(f"      Row {j+1}: {line}")
                        else:
                            print(f"   ⚠️ No data returned")
                        break
                    elif status == 'FAILED':
                        print(f"   ❌ Report failed")
                        break
                    else:
                        if attempt < 5:  # Only wait for first few attempts
                            import time
                            time.sleep(1)
                        else:
                            print(f"   ⏱️ Still processing... (status: {status})")
                            break
                
            except Exception as e:
                error_msg = str(e)
                print(f"   ❌ Failed: {error_msg}")
                
                if 'NOT_NULL' in error_msg:
                    print(f"      💡 No data available for this dimension combination")
                elif 'PERMISSION' in error_msg.upper():
                    print(f"      💡 Permission denied for this dimension")
                elif 'INVALID' in error_msg.upper():
                    print(f"      💡 Invalid dimension or column combination")
        
        print("\n" + "=" * 60)
        print("📝 Analysis Summary:")
        print("   - AD_UNIT_NAME returns internal unit names like 'Ad Exchange Display'")
        print("   - Need to find dimension that returns actual domain names")
        print("   - May need to use different approach or mapping")
        print("=" * 60)
        
    except Exception as e:
        print(f"❌ Exception occurred: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    debug_site_name_issue()
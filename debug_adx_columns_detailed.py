#!/usr/bin/env python3
"""
Debug script untuk memeriksa kolom AdX yang tersedia secara detail
"""

import os
import sys
import django
from datetime import datetime, timedelta

# Setup Django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'hris.settings')
sys.path.append('/Users/ariefdwicahyoadi/hris')
django.setup()

from management.googleads_patch_v2 import apply_googleads_patches
from management.utils import get_user_ad_manager_client

# Apply patches
apply_googleads_patches()

def test_adx_columns_detailed():
    """Test setiap kombinasi kolom AdX secara detail"""
    
    print("=" * 60)
    print("🔍 DEBUG: Testing AdX Columns Detailed")
    print("=" * 60)
    
    user_email = "adiarief463@gmail.com"
    end_date = datetime.now().date()
    start_date = end_date - timedelta(days=7)
    
    print(f"📅 Date range: {start_date} to {end_date}")
    print(f"👤 User email: {user_email}")
    
    try:
        # Get client
        client_result = get_user_ad_manager_client(user_email)
        if not client_result.get('status'):
            print(f"❌ Failed to get Ad Manager client: {client_result.get('error')}")
            return
            
        client = client_result['client']
        print("✅ Ad Manager client obtained")
        
        report_service = client.GetService('ReportService', version='v202408')
        print("✅ Report service obtained")
        
        # Test individual AdX columns
        adx_columns_to_test = [
            'AD_EXCHANGE_IMPRESSIONS',
            'AD_EXCHANGE_CLICKS', 
            'AD_EXCHANGE_TOTAL_EARNINGS',
            'AD_EXCHANGE_CPC',
            'AD_EXCHANGE_CTR',
            'AD_EXCHANGE_ECPM',
            'AD_EXCHANGE_REVENUE_SHARE',
            'AD_EXCHANGE_COVERAGE'
        ]
        
        print("\n🧪 Testing individual AdX columns:")
        working_columns = []
        
        for column in adx_columns_to_test:
            try:
                print(f"\n   Testing: {column}")
                
                report_query = {
                    'reportQuery': {
                        'dimensions': ['DATE', 'AD_EXCHANGE_SITE_NAME'],
                        'columns': [column],
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
                
                # Try to create report job
                report_job = report_service.runReportJob(report_query)
                print(f"   ✅ {column} - Report job created: {report_job['id']}")
                working_columns.append(column)
                
                # Wait a bit and check status
                import time
                time.sleep(2)
                status = report_service.getReportJobStatus(report_job['id'])
                print(f"   📊 {column} - Status: {status}")
                
            except Exception as e:
                error_msg = str(e)
                print(f"   ❌ {column} - Error: {error_msg}")
                
                if 'NOT_NULL' in error_msg:
                    print(f"      💡 {column} requires NOT_NULL constraint (no data available)")
                elif 'PERMISSION' in error_msg.upper():
                    print(f"      🔒 {column} permission denied")
                elif 'INVALID' in error_msg.upper():
                    print(f"      ⚠️ {column} invalid column")
                else:
                    print(f"      🤔 {column} unknown error")
        
        print(f"\n📋 Working columns: {working_columns}")
        
        if working_columns:
            print("\n🔄 Testing combinations of working columns:")
            
            # Test combinations
            combinations_to_test = [
                working_columns[:1],  # First working column only
                working_columns[:2],  # First two working columns
                working_columns[:3],  # First three working columns
                working_columns       # All working columns
            ]
            
            for i, combo in enumerate(combinations_to_test):
                if not combo:
                    continue
                    
                try:
                    print(f"\n   Testing combination {i+1}: {combo}")
                    
                    report_query = {
                        'reportQuery': {
                            'dimensions': ['DATE', 'AD_EXCHANGE_SITE_NAME'],
                            'columns': combo,
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
                    
                    report_job = report_service.runReportJob(report_query)
                    print(f"   ✅ Combination {i+1} - Report job created: {report_job['id']}")
                    
                    # Wait and check status
                    time.sleep(2)
                    status = report_service.getReportJobStatus(report_job['id'])
                    print(f"   📊 Combination {i+1} - Status: {status}")
                    
                    if status == 'COMPLETED':
                        print(f"   🎉 Combination {i+1} - COMPLETED! This combination works!")
                        
                        # Try to download a sample
                        try:
                            downloader = client.GetDataDownloader(version='v202408')
                            report_data = downloader.DownloadReportToString(report_job['id'], 'CSV_DUMP')
                            lines = report_data.strip().split('\n')
                            print(f"   📊 Combination {i+1} - Downloaded {len(lines)} lines")
                            
                            if len(lines) > 1:
                                print(f"   📋 Combination {i+1} - Sample data: {lines[1][:100]}...")
                            else:
                                print(f"   ⚠️ Combination {i+1} - No data rows")
                                
                        except Exception as download_error:
                            print(f"   ❌ Combination {i+1} - Download error: {download_error}")
                    
                except Exception as e:
                    error_msg = str(e)
                    print(f"   ❌ Combination {i+1} - Error: {error_msg}")
        else:
            print("\n❌ No working AdX columns found")
            print("\n💡 Possible reasons:")
            print("   1. Account doesn't have AdX enabled")
            print("   2. No AdX data for the selected date range")
            print("   3. Insufficient permissions for AdX reporting")
            print("   4. AdX not properly configured")
        
    except Exception as e:
        print(f"❌ Error in main test: {e}")
        import traceback
        traceback.print_exc()
    
    print("\n" + "=" * 60)
    print("🏁 Debug completed")
    print("=" * 60)

if __name__ == "__main__":
    test_adx_columns_detailed()
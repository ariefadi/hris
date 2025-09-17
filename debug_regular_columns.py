#!/usr/bin/env python3
"""
Debug script untuk memeriksa kolom regular Ad Manager yang tersedia
Berdasarkan gambar yang diberikan user, data yang ditampilkan adalah regular Ad Manager, bukan AdX
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

def test_regular_columns():
    """Test kolom regular Ad Manager yang tersedia"""
    
    print("=" * 60)
    print("🔍 DEBUG: Testing Regular Ad Manager Columns")
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
        
        # Test regular Ad Manager columns yang mungkin mengandung clicks dan revenue
        regular_columns_to_test = [
            'TOTAL_IMPRESSIONS',
            'TOTAL_CLICKS', 
            'TOTAL_LINE_ITEM_LEVEL_IMPRESSIONS',
            'TOTAL_LINE_ITEM_LEVEL_CLICKS',
            'TOTAL_LINE_ITEM_LEVEL_CTR',
            'TOTAL_LINE_ITEM_LEVEL_CPM_AND_CPC_REVENUE',
            'TOTAL_LINE_ITEM_LEVEL_WITHOUT_CPD_AVERAGE_ECPM',
            'AD_SERVER_IMPRESSIONS',
            'AD_SERVER_CLICKS',
            'AD_SERVER_CTR',
            'AD_SERVER_CPM_AND_CPC_REVENUE',
            'AD_SERVER_AVERAGE_ECPM',
            'AD_SERVER_WITHOUT_CPD_AVERAGE_ECPM',
            'ADSENSE_LINE_ITEM_LEVEL_IMPRESSIONS',
            'ADSENSE_LINE_ITEM_LEVEL_CLICKS',
            'ADSENSE_LINE_ITEM_LEVEL_CTR',
            'ADSENSE_LINE_ITEM_LEVEL_REVENUE',
            'ADSENSE_LINE_ITEM_LEVEL_AVERAGE_ECPM'
        ]
        
        print("\n🧪 Testing individual regular columns:")
        working_columns = []
        
        for column in regular_columns_to_test:
            try:
                print(f"\n   Testing: {column}")
                
                report_query = {
                    'reportQuery': {
                        'dimensions': ['DATE', 'AD_UNIT_NAME'],
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
                
                # If completed, try to get sample data
                if status == 'COMPLETED':
                    try:
                        downloader = client.GetDataDownloader(version='v202408')
                        report_data = downloader.DownloadReportToString(report_job['id'], 'CSV_DUMP')
                        lines = report_data.strip().split('\n')
                        print(f"   📊 {column} - Downloaded {len(lines)} lines")
                        
                        if len(lines) > 1:
                            # Show header and first data row
                            print(f"   📋 {column} - Header: {lines[0]}")
                            print(f"   📋 {column} - Sample: {lines[1][:100]}...")
                            
                            # Check if this column has non-zero values
                            data_values = lines[1].split(',')
                            if len(data_values) >= 3:  # DATE, AD_UNIT_NAME, COLUMN_VALUE
                                column_value = data_values[2].strip()
                                if column_value and column_value != '0' and column_value != '0.0':
                                    print(f"   🎉 {column} - HAS NON-ZERO DATA: {column_value}")
                                else:
                                    print(f"   ⚠️ {column} - Data is zero: {column_value}")
                        else:
                            print(f"   ⚠️ {column} - No data rows")
                            
                    except Exception as download_error:
                        print(f"   ❌ {column} - Download error: {download_error}")
                
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
            print("\n🔄 Testing best combination for clicks and revenue:")
            
            # Test the best combination that should include clicks and revenue
            best_combinations = [
                ['TOTAL_IMPRESSIONS', 'TOTAL_CLICKS', 'AD_SERVER_CPM_AND_CPC_REVENUE'],
                ['TOTAL_LINE_ITEM_LEVEL_IMPRESSIONS', 'TOTAL_LINE_ITEM_LEVEL_CLICKS', 'TOTAL_LINE_ITEM_LEVEL_CPM_AND_CPC_REVENUE'],
                ['AD_SERVER_IMPRESSIONS', 'AD_SERVER_CLICKS', 'AD_SERVER_CPM_AND_CPC_REVENUE'],
                ['ADSENSE_LINE_ITEM_LEVEL_IMPRESSIONS', 'ADSENSE_LINE_ITEM_LEVEL_CLICKS', 'ADSENSE_LINE_ITEM_LEVEL_REVENUE']
            ]
            
            for i, combo in enumerate(best_combinations):
                # Check if all columns in combo are working
                if all(col in working_columns for col in combo):
                    try:
                        print(f"\n   Testing best combination {i+1}: {combo}")
                        
                        report_query = {
                            'reportQuery': {
                                'dimensions': ['DATE', 'AD_UNIT_NAME'],
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
                        print(f"   ✅ Best combo {i+1} - Report job created: {report_job['id']}")
                        
                        # Wait and check status
                        time.sleep(3)
                        status = report_service.getReportJobStatus(report_job['id'])
                        print(f"   📊 Best combo {i+1} - Status: {status}")
                        
                        if status == 'COMPLETED':
                            print(f"   🎉 Best combo {i+1} - COMPLETED! This combination works!")
                            
                            # Try to download and analyze data
                            try:
                                downloader = client.GetDataDownloader(version='v202408')
                                report_data = downloader.DownloadReportToString(report_job['id'], 'CSV_DUMP')
                                lines = report_data.strip().split('\n')
                                print(f"   📊 Best combo {i+1} - Downloaded {len(lines)} lines")
                                
                                if len(lines) > 1:
                                    print(f"   📋 Best combo {i+1} - Header: {lines[0]}")
                                    print(f"   📋 Best combo {i+1} - Sample: {lines[1]}")
                                    
                                    # Analyze the data
                                    headers = lines[0].split(',')
                                    sample_data = lines[1].split(',')
                                    
                                    print(f"   🔍 Best combo {i+1} - Data analysis:")
                                    for j, (header, value) in enumerate(zip(headers, sample_data)):
                                        print(f"      {header}: {value}")
                                        
                                    # Check for non-zero clicks and revenue
                                    has_clicks = any('CLICK' in header.upper() for header in headers)
                                    has_revenue = any('REVENUE' in header.upper() or 'CPM' in header.upper() for header in headers)
                                    
                                    if has_clicks and has_revenue:
                                        print(f"   🎉 Best combo {i+1} - HAS BOTH CLICKS AND REVENUE COLUMNS!")
                                        print(f"   💡 This combination should be used for the final implementation")
                                        break
                                else:
                                    print(f"   ⚠️ Best combo {i+1} - No data rows")
                                    
                            except Exception as download_error:
                                print(f"   ❌ Best combo {i+1} - Download error: {download_error}")
                        
                    except Exception as e:
                        error_msg = str(e)
                        print(f"   ❌ Best combo {i+1} - Error: {error_msg}")
                else:
                    print(f"   ⏭️ Skipping combination {i+1}: {combo} (some columns not working)")
        else:
            print("\n❌ No working regular columns found")
        
    except Exception as e:
        print(f"❌ Error in main test: {e}")
        import traceback
        traceback.print_exc()
    
    print("\n" + "=" * 60)
    print("🏁 Debug completed")
    print("=" * 60)

if __name__ == "__main__":
    test_regular_columns()
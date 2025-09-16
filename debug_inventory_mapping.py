#!/usr/bin/env python3
"""
Debug untuk mencari mapping antara ad unit dan domain sebenarnya
melalui inventory service
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

def debug_inventory_mapping():
    """Debug inventory untuk mencari mapping domain"""
    
    print("=" * 60)
    print("🔍 DEBUG: Inventory Mapping Analysis")
    print("=" * 60)
    
    user_email = "adiarief463@gmail.com"
    
    try:
        # Get client
        client_result = get_user_ad_manager_client(user_email)
        if not client_result['status']:
            print(f"❌ Failed to get client: {client_result['error']}")
            return
        
        client = client_result['client']
        
        print("\n📋 1. Getting Inventory Service...")
        inventory_service = client.GetService('InventoryService', version='v202408')
        
        print("\n📋 2. Getting Ad Units (simple query)...")
        try:
            # Simple query without WHERE clause
            statement = {
                'query': 'SELECT Id, Name, AdUnitCode LIMIT 20'
            }
            
            page = inventory_service.getAdUnitsByStatement(statement)
            
            if 'results' in page and page['results']:
                print(f"   ✅ Found {len(page['results'])} ad units:")
                
                for i, ad_unit in enumerate(page['results'][:10]):
                    print(f"      {i+1}. ID: {ad_unit['id']}")
                    print(f"         Name: {ad_unit['name']}")
                    print(f"         Code: {ad_unit.get('adUnitCode', 'N/A')}")
                    print()
            else:
                print(f"   ⚠️ No ad units found")
                
        except Exception as e:
            print(f"   ❌ Failed to get ad units: {e}")
        
        print("\n📋 3. Getting Network Service info...")
        try:
            network_service = client.GetService('NetworkService', version='v202408')
            current_network = network_service.getCurrentNetwork()
            
            print(f"   Network Name: {current_network['displayName']}")
            print(f"   Network Code: {current_network['networkCode']}")
            print(f"   Currency: {current_network['currencyCode']}")
            
        except Exception as e:
            print(f"   ❌ Failed to get network info: {e}")
        
        print("\n📋 4. Testing Report with different dimensions...")
        try:
            report_service = client.GetService('ReportService', version='v202408')
            
            # Test with AD_UNIT_NAME dimension
            report_query = {
                'dimensions': ['DATE', 'AD_UNIT_NAME'],
                'columns': ['TOTAL_LINE_ITEM_LEVEL_IMPRESSIONS'],
                'startDate': {
                    'year': 2025,
                    'month': 8,
                    'day': 24
                },
                'endDate': {
                    'year': 2025,
                    'month': 8,
                    'day': 31
                },
                'dateRangeType': 'CUSTOM_DATE'
            }
            
            print("   📊 Creating report with AD_UNIT_NAME...")
            report_job = {
                'reportQuery': report_query
            }
            
            report_job = report_service.runReportJob(report_job)
            print(f"   ✅ Report created (ID: {report_job['id']})")
            
            # Wait for completion
            import time
            max_wait = 30
            waited = 0
            
            while waited < max_wait:
                status = report_service.getReportJobStatus(report_job['id'])
                print(f"   📊 Status: {status}")
                
                if status == 'COMPLETED':
                    break
                elif status == 'FAILED':
                    print("   ❌ Report failed")
                    return
                    
                time.sleep(2)
                waited += 2
            
            if status == 'COMPLETED':
                print("   📥 Downloading report...")
                report_downloader = client.GetDataDownloader(version='v202408')
                report_data = report_downloader.DownloadReportToString(
                    report_job['id'], 'CSV_DUMP'
                )
                
                lines = report_data.strip().split('\n')
                print(f"   📋 Sample data (first 5 rows):")
                for i, line in enumerate(lines[:6]):
                    if i == 0:
                        print(f"      Header: {line}")
                    else:
                        print(f"      Row {i}: {line}")
                        
                # Look for domain-like patterns
                print("\n   🔍 Looking for domain patterns...")
                domain_candidates = set()
                for line in lines[1:]:  # Skip header
                    parts = line.split(',')
                    if len(parts) >= 2:
                        ad_unit_name = parts[1]
                        if '.' in ad_unit_name and ('com' in ad_unit_name or 'net' in ad_unit_name or 'org' in ad_unit_name):
                            domain_candidates.add(ad_unit_name)
                
                if domain_candidates:
                    print(f"   ✅ Found potential domains: {list(domain_candidates)}")
                else:
                    print(f"   ⚠️ No domain-like patterns found in ad unit names")
            
        except Exception as e:
            print(f"   ❌ Failed to create report: {e}")
        
        print("\n" + "=" * 60)
        print("📝 Analysis Summary:")
        print("   - Network: Adzone 3 (Code: 23303534834)")
        print("   - Need to check ad unit names for domain patterns")
        print("   - May need manual mapping if no domain info in ad units")
        print("=" * 60)
        
    except Exception as e:
        print(f"❌ Exception occurred: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    debug_inventory_mapping()
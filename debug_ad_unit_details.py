#!/usr/bin/env python3
"""
Debug untuk mendapatkan detail ad unit dan mencari informasi domain
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

def debug_ad_unit_details():
    """Debug detail ad unit untuk mencari informasi domain"""
    
    print("=" * 60)
    print("🔍 DEBUG: Ad Unit Details Analysis")
    print("=" * 60)
    
    user_email = "adiarief463@gmail.com"
    
    try:
        # Get client
        client_result = get_user_ad_manager_client(user_email)
        if not client_result['status']:
            print(f"❌ Failed to get client: {client_result['error']}")
            return
        
        client = client_result['client']
        
        print("\n📋 1. Getting specific Ad Unit details...")
        inventory_service = client.GetService('InventoryService', version='v202408')
        
        try:
            # Get specific ad unit by ID
            ad_unit_id = "23302762549"
            statement = {
                'query': f'SELECT Id, Name, AdUnitCode, Description, ParentId, Status WHERE Id = {ad_unit_id}'
            }
            
            page = inventory_service.getAdUnitsByStatement(statement)
            
            if 'results' in page and page['results']:
                ad_unit = page['results'][0]
                print(f"   ✅ Found ad unit:")
                print(f"      ID: {ad_unit['id']}")
                print(f"      Name: {ad_unit['name']}")
                print(f"      Code: {ad_unit.get('adUnitCode', 'N/A')}")
                print(f"      Description: {ad_unit.get('description', 'N/A')}")
                print(f"      Parent ID: {ad_unit.get('parentId', 'N/A')}")
                print(f"      Status: {ad_unit.get('status', 'N/A')}")
                
                # Check if there's a parent
                if ad_unit.get('parentId'):
                    print(f"\n   📁 Getting parent ad unit...")
                    parent_statement = {
                        'query': f'SELECT Id, Name, AdUnitCode, Description WHERE Id = {ad_unit["parentId"]}'
                    }
                    
                    parent_page = inventory_service.getAdUnitsByStatement(parent_statement)
                    if 'results' in parent_page and parent_page['results']:
                        parent = parent_page['results'][0]
                        print(f"      Parent Name: {parent['name']}")
                        print(f"      Parent Code: {parent.get('adUnitCode', 'N/A')}")
                        print(f"      Parent Description: {parent.get('description', 'N/A')}")
            else:
                print(f"   ⚠️ Ad unit not found")
                
        except Exception as e:
            print(f"   ❌ Failed to get ad unit details: {e}")
        
        print("\n📋 2. Getting all ad units to find hierarchy...")
        try:
            # Get all ad units to understand hierarchy
            statement = {
                'query': 'SELECT Id, Name, AdUnitCode, ParentId LIMIT 50'
            }
            
            page = inventory_service.getAdUnitsByStatement(statement)
            
            if 'results' in page and page['results']:
                print(f"   ✅ Found {len(page['results'])} ad units:")
                
                # Group by parent
                root_units = []
                child_units = {}
                
                for ad_unit in page['results']:
                    if not ad_unit.get('parentId'):
                        root_units.append(ad_unit)
                    else:
                        parent_id = ad_unit['parentId']
                        if parent_id not in child_units:
                            child_units[parent_id] = []
                        child_units[parent_id].append(ad_unit)
                
                print(f"\n   🌳 Ad Unit Hierarchy:")
                for root in root_units[:5]:  # Show first 5 root units
                    print(f"      📁 {root['name']} (ID: {root['id']}, Code: {root.get('adUnitCode', 'N/A')})")
                    
                    if root['id'] in child_units:
                        for child in child_units[root['id']][:3]:  # Show first 3 children
                            print(f"         └── {child['name']} (ID: {child['id']}, Code: {child.get('adUnitCode', 'N/A')})")
                        
                        if len(child_units[root['id']]) > 3:
                            print(f"         └── ... and {len(child_units[root['id']]) - 3} more")
                    print()
            else:
                print(f"   ⚠️ No ad units found")
                
        except Exception as e:
            print(f"   ❌ Failed to get ad units: {e}")
        
        print("\n📋 3. Testing different report dimensions...")
        try:
            report_service = client.GetService('ReportService', version='v202408')
            
            # Test different dimension combinations
            test_dimensions = [
                ['DATE', 'AD_UNIT_NAME', 'AD_UNIT_ID'],
                ['DATE', 'ADVERTISER_NAME'],
                ['DATE', 'ORDER_NAME'],
                ['DATE', 'LINE_ITEM_NAME', 'LINE_ITEM_ID']
            ]
            
            for dims in test_dimensions:
                try:
                    print(f"\n   📊 Testing dimensions: {dims}")
                    
                    report_query = {
                        'dimensions': dims,
                        'columns': ['TOTAL_LINE_ITEM_LEVEL_IMPRESSIONS'],
                        'startDate': {
                            'year': 2025,
                            'month': 8,
                            'day': 30
                        },
                        'endDate': {
                            'year': 2025,
                            'month': 8,
                            'day': 31
                        },
                        'dateRangeType': 'CUSTOM_DATE'
                    }
                    
                    report_job = {
                        'reportQuery': report_query
                    }
                    
                    report_job = report_service.runReportJob(report_job)
                    
                    # Wait for completion
                    import time
                    max_wait = 20
                    waited = 0
                    
                    while waited < max_wait:
                        status = report_service.getReportJobStatus(report_job['id'])
                        
                        if status == 'COMPLETED':
                            break
                        elif status == 'FAILED':
                            print(f"      ❌ Report failed")
                            break
                            
                        time.sleep(1)
                        waited += 1
                    
                    if status == 'COMPLETED':
                        report_downloader = client.GetDataDownloader(version='v202408')
                        report_data = report_downloader.DownloadReportToString(
                            report_job['id'], 'CSV_DUMP'
                        )
                        
                        lines = report_data.strip().split('\n')
                        print(f"      ✅ Got {len(lines)-1} data rows")
                        
                        if len(lines) > 1:
                            print(f"      Header: {lines[0]}")
                            print(f"      Sample: {lines[1]}")
                            
                            # Look for domain patterns in any column
                            for line in lines[1:3]:  # Check first 2 data rows
                                parts = line.split(',')
                                for i, part in enumerate(parts):
                                    if '.' in part and ('com' in part or 'net' in part or 'org' in part):
                                        print(f"      🎯 Potential domain in column {i}: {part}")
                    
                except Exception as e:
                    print(f"      ❌ Failed: {e}")
            
        except Exception as e:
            print(f"   ❌ Failed to test dimensions: {e}")
        
        print("\n" + "=" * 60)
        print("📝 Conclusion:")
        print("   - Ad unit 'Ad Exchange Display' is likely a generic container")
        print("   - Domain info may not be available in Google Ad Manager")
        print("   - Consider creating manual mapping table:")
        print("     * Ad Unit ID -> Domain mapping")
        print("     * Or use external configuration")
        print("=" * 60)
        
    except Exception as e:
        print(f"❌ Exception occurred: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    debug_ad_unit_details()
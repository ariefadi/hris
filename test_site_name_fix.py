#!/usr/bin/env python3
"""
Test untuk memverifikasi perbaikan site name mapping
"""

import os
import sys
import django
from datetime import datetime, timedelta

# Setup Django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'hris.settings')
sys.path.append('/Users/ariefdwicahyoadi/hris')
django.setup()

from management.utils import fetch_adx_traffic_account_by_user

def test_site_name_fix():
    """Test site name mapping fix"""
    
    print("=" * 60)
    print("🧪 TEST: Site Name Mapping Fix")
    print("=" * 60)
    
    user_email = "adiarief463@gmail.com"
    
    # Test dengan tanggal yang sama seperti sebelumnya
    start_date = "2025-08-24"
    end_date = "2025-08-31"
    
    print(f"\n📋 Testing with:")
    print(f"   User: {user_email}")
    print(f"   Date range: {start_date} to {end_date}")
    
    try:
        print(f"\n🔄 Fetching AdX traffic data...")
        result = fetch_adx_traffic_account_by_user(user_email, start_date, end_date)
        
        if result['status']:
            print(f"   ✅ Success! Got {len(result['data'])} records")
            
            # Check summary
            summary = result['summary']
            print(f"\n📊 Summary:")
            print(f"   Total Impressions: {summary['total_impressions']:,}")
            print(f"   Total Clicks: {summary['total_clicks']:,}")
            print(f"   Total Revenue: ${summary['total_revenue']:,.2f}")
            
            # Handle optional summary fields
            if 'average_ctr' in summary:
                print(f"   Average CTR: {summary['average_ctr']:.2f}%")
            if 'average_cpc' in summary:
                print(f"   Average CPC: ${summary['average_cpc']:.4f}")
            if 'average_ecpm' in summary:
                print(f"   Average eCPM: ${summary['average_ecpm']:.2f}")
            
            # Check site names in data
            print(f"\n🏷️ Site Names Found:")
            site_names = set()
            for record in result['data']:
                site_names.add(record['site_name'])
            
            for site_name in sorted(site_names):
                print(f"   - {site_name}")
            
            # Check if we have the correct domain name
            if 'blog.missagendalimon.com' in site_names:
                print(f"\n   ✅ SUCCESS: Found correct domain 'blog.missagendalimon.com'")
                
                # Show sample data with correct domain
                print(f"\n📋 Sample data with correct domain:")
                for i, record in enumerate(result['data'][:3]):
                    if record['site_name'] == 'blog.missagendalimon.com':
                        print(f"      {i+1}. Date: {record['date']}")
                        print(f"         Site: {record['site_name']}")
                        print(f"         Impressions: {record['impressions']:,}")
                        print(f"         Clicks: {record['clicks']:,}")
                        print(f"         Revenue: ${record['revenue']:,.2f}")
                        print(f"         CTR: {record['ctr']:.2f}%")
                        print()
            else:
                print(f"\n   ❌ ISSUE: Still showing generic names: {list(site_names)}")
                
                # Show what we're getting instead
                print(f"\n📋 Sample data (first 3 records):")
                for i, record in enumerate(result['data'][:3]):
                    print(f"      {i+1}. Date: {record['date']}")
                    print(f"         Site: {record['site_name']}")
                    print(f"         Impressions: {record['impressions']:,}")
                    print(f"         Clicks: {record['clicks']:,}")
                    print(f"         Revenue: ${record['revenue']:,.2f}")
                    print()
        else:
            print(f"   ❌ Failed: {result['error']}")
            print(f"   Method used: {result.get('method', 'Unknown')}")
    
    except Exception as e:
        print(f"❌ Exception occurred: {e}")
        import traceback
        traceback.print_exc()
    
    print("\n" + "=" * 60)
    print("📝 Test Summary:")
    print("   - Checking if site_name now shows 'blog.missagendalimon.com'")
    print("   - Instead of generic 'Ad Exchange Display'")
    print("   - This should fix the admin interface display")
    print("=" * 60)

if __name__ == "__main__":
    test_site_name_fix()
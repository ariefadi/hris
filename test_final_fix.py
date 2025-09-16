#!/usr/bin/env python3
"""
Test final fix untuk Ad Manager tanpa developer token
Dengan kolom yang tepat untuk clicks dan revenue
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

def test_final_fix():
    """Test final fix untuk Ad Manager"""
    
    print("=" * 60)
    print("🔧 FINAL TEST: Ad Manager tanpa Developer Token")
    print("=" * 60)
    
    user_email = "adiarief463@gmail.com"
    end_date = datetime.now().date()
    start_date = end_date - timedelta(days=7)
    
    print(f"📅 Date range: {start_date} to {end_date}")
    print(f"👤 User email: {user_email}")
    
    try:
        print("\n🚀 Testing fetch_adx_traffic_account_by_user...")
        
        result = fetch_adx_traffic_account_by_user(
            user_email=user_email,
            start_date=start_date,
            end_date=end_date
        )
        
        print(f"\n📊 Result status: {result.get('status')}")
        
        if result.get('status'):
            print("✅ SUCCESS! Data berhasil diambil")
            
            # Check summary data
            summary = result.get('summary', {})
            print(f"\n📈 Summary data:")
            print(f"   Total Impressions: {summary.get('total_impressions', 0):,}")
            print(f"   Total Clicks: {summary.get('total_clicks', 0):,}")
            print(f"   Total Revenue: ${summary.get('total_revenue', 0):,.2f}")
            print(f"   Average CPC: ${summary.get('average_cpc', 0):.4f}")
            print(f"   Average CTR: {summary.get('average_ctr', 0):.2f}%")
            print(f"   Average eCPM: ${summary.get('average_ecpm', 0):.2f}")
            
            # Check if we have clicks and revenue
            has_clicks = summary.get('total_clicks', 0) > 0
            has_revenue = summary.get('total_revenue', 0) > 0
            
            print(f"\n🎯 Data Quality Check:")
            print(f"   Has Clicks: {'✅ YES' if has_clicks else '❌ NO'}")
            print(f"   Has Revenue: {'✅ YES' if has_revenue else '❌ NO'}")
            
            if has_clicks and has_revenue:
                print(f"\n🎉 PERFECT! Clicks dan Revenue berhasil diambil!")
                print(f"   💡 Fix berhasil: Ad Manager bekerja tanpa developer token")
            elif has_clicks:
                print(f"\n⚠️ Clicks ada, tapi Revenue masih 0")
            elif has_revenue:
                print(f"\n⚠️ Revenue ada, tapi Clicks masih 0")
            else:
                print(f"\n❌ Clicks dan Revenue masih 0")
            
            # Check detailed data
            data = result.get('data', [])
            print(f"\n📋 Detail data: {len(data)} rows")
            
            if data:
                print(f"\n📝 Sample data (first 3 rows):")
                for i, row in enumerate(data[:3]):
                    print(f"   Row {i+1}: {row['date']} | {row['site_name']} | "
                          f"Imp: {row['impressions']:,} | Clicks: {row['clicks']:,} | "
                          f"Revenue: ${row['revenue']:.2f}")
            
            # Check API method used
            api_method = result.get('api_method', 'unknown')
            note = result.get('note', '')
            print(f"\n🔧 Technical Info:")
            print(f"   API Method: {api_method}")
            print(f"   Note: {note}")
            
        else:
            print(f"❌ FAILED: {result.get('error', 'Unknown error')}")
            
            # Check if it's a credential issue
            error_msg = result.get('error', '')
            if 'developer_token' in error_msg.lower():
                print(f"   💡 Masih ada masalah dengan developer token")
            elif 'credential' in error_msg.lower():
                print(f"   💡 Masalah dengan kredensial OAuth")
            elif 'permission' in error_msg.lower():
                print(f"   💡 Masalah permission di Ad Manager")
            else:
                print(f"   💡 Error lainnya")
        
    except Exception as e:
        print(f"❌ Exception occurred: {e}")
        import traceback
        traceback.print_exc()
    
    print("\n" + "=" * 60)
    print("🏁 Final test completed")
    print("=" * 60)

if __name__ == "__main__":
    test_final_fix()
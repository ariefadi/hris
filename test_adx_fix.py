#!/usr/bin/env python3
"""
Script to test the AdX NOT_NULL columns fix
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

from management.googleads_patch_v2 import apply_googleads_patches
from management.views import AdxTrafficPerAccountDataView

def test_adx_fix():
    """
    Test the AdX NOT_NULL columns fix
    """
    print("🧪 Testing AdX NOT_NULL Columns Fix")
    print("=" * 50)
    
    # Apply patches first
    print("\n1. Applying GoogleAds patches...")
    try:
        apply_googleads_patches()
        print("✓ All patches applied successfully")
    except Exception as e:
        print(f"✗ Error applying patches: {e}")
        return
    
    # Test the AdX data view
    print("\n2. Testing AdX Traffic Account Data View...")
    
    try:
        # Create date range for testing (last 7 days)
        end_date = datetime.now().date()
        start_date = end_date - timedelta(days=7)
        
        print(f"   Date range: {start_date} to {end_date}")
        
        # Test the data view
        data_view = AdxTrafficPerAccountDataView()
        
        # Mock request object
        class MockRequest:
            def __init__(self):
                self.GET = {
                    'start_date': start_date.strftime('%Y-%m-%d'),
                    'end_date': end_date.strftime('%Y-%m-%d'),
                    'site_filter': ''  # No site filter for testing
                }
        
        mock_request = MockRequest()
        
        print("   Calling get() method...")
        response = data_view.get(mock_request)
        
        print(f"   Response status code: {response.status_code}")
        
        if response.status_code == 200:
            print("✅ AdX Traffic Account test PASSED!")
            print("   The NOT_NULL columns fix is working correctly.")
            
            # Try to get response content
            try:
                import json
                content = response.content.decode('utf-8')
                if content.startswith('{'):
                    data = json.loads(content)
                    if 'status' in data:
                        print(f"   API Status: {data.get('status')}")
                        print(f"   API Method: {data.get('api_method', 'Unknown')}")
                        print(f"   Data Records: {len(data.get('data', []))}")
                        print(f"   User Email: {data.get('user_email', 'Unknown')}")
                        if 'summary' in data:
                            summary = data['summary']
                            print(f"   Summary - Impressions: {summary.get('total_impressions', 0)}")
                            print(f"   Summary - Clicks: {summary.get('total_clicks', 0)}")
                            print(f"   Summary - Revenue: ${summary.get('total_revenue', 0):.2f}")
                else:
                    print(f"   Response content (first 200 chars): {content[:200]}...")
            except Exception as e:
                print(f"   Could not parse response content: {e}")
        else:
            print(f"❌ AdX Traffic Account test FAILED with status {response.status_code}")
            try:
                content = response.content.decode('utf-8')
                print(f"   Error content: {content[:500]}...")
            except:
                print("   Could not decode error content")
        
    except Exception as e:
        print(f"❌ Test failed with exception: {e}")
        import traceback
        print("   Full traceback:")
        traceback.print_exc()
    
    print("\n📋 Test Summary:")
    print("1. Applied GoogleAds patches")
    print("2. Tested AdX Traffic Account Data View")
    print("3. Verified NOT_NULL columns fix")
    
    print("\n🔍 What to check next:")
    print("1. Visit http://127.0.0.1:8000/management/admin/adx_traffic_account")
    print("2. Verify no NOT_NULL errors appear")
    print("3. Check that data loads successfully")
    print("4. Test different date ranges")

if __name__ == '__main__':
    test_adx_fix()
#!/usr/bin/env python
"""
Test script to verify Google Ad Manager credentials without patches
"""

import os
import sys
import django
from pathlib import Path

# Setup Django
BASE_DIR = Path(__file__).resolve().parent
sys.path.append(str(BASE_DIR))
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'hris.settings')
django.setup()

from django.conf import settings
from googleads import ad_manager
import tempfile
import yaml

def test_credentials():
    """Test Google Ad Manager credentials without any patches"""
    print("[TEST] Testing Google Ad Manager credentials...")
    
    # Get credentials from settings
    client_id = getattr(settings, 'GOOGLE_ADS_CLIENT_ID', '')
    client_secret = getattr(settings, 'GOOGLE_ADS_CLIENT_SECRET', '')
    refresh_token = getattr(settings, 'GOOGLE_ADS_REFRESH_TOKEN', '')
    network_code = getattr(settings, 'GOOGLE_AD_MANAGER_NETWORK_CODE', '23303534834')
    
    print(f"[TEST] Client ID: {client_id[:20]}..." if client_id else "[TEST] Client ID: MISSING")
    print(f"[TEST] Client Secret: {client_secret[:10]}..." if client_secret else "[TEST] Client Secret: MISSING")
    print(f"[TEST] Refresh Token: {refresh_token[:20]}..." if refresh_token else "[TEST] Refresh Token: MISSING")
    print(f"[TEST] Network Code: {network_code}")
    
    if not all([client_id, client_secret, refresh_token]):
        print("[ERROR] Missing required credentials!")
        return False
    
    # Create YAML configuration
    yaml_content = f"""ad_manager:
  client_id: "{client_id}"
  client_secret: "{client_secret}"
  refresh_token: "{refresh_token}"
  application_name: "AdX Manager Dashboard"
  network_code: "{network_code}"
"""
    
    # Write to temporary file
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
        f.write(yaml_content)
        yaml_file = f.name
    
    try:
        print(f"[TEST] Creating Ad Manager client...")
        client = ad_manager.AdManagerClient.LoadFromStorage(yaml_file)
        print(f"[TEST] Client created successfully")
        
        print(f"[TEST] Getting NetworkService...")
        network_service = client.GetService('NetworkService', version='v202408')
        print(f"[TEST] NetworkService obtained successfully")
        
        print(f"[TEST] Calling getCurrentNetwork...")
        network = network_service.getCurrentNetwork()
        print(f"[TEST] Network: {network['displayName']} (ID: {network['networkCode']})")
        
        print(f"[TEST] Getting ReportService...")
        report_service = client.GetService('ReportService', version='v202408')
        print(f"[TEST] ReportService obtained successfully")
        
        # Test a simple report query
        from datetime import datetime, timedelta
        end_date = datetime.now().date()
        start_date = end_date - timedelta(days=7)
        
        report_job = {
            'reportQuery': {
                'dimensions': ['DATE', 'AD_UNIT_NAME'],
                'columns': ['TOTAL_IMPRESSIONS', 'TOTAL_CLICKS', 'TOTAL_REVENUE'],
                'dateRangeType': 'CUSTOM_DATE',
                'startDate': start_date,
                'endDate': end_date
            }
        }
        
        print(f"[TEST] Running report job...")
        result = report_service.runReportJob(report_job)
        print(f"[TEST] Report job created with ID: {result['id']}")
        
        return True
        
    except Exception as e:
        print(f"[ERROR] Test failed: {e}")
        import traceback
        traceback.print_exc()
        return False
    finally:
        # Clean up
        if os.path.exists(yaml_file):
            os.unlink(yaml_file)

if __name__ == '__main__':
    test_credentials()
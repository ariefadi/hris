#!/usr/bin/env python3
"""
Test Google Ad Manager API with our patch to see the real SOAP fault
"""

import os
import sys
import django
from pathlib import Path

# Add the project directory to Python path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

# Setup Django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'hris.settings')
django.setup()

# Import and apply our patches
from management.googleads_patch_v2 import apply_googleads_patches

def test_with_patch():
    """Test Google Ad Manager API with our patch applied"""
    try:
        print("[INFO] Applying googleads patches...")
        apply_googleads_patches()
        
        print("[INFO] Creating Ad Manager client...")
        from management.utils import get_ad_manager_client
        
        # Get client using our existing utility
        client = get_ad_manager_client()
        if not client:
            print("[ERROR] Failed to create Ad Manager client")
            return
        
        print("[INFO] Getting NetworkService...")
        network_service = client.GetService('NetworkService', version='v202408')
        
        print("[INFO] Calling getCurrentNetwork...")
        try:
            network = network_service.getCurrentNetwork()
            print(f"[SUCCESS] Network: {network.displayName} ({network.networkCode})")
        except Exception as e:
            print(f"[ERROR] getCurrentNetwork failed: {e}")
            print(f"[ERROR] Error type: {type(e).__name__}")
            return
        
        print("[INFO] Getting ReportService...")
        report_service = client.GetService('ReportService', version='v202408')
        
        print("[INFO] Creating simple report query...")
        from googleads.ad_manager import ReportQuery, Dimension, Column, DateRange
        
        # Create a simple report query
        report_query = {
            'dimensions': [Dimension.DATE],
            'columns': [Column.AD_SERVER_IMPRESSIONS, Column.AD_SERVER_CLICKS],
            'dateRangeType': 'LAST_7_DAYS'
        }
        
        print("[INFO] Running report job...")
        try:
            report_job = report_service.runReportJob(report_query)
            print(f"[SUCCESS] Report job created: {report_job.id}")
        except Exception as e:
            print(f"[ERROR] runReportJob failed: {e}")
            print(f"[ERROR] Error type: {type(e).__name__}")
            return
        
        print("[SUCCESS] All API calls completed successfully!")
        
    except Exception as e:
        print(f"[ERROR] Test failed: {e}")
        print(f"[ERROR] Error type: {type(e).__name__}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_with_patch()
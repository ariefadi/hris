#!/usr/bin/env python3
"""
Test Google Ad Manager API using service account authentication
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

def test_service_account():
    """Test Google Ad Manager API using service account"""
    
    try:
        print("[INFO] Testing with service account authentication...")
        
        # Import and apply patches
        from management.googleads_patch_v2 import apply_googleads_patches
        apply_googleads_patches()
        
        from googleads import ad_manager
        import tempfile
        import yaml
        
        # Create YAML config for service account
        config = {
            'ad_manager': {
                'application_name': 'HRIS Ad Manager Integration',
                'network_code': os.getenv('GOOGLE_AD_MANAGER_NETWORK_CODE'),
                'path_to_private_key_file': os.getenv('GOOGLE_AD_MANAGER_KEY_FILE')
            }
        }
        
        # Write temporary YAML file
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            yaml.dump(config, f)
            temp_yaml_path = f.name
        
        try:
            print(f"[INFO] Creating Ad Manager client with service account...")
            client = ad_manager.AdManagerClient.LoadFromStorage(temp_yaml_path)
            
            print(f"[INFO] Getting NetworkService...")
            network_service = client.GetService('NetworkService', version='v202408')
            
            print(f"[INFO] Calling getCurrentNetwork...")
            network = network_service.getCurrentNetwork()
            print(f"[SUCCESS] Network: {network.displayName} ({network.networkCode})")
            
            print(f"[INFO] Getting ReportService...")
            report_service = client.GetService('ReportService', version='v202408')
            
            print(f"[INFO] Creating simple report query...")
            from googleads.ad_manager import ReportQuery, Dimension, Column
            
            # Create a simple report query
            report_query = {
                'dimensions': [Dimension.DATE],
                'columns': [Column.AD_SERVER_IMPRESSIONS, Column.AD_SERVER_CLICKS],
                'dateRangeType': 'LAST_7_DAYS'
            }
            
            print(f"[INFO] Running report job...")
            report_job = report_service.runReportJob(report_query)
            print(f"[SUCCESS] Report job created: {report_job.id}")
            
            print(f"[SUCCESS] Service account authentication works!")
            
        finally:
            # Clean up temp file
            os.unlink(temp_yaml_path)
            
    except Exception as e:
        print(f"[ERROR] Service account test failed: {e}")
        import traceback
        traceback.print_exc()
        
        # Try OAuth2 approach with corrected config
        print(f"\n[INFO] Trying OAuth2 with corrected configuration...")
        try:
            config = {
                'ad_manager': {
                    'application_name': 'HRIS Ad Manager Integration',
                    'network_code': os.getenv('GOOGLE_AD_MANAGER_NETWORK_CODE'),
                    'client_id': os.getenv('GOOGLE_ADS_CLIENT_ID'),
                    'client_secret': os.getenv('GOOGLE_ADS_CLIENT_SECRET'),
                    'refresh_token': os.getenv('GOOGLE_ADS_REFRESH_TOKEN')
                }
            }
            
            # Write temporary YAML file
            with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
                yaml.dump(config, f)
                temp_yaml_path = f.name
            
            try:
                print(f"[INFO] Creating Ad Manager client with OAuth2...")
                client = ad_manager.AdManagerClient.LoadFromStorage(temp_yaml_path)
                
                print(f"[INFO] Getting NetworkService...")
                network_service = client.GetService('NetworkService', version='v202408')
                
                print(f"[INFO] Calling getCurrentNetwork...")
                network = network_service.getCurrentNetwork()
                print(f"[SUCCESS] Network: {network.displayName} ({network.networkCode})")
                
                print(f"[SUCCESS] OAuth2 authentication works!")
                
            finally:
                # Clean up temp file
                os.unlink(temp_yaml_path)
                
        except Exception as e2:
            print(f"[ERROR] OAuth2 test also failed: {e2}")
            
            # Check if it's a network access issue
            error_str = str(e2).lower()
            if 'network' in error_str and 'code' in error_str:
                print(f"\n[DIAGNOSIS] Network code issue detected")
                print(f"[INFO] Current network code: {os.getenv('GOOGLE_AD_MANAGER_NETWORK_CODE')}")
                print(f"[SUGGESTION] Verify that:")
                print(f"  1. The network code is correct")
                print(f"  2. Your account has access to this network")
                print(f"  3. The network is active and not suspended")

if __name__ == "__main__":
    test_service_account()
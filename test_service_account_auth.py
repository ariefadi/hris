#!/usr/bin/env python
import os
import sys
import django
from datetime import datetime, timedelta

# Setup Django environment
sys.path.append('/Users/ariefdwicahyoadi/hris')
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'hris.settings')
django.setup()

# Apply patches first
from management.googleads_patch_v2 import apply_googleads_patches
apply_googleads_patches()
print("[PATCH] GoogleAds patches applied")

# Import after Django setup and patches
from management.database import data_mysql
from google.oauth2 import service_account
from googleads import ad_manager
import tempfile
import yaml
import json

def test_service_account_auth():
    print("=== Testing Service Account Authentication ===")
    
    # Load service account credentials
    print("\n1. Loading service account credentials...")
    try:
        service_account_path = '/Users/ariefdwicahyoadi/hris/service-account-key.json'
        
        with open(service_account_path, 'r') as f:
            service_account_info = json.load(f)
        
        print(f"✓ Service account loaded")
        print(f"   Project ID: {service_account_info['project_id']}")
        print(f"   Client Email: {service_account_info['client_email']}")
        
        # Create credentials from service account
        credentials = service_account.Credentials.from_service_account_file(
            service_account_path,
            scopes=['https://www.googleapis.com/auth/dfp']
        )
        
        print(f"✓ Service account credentials created")
        
    except Exception as e:
        print(f"✗ Error loading service account: {e}")
        return
    
    # Get network code from database
    print("\n2. Getting network code from database...")
    try:
        users_result = data_mysql().data_user_by_params()
        if not users_result['status'] or not users_result['data']:
            print("✗ No users found")
            return
        
        test_user = None
        for user in users_result['data']:
            if user.get('user_mail'):
                test_user = user
                break
        
        if not test_user:
            print("✗ No user with email found")
            return
        
        user_email = test_user['user_mail']
        
        # Get network code
        db = data_mysql()
        sql = """
            SELECT network_code, developer_token
            FROM app_users 
            WHERE user_mail = %s
        """
        
        db.cur_hris.execute(sql, (user_email,))
        user_data = db.cur_hris.fetchone()
        
        if not user_data:
            print(f"✗ No data for {user_email}")
            return
        
        network_code = user_data.get('network_code')
        developer_token = user_data.get('developer_token')
        
        print(f"✓ Network code: {network_code}")
        print(f"✓ Developer token: {developer_token[:20]}...")
        
    except Exception as e:
        print(f"✗ Error getting network code: {e}")
        return
    
    # Create YAML configuration for service account
    print("\n3. Creating Ad Manager configuration with service account...")
    try:
        yaml_content = {
            'ad_manager': {
                'application_name': 'HRIS AdX Integration',
                'developer_token': developer_token,
                'service_account_email': service_account_info['client_email'],
                'key_file': service_account_path,
                'network_code': network_code
            }
        }
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as temp_file:
            yaml.dump(yaml_content, temp_file, default_flow_style=False)
            yaml_path = temp_file.name
        
        print(f"✓ YAML config created with service account")
        
    except Exception as e:
        print(f"✗ Error creating YAML: {e}")
        return
    
    # Test Ad Manager client with service account
    print("\n4. Testing Ad Manager client with service account...")
    try:
        client = ad_manager.AdManagerClient.LoadFromStorage(yaml_path)
        print("✓ Ad Manager client created successfully with service account")
        
    except Exception as e:
        print(f"✗ Error creating client with service account: {e}")
        
        # Try alternative approach - manual service account setup
        print("\n   Trying alternative service account setup...")
        try:
            # Create client manually with service account
            yaml_content_alt = {
                'ad_manager': {
                    'application_name': 'HRIS AdX Integration',
                    'developer_token': developer_token,
                    'network_code': network_code,
                    'service_account_email': service_account_info['client_email'],
                    'private_key': service_account_info['private_key'],
                    'private_key_id': service_account_info['private_key_id']
                }
            }
            
            with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as temp_file:
                yaml.dump(yaml_content_alt, temp_file, default_flow_style=False)
                yaml_path_alt = temp_file.name
            
            client = ad_manager.AdManagerClient.LoadFromStorage(yaml_path_alt)
            print("✓ Alternative service account setup successful")
            yaml_path = yaml_path_alt
            
        except Exception as e2:
            print(f"✗ Alternative setup also failed: {e2}")
            os.unlink(yaml_path)
            return
    
    # Test network access with service account
    print("\n5. Testing network access with service account...")
    try:
        network_service = client.GetService('NetworkService', version='v202408')
        current_network = network_service.getCurrentNetwork()
        
        print(f"✓ Network access successful with service account")
        print(f"   Network Name: {current_network.displayName}")
        print(f"   Network Code: {current_network.networkCode}")
        print(f"   Currency: {current_network.currencyCode}")
        
    except Exception as e:
        print(f"✗ Error accessing network with service account: {e}")
        os.unlink(yaml_path)
        return
    
    # Test AdX report with service account
    print("\n6. Testing AdX report with service account...")
    try:
        report_service = client.GetService('ReportService', version='v202408')
        
        # Try simple configuration first
        report_job = {
            'reportQuery': {
                'dimensions': ['DATE'],
                'columns': ['TOTAL_IMPRESSIONS'],
                'dateRangeType': 'LAST_7_DAYS'
            }
        }
        
        result = report_service.runReportJob(report_job)
        
        if result and hasattr(result, 'id'):
            print(f"✓ Report job created successfully (ID: {result.id})")
            
            # Check status
            import time
            time.sleep(3)
            
            status = report_service.getReportJob(result.id)
            print(f"   Report Status: {status.reportJobStatus}")
            
            if status.reportJobStatus == 'COMPLETED':
                print(f"✓ Report completed successfully")
                
                # Download report
                try:
                    report_downloader = client.GetDataDownloader(version='v202408')
                    report_data = report_downloader.DownloadReportToString(
                        result.id, 'CSV_DUMP'
                    )
                    
                    if report_data:
                        lines = report_data.strip().split('\n')
                        print(f"✓ Report downloaded successfully ({len(lines)} lines)")
                        
                        if len(lines) > 1:
                            print(f"✓ Data found: {lines[1]}")
                            print(f"\n🎉 SERVICE ACCOUNT AUTHENTICATION BERHASIL!")
                            print(f"Data dapat diambil menggunakan service account.")
                        else:
                            print(f"⚠ No data for this period, but authentication works")
                    else:
                        print(f"⚠ Report completed but no data returned")
                        
                except Exception as download_error:
                    print(f"✗ Error downloading report: {download_error}")
            
            elif status.reportJobStatus == 'FAILED':
                print(f"✗ Report failed")
            else:
                print(f"⚠ Report still running")
                
        else:
            print(f"✗ Failed to create report job")
            
    except Exception as e:
        print(f"✗ Error testing report with service account: {e}")
    
    # Test different AdX columns with service account
    print("\n7. Testing AdX-specific columns with service account...")
    
    adx_columns_to_test = [
        ['AD_EXCHANGE_IMPRESSIONS'],
        ['AD_EXCHANGE_TOTAL_EARNINGS'],
        ['AD_EXCHANGE_CLICKS'],
        ['TOTAL_IMPRESSIONS'],
        ['TOTAL_REVENUE']
    ]
    
    successful_columns = []
    
    for columns in adx_columns_to_test:
        try:
            print(f"\n   Testing columns: {columns}")
            
            report_job = {
                'reportQuery': {
                    'dimensions': ['DATE'],
                    'columns': columns,
                    'dateRangeType': 'LAST_7_DAYS'
                }
            }
            
            result = report_service.runReportJob(report_job)
            
            if result and hasattr(result, 'id'):
                print(f"   ✓ Report job created for {columns}")
                successful_columns.extend(columns)
            else:
                print(f"   ✗ Failed to create report for {columns}")
                
        except Exception as e:
            print(f"   ✗ Error with {columns}: {e}")
    
    # Cleanup
    try:
        os.unlink(yaml_path)
    except:
        pass
    
    print(f"\n=== SUMMARY ===")
    
    if successful_columns:
        print(f"\n✅ SERVICE ACCOUNT AUTHENTICATION BERHASIL!")
        print(f"\nKolom yang berhasil diuji:")
        for col in set(successful_columns):
            print(f"   • {col}")
        
        print(f"\n🔧 LANGKAH SELANJUTNYA:")
        print(f"1. Update konfigurasi aplikasi untuk menggunakan service account")
        print(f"2. Ganti OAuth2 authentication dengan service account authentication")
        print(f"3. Update utils.py untuk menggunakan service account")
        
        print(f"\n📝 KONFIGURASI YANG DIREKOMENDASIKAN:")
        print(f"```yaml")
        print(f"ad_manager:")
        print(f"  application_name: 'HRIS AdX Integration'")
        print(f"  developer_token: '{developer_token}'")
        print(f"  service_account_email: '{service_account_info['client_email']}'")
        print(f"  key_file: '/path/to/service-account-key.json'")
        print(f"  network_code: '{network_code}'")
        print(f"```")
        
    else:
        print(f"\n❌ Service account authentication juga gagal")
        print(f"Kemungkinan masalah:")
        print(f"1. Service account tidak memiliki akses ke Google Ad Manager")
        print(f"2. Network code tidak valid")
        print(f"3. Developer token tidak valid")
        print(f"4. Service account perlu ditambahkan ke Google Ad Manager sebagai user")

if __name__ == "__main__":
    test_service_account_auth()
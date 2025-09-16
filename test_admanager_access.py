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
from googleads import ad_manager
import tempfile
import yaml

def test_admanager_access():
    print("=== Testing Google Ad Manager API Access ===")
    
    # Get user credentials
    print("\n1. Getting user credentials...")
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
        print(f"✓ Using email: {user_email}")
        
        # Get detailed credentials
        db = data_mysql()
        sql = """
            SELECT client_id, client_secret, refresh_token, network_code, developer_token
            FROM app_users 
            WHERE user_mail = %s
        """
        
        db.cur_hris.execute(sql, (user_email,))
        user_data = db.cur_hris.fetchone()
        
        if not user_data:
            print(f"✗ No credential data for {user_email}")
            return
        
        client_id = user_data.get('client_id')
        client_secret = user_data.get('client_secret')
        refresh_token = user_data.get('refresh_token')
        network_code = user_data.get('network_code')
        developer_token = user_data.get('developer_token')
        
        print(f"✓ All credentials found")
        
    except Exception as e:
        print(f"✗ Error getting credentials: {e}")
        return
    
    # Create YAML configuration
    print("\n2. Creating Ad Manager configuration...")
    try:
        yaml_content = {
            'ad_manager': {
                'application_name': 'HRIS AdX Integration',
                'developer_token': developer_token,
                'client_id': client_id,
                'client_secret': client_secret,
                'refresh_token': refresh_token,
                'network_code': network_code
            }
        }
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as temp_file:
            yaml.dump(yaml_content, temp_file, default_flow_style=False)
            yaml_path = temp_file.name
        
        print(f"✓ YAML config created: {yaml_path}")
        
    except Exception as e:
        print(f"✗ Error creating YAML: {e}")
        return
    
    # Test Ad Manager client initialization
    print("\n3. Testing Ad Manager client initialization...")
    try:
        client = ad_manager.AdManagerClient.LoadFromStorage(yaml_path)
        print("✓ Ad Manager client created successfully")
        
    except Exception as e:
        print(f"✗ Error creating client: {e}")
        os.unlink(yaml_path)
        return
    
    # Test basic network access
    print("\n4. Testing network access...")
    try:
        network_service = client.GetService('NetworkService', version='v202408')
        current_network = network_service.getCurrentNetwork()
        
        print(f"✓ Network access successful")
        print(f"   Network Name: {current_network.displayName}")
        print(f"   Network Code: {current_network.networkCode}")
        print(f"   Currency Code: {current_network.currencyCode}")
        print(f"   Time Zone: {current_network.timeZone}")
        
    except Exception as e:
        print(f"✗ Error accessing network: {e}")
        os.unlink(yaml_path)
        return
    
    # Test available services
    print("\n5. Testing available services...")
    services_to_test = [
        'InventoryService',
        'UserService', 
        'ReportService',
        'PublisherQueryLanguageService'
    ]
    
    available_services = []
    for service_name in services_to_test:
        try:
            service = client.GetService(service_name, version='v202408')
            available_services.append(service_name)
            print(f"✓ {service_name} accessible")
        except Exception as e:
            print(f"✗ {service_name} not accessible: {e}")
    
    # Test AdX specific access
    print("\n6. Testing AdX specific access...")
    try:
        # Test if we can access AdX related data through PQL
        pql_service = client.GetService('PublisherQueryLanguageService', version='v202408')
        
        # Simple query to test AdX access
        statement = {
            'query': 'SELECT Id, Name FROM Ad_Unit LIMIT 5'
        }
        
        result = pql_service.select(statement)
        
        if result and hasattr(result, 'rows') and result.rows:
            print(f"✓ PQL service working - found {len(result.rows)} ad units")
            for row in result.rows[:3]:  # Show first 3
                values = [str(value.value) if hasattr(value, 'value') else str(value) for value in row.values]
                print(f"   Ad Unit: {values}")
        else:
            print("⚠ PQL service working but no ad units found")
            
    except Exception as e:
        print(f"✗ Error testing PQL service: {e}")
    
    # Test Report Service specifically for AdX
    print("\n7. Testing Report Service for AdX capabilities...")
    try:
        report_service = client.GetService('ReportService', version='v202408')
        
        # Create a simple AdX report query
        report_job = {
            'reportQuery': {
                'dimensions': ['DATE'],
                'columns': ['AD_EXCHANGE_IMPRESSIONS'],
                'dateRangeType': 'LAST_7_DAYS'
            }
        }
        
        # Try to run the report
        report_job = report_service.runReportJob(report_job)
        
        if report_job and hasattr(report_job, 'id'):
            print(f"✓ AdX report job created successfully")
            print(f"   Report Job ID: {report_job.id}")
            
            # Check report status
            import time
            max_wait = 30  # seconds
            wait_time = 0
            
            while wait_time < max_wait:
                report_job_status = report_service.getReportJob(report_job.id)
                print(f"   Report Status: {report_job_status.reportJobStatus}")
                
                if report_job_status.reportJobStatus == 'COMPLETED':
                    print("✓ AdX report completed successfully")
                    
                    # Try to download report
                    try:
                        report_downloader = client.GetDataDownloader(version='v202408')
                        report_data = report_downloader.DownloadReportToString(
                            report_job.id, 'CSV_DUMP'
                        )
                        
                        if report_data:
                            lines = report_data.strip().split('\n')
                            print(f"✓ Report downloaded - {len(lines)} lines")
                            
                            if len(lines) > 1:  # Has data beyond header
                                print("✓ AdX data found in report")
                                print(f"   Sample data: {lines[1] if len(lines) > 1 else 'No data'}")
                            else:
                                print("⚠ Report completed but no AdX data found")
                                print("   This could mean:")
                                print("   - No AdX traffic in the last 7 days")
                                print("   - AdX not properly configured")
                                print("   - Account doesn't have AdX access")
                        else:
                            print("⚠ Report completed but download failed")
                            
                    except Exception as download_error:
                        print(f"✗ Error downloading report: {download_error}")
                    
                    break
                    
                elif report_job_status.reportJobStatus == 'FAILED':
                    print(f"✗ AdX report failed")
                    break
                    
                else:
                    time.sleep(2)
                    wait_time += 2
            
            if wait_time >= max_wait:
                print(f"⚠ Report still running after {max_wait} seconds")
                
        else:
            print("✗ Failed to create AdX report job")
            
    except Exception as e:
        print(f"✗ Error testing Report Service: {e}")
        if "PERMISSION_DENIED" in str(e):
            print("   This indicates insufficient permissions for AdX reporting")
        elif "INVALID_QUERY" in str(e):
            print("   This indicates the AdX query format is not supported")
    
    # Cleanup
    try:
        os.unlink(yaml_path)
    except:
        pass
    
    print("\n=== Summary ===")
    print("\nIf network access works but AdX reports fail:")
    print("1. Check if your Ad Manager account has AdX enabled")
    print("2. Verify AdX permissions in Google Ad Manager Console")
    print("3. Ensure your account has historical AdX data")
    print("4. Check if AdX reporting is enabled for your network")
    
    print("\nIf everything works but no data:")
    print("1. Your account may not have AdX traffic")
    print("2. AdX may not be properly configured")
    print("3. Try checking different date ranges")
    print("4. Verify AdX setup in Google Ad Manager Console")

if __name__ == "__main__":
    test_admanager_access()
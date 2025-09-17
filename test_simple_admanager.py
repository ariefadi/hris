#!/usr/bin/env python3
"""
Simple test of Ad Manager API without complex patches
"""

import os
import sys
import django
import tempfile
import yaml

# Add the project directory to Python path
sys.path.append('/Users/ariefdwicahyoadi/hris')

# Setup Django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'hris.settings')
django.setup()

from management.utils import get_user_adx_credentials
from googleads import ad_manager

def test_simple_admanager():
    """Test simple Ad Manager API access"""
    print("=== Simple Ad Manager API Test ===")
    
    test_email = "adiarief463@gmail.com"
    
    print(f"\n1. Getting credentials for: {test_email}")
    
    # Get credentials
    creds_result = get_user_adx_credentials(test_email)
    if not creds_result['status']:
        print(f"✗ Failed to get credentials: {creds_result['error']}")
        return
        
    credentials = creds_result['credentials']
    print("✓ Successfully retrieved credentials")
    
    # Create simple YAML without patches
    print(f"\n2. Creating simple YAML configuration...")
    
    developer_token = str(credentials.get('developer_token', '')).strip()
    client_id = str(credentials.get('client_id', '')).strip()
    client_secret = str(credentials.get('client_secret', '')).strip()
    refresh_token = str(credentials.get('refresh_token', '')).strip()
    network_code = int(credentials.get('network_code', 23303534834))
    
    # Create minimal YAML configuration
    yaml_content = {
        'ad_manager': {
            'developer_token': developer_token,
            'client_id': client_id,
            'client_secret': client_secret,
            'refresh_token': refresh_token,
            'application_name': 'AdX Manager Dashboard',
            'network_code': network_code
        }
    }
    
    # Write to temporary file
    yaml_file = tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False, encoding='utf-8')
    yaml.dump(yaml_content, yaml_file, default_flow_style=False)
    yaml_file.close()
    
    print(f"YAML file created: {yaml_file.name}")
    
    try:
        print(f"\n3. Testing Ad Manager client creation...")
        
        # Create client without patches
        client = ad_manager.AdManagerClient.LoadFromStorage(yaml_file.name)
        print("✓ Successfully created Ad Manager client")
        
        print(f"\n4. Testing NetworkService...")
        
        # Test different API versions
        versions = ['v202408', 'v202411', 'v202502', 'v202505']
        
        for version in versions:
            try:
                print(f"\nTrying version {version}...")
                network_service = client.GetService('NetworkService', version=version)
                print(f"✓ Successfully created NetworkService with version {version}")
                
                # Try to get current network
                print(f"Getting current network...")
                network = network_service.getCurrentNetwork()
                print(f"✓ Successfully got network: {network.displayName} (Code: {network.networkCode})")
                
                # If we get here, this version works
                print(f"\n🎉 Version {version} works successfully!")
                break
                
            except Exception as e:
                print(f"✗ Version {version} failed: {str(e)}")
                continue
        
    except Exception as e:
        print(f"✗ Error: {str(e)}")
        import traceback
        print(f"Traceback: {traceback.format_exc()}")
        
    finally:
        # Cleanup
        if os.path.exists(yaml_file.name):
            os.unlink(yaml_file.name)
            print(f"\nCleaned up temporary file: {yaml_file.name}")

if __name__ == "__main__":
    test_simple_admanager()
    print("\n=== Test Complete ===")
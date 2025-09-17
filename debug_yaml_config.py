#!/usr/bin/env python3
"""
Debug YAML configuration for user credentials
"""

import os
import sys
import django
import tempfile

# Add the project directory to Python path
sys.path.append('/Users/ariefdwicahyoadi/hris')

# Setup Django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'hris.settings')
django.setup()

from management.utils import get_user_adx_credentials, get_user_ad_manager_client

def debug_yaml_config():
    """Debug YAML configuration creation"""
    print("=== Debug YAML Configuration ===")
    
    test_email = "adiarief463@gmail.com"
    
    print(f"\n1. Getting credentials for: {test_email}")
    
    # Get credentials
    creds_result = get_user_adx_credentials(test_email)
    if not creds_result['status']:
        print(f"✗ Failed to get credentials: {creds_result['error']}")
        return
        
    credentials = creds_result['credentials']
    print("✓ Successfully retrieved credentials")
    
    # Print credential details (masked)
    print(f"\nCredential details:")
    print(f"  - Developer Token: {credentials.get('developer_token', '')[:10]}...")
    print(f"  - Client ID: {credentials.get('client_id', '')[:20]}...")
    print(f"  - Client Secret: {credentials.get('client_secret', '')[:10]}...")
    print(f"  - Refresh Token: {credentials.get('refresh_token', '')[:20]}...")
    print(f"  - Network Code: {credentials.get('network_code')}")
    print(f"  - Email: {credentials.get('email')}")
    
    # Create YAML manually to see the content
    print(f"\n2. Creating YAML configuration manually...")
    
    developer_token = str(credentials.get('developer_token', '')).strip()
    client_id = str(credentials.get('client_id', '')).strip()
    client_secret = str(credentials.get('client_secret', '')).strip()
    refresh_token = str(credentials.get('refresh_token', '')).strip()
    
    network_code = 23303534834
    try:
        if 'network_code' in credentials:
            network_code = int(credentials['network_code'])
    except (ValueError, TypeError):
        network_code = 23303534834
    
    yaml_content = f"""ad_manager:
  developer_token: "{developer_token}"
  client_id: "{client_id}"
  client_secret: "{client_secret}"
  refresh_token: "{refresh_token}"
  application_name: "AdX Manager Dashboard"
  network_code: {network_code}
use_proto_plus: true
"""
    
    print("YAML Content (with masked credentials):")
    masked_yaml = yaml_content
    masked_yaml = masked_yaml.replace(developer_token, developer_token[:10] + "...")
    masked_yaml = masked_yaml.replace(client_id, client_id[:20] + "...")
    masked_yaml = masked_yaml.replace(client_secret, client_secret[:10] + "...")
    masked_yaml = masked_yaml.replace(refresh_token, refresh_token[:20] + "...")
    print(masked_yaml)
    
    # Test client creation
    print(f"\n3. Testing Ad Manager client creation...")
    
    client_result = get_user_ad_manager_client(test_email)
    print(f"Client creation status: {client_result['status']}")
    
    if client_result['status']:
        print("✓ Successfully created Ad Manager client")
        
        # Try to get services without calling network methods
        try:
            client = client_result['client']
            print(f"\n4. Testing service creation...")
            
            # Test different service versions
            versions = ['v202408', 'v202411', 'v202502', 'v202505']
            
            for version in versions:
                try:
                    network_service = client.GetService('NetworkService', version=version)
                    print(f"✓ Successfully created NetworkService with version {version}")
                    break
                except Exception as e:
                    print(f"✗ Failed to create NetworkService with version {version}: {str(e)}")
                    
        except Exception as e:
            print(f"✗ Error testing service creation: {str(e)}")
            
    else:
        print(f"✗ Failed to create Ad Manager client: {client_result['error']}")

if __name__ == "__main__":
    debug_yaml_config()
    print("\n=== Debug Complete ===")
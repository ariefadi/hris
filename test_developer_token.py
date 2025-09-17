#!/usr/bin/env python3
"""
Test developer token validity for Ad Manager API
"""

import os
import sys
import django
import requests
import json

# Add the project directory to Python path
sys.path.append('/Users/ariefdwicahyoadi/hris')

# Setup Django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'hris.settings')
django.setup()

from management.utils import get_user_adx_credentials

def test_developer_token():
    """Test developer token validity for Ad Manager API"""
    print("=== Developer Token Test ===")
    
    test_email = "adiarief463@gmail.com"
    
    print(f"\n1. Getting credentials for: {test_email}")
    
    # Get credentials
    creds_result = get_user_adx_credentials(test_email)
    if not creds_result['status']:
        print(f"✗ Failed to get credentials: {creds_result['error']}")
        return
        
    credentials = creds_result['credentials']
    print("✓ Successfully retrieved credentials")
    
    client_id = credentials.get('client_id')
    client_secret = credentials.get('client_secret')
    refresh_token = credentials.get('refresh_token')
    developer_token = credentials.get('developer_token')
    network_code = credentials.get('network_code')
    
    print(f"\nCredential details:")
    print(f"Developer Token: {developer_token}")
    print(f"Network Code: {network_code}")
    print(f"Client ID: {client_id[:20]}...")
    
    # First get a fresh access token
    print(f"\n2. Getting fresh access token...")
    
    token_url = "https://oauth2.googleapis.com/token"
    data = {
        'client_id': client_id,
        'client_secret': client_secret,
        'refresh_token': refresh_token,
        'grant_type': 'refresh_token'
    }
    
    try:
        response = requests.post(token_url, data=data)
        
        if response.status_code != 200:
            print(f"✗ Failed to get access token: {response.text}")
            return
            
        token_data = response.json()
        access_token = token_data.get('access_token')
        print(f"✓ Got fresh access token: {access_token[:20]}...")
        
        # Test developer token with Ad Manager API
        print(f"\n3. Testing developer token with Ad Manager API...")
        
        # Try to make a simple API call to validate developer token
        # We'll use the REST API endpoint for testing
        
        # Check if developer token format is correct
        if not developer_token or len(developer_token) < 10:
            print(f"✗ Developer token appears to be invalid: '{developer_token}'")
            print("   Developer tokens should be longer alphanumeric strings")
            return
            
        print(f"✓ Developer token format appears valid")
        
        # Check network code
        if not network_code or network_code == 0:
            print(f"✗ Network code appears to be invalid: {network_code}")
            return
            
        print(f"✓ Network code appears valid: {network_code}")
        
        # Test with a simple SOAP request to validate credentials
        print(f"\n4. Testing SOAP request with credentials...")
        
        # Create a minimal SOAP envelope for testing
        soap_envelope = f'''
        <soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
            <soap:Header>
                <RequestHeader xmlns="https://www.google.com/apis/ads/publisher/v202408">
                    <networkCode>{network_code}</networkCode>
                    <applicationName>AdX Manager Dashboard</applicationName>
                </RequestHeader>
            </soap:Header>
            <soap:Body>
                <getCurrentNetwork xmlns="https://www.google.com/apis/ads/publisher/v202408"/>
            </soap:Body>
        </soap:Envelope>
        '''
        
        headers = {
            'Content-Type': 'text/xml; charset=utf-8',
            'SOAPAction': '',
            'Authorization': f'Bearer {access_token}',
            'developerToken': developer_token
        }
        
        # Ad Manager API endpoint
        api_url = "https://ads.google.com/apis/ads/publisher/v202408/NetworkService"
        
        print(f"Making SOAP request to: {api_url}")
        print(f"Headers: {dict((k, v[:20] + '...' if len(str(v)) > 20 else v) for k, v in headers.items())}")
        
        soap_response = requests.post(api_url, data=soap_envelope, headers=headers)
        
        print(f"\nSOAP Response status: {soap_response.status_code}")
        print(f"Response headers: {dict(soap_response.headers)}")
        
        if soap_response.status_code == 200:
            print("✓ SOAP request successful!")
            print(f"Response content (first 500 chars): {soap_response.text[:500]}...")
        else:
            print(f"✗ SOAP request failed")
            print(f"Response content: {soap_response.text}")
            
            # Analyze common error patterns
            response_text = soap_response.text.lower()
            
            if 'authenticationerror' in response_text:
                print("\n❌ Authentication Error: Invalid developer token or access token")
            elif 'networkerror' in response_text:
                print("\n❌ Network Error: Invalid network code or no access to this network")
            elif 'permissionerror' in response_text:
                print("\n❌ Permission Error: Insufficient permissions for Ad Manager API")
            elif 'quotaerror' in response_text:
                print("\n❌ Quota Error: API quota exceeded")
            else:
                print(f"\n❌ Unknown error: {soap_response.status_code}")
                
    except Exception as e:
        print(f"✗ Error testing developer token: {str(e)}")
        import traceback
        print(f"Traceback: {traceback.format_exc()}")

if __name__ == "__main__":
    test_developer_token()
    print("\n=== Test Complete ===")
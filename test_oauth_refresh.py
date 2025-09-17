#!/usr/bin/env python3
"""
Test OAuth refresh token validity
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

def test_oauth_refresh():
    """Test OAuth refresh token validity"""
    print("=== OAuth Refresh Token Test ===")
    
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
    
    print(f"\n2. Testing refresh token validity...")
    print(f"Client ID: {client_id[:20]}...")
    print(f"Client Secret: {client_secret[:10]}...")
    print(f"Refresh Token: {refresh_token[:20]}...")
    
    # Test refresh token by requesting new access token
    token_url = "https://oauth2.googleapis.com/token"
    
    data = {
        'client_id': client_id,
        'client_secret': client_secret,
        'refresh_token': refresh_token,
        'grant_type': 'refresh_token'
    }
    
    try:
        print(f"\n3. Requesting new access token...")
        response = requests.post(token_url, data=data)
        
        print(f"Response status: {response.status_code}")
        
        if response.status_code == 200:
            token_data = response.json()
            print("✓ Successfully refreshed access token!")
            print(f"Access token: {token_data.get('access_token', '')[:20]}...")
            print(f"Token type: {token_data.get('token_type')}")
            print(f"Expires in: {token_data.get('expires_in')} seconds")
            
            # Test if we can use this access token to make API calls
            access_token = token_data.get('access_token')
            
            print(f"\n4. Testing access token with Google API...")
            
            # Test with Google OAuth2 userinfo endpoint
            userinfo_url = "https://www.googleapis.com/oauth2/v2/userinfo"
            headers = {'Authorization': f'Bearer {access_token}'}
            
            userinfo_response = requests.get(userinfo_url, headers=headers)
            
            if userinfo_response.status_code == 200:
                user_info = userinfo_response.json()
                print("✓ Access token is valid!")
                print(f"User email: {user_info.get('email')}")
                print(f"User name: {user_info.get('name')}")
                
                # Check if the email matches
                if user_info.get('email') == test_email:
                    print("✓ Email matches the test user")
                else:
                    print(f"⚠️  Email mismatch: expected {test_email}, got {user_info.get('email')}")
                    
            else:
                print(f"✗ Access token test failed: {userinfo_response.status_code}")
                print(f"Response: {userinfo_response.text}")
                
        else:
            print(f"✗ Failed to refresh access token")
            error_data = response.json() if response.headers.get('content-type', '').startswith('application/json') else response.text
            print(f"Error: {error_data}")
            
            # Common error messages
            if response.status_code == 400:
                if 'invalid_grant' in str(error_data):
                    print("\n❌ The refresh token is invalid or expired.")
                    print("   This usually means the user needs to re-authorize the application.")
                elif 'invalid_client' in str(error_data):
                    print("\n❌ The client credentials (client_id/client_secret) are invalid.")
                else:
                    print(f"\n❌ Bad request: {error_data}")
            elif response.status_code == 401:
                print("\n❌ Unauthorized: Invalid client credentials")
                
    except Exception as e:
        print(f"✗ Error testing refresh token: {str(e)}")
        import traceback
        print(f"Traceback: {traceback.format_exc()}")

if __name__ == "__main__":
    test_oauth_refresh()
    print("\n=== Test Complete ===")
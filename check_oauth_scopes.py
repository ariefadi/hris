#!/usr/bin/env python3
"""
Check OAuth scopes and token info
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

def check_oauth_scopes():
    """Check OAuth scopes and token info"""
    print("=== OAuth Scopes Check ===")
    
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
    
    # Get fresh access token
    print(f"\n2. Getting fresh access token...")
    
    token_url = "https://oauth2.googleapis.com/token"
    data = {
        'client_id': client_id,
        'client_secret': client_secret,
        'refresh_token': refresh_token,
        'grant_type': 'refresh_token'
    }
    print(f"✓ Got data params nya: {data}")
    try:
        response = requests.post(token_url, data=data)
    
        if response.status_code != 200:
            print(f"✗ Failed to get access token: {response.text}")
            return
            
        token_data = response.json()
        
        access_token = token_data.get('access_token')
        print(f"✓ Got fresh access token: {access_token}")
        
        # Check token info and scopes
        print(f"\n3. Checking token info and scopes...")
        
        # Use Google's tokeninfo endpoint
        tokeninfo_url = f"https://oauth2.googleapis.com/tokeninfo?access_token={access_token}"
        
        tokeninfo_response = requests.get(tokeninfo_url)
        
        if tokeninfo_response.status_code == 200:
            token_info = tokeninfo_response.json()
            print("✓ Token info retrieved successfully")
            
            print(f"\nToken details:")
            print(f"  - Audience (client_id): {token_info.get('aud')}")
            print(f"  - Issued to: {token_info.get('azp')}")
            print(f"  - Email: {token_info.get('email')}")
            print(f"  - Email verified: {token_info.get('email_verified')}")
            print(f"  - Expires in: {token_info.get('expires_in')} seconds")
            print(f"  - Scope: {token_info.get('scope')}")
            
            # Check if required scopes are present
            scopes = token_info.get('scope', '').split()
            print(f"\n4. Analyzing scopes...")
            print(f"Current scopes: {scopes}")
            
            # Required scopes for Ad Manager API
            required_scopes = [
                'https://www.googleapis.com/auth/dfp',  # Ad Manager API
                # Alternative scopes:
                # 'https://www.googleapis.com/auth/adexchange.buyer',
                # 'https://www.googleapis.com/auth/adexchange.seller'
            ]
            
            print(f"\nRequired scopes for Ad Manager API:")
            for scope in required_scopes:
                if scope in scopes:
                    print(f"  ✓ {scope} - PRESENT")
                else:
                    print(f"  ✗ {scope} - MISSING")
                    
            # Check for any Ad Manager related scopes
            ad_manager_scopes = [s for s in scopes if 'dfp' in s or 'adexchange' in s or 'ads' in s]
            
            if ad_manager_scopes:
                print(f"\nAd Manager related scopes found:")
                for scope in ad_manager_scopes:
                    print(f"  ✓ {scope}")
            else:
                print(f"\n❌ No Ad Manager related scopes found!")
                print(f"   This explains why the API calls are failing with 401 Unauthorized.")
                print(f"   The user needs to re-authorize with the correct scopes.")
                
            # Suggest solution
            if not any('dfp' in s for s in scopes):
                print(f"\n💡 Solution:")
                print(f"   1. The user needs to re-authorize the application")
                print(f"   2. Make sure to request the 'https://www.googleapis.com/auth/dfp' scope")
                print(f"   3. This scope provides access to Google Ad Manager API")
                
        else:
            print(f"✗ Failed to get token info: {tokeninfo_response.status_code}")
            print(f"Response: {tokeninfo_response.text}")
            
        # Test with different API endpoints to see what works
        print(f"\n5. Testing access to different Google APIs...")
        
        headers = {'Authorization': f'Bearer {access_token}'}
        
        # Test endpoints
        test_endpoints = [
            ('Google OAuth2 UserInfo', 'https://www.googleapis.com/oauth2/v2/userinfo'),
            ('Google Plus (deprecated)', 'https://www.googleapis.com/plus/v1/people/me'),
            ('Google Drive', 'https://www.googleapis.com/drive/v3/about'),
            ('Google Ads', 'https://googleads.googleapis.com/v16/customers'),
            ('Ad Manager (test)', 'https://admanager.googleapis.com/v1/networks')
        ]
        
        for name, url in test_endpoints:
            try:
                test_response = requests.get(url, headers=headers)
                if test_response.status_code == 200:
                    print(f"  ✓ {name}: Access granted")
                elif test_response.status_code == 401:
                    print(f"  ✗ {name}: Unauthorized (401)")
                elif test_response.status_code == 403:
                    print(f"  ⚠️  {name}: Forbidden (403) - scope issue")
                else:
                    print(f"  ? {name}: {test_response.status_code}")
            except Exception as e:
                print(f"  ✗ {name}: Error - {str(e)}")
                
    except Exception as e:
        print(f"✗ Error checking OAuth scopes: {str(e)}")
        import traceback
        print(f"Traceback: {traceback.format_exc()}")

if __name__ == "__main__":
    check_oauth_scopes()
    print("\n=== Check Complete ===")
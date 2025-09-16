#!/usr/bin/env python3
"""
Script to update OAuth scopes for Google Ad Manager API authentication
Based on the latest Google documentation and API requirements
"""

import os
import sys
import django
import requests
import json
from datetime import datetime

# Add project root to Python path
sys.path.append('/Users/ariefdwicahyoadi/hris')

# Setup Django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'hris.settings')
django.setup()

from management.utils import get_user_adx_credentials
from management.database import data_mysql

def test_current_scopes():
    """
    Test current OAuth scopes and check if they work with Ad Manager API
    """
    print("=== Testing Current OAuth Scopes ===")
    
    # Get database connection
    db = data_mysql()
    
    try:
        # Get all users to test
        users = db.data_user_by_params()
        if not users:
            print("No users found in database")
            return False
            
        # Test with first user
        test_user = users[0]
        user_email = test_user.get('email')
        print(f"Testing with user: {user_email}")
        
        # Get user credentials
        credentials = get_user_adx_credentials(user_email)
        if not credentials:
            print(f"No credentials found for user: {user_email}")
            return False
            
        # Check refresh token validity
        refresh_token = credentials.get('refresh_token')
        client_id = credentials.get('client_id')
        client_secret = credentials.get('client_secret')
        
        if not all([refresh_token, client_id, client_secret]):
            print("Missing required OAuth credentials")
            return False
            
        # Test token refresh and scope validation
        token_url = 'https://oauth2.googleapis.com/token'
        token_data = {
            'client_id': client_id,
            'client_secret': client_secret,
            'refresh_token': refresh_token,
            'grant_type': 'refresh_token'
        }
        
        print("\n1. Testing token refresh...")
        response = requests.post(token_url, data=token_data)
        
        if response.status_code != 200:
            print(f"Token refresh failed: {response.status_code}")
            print(f"Response: {response.text}")
            return False
            
        token_info = response.json()
        access_token = token_info.get('access_token')
        print(f"✓ Token refresh successful")
        
        # Check token info to see current scopes
        print("\n2. Checking current token scopes...")
        token_info_url = f'https://oauth2.googleapis.com/tokeninfo?access_token={access_token}'
        scope_response = requests.get(token_info_url)
        
        if scope_response.status_code == 200:
            scope_info = scope_response.json()
            current_scopes = scope_info.get('scope', '').split()
            print(f"Current scopes: {current_scopes}")
            
            # Check for required Ad Manager scopes
            required_scopes = [
                'https://www.googleapis.com/auth/dfp',  # Ad Manager SOAP API
                'https://www.googleapis.com/auth/admanager',  # Ad Manager REST API (Beta)
            ]
            
            print("\n3. Scope validation:")
            has_required_scope = False
            for scope in required_scopes:
                if scope in current_scopes:
                    print(f"  ✓ {scope} - PRESENT")
                    has_required_scope = True
                else:
                    print(f"  ✗ {scope} - MISSING")
                    
            if not has_required_scope:
                print("\n❌ No valid Ad Manager scopes found!")
                print("User needs to re-authorize with correct scopes.")
                return False
            else:
                print("\n✅ Valid Ad Manager scope found!")
                return True
                
        else:
            print(f"Failed to get token info: {scope_response.status_code}")
            return False
            
    except Exception as e:
        print(f"Error testing scopes: {str(e)}")
        return False

def update_django_settings():
    """
    Update Django settings with correct OAuth scopes
    """
    print("\n=== Updating Django Settings ===")
    
    settings_file = '/Users/ariefdwicahyoadi/hris/hris/settings.py'
    
    try:
        with open(settings_file, 'r') as f:
            content = f.read()
            
        # Update OAuth scopes to include both SOAP and REST API scopes
        old_scope_line = "SOCIAL_AUTH_GOOGLE_OAUTH2_SCOPE = ['email', 'profile', 'https://www.googleapis.com/auth/dfp']"
        new_scope_line = "SOCIAL_AUTH_GOOGLE_OAUTH2_SCOPE = ['email', 'profile', 'https://www.googleapis.com/auth/dfp', 'https://www.googleapis.com/auth/admanager']"
        
        if old_scope_line in content:
            content = content.replace(old_scope_line, new_scope_line)
            print("✓ Updated SOCIAL_AUTH_GOOGLE_OAUTH2_SCOPE")
        else:
            print("⚠ SOCIAL_AUTH_GOOGLE_OAUTH2_SCOPE not found or already updated")
            
        # Update GOOGLE_SCOPES as well
        old_google_scopes = "GOOGLE_SCOPES = [\n    'https://www.googleapis.com/auth/dfp',\n]"
        new_google_scopes = "GOOGLE_SCOPES = [\n    'https://www.googleapis.com/auth/dfp',\n    'https://www.googleapis.com/auth/admanager',\n]"
        
        if old_google_scopes in content:
            content = content.replace(old_google_scopes, new_google_scopes)
            print("✓ Updated GOOGLE_SCOPES")
        else:
            print("⚠ GOOGLE_SCOPES not found or already updated")
            
        # Write back to file
        with open(settings_file, 'w') as f:
            f.write(content)
            
        print("✅ Django settings updated successfully!")
        return True
        
    except Exception as e:
        print(f"❌ Error updating settings: {str(e)}")
        return False

def generate_reauth_instructions():
    """
    Generate instructions for users to re-authorize with new scopes
    """
    print("\n=== Re-authorization Instructions ===")
    print("""
    IMPORTANT: After updating OAuth scopes, users need to re-authorize the application.
    
    Steps for users:
    1. Go to: http://127.0.0.1:8000/management/admin/login
    2. Click "Login with Google"
    3. Grant permissions for the new scopes:
       - Email and profile access
       - Google Ad Manager API access (DFP)
       - Google Ad Manager REST API access
    4. Complete the authorization flow
    
    The new scopes include:
    - https://www.googleapis.com/auth/dfp (Ad Manager SOAP API)
    - https://www.googleapis.com/auth/admanager (Ad Manager REST API - Beta)
    
    This ensures compatibility with both current and future Ad Manager API versions.
    """)

def main():
    print("OAuth Scopes Update Tool for Google Ad Manager API")
    print("=" * 50)
    
    # Test current scopes
    current_scopes_valid = test_current_scopes()
    
    if not current_scopes_valid:
        print("\nCurrent scopes are insufficient. Updating configuration...")
        
        # Update Django settings
        if update_django_settings():
            print("\n✅ Configuration updated successfully!")
            generate_reauth_instructions()
        else:
            print("\n❌ Failed to update configuration")
            return False
    else:
        print("\n✅ Current scopes are valid!")
        print("No configuration changes needed.")
    
    return True

if __name__ == "__main__":
    success = main()
    print("\n" + "=" * 50)
    if success:
        print("✅ OAuth scopes update completed successfully!")
    else:
        print("❌ OAuth scopes update failed!")
    print("=" * 50)
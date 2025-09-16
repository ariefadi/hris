#!/usr/bin/env python
"""
Script to generate new refresh token with correct OAuth scopes for Google Ad Manager API
"""

import os
import sys
import django
from pathlib import Path

# Setup Django
BASE_DIR = Path(__file__).resolve().parent
sys.path.append(str(BASE_DIR))
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'hris.settings')
django.setup()

from django.conf import settings
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
import json

def generate_refresh_token():
    """Generate new refresh token with correct OAuth scopes"""
    print("=== Generating New Refresh Token with DFP Scope ===")
    
    # OAuth2 credentials from settings
    client_id = settings.GOOGLE_ADS_CLIENT_ID
    client_secret = settings.GOOGLE_ADS_CLIENT_SECRET
    
    if not client_id or not client_secret:
        print("[ERROR] Missing OAuth2 credentials in settings")
        return
    
    print(f"[INFO] Using Client ID: {client_id[:20]}...")
    print(f"[INFO] Using Client Secret: {client_secret[:10]}...")
    
    # Required scopes for Google Ad Manager API
    SCOPES = [
        'https://www.googleapis.com/auth/dfp',  # Google Ad Manager API
        'email',  # For user identification
        'profile'  # For user profile
    ]
    
    print(f"[INFO] Required scopes: {SCOPES}")
    
    # Create OAuth2 flow
    try:
        flow = InstalledAppFlow.from_client_config(
            {
                "installed": {
                    "client_id": client_id,
                    "client_secret": client_secret,
                    "auth_uri": "https://accounts.google.com/o/oauth2/auth",
                    "token_uri": "https://oauth2.googleapis.com/token",
                    "redirect_uris": ["urn:ietf:wg:oauth:2.0:oob", "http://localhost"]
                }
            },
            SCOPES
        )
        
        print("\n[INFO] Starting OAuth2 flow...")
        print("[INFO] This will open a browser window for authentication")
        print("[INFO] Please log in with the Google account that has Ad Manager access")
        
        # Run the OAuth flow
        credentials = flow.run_local_server(port=0)
        
        if credentials and credentials.refresh_token:
            print("\n✅ Successfully generated new refresh token!")
            print(f"\n=== New Credentials ===")
            print(f"Refresh Token: {credentials.refresh_token}")
            print(f"Access Token: {credentials.token[:20]}...")
            
            # Test the credentials
            print("\n[INFO] Testing new credentials...")
            try:
                # Refresh to ensure they work
                credentials.refresh(Request())
                print("✅ Credentials are valid and can be refreshed")
                
                # Update .env file
                update_env_file(credentials.refresh_token)
                
            except Exception as e:
                print(f"❌ Credential test failed: {e}")
                
        else:
            print("❌ Failed to get refresh token")
            
    except Exception as e:
        print(f"[ERROR] OAuth flow failed: {e}")
        import traceback
        traceback.print_exc()

def update_env_file(new_refresh_token):
    """Update .env file with new refresh token"""
    try:
        env_file = Path(BASE_DIR) / '.env'
        
        if not env_file.exists():
            print(f"[WARNING] .env file not found: {env_file}")
            return
        
        # Read current .env content
        with open(env_file, 'r') as f:
            lines = f.readlines()
        
        # Update refresh token line
        updated = False
        for i, line in enumerate(lines):
            if line.startswith('GOOGLE_ADS_REFRESH_TOKEN='):
                lines[i] = f'GOOGLE_ADS_REFRESH_TOKEN={new_refresh_token}\n'
                updated = True
                break
        
        if not updated:
            # Add new line if not found
            lines.append(f'GOOGLE_ADS_REFRESH_TOKEN={new_refresh_token}\n')
        
        # Write back to file
        with open(env_file, 'w') as f:
            f.writelines(lines)
        
        print(f"✅ Updated .env file with new refresh token")
        print(f"[INFO] Please restart the Django server to load new credentials")
        
    except Exception as e:
        print(f"[ERROR] Failed to update .env file: {e}")

def test_current_token():
    """Test current refresh token"""
    print("\n=== Testing Current Refresh Token ===")
    
    current_token = settings.GOOGLE_ADS_REFRESH_TOKEN
    client_id = settings.GOOGLE_ADS_CLIENT_ID
    client_secret = settings.GOOGLE_ADS_CLIENT_SECRET
    
    if not all([current_token, client_id, client_secret]):
        print("[ERROR] Missing required credentials")
        return False
    
    try:
        # Create credentials object
        credentials = Credentials(
            token=None,
            refresh_token=current_token,
            token_uri='https://oauth2.googleapis.com/token',
            client_id=client_id,
            client_secret=client_secret
        )
        
        # Try to refresh
        print("[INFO] Attempting to refresh current token...")
        credentials.refresh(Request())
        
        print("✅ Current refresh token is valid")
        print(f"[INFO] Access token: {credentials.token[:20]}...")
        
        # Check scopes (this requires additional API call)
        print("[INFO] Current token appears to be working")
        print("[WARNING] However, it may not have the required DFP scope")
        print("[INFO] Consider generating a new token with correct scopes")
        
        return True
        
    except Exception as e:
        print(f"❌ Current refresh token is invalid: {e}")
        return False

if __name__ == '__main__':
    print("Google Ad Manager OAuth Token Generator")
    print("=======================================")
    
    # Test current token first
    if test_current_token():
        print("\n[INFO] Current token works, but may lack DFP scope")
        response = input("\nDo you want to generate a new token with correct scopes? (y/n): ")
        if response.lower() != 'y':
            print("[INFO] Keeping current token. Note: Ad Manager API may still fail due to missing scopes.")
            sys.exit(0)
    
    # Generate new token
    generate_refresh_token()
    
    print("\n=== Next Steps ===")
    print("1. Restart your Django development server")
    print("2. Test the AdX Traffic Account functionality")
    print("3. Users may need to log out and log back in to the application")
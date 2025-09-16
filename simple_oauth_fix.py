#!/usr/bin/env python
"""
Simple OAuth Re-authorization Script
This script provides step-by-step instructions to fix Google Ad Manager API authentication.
"""

import os
import sys
import django
import webbrowser
from urllib.parse import urlencode

# Setup Django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'hris.settings')
django.setup()

from django.conf import settings
from management.database import data_mysql

def main():
    print("🔧 Google Ad Manager API - OAuth Fix")
    print("=" * 50)
    
    # Step 1: Show current configuration
    print("\n📋 Current OAuth Configuration:")
    print(f"Client ID: {getattr(settings, 'SOCIAL_AUTH_GOOGLE_OAUTH2_KEY', 'Not configured')}")
    print(f"Scopes: {getattr(settings, 'SOCIAL_AUTH_GOOGLE_OAUTH2_SCOPE', [])}")
    
    # Step 2: Check users
    print("\n👥 Checking registered users:")
    db = data_mysql()
    try:
        result = db.data_user_by_params()
        if result['status'] and result['data']:
            users = result['data']
            for i, user in enumerate(users, 1):
                email = user.get('user_mail', 'Unknown')
                name = user.get('user_alias', 'Unknown')
                print(f"  {i}. {name} ({email})")
        else:
            print("  ❌ No users found")
            return
    except Exception as e:
        print(f"  ❌ Error: {e}")
        return
    
    # Step 3: Generate authorization URL
    print("\n🔗 OAuth Authorization Required:")
    
    client_id = getattr(settings, 'SOCIAL_AUTH_GOOGLE_OAUTH2_KEY', '')
    if not client_id:
        print("❌ OAuth client ID not configured")
        return
    
    # Build authorization URL
    auth_params = {
        'client_id': client_id,
        'redirect_uri': 'http://127.0.0.1:8000/accounts/complete/google-oauth2/',
        'scope': 'email profile https://www.googleapis.com/auth/dfp https://www.googleapis.com/auth/admanager',
        'response_type': 'code',
        'access_type': 'offline',
        'approval_prompt': 'force',
        'include_granted_scopes': 'true'
    }
    
    auth_url = 'https://accounts.google.com/o/oauth2/auth?' + urlencode(auth_params)
    
    print("\n📝 INSTRUCTIONS:")
    print("1. Start Django server: python manage.py runserver 127.0.0.1:8000")
    print("2. Open the authorization URL below in your browser")
    print("3. Sign in and authorize all requested permissions")
    print("4. Complete the OAuth flow")
    print("5. Test AdX Traffic Account access")
    
    print("\n🌐 Authorization URL:")
    print(auth_url)
    
    # Ask if user wants to open URL
    try:
        choice = input("\nOpen URL in browser? (y/n): ").lower().strip()
        if choice == 'y':
            webbrowser.open(auth_url)
            print("✅ URL opened in browser")
    except KeyboardInterrupt:
        print("\n👋 Exiting...")
        return
    
    print("\n✨ Next Steps:")
    print("1. Complete OAuth authorization in browser")
    print("2. Test access to: http://127.0.0.1:8000/management/admin/adx_traffic_account")
    print("3. Verify that the authentication error is resolved")
    
    print("\n📚 For detailed troubleshooting, see: OAUTH_FIX_GUIDE.md")

if __name__ == '__main__':
    main()
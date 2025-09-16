#!/usr/bin/env python3
"""
Script to help users re-authorize OAuth with updated scopes for Google Ad Manager API
This script provides a guided process for re-authorization
"""

import os
import sys
import django
import webbrowser
from urllib.parse import urlencode

# Add project root to Python path
sys.path.append('/Users/ariefdwicahyoadi/hris')

# Setup Django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'hris.settings')
django.setup()

from django.conf import settings
from management.database import data_mysql

def get_oauth_authorization_url():
    """
    Generate OAuth authorization URL with updated scopes
    """
    # OAuth configuration
    client_id = settings.SOCIAL_AUTH_GOOGLE_OAUTH2_KEY
    redirect_uri = 'http://127.0.0.1:8000/accounts/complete/google-oauth2/'
    
    # Updated scopes including both DFP and Ad Manager
    scopes = [
        'email',
        'profile', 
        'https://www.googleapis.com/auth/dfp',
        'https://www.googleapis.com/auth/admanager'
    ]
    
    # OAuth parameters
    params = {
        'client_id': client_id,
        'redirect_uri': redirect_uri,
        'scope': ' '.join(scopes),
        'response_type': 'code',
        'access_type': 'offline',
        'approval_prompt': 'force',  # Force re-authorization
        'include_granted_scopes': 'true'
    }
    
    # Generate authorization URL
    auth_url = 'https://accounts.google.com/o/oauth2/auth?' + urlencode(params)
    return auth_url

def check_current_users():
    """
    Check current users in the system
    """
    print("=== Checking Current Users ===")
    
    db = data_mysql()
    try:
        result = db.data_user_by_params()
        if not result['status'] or not result['data']:
            print("No users found in database")
            return []
            
        users = result['data']
        print(f"Found {len(users)} users in the system:")
        for i, user in enumerate(users, 1):
            # Database columns: user_mail, user_alias
            email = user.get('user_mail', 'Unknown')
            name = user.get('user_alias', 'Unknown')
            print(f"  {i}. {name} ({email})")
            
        return users
        
    except Exception as e:
        print(f"Error checking users: {str(e)}")
        return []

def clear_existing_tokens():
    """
    Clear existing OAuth tokens to force re-authorization
    """
    print("\n=== Clearing Existing Tokens ===")
    
    db = data_mysql()
    try:
        # Get all users
        result = db.data_user_by_params()
        if not result['status'] or not result['data']:
            print("No users to clear tokens for")
            return
            
        users = result['data']
        for user in users:
            user_id = user.get('user_id')
            email = user.get('user_mail')
            
            if user_id:
                # Clear refresh token by updating user record
                # Note: We'll clear OAuth-related fields if they exist
                try:
                    # Use SQL to clear OAuth tokens
                    sql = """
                        UPDATE app_users 
                        SET refresh_token = NULL, 
                            access_token = NULL, 
                            token_expires_at = NULL
                        WHERE user_id = %s
                    """
                    db.cur_hris.execute(sql, (user_id,))
                    db.comit_hris.commit()
                    print(f"✓ Cleared tokens for {email}")
                except Exception as e:
                    print(f"✗ Failed to clear tokens for {email}: {str(e)}")
                    
        print("\n✅ Token clearing completed!")
        
    except Exception as e:
        print(f"❌ Error clearing tokens: {str(e)}")

def start_django_server():
    """
    Instructions to start Django development server
    """
    print("\n=== Starting Django Server ===")
    print("To complete re-authorization, you need to start the Django server.")
    print("\nRun this command in a separate terminal:")
    print("  cd /Users/ariefdwicahyoadi/hris")
    print("  python manage.py runserver 127.0.0.1:8000")
    print("\nPress Enter when the server is running...")
    input()

def guide_reauthorization():
    """
    Guide user through the re-authorization process
    """
    print("\n=== Re-authorization Guide ===")
    
    # Generate authorization URL
    auth_url = get_oauth_authorization_url()
    
    print("\nStep 1: Open the authorization URL")
    print(f"URL: {auth_url}")
    print("\nThis URL will:")
    print("- Request email and profile access")
    print("- Request Google Ad Manager API access (DFP scope)")
    print("- Request Google Ad Manager REST API access (new scope)")
    print("- Force re-authorization to get new scopes")
    
    # Ask if user wants to open automatically
    response = input("\nWould you like to open this URL automatically? (y/n): ").lower().strip()
    if response in ['y', 'yes']:
        try:
            webbrowser.open(auth_url)
            print("✓ Authorization URL opened in browser")
        except Exception as e:
            print(f"Could not open browser automatically: {e}")
            print("Please copy and paste the URL manually.")
    else:
        print("Please copy and paste the URL into your browser.")
    
    print("\nStep 2: Complete authorization in browser")
    print("- Sign in with your Google account")
    print("- Review and accept the requested permissions")
    print("- You should see all the new scopes listed")
    print("- Complete the authorization flow")
    
    print("\nStep 3: Verify successful authorization")
    print("- You should be redirected to the HRIS application")
    print("- Check that you can access AdX Traffic Account without errors")
    
    input("\nPress Enter when authorization is complete...")

def verify_new_authorization():
    """
    Verify that new authorization worked
    """
    print("\n=== Verifying New Authorization ===")
    
    db = data_mysql()
    try:
        result = db.data_user_by_params()
        if not result['status'] or not result['data']:
            print("No users found")
            return False
            
        users = result['data']
        # Check if users have new refresh tokens
        authorized_users = 0
        for user in users:
            email = user.get('user_mail')
            # Check for refresh token in database
            try:
                sql = "SELECT refresh_token FROM app_users WHERE user_id = %s"
                db.cur_hris.execute(sql, (user.get('user_id'),))
                token_result = db.cur_hris.fetchone()
                refresh_token = token_result.get('refresh_token') if token_result else None
                
                if refresh_token:
                    print(f"✓ {email} - Has refresh token")
                    authorized_users += 1
                else:
                    print(f"✗ {email} - No refresh token")
            except Exception as e:
                print(f"✗ {email} - Error checking token: {str(e)}")
                
        if authorized_users > 0:
            print(f"\n✅ {authorized_users} user(s) successfully re-authorized!")
            return True
        else:
            print("\n❌ No users have been re-authorized yet.")
            return False
            
    except Exception as e:
        print(f"Error verifying authorization: {str(e)}")
        return False

def main():
    print("Google Ad Manager OAuth Re-authorization Tool")
    print("=" * 50)
    
    # Check current users
    users = check_current_users()
    if not users:
        print("\n❌ No users found. Please ensure users are registered first.")
        return False
    
    print("\n" + "=" * 50)
    print("IMPORTANT: OAuth Scope Update")
    print("=" * 50)
    print("The OAuth scopes have been updated to include:")
    print("- https://www.googleapis.com/auth/dfp (Ad Manager SOAP API)")
    print("- https://www.googleapis.com/auth/admanager (Ad Manager REST API)")
    print("\nThis should resolve authentication issues with Ad Manager API.")
    print("\nAll users need to re-authorize to get the new scopes.")
    
    # Ask if user wants to proceed
    response = input("\nDo you want to proceed with re-authorization? (y/n): ").lower().strip()
    if response not in ['y', 'yes']:
        print("Re-authorization cancelled.")
        return False
    
    # Clear existing tokens
    clear_existing_tokens()
    
    # Start server instructions
    start_django_server()
    
    # Guide through re-authorization
    guide_reauthorization()
    
    # Verify new authorization
    success = verify_new_authorization()
    
    if success:
        print("\n" + "=" * 50)
        print("✅ Re-authorization completed successfully!")
        print("\nNext steps:")
        print("1. Test AdX Traffic Account functionality")
        print("2. Verify that authentication errors are resolved")
        print("3. Check that data loads properly")
        print("=" * 50)
    else:
        print("\n" + "=" * 50)
        print("❌ Re-authorization may not be complete.")
        print("\nTroubleshooting:")
        print("1. Ensure Django server is running")
        print("2. Complete the OAuth flow in browser")
        print("3. Check for any error messages")
        print("4. Try running this script again")
        print("=" * 50)
    
    return success

if __name__ == "__main__":
    try:
        success = main()
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        print("\n\nRe-authorization cancelled by user.")
        sys.exit(1)
    except Exception as e:
        print(f"\n\nUnexpected error: {str(e)}")
        sys.exit(1)
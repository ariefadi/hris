#!/usr/bin/env python3
"""
Test script to verify user credentials functionality for AdX traffic data
"""

import os
import sys
import django
from datetime import datetime, timedelta

# Add the project directory to Python path
sys.path.append('/Users/ariefdwicahyoadi/hris')

# Setup Django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'hris.settings')
django.setup()

from management.utils import (
    get_user_adx_credentials,
    get_user_ad_manager_client,
    fetch_adx_traffic_account_by_user
)

def test_user_credentials():
    """Test user credentials retrieval and Ad Manager client creation"""
    print("=== Testing User Credentials Functionality ===")
    
    # Test email - using actual user email from database with complete credentials
    test_email = "adiarief463@gmail.com"  # User with complete AdX credentials
    
    print(f"\n1. Testing credential retrieval for: {test_email}")
    
    # Test credential retrieval
    creds_result = get_user_adx_credentials(test_email)
    print(f"Credentials status: {creds_result['status']}")
    
    if creds_result['status']:
        print("✓ Successfully retrieved user credentials")
        credentials = creds_result['credentials']
        print(f"  - Network Code: {credentials.get('network_code')}")
        print(f"  - Email: {credentials.get('email')}")
        print(f"  - Has Client ID: {'Yes' if credentials.get('client_id') else 'No'}")
        print(f"  - Has Client Secret: {'Yes' if credentials.get('client_secret') else 'No'}")
        print(f"  - Has Refresh Token: {'Yes' if credentials.get('refresh_token') else 'No'}")
        print(f"  - Has Developer Token: {'Yes' if credentials.get('developer_token') else 'No'}")
        
        print(f"\n2. Testing Ad Manager client creation for: {test_email}")
        
        # Test Ad Manager client creation
        client_result = get_user_ad_manager_client(test_email)
        print(f"Client creation status: {client_result['status']}")
        
        if client_result['status']:
            print("✓ Successfully created Ad Manager client")
            client = client_result['client']
            
            try:
                # Test network connection
                network_service = client.GetService('NetworkService', version='v202408')
                current_network = network_service.getCurrentNetwork()
                print(f"  - Connected to network: {current_network['displayName']}")
                print(f"  - Network Code: {current_network['networkCode']}")
                
                print(f"\n3. Testing AdX traffic data fetch for: {test_email}")
                
                # Test data fetching
                end_date = datetime.now().date()
                start_date = end_date - timedelta(days=7)
                
                result = fetch_adx_traffic_account_by_user(
                    test_email, 
                    start_date.strftime('%Y-%m-%d'), 
                    end_date.strftime('%Y-%m-%d')
                )
                
                print(f"Data fetch status: {result.get('status', False)}")
                
                if result.get('status'):
                    print("✓ Successfully fetched AdX traffic data")
                    print(f"  - Records retrieved: {len(result.get('data', []))}")
                    print(f"  - API Method: {result.get('api_method')}")
                    print(f"  - Note: {result.get('note')}")
                else:
                    print("✗ Failed to fetch AdX traffic data")
                    print(f"  - Error: {result.get('error', 'Unknown error')}")
                    
            except Exception as e:
                print(f"✗ Error testing network connection: {str(e)}")
                
        else:
            print("✗ Failed to create Ad Manager client")
            print(f"  - Error: {client_result['error']}")
            
    else:
        print("✗ Failed to retrieve user credentials")
        print(f"  - Error: {creds_result['error']}")
        print("\nNote: Make sure the test email exists in the app_users table with valid AdX credentials")

def list_available_users():
    """List available users in the database for testing"""
    print("\n=== Available Users in Database ===")
    
    try:
        from management.database import data_mysql
        
        db = data_mysql()
        sql = """
            SELECT user_mail, network_code, 
                   CASE WHEN client_id IS NOT NULL AND client_id != '' THEN 'Yes' ELSE 'No' END as has_client_id,
                   CASE WHEN client_secret IS NOT NULL AND client_secret != '' THEN 'Yes' ELSE 'No' END as has_client_secret,
                   CASE WHEN refresh_token IS NOT NULL AND refresh_token != '' THEN 'Yes' ELSE 'No' END as has_refresh_token,
                   CASE WHEN developer_token IS NOT NULL AND developer_token != '' THEN 'Yes' ELSE 'No' END as has_developer_token
            FROM app_users 
            WHERE user_mail IS NOT NULL AND user_mail != ''
            ORDER BY user_mail
            LIMIT 10
        """
        
        db.cur_hris.execute(sql)
        users = db.cur_hris.fetchall()
        
        if users:
            print(f"Found {len(users)} users:")
            print("\nEmail\t\t\t\tNetwork Code\tCredentials (ID/Secret/Token/Dev)")
            print("-" * 80)
            
            for user in users:
                email = user['user_mail'][:30] + '...' if len(user['user_mail']) > 30 else user['user_mail']
                network = user['network_code'] or 'None'
                creds = f"{user['has_client_id']}/{user['has_client_secret']}/{user['has_refresh_token']}/{user['has_developer_token']}"
                print(f"{email:<35} {network:<15} {creds}")
                
            print("\nTo test with a specific user, update the test_email variable in the script.")
        else:
            print("No users found in the app_users table.")
            
    except Exception as e:
        print(f"Error retrieving users: {str(e)}")

if __name__ == "__main__":
    print("AdX User Credentials Test Script")
    print("=" * 50)
    
    # List available users first
    list_available_users()
    
    # Test credentials functionality
    test_user_credentials()
    
    print("\n=== Test Complete ===")
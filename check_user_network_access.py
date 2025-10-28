#!/usr/bin/env python3
import os
import sys
import django
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
sys.path.append(str(BASE_DIR))
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'hris.settings')
django.setup()

from management.database import data_mysql
from management.credential_loader import get_credentials_from_db
from googleads import ad_manager
from google.oauth2.credentials import Credentials
import requests

def check_user_network_access(user_mail):
    """Check if user has access to Google Ad Manager network"""
    print(f"🔍 Mengecek akses network untuk user: {user_mail}")
    
    # Get credentials from database
    db = data_mysql()
    sql = "SELECT google_oauth2_client_id, google_oauth2_client_secret, google_ads_refresh_token, google_ad_manager_network_code FROM app_oauth_credentials WHERE user_mail = %s"
    success = db.execute_query(sql, (user_mail,))
    
    if not success:
        print(f"❌ Error executing query")
        return False
        
    result = db.cur_hris.fetchone()
    
    if not result:
        print(f"❌ User {user_mail} tidak ditemukan di database")
        return False
    
    client_id, client_secret, refresh_token, network_code = result
    
    if not refresh_token:
        print(f"❌ User {user_mail} tidak memiliki refresh token")
        return False
    
    if not network_code:
        print(f"❌ Network code tidak ditemukan untuk user {user_mail}")
        return False
    
    print(f"✅ Credentials ditemukan:")
    print(f"   - Client ID: {client_id[:20]}...")
    print(f"   - Network Code: {network_code}")
    print(f"   - Refresh Token: {refresh_token[:20]}...")
    
    try:
        # Create OAuth2 credentials
        credentials = Credentials(
            token=None,
            refresh_token=refresh_token,
            token_uri='https://oauth2.googleapis.com/token',
            client_id=client_id,
            client_secret=client_secret,
            scopes=[
                'https://www.googleapis.com/auth/dfp',
                'https://www.googleapis.com/auth/admanager'
            ]
        )
        
        # Test token refresh
        print(f"🔄 Testing token refresh...")
        from google.auth.transport.requests import Request
        credentials.refresh(Request())
        print(f"✅ Token refresh berhasil")
        
        # Create Ad Manager client
        print(f"🔄 Testing Ad Manager client connection...")
        ad_manager_client = ad_manager.AdManagerClient(
            credentials,
            'HRIS Dashboard',
            network_code=network_code
        )
        
        # Test network access by getting current network
        network_service = ad_manager_client.GetService('NetworkService', version='v202408')
        current_network = network_service.getCurrentNetwork()
        
        print(f"✅ Berhasil terhubung ke Ad Manager network:")
        print(f"   - Network Code: {current_network.networkCode}")
        print(f"   - Display Name: {current_network.displayName}")
        print(f"   - Currency: {current_network.currencyCode}")
        print(f"   - Time Zone: {current_network.timeZone}")
        
        # Test user access by getting users
        print(f"🔄 Testing user access permissions...")
        user_service = ad_manager_client.GetService('UserService', version='v202408')
        
        # Create statement to get current user
        from googleads.ad_manager import FilterStatement
        statement = FilterStatement()
        statement.limit = 1
        
        users_page = user_service.getUsersByStatement(statement.ToStatement())
        
        if users_page and users_page.results:
            user = users_page.results[0]
            print(f"✅ User access verified:")
            print(f"   - User ID: {user.id}")
            print(f"   - Name: {user.name}")
            print(f"   - Email: {user.email}")
            print(f"   - Role: {user.roleName}")
        else:
            print(f"⚠️  Tidak dapat mengambil data user, mungkin permission terbatas")
        
        return True
        
    except Exception as e:
        print(f"❌ Error mengakses Ad Manager network:")
        print(f"   Error: {str(e)}")
        
        # Check specific error types
        error_str = str(e).lower()
        if 'unauthorized' in error_str:
            print(f"💡 Solusi: User perlu melakukan re-authorization OAuth")
        elif 'invalid_client' in error_str:
            print(f"💡 Solusi: Periksa Client ID dan Client Secret")
        elif 'access_denied' in error_str:
            print(f"💡 Solusi: User tidak memiliki akses ke network {network_code}")
        elif 'network' in error_str:
            print(f"💡 Solusi: Periksa network_code {network_code} apakah benar")
        
        return False

def main():
    print("🔍 Google Ad Manager Network Access Checker")
    print("=" * 50)
    
    # Check specific user
    user_mail = "adiarief463@gmail.com"
    success = check_user_network_access(user_mail)
    
    if success:
        print(f"\n🎉 User {user_mail} memiliki akses penuh ke Ad Manager network")
    else:
        print(f"\n❌ User {user_mail} tidak memiliki akses yang proper ke Ad Manager network")
        print(f"\n📋 Langkah perbaikan:")
        print(f"1. Pastikan user sudah melakukan OAuth re-authorization")
        print(f"2. Periksa network_code sudah benar")
        print(f"3. Pastikan user ditambahkan ke Ad Manager account")
        print(f"4. Lihat ADMANAGER_SETUP_GUIDE.md untuk detail")

if __name__ == "__main__":
    main()
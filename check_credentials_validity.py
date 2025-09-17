#!/usr/bin/env python
import os
import sys
import django
from datetime import datetime, timedelta

# Setup Django environment
sys.path.append('/Users/ariefdwicahyoadi/hris')
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'hris.settings')
django.setup()

# Import after Django setup
from management.database import data_mysql
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
import requests
import json

def check_credentials_validity():
    print("=== Memeriksa Validitas Kredensial Google Ad Manager ===")
    
    # Get user credentials from database
    print("\n1. Mengambil kredensial dari database...")
    try:
        users_result = data_mysql().data_user_by_params()
        if not users_result['status'] or not users_result['data']:
            print("✗ Tidak ada user ditemukan")
            return
        
        test_user = None
        for user in users_result['data']:
            if user.get('user_mail'):
                test_user = user
                break
        
        if not test_user:
            print("✗ Tidak ada user dengan email")
            return
        
        user_email = test_user['user_mail']
        print(f"✓ User email: {user_email}")
        
        # Get detailed credentials
        db = data_mysql()
        sql = """
            SELECT client_id, client_secret, refresh_token, network_code, developer_token, user_mail
            FROM app_users 
            WHERE user_mail = %s
        """
        
        db.cur_hris.execute(sql, (user_email,))
        user_data = db.cur_hris.fetchone()
        
        if not user_data:
            print(f"✗ Tidak ada data kredensial untuk {user_email}")
            return
        
        print(f"✓ Kredensial ditemukan untuk {user_email}")
        
    except Exception as e:
        print(f"✗ Error mengambil kredensial: {e}")
        return
    
    # Check each credential component
    print("\n2. Memeriksa komponen kredensial...")
    
    client_id = user_data.get('client_id')
    client_secret = user_data.get('client_secret')
    refresh_token = user_data.get('refresh_token')
    network_code = user_data.get('network_code')
    developer_token = user_data.get('developer_token')
    
    print(f"   Client ID: {'✓' if client_id else '✗'} {client_id[:20] + '...' if client_id else 'MISSING'}")
    print(f"   Client Secret: {'✓' if client_secret else '✗'} {client_secret[:20] + '...' if client_secret else 'MISSING'}")
    print(f"   Refresh Token: {'✓' if refresh_token else '✗'} {refresh_token[:20] + '...' if refresh_token else 'MISSING'}")
    print(f"   Network Code: {'✓' if network_code else '✗'} {network_code if network_code else 'MISSING'}")
    print(f"   Developer Token: {'✓' if developer_token else '✗'} {developer_token[:20] + '...' if developer_token else 'MISSING'}")
    
    if not all([client_id, client_secret, refresh_token, network_code, developer_token]):
        print("\n✗ Beberapa kredensial hilang. Pastikan semua field terisi di database.")
        return
    
    # Test OAuth2 token refresh
    print("\n3. Menguji refresh OAuth2 token...")
    try:
        # Create credentials object
        creds = Credentials(
            token=None,
            refresh_token=refresh_token,
            token_uri='https://oauth2.googleapis.com/token',
            client_id=client_id,
            client_secret=client_secret
        )
        
        # Try to refresh the token
        request = Request()
        creds.refresh(request)
        
        if creds.token:
            print("✓ OAuth2 token berhasil di-refresh")
            print(f"   Access Token: {creds.token[:30]}...")
            print(f"   Expires: {creds.expiry}")
        else:
            print("✗ Gagal mendapatkan access token")
            return
            
    except Exception as e:
        print(f"✗ Error refresh token: {e}")
        print("   Kemungkinan penyebab:")
        print("   - Refresh token sudah expired")
        print("   - Client ID/Secret tidak valid")
        print("   - Akun Google sudah revoke akses")
        return
    
    # Test Developer Token validity
    print("\n4. Menguji validitas Developer Token...")
    try:
        # Test with a simple API call using the access token
        headers = {
            'Authorization': f'Bearer {creds.token}',
            'Content-Type': 'application/json'
        }
        
        # Try to access Google Ads API to validate developer token
        test_url = 'https://googleads.googleapis.com/v16/customers:listAccessibleCustomers'
        response = requests.get(test_url, headers=headers)
        
        if response.status_code == 200:
            print("✓ Developer Token dan Access Token valid")
            data = response.json()
            if 'resourceNames' in data:
                print(f"   Accessible customers: {len(data['resourceNames'])}")
        elif response.status_code == 401:
            print("✗ Developer Token atau Access Token tidak valid")
            print(f"   Response: {response.text}")
        elif response.status_code == 403:
            print("⚠ Developer Token valid tapi tidak memiliki akses")
            print(f"   Response: {response.text}")
        else:
            print(f"⚠ Response tidak terduga: {response.status_code}")
            print(f"   Response: {response.text}")
            
    except Exception as e:
        print(f"✗ Error testing developer token: {e}")
    
    # Check network code validity
    print("\n5. Memeriksa Network Code...")
    try:
        network_code_int = int(network_code)
        if network_code_int > 0:
            print(f"✓ Network Code format valid: {network_code_int}")
        else:
            print(f"✗ Network Code tidak valid: {network_code}")
    except ValueError:
        print(f"✗ Network Code bukan angka: {network_code}")
    
    # Final recommendations
    print("\n=== Rekomendasi Berdasarkan Hasil Pemeriksaan ===")
    print("\nJika OAuth2 token gagal di-refresh:")
    print("1. Generate ulang refresh token melalui OAuth2 flow")
    print("2. Pastikan Client ID/Secret masih aktif di Google Cloud Console")
    print("3. Periksa apakah user sudah revoke akses aplikasi")
    
    print("\nJika Developer Token tidak valid:")
    print("1. Periksa status Developer Token di Google Ads Manager")
    print("2. Pastikan akun Google Ads sudah approved untuk API access")
    print("3. Verifikasi Developer Token belum expired")
    
    print("\nJika Network Code salah:")
    print("1. Login ke Google Ad Manager Console")
    print("2. Periksa Network Code di Settings > Global Settings")
    print("3. Pastikan menggunakan Network Code yang benar")
    
    print("\nJika semua kredensial valid tapi masih error:")
    print("1. Periksa apakah akun memiliki akses ke AdX")
    print("2. Verifikasi permissions di Google Ad Manager")
    print("3. Coba akses manual melalui Google Ad Manager Console")

if __name__ == "__main__":
    check_credentials_validity()
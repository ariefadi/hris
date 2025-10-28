#!/usr/bin/env python3
"""
Script Universal untuk OAuth Re-Authorization
Bisa digunakan untuk semua user email yang mengalami error "unauthorized_client"
"""

import os
import sys
import django
from pathlib import Path

# Setup Django environment
BASE_DIR = Path(__file__).resolve().parent
sys.path.append(str(BASE_DIR))
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'hris.settings')
django.setup()

from management.database import data_mysql
from management.oauth_utils import generate_oauth_url_for_user
from django.conf import settings
import urllib.parse

def main():
    print("🔧 OAuth Re-Authorization Universal")
    print("=" * 60)
    
    # Input user email
    user_mail = input("📧 Masukkan email user yang mengalami error: ").strip()
    
    if not user_mail:
        print("❌ Email user diperlukan")
        return
    
    # Validasi format email
    if '@' not in user_mail or '.' not in user_mail:
        print("❌ Format email tidak valid")
        return
    
    print(f"👤 User: {user_mail}")
    
    # Periksa apakah user ada di database
    db = data_mysql()
    user_data = db.get_user_by_email(user_mail)
    
    if not user_data['status']:
        print(f"❌ User {user_mail} tidak ditemukan di database")
        print("💡 Pastikan user sudah terdaftar di sistem")
        return
    
    print("✅ User ditemukan di database")
    
    # Periksa OAuth credentials
    oauth_creds = db.get_user_oauth_credentials(user_mail=user_mail)
    
    if not oauth_creds['status']:
        print("❌ OAuth credentials tidak ditemukan untuk user ini")
        print("💡 User perlu ditambahkan ke tabel app_oauth_credentials terlebih dahulu")
        return
    
    credentials = oauth_creds['data']
    print("✅ OAuth credentials ditemukan")
    print(f"   Client ID: {credentials.get('google_oauth2_client_id', 'N/A')}")
    print(f"   Network Code: {credentials.get('google_ad_manager_network_code', 'N/A')}")
    
    # Generate OAuth URL dengan scope yang benar
    scopes = [
        'openid',
        'email', 
        'profile',
        'https://www.googleapis.com/auth/dfp',        # Ad Manager SOAP API
        'https://www.googleapis.com/auth/admanager'   # Ad Manager REST API (Beta)
    ]
    
    print(f"\n📋 Scope yang akan diminta:")
    for scope in scopes:
        print(f"   - {scope}")
    
    # Generate OAuth URL
    oauth_url, error = generate_oauth_url_for_user(
        user_mail=user_mail,
        scopes=scopes,
        redirect_uri='urn:ietf:wg:oauth:2.0:oob'  # Manual copy-paste flow
    )
    
    if error:
        print(f"❌ Error generating OAuth URL: {error}")
        return
    
    print(f"\n🔗 OAuth Authorization URL:")
    print("=" * 80)
    print(oauth_url)
    print("=" * 80)
    
    print(f"\n📝 LANGKAH-LANGKAH PERBAIKAN:")
    print("1. 📋 Copy URL di atas dan buka di browser")
    print(f"2. 🔐 Login dengan akun: {user_mail}")
    print("3. ✅ Berikan izin untuk SEMUA scope yang diminta:")
    for scope in scopes:
        print(f"   - {scope}")
    print("4. 📄 Copy authorization code yang diberikan Google")
    print("5. 🔄 Jalankan script berikut untuk menukar code dengan refresh token:")
    print(f"   python exchange_oauth_code_universal.py")
    
    print(f"\n⚠️  PENTING:")
    print("- Pastikan Anda memberikan izin untuk SEMUA scope yang diminta")
    print("- Jika sebelumnya sudah pernah authorize, Google mungkin tidak menampilkan")
    print("  semua permission lagi. Dalam hal ini:")
    print("  1. Buka https://myaccount.google.com/permissions")
    print("  2. Cari aplikasi HRIS/OAuth app")
    print("  3. Klik 'Remove access'")
    print("  4. Ulangi proses authorization")
    
    print(f"\n🔍 TROUBLESHOOTING:")
    print("- Jika masih error 'unauthorized_client' setelah re-auth:")
    print("  1. Periksa apakah refresh token sudah terupdate di database")
    print(f"  2. Pastikan user memiliki akses ke Ad Manager network {credentials.get('google_ad_manager_network_code')}")
    print("  3. Verifikasi Client ID dan Client Secret masih valid")
    
    # Tampilkan informasi tambahan untuk debugging
    print(f"\n📊 INFORMASI DEBUG:")
    print(f"   Client ID: {credentials.get('google_oauth2_client_id')}")
    print(f"   Network Code: {credentials.get('google_ad_manager_network_code')}")
    print(f"   Refresh Token (10 char): {str(credentials.get('google_ads_refresh_token', ''))[:10]}...")
    
    # Simpan informasi user untuk script exchange
    with open('.oauth_temp_user', 'w') as f:
        f.write(user_mail)
    
    print(f"\n💾 User email disimpan untuk script exchange")

if __name__ == "__main__":
    main()
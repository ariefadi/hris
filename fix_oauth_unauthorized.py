#!/usr/bin/env python3
"""
Script untuk memperbaiki masalah "unauthorized_client: Unauthorized"
Melakukan re-authorization OAuth dengan scope yang benar untuk Google Ad Manager API
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

def main():
    print("🔧 Memperbaiki masalah 'unauthorized_client: Unauthorized'")
    print("=" * 60)
    
    # Ambil user dari session atau input manual
    user_mail = input("Masukkan email user yang mengalami error: ").strip()
    
    if not user_mail:
        print("❌ Email user diperlukan")
        return
    
    print(f"\n👤 Memperbaiki OAuth untuk user: {user_mail}")
    
    # Periksa apakah user ada di database
    db = data_mysql()
    user_data = db.get_user_by_email(user_mail)
    
    if not user_data['status']:
        print(f"❌ User {user_mail} tidak ditemukan di database")
        return
    
    print("✅ User ditemukan di database")
    
    # Periksa OAuth credentials
    oauth_creds = db.get_user_credentials(user_mail=user_mail)
    
    if not oauth_creds['status']:
        print("❌ OAuth credentials tidak ditemukan untuk user ini")
        print("💡 User perlu ditambahkan ke tabel app_credentials terlebih dahulu")
        return
    
    creds = oauth_creds['data']
    client_id = creds.get('client_id')
    client_secret = creds.get('client_secret')
    
    if not client_id or not client_secret:
        print("❌ Client ID atau Client Secret tidak lengkap")
        print("💡 Pastikan OAuth credentials sudah dikonfigurasi dengan benar")
        return
    
    print(f"✅ OAuth credentials ditemukan")
    print(f"🔑 Client ID: {client_id[:20]}...")
    
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
    print("=" * 60)
    print(oauth_url)
    print("=" * 60)
    
    print(f"\n📝 Langkah-langkah perbaikan:")
    print("1. Copy URL di atas dan buka di browser")
    print("2. Login dengan akun Google yang sesuai")
    print("3. Berikan izin untuk semua scope yang diminta")
    print("4. Copy authorization code yang diberikan")
    print("5. Jalankan script berikut untuk menukar code dengan refresh token:")
    print(f"   python exchange_oauth_code.py {user_mail}")
    
    print(f"\n⚠️  PENTING:")
    print("- Pastikan Anda memberikan izin untuk SEMUA scope yang diminta")
    print("- Jika sebelumnya sudah pernah authorize, Google mungkin tidak menampilkan")
    print("  semua permission lagi. Dalam hal ini, revoke akses aplikasi terlebih dahulu")
    print("  di https://myaccount.google.com/permissions")
    
    # Buat script exchange code
    exchange_script = f"""#!/usr/bin/env python3
import os
import sys
import django
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
sys.path.append(str(BASE_DIR))
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'hris.settings')
django.setup()

from management.oauth_utils import exchange_code_for_refresh_token
from management.database import data_mysql

def main():
    user_mail = "{user_mail}"
    auth_code = input("Masukkan authorization code: ").strip()
    
    if not auth_code:
        print("❌ Authorization code diperlukan")
        return
    
    print(f"🔄 Menukar authorization code dengan refresh token...")
    
    refresh_token, error = exchange_code_for_refresh_token(auth_code, user_mail)
    
    if error:
        print(f"❌ Error: {{error}}")
        return
    
    if refresh_token:
        # Simpan refresh token ke database
        db = data_mysql()
        result = db.update_refresh_token(user_mail, refresh_token)
        
        if result['status']:
            print(f"✅ Refresh token berhasil disimpan!")
            print(f"🎫 Token: {{refresh_token[:20]}}...")
            print(f"\\n🎉 OAuth berhasil diperbaiki untuk {{user_mail}}")
            print(f"💡 Sekarang coba akses AdX Account Data lagi")
        else:
            print(f"❌ Error menyimpan refresh token: {{result['error']}}")
    else:
        print("❌ Tidak mendapatkan refresh token")

if __name__ == "__main__":
    main()
"""
    
    with open('exchange_oauth_code.py', 'w') as f:
        f.write(exchange_script)
    
    print(f"\n✅ Script exchange_oauth_code.py telah dibuat")
    print(f"🚀 Silakan ikuti langkah-langkah di atas untuk menyelesaikan perbaikan")

if __name__ == "__main__":
    main()
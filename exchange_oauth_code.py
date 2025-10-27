#!/usr/bin/env python3
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
    user_mail = "adiarief463@gmail.com"
    auth_code = input("Masukkan authorization code: ").strip()
    
    if not auth_code:
        print("❌ Authorization code diperlukan")
        return
    
    print(f"🔄 Menukar authorization code dengan refresh token...")
    
    refresh_token, error = exchange_code_for_refresh_token(auth_code, user_mail)
    
    if error:
        print(f"❌ Error: {error}")
        return
    
    if refresh_token:
        # Simpan refresh token ke database
        db = data_mysql()
        result = db.update_refresh_token(user_mail, refresh_token)
        
        if result['status']:
            print(f"✅ Refresh token berhasil disimpan!")
            print(f"🎫 Token: {refresh_token[:20]}...")
            print(f"\n🎉 OAuth berhasil diperbaiki untuk {user_mail}")
            print(f"💡 Sekarang coba akses AdX Account Data lagi")
        else:
            print(f"❌ Error menyimpan refresh token: {result['error']}")
    else:
        print("❌ Tidak mendapatkan refresh token")

if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""
Script Universal untuk menukar authorization code dengan refresh token
Bisa digunakan untuk semua user email
"""

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
    print("🔄 Menukar Authorization Code dengan Refresh Token (Universal)")
    print("=" * 70)
    
    # Coba baca user dari file temp
    user_mail = None
    if os.path.exists('.oauth_temp_user'):
        with open('.oauth_temp_user', 'r') as f:
            user_mail = f.read().strip()
        print(f"👤 User dari session sebelumnya: {user_mail}")
        
        # Konfirmasi user
        confirm = input(f"Gunakan user {user_mail}? (y/n): ").strip().lower()
        if confirm not in ['y', 'yes', '']:
            user_mail = None
    
    # Input manual jika tidak ada atau tidak dikonfirmasi
    if not user_mail:
        user_mail = input("📧 Masukkan email user: ").strip()
        
        if not user_mail:
            print("❌ Email user diperlukan")
            return
        
        # Validasi format email
        if '@' not in user_mail or '.' not in user_mail:
            print("❌ Format email tidak valid")
            return
    
    print(f"👤 User: {user_mail}")
    
    # Periksa user di database
    db = data_mysql()
    user_data = db.get_user_by_email(user_mail)
    
    if not user_data['status']:
        print(f"❌ User {user_mail} tidak ditemukan di database")
        return
    
    print("✅ User ditemukan di database")
    
    auth_code = input("\n📄 Masukkan authorization code dari Google: ").strip()
    
    if not auth_code:
        print("❌ Authorization code diperlukan")
        return
    
    print(f"\n🔄 Menukar authorization code dengan refresh token...")
    print("⏳ Mohon tunggu...")
    
    try:
        refresh_token, error = exchange_code_for_refresh_token(auth_code, user_mail)
        
        if error:
            print(f"❌ Error: {error}")
            print(f"\n🔍 TROUBLESHOOTING:")
            print("1. Pastikan authorization code masih valid (tidak expired)")
            print("2. Pastikan code belum pernah digunakan sebelumnya")
            print("3. Pastikan Client ID dan Client Secret benar")
            print("4. Coba generate OAuth URL baru jika code sudah expired:")
            print(f"   python reauth_oauth_universal.py")
            return
        
        if refresh_token:
            print(f"✅ Berhasil mendapatkan refresh token!")
            print(f"🎫 Token: {refresh_token[:20]}...")
            
            # Simpan refresh token ke database
            print(f"\n💾 Menyimpan refresh token ke database...")
            result = db.update_refresh_token(user_mail, refresh_token)
            
            if result['status']:
                print(f"✅ Refresh token berhasil disimpan ke database!")
                print(f"\n🎉 OAuth berhasil diperbaiki untuk {user_mail}")
                print(f"💡 Sekarang coba login dan akses AdX Account Data lagi")
                
                # Test apakah token berfungsi
                print(f"\n🧪 Testing refresh token...")
                from management.utils import fetch_user_adx_account_data
                
                test_result = fetch_user_adx_account_data(user_mail)
                if test_result['status']:
                    print("✅ Test berhasil! Token berfungsi dengan baik")
                    print(f"📊 Data yang berhasil diambil:")
                    data = test_result['data']
                    print(f"   - Network Code: {data.get('network_code')}")
                    print(f"   - Display Name: {data.get('display_name')}")
                    print(f"   - Currency: {data.get('currency_code')}")
                    print(f"   - Active Ad Units: {data.get('active_ad_units', 0)}")
                else:
                    print(f"❌ Test gagal: {test_result.get('error')}")
                    print("💡 Mungkin masih ada masalah dengan akses network atau permissions")
                    
                # Cleanup temp file
                if os.path.exists('.oauth_temp_user'):
                    os.remove('.oauth_temp_user')
                    
            else:
                print(f"❌ Error menyimpan refresh token: {result['error']}")
                print(f"💡 Token berhasil didapat tapi gagal disimpan ke database")
        else:
            print("❌ Tidak mendapatkan refresh token")
            print("💡 Coba ulangi proses authorization")
            
    except Exception as e:
        print(f"❌ Exception: {str(e)}")
        print(f"\n🔍 TROUBLESHOOTING:")
        print("1. Pastikan Django environment sudah setup dengan benar")
        print("2. Periksa koneksi database")
        print("3. Pastikan OAuth credentials tersedia di database")
        print("4. Coba jalankan script reauth_oauth_universal.py untuk generate URL baru")

if __name__ == "__main__":
    main()
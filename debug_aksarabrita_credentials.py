#!/usr/bin/env python3
"""
Debug script untuk memeriksa kredensial OAuth aksarabrita470@gmail.com dan membandingkan dengan adiarief463@gmail.com
"""

import os
import sys
import django

# Setup Django environment
sys.path.append('/Users/ariefdwicahyoadi/hris')
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'hris.settings')
django.setup()

from management.database import data_mysql
from management.utils import get_user_adx_credentials, get_user_ad_manager_client, fetch_user_adx_account_data
import json

def compare_user_credentials(user1_mail, user2_mail):
    """Bandingkan kredensial OAuth antara dua user"""
    print(f"🔍 Membandingkan kredensial OAuth:")
    print(f"   User 1: {user1_mail}")
    print(f"   User 2: {user2_mail}")
    print("=" * 80)
    
    users_data = {}
    
    for user_mail in [user1_mail, user2_mail]:
        print(f"\n📋 Memeriksa kredensial untuk: {user_mail}")
        print("-" * 50)
        
        try:
            # 1. Periksa kredensial di database
            db = data_mysql()
            creds_result = db.get_user_credentials(user_mail=user_mail)
            
            if not creds_result['status']:
                print(f"❌ Gagal mengambil kredensial: {creds_result.get('error')}")
                users_data[user_mail] = {'status': False, 'error': creds_result.get('error')}
                continue
            
            credentials = creds_result['data']
            users_data[user_mail] = {'status': True, 'credentials': credentials}
            
            print(f"✅ Kredensial ditemukan:")
            
            # Tampilkan kredensial (dengan menyembunyikan sebagian untuk keamanan)
            for key, value in credentials.items():
                if key in ['google_oauth2_client_secret', 'google_ads_refresh_token']:
                    # Tampilkan hanya 10 karakter pertama dan terakhir
                    if value and len(str(value)) > 20:
                        masked_value = str(value)[:10] + "..." + str(value)[-10:]
                    else:
                        masked_value = "***" if value else "KOSONG"
                    print(f"   {key}: {masked_value}")
                else:
                    print(f"   {key}: {value}")
            
            # 2. Validasi kredensial yang diperlukan
            print(f"\n📝 Validasi kredensial untuk {user_mail}:")
            required_fields = [
                'client_id',
                'client_secret', 
                'refresh_token',
                'network_code'
            ]
            
            missing_fields = []
            invalid_fields = []
            
            for field in required_fields:
                value = credentials.get(field)
                if not value:
                    missing_fields.append(field)
                elif str(value).strip() in ['', 'None', 'null']:
                    invalid_fields.append(field)
            
            if missing_fields:
                print(f"❌ Field yang hilang: {', '.join(missing_fields)}")
                users_data[user_mail]['missing_fields'] = missing_fields
            if invalid_fields:
                print(f"❌ Field dengan nilai tidak valid: {', '.join(invalid_fields)}")
                users_data[user_mail]['invalid_fields'] = invalid_fields
            
            if not missing_fields and not invalid_fields:
                print("✅ Semua field kredensial tersedia")
                users_data[user_mail]['credentials_valid'] = True
            else:
                users_data[user_mail]['credentials_valid'] = False
            
            # 3. Test pembuatan Ad Manager client
            print(f"\n🧪 Testing Ad Manager client untuk {user_mail}:")
            try:
                client_result = get_user_ad_manager_client(user_mail, skip_network_verification=True)
                
                if client_result['status']:
                    print("✅ Ad Manager client berhasil dibuat")
                    users_data[user_mail]['client_creation'] = True
                    
                    # Test fetch account data
                    print(f"🧪 Testing fetch account data untuk {user_mail}:")
                    account_result = fetch_user_adx_account_data(user_mail)
                    
                    if account_result['status']:
                        print("✅ Fetch account data berhasil")
                        users_data[user_mail]['account_fetch'] = True
                        users_data[user_mail]['account_data'] = {
                            'network_code': account_result['data'].get('network_code'),
                            'display_name': account_result['data'].get('display_name'),
                            'currency_code': account_result['data'].get('currency_code')
                        }
                    else:
                        print(f"❌ Fetch account data gagal: {account_result.get('error')}")
                        users_data[user_mail]['account_fetch'] = False
                        users_data[user_mail]['account_error'] = account_result.get('error')
                        
                else:
                    print(f"❌ Gagal membuat Ad Manager client: {client_result.get('error')}")
                    users_data[user_mail]['client_creation'] = False
                    users_data[user_mail]['client_error'] = client_result.get('error')
                    
            except Exception as e:
                print(f"❌ Exception saat testing client: {str(e)}")
                users_data[user_mail]['client_creation'] = False
                users_data[user_mail]['client_error'] = str(e)
                
        except Exception as e:
            print(f"❌ Error saat memeriksa {user_mail}: {str(e)}")
            users_data[user_mail] = {'status': False, 'error': str(e)}
    
    # 4. Analisis perbandingan
    print(f"\n🔍 ANALISIS PERBANDINGAN:")
    print("=" * 50)
    
    user1_data = users_data.get(user1_mail, {})
    user2_data = users_data.get(user2_mail, {})
    
    if user1_data.get('status') and user2_data.get('status'):
        user1_creds = user1_data.get('credentials', {})
        user2_creds = user2_data.get('credentials', {})
        
        print(f"📊 Perbandingan kredensial:")
        
        # Bandingkan client_id
        if user1_creds.get('google_oauth2_client_id') == user2_creds.get('google_oauth2_client_id'):
            print(f"✅ Client ID sama: {user1_creds.get('google_oauth2_client_id')}")
        else:
            print(f"❌ Client ID berbeda:")
            print(f"   {user1_mail}: {user1_creds.get('google_oauth2_client_id')}")
            print(f"   {user2_mail}: {user2_creds.get('google_oauth2_client_id')}")
        
        # Bandingkan client_secret
        if user1_creds.get('google_oauth2_client_secret') == user2_creds.get('google_oauth2_client_secret'):
            print(f"✅ Client Secret sama")
        else:
            print(f"❌ Client Secret berbeda")
        
        # Bandingkan network_code
        if user1_creds.get('google_ad_manager_network_code') == user2_creds.get('google_ad_manager_network_code'):
            print(f"✅ Network Code sama: {user1_creds.get('google_ad_manager_network_code')}")
        else:
            print(f"❌ Network Code berbeda:")
            print(f"   {user1_mail}: {user1_creds.get('google_ad_manager_network_code')}")
            print(f"   {user2_mail}: {user2_creds.get('google_ad_manager_network_code')}")
        
        # Bandingkan hasil testing
        print(f"\n📊 Perbandingan hasil testing:")
        print(f"   {user1_mail}:")
        print(f"     - Credentials valid: {user1_data.get('credentials_valid', False)}")
        print(f"     - Client creation: {user1_data.get('client_creation', False)}")
        print(f"     - Account fetch: {user1_data.get('account_fetch', False)}")
        
        print(f"   {user2_mail}:")
        print(f"     - Credentials valid: {user2_data.get('credentials_valid', False)}")
        print(f"     - Client creation: {user2_data.get('client_creation', False)}")
        print(f"     - Account fetch: {user2_data.get('account_fetch', False)}")
        
        # Analisis penyebab error
        if user1_data.get('account_fetch') and not user2_data.get('account_fetch'):
            print(f"\n🔍 PENYEBAB ERROR untuk {user2_mail}:")
            error_msg = user2_data.get('account_error', '').lower()
            
            if 'unauthorized_client' in error_msg:
                print("   ❌ Error 'unauthorized_client' menunjukkan:")
                print("     1. Refresh token sudah expired atau invalid")
                print("     2. Client ID tidak dikenal oleh Google")
                print("     3. OAuth scope tidak mencakup Ad Manager API")
                print("     4. User tidak memiliki akses ke Ad Manager network")
                
                # Cek apakah refresh token berbeda
                if user1_creds.get('google_ads_refresh_token') != user2_creds.get('google_ads_refresh_token'):
                    print("   🔍 Refresh token berbeda - kemungkinan token expired")
                
                # Cek apakah network code berbeda
                if user1_creds.get('google_ad_manager_network_code') != user2_creds.get('google_ad_manager_network_code'):
                    print("   🔍 Network code berbeda - user mungkin tidak memiliki akses")
    
    # 5. Rekomendasi perbaikan
    print(f"\n📋 REKOMENDASI PERBAIKAN untuk {user2_mail}:")
    print("=" * 50)
    
    if not user2_data.get('status'):
        print("1. ❌ User tidak ditemukan di database")
        print("   - Pastikan user sudah terdaftar")
        print("   - Periksa ejaan email")
    elif not user2_data.get('credentials_valid'):
        print("1. ❌ Kredensial tidak lengkap atau tidak valid")
        print("   - Jalankan proses OAuth re-authorization")
        print("   - Pastikan semua field terisi dengan benar")
    elif not user2_data.get('client_creation'):
        print("1. ❌ Gagal membuat Ad Manager client")
        print("   - Periksa konfigurasi OAuth di Google Cloud Console")
        print("   - Pastikan Client ID dan Client Secret valid")
    elif not user2_data.get('account_fetch'):
        print("1. ❌ Gagal mengambil data account")
        print("   - Lakukan OAuth re-authorization dengan scope yang benar")
        print("   - Pastikan user memiliki akses ke Ad Manager network")
        print("   - Periksa apakah refresh token masih valid")
    
    print(f"\n🛠️ LANGKAH PERBAIKAN:")
    print("1. Jalankan script OAuth re-authorization untuk aksarabrita470@gmail.com")
    print("2. Pastikan menggunakan scope yang sama dengan adiarief463@gmail.com")
    print("3. Verifikasi akses user ke Ad Manager network")

if __name__ == "__main__":
    user1_email = "adiarief463@gmail.com"  # User yang berhasil
    user2_email = "aksarabrita470@gmail.com"  # User yang error
    compare_user_credentials(user1_email, user2_email)
#!/usr/bin/env python3
"""
Script otomatis untuk memperbaiki OAuth error unauthorized_client
dengan revoke dan re-authorization lengkap
"""

import os
import sys
import django
import requests
import pymysql.cursors
import webbrowser
from datetime import datetime

# Setup Django environment
sys.path.append('/Users/ariefdwicahyoadi/hris')
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'hris.settings')
django.setup()

from management.models import AppOAuthCredentials
from management.oauth_utils import generate_oauth_url_for_user, exchange_code_for_refresh_token

def fix_oauth_complete_auto():
    """Script otomatis untuk fix OAuth error lengkap"""
    
    print("🔧 SCRIPT OTOMATIS PERBAIKAN OAUTH ERROR")
    print("=" * 60)
    print("Script ini akan:")
    print("1. 🔍 Check status OAuth saat ini")
    print("2. 🧹 Revoke existing permissions (manual)")
    print("3. 🔄 Generate OAuth URL baru")
    print("4. 📝 Exchange authorization code")
    print("5. 🧪 Test token baru")
    print("=" * 60)
    
    # Input email user
    email = input("\n📧 Masukkan email user yang bermasalah: ").strip()
    if not email:
        print("❌ Email tidak boleh kosong!")
        return
    
    print(f"\n🔍 STEP 1: CHECKING STATUS OAUTH UNTUK {email}")
    print("-" * 50)
    
    # 1. Check current OAuth status
    oauth_status = check_oauth_status(email)
    if not oauth_status:
        print("❌ User tidak ditemukan atau tidak memiliki OAuth credentials!")
        return
    
    print(f"\n🧹 STEP 2: REVOKE EXISTING PERMISSIONS")
    print("-" * 50)
    print("⚠️  MANUAL ACTION REQUIRED:")
    print(f"1. Buka: https://myaccount.google.com/permissions")
    print(f"2. Login dengan: {email}")
    print(f"3. Cari aplikasi dengan Client ID: {oauth_status['client_id'][:20]}...")
    print(f"4. Klik 'Remove access' atau 'Revoke'")
    print(f"5. Konfirmasi penghapusan")
    
    input("\n⏳ Tekan ENTER setelah selesai revoke permissions...")
    
    print(f"\n🔄 STEP 3: GENERATE OAUTH URL BARU")
    print("-" * 50)
    
    # 2. Generate new OAuth URL
    oauth_url = generate_new_oauth_url(email, oauth_status)
    if not oauth_url:
        print("❌ Gagal generate OAuth URL!")
        return
    
    print(f"✅ OAuth URL berhasil dibuat!")
    print(f"🔗 URL: {oauth_url}")
    
    # Auto open browser
    try:
        webbrowser.open(oauth_url)
        print("🌐 Browser otomatis terbuka...")
    except:
        print("⚠️  Browser tidak bisa dibuka otomatis, copy URL di atas")
    
    print(f"\n📝 STEP 4: AUTHORIZATION PROCESS")
    print("-" * 50)
    print("⚠️  MANUAL ACTION REQUIRED:")
    print(f"1. Login dengan: {email}")
    print(f"2. Berikan izin untuk SEMUA scope yang diminta:")
    print(f"   - openid, email, profile")
    print(f"   - https://www.googleapis.com/auth/dfp")
    print(f"   - https://www.googleapis.com/auth/admanager")
    print(f"3. Setelah autorisasi, Anda akan diarahkan ke halaman error")
    print(f"   (ini normal karena server lokal tidak berjalan)")
    print(f"4. Dari URL di address bar, salin HANYA kode setelah 'code='")
    print(f"   Contoh: jika URL adalah:")
    print(f"   http://localhost:8000/accounts/complete/google-oauth2/?code=4/0AX4XfWh...")
    print(f"   Maka salin: 4/0AX4XfWh...")
    
    auth_code = input("\n🔑 Paste authorization code di sini: ").strip()
    if not auth_code:
        print("❌ Authorization code tidak boleh kosong!")
        return
    
    print(f"\n🔄 STEP 5: EXCHANGE AUTHORIZATION CODE")
    print("-" * 50)
    
    # 3. Exchange authorization code
    success = exchange_authorization_code(email, auth_code, oauth_status)
    if not success:
        print("❌ Gagal exchange authorization code!")
        return
    
    print(f"\n🧪 STEP 6: TEST TOKEN BARU")
    print("-" * 50)
    
    # 4. Test new token
    test_success = test_new_token(email)
    
    print(f"\n" + "=" * 60)
    if test_success:
        print("🎉 OAUTH BERHASIL DIPERBAIKI!")
        print(f"✅ {email} sekarang bisa akses AdX Account Data")
        print("💡 Coba login dan akses menu AdX Account Data lagi")
    else:
        print("❌ MASIH ADA MASALAH!")
        print("💡 Mungkin perlu check akses Ad Manager network")
    print("=" * 60)

def check_oauth_status(email):
    """Check status OAuth credentials saat ini"""
    
    try:
        conn = pymysql.connect(
            host='127.0.0.1',
            port=3307,
            user='root',
            password='',
            database='hris_trendHorizone',
            cursorclass=pymysql.cursors.DictCursor
        )
        
        cursor = conn.cursor()
        query = """
        SELECT 
            google_oauth2_client_id,
            google_oauth2_client_secret,
            google_ads_refresh_token,
            google_ad_manager_network_code
        FROM app_oauth_credentials 
        WHERE user_mail = %s
        """
        
        cursor.execute(query, (email,))
        result = cursor.fetchone()
        
        cursor.close()
        conn.close()
        
        if result:
            print("✅ User ditemukan di database")
            print(f"   - Client ID: {result['google_oauth2_client_id'][:20]}...")
            print(f"   - Network Code: {result['google_ad_manager_network_code']}")
            print(f"   - Refresh Token: {'Ada' if result['google_ads_refresh_token'] else 'Kosong'}")
            
            return {
                'client_id': result['google_oauth2_client_id'],
                'client_secret': result['google_oauth2_client_secret'],
                'network_code': result['google_ad_manager_network_code']
            }
        else:
            return None
            
    except Exception as e:
        print(f"❌ Error check OAuth status: {e}")
        return None

def generate_new_oauth_url(email, oauth_status):
    """Generate OAuth URL baru dengan scope lengkap"""
    
    try:
        # Scope lengkap untuk Ad Manager
        scopes = [
            'openid',
            'email', 
            'profile',
            'https://www.googleapis.com/auth/dfp',
            'https://www.googleapis.com/auth/admanager'
        ]
        
        # OAuth URL parameters
        client_id = oauth_status['client_id']
        # Gunakan standard Django social auth redirect URI
        redirect_uri = 'http://127.0.0.1:8000/accounts/complete/google-oauth2/'
        scope = ' '.join(scopes)
        
        oauth_url = (
            f"https://accounts.google.com/o/oauth2/auth?"
            f"client_id={client_id}&"
            f"redirect_uri={redirect_uri}&"
            f"scope={scope}&"
            f"response_type=code&"
            f"access_type=offline&"
            f"prompt=consent"
        )
        
        # Save user email untuk exchange script
        with open('.oauth_temp_user', 'w') as f:
            f.write(email)
        
        return oauth_url
        
    except Exception as e:
        print(f"❌ Error generate OAuth URL: {e}")
        return None

def exchange_authorization_code(email, auth_code, oauth_status):
    """Exchange authorization code dengan refresh token"""
    
    try:
        print("📡 Mengirim request ke Google OAuth API...")
        
        # Exchange code untuk refresh token
        token_url = "https://oauth2.googleapis.com/token"
        data = {
            'client_id': oauth_status['client_id'],
            'client_secret': oauth_status['client_secret'],
            'code': auth_code,
            'grant_type': 'authorization_code',
            'redirect_uri': 'http://127.0.0.1:8000/accounts/complete/google-oauth2/'
        }
        
        response = requests.post(token_url, data=data)
        
        if response.status_code == 200:
            token_data = response.json()
            refresh_token = token_data.get('refresh_token')
            
            if not refresh_token:
                print("❌ Tidak mendapat refresh token!")
                print("💡 Mungkin perlu revoke permissions dulu")
                return False
            
            print("✅ Berhasil mendapat refresh token!")
            print(f"   - Refresh Token: {refresh_token[:20]}...")
            
            # Update database
            success = update_refresh_token_db(email, refresh_token)
            if success:
                print("✅ Refresh token berhasil disimpan ke database!")
                return True
            else:
                print("❌ Gagal simpan refresh token ke database!")
                return False
                
        else:
            error_data = response.json() if response.content else {}
            print(f"❌ Error exchange code: {response.status_code}")
            print(f"   Error: {error_data.get('error', 'Unknown')}")
            print(f"   Description: {error_data.get('error_description', 'No description')}")
            return False
            
    except Exception as e:
        print(f"❌ Error exchange authorization code: {e}")
        return False

def update_refresh_token_db(email, refresh_token):
    """Update refresh token di database"""
    
    try:
        conn = pymysql.connect(
            host='127.0.0.1',
            port=3307,
            user='root',
            password='',
            database='hris_trendHorizone',
            cursorclass=pymysql.cursors.DictCursor
        )
        
        cursor = conn.cursor()
        query = """
        UPDATE app_oauth_credentials 
        SET google_ads_refresh_token = %s, updated_at = %s
        WHERE user_mail = %s
        """
        
        cursor.execute(query, (refresh_token, datetime.now(), email))
        conn.commit()
        
        cursor.close()
        conn.close()
        
        return True
        
    except Exception as e:
        print(f"❌ Error update database: {e}")
        return False

def test_new_token(email):
    """Test refresh token baru"""
    
    try:
        print("🧪 Testing refresh token baru...")
        
        # Ambil token dari database
        conn = pymysql.connect(
            host='127.0.0.1',
            port=3307,
            user='root',
            password='',
            database='hris_trendHorizone',
            cursorclass=pymysql.cursors.DictCursor
        )
        
        cursor = conn.cursor()
        query = """
        SELECT 
            google_oauth2_client_id,
            google_oauth2_client_secret,
            google_ads_refresh_token
        FROM app_oauth_credentials 
        WHERE user_mail = %s
        """
        
        cursor.execute(query, (email,))
        result = cursor.fetchone()
        
        cursor.close()
        conn.close()
        
        if not result or not result['google_ads_refresh_token']:
            print("❌ Refresh token tidak ditemukan di database!")
            return False
        
        # Test refresh token
        token_url = "https://oauth2.googleapis.com/token"
        data = {
            'client_id': result['google_oauth2_client_id'],
            'client_secret': result['google_oauth2_client_secret'],
            'refresh_token': result['google_ads_refresh_token'],
            'grant_type': 'refresh_token'
        }
        
        response = requests.post(token_url, data=data)
        
        if response.status_code == 200:
            token_data = response.json()
            print("✅ Refresh token VALID!")
            
            # Check scopes
            if 'scope' in token_data:
                scopes = token_data['scope'].split(' ')
                required_scopes = [
                    'https://www.googleapis.com/auth/dfp',
                    'https://www.googleapis.com/auth/admanager'
                ]
                
                missing_scopes = [scope for scope in required_scopes if scope not in scopes]
                
                if missing_scopes:
                    print(f"⚠️  Missing scopes: {missing_scopes}")
                    return False
                else:
                    print("✅ Semua required scopes tersedia!")
                    return True
            else:
                print("✅ Token valid (scope tidak terdeteksi)")
                return True
                
        else:
            error_data = response.json() if response.content else {}
            print(f"❌ Token test failed: {response.status_code}")
            print(f"   Error: {error_data.get('error', 'Unknown')}")
            return False
            
    except Exception as e:
        print(f"❌ Error test token: {e}")
        return False

if __name__ == "__main__":
    fix_oauth_complete_auto()
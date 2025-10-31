#!/usr/bin/env python3
"""
Script untuk test re-authorization flow setelah redirect URIs diperbaiki
"""

import requests
import json
from urllib.parse import urlencode

def generate_oauth_url_test(client_id):
    """Generate OAuth URL untuk test re-authorization"""
    
    print(f"🔗 Generating OAuth URL for re-authorization:")
    print(f"   Client ID: {client_id[:20]}...")
    print("-" * 60)
    
    # Parameter OAuth sesuai dengan yang digunakan aplikasi
    params = {
        'client_id': client_id,
        'redirect_uri': 'https://kiwipixel.com/management/admin/oauth/callback/',
        'scope': 'https://www.googleapis.com/auth/dfp https://www.googleapis.com/auth/admanager',
        'response_type': 'code',
        'access_type': 'offline',
        'prompt': 'consent',  # Force consent untuk generate refresh token baru
        'include_granted_scopes': 'true'
    }
    
    oauth_url = f"https://accounts.google.com/o/oauth2/auth?{urlencode(params)}"
    
    print(f"✅ OAuth URL generated:")
    print(f"   {oauth_url}")
    print()
    print(f"📋 Langkah selanjutnya:")
    print(f"   1. Buka URL di atas di browser")
    print(f"   2. Login dengan akun aksarabrita470@gmail.com")
    print(f"   3. Berikan consent untuk semua scope")
    print(f"   4. Setelah redirect, ambil 'code' parameter dari URL")
    print(f"   5. Gunakan code tersebut untuk exchange ke refresh token")
    
    return oauth_url

def test_code_exchange(client_id, client_secret, auth_code):
    """Test exchange authorization code ke refresh token"""
    
    print(f"\n🔄 Testing code exchange:")
    print(f"   Client ID: {client_id[:20]}...")
    print(f"   Client Secret: {client_secret[:10]}...")
    print(f"   Auth Code: {auth_code[:20]}...")
    print("-" * 60)
    
    data = {
        'client_id': client_id,
        'client_secret': client_secret,
        'code': auth_code,
        'grant_type': 'authorization_code',
        'redirect_uri': 'https://kiwipixel.com/management/admin/oauth/callback/'
    }
    
    try:
        response = requests.post(
            'https://oauth2.googleapis.com/token',
            data=data,
            headers={'Content-Type': 'application/x-www-form-urlencoded'}
        )
        
        print(f"Status Code: {response.status_code}")
        
        if response.status_code == 200:
            result = response.json()
            print("✅ Code exchange berhasil!")
            print(f"   Access Token: {result.get('access_token', '')[:20]}...")
            print(f"   Refresh Token: {result.get('refresh_token', '')[:20]}...")
            print(f"   Token Type: {result.get('token_type', 'N/A')}")
            print(f"   Expires In: {result.get('expires_in', 'N/A')} seconds")
            print(f"   Scope: {result.get('scope', 'N/A')}")
            
            # Simpan refresh token untuk update database
            if 'refresh_token' in result:
                print(f"\n💾 REFRESH TOKEN BARU:")
                print(f"   {result['refresh_token']}")
                print(f"\n📋 Update database dengan query:")
                print(f"   UPDATE app_credentials")
                print(f"   SET refresh_token = '{result['refresh_token']}'")
                print(f"   WHERE user_mail = 'aksarabrita470@gmail.com';")
            
            return True, result
        else:
            error_data = response.json() if response.content else {}
            print("❌ Code exchange gagal!")
            print(f"   Error: {error_data.get('error', 'Unknown')}")
            print(f"   Description: {error_data.get('error_description', 'No description')}")
            
            # Analisis error
            if error_data.get('error') == 'unauthorized_client':
                print(f"\n🔍 UNAUTHORIZED_CLIENT masih terjadi:")
                print(f"   Kemungkinan penyebab:")
                print(f"   1. ❌ Client ID tidak valid")
                print(f"   2. ❌ Client secret salah")
                print(f"   3. ❌ Redirect URI masih belum sync (tunggu beberapa menit)")
                
            return False, error_data
            
    except Exception as e:
        print(f"❌ Exception: {e}")
        return False, str(e)

def test_refresh_token(client_id, client_secret, refresh_token):
    """Test refresh token yang sudah ada"""
    
    print(f"\n🔄 Testing existing refresh token:")
    print(f"   Client ID: {client_id[:20]}...")
    print(f"   Client Secret: {client_secret[:10]}...")
    print(f"   Refresh Token: {refresh_token[:20]}...")
    print("-" * 60)
    
    data = {
        'client_id': client_id,
        'client_secret': client_secret,
        'refresh_token': refresh_token,
        'grant_type': 'refresh_token'
    }
    
    try:
        response = requests.post(
            'https://oauth2.googleapis.com/token',
            data=data,
            headers={'Content-Type': 'application/x-www-form-urlencoded'}
        )
        
        print(f"Status Code: {response.status_code}")
        
        if response.status_code == 200:
            result = response.json()
            print("✅ Refresh token berhasil!")
            print(f"   Access Token: {result.get('access_token', '')[:20]}...")
            print(f"   Token Type: {result.get('token_type', 'N/A')}")
            print(f"   Expires In: {result.get('expires_in', 'N/A')} seconds")
            
            # Test tokeninfo untuk cek scope
            if 'access_token' in result:
                test_token_scopes(result['access_token'])
                
            return True, result
        else:
            error_data = response.json() if response.content else {}
            print("❌ Refresh token gagal!")
            print(f"   Error: {error_data.get('error', 'Unknown')}")
            print(f"   Description: {error_data.get('error_description', 'No description')}")
            return False, error_data
            
    except Exception as e:
        print(f"❌ Exception: {e}")
        return False, str(e)

def test_token_scopes(access_token):
    """Test scope dari access token"""
    
    print(f"\n🔍 Testing token scopes:")
    
    try:
        response = requests.get(
            f'https://oauth2.googleapis.com/tokeninfo?access_token={access_token}'
        )
        
        if response.status_code == 200:
            info = response.json()
            print("✅ Token info berhasil!")
            
            scope = info.get('scope', '')
            print(f"   Granted Scopes: {scope}")
            
            # Check required scopes
            required_scopes = [
                'https://www.googleapis.com/auth/dfp',
                'https://www.googleapis.com/auth/admanager'
            ]
            
            print(f"\n📋 Scope Analysis:")
            all_scopes_present = True
            for req_scope in required_scopes:
                if req_scope in scope:
                    print(f"   ✅ {req_scope}")
                else:
                    print(f"   ❌ {req_scope} - MISSING!")
                    all_scopes_present = False
            
            if all_scopes_present:
                print(f"\n🎉 Semua scope yang diperlukan sudah ada!")
                print(f"   AdX Account page seharusnya bisa diakses sekarang.")
            else:
                print(f"\n⚠️  Ada scope yang hilang!")
                print(f"   Perlu re-authorization dengan scope yang lengkap.")
                
        else:
            print(f"❌ Token info gagal: {response.status_code}")
            
    except Exception as e:
        print(f"❌ Exception: {e}")

if __name__ == "__main__":
    print("=" * 80)
    print("🚀 RE-AUTHORIZATION FLOW TEST")
    print("=" * 80)
    
    print("\n⚠️  PERLU DIISI MANUAL:")
    print("   Ambil CLIENT_ID dan CLIENT_SECRET dari database")
    print("   app_credentials untuk user aksarabrita470@gmail.com")
    print()
    
    # Kredensial - HARUS DIISI MANUAL
    CLIENT_ID = "YOUR_CLIENT_ID_HERE"
    CLIENT_SECRET = "YOUR_CLIENT_SECRET_HERE"
    EXISTING_REFRESH_TOKEN = "YOUR_EXISTING_REFRESH_TOKEN_HERE"  # Optional
    
    if CLIENT_ID == "YOUR_CLIENT_ID_HERE":
        print("❌ Kredensial belum diisi!")
        print("\n📋 Cara mengambil kredensial:")
        print("   1. Akses database MySQL")
        print("   2. Query: SELECT client_id, client_secret, refresh_token")
        print("      FROM app_credentials WHERE user_mail = 'aksarabrita470@gmail.com'")
        print("   3. Copy nilai ke script ini")
        exit(1)
    
    print(f"\n1. GENERATE OAUTH URL")
    print("=" * 50)
    oauth_url = generate_oauth_url_test(CLIENT_ID)
    
    print(f"\n2. TEST EXISTING REFRESH TOKEN (jika ada)")
    print("=" * 50)
    if EXISTING_REFRESH_TOKEN != "YOUR_EXISTING_REFRESH_TOKEN_HERE":
        test_refresh_token(CLIENT_ID, CLIENT_SECRET, EXISTING_REFRESH_TOKEN)
    else:
        print("   Refresh token tidak diisi - skip test")
    
    print(f"\n3. CODE EXCHANGE TEST")
    print("=" * 50)
    print("   Setelah mendapat authorization code dari OAuth URL di atas,")
    print("   edit script ini dan isi AUTH_CODE, lalu jalankan ulang.")
    
    AUTH_CODE = "YOUR_AUTH_CODE_HERE"  # Isi setelah mendapat code dari OAuth flow
    
    if AUTH_CODE != "YOUR_AUTH_CODE_HERE":
        test_code_exchange(CLIENT_ID, CLIENT_SECRET, AUTH_CODE)
    else:
        print("   Authorization code belum diisi - skip test")
    
    print(f"\n" + "=" * 80)
    print("🏁 TEST SELESAI")
    print("=" * 80)
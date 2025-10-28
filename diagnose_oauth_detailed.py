#!/usr/bin/env python3
"""
Script untuk diagnosa detail masalah OAuth unauthorized_client
"""

import requests
import json
from urllib.parse import urlencode, parse_qs, urlparse

def check_oauth_client_config(client_id):
    """
    Cek konfigurasi OAuth client (terbatas - Google tidak expose semua info)
    """
    print(f"🔍 Checking OAuth Client Configuration:")
    print(f"   Client ID: {client_id}")
    print("-" * 60)
    
    # Test dengan authorization URL untuk lihat error message
    auth_params = {
        'client_id': client_id,
        'redirect_uri': 'https://kiwipixel.com/management/admin/oauth/callback/',
        'scope': 'https://www.googleapis.com/auth/dfp https://www.googleapis.com/auth/admanager',
        'response_type': 'code',
        'access_type': 'offline',
        'prompt': 'consent'
    }
    
    auth_url = f"https://accounts.google.com/o/oauth2/auth?{urlencode(auth_params)}"
    print(f"✅ Authorization URL generated:")
    print(f"   {auth_url}")
    print()
    
    # Test dengan redirect URI yang salah untuk lihat error
    wrong_redirect_params = auth_params.copy()
    wrong_redirect_params['redirect_uri'] = 'https://wrong-domain.com/callback/'
    
    wrong_auth_url = f"https://accounts.google.com/o/oauth2/auth?{urlencode(wrong_redirect_params)}"
    print(f"🧪 Test URL dengan redirect URI salah:")
    print(f"   {wrong_auth_url}")
    print()
    
    return auth_url

def test_token_exchange_simulation(client_id, client_secret, auth_code="DUMMY_CODE"):
    """
    Simulasi token exchange untuk lihat error message
    """
    print(f"🔍 Simulating Token Exchange:")
    print(f"   Client ID: {client_id[:20]}...")
    print(f"   Client Secret: {client_secret[:10]}...")
    print(f"   Auth Code: {auth_code}")
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
        
        if response.status_code != 200:
            error_data = response.json() if response.content else {}
            error_code = error_data.get('error', 'unknown')
            error_desc = error_data.get('error_description', 'No description')
            
            print(f"❌ Token exchange failed:")
            print(f"   Error: {error_code}")
            print(f"   Description: {error_desc}")
            
            # Analisis error
            if error_code == 'unauthorized_client':
                print(f"\n🔍 UNAUTHORIZED_CLIENT Analysis:")
                print(f"   Kemungkinan penyebab:")
                print(f"   1. ❌ Redirect URI tidak terdaftar di OAuth client")
                print(f"   2. ❌ Client ID tidak valid atau tidak aktif")
                print(f"   3. ❌ Client secret salah")
                print(f"   4. ❌ OAuth client tidak dikonfigurasi untuk web application")
                
            elif error_code == 'invalid_grant':
                print(f"\n🔍 INVALID_GRANT Analysis:")
                print(f"   Kemungkinan penyebab:")
                print(f"   1. ❌ Authorization code sudah expired/dipakai")
                print(f"   2. ❌ Redirect URI tidak sama dengan saat request auth code")
                print(f"   3. ❌ Client ID tidak sama dengan saat request auth code")
                
        else:
            print(f"✅ Token exchange berhasil (unexpected dengan dummy code)")
            
    except Exception as e:
        print(f"❌ Exception: {e}")

def check_current_oauth_flow_urls():
    """
    Cek URL yang digunakan dalam OAuth flow saat ini
    """
    print(f"🔍 Current OAuth Flow URLs:")
    print("-" * 60)
    
    # URL yang seharusnya dikonfigurasi di Google Cloud Console
    expected_urls = [
        'https://kiwipixel.com/accounts/complete/google-oauth2/',  # Django social auth
        'https://kiwipixel.com/management/admin/oauth/callback/',  # Custom OAuth callback
    ]
    
    print(f"📋 Expected Authorized Redirect URIs in Google Cloud Console:")
    for i, url in enumerate(expected_urls, 1):
        print(f"   {i}. {url}")
    
    print(f"\n⚠️  PENTING:")
    print(f"   Pastikan KEDUA URL di atas terdaftar di Google Cloud Console")
    print(f"   di bagian 'Authorized redirect URIs' untuk OAuth client Anda")

def generate_reauth_instructions(client_id):
    """
    Generate instruksi untuk re-authorization
    """
    print(f"\n🔧 RE-AUTHORIZATION INSTRUCTIONS:")
    print("=" * 60)
    
    print(f"1. 🌐 Buka Google Cloud Console:")
    print(f"   https://console.cloud.google.com/apis/credentials")
    
    print(f"\n2. 🔍 Cari OAuth 2.0 Client dengan ID:")
    print(f"   {client_id}")
    
    print(f"\n3. ✏️  Edit OAuth client dan pastikan 'Authorized redirect URIs' berisi:")
    print(f"   - https://kiwipixel.com/accounts/complete/google-oauth2/")
    print(f"   - https://kiwipixel.com/management/admin/oauth/callback/")
    
    print(f"\n4. 💾 Save perubahan di Google Cloud Console")
    
    print(f"\n5. 🔄 Lakukan re-authorization:")
    print(f"   - Buka: https://kiwipixel.com/management/admin/oauth/management/")
    print(f"   - Klik 'Generate OAuth URL'")
    print(f"   - Login dengan akun target")
    print(f"   - Berikan consent untuk scope yang diminta")
    
    print(f"\n6. ✅ Verifikasi:")
    print(f"   - Cek database app_oauth_credentials untuk refresh token baru")
    print(f"   - Test akses AdX Account page")

if __name__ == "__main__":
    print("=" * 80)
    print("🚀 OAUTH UNAUTHORIZED_CLIENT DETAILED DIAGNOSTIC")
    print("=" * 80)
    
    # Kredensial untuk test - HARUS DIISI MANUAL
    print("\n⚠️  PERLU DIISI MANUAL:")
    print("   Ambil CLIENT_ID dan CLIENT_SECRET dari database")
    print("   app_oauth_credentials untuk user aksarabrita470@gmail.com")
    print()
    
    CLIENT_ID = "YOUR_CLIENT_ID_HERE"
    CLIENT_SECRET = "YOUR_CLIENT_SECRET_HERE"
    
    if CLIENT_ID == "YOUR_CLIENT_ID_HERE":
        print("❌ CLIENT_ID belum diisi!")
        print("\n📋 Cara mengambil kredensial:")
        print("   1. Akses database MySQL")
        print("   2. Query: SELECT google_oauth2_client_id, google_oauth2_client_secret")
        print("      FROM app_oauth_credentials WHERE user_mail = 'aksarabrita470@gmail.com'")
        print("   3. Copy nilai ke script ini")
        print()
        
        # Generate instruksi umum
        check_current_oauth_flow_urls()
        exit(1)
    
    # Jalankan diagnostic
    print(f"\n1. OAUTH CLIENT CONFIGURATION CHECK")
    print("=" * 50)
    auth_url = check_oauth_client_config(CLIENT_ID)
    
    print(f"\n2. TOKEN EXCHANGE SIMULATION")
    print("=" * 50)
    test_token_exchange_simulation(CLIENT_ID, CLIENT_SECRET)
    
    print(f"\n3. OAUTH FLOW URLS CHECK")
    print("=" * 50)
    check_current_oauth_flow_urls()
    
    print(f"\n4. RE-AUTHORIZATION INSTRUCTIONS")
    print("=" * 50)
    generate_reauth_instructions(CLIENT_ID)
    
    print(f"\n" + "=" * 80)
    print("🏁 DIAGNOSTIC SELESAI")
    print("=" * 80)
    print(f"\n💡 NEXT STEPS:")
    print(f"   1. Isi CLIENT_ID dan CLIENT_SECRET di script ini")
    print(f"   2. Jalankan ulang untuk diagnostic detail")
    print(f"   3. Ikuti instruksi re-authorization")
    print(f"   4. Test ulang AdX Account page")
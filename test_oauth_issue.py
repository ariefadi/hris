#!/usr/bin/env python3
"""
Script untuk menguji masalah unauthorized_client tanpa Django
"""

import requests
import json
from urllib.parse import urlencode

def test_token_refresh(client_id, client_secret, refresh_token):
    """Test refresh token dengan client_id/client_secret tertentu"""
    
    print(f"🔍 Testing token refresh:")
    print(f"   Client ID: {client_id[:20]}...")
    print(f"   Client Secret: {client_secret[:10]}...")
    print(f"   Refresh Token: {refresh_token[:20]}...")
    print("-" * 60)
    
    # Data untuk refresh token
    data = {
        'client_id': client_id,
        'client_secret': client_secret,
        'refresh_token': refresh_token,
        'grant_type': 'refresh_token'
    }
    
    try:
        # Request ke Google OAuth2 endpoint
        response = requests.post(
            'https://oauth2.googleapis.com/token',
            data=data,
            headers={'Content-Type': 'application/x-www-form-urlencoded'}
        )
        
        print(f"Status Code: {response.status_code}")
        
        if response.status_code == 200:
            result = response.json()
            print("✅ Token refresh berhasil!")
            print(f"   Access Token: {result.get('access_token', '')[:20]}...")
            print(f"   Token Type: {result.get('token_type', 'N/A')}")
            print(f"   Expires In: {result.get('expires_in', 'N/A')} seconds")
            
            # Test scope dengan tokeninfo
            if 'access_token' in result:
                test_token_info(result['access_token'])
                
            return True, result
        else:
            error_data = response.json() if response.content else {}
            print("❌ Token refresh gagal!")
            print(f"   Error: {error_data.get('error', 'Unknown')}")
            print(f"   Description: {error_data.get('error_description', 'No description')}")
            return False, error_data
            
    except Exception as e:
        print(f"❌ Exception during token refresh: {e}")
        return False, str(e)

def test_token_info(access_token):
    """Test tokeninfo untuk melihat scope"""
    print(f"\n🔍 Testing token info:")
    
    try:
        response = requests.get(
            f'https://oauth2.googleapis.com/tokeninfo?access_token={access_token}'
        )
        
        if response.status_code == 200:
            info = response.json()
            print("✅ Token info berhasil!")
            print(f"   Scope: {info.get('scope', 'N/A')}")
            print(f"   Audience: {info.get('aud', 'N/A')}")
            print(f"   Expires In: {info.get('expires_in', 'N/A')} seconds")
            
            # Check Ad Manager scopes
            scope = info.get('scope', '')
            required_scopes = [
                'https://www.googleapis.com/auth/dfp',
                'https://www.googleapis.com/auth/admanager'
            ]
            
            print(f"\n📋 Scope Analysis:")
            for req_scope in required_scopes:
                if req_scope in scope:
                    print(f"   ✅ {req_scope}")
                else:
                    print(f"   ❌ {req_scope} - MISSING!")
                    
        else:
            print(f"❌ Token info gagal: {response.status_code}")
            print(f"   Response: {response.text}")
            
    except Exception as e:
        print(f"❌ Exception during token info: {e}")

def test_ad_manager_api(access_token, network_code):
    """Test akses ke Ad Manager API"""
    print(f"\n🔍 Testing Ad Manager API access:")
    print(f"   Network Code: {network_code}")
    
    # Ini simulasi - dalam prakteknya perlu library googleads
    print("   (Simulasi - perlu library googleads untuk test sebenarnya)")

if __name__ == "__main__":
    print("=" * 80)
    print("🚀 OAUTH UNAUTHORIZED_CLIENT DIAGNOSTIC TOOL")
    print("=" * 80)
    
    # Kredensial untuk test - ganti dengan nilai sebenarnya
    # Ambil dari database app_oauth_credentials untuk user aksarabrita470@gmail.com
    
    print("\n⚠️  PERLU DIISI MANUAL:")
    print("   Ambil kredensial dari database app_oauth_credentials")
    print("   untuk user aksarabrita470@gmail.com")
    print()
    
    # Contoh - ganti dengan nilai sebenarnya:
    CLIENT_ID = "YOUR_CLIENT_ID_HERE"
    CLIENT_SECRET = "YOUR_CLIENT_SECRET_HERE" 
    REFRESH_TOKEN = "YOUR_REFRESH_TOKEN_HERE"
    NETWORK_CODE = "YOUR_NETWORK_CODE_HERE"
    
    if CLIENT_ID == "YOUR_CLIENT_ID_HERE":
        print("❌ Kredensial belum diisi! Edit script ini dengan nilai dari database.")
        exit(1)
    
    # Test refresh token
    success, result = test_token_refresh(CLIENT_ID, CLIENT_SECRET, REFRESH_TOKEN)
    
    if success and 'access_token' in result:
        # Test Ad Manager API access (simulasi)
        test_ad_manager_api(result['access_token'], NETWORK_CODE)
    
    print("\n" + "=" * 80)
    print("🏁 Test selesai")
    print("=" * 80)
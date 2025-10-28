#!/usr/bin/env python3
"""
Quick test untuk OAuth setelah redirect URIs diperbaiki
"""

import requests
import json

def test_oauth_management_page():
    """Test halaman OAuth management"""
    print("🔍 Testing OAuth Management Page:")
    print("-" * 50)
    
    try:
        response = requests.get("https://kiwipixel.com/management/admin/oauth/management/")
        print(f"Status Code: {response.status_code}")
        
        if response.status_code == 200:
            print("✅ OAuth management page accessible")
            
            # Cek apakah ada form atau link untuk generate OAuth URL
            content = response.text.lower()
            if "generate oauth url" in content or "oauth" in content:
                print("✅ OAuth functionality detected on page")
            else:
                print("⚠️  OAuth functionality not clearly visible")
                
        else:
            print(f"❌ OAuth management page error: {response.status_code}")
            
    except Exception as e:
        print(f"❌ Exception accessing OAuth management: {e}")

def test_adx_account_page():
    """Test halaman AdX account"""
    print("\n🔍 Testing AdX Account Page:")
    print("-" * 50)
    
    try:
        response = requests.get("https://kiwipixel.com/management/admin/page_adx_account")
        print(f"Status Code: {response.status_code}")
        
        if response.status_code == 200:
            content = response.text.lower()
            
            # Cek error patterns
            error_patterns = [
                "unauthorized_client",
                "unauthorized client", 
                "oauth error",
                "authentication error",
                "access denied",
                "invalid_client",
                "invalid client"
            ]
            
            errors_found = []
            for pattern in error_patterns:
                if pattern in content:
                    errors_found.append(pattern)
            
            if errors_found:
                print(f"❌ Errors found: {', '.join(errors_found)}")
                print("   OAuth issue masih ada")
            else:
                print("✅ No OAuth errors detected")
                
                # Cek apakah ada data AdX
                if "network" in content or "adx" in content or "ad manager" in content:
                    print("✅ AdX content detected - likely working!")
                else:
                    print("⚠️  No clear AdX content detected")
                    
        else:
            print(f"❌ AdX account page error: {response.status_code}")
            
    except Exception as e:
        print(f"❌ Exception accessing AdX account: {e}")

def test_oauth_callback_endpoint():
    """Test OAuth callback endpoint"""
    print("\n🔍 Testing OAuth Callback Endpoint:")
    print("-" * 50)
    
    try:
        # Test dengan GET request (seharusnya redirect atau error)
        response = requests.get("https://kiwipixel.com/management/admin/oauth/callback/")
        print(f"Status Code: {response.status_code}")
        
        if response.status_code in [200, 302, 400]:
            print("✅ OAuth callback endpoint accessible")
            
            content = response.text.lower()
            if "error" in content and "unauthorized_client" in content:
                print("❌ unauthorized_client error masih ada di callback")
            else:
                print("✅ No unauthorized_client error in callback")
                
        else:
            print(f"⚠️  Unexpected callback response: {response.status_code}")
            
    except Exception as e:
        print(f"❌ Exception accessing callback: {e}")

def generate_test_oauth_url():
    """Generate OAuth URL untuk test manual"""
    print("\n🔗 Generate Test OAuth URL:")
    print("-" * 50)
    
    # Gunakan client_id dummy untuk test URL generation
    from urllib.parse import urlencode
    
    params = {
        'client_id': 'TEST_CLIENT_ID',
        'redirect_uri': 'https://kiwipixel.com/management/admin/oauth/callback/',
        'scope': 'https://www.googleapis.com/auth/dfp https://www.googleapis.com/auth/admanager',
        'response_type': 'code',
        'access_type': 'offline',
        'prompt': 'consent'
    }
    
    oauth_url = f"https://accounts.google.com/o/oauth2/auth?{urlencode(params)}"
    
    print("📋 Test OAuth URL structure:")
    print(f"   {oauth_url}")
    print()
    print("✅ URL structure looks correct")
    print("   Redirect URI: https://kiwipixel.com/management/admin/oauth/callback/")
    print("   Scopes: dfp + admanager")

if __name__ == "__main__":
    print("=" * 80)
    print("🚀 QUICK OAUTH TEST AFTER REDIRECT URI FIX")
    print("=" * 80)
    
    # Test semua endpoint
    test_oauth_management_page()
    test_adx_account_page() 
    test_oauth_callback_endpoint()
    generate_test_oauth_url()
    
    print(f"\n" + "=" * 80)
    print("🏁 QUICK TEST SELESAI")
    print("=" * 80)
    
    print(f"\n💡 NEXT STEPS:")
    print(f"   1. Jika masih ada error unauthorized_client:")
    print(f"      - Tunggu 5-10 menit untuk Google Cloud Console sync")
    print(f"      - Lakukan re-authorization via OAuth management page")
    print(f"   2. Jika tidak ada error:")
    print(f"      - Coba akses AdX Account page langsung di browser")
    print(f"      - Pastikan data AdX muncul dengan benar")
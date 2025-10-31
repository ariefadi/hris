#!/usr/bin/env python3
"""
Force re-authorization untuk user aksarabrita470@gmail.com
Manual version tanpa Django dependency
"""

from urllib.parse import urlencode

def generate_oauth_url(client_id):
    """Generate OAuth URL untuk re-authorization"""
    params = {
        'client_id': client_id,
        'redirect_uri': 'https://kiwipixel.com/management/admin/oauth/callback/',
        'scope': 'https://www.googleapis.com/auth/dfp https://www.googleapis.com/auth/admanager',
        'response_type': 'code',
        'access_type': 'offline',
        'prompt': 'consent',  # Force consent screen untuk refresh token baru
        'include_granted_scopes': 'true'
    }
    
    return f"https://accounts.google.com/o/oauth2/auth?{urlencode(params)}"

def main():
    print("=" * 80)
    print("🚀 FORCE RE-AUTHORIZATION FOR AKSARABRITA470@GMAIL.COM")
    print("=" * 80)
    
    print("1️⃣ LANGKAH PERTAMA - AMBIL CLIENT_ID DARI DATABASE:")
    print("=" * 60)
    print("Jalankan query SQL ini untuk mendapatkan client_id:")
    print()
    print("```sql")
    print("SELECT google_oauth2_client_id, google_oauth2_client_secret,")
    print("       google_ads_refresh_token, google_ad_manager_network_code")
    print("FROM app_credentials")
    print("WHERE user_mail = 'aksarabrita470@gmail.com';")
    print("```")
    print()
    
    print("2️⃣ LANGKAH KEDUA - CLEAR REFRESH TOKEN (OPTIONAL):")
    print("=" * 60)
    print("Untuk memaksa re-authorization, clear refresh token:")
    print()
    print("```sql")
    print("UPDATE app_credentials")
    print("SET refresh_token = NULL")
    print("WHERE user_mail = 'aksarabrita470@gmail.com';")
    print("```")
    print()
    
    print("3️⃣ LANGKAH KETIGA - GENERATE OAUTH URL:")
    print("=" * 60)
    
    # Generate dengan placeholder client_id
    placeholder_client_id = "YOUR_CLIENT_ID_FROM_DATABASE"
    oauth_url = generate_oauth_url(placeholder_client_id)
    
    print("Ganti YOUR_CLIENT_ID_FROM_DATABASE dengan client_id dari step 1:")
    print()
    print(f"OAuth URL Template:")
    print(f"{oauth_url}")
    print()
    
    print("4️⃣ LANGKAH KEEMPAT - MANUAL RE-AUTHORIZATION:")
    print("=" * 60)
    print("1. Copy OAuth URL yang sudah diisi client_id")
    print("2. Buka di browser")
    print("3. Login dengan aksarabrita470@gmail.com")
    print("4. Accept semua permissions (jangan skip!)")
    print("5. Tunggu redirect ke:")
    print("   https://kiwipixel.com/management/admin/oauth/callback/")
    print("6. Cek apakah ada error di halaman callback")
    print()
    
    print("5️⃣ LANGKAH KELIMA - VERIFICATION:")
    print("=" * 60)
    print("Setelah re-authorization berhasil:")
    print("1. Cek database bahwa refresh_token sudah terupdate:")
    print()
    print("```sql")
    print("SELECT refresh_token, updated_at")
    print("FROM app_credentials")
    print("WHERE user_mail = 'aksarabrita470@gmail.com';")
    print("```")
    print()
    print("2. Test AdX Account page:")
    print("   python3 quick_oauth_test.py")
    print()
    print("3. Atau akses langsung:")
    print("   https://kiwipixel.com/management/admin/page_adx_account")
    print()
    
    print("6️⃣ TROUBLESHOOTING JIKA MASIH ERROR:")
    print("=" * 60)
    
    issues = [
        {
            "error": "unauthorized_client di callback",
            "cause": "Client ID tidak match atau redirect URI salah",
            "fix": "Cek Google Cloud Console OAuth client configuration"
        },
        {
            "error": "access_denied di callback", 
            "cause": "User tidak accept permissions atau account tidak punya akses",
            "fix": "Re-authorize dengan account yang benar dan accept semua permissions"
        },
        {
            "error": "invalid_scope di callback",
            "cause": "OAuth client tidak enable Ad Manager scopes",
            "fix": "Enable dfp dan admanager scopes di Google Cloud Console"
        },
        {
            "error": "Masih oauth error di AdX page",
            "cause": "Network code salah atau API tidak enabled",
            "fix": "Cek network code di Ad Manager dan enable Ad Manager API"
        }
    ]
    
    for i, issue in enumerate(issues, 1):
        print(f"{i}. Error: {issue['error']}")
        print(f"   Cause: {issue['cause']}")
        print(f"   Fix: {issue['fix']}")
        print()
    
    print("7️⃣ GOOGLE CLOUD CONSOLE CHECKLIST:")
    print("=" * 60)
    print("Pastikan di Google Cloud Console:")
    print("✅ Project yang benar dipilih")
    print("✅ APIs & Services > Enabled APIs:")
    print("   - Ad Manager API = ENABLED")
    print("   - Google Ads API = ENABLED (optional)")
    print("✅ APIs & Services > Credentials:")
    print("   - OAuth 2.0 Client IDs ada dan aktif")
    print("   - Authorized redirect URIs include:")
    print("     * https://kiwipixel.com/management/admin/oauth/callback/")
    print("     * https://kiwipixel.com/accounts/complete/google-oauth2/")
    print("✅ OAuth consent screen configured")
    print("✅ Scopes include:")
    print("   - https://www.googleapis.com/auth/dfp")
    print("   - https://www.googleapis.com/auth/admanager")
    
    print(f"\n" + "=" * 80)
    print("🏁 READY FOR RE-AUTHORIZATION")
    print("=" * 80)
    print("Ikuti langkah 1-5 di atas secara berurutan.")
    print("Jika masih ada masalah, cek troubleshooting di langkah 6-7.")

if __name__ == "__main__":
    main()
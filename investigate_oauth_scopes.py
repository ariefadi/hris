#!/usr/bin/env python3
"""
Investigasi OAuth scopes dan permission issues
"""

import requests
import json
from urllib.parse import urlencode, parse_qs, urlparse

def check_google_oauth_scopes():
    """Cek scopes yang diperlukan untuk Ad Manager API"""
    print("🔍 Required OAuth Scopes for Ad Manager:")
    print("-" * 50)
    
    required_scopes = [
        "https://www.googleapis.com/auth/dfp",
        "https://www.googleapis.com/auth/admanager"
    ]
    
    print("✅ Required scopes:")
    for scope in required_scopes:
        print(f"   - {scope}")
    
    print(f"\n💡 Note: Kedua scope ini harus ada dalam OAuth client configuration")

def test_oauth_token_info_endpoint():
    """Test OAuth token info endpoint untuk debugging"""
    print(f"\n🔍 OAuth Token Info Endpoint Test:")
    print("-" * 50)
    
    # Simulasi test dengan dummy token
    print("📋 Untuk test manual, gunakan URL ini dengan access_token yang valid:")
    print("   https://www.googleapis.com/oauth2/v1/tokeninfo?access_token=YOUR_ACCESS_TOKEN")
    print()
    print("✅ Response yang diharapkan:")
    print("   - scope: harus include 'dfp' atau 'admanager'")
    print("   - audience: harus match dengan client_id")
    print("   - expires_in: harus > 0")

def check_common_oauth_issues():
    """Cek common OAuth issues"""
    print(f"\n🔍 Common OAuth Issues Checklist:")
    print("-" * 50)
    
    issues = [
        {
            "issue": "Refresh token expired",
            "solution": "Re-authorize user melalui OAuth flow",
            "check": "Cek apakah token dibuat > 6 bulan lalu"
        },
        {
            "issue": "Client ID/Secret mismatch", 
            "solution": "Pastikan client_id di database = client_id di Google Cloud",
            "check": "Bandingkan nilai di app_oauth_credentials dengan Google Cloud Console"
        },
        {
            "issue": "Insufficient scopes",
            "solution": "Re-authorize dengan scope lengkap (dfp + admanager)",
            "check": "Pastikan OAuth client di Google Cloud enable kedua scopes"
        },
        {
            "issue": "Network code mismatch",
            "solution": "Pastikan network_code di database sesuai dengan Ad Manager account",
            "check": "Login ke Google Ad Manager, cek network code di URL"
        },
        {
            "issue": "OAuth client disabled/deleted",
            "solution": "Cek status OAuth client di Google Cloud Console",
            "check": "Pastikan OAuth client masih aktif dan tidak di-disable"
        }
    ]
    
    for i, issue in enumerate(issues, 1):
        print(f"{i}. ❌ {issue['issue']}")
        print(f"   🔧 Solution: {issue['solution']}")
        print(f"   ✅ Check: {issue['check']}")
        print()

def generate_reauth_instructions():
    """Generate step-by-step re-authorization instructions"""
    print(f"\n🚀 STEP-BY-STEP RE-AUTHORIZATION:")
    print("=" * 60)
    
    steps = [
        "1. Buka https://kiwipixel.com/management/admin/oauth/management/",
        "2. Login sebagai admin yang memiliki akses",
        "3. Cari section untuk 'Generate OAuth URL' atau 'Re-authorize'",
        "4. Klik untuk generate OAuth URL baru",
        "5. Copy URL dan buka di browser baru",
        "6. Login dengan akun aksarabrita470@gmail.com",
        "7. Accept semua permissions yang diminta",
        "8. Pastikan redirect kembali ke callback URL tanpa error",
        "9. Cek database bahwa refresh_token sudah terupdate",
        "10. Test akses AdX Account page lagi"
    ]
    
    for step in steps:
        print(f"   {step}")
    
    print(f"\n⚠️  PENTING:")
    print(f"   - Pastikan login dengan akun yang sama (aksarabrita470@gmail.com)")
    print(f"   - Jangan skip permission screens")
    print(f"   - Tunggu sampai redirect selesai sebelum test lagi")

def check_ad_manager_api_status():
    """Cek status Ad Manager API"""
    print(f"\n🔍 Ad Manager API Status Check:")
    print("-" * 50)
    
    print("📋 Manual checks yang perlu dilakukan:")
    print("   1. Buka Google Cloud Console")
    print("   2. Pilih project yang benar")
    print("   3. Ke APIs & Services > Enabled APIs")
    print("   4. Cari 'Ad Manager API' atau 'DoubleClick for Publishers API'")
    print("   5. Pastikan status = ENABLED")
    print()
    print("✅ Jika API tidak enabled:")
    print("   - Enable Ad Manager API")
    print("   - Tunggu beberapa menit untuk propagation")
    print("   - Re-test OAuth flow")

def generate_debug_sql_queries():
    """Generate SQL queries untuk debugging"""
    print(f"\n🔍 Debug SQL Queries:")
    print("-" * 50)
    
    queries = [
        {
            "purpose": "Cek kredensial user aksarabrita",
            "query": """
SELECT 
    user_mail,
    google_oauth2_client_id,
    google_oauth2_client_secret,
    google_ads_refresh_token,
    google_ad_manager_network_code,
    created_at,
    updated_at
FROM app_oauth_credentials 
WHERE user_mail = 'aksarabrita470@gmail.com';
"""
        },
        {
            "purpose": "Cek semua users dengan OAuth credentials",
            "query": """
SELECT 
    user_mail,
    google_oauth2_client_id,
    google_ad_manager_network_code,
    updated_at
FROM app_oauth_credentials 
WHERE google_ads_refresh_token IS NOT NULL
ORDER BY updated_at DESC;
"""
        },
        {
            "purpose": "Cek apakah ada duplicate client_id",
            "query": """
SELECT 
    google_oauth2_client_id,
    COUNT(*) as count,
    GROUP_CONCAT(user_mail) as users
FROM app_oauth_credentials 
WHERE google_oauth2_client_id IS NOT NULL
GROUP BY google_oauth2_client_id
HAVING count > 1;
"""
        }
    ]
    
    for i, query in enumerate(queries, 1):
        print(f"{i}. {query['purpose']}:")
        print(f"```sql")
        print(query['query'].strip())
        print(f"```")
        print()

if __name__ == "__main__":
    print("=" * 80)
    print("🔍 OAUTH SCOPES & PERMISSIONS INVESTIGATION")
    print("=" * 80)
    
    check_google_oauth_scopes()
    test_oauth_token_info_endpoint()
    check_common_oauth_issues()
    generate_reauth_instructions()
    check_ad_manager_api_status()
    generate_debug_sql_queries()
    
    print("=" * 80)
    print("🏁 INVESTIGATION COMPLETE")
    print("=" * 80)
    
    print(f"\n💡 RECOMMENDED NEXT ACTION:")
    print(f"   Lakukan re-authorization lengkap untuk user aksarabrita470@gmail.com")
    print(f"   karena kemungkinan besar refresh token sudah expired atau scope tidak lengkap.")
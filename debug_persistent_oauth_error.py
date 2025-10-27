#!/usr/bin/env python3
"""
Debug script untuk menganalisis mengapa error unauthorized_client masih terjadi
setelah OAuth re-authorization untuk aksarabrita470@gmail.com
"""

import os
import sys
import django
from datetime import datetime

# Setup Django environment
sys.path.append('/Users/ariefdwicahyoadi/hris')
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'hris.settings')
django.setup()

from management.models import AppOAuthCredentials
from management.oauth_utils import exchange_code_for_refresh_token
from googleads import ad_manager
import json

def debug_oauth_status():
    """Debug OAuth status untuk aksarabrita470@gmail.com"""
    
    print("🔍 DEBUGGING OAUTH STATUS UNTUK aksarabrita470@gmail.com")
    print("=" * 60)
    
    # 1. Cek user di database
    try:
        oauth_creds = AppOAuthCredentials.objects.get(user_mail='aksarabrita470@gmail.com')
        print(f"✅ User ditemukan: {oauth_creds.user_mail}")
        print(f"   - ID: {oauth_creds.user_id}")
        print(f"   - Created: {oauth_creds.created_at}")
        print(f"   - Updated: {oauth_creds.updated_at}")
    except AppOAuthCredentials.DoesNotExist:
        print("❌ User tidak ditemukan di database!")
        return
    
    # 2. Cek OAuth credentials
    print(f"✅ OAuth credentials ditemukan")
    print(f"   - Client ID: {oauth_creds.google_oauth2_client_id[:20]}...")
    print(f"   - Client Secret: {oauth_creds.google_oauth2_client_secret[:10]}...")
    print(f"   - Network Code: {oauth_creds.google_ad_manager_network_code}")
    print(f"   - Refresh Token: {oauth_creds.google_ads_refresh_token[:20] if oauth_creds.google_ads_refresh_token else 'None'}...")
    
    # Cek apakah refresh token ada
    if not oauth_creds.google_ads_refresh_token:
        print("❌ MASALAH: Refresh token kosong!")
        return
    
    print("\n" + "=" * 60)
    print("🧪 TESTING AD MANAGER CLIENT CREATION")
    print("=" * 60)
    
    # 3. Test Ad Manager client creation
    try:
        # Setup Ad Manager client
        ad_manager_client = ad_manager.AdManagerClient.LoadFromString(f"""
        [ad_manager]
        application_name = HRIS AdX Integration
        network_code = {oauth_creds.google_ad_manager_network_code}
        
        [OAUTH2]
        client_id = {oauth_creds.google_oauth2_client_id}
        client_secret = {oauth_creds.google_oauth2_client_secret}
        refresh_token = {oauth_creds.google_ads_refresh_token}
        """)
        
        print("✅ Ad Manager client berhasil dibuat")
        
        # Test dengan NetworkService
        network_service = ad_manager_client.GetService('NetworkService', version='v202311')
        print("✅ NetworkService berhasil diambil")
        
        # Test getCurrentNetwork
        current_network = network_service.getCurrentNetwork()
        print("✅ getCurrentNetwork berhasil!")
        print(f"   - Network Code: {current_network['networkCode']}")
        print(f"   - Display Name: {current_network['displayName']}")
        print(f"   - Currency: {current_network['currencyCode']}")
        
    except Exception as e:
        print(f"❌ Error saat test Ad Manager client: {e}")
        print(f"   Error type: {type(e).__name__}")
        
        # Analisis error lebih detail
        if "unauthorized_client" in str(e).lower():
            print("\n🔍 ANALISIS ERROR unauthorized_client:")
            print("   Kemungkinan penyebab:")
            print("   1. Refresh token expired atau invalid")
            print("   2. Client ID/Secret tidak cocok dengan refresh token")
            print("   3. Scope OAuth tidak lengkap")
            print("   4. Network code tidak sesuai dengan akun")
            
        return
    
    print("\n" + "=" * 60)
    print("🎯 TESTING INVENTORY SERVICE (AdX Account Data)")
    print("=" * 60)
    
    # 4. Test InventoryService untuk AdX account data
    try:
        inventory_service = ad_manager_client.GetService('InventoryService', version='v202311')
        print("✅ InventoryService berhasil diambil")
        
        # Test getAdUnitsByStatement
        from googleads.ad_manager import FilterStatement
        statement = FilterStatement(limit=5)
        ad_units = inventory_service.getAdUnitsByStatement(statement.ToStatement())
        
        print("✅ getAdUnitsByStatement berhasil!")
        print(f"   - Total Ad Units: {ad_units.get('totalResultSetSize', 0)}")
        
        if ad_units.get('results'):
            for i, ad_unit in enumerate(ad_units['results'][:3]):
                print(f"   - Ad Unit {i+1}: {ad_unit.get('name', 'N/A')} (ID: {ad_unit.get('id', 'N/A')})")
        
    except Exception as e:
        print(f"❌ Error saat test InventoryService: {e}")
        print(f"   Error type: {type(e).__name__}")
        
        if "unauthorized_client" in str(e).lower():
            print("\n🚨 MASALAH DITEMUKAN!")
            print("   InventoryService gagal dengan unauthorized_client")
            print("   Ini menunjukkan masalah dengan:")
            print("   1. Akses ke Ad Manager API")
            print("   2. Permissions untuk network tertentu")
            print("   3. Scope OAuth yang tidak lengkap")
    
    print("\n" + "=" * 60)
    print("💡 REKOMENDASI PERBAIKAN")
    print("=" * 60)
    
    print("1. 🔄 Coba re-authorization dengan scope lengkap:")
    print("   - https://www.googleapis.com/auth/dfp")
    print("   - https://www.googleapis.com/auth/admanager")
    print("   - openid, email, profile")
    
    print("\n2. 🔍 Periksa Google Cloud Console:")
    print("   - Pastikan Client ID aktif")
    print("   - Periksa authorized redirect URIs")
    print("   - Verifikasi API yang diaktifkan")
    
    print("\n3. 🏢 Verifikasi akses Ad Manager:")
    print("   - Login ke Ad Manager dengan aksarabrita470@gmail.com")
    print("   - Periksa akses ke network 23303542838")
    print("   - Pastikan role/permissions mencukupi")
    
    print("\n4. 🧹 Clean up dan re-authorize:")
    print("   - Revoke existing app permissions di Google Account")
    print("   - Generate OAuth URL baru")
    print("   - Berikan semua permissions yang diminta")

if __name__ == "__main__":
    debug_oauth_status()
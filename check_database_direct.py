#!/usr/bin/env python3
"""
Script untuk mengecek database secara langsung tanpa Django ORM
untuk menghindari masalah UUID conversion
"""

import pymysql.cursors
import os
from datetime import datetime

def check_database_direct():
    """Check database langsung dengan PyMySQL connector"""
    
    print("🔍 CHECKING DATABASE LANGSUNG UNTUK aksarabrita470@gmail.com")
    print("=" * 70)
    
    try:
        # Koneksi ke database berdasarkan konfigurasi yang ditemukan
        conn = pymysql.connect(
            host='127.0.0.1',
            port=3307,
            user='root',
            password='',
            database='hris_trendHorizone',
            cursorclass=pymysql.cursors.DictCursor
        )
        
        cursor = conn.cursor()
        
        # Query untuk mencari user aksarabrita470@gmail.com
        query = """
        SELECT 
            user_id,
            user_mail,
            google_oauth2_client_id,
            google_oauth2_client_secret,
            google_ads_client_id,
            google_ads_client_secret,
            google_ads_refresh_token,
            google_ad_manager_network_code,
            is_active,
            created_at,
            updated_at
        FROM app_oauth_credentials 
        WHERE user_mail = %s
        """
        
        cursor.execute(query, ('aksarabrita470@gmail.com',))
        result = cursor.fetchone()
        
        if not result:
            print("❌ User aksarabrita470@gmail.com tidak ditemukan di database!")
            
            # Cek semua user yang ada
            cursor.execute("SELECT user_mail FROM app_oauth_credentials")
            all_users = cursor.fetchall()
            print(f"\n📋 User yang ada di database ({len(all_users)}):")
            for user in all_users:
                print(f"   - {user['user_mail']}")
            
            return
        
        print("✅ User ditemukan di database!")
        print(f"   - User ID: {result['user_id']}")
        print(f"   - Email: {result['user_mail']}")
        print(f"   - Created: {result['created_at']}")
        print(f"   - Updated: {result['updated_at']}")
        print(f"   - Active: {result['is_active']}")
        
        print("\n" + "=" * 70)
        print("🔑 OAUTH CREDENTIALS")
        print("=" * 70)
        
        print(f"✅ OAuth2 Client ID: {result['google_oauth2_client_id'][:20]}...")
        print(f"✅ OAuth2 Client Secret: {result['google_oauth2_client_secret'][:10]}...")
        print(f"✅ Network Code: {result['google_ad_manager_network_code']}")
        
        # Cek refresh token
        refresh_token = result['google_ads_refresh_token']
        if refresh_token:
            print(f"✅ Refresh Token: {refresh_token[:20]}... (Length: {len(refresh_token)})")
        else:
            print("❌ Refresh Token: KOSONG!")
            
        # Cek Google Ads credentials (optional)
        if result['google_ads_client_id']:
            print(f"✅ Google Ads Client ID: {result['google_ads_client_id'][:20]}...")
        else:
            print("⚠️  Google Ads Client ID: Tidak ada")
            
        if result['google_ads_client_secret']:
            print(f"✅ Google Ads Client Secret: {result['google_ads_client_secret'][:10]}...")
        else:
            print("⚠️  Google Ads Client Secret: Tidak ada")
        
        print("\n" + "=" * 70)
        print("🧪 ANALISIS MASALAH")
        print("=" * 70)
        
        issues = []
        
        if not refresh_token:
            issues.append("❌ Refresh token kosong - perlu re-authorization")
        
        if not result['google_ad_manager_network_code']:
            issues.append("❌ Network code kosong")
            
        if not result['is_active']:
            issues.append("❌ User tidak aktif")
            
        if issues:
            print("🚨 MASALAH DITEMUKAN:")
            for issue in issues:
                print(f"   {issue}")
        else:
            print("✅ Semua credentials terlihat lengkap")
            print("   Masalah mungkin ada di:")
            print("   1. Refresh token expired")
            print("   2. Scope OAuth tidak lengkap")
            print("   3. Akses network Ad Manager")
        
        print("\n" + "=" * 70)
        print("💡 REKOMENDASI")
        print("=" * 70)
        
        if not refresh_token:
            print("1. 🔄 Jalankan re-authorization:")
            print("   python reauth_oauth_universal.py")
            print("   python exchange_oauth_code_universal.py")
        else:
            print("1. 🧪 Test refresh token dengan script terpisah")
            print("2. 🔄 Coba revoke dan re-authorize dengan scope lengkap")
            print("3. 🏢 Periksa akses Ad Manager network di Google Ad Manager")
        
        cursor.close()
        conn.close()
        
    except pymysql.Error as e:
        print(f"❌ Error koneksi database: {e}")
        print("💡 Periksa konfigurasi database (host, user, password, database)")
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    check_database_direct()
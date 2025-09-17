#!/usr/bin/env python3
"""
Script untuk mendiagnosis masalah AdX Traffic Account
Memeriksa kredensial user, koneksi database, dan pengambilan data
"""

import os
import sys
import django
from datetime import datetime, timedelta

# Setup Django environment
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'hris.settings')
django.setup()

from management.database import data_mysql
from management.utils import (
    get_user_adx_credentials, 
    get_user_ad_manager_client,
    fetch_adx_traffic_account_by_user
)

def check_user_credentials(user_email):
    """Periksa kredensial user di database"""
    print(f"\n=== CHECKING USER CREDENTIALS FOR: {user_email} ===")
    
    try:
        db = data_mysql()
        sql = """
            SELECT user_id, user_name, user_mail, client_id, client_secret, 
                   refresh_token, network_code, developer_token
            FROM app_users 
            WHERE user_mail = %s
        """
        
        db.cur_hris.execute(sql, (user_email,))
        user_data = db.cur_hris.fetchone()
        
        if not user_data:
            print(f"❌ User tidak ditemukan untuk email: {user_email}")
            return False
        
        print(f"✅ User ditemukan:")
        print(f"   - User ID: {user_data.get('user_id')}")
        print(f"   - User Name: {user_data.get('user_name')}")
        print(f"   - Email: {user_data.get('user_mail')}")
        
        # Check required credentials
        required_fields = ['client_id', 'client_secret', 'refresh_token', 'network_code', 'developer_token']
        missing_fields = []
        
        for field in required_fields:
            value = user_data.get(field)
            if not value or str(value).strip() == '':
                missing_fields.append(field)
                print(f"   ❌ {field}: KOSONG")
            else:
                # Mask sensitive data
                if field in ['client_secret', 'refresh_token', 'developer_token']:
                    masked_value = str(value)[:8] + '...' + str(value)[-4:] if len(str(value)) > 12 else '***'
                    print(f"   ✅ {field}: {masked_value}")
                else:
                    print(f"   ✅ {field}: {value}")
        
        if missing_fields:
            print(f"\n❌ KREDENSIAL TIDAK LENGKAP! Missing: {', '.join(missing_fields)}")
            return False
        else:
            print(f"\n✅ SEMUA KREDENSIAL TERSEDIA")
            return True
            
    except Exception as e:
        print(f"❌ Error checking user credentials: {e}")
        return False

def test_ad_manager_client(user_email):
    """Test koneksi Ad Manager client"""
    print(f"\n=== TESTING AD MANAGER CLIENT ===")
    
    try:
        client_result = get_user_ad_manager_client(user_email)
        
        if not client_result['status']:
            print(f"❌ Gagal membuat Ad Manager client: {client_result['error']}")
            return False
        
        client = client_result['client']
        print(f"✅ Ad Manager client berhasil dibuat")
        
        # Test basic service access
        try:
            report_service = client.GetService('ReportService', version='v202408')
            print(f"✅ Report Service berhasil diakses")
            return True
        except Exception as service_error:
            print(f"❌ Gagal mengakses Report Service: {service_error}")
            return False
            
    except Exception as e:
        print(f"❌ Error testing Ad Manager client: {e}")
        return False

def test_data_fetch(user_email, days_back=7):
    """Test pengambilan data AdX"""
    print(f"\n=== TESTING DATA FETCH (Last {days_back} days) ===")
    
    try:
        # Calculate date range
        end_date = datetime.now().date()
        start_date = end_date - timedelta(days=days_back)
        
        print(f"Date range: {start_date} to {end_date}")
        
        # Test data fetch
        result = fetch_adx_traffic_account_by_user(
            user_email, 
            start_date.strftime('%Y-%m-%d'), 
            end_date.strftime('%Y-%m-%d')
        )
        
        if not result.get('status', False):
            print(f"❌ Gagal mengambil data: {result.get('error', 'Unknown error')}")
            return False
        
        data = result.get('data', [])
        print(f"✅ Data berhasil diambil: {len(data)} rows")
        
        if len(data) > 0:
            print(f"\nSample data (first 3 rows):")
            for i, row in enumerate(data[:3]):
                print(f"  Row {i+1}: {row}")
        else:
            print(f"\n⚠️  Data kosong - mungkin tidak ada traffic untuk periode ini")
        
        return True
        
    except Exception as e:
        print(f"❌ Error testing data fetch: {e}")
        return False

def get_all_users_with_credentials():
    """Dapatkan semua user yang memiliki kredensial AdX"""
    print(f"\n=== CHECKING ALL USERS WITH ADX CREDENTIALS ===")
    
    try:
        db = data_mysql()
        sql = """
            SELECT user_mail, user_name,
                   CASE WHEN client_id IS NOT NULL AND client_id != '' THEN 1 ELSE 0 END as has_client_id,
                   CASE WHEN client_secret IS NOT NULL AND client_secret != '' THEN 1 ELSE 0 END as has_client_secret,
                   CASE WHEN refresh_token IS NOT NULL AND refresh_token != '' THEN 1 ELSE 0 END as has_refresh_token,
                   CASE WHEN developer_token IS NOT NULL AND developer_token != '' THEN 1 ELSE 0 END as has_developer_token,
                   CASE WHEN network_code IS NOT NULL AND network_code != '' THEN 1 ELSE 0 END as has_network_code
            FROM app_users 
            WHERE user_mail IS NOT NULL AND user_mail != ''
            ORDER BY user_mail
        """
        
        db.cur_hris.execute(sql)
        users = db.cur_hris.fetchall()
        
        print(f"\nFound {len(users)} users:")
        
        complete_users = []
        for user in users:
            email = user['user_mail']
            name = user['user_name']
            
            # Check completeness
            credentials_count = (
                user['has_client_id'] + user['has_client_secret'] + 
                user['has_refresh_token'] + user['has_developer_token'] + 
                user['has_network_code']
            )
            
            status = "✅ COMPLETE" if credentials_count == 5 else f"❌ INCOMPLETE ({credentials_count}/5)"
            print(f"  {email} ({name}): {status}")
            
            if credentials_count == 5:
                complete_users.append(email)
        
        return complete_users
        
    except Exception as e:
        print(f"❌ Error checking all users: {e}")
        return []

def main():
    print("🔍 DIAGNOSA ADX TRAFFIC ACCOUNT")
    print("=" * 50)
    
    # Get all users with complete credentials
    complete_users = get_all_users_with_credentials()
    
    if not complete_users:
        print("\n❌ TIDAK ADA USER DENGAN KREDENSIAL LENGKAP!")
        print("\nSolusi:")
        print("1. Pastikan user sudah melakukan OAuth flow")
        print("2. Periksa tabel app_users di database")
        print("3. Pastikan semua field kredensial terisi:")
        print("   - client_id")
        print("   - client_secret")
        print("   - refresh_token")
        print("   - developer_token")
        print("   - network_code")
        return
    
    print(f"\n✅ Found {len(complete_users)} users with complete credentials")
    
    # Test with first complete user
    test_email = complete_users[0]
    print(f"\n🧪 TESTING WITH USER: {test_email}")
    
    # Step 1: Check credentials
    if not check_user_credentials(test_email):
        return
    
    # Step 2: Test Ad Manager client
    if not test_ad_manager_client(test_email):
        return
    
    # Step 3: Test data fetch
    if not test_data_fetch(test_email):
        return
    
    print(f"\n🎉 SEMUA TEST BERHASIL!")
    print(f"\nJika data masih kosong di frontend, periksa:")
    print(f"1. Apakah user yang login memiliki kredensial lengkap?")
    print(f"2. Apakah periode tanggal yang dipilih memiliki data?")
    print(f"3. Apakah network memiliki traffic AdX?")
    print(f"4. Periksa browser console untuk error JavaScript")

if __name__ == '__main__':
    main()
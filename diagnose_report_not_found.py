#!/usr/bin/env python3
"""
Script untuk mendiagnosis error REPORT_NOT_FOUND pada AdX Traffic Account
"""

import os
import sys
import django
from datetime import datetime, timedelta
import traceback

# Add the project directory to Python path
sys.path.insert(0, '/Users/ariefdwicahyoadi/hris')

# Set Django settings
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'hris.settings')
django.setup()

from management.googleads_patch_v2 import apply_googleads_patches
from management.database import data_mysql

def diagnose_report_not_found():
    """
    Diagnosa error REPORT_NOT_FOUND pada AdX Traffic Account
    """
    print("🔍 Diagnosa Error REPORT_NOT_FOUND")
    print("=" * 50)
    
    # Apply patches first
    print("\n1. Menerapkan GoogleAds patches...")
    try:
        apply_googleads_patches()
        print("✓ Semua patches berhasil diterapkan")
    except Exception as e:
        print(f"✗ Error menerapkan patches: {e}")
        return
    
    # Check users in database
    print("\n2. Memeriksa pengguna dalam database...")
    try:
        db = data_mysql()
        users_result = db.data_user_by_params()
        
        if users_result.get('status'):
            users = users_result.get('data', [])
            print(f"✓ Ditemukan {len(users)} pengguna dalam database")
            
            for user in users[:3]:  # Show first 3 users
                email = user.get('user_mail', 'Unknown')
                name = user.get('user_alias', 'Unknown')
                print(f"   - {name} ({email})")
                
            if len(users) > 3:
                print(f"   ... dan {len(users) - 3} pengguna lainnya")
        else:
            print(f"✗ Gagal mengambil data pengguna: {users_result.get('error')}")
            return
    except Exception as e:
        print(f"✗ Error memeriksa pengguna: {e}")
        return
    
    # Test with first user
    if users:
        test_user = users[0]
        user_email = test_user.get('user_mail')
        user_name = test_user.get('user_alias')
        
        print(f"\n3. Testing dengan pengguna: {user_name} ({user_email})")
        
        # Test credentials
        print("\n   a. Memeriksa kredensial pengguna...")
        try:
            from management.utils import get_user_adx_credentials, get_user_ad_manager_client
            
            creds_result = get_user_adx_credentials(user_email)
            if creds_result.get('status'):
                print("   ✓ Kredensial AdX berhasil diperoleh")
            else:
                print(f"   ✗ Gagal mendapatkan kredensial AdX: {creds_result.get('error')}")
                return
            
            client_result = get_user_ad_manager_client(user_email)
            if client_result.get('status'):
                print("   ✓ Ad Manager client berhasil dibuat")
                client = client_result['client']
            else:
                print(f"   ✗ Gagal membuat Ad Manager client: {client_result.get('error')}")
                return
                
        except Exception as e:
            print(f"   ✗ Error memeriksa kredensial: {e}")
            return
        
        # Test network access
        print("\n   b. Memeriksa akses network...")
        try:
            network_service = client.GetService('NetworkService', version='v202408')
            network = network_service.getCurrentNetwork()
            print(f"   ✓ Terhubung ke network: {network['displayName']} ({network['networkCode']})")
        except Exception as e:
            print(f"   ✗ Error mengakses network: {e}")
            return
        
        # Test report service
        print("\n   c. Memeriksa Report Service...")
        try:
            report_service = client.GetService('ReportService', version='v202408')
            print("   ✓ Report Service berhasil diinisialisasi")
        except Exception as e:
            print(f"   ✗ Error inisialisasi Report Service: {e}")
            return
        
        # Test simple report creation
        print("\n   d. Testing pembuatan laporan sederhana...")
        try:
            # Use recent date range (last 7 days)
            end_date = datetime.now().date()
            start_date = end_date - timedelta(days=7)
            
            print(f"      Rentang tanggal: {start_date} sampai {end_date}")
            
            # Try the simplest possible report first
            simple_report_query = {
                'reportQuery': {
                    'dimensions': ['DATE'],
                    'columns': ['AD_EXCHANGE_IMPRESSIONS'],
                    'dateRangeType': 'CUSTOM_DATE',
                    'startDate': {
                        'year': start_date.year,
                        'month': start_date.month,
                        'day': start_date.day
                    },
                    'endDate': {
                        'year': end_date.year,
                        'month': end_date.month,
                        'day': end_date.day
                    }
                }
            }
            
            print("      Mencoba laporan sederhana (DATE + AD_EXCHANGE_IMPRESSIONS)...")
            report_job = report_service.runReportJob(simple_report_query)
            
            if report_job and 'id' in report_job:
                report_job_id = report_job['id']
                print(f"   ✓ Laporan berhasil dibuat dengan ID: {report_job_id}")
                
                # Check report status
                print("      Memeriksa status laporan...")
                import time
                max_wait = 60  # 1 minute
                elapsed = 0
                
                while elapsed < max_wait:
                    try:
                        status = report_service.getReportJobStatus(report_job_id)
                        print(f"      Status laporan: {status}")
                        
                        if status == 'COMPLETED':
                            print("   ✓ Laporan berhasil diselesaikan")
                            
                            # Try to download
                            try:
                                downloader = client.GetDataDownloader(version='v202408')
                                report_data = downloader.DownloadReportToString(report_job_id, 'CSV_DUMP')
                                
                                if report_data:
                                    lines = report_data.strip().split('\n')
                                    print(f"   ✓ Data laporan berhasil diunduh ({len(lines)} baris)")
                                    
                                    # Show first few lines
                                    for i, line in enumerate(lines[:5]):
                                        print(f"      Baris {i+1}: {line[:100]}..." if len(line) > 100 else f"      Baris {i+1}: {line}")
                                else:
                                    print("   ⚠ Data laporan kosong")
                                    
                            except Exception as download_error:
                                print(f"   ✗ Error mengunduh laporan: {download_error}")
                            
                            break
                            
                        elif status == 'FAILED':
                            print("   ✗ Laporan gagal")
                            break
                        else:
                            print(f"      Menunggu... ({elapsed}s/{max_wait}s)")
                            time.sleep(5)
                            elapsed += 5
                            
                    except Exception as status_error:
                        print(f"   ✗ Error memeriksa status laporan: {status_error}")
                        break
                
                if elapsed >= max_wait:
                    print("   ⚠ Timeout menunggu laporan selesai")
                    
            else:
                print("   ✗ Gagal membuat laporan - tidak ada ID yang dikembalikan")
                
        except Exception as e:
            error_msg = str(e)
            print(f"   ✗ Error membuat laporan: {error_msg}")
            
            if 'REPORT_NOT_FOUND' in error_msg:
                print("\n   🔍 ANALISIS ERROR REPORT_NOT_FOUND:")
                print("   - Error ini biasanya terjadi karena:")
                print("     1. Network tidak memiliki data AdX untuk periode yang diminta")
                print("     2. Akun tidak memiliki akses ke AdX reporting")
                print("     3. Konfigurasi AdX belum diaktifkan untuk network ini")
                print("     4. Periode tanggal yang diminta tidak valid")
                
            elif 'NOT_NULL' in error_msg:
                print("\n   🔍 ANALISIS ERROR NOT_NULL:")
                print("   - Beberapa kolom memerlukan data yang tidak null")
                print("   - Coba dengan kombinasi kolom yang berbeda")
                
            print("\n   📋 Detail error:")
            traceback.print_exc()
            
    print("\n📊 RINGKASAN DIAGNOSA:")
    print("1. OAuth dan autentikasi: ✓ Berfungsi")
    print("2. Database dan pengguna: ✓ Tersedia")
    print("3. Ad Manager client: ✓ Dapat dibuat")
    print("4. Network access: ✓ Dapat mengakses")
    print("5. Report Service: ✓ Dapat diinisialisasi")
    print("6. Report creation: ❓ Perlu diperiksa lebih lanjut")
    
    print("\n💡 REKOMENDASI:")
    print("1. Pastikan akun Ad Manager memiliki akses AdX")
    print("2. Verifikasi bahwa network memiliki data AdX")
    print("3. Coba dengan rentang tanggal yang berbeda")
    print("4. Periksa konfigurasi AdX di Google Ad Manager")
    print("5. Hubungi administrator Google Ad Manager jika masalah berlanjut")

if __name__ == '__main__':
    diagnose_report_not_found()
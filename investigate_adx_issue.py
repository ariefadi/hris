#!/usr/bin/env python
import os
import sys
import django
from datetime import datetime, timedelta

# Setup Django environment
sys.path.append('/Users/ariefdwicahyoadi/hris')
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'hris.settings')
django.setup()

# Import after Django setup
from management.utils import (
    fetch_adx_traffic_account_by_user, 
    get_user_adx_credentials,
    get_user_ad_manager_client,
    create_dynamic_googleads_yaml
)
from management.googleads_patch_v2 import apply_googleads_patches
from management.database import data_mysql
from googleads import ad_manager
import traceback

def investigate_adx_detailed():
    print("=== Investigasi Mendalam Masalah AdX Data ===")
    
    # Apply patches
    print("\n1. Menerapkan patches...")
    try:
        apply_googleads_patches()
        print("✓ Patches berhasil diterapkan")
    except Exception as e:
        print(f"✗ Error applying patches: {e}")
        return
    
    # Get user email
    print("\n2. Mendapatkan data user...")
    try:
        users_result = data_mysql().data_user_by_params()
        if not users_result['status'] or not users_result['data']:
            print("✗ Tidak ada user ditemukan")
            return
        
        test_user = None
        for user in users_result['data']:
            if user.get('user_mail'):
                test_user = user
                break
        
        if not test_user:
            print("✗ Tidak ada user dengan email")
            return
        
        user_email = test_user['user_mail']
        print(f"✓ Menggunakan email: {user_email}")
        
    except Exception as e:
        print(f"✗ Error mendapatkan user: {e}")
        return
    
    # Check credentials in detail
    print("\n3. Memeriksa kredensial AdX secara detail...")
    try:
        creds_result = get_user_adx_credentials(user_email)
        if creds_result['status']:
            creds = creds_result['credentials']
            print(f"✓ Kredensial ditemukan:")
            print(f"   - Client ID: {creds['client_id'][:10]}...")
            print(f"   - Network Code: {creds['network_code']}")
            print(f"   - Developer Token: {creds['developer_token'][:10]}...")
            print(f"   - Email: {creds['email']}")
        else:
            print(f"✗ Error kredensial: {creds_result['error']}")
            return
    except Exception as e:
        print(f"✗ Error checking credentials: {e}")
        return
    
    # Test direct Ad Manager client
    print("\n4. Menguji koneksi Ad Manager langsung...")
    try:
        client_result = get_user_ad_manager_client(user_email)
        if client_result['status']:
            client = client_result['client']
            
            # Test network access
            network_service = client.GetService('NetworkService')
            network = network_service.getCurrentNetwork()
            print(f"✓ Terhubung ke jaringan: {network['displayName']} ({network['networkCode']})")
            
            # Check available services
            print("\n5. Memeriksa layanan yang tersedia...")
            try:
                report_service = client.GetService('ReportService')
                print("✓ ReportService tersedia")
                
                inventory_service = client.GetService('InventoryService')
                print("✓ InventoryService tersedia")
                
                # Test if we can access AdX specific data
                print("\n6. Menguji akses data AdX...")
                
                # Try to get ad units to see if there's any inventory
                statement = ad_manager.StatementBuilder(version='v202408')
                statement.Where('status = :status')
                statement.WithBindVariable('status', 'ACTIVE')
                statement.limit = 10
                
                ad_units = inventory_service.getAdUnitsByStatement(statement.ToStatement())
                if ad_units and 'results' in ad_units:
                    print(f"✓ Ditemukan {len(ad_units['results'])} ad units aktif")
                    for i, unit in enumerate(ad_units['results'][:3]):
                        print(f"   - {unit['name']} (ID: {unit['id']})")
                else:
                    print("⚠ Tidak ada ad units aktif ditemukan")
                
            except Exception as e:
                print(f"✗ Error mengakses services: {e}")
                print(f"Traceback: {traceback.format_exc()}")
        else:
            print(f"✗ Error client: {client_result['error']}")
            return
    except Exception as e:
        print(f"✗ Error testing client: {e}")
        return
    
    # Test different report configurations
    print("\n7. Menguji konfigurasi laporan yang berbeda...")
    
    # Test with different dimensions and columns
    test_configs = [
        {
            'name': 'Basic AdX Report',
            'dimensions': ['DATE'],
            'columns': ['AD_EXCHANGE_IMPRESSIONS']
        },
        {
            'name': 'AdX with Site Name',
            'dimensions': ['DATE', 'AD_EXCHANGE_SITE_NAME'],
            'columns': ['AD_EXCHANGE_IMPRESSIONS', 'AD_EXCHANGE_CLICKS']
        },
        {
            'name': 'Standard Ad Manager Report',
            'dimensions': ['DATE'],
            'columns': ['TOTAL_IMPRESSIONS', 'TOTAL_CLICKS']
        }
    ]
    
    end_date = datetime.now().date()
    start_date = end_date - timedelta(days=30)
    
    for config in test_configs:
        print(f"\n   Testing: {config['name']}")
        try:
            client_result = get_user_ad_manager_client(user_email)
            if not client_result['status']:
                continue
                
            client = client_result['client']
            report_service = client.GetService('ReportService')
            
            report_query = {
                'reportQuery': {
                    'dimensions': config['dimensions'],
                    'columns': config['columns'],
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
            
            report_job = report_service.runReportJob(report_query)
            print(f"   ✓ Report job berhasil dibuat: {report_job.get('id', 'Unknown')}")
            
        except Exception as e:
            print(f"   ✗ Error: {str(e)}")
            if 'PERMISSION_DENIED' in str(e):
                print(f"   → Kemungkinan masalah permission untuk {config['name']}")
            elif 'INVALID_VALUE' in str(e):
                print(f"   → Kemungkinan kolom/dimensi tidak tersedia")
    
    print("\n=== Kesimpulan Investigasi ===")
    print("Berdasarkan hasil investigasi di atas:")
    print("1. Jika semua koneksi berhasil tetapi tidak ada ad units → Tidak ada inventory yang dikonfigurasi")
    print("2. Jika ada error permission pada AdX reports → Akun belum memiliki akses AdX")
    print("3. Jika standard reports berhasil tapi AdX gagal → AdX belum diaktifkan")
    print("4. Jika semua berhasil tapi data kosong → Memang tidak ada traffic AdX")
    print("\nLangkah selanjutnya:")
    print("- Periksa Google Ad Manager Console untuk memastikan AdX sudah aktif")
    print("- Verifikasi ad units sudah dikonfigurasi untuk AdX")
    print("- Pastikan ada traffic yang masuk ke situs")

if __name__ == "__main__":
    investigate_adx_detailed()
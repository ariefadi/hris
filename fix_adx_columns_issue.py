#!/usr/bin/env python
import os
import sys
import django
from datetime import datetime, timedelta

# Setup Django environment
sys.path.append('/Users/ariefdwicahyoadi/hris')
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'hris.settings')
django.setup()

# Apply patches first
from management.googleads_patch_v2 import apply_googleads_patches
apply_googleads_patches()
print("[PATCH] GoogleAds patches applied")

# Import after Django setup and patches
from management.database import data_mysql
from googleads import ad_manager
import tempfile
import yaml
import time

def fix_adx_columns_issue():
    print("=== Memperbaiki Masalah Kolom AdX Report ===")
    
    # Get user credentials
    print("\n1. Mengambil kredensial...")
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
        print(f"✓ Email: {user_email}")
        
        # Get detailed credentials
        db = data_mysql()
        sql = """
            SELECT client_id, client_secret, refresh_token, network_code, developer_token
            FROM app_users 
            WHERE user_mail = %s
        """
        
        db.cur_hris.execute(sql, (user_email,))
        user_data = db.cur_hris.fetchone()
        
        if not user_data:
            print(f"✗ Tidak ada data kredensial untuk {user_email}")
            return
        
        client_id = user_data.get('client_id')
        client_secret = user_data.get('client_secret')
        refresh_token = user_data.get('refresh_token')
        network_code = user_data.get('network_code')
        developer_token = user_data.get('developer_token')
        
        print(f"✓ Kredensial ditemukan")
        
    except Exception as e:
        print(f"✗ Error mengambil kredensial: {e}")
        return
    
    # Create YAML configuration
    print("\n2. Membuat konfigurasi Ad Manager...")
    try:
        yaml_content = {
            'ad_manager': {
                'application_name': 'HRIS AdX Integration',
                'developer_token': developer_token,
                'client_id': client_id,
                'client_secret': client_secret,
                'refresh_token': refresh_token,
                'network_code': network_code
            }
        }
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as temp_file:
            yaml.dump(yaml_content, temp_file, default_flow_style=False)
            yaml_path = temp_file.name
        
        print(f"✓ YAML config dibuat")
        
    except Exception as e:
        print(f"✗ Error membuat YAML: {e}")
        return
    
    # Test Ad Manager client initialization
    print("\n3. Menginisialisasi Ad Manager client...")
    try:
        client = ad_manager.AdManagerClient.LoadFromStorage(yaml_path)
        print("✓ Ad Manager client berhasil dibuat")
        
    except Exception as e:
        print(f"✗ Error membuat client: {e}")
        os.unlink(yaml_path)
        return
    
    # Test different AdX column configurations
    print("\n4. Menguji berbagai konfigurasi kolom AdX...")
    
    # List of valid AdX column combinations to test
    column_configurations = [
        # Basic AdX columns
        {
            'name': 'Basic AdX Impressions',
            'dimensions': ['DATE'],
            'columns': ['AD_EXCHANGE_IMPRESSIONS']
        },
        {
            'name': 'AdX Revenue',
            'dimensions': ['DATE'],
            'columns': ['AD_EXCHANGE_TOTAL_EARNINGS']
        },
        {
            'name': 'AdX Clicks',
            'dimensions': ['DATE'],
            'columns': ['AD_EXCHANGE_CLICKS']
        },
        # Combined AdX metrics
        {
            'name': 'AdX Basic Metrics',
            'dimensions': ['DATE'],
            'columns': ['AD_EXCHANGE_IMPRESSIONS', 'AD_EXCHANGE_CLICKS', 'AD_EXCHANGE_TOTAL_EARNINGS']
        },
        # With additional dimensions
        {
            'name': 'AdX by Ad Unit',
            'dimensions': ['DATE', 'AD_UNIT_NAME'],
            'columns': ['AD_EXCHANGE_IMPRESSIONS']
        },
        {
            'name': 'AdX by Country',
            'dimensions': ['DATE', 'COUNTRY_NAME'],
            'columns': ['AD_EXCHANGE_IMPRESSIONS']
        },
        # Alternative column names
        {
            'name': 'Total Impressions',
            'dimensions': ['DATE'],
            'columns': ['TOTAL_IMPRESSIONS']
        },
        {
            'name': 'Total Revenue',
            'dimensions': ['DATE'],
            'columns': ['TOTAL_REVENUE']
        },
        # Network-level metrics
        {
            'name': 'Network Impressions',
            'dimensions': ['DATE'],
            'columns': ['TOTAL_IMPRESSIONS']
        },
        # Simple test without AdX prefix
        {
            'name': 'Basic Impressions',
            'dimensions': ['DATE'],
            'columns': ['IMPRESSIONS']
        }
    ]
    
    successful_configs = []
    
    try:
        report_service = client.GetService('ReportService', version='v202408')
        
        for config in column_configurations:
            print(f"\n   Testing: {config['name']}")
            print(f"   Dimensions: {config['dimensions']}")
            print(f"   Columns: {config['columns']}")
            
            try:
                # Create report job
                report_job = {
                    'reportQuery': {
                        'dimensions': config['dimensions'],
                        'columns': config['columns'],
                        'dateRangeType': 'LAST_7_DAYS'
                    }
                }
                
                # Try to run the report
                result = report_service.runReportJob(report_job)
                
                if result and hasattr(result, 'id'):
                    print(f"   ✓ Report job berhasil dibuat (ID: {result.id})")
                    successful_configs.append(config)
                    
                    # Check report status briefly
                    time.sleep(2)
                    status = report_service.getReportJob(result.id)
                    print(f"   Status: {status.reportJobStatus}")
                    
                    if status.reportJobStatus == 'COMPLETED':
                        print(f"   ✓ Report selesai dengan sukses")
                        
                        # Try to download a small sample
                        try:
                            report_downloader = client.GetDataDownloader(version='v202408')
                            report_data = report_downloader.DownloadReportToString(
                                result.id, 'CSV_DUMP'
                            )
                            
                            if report_data:
                                lines = report_data.strip().split('\n')
                                print(f"   ✓ Data berhasil diunduh ({len(lines)} baris)")
                                
                                if len(lines) > 1:
                                    print(f"   ✓ Ada data: {lines[1][:100]}...")
                                else:
                                    print(f"   ⚠ Tidak ada data untuk periode ini")
                            else:
                                print(f"   ⚠ Download berhasil tapi data kosong")
                                
                        except Exception as download_error:
                            print(f"   ⚠ Error download: {download_error}")
                    
                    elif status.reportJobStatus == 'FAILED':
                        print(f"   ✗ Report gagal")
                    else:
                        print(f"   ⚠ Report masih berjalan")
                        
                else:
                    print(f"   ✗ Gagal membuat report job")
                    
            except Exception as e:
                error_msg = str(e)
                print(f"   ✗ Error: {error_msg}")
                
                # Analyze specific errors
                if "NOT_NULL" in error_msg:
                    print(f"     → Kolom tidak boleh null atau tidak valid")
                elif "PERMISSION_DENIED" in error_msg:
                    print(f"     → Tidak memiliki permission untuk kolom ini")
                elif "INVALID_QUERY" in error_msg:
                    print(f"     → Query tidak valid")
                elif "UNKNOWN_COLUMN" in error_msg:
                    print(f"     → Kolom tidak dikenal")
    
    except Exception as e:
        print(f"✗ Error mengakses ReportService: {e}")
        os.unlink(yaml_path)
        return
    
    # Summary of results
    print(f"\n=== HASIL PENGUJIAN ===")
    
    if successful_configs:
        print(f"\n✓ Konfigurasi yang berhasil ({len(successful_configs)}):\n")
        for config in successful_configs:
            print(f"   • {config['name']}")
            print(f"     Dimensions: {config['dimensions']}")
            print(f"     Columns: {config['columns']}\n")
        
        # Recommend the best configuration
        print("\n🎯 REKOMENDASI:")
        best_config = successful_configs[0]
        print(f"Gunakan konfigurasi '{best_config['name']}' untuk implementasi:")
        print(f"```python")
        print(f"report_job = {{")
        print(f"    'reportQuery': {{")
        print(f"        'dimensions': {best_config['dimensions']},")
        print(f"        'columns': {best_config['columns']},")
        print(f"        'dateRangeType': 'LAST_7_DAYS'")
        print(f"    }}")
        print(f"}}")
        print(f"```")
        
    else:
        print("\n✗ Tidak ada konfigurasi yang berhasil")
        print("\nKemungkinan penyebab:")
        print("1. Akun tidak memiliki akses AdX")
        print("2. AdX tidak diaktifkan untuk network ini")
        print("3. Tidak ada data AdX untuk periode yang diuji")
        print("4. Permission tidak mencukupi")
    
    # Update the utils.py function if we found working config
    if successful_configs:
        print(f"\n5. Memperbarui fungsi di utils.py...")
        try:
            best_config = successful_configs[0]
            
            # Read current utils.py
            utils_path = '/Users/ariefdwicahyoadi/hris/management/utils.py'
            with open(utils_path, 'r') as f:
                utils_content = f.read()
            
            # Find and replace the problematic report configuration
            old_pattern = "'columns': ['AD_EXCHANGE_IMPRESSIONS', 'AD_EXCHANGE_CLICKS', 'AD_EXCHANGE_TOTAL_EARNINGS']"
            new_pattern = f"'columns': {best_config['columns']}"
            
            if old_pattern in utils_content:
                updated_content = utils_content.replace(old_pattern, new_pattern)
                
                with open(utils_path, 'w') as f:
                    f.write(updated_content)
                
                print(f"✓ utils.py berhasil diperbarui dengan konfigurasi yang bekerja")
            else:
                print(f"⚠ Tidak menemukan pola yang tepat di utils.py untuk diperbarui")
                
        except Exception as e:
            print(f"✗ Error memperbarui utils.py: {e}")
    
    # Cleanup
    try:
        os.unlink(yaml_path)
    except:
        pass
    
    print(f"\n=== LANGKAH SELANJUTNYA ===")
    if successful_configs:
        print("1. Konfigurasi kolom yang bekerja telah ditemukan")
        print("2. Coba jalankan kembali fetch data AdX")
        print("3. Periksa apakah data muncul di admin panel")
    else:
        print("1. Periksa akses AdX di Google Ad Manager Console")
        print("2. Verifikasi AdX diaktifkan untuk network Anda")
        print("3. Pastikan ada data AdX untuk periode yang diuji")

if __name__ == "__main__":
    fix_adx_columns_issue()
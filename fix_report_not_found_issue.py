#!/usr/bin/env python3
"""
Script untuk memperbaiki masalah REPORT_NOT_FOUND dengan menghapus patch yang bermasalah
dan mengimplementasikan penanganan error yang lebih baik
"""

import os
import sys
import django
from datetime import datetime, timedelta

# Add the project directory to Python path
sys.path.insert(0, '/Users/ariefdwicahyoadi/hris')

# Set Django settings
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'hris.settings')
django.setup()

def fix_report_not_found_issue():
    """
    Perbaiki masalah REPORT_NOT_FOUND dengan menghapus patch yang bermasalah
    """
    print("🔧 Memperbaiki Masalah REPORT_NOT_FOUND")
    print("=" * 50)
    
    # Read current googleads_patch_v2.py
    patch_path = '/Users/ariefdwicahyoadi/hris/management/googleads_patch_v2.py'
    
    print("\n1. Membaca file patch saat ini...")
    try:
        with open(patch_path, 'r') as f:
            content = f.read()
        print("✓ File patch berhasil dibaca")
    except Exception as e:
        print(f"✗ Error membaca file patch: {e}")
        return
    
    print("\n2. Menghapus patch yang bermasalah...")
    
    # Remove the problematic patches that return fake data
    patches_to_remove = [
        # Remove the fake getReportJobStatus patch
        '''                            def patched_get_report_job_status_method(*args, **kwargs):
                                try:
                                    result = original_get_report_job_status(*args, **kwargs)
                                    print(f"[PATCH] getReportJobStatus successful, status: {result}")
                                    return result
                                except Exception as e:
                                    error_msg = str(e)
                                    print(f"[PATCH] getReportJobStatus unexpected error: {error_msg}")
                                    
                                    # If it's a REPORT_NOT_FOUND error, return COMPLETED to bypass
                                    if 'REPORT_NOT_FOUND' in error_msg:
                                        print(f"[PATCH] getReportJobStatus bypassing REPORT_NOT_FOUND, returning COMPLETED")
                                        return 'COMPLETED'
                                    
                                    # For other errors, re-raise
                                    raise e''',
        
        # Remove the fake runReportJob patch
        '''                                except Exception as e:
                                    error_msg = str(e)
                                    print(f"[PATCH] runReportJob final fallback due to: {error_msg}")
                                    
                                    # Return a fake report job ID to bypass the error
                                    fake_report_job = {'id': '12345'}
                                    print(f"[PATCH] runReportJob successful, got report job: {fake_report_job['id']}")
                                    return fake_report_job''',
        
        # Remove the fake DownloadReportToString patch
        '''                        # Add DownloadReportToString method if missing
                        if not hasattr(downloader, 'DownloadReportToString'):
                            def patched_download_report_to_string(report_job_id, export_format):
                                print(f"[PATCH] Adding missing DownloadReportToString method, returning empty CSV")
                                # Return minimal CSV structure
                                csv_data = "Dimension.DATE,Dimension.AD_EXCHANGE_SITE_NAME,Column.AD_EXCHANGE_IMPRESSIONS,Column.AD_EXCHANGE_CLICKS,Column.AD_EXCHANGE_TOTAL_EARNINGS\\n"
                                print(f"[PATCH] DownloadReportToString successful, got {len(csv_data)} characters")
                                return csv_data
                            
                            downloader.DownloadReportToString = patched_download_report_to_string'''
    ]
    
    updated_content = content
    removed_count = 0
    
    for patch_text in patches_to_remove:
        if patch_text in updated_content:
            updated_content = updated_content.replace(patch_text, '')
            removed_count += 1
            print(f"   ✓ Menghapus patch bermasalah #{removed_count}")
    
    print(f"\n3. Menambahkan penanganan error yang lebih baik...")
    
    # Add better error handling for runReportJob
    better_run_report_job_patch = '''                                except Exception as e:
                                    error_msg = str(e)
                                    print(f"[PATCH] runReportJob error: {error_msg}")
                                    
                                    # Don't return fake data, let the error propagate properly
                                    # but provide more context
                                    if 'NOT_NULL' in error_msg:
                                        print(f"[PATCH] NOT_NULL error detected - this column combination is not supported")
                                    elif 'REPORT_NOT_FOUND' in error_msg:
                                        print(f"[PATCH] REPORT_NOT_FOUND error - network may not have AdX data")
                                    elif 'PERMISSION' in error_msg.upper():
                                        print(f"[PATCH] Permission error - check AdX access rights")
                                    
                                    # Re-raise the original error
                                    raise e'''
    
    # Add better error handling for getReportJobStatus
    better_get_status_patch = '''                            def patched_get_report_job_status_method(*args, **kwargs):
                                try:
                                    result = original_get_report_job_status(*args, **kwargs)
                                    print(f"[PATCH] getReportJobStatus successful, status: {result}")
                                    return result
                                except Exception as e:
                                    error_msg = str(e)
                                    print(f"[PATCH] getReportJobStatus error: {error_msg}")
                                    
                                    # Provide context but don't fake the response
                                    if 'REPORT_NOT_FOUND' in error_msg:
                                        print(f"[PATCH] Report ID not found - this may indicate the report was not created successfully")
                                    
                                    # Re-raise the original error
                                    raise e'''
    
    # Replace the problematic patches with better ones
    # Find and replace the runReportJob method
    import re
    
    # Pattern to find the runReportJob method
    run_report_pattern = r'def patched_run_report_job_method\(\*args, \*\*kwargs\):.*?except Exception as e:.*?raise e'
    
    if re.search(run_report_pattern, updated_content, re.DOTALL):
        # Replace with better error handling
        updated_content = re.sub(
            r'(def patched_run_report_job_method\(\*args, \*\*kwargs\):.*?except Exception as e:).*?(\s+service\.runReportJob = patched_run_report_job_method)',
            r'\1' + better_run_report_job_patch + r'\2',
            updated_content,
            flags=re.DOTALL
        )
        print("   ✓ Mengganti patch runReportJob dengan versi yang lebih baik")
    
    # Pattern to find the getReportJobStatus method
    get_status_pattern = r'def patched_get_report_job_status_method\(\*args, \*\*kwargs\):.*?service\.getReportJobStatus = patched_get_report_job_status_method'
    
    if re.search(get_status_pattern, updated_content, re.DOTALL):
        # Replace with better error handling
        updated_content = re.sub(
            get_status_pattern,
            better_get_status_patch + '\n\n                            service.getReportJobStatus = patched_get_report_job_status_method',
            updated_content,
            flags=re.DOTALL
        )
        print("   ✓ Mengganti patch getReportJobStatus dengan versi yang lebih baik")
    
    print("\n4. Menambahkan penanganan khusus untuk AdX...")
    
    # Add specific AdX error handling to utils.py
    utils_path = '/Users/ariefdwicahyoadi/hris/management/utils.py'
    
    try:
        with open(utils_path, 'r') as f:
            utils_content = f.read()
        
        # Add better error handling in _run_adx_report function
        adx_error_handling = '''            # If all combinations failed, provide specific error messages
            if last_error:
                error_msg = str(last_error)
                if 'REPORT_NOT_FOUND' in error_msg:
                    raise Exception("Network tidak memiliki data AdX untuk periode yang diminta. Pastikan: 1) Akun memiliki akses AdX, 2) Network memiliki traffic AdX, 3) Periode tanggal valid")
                elif 'NOT_NULL' in error_msg:
                    raise Exception("Semua kombinasi kolom gagal karena constraint NOT_NULL. Network mungkin tidak memiliki data AdX yang lengkap")
                elif 'PERMISSION' in error_msg.upper():
                    raise Exception("Tidak memiliki izin untuk mengakses data AdX. Hubungi administrator untuk memberikan akses AdX")
                else:
                    raise last_error
            else:
                raise Exception("Semua kombinasi kolom gagal tanpa error spesifik")'''
        
        # Replace the generic error handling
        if 'raise Exception("All column combinations failed")' in utils_content:
            utils_content = utils_content.replace(
                'raise Exception("All column combinations failed")',
                adx_error_handling
            )
            print("   ✓ Menambahkan penanganan error khusus AdX ke utils.py")
            
            # Write updated utils.py
            with open(utils_path, 'w') as f:
                f.write(utils_content)
        
    except Exception as e:
        print(f"   ⚠ Warning: Tidak dapat memperbarui utils.py: {e}")
    
    print("\n5. Menyimpan file patch yang telah diperbaiki...")
    try:
        with open(patch_path, 'w') as f:
            f.write(updated_content)
        print("✓ File patch berhasil diperbarui")
    except Exception as e:
        print(f"✗ Error menyimpan file patch: {e}")
        return
    
    print("\n✅ Perbaikan REPORT_NOT_FOUND Berhasil Diterapkan!")
    print("\n📋 Yang telah diperbaiki:")
    print("1. Menghapus patch yang mengembalikan data palsu")
    print("2. Menambahkan penanganan error yang lebih informatif")
    print("3. Memberikan pesan error yang spesifik untuk masalah AdX")
    print("4. Mempertahankan error asli untuk debugging yang lebih baik")
    
    print("\n🚀 Langkah selanjutnya:")
    print("1. Restart server Django")
    print("2. Test menu AdX Traffic Account")
    print("3. Periksa pesan error yang lebih informatif")
    print("4. Verifikasi akses AdX di Google Ad Manager")
    
    print("\n💡 Kemungkinan penyebab REPORT_NOT_FOUND:")
    print("1. Network tidak memiliki data AdX")
    print("2. Akun tidak memiliki akses AdX")
    print("3. Konfigurasi AdX belum diaktifkan")
    print("4. Periode tanggal tidak memiliki data")

if __name__ == '__main__':
    fix_report_not_found_issue()
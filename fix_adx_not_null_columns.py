#!/usr/bin/env python3
"""
Script to fix AdX NOT_NULL columns error by implementing safer column combinations
and better error handling in the report generation.
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

from management.googleads_patch_v2 import apply_googleads_patches

def fix_adx_not_null_error():
    """
    Fix the NOT_NULL @ columns error by updating the report query configuration
    """
    print("🔧 Fixing AdX NOT_NULL Columns Error")
    print("=" * 50)
    
    # Apply patches first
    print("\n1. Applying GoogleAds patches...")
    try:
        apply_googleads_patches()
        print("✓ All patches applied successfully")
    except Exception as e:
        print(f"✗ Error applying patches: {e}")
        return
    
    # Read current utils.py
    utils_path = '/Users/ariefdwicahyoadi/hris/management/utils.py'
    
    print("\n2. Reading current utils.py...")
    try:
        with open(utils_path, 'r') as f:
            content = f.read()
        print("✓ utils.py loaded successfully")
    except Exception as e:
        print(f"✗ Error reading utils.py: {e}")
        return
    
    # Find and replace the problematic report query
    print("\n3. Updating _run_adx_report function...")
    
    # Original problematic query
    old_query = '''    report_query = {
        'reportQuery': {
            'dimensions': ['DATE', 'AD_EXCHANGE_SITE_NAME'],
            'columns': ['AD_EXCHANGE_IMPRESSIONS', 'AD_EXCHANGE_CLICKS', 'AD_EXCHANGE_TOTAL_EARNINGS'],
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
    }'''
    
    # New safer query with fallback options
    new_query = '''    # Try different column combinations to avoid NOT_NULL errors
    column_combinations = [
        # Primary: Full metrics (most comprehensive)
        ['AD_EXCHANGE_IMPRESSIONS', 'AD_EXCHANGE_CLICKS', 'AD_EXCHANGE_TOTAL_EARNINGS'],
        # Fallback 1: Basic metrics only
        ['AD_EXCHANGE_IMPRESSIONS', 'AD_EXCHANGE_TOTAL_EARNINGS'],
        # Fallback 2: Impressions only
        ['AD_EXCHANGE_IMPRESSIONS'],
        # Fallback 3: Revenue only
        ['AD_EXCHANGE_TOTAL_EARNINGS']
    ]
    
    report_job = None
    last_error = None
    
    for i, columns in enumerate(column_combinations):
        try:
            print(f"[DEBUG] Trying column combination {i+1}: {columns}")
            
            report_query = {
                'reportQuery': {
                    'dimensions': ['DATE', 'AD_EXCHANGE_SITE_NAME'],
                    'columns': columns,
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
            
            if site_filter:
                report_query['reportQuery']['dimensionFilters'] = [{
                    'dimension': 'AD_EXCHANGE_SITE_NAME',
                    'operator': 'CONTAINS',
                    'values': [site_filter]
                }]
            
            # Try to run the report job
            report_job = report_service.runReportJob(report_query)
            print(f"[DEBUG] Successfully created report job with columns: {columns}")
            break
            
        except Exception as e:
            last_error = e
            error_msg = str(e)
            print(f"[DEBUG] Column combination {i+1} failed: {error_msg}")
            
            # If this is a NOT_NULL error, try the next combination
            if 'NOT_NULL' in error_msg:
                continue
            # If it's an authentication error, re-raise it
            elif 'authentication' in error_msg.lower() or 'permission' in error_msg.lower():
                raise e
            # For other errors, try next combination
            else:
                continue
    
    # If all combinations failed, raise the last error
    if report_job is None:
        if last_error:
            raise last_error
        else:
            raise Exception("All column combinations failed")'''
    
    # Replace the old query with the new one
    if old_query in content:
        updated_content = content.replace(old_query, new_query)
        print("✓ Found and updated report query configuration")
    else:
        print("⚠ Could not find exact match for report query, trying alternative approach...")
        
        # Alternative: Look for the function and replace it entirely
        import re
        
        # Pattern to match the entire _run_adx_report function
        pattern = r'def _run_adx_report\(client, start_date, end_date, site_filter\):[\s\S]*?return downloader\.DownloadReportToString\(report_job_id, \'CSV_DUMP\'\)'
        
        new_function = '''def _run_adx_report(client, start_date, end_date, site_filter):
    report_service = client.GetService('ReportService', version='v202408')

    # Try different column combinations to avoid NOT_NULL errors
    column_combinations = [
        # Primary: Full metrics (most comprehensive)
        ['AD_EXCHANGE_IMPRESSIONS', 'AD_EXCHANGE_CLICKS', 'AD_EXCHANGE_TOTAL_EARNINGS'],
        # Fallback 1: Basic metrics only
        ['AD_EXCHANGE_IMPRESSIONS', 'AD_EXCHANGE_TOTAL_EARNINGS'],
        # Fallback 2: Impressions only
        ['AD_EXCHANGE_IMPRESSIONS'],
        # Fallback 3: Revenue only
        ['AD_EXCHANGE_TOTAL_EARNINGS']
    ]
    
    report_job = None
    last_error = None
    
    for i, columns in enumerate(column_combinations):
        try:
            print(f"[DEBUG] Trying column combination {i+1}: {columns}")
            
            report_query = {
                'reportQuery': {
                    'dimensions': ['DATE', 'AD_EXCHANGE_SITE_NAME'],
                    'columns': columns,
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
            
            if site_filter:
                report_query['reportQuery']['dimensionFilters'] = [{
                    'dimension': 'AD_EXCHANGE_SITE_NAME',
                    'operator': 'CONTAINS',
                    'values': [site_filter]
                }]
            
            # Try to run the report job
            report_job = report_service.runReportJob(report_query)
            print(f"[DEBUG] Successfully created report job with columns: {columns}")
            break
            
        except Exception as e:
            last_error = e
            error_msg = str(e)
            print(f"[DEBUG] Column combination {i+1} failed: {error_msg}")
            
            # If this is a NOT_NULL error, try the next combination
            if 'NOT_NULL' in error_msg:
                continue
            # If it's an authentication error, re-raise it
            elif 'authentication' in error_msg.lower() or 'permission' in error_msg.lower():
                raise e
            # For other errors, try next combination
            else:
                continue
    
    # If all combinations failed, raise the last error
    if report_job is None:
        if last_error:
            raise last_error
        else:
            raise Exception("All column combinations failed")
    
    report_job_id = report_job['id']
    
    # Ensure report_job_id is an integer for API calls
    if isinstance(report_job_id, str):
        try:
            report_job_id = int(report_job_id)
        except ValueError:
            print(f"[DEBUG] Warning: Could not convert report_job_id '{report_job_id}' to integer")
    
    print(f"[DEBUG] Waiting for report job {report_job_id} (type: {type(report_job_id)})")
    elapsed = 0
    while elapsed < 300:
        status = report_service.getReportJobStatus(report_job_id)
        print(f"[DEBUG] Report status: {status}")
        if status == 'COMPLETED':
            break
        elif status == 'FAILED':
            raise Exception("Report job failed")
        time.sleep(10)
        elapsed += 10

    if elapsed >= 300:
        raise Exception("Report job timed out")

    downloader = client.GetDataDownloader(version='v202408')
    return downloader.DownloadReportToString(report_job_id, 'CSV_DUMP')'''
        
        match = re.search(pattern, content)
        if match:
            updated_content = content.replace(match.group(0), new_function)
            print("✓ Found and replaced entire _run_adx_report function")
        else:
            print("✗ Could not find _run_adx_report function to replace")
            return
    
    # Write the updated content back to the file
    print("\n4. Writing updated utils.py...")
    try:
        with open(utils_path, 'w') as f:
            f.write(updated_content)
        print("✓ utils.py successfully updated")
    except Exception as e:
        print(f"✗ Error writing utils.py: {e}")
        return
    
    print("\n✅ AdX NOT_NULL Columns Error Fix Applied Successfully!")
    print("\n📋 What was fixed:")
    print("1. Added fallback column combinations to handle NOT_NULL errors")
    print("2. Implemented progressive fallback from full metrics to basic metrics")
    print("3. Added proper error handling for different error types")
    print("4. Maintained authentication error propagation")
    
    print("\n🚀 Next Steps:")
    print("1. Test the AdX Traffic Account page")
    print("2. Check if data loads without NOT_NULL errors")
    print("3. Verify that fallback columns work correctly")
    
    print("\n📊 Column Fallback Strategy:")
    print("1. Primary: Impressions + Clicks + Revenue (most comprehensive)")
    print("2. Fallback 1: Impressions + Revenue (basic metrics)")
    print("3. Fallback 2: Impressions only (minimal data)")
    print("4. Fallback 3: Revenue only (revenue focus)")

if __name__ == '__main__':
    fix_adx_not_null_error()
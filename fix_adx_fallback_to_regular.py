#!/usr/bin/env python3
"""
Fix AdX traffic account to fallback to regular Ad Manager metrics when AdX is not available
"""

import os
import sys
import re

def fix_adx_fallback():
    print("🔧 Fixing AdX Traffic Account to use regular Ad Manager metrics")
    print("=" * 60)
    
    # Path to utils.py
    utils_path = '/Users/ariefdwicahyoadi/hris/management/utils.py'
    
    print("\n1. Reading current utils.py...")
    try:
        with open(utils_path, 'r') as f:
            content = f.read()
        print("✓ Successfully read utils.py")
    except Exception as e:
        print(f"✗ Error reading utils.py: {e}")
        return
    
    print("\n2. Updating fetch_adx_traffic_account_by_user function...")
    
    # Find and replace the fetch_adx_traffic_account_by_user function
    old_function_pattern = r'def fetch_adx_traffic_account_by_user\(user_email, start_date, end_date, site_filter=None\):.*?return \{.*?\}'
    
    new_function = '''def fetch_adx_traffic_account_by_user(user_email, start_date, end_date, site_filter=None):
    """Fetch traffic account data using user's credentials with AdX fallback to regular metrics"""
    try:
        # Get user's Ad Manager client
        client_result = get_user_ad_manager_client(user_email)
        if not client_result['status']:
            return client_result
            
        client = client_result['client']
        
        # Convert string dates to datetime.date objects
        if isinstance(start_date, str):
            start_date = datetime.strptime(start_date, '%Y-%m-%d').date()
        if isinstance(end_date, str):
            end_date = datetime.strptime(end_date, '%Y-%m-%d').date()
        
        # Try AdX first, then fallback to regular metrics
        try:
            print(f"[DEBUG] Attempting AdX report for {user_email}")
            return _run_adx_report_with_fallback(client, start_date, end_date, site_filter)
        except Exception as adx_error:
            print(f"[DEBUG] AdX failed: {adx_error}")
            print(f"[DEBUG] Falling back to regular Ad Manager metrics")
            return _run_regular_report(client, start_date, end_date, site_filter)
            
    except Exception as e:
        print(f"[ERROR] fetch_adx_traffic_account_by_user failed: {e}")
        return {
            'status': False,
            'error': f'Failed to fetch traffic data: {str(e)}'
        }'''
    
    # Replace the function
    if re.search(r'def fetch_adx_traffic_account_by_user\(', content):
        # Find the complete function
        lines = content.split('\n')
        start_idx = None
        end_idx = None
        indent_level = None
        
        for i, line in enumerate(lines):
            if 'def fetch_adx_traffic_account_by_user(' in line:
                start_idx = i
                indent_level = len(line) - len(line.lstrip())
                break
        
        if start_idx is not None:
            # Find the end of the function
            for i in range(start_idx + 1, len(lines)):
                line = lines[i]
                if line.strip() == '':
                    continue
                current_indent = len(line) - len(line.lstrip())
                if current_indent <= indent_level and line.strip() and not line.strip().startswith('#'):
                    end_idx = i
                    break
            
            if end_idx is None:
                end_idx = len(lines)
            
            # Replace the function
            new_lines = lines[:start_idx] + [new_function] + lines[end_idx:]
            content = '\n'.join(new_lines)
            print("✓ Updated fetch_adx_traffic_account_by_user function")
    
    print("\n3. Adding new helper functions...")
    
    # Add the new helper functions
    helper_functions = '''

def _run_adx_report_with_fallback(client, start_date, end_date, site_filter):
    """Try AdX report with fallback to regular metrics"""
    report_service = client.GetService('ReportService', version='v202408')

    # Try AdX columns first
    adx_column_combinations = [
        ['AD_EXCHANGE_IMPRESSIONS'],
        ['AD_EXCHANGE_TOTAL_EARNINGS'],
        ['AD_EXCHANGE_IMPRESSIONS', 'AD_EXCHANGE_TOTAL_EARNINGS']
    ]
    
    for columns in adx_column_combinations:
        try:
            print(f"[DEBUG] Trying AdX columns: {columns}")
            
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
            
            # Add site filter if specified
            if site_filter:
                report_query['reportQuery']['dimensionFilters'] = [{
                    'dimension': 'AD_EXCHANGE_SITE_NAME',
                    'operator': 'CONTAINS',
                    'values': [site_filter]
                }]
            
            # Try to run the report job
            report_job = report_service.runReportJob(report_query)
            print(f"[DEBUG] AdX report created successfully with columns: {columns}")
            
            # Wait for completion and download
            return _wait_and_download_report(client, report_job['id'])
            
        except Exception as e:
            error_msg = str(e)
            print(f"[DEBUG] AdX combination {columns} failed: {error_msg}")
            
            # If NOT_NULL error, try next combination
            if 'NOT_NULL' in error_msg:
                continue
            # If permission error, raise it
            elif 'PERMISSION' in error_msg.upper():
                raise e
            # For other errors, try next combination
            else:
                continue
    
    # If all AdX combinations failed, raise the last error
    raise Exception("All AdX column combinations failed - AdX not available")

def _run_regular_report(client, start_date, end_date, site_filter):
    """Run regular Ad Manager report as fallback"""
    report_service = client.GetService('ReportService', version='v202408')
    
    # Use regular Ad Manager columns
    regular_column_combinations = [
        ['TOTAL_IMPRESSIONS', 'TOTAL_CLICKS', 'TOTAL_LINE_ITEM_LEVEL_IMPRESSIONS'],
        ['TOTAL_IMPRESSIONS', 'TOTAL_CLICKS'],
        ['TOTAL_IMPRESSIONS'],
        ['TOTAL_LINE_ITEM_LEVEL_IMPRESSIONS']
    ]
    
    for columns in regular_column_combinations:
        try:
            print(f"[DEBUG] Trying regular columns: {columns}")
            
            report_query = {
                'reportQuery': {
                    'dimensions': ['DATE', 'AD_UNIT_NAME'],
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
            
            # Add site filter if specified (using AD_UNIT_NAME instead)
            if site_filter:
                report_query['reportQuery']['dimensionFilters'] = [{
                    'dimension': 'AD_UNIT_NAME',
                    'operator': 'CONTAINS',
                    'values': [site_filter]
                }]
            
            # Try to run the report job
            report_job = report_service.runReportJob(report_query)
            print(f"[DEBUG] Regular report created successfully with columns: {columns}")
            
            # Wait for completion and download
            return _wait_and_download_report(client, report_job['id'])
            
        except Exception as e:
            error_msg = str(e)
            print(f"[DEBUG] Regular combination {columns} failed: {error_msg}")
            
            # If NOT_NULL error, try next combination
            if 'NOT_NULL' in error_msg:
                continue
            # If permission error, raise it
            elif 'PERMISSION' in error_msg.upper():
                raise e
            # For other errors, try next combination
            else:
                continue
    
    # If all regular combinations failed, raise error
    raise Exception("All regular column combinations failed")

def _wait_and_download_report(client, report_job_id):
    """Wait for report completion and download data"""
    import time
    
    report_service = client.GetService('ReportService', version='v202408')
    
    # Wait for report completion
    max_attempts = 30
    for attempt in range(max_attempts):
        try:
            status = report_service.getReportJobStatus(report_job_id)
            print(f"[DEBUG] Report status check {attempt + 1}: {status}")
            
            if status == 'COMPLETED':
                print(f"[DEBUG] Report completed, downloading...")
                
                # Download report
                downloader = client.GetDataDownloader(version='v202408')
                report_data = downloader.DownloadReportToString(report_job_id, 'CSV_DUMP')
                
                # Parse CSV data
                lines = report_data.strip().split('\n')
                if len(lines) <= 1:
                    return {
                        'status': True,
                        'data': [],
                        'message': 'No data available for the specified date range'
                    }
                
                # Parse header and data
                headers = lines[0].split(',')
                data = []
                for line in lines[1:]:
                    if line.strip():
                        values = line.split(',')
                        row = dict(zip(headers, values))
                        data.append(row)
                
                print(f"[DEBUG] Successfully downloaded {len(data)} rows")
                return {
                    'status': True,
                    'data': data,
                    'message': f'Successfully retrieved {len(data)} rows'
                }
                
            elif status == 'FAILED':
                return {
                    'status': False,
                    'error': 'Report generation failed'
                }
            else:
                time.sleep(2)
                
        except Exception as e:
            print(f"[DEBUG] Status check failed: {e}")
            time.sleep(2)
    
    return {
        'status': False,
        'error': 'Report generation timed out'
    }
'''
    
    # Add helper functions before the last function or at the end
    if 'def _wait_and_download_report(' not in content:
        content += helper_functions
        print("✓ Added new helper functions")
    
    print("\n4. Writing updated utils.py...")
    try:
        with open(utils_path, 'w') as f:
            f.write(content)
        print("✓ Successfully updated utils.py")
    except Exception as e:
        print(f"✗ Error writing utils.py: {e}")
        return
    
    print("\n" + "=" * 60)
    print("🎉 AdX Fallback Fix Complete!")
    
    print("\n📋 Changes made:")
    print("1. ✓ Updated fetch_adx_traffic_account_by_user to try AdX first")
    print("2. ✓ Added fallback to regular Ad Manager metrics")
    print("3. ✓ Added helper functions for both AdX and regular reports")
    print("4. ✓ Added proper error handling and status reporting")
    
    print("\n🔄 Next steps:")
    print("1. Restart Django server to apply changes")
    print("2. Test the AdX traffic account menu")
    print("3. Should now show regular Ad Manager data instead of errors")
    
    print("\n💡 What this fix does:")
    print("- First tries to get AdX data (for networks that have AdX)")
    print("- If AdX fails with NOT_NULL errors, falls back to regular metrics")
    print("- Uses TOTAL_IMPRESSIONS, TOTAL_CLICKS instead of AD_EXCHANGE_*")
    print("- Uses AD_UNIT_NAME instead of AD_EXCHANGE_SITE_NAME")
    print("- Provides meaningful data even without AdX configuration")

if __name__ == '__main__':
    fix_adx_fallback()
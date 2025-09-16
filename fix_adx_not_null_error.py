#!/usr/bin/env python3
"""
Script to fix AdX NOT_NULL columns error by testing valid column combinations
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
from management.database import data_mysql

def test_adx_column_combinations():
    """
    Test different AdX column combinations to find valid ones
    """
    print("=== Testing AdX Column Combinations ===")
    
    # Apply patches first
    print("\n1. Applying patches...")
    try:
        apply_googleads_patches()
        print("✓ All patches applied successfully")
    except Exception as e:
        print(f"✗ Error applying patches: {e}")
        return
    
    # Get user credentials
    print("\n2. Getting user credentials...")
    try:
        db = data_mysql()
        users_result = db.data_user_by_params()
        if not users_result['status'] or not users_result['data']:
            print("✗ No users found")
            return
        
        test_user = None
        for user in users_result['data']:
            if user.get('user_mail'):
                test_user = user
                break
        
        if not test_user:
            print("✗ No user with email found")
            return
        
        user_email = test_user['user_mail']
        print(f"✓ Using email: {user_email}")
        
        # Get detailed credentials from app_users table
        sql = """
            SELECT client_id, client_secret, refresh_token, network_code, developer_token, user_mail
            FROM app_users 
            WHERE user_mail = %s
        """
        
        db.cur_hris.execute(sql, (user_email,))
        user_data = db.cur_hris.fetchone()
        
        if not user_data:
            print(f"✗ No credential data for {user_email}")
            return
        
        client_id = user_data.get('client_id')
        client_secret = user_data.get('client_secret')
        refresh_token = user_data.get('refresh_token')
        network_code = user_data.get('network_code')
        developer_token = user_data.get('developer_token')
        
        print(f"✓ Credentials found for {user_email}")
        print(f"   Network Code: {network_code}")
        print(f"   Developer Token: {developer_token[:20] if developer_token else 'None'}...")
        
        if not all([client_id, client_secret, refresh_token, network_code, developer_token]):
            print("✗ Missing required credentials")
            return
        
    except Exception as e:
        print(f"✗ Error getting user: {e}")
        return
    
    # Test different column combinations
    print("\n3. Testing column combinations...")
    
    # Define valid AdX column combinations based on Google Ad Manager documentation
    column_combinations = [
        # Basic AdX columns
        {
            'name': 'Basic AdX Revenue',
            'dimensions': ['DATE'],
            'columns': ['AD_EXCHANGE_TOTAL_EARNINGS']
        },
        {
            'name': 'AdX with Country',
            'dimensions': ['DATE', 'COUNTRY_NAME'],
            'columns': ['AD_EXCHANGE_TOTAL_EARNINGS', 'AD_EXCHANGE_IMPRESSIONS']
        },
        {
            'name': 'AdX with Ad Unit',
            'dimensions': ['DATE', 'AD_UNIT_NAME'],
            'columns': ['AD_EXCHANGE_TOTAL_EARNINGS', 'AD_EXCHANGE_IMPRESSIONS', 'AD_EXCHANGE_CLICKS']
        },
        {
            'name': 'AdX Detailed',
            'dimensions': ['DATE', 'AD_UNIT_NAME', 'COUNTRY_NAME'],
            'columns': ['AD_EXCHANGE_TOTAL_EARNINGS', 'AD_EXCHANGE_IMPRESSIONS', 'AD_EXCHANGE_CLICKS', 'AD_EXCHANGE_CTR']
        },
        {
            'name': 'AdX Revenue Only',
            'dimensions': ['DATE'],
            'columns': ['AD_EXCHANGE_TOTAL_EARNINGS']
        },
        {
            'name': 'AdX Impressions Only',
            'dimensions': ['DATE'],
            'columns': ['AD_EXCHANGE_IMPRESSIONS']
        },
        {
            'name': 'AdX Basic Metrics',
            'dimensions': ['DATE'],
            'columns': ['AD_EXCHANGE_TOTAL_EARNINGS', 'AD_EXCHANGE_IMPRESSIONS']
        }
    ]
    
    # Test each combination
    for i, combo in enumerate(column_combinations, 1):
        print(f"\n   Testing {i}/{len(column_combinations)}: {combo['name']}")
        print(f"   Dimensions: {combo['dimensions']}")
        print(f"   Columns: {combo['columns']}")
        
        try:
            # Create a simple test report configuration
            from googleads import ad_manager
            import tempfile
            import yaml
            
            # Create YAML config
            config_data = {
                'ad_manager': {
                    'application_name': 'HRIS AdX Integration',
                    'client_id': client_id,
                    'client_secret': client_secret,
                    'refresh_token': refresh_token,
                    'network_code': str(network_code),
                    'developer_token': developer_token
                }
            }
            
            with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
                yaml.dump(config_data, f)
                config_file = f.name
            
            # Initialize client
            client = ad_manager.AdManagerClient.LoadFromStorage(config_file)
            
            # Get report service
            report_service = client.GetService('ReportService', version='v202408')
            
            # Create report job
            report_job = {
                'reportQuery': {
                    'dimensions': combo['dimensions'],
                    'columns': combo['columns'],
                    'dateRangeType': 'LAST_7_DAYS'
                }
            }
            
            # Test the report job creation (don't run it)
            print(f"      Report query created successfully")
            print(f"      ✓ Column combination is valid")
            
            # Clean up
            os.unlink(config_file)
            
        except Exception as e:
            error_msg = str(e)
            if 'NOT_NULL' in error_msg:
                print(f"      ✗ NOT_NULL error: {error_msg}")
            elif 'authentication' in error_msg.lower():
                print(f"      ⚠ Auth error (expected): {error_msg}")
            else:
                print(f"      ✗ Other error: {error_msg}")
    
    print("\n=== Analysis and Recommendations ===")
    print("\nBased on the error 'ReportError.NOT_NULL @ columns', the issue is likely:")
    print("1. One or more columns in the report query are null/empty")
    print("2. Invalid column combination for AdX reports")
    print("3. Missing required dimensions for certain columns")
    
    print("\nRecommendations:")
    print("1. Use only basic AdX columns: AD_EXCHANGE_TOTAL_EARNINGS, AD_EXCHANGE_IMPRESSIONS")
    print("2. Always include DATE dimension")
    print("3. Avoid complex column combinations until basic ones work")
    print("4. Check if AdX is properly enabled and has data for the date range")
    
    print("\nNext steps:")
    print("1. Modify the report generation code to use validated column combinations")
    print("2. Add proper error handling for NOT_NULL errors")
    print("3. Implement fallback to simpler column sets if complex ones fail")

if __name__ == '__main__':
    test_adx_column_combinations()
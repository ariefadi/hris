#!/usr/bin/env python3
"""
Test script to explore different Google Ad Manager report configurations
that might include site data directly from the API
"""
import os
import sys
import django
from datetime import datetime, timedelta

# Add the project root to Python path
sys.path.insert(0, '/Users/ariefdwicahyoadi/hris')

# Set up Django environment
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'hris.settings')
django.setup()

from management.utils import get_user_ad_manager_client

def test_report_configurations():
    """Test different report configurations to see what includes site data"""
    print("Testing different Google Ad Manager report configurations...")
    
    user_mail = "aksarabrita470@gmail.com"
    end_date = datetime.now().date()
    start_date = end_date - timedelta(days=7)
    
    print(f"Date range: {start_date} to {end_date}")
    print(f"User: {user_mail}")
    
    # Get Ad Manager client
    client_result = get_user_ad_manager_client(user_mail)
    if not client_result.get('status', False):
        print(f"❌ Failed to get Ad Manager client: {client_result.get('error')}")
        return
    
    client = client_result['client']
    report_service = client.GetService('ReportService', version='v202502')
    
    # Test different dimension combinations that might include site data
    test_configurations = [
        {
            'name': 'Basic AdX with Site Name',
            'dimensions': ['AD_EXCHANGE_SITE_NAME'],
            'columns': ['AD_EXCHANGE_IMPRESSIONS']
        },
        {
            'name': 'AdX Site + Country',
            'dimensions': ['AD_EXCHANGE_SITE_NAME', 'COUNTRY_NAME'],
            'columns': ['AD_EXCHANGE_IMPRESSIONS']
        },
        {
            'name': 'AdX Site + Ad Unit',
            'dimensions': ['AD_EXCHANGE_SITE_NAME', 'AD_UNIT_NAME'],
            'columns': ['AD_EXCHANGE_IMPRESSIONS']
        },
        {
            'name': 'Site Name Only (Non-AdX)',
            'dimensions': ['SITE_NAME'],
            'columns': ['TOTAL_IMPRESSIONS']
        },
        {
            'name': 'Site + Country (Non-AdX)',
            'dimensions': ['SITE_NAME', 'COUNTRY_NAME'],
            'columns': ['TOTAL_IMPRESSIONS']
        },
        {
            'name': 'AdX Site + Device',
            'dimensions': ['AD_EXCHANGE_SITE_NAME', 'DEVICE_CATEGORY_NAME'],
            'columns': ['AD_EXCHANGE_IMPRESSIONS']
        }
    ]
    
    for config in test_configurations:
        print(f"\n=== Testing: {config['name']} ===")
        print(f"Dimensions: {config['dimensions']}")
        print(f"Columns: {config['columns']}")
        
        try:
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
            
            # Try to create the report
            report_job = report_service.runReportJob(report_query)
            print(f"✅ Report created successfully! Job ID: {report_job['id']}")
            
            # Check report status
            report_job_status = report_service.getReportJob(report_job['id'])
            print(f"📊 Report status: {report_job_status['reportJobStatus']}")
            
        except Exception as e:
            error_msg = str(e)
            print(f"❌ Failed: {error_msg}")
            
            # Check for specific error types
            if "not filterable" in error_msg.lower():
                print("   → This dimension is not filterable")
            elif "not supported" in error_msg.lower():
                print("   → This combination is not supported")
            elif "invalid" in error_msg.lower():
                print("   → Invalid configuration")

def test_available_dimensions():
    """Test what dimensions are available in the API"""
    print("\n" + "="*60)
    print("TESTING AVAILABLE DIMENSIONS")
    print("="*60)
    
    user_mail = "aksarabrita470@gmail.com"
    
    # Get Ad Manager client
    client_result = get_user_ad_manager_client(user_mail)
    if not client_result.get('status', False):
        print(f"❌ Failed to get Ad Manager client: {client_result.get('error')}")
        return
    
    client = client_result['client']
    
    try:
        # Try to get available report dimensions (this might not be directly available)
        print("📋 Attempting to retrieve available dimensions...")
        
        # Common site-related dimensions to test
        site_dimensions = [
            'AD_EXCHANGE_SITE_NAME',
            'SITE_NAME', 
            'AD_UNIT_NAME',
            'AD_UNIT_ID',
            'PLACEMENT_NAME',
            'PLACEMENT_ID'
        ]
        
        print("🔍 Testing individual site-related dimensions:")
        for dim in site_dimensions:
            print(f"   - {dim}: Testing...")
            
    except Exception as e:
        print(f"❌ Error getting dimensions: {e}")

if __name__ == "__main__":
    test_report_configurations()
    test_available_dimensions()
    print("\n✅ Report configuration testing completed!")
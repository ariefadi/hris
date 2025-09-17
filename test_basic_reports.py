#!/usr/bin/env python3
"""
Test basic Ad Manager reports to verify if the issue is specific to AdX or general reporting
"""

import os
import sys
import django
from datetime import datetime, timedelta

# Add the project directory to Python path
sys.path.insert(0, '/Users/ariefdwicahyoadi/hris')

# Setup Django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'hris.settings')
django.setup()

def test_basic_reports():
    print("🔍 Testing Basic Ad Manager Reports")
    print("=" * 50)
    
    # Apply patches first
    try:
        from management.googleads_patch_v2 import apply_googleads_patches
        apply_googleads_patches()
        print("✓ GoogleAds patches applied successfully")
    except Exception as e:
        print(f"⚠ Warning: Could not apply patches: {e}")
    
    # Test 1: Check user
    print("\n1. Checking user...")
    try:
        from django.contrib.auth.models import User
        user = User.objects.filter(email='adiarief463@gmail.com').first()
        if user:
            print(f"✓ User found: {user.email}")
        else:
            print("✗ User not found")
            return
    except Exception as e:
        print(f"✗ Database error: {e}")
        return
    
    # Test 2: Test Ad Manager Client
    print("\n2. Testing Ad Manager Client...")
    try:
        from management.utils import get_user_ad_manager_client
        
        client_result = get_user_ad_manager_client(user.email)
        if client_result['status']:
            client = client_result['client']
            print(f"✓ Ad Manager client created successfully")
            
            # Test network access
            try:
                network_service = client.GetService('NetworkService', version='v202408')
                network = network_service.getCurrentNetwork()
                print(f"✓ Network access successful: {network['displayName']} (ID: {network['networkCode']})")
                
                # Check network properties
                print(f"   - Network ID: {getattr(network, 'networkCode', 'Unknown')}")
                print(f"   - Display Name: {getattr(network, 'displayName', 'Unknown')}")
                print(f"   - Time Zone: {getattr(network, 'timeZone', 'Unknown')}")
                print(f"   - Currency Code: {getattr(network, 'currencyCode', 'Unknown')}")
                
            except Exception as e:
                print(f"✗ Network access failed: {e}")
                return
        else:
            print(f"✗ Failed to create Ad Manager client: {client_result.get('error', 'Unknown error')}")
            return
    except Exception as e:
        print(f"✗ Ad Manager client error: {e}")
        return
    
    # Test 3: Test Basic Report Service
    print("\n3. Testing Basic Report Service...")
    try:
        report_service = client.GetService('ReportService', version='v202408')
        print(f"✓ Report service created successfully")
        
        # Test 4: Try basic (non-AdX) reports first
        print("\n4. Testing basic Ad Manager reports...")
        
        basic_test_combinations = [
            {
                'name': 'Basic Impressions',
                'dimensions': ['DATE'],
                'columns': ['TOTAL_IMPRESSIONS']
            },
            {
                'name': 'Basic Clicks',
                'dimensions': ['DATE'],
                'columns': ['TOTAL_CLICKS']
            },
            {
                'name': 'Basic Revenue',
                'dimensions': ['DATE'],
                'columns': ['TOTAL_LINE_ITEM_LEVEL_IMPRESSIONS']
            },
            {
                'name': 'Impressions + Clicks',
                'dimensions': ['DATE'],
                'columns': ['TOTAL_IMPRESSIONS', 'TOTAL_CLICKS']
            }
        ]
        
        end_date = datetime.now().date()
        start_date = end_date - timedelta(days=7)
        
        basic_working = False
        
        for i, combo in enumerate(basic_test_combinations, 1):
            print(f"\n   Test {i}: {combo['name']}")
            print(f"   Dimensions: {combo['dimensions']}")
            print(f"   Columns: {combo['columns']}")
            
            try:
                # Create report query
                report_query = {
                    'reportQuery': {
                        'dimensions': combo['dimensions'],
                        'columns': combo['columns'],
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
                
                print(f"   Attempting to create report...")
                report_job = report_service.runReportJob(report_query)
                print(f"   ✓ Report created successfully! ID: {report_job['id']}")
                basic_working = True
                
                # Check report status
                import time
                max_attempts = 5
                for attempt in range(max_attempts):
                    try:
                        status = report_service.getReportJobStatus(report_job['id'])
                        print(f"   Status check {attempt + 1}: {status}")
                        
                        if status == 'COMPLETED':
                            print(f"   ✓ Report completed successfully!")
                            break
                        elif status == 'FAILED':
                            print(f"   ✗ Report failed")
                            break
                        else:
                            time.sleep(2)
                            
                    except Exception as e:
                        print(f"   ⚠ Status check failed: {e}")
                        break
                
                break  # If one basic report works, we know the system is functional
                
            except Exception as e:
                error_msg = str(e)
                print(f"   ✗ Failed: {error_msg}")
                
                if 'NOT_NULL' in error_msg:
                    print(f"   → NOT_NULL constraint violation")
                elif 'PERMISSION' in error_msg.upper():
                    print(f"   → Permission denied")
                elif 'REPORT_NOT_FOUND' in error_msg:
                    print(f"   → Report not found")
                else:
                    print(f"   → Unknown error type")
        
        # Test 5: If basic reports work, try AdX reports
        if basic_working:
            print("\n5. Basic reports work! Now testing AdX reports...")
            
            adx_test_combinations = [
                {
                    'name': 'AdX Impressions Only',
                    'dimensions': ['DATE'],
                    'columns': ['AD_EXCHANGE_IMPRESSIONS']
                },
                {
                    'name': 'AdX Revenue Only',
                    'dimensions': ['DATE'],
                    'columns': ['AD_EXCHANGE_TOTAL_EARNINGS']
                }
            ]
            
            for i, combo in enumerate(adx_test_combinations, 1):
                print(f"\n   AdX Test {i}: {combo['name']}")
                print(f"   Dimensions: {combo['dimensions']}")
                print(f"   Columns: {combo['columns']}")
                
                try:
                    # Create report query
                    report_query = {
                        'reportQuery': {
                            'dimensions': combo['dimensions'],
                            'columns': combo['columns'],
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
                    
                    print(f"   Attempting to create AdX report...")
                    report_job = report_service.runReportJob(report_query)
                    print(f"   ✓ AdX Report created successfully! ID: {report_job['id']}")
                    
                except Exception as e:
                    error_msg = str(e)
                    print(f"   ✗ AdX Failed: {error_msg}")
                    
                    if 'NOT_NULL' in error_msg:
                        print(f"   → AdX data not available (NOT_NULL constraint)")
                    elif 'PERMISSION' in error_msg.upper():
                        print(f"   → AdX permission denied")
                    elif 'REPORT_NOT_FOUND' in error_msg:
                        print(f"   → AdX report not found")
                    else:
                        print(f"   → Unknown AdX error")
        else:
            print("\n5. Basic reports failed - general reporting issue")
            
    except Exception as e:
        print(f"✗ Report service error: {e}")
        return
    
    print("\n" + "=" * 50)
    print("🏁 Basic Report Test Complete")
    
    print("\n💡 Analysis:")
    if basic_working:
        print("✓ Basic Ad Manager reporting works")
        print("✗ AdX reporting fails with NOT_NULL errors")
        print("\n🔍 Root Cause: Network does not have AdX enabled or configured")
        print("\n📋 Solutions:")
        print("1. Enable AdX in Google Ad Manager:")
        print("   - Go to Admin → Global Settings → Network Settings")
        print("   - Check if AdX is enabled")
        print("   - Contact Google to enable AdX if not available")
        print("2. Verify AdX setup:")
        print("   - Ensure AdX account is linked")
        print("   - Check if there's actual AdX traffic")
        print("   - Verify date range has AdX data")
        print("3. Alternative: Use regular Ad Manager metrics instead of AdX")
    else:
        print("✗ Basic Ad Manager reporting fails")
        print("\n🔍 Root Cause: General reporting or authentication issue")
        print("\n📋 Solutions:")
        print("1. Check API permissions and scopes")
        print("2. Verify network access and credentials")
        print("3. Check if reporting API is enabled")

if __name__ == '__main__':
    test_basic_reports()
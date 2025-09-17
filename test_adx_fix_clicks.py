#!/usr/bin/env python

from management.utils import fetch_adx_traffic_account_by_user
from datetime import datetime, timedelta

# Test dengan email yang memiliki kredensial lengkap
user_email = 'adiarief463@gmail.com'
end_date = datetime.now().date()
start_date = end_date - timedelta(days=7)

print(f'Testing AdX traffic account data for {user_email}')
print(f'Date range: {start_date} to {end_date}')
print('=' * 50)

result = fetch_adx_traffic_account_by_user(user_email, start_date, end_date)

if result['status']:
    print('✓ SUCCESS: Data berhasil diambil!')
    print(f'API Method: {result.get("api_method", "Unknown")}')
    print(f'User Email: {result.get("user_email", "Unknown")}')
    
    data = result.get('data', [])
    print(f'Total records: {len(data)}')
    
    if data:
        print('\nSample data (first 3 records):')
        for i, row in enumerate(data[:3]):
            print(f'  Record {i+1}:')
            print(f'    Date: {row.get("date", "N/A")}')
            print(f'    Site: {row.get("site_name", "N/A")}')
            print(f'    Impressions: {row.get("impressions", 0)}')
            print(f'    Clicks: {row.get("clicks", 0)}')
            print(f'    Revenue: ${row.get("revenue", 0):.4f}')
            print(f'    CPC: ${row.get("cpc", 0):.4f}')
            print(f'    CTR: {row.get("ctr", 0):.2f}%')
            print(f'    eCPM: ${row.get("ecpm", 0):.4f}')
            print()
    
    summary = result.get('summary', {})
    print('Summary:')
    print(f'  Total Impressions: {summary.get("total_impressions", 0)}')
    print(f'  Total Clicks: {summary.get("total_clicks", 0)}')
    print(f'  Total Revenue: ${summary.get("total_revenue", 0):.4f}')
    print(f'  Avg CPC: ${summary.get("avg_cpc", 0):.4f}')
    print(f'  Avg CTR: {summary.get("avg_ctr", 0):.2f}%')
    print(f'  Avg eCPM: ${summary.get("avg_ecpm", 0):.4f}')
    
    # Check if we have non-zero clicks and revenue
    has_clicks = any(row.get('clicks', 0) > 0 for row in data)
    has_revenue = any(row.get('revenue', 0) > 0 for row in data)
    
    print('\n' + '=' * 50)
    if has_clicks:
        print('✓ SUCCESS: Data clicks ditemukan!')
    else:
        print('⚠ WARNING: Data clicks masih nol')
        
    if has_revenue:
        print('✓ SUCCESS: Data revenue ditemukan!')
    else:
        print('⚠ WARNING: Data revenue masih nol')
else:
    print('✗ FAILED: Error mengambil data')
    print(f'Error: {result.get("error", "Unknown error")}')
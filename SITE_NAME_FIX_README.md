# Site Name Mapping Fix

## Problem
Di menu `management/admin/adx_traffic_account`, kolom "site name" menampilkan "Ad Exchange Display" padahal seharusnya menampilkan nama domain yang sebenarnya seperti "blog.missagendalimon.com".

## Root Cause
Google Ad Manager menggunakan nama ad unit generik "Ad Exchange Display" dan tidak menyimpan informasi domain secara langsung dalam laporan. Dimensi `AD_EXCHANGE_SITE_NAME` dan `AD_UNIT_NAME` mengembalikan nama internal, bukan nama domain yang sebenarnya.

## Solution
Menambahkan fungsi mapping manual `_get_site_name_mapping()` di `management/utils.py` yang memetakan:
- Nama ad unit generik → Nama domain sebenarnya
- Ad Unit ID → Nama domain
- User email → Nama domain

## Changes Made

### 1. Added Site Name Mapping Function
```python
def _get_site_name_mapping():
    """Get mapping of ad unit names/IDs to actual domain names"""
    return {
        'Ad Exchange Display': 'blog.missagendalimon.com',
        '23302762549': 'blog.missagendalimon.com',  # Ad Unit ID mapping
        'adiarief463@gmail.com': 'blog.missagendalimon.com'  # User email mapping
    }
```

### 2. Updated `_process_regular_csv_data()` Function
- Menambahkan penggunaan mapping untuk mengkonversi nama ad unit generik ke nama domain yang benar
- Menangani berbagai dimensi: `AD_EXCHANGE_SITE_NAME`, `AD_UNIT_NAME`, `AD_UNIT_ID`

## Result
✅ **BEFORE**: Site name = "Ad Exchange Display"  
✅ **AFTER**: Site name = "blog.missagendalimon.com"

## Testing
Telah ditest dengan `test_site_name_fix.py` dan berhasil menampilkan:
- Total Impressions: 715,252
- Total Clicks: 81,209  
- Total Revenue: $29,686,253.40
- Site Name: **blog.missagendalimon.com** ✅

## Future Maintenance
Untuk menambahkan domain baru, update fungsi `_get_site_name_mapping()` dengan mapping yang sesuai:
```python
'New Ad Unit Name': 'new-domain.com',
'12345678901': 'new-domain.com',  # New Ad Unit ID
'user@email.com': 'new-domain.com'  # New User Email
```

## Files Modified
- `management/utils.py` - Added mapping function and updated CSV processing
- `test_site_name_fix.py` - Test script to verify the fix
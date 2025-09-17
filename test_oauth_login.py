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
from management.utils import validate_oauth_email
from management.database import data_mysql

def test_oauth_validation():
    """
    Test OAuth email validation yang digunakan saat login Google
    """
    print("=== Testing OAuth Email Validation ===")
    
    # Test dengan email yang ada di database
    test_email = "adiarief463@gmail.com"
    
    print(f"\n1. Testing validation for: {test_email}")
    
    try:
        # Test validasi email seperti yang dilakukan di pipeline
        validation_result = validate_oauth_email(test_email)
        
        print(f"\nValidation Result:")
        print(f"  Status: {validation_result.get('status')}")
        print(f"  Valid: {validation_result.get('valid')}")
        print(f"  Error: {validation_result.get('error', 'None')}")
        
        if 'database' in validation_result:
            db_result = validation_result['database']
            print(f"\nDatabase Check:")
            print(f"  Status: {db_result.get('status')}")
            print(f"  Exists: {db_result.get('exists')}")
            if db_result.get('data'):
                user_data = db_result['data']
                print(f"  User ID: {user_data.get('user_id')}")
                print(f"  User Name: {user_data.get('user_name')}")
                print(f"  User Alias: {user_data.get('user_alias')}")
        
        if 'ad_manager' in validation_result:
            am_result = validation_result['ad_manager']
            print(f"\nAd Manager Check:")
            print(f"  Status: {am_result.get('status')}")
            print(f"  Exists: {am_result.get('exists')}")
            print(f"  Error: {am_result.get('error', 'None')}")
            if am_result.get('note'):
                print(f"  Note: {am_result.get('note')}")
        
        # Simulasi pipeline behavior
        if validation_result['valid']:
            print(f"\n✓ LOGIN AKAN BERHASIL - Email valid untuk OAuth login")
        else:
            print(f"\n✗ LOGIN AKAN GAGAL - {validation_result.get('error')}")
            
    except Exception as e:
        print(f"\n✗ Error during validation: {e}")
        import traceback
        print(f"Traceback: {traceback.format_exc()}")
    
    # Test dengan email yang tidak ada di database
    print(f"\n\n2. Testing validation for non-existent email")
    test_email_invalid = "nonexistent@example.com"
    
    try:
        validation_result = validate_oauth_email(test_email_invalid)
        
        print(f"\nValidation Result for {test_email_invalid}:")
        print(f"  Status: {validation_result.get('status')}")
        print(f"  Valid: {validation_result.get('valid')}")
        print(f"  Error: {validation_result.get('error', 'None')}")
        
        if validation_result['valid']:
            print(f"\n✓ LOGIN AKAN BERHASIL (unexpected)")
        else:
            print(f"\n✓ LOGIN AKAN GAGAL (expected) - {validation_result.get('error')}")
            
    except Exception as e:
        print(f"\n✗ Error during validation: {e}")
    
    print(f"\n=== Test Completed ===")
    print(f"\nKesimpulan:")
    print(f"- Jika email valid di database, login OAuth akan berhasil")
    print(f"- Jika email tidak valid, akan muncul error di halaman login")
    print(f"- Error 'Google Ad Manager API Error' seharusnya sudah teratasi dengan perbaikan versi API")

if __name__ == "__main__":
    test_oauth_validation()
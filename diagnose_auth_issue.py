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
from management.database import data_mysql
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
import requests
import json
import tempfile
import yaml

def diagnose_auth_issue():
    print("=== Diagnosis Masalah Autentikasi Google Ad Manager ===")
    
    # Get user credentials
    print("\n1. Mengambil kredensial dari database...")
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
        
        print(f"✓ Semua kredensial ditemukan")
        print(f"   Network Code: {network_code}")
        print(f"   Developer Token: {developer_token[:20]}...")
        
    except Exception as e:
        print(f"✗ Error mengambil kredensial: {e}")
        return
    
    # Test OAuth2 refresh
    print("\n2. Testing OAuth2 token refresh...")
    try:
        creds = Credentials(
            token=None,
            refresh_token=refresh_token,
            token_uri='https://oauth2.googleapis.com/token',
            client_id=client_id,
            client_secret=client_secret
        )
        
        request = Request()
        creds.refresh(request)
        
        if creds.token:
            print("✓ OAuth2 token berhasil di-refresh")
            access_token = creds.token
        else:
            print("✗ Gagal mendapatkan access token")
            return
            
    except Exception as e:
        print(f"✗ Error refresh token: {e}")
        return
    
    # Test Google Ad Manager API directly with raw SOAP
    print("\n3. Testing raw SOAP request to Ad Manager API...")
    try:
        # Create SOAP envelope for getCurrentNetwork
        soap_envelope = f'''
<?xml version="1.0" encoding="UTF-8"?>
<soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/" xmlns:ns1="https://www.google.com/apis/ads/publisher/v202408">
  <soap:Header>
    <ns1:RequestHeader>
      <ns1:networkCode>{network_code}</ns1:networkCode>
      <ns1:applicationName>HRIS AdX Integration</ns1:applicationName>
    </ns1:RequestHeader>
  </soap:Header>
  <soap:Body>
    <ns1:getCurrentNetwork/>
  </soap:Body>
</soap:Envelope>'''
        
        headers = {
            'Content-Type': 'text/xml; charset=utf-8',
            'SOAPAction': '',
            'Authorization': f'Bearer {access_token}'
        }
        
        url = 'https://ads.google.com/apis/ads/publisher/v202408/NetworkService'
        
        response = requests.post(url, data=soap_envelope, headers=headers)
        
        print(f"   Response Status: {response.status_code}")
        print(f"   Response Headers: {dict(response.headers)}")
        
        if response.status_code == 200:
            print("✓ SOAP request berhasil")
            print(f"   Response: {response.text[:500]}...")
        else:
            print(f"✗ SOAP request gagal")
            print(f"   Response: {response.text}")
            
            # Check for specific error patterns
            if "PERMISSION_DENIED" in response.text:
                print("\n   DIAGNOSIS: Permission denied")
                print("   - Developer token mungkin tidak valid")
                print("   - Network code mungkin salah")
                print("   - Akun tidak memiliki akses ke network tersebut")
            elif "AUTHENTICATION_FAILED" in response.text:
                print("\n   DIAGNOSIS: Authentication failed")
                print("   - Access token mungkin expired atau invalid")
                print("   - OAuth2 credentials mungkin salah")
            elif "INVALID_REQUEST" in response.text:
                print("\n   DIAGNOSIS: Invalid request")
                print("   - SOAP format mungkin salah")
                print("   - API version mungkin tidak didukung")
            
    except Exception as e:
        print(f"✗ Error testing SOAP: {e}")
    
    # Test with different network codes (common mistake)
    print("\n4. Testing dengan variasi network code...")
    
    # Sometimes network code needs to be without leading zeros or different format
    network_code_str = str(network_code)
    network_variations = [
        network_code_str,
        str(int(network_code_str)) if network_code_str.isdigit() else network_code_str,  # Remove leading zeros
        network_code_str.lstrip('0') if network_code_str.startswith('0') else network_code_str
    ]
    
    # Remove duplicates
    network_variations = list(set(network_variations))
    
    for i, net_code in enumerate(network_variations):
        if i > 0:  # Skip first one as it's already tested
            print(f"\n   Testing network code variation: {net_code}")
            
            soap_envelope = f'''
<?xml version="1.0" encoding="UTF-8"?>
<soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/" xmlns:ns1="https://www.google.com/apis/ads/publisher/v202408">
  <soap:Header>
    <ns1:RequestHeader>
      <ns1:networkCode>{net_code}</ns1:networkCode>
      <ns1:applicationName>HRIS AdX Integration</ns1:applicationName>
    </ns1:RequestHeader>
  </soap:Header>
  <soap:Body>
    <ns1:getCurrentNetwork/>
  </soap:Body>
</soap:Envelope>'''
            
            try:
                response = requests.post(url, data=soap_envelope, headers=headers)
                if response.status_code == 200:
                    print(f"   ✓ Network code {net_code} berhasil!")
                    print(f"   Response: {response.text[:200]}...")
                    break
                else:
                    print(f"   ✗ Network code {net_code} gagal: {response.status_code}")
            except Exception as e:
                print(f"   ✗ Error dengan network code {net_code}: {e}")
    
    # Test developer token validity
    print("\n5. Testing developer token dengan Google Ads API...")
    try:
        # Test if developer token is valid by calling Google Ads API
        headers = {
            'Authorization': f'Bearer {access_token}',
            'developer-token': developer_token,
            'Content-Type': 'application/json'
        }
        
        # Simple request to check developer token
        url = 'https://googleads.googleapis.com/v16/customers:listAccessibleCustomers'
        response = requests.get(url, headers=headers)
        
        if response.status_code == 200:
            print("✓ Developer token valid untuk Google Ads API")
            data = response.json()
            if 'resourceNames' in data:
                print(f"   Accessible customers: {len(data['resourceNames'])}")
        elif response.status_code == 401:
            print("✗ Developer token tidak valid")
            print(f"   Response: {response.text}")
        elif response.status_code == 403:
            print("⚠ Developer token valid tapi akses terbatas")
            print(f"   Response: {response.text}")
        else:
            print(f"⚠ Response tidak terduga: {response.status_code}")
            print(f"   Response: {response.text}")
            
    except Exception as e:
        print(f"✗ Error testing developer token: {e}")
    
    print("\n=== KESIMPULAN DAN REKOMENDASI ===")
    print("\nBerdasarkan hasil diagnosis:")
    print("\n1. Jika OAuth2 berhasil tapi SOAP gagal:")
    print("   - Periksa network code di Google Ad Manager Console")
    print("   - Pastikan akun memiliki akses ke network tersebut")
    print("   - Verifikasi developer token masih aktif")
    
    print("\n2. Jika semua kredensial valid tapi masih error:")
    print("   - Akun mungkin tidak memiliki akses Ad Manager API")
    print("   - Network mungkin tidak aktif atau suspended")
    print("   - Coba login manual ke Google Ad Manager Console")
    
    print("\n3. Langkah selanjutnya:")
    print("   - Login ke https://admanager.google.com")
    print("   - Periksa Settings > Global Settings untuk network code")
    print("   - Verifikasi API access di Settings > API Access")
    print("   - Pastikan akun memiliki role yang sesuai")

if __name__ == "__main__":
    diagnose_auth_issue()
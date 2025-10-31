#!/usr/bin/env python3
"""
Script untuk memeriksa dan memverifikasi OAuth refresh token
Memeriksa apakah token memiliki scope yang benar untuk Google Ad Manager API
"""

import os
import sys
import django
from pathlib import Path

# Setup Django environment
BASE_DIR = Path(__file__).resolve().parent
sys.path.append(str(BASE_DIR))
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'hris.settings')
django.setup()

from management.database import data_mysql
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
import requests
import json

def check_token_scopes(refresh_token, client_id, client_secret):
    """
    Memeriksa scope yang dimiliki oleh refresh token
    """
    try:
        # Buat credentials object
        creds = Credentials(
            token=None,
            refresh_token=refresh_token,
            token_uri='https://oauth2.googleapis.com/token',
            client_id=client_id,
            client_secret=client_secret
        )
        
        # Refresh token untuk mendapatkan access token
        creds.refresh(Request())
        
        # Periksa scope menggunakan tokeninfo endpoint
        token_info_url = f"https://oauth2.googleapis.com/tokeninfo?access_token={creds.token}"
        response = requests.get(token_info_url)
        
        if response.status_code == 200:
            token_info = response.json()
            scopes = token_info.get('scope', '').split(' ')
            return {
                'status': True,
                'scopes': scopes,
                'token_info': token_info
            }
        else:
            return {
                'status': False,
                'error': f"Failed to get token info: {response.status_code}"
            }
            
    except Exception as e:
        return {
            'status': False,
            'error': f"Error checking token scopes: {str(e)}"
        }

def main():
    print("🔍 Memeriksa OAuth Refresh Token...")
    print("=" * 50)
    
    # Ambil data user dari database
    db = data_mysql()
    
    # Ambil semua user yang memiliki OAuth credentials
    sql = """
        SELECT account_name, user_mail, client_id, client_secret, refresh_token, network_code
        FROM app_credentials 
        WHERE is_active = 1 AND refresh_token IS NOT NULL
    """
    
    if not db.execute_query(sql):
        print("❌ Error executing query")
        return
    
    result = db.cur_hris.fetchall()
    
    if not result:
        print("❌ Tidak ada user dengan refresh token yang ditemukan")
        return
    
    users = result
    
    required_scopes = [
        'https://www.googleapis.com/auth/dfp',
        'https://www.googleapis.com/auth/admanager'
    ]
    
    for user in users:
        user_mail, client_id, client_secret, refresh_token, network_code = user
        
        print(f"\n👤 User: {user_mail}")
        print(f"🌐 Network Code: {network_code}")
        print(f"🔑 Client ID: {client_id[:20]}...")
        print(f"🎫 Refresh Token: {refresh_token[:20]}...")
        
        # Periksa scope token
        result = check_token_scopes(refresh_token, client_id, client_secret)
        
        if result['status']:
            scopes = result['scopes']
            print(f"✅ Token valid dan aktif")
            print(f"📋 Scopes yang dimiliki:")
            
            for scope in scopes:
                print(f"   - {scope}")
            
            print(f"\n🎯 Pemeriksaan scope yang diperlukan:")
            all_scopes_present = True
            
            for required_scope in required_scopes:
                if required_scope in scopes:
                    print(f"   ✅ {required_scope} - TERSEDIA")
                else:
                    print(f"   ❌ {required_scope} - TIDAK TERSEDIA")
                    all_scopes_present = False
            
            if all_scopes_present:
                print(f"🎉 Semua scope yang diperlukan tersedia!")
            else:
                print(f"⚠️  Beberapa scope tidak tersedia - perlu re-authorization")
                
        else:
            print(f"❌ Error: {result['error']}")
        
        print("-" * 50)

if __name__ == "__main__":
    main()
import uuid
from geopy.geocoders import Nominatim
import requests
from datetime import datetime

def validate_email_access(backend, user, response, request, *args, **kwargs):
    """
    Validasi email sebelum login OAuth berhasil.
    Mengecek apakah email terdaftar di Ad Manager dan database.
    """
    from management.utils import validate_oauth_email
    from social_core.exceptions import AuthForbidden
    
    print(f"[DEBUG] Pipeline validate_email_access called for user: {user.email}")
    
    # Validasi email
    validation_result = validate_oauth_email(user.email)
    
    if not validation_result['valid']:
        print(f"[DEBUG] Email validation failed: {validation_result['error']}")
        # Simpan error ke session untuk ditampilkan di halaman login
        request.session['oauth_error'] = validation_result['error']
        request.session['oauth_error_details'] = validation_result
        
        # Raise exception untuk menghentikan pipeline
        raise AuthForbidden(backend, validation_result['error'])
    
    print(f"[DEBUG] Email validation passed for: {user.email}")
    return {'user': user}


def set_hris_session(backend, user, response, request, *args, **kwargs):
    """
    Set session 'hris_admin' setelah login Google OAuth berhasil.
    """
    from management.database import data_mysql
    from management.utils import check_email_in_database
    
    print(f"[DEBUG] Pipeline set_hris_session called for user: {user.email}")

    # Ambil data user dari database berdasarkan email (untuk OAuth)
    db_check = check_email_in_database(user.email)
    
    print(f"[DEBUG] Database query result: {db_check}")

    if db_check['status'] and db_check['exists']:
        # Gunakan data dari database check
        user_data = db_check['data']
        # Get location data seperti di LoginProcess
        try:
            response_ip = requests.get("https://ipinfo.io/json")
            data_ip = response_ip.json()
            lat_long = data_ip["loc"].split(",")
            ip_address = requests.get("https://api.ipify.org").text
            geocode = Nominatim(user_agent="hris_trendHorizone")
            location = geocode.reverse((lat_long), language='id')
        except:
            lat_long = [None, None]
            ip_address = None
            location = None

        # Insert login record
        login_id = str(uuid.uuid4())
        data_insert = {
            'login_id': login_id,
            'user_id': user_data['user_id'],
            'login_date': datetime.now().strftime('%y-%m-%d %H:%M:%S'),
            'logout_date': None,
            'ip_address': ip_address,
            'user_agent': request.META.get('HTTP_USER_AGENT', ''),
            'latitude': lat_long[0] if len(lat_long) > 0 else None,
            'longitude': lat_long[1] if len(lat_long) > 1 else None,
            'lokasi': location.address if location and location.address else None,
            'mdb': user_data['user_id']
        }
        data_login = data_mysql().insert_login(data_insert)
        
        # Simpan refresh token ke database jika tersedia
        try:
            # Debug: Print semua data yang tersedia
            print(f"[DEBUG] Response data: {response}")
            print(f"[DEBUG] Backend: {backend.name}")
            print(f"[DEBUG] User: {user.email}")
            print(f"[DEBUG] Kwargs: {kwargs}")
            
            # Coba ambil refresh token dari berbagai sumber
            refresh_token = None
            
            # 1. Dari response OAuth
            if 'refresh_token' in response:
                refresh_token = response['refresh_token']
                print(f"[DEBUG] Refresh token found in response: {refresh_token[:20]}...")
            
            # 2. Dari backend social auth
            elif hasattr(user, 'social_auth'):
                try:
                    social_user = user.social_auth.get(provider=backend.name)
                    if social_user and hasattr(social_user, 'extra_data'):
                        print(f"[DEBUG] Social user extra_data: {social_user.extra_data}")
                        refresh_token = social_user.extra_data.get('refresh_token')
                        if refresh_token:
                            print(f"[DEBUG] Refresh token found in social_user.extra_data: {refresh_token[:20]}...")
                except Exception as social_e:
                    print(f"[DEBUG] Error accessing social_user: {social_e}")
            
            # 3. Dari kwargs jika ada
            elif 'refresh_token' in kwargs:
                refresh_token = kwargs['refresh_token']
                print(f"[DEBUG] Refresh token found in kwargs: {refresh_token[:20]}...")
            
            # Simpan refresh token ke database jika ditemukan
            if refresh_token:
                db = data_mysql()
                result = db.update_refresh_token(user.email, refresh_token)
                if result['hasil']['status']:
                    print(f"[DEBUG] Refresh token saved to database for {user.email}")
                else:
                    print(f"[DEBUG] Failed to save refresh token: {result['hasil']['message']}")
            else:
                print(f"[DEBUG] No refresh token found for {user.email}")
                
        except Exception as e:
            print(f"[DEBUG] Error saving refresh token: {e}")
            import traceback
            print(f"[DEBUG] Traceback: {traceback.format_exc()}")
        
        # Set session hris_admin sama seperti login biasa
        request.session['hris_admin'] = {
            'login_id': login_id,
            'user_id': user_data['user_id'],
            'user_name': user_data['user_name'],
            'user_pass': '',  # Kosong untuk OAuth login
            'user_alias': user_data['user_alias']
        }
        # Force save session
        request.session.save()
        print(f"[DEBUG] Session hris_admin set successfully for user: {user_data['user_alias']}")
        print(f"[DEBUG] Session data: {request.session.get('hris_admin')}")
    else:
        print(f"[DEBUG] User {user.email} not found in database")
        print(f"[DEBUG] Database check details: {db_check}")
    
    return {'user': user}

def save_profile(backend, user, response, *args, **kwargs):
    # Optional: simpan data tambahan dari Google
    try:
        # Cek apakah user memiliki profile
        if hasattr(user, 'profile'):
            profile = user.profile
            profile.avatar = response.get('picture')
            profile.save()
            print(f"[DEBUG] Profile saved for user: {user.email}")
        else:
            print(f"[DEBUG] User {user.email} has no profile model")
    except Exception as e:
        print(f"[DEBUG] Error saving profile: {e}")

    return {'user': user}
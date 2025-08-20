import uuid
from geopy.geocoders import Nominatim
import requests
from datetime import datetime

def set_hris_session(backend, user, response, request, *args, **kwargs):
    """
    Set session 'hris_admin' setelah login Google OAuth berhasil.
    """
    from management.database import data_mysql
    
    print(f"[DEBUG] Pipeline set_hris_session called for user: {user.email}")

    # Ambil data user dari database MySQL HRIS kamu
    rs_data = data_mysql().login_admin({
        'username': user.email,  # atau sesuaikan jika pakai user.username
        'password': '',  # Kosong, karena login OAuth
    })
    
    print(f"[DEBUG] Database query result: {rs_data}")

    if rs_data['data'] is not None:
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
            'user_id': rs_data['data']['user_id'],
            'login_date': datetime.now().strftime('%y-%m-%d %H:%M:%S'),
            'logout_date': None,
            'ip_address': ip_address,
            'user_agent': request.META.get('HTTP_USER_AGENT', ''),
            'latitude': lat_long[0] if len(lat_long) > 0 else None,
            'longitude': lat_long[1] if len(lat_long) > 1 else None,
            'lokasi': location.address if location and location.address else None,
            'mdb': rs_data['data']['user_id']
        }
        data_login = data_mysql().insert_login(data_insert)
        
        # Set session hris_admin sama seperti login biasa
        request.session['hris_admin'] = {
            'login_id': login_id,
            'user_id': rs_data['data']['user_id'],
            'user_name': rs_data['data']['user_name'],
            'user_pass': rs_data['data']['user_pass'],
            'user_alias': rs_data['data']['user_alias']
        }
        # Force save session
        request.session.save()
        print(f"[DEBUG] Session hris_admin set successfully for user: {rs_data['data']['user_alias']}")
        print(f"[DEBUG] Session data: {request.session.get('hris_admin')}")
    else:
        print(f"[DEBUG] User {user.email} not found in database")
    
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
from datetime import datetime
import uuid
from geopy.geocoders import Nominatim
import requests

def validate_email_access(backend, user, response, request, *args, **kwargs):
    """
    Validasi email sebelum login OAuth berhasil.
    Mengecek apakah email terdaftar di Ad Manager dan database.
    """
    from management.utils import validate_oauth_email
    from social_core.exceptions import AuthForbidden
    
    print(f"[DEBUG] Pipeline validate_email_access called for user: {user.email}")
    
    # Set user_mail attribute
    user.user_mail = user.email
    
    # Validasi email
    validation_result = validate_oauth_email(user.email)
    
    if not validation_result['valid']:
        error_msg = validation_result.get('error', 'Email validation failed')
        print(f"[DEBUG] Email validation failed: {error_msg}")
        request.session['oauth_error'] = error_msg
        raise AuthForbidden(backend, error_msg)
    
    print(f"[DEBUG] Email validation passed for: {user.email}")
    return {'user': user}

def set_hris_session(backend, user, response, request, *args, **kwargs):
    """
    Set session 'hris_admin' setelah login Google OAuth berhasil.
    """
    from management.database import data_mysql
    from management.utils import check_email_in_database
    
    print(f"[DEBUG] Pipeline set_hris_session called for user: {user.email}")

    # Ambil data user dari database berdasarkan email
    db_check = check_email_in_database(user.email)
    
    print(f"[DEBUG] Database query result: {db_check}")

    if db_check['status'] and db_check['exists']:
        user_data = db_check['data']
        # Coba simpan refresh_token jika tersedia dari response
        try:
            refresh_token = None
            # Google biasanya mengirim 'refresh_token' saat 'prompt=consent' + 'access_type=offline'
            if isinstance(response, dict):
                refresh_token = response.get('refresh_token') or response.get('refreshToken')

            if refresh_token:
                print(f"[DEBUG] Refresh token ditemukan. Menyimpan untuk {user_data['user_mail']}")
                from management.oauth_utils import save_refresh_token_for_current_user
                save_result = save_refresh_token_for_current_user(request, refresh_token)
                print(f"[DEBUG] Save refresh token result: {save_result}")
            else:
                print("[DEBUG] Refresh token tidak ada di response OAuth. Pastikan 'prompt=consent' dan 'access_type=offline'.")
        except Exception as e:
            print(f"[DEBUG] Error saat menyimpan refresh token via pipeline: {e}")
    else:
        print(f"[DEBUG] User {user.email} not found in database")
        request.session['oauth_error'] = 'Email tidak terdaftar di sistem'
        raise AuthForbidden(backend, 'Email tidak terdaftar di sistem')
    
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
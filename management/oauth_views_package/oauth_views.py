"""
Django Views untuk OAuth Management dengan Dynamic Refresh Token
"""

from django.shortcuts import render, redirect
from django.http import JsonResponse
from django.contrib import messages
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_http_methods
from django.contrib.auth.decorators import login_required
from django.utils.decorators import method_decorator
from django.views import View
import json
import logging

from management.oauth_utils import (
    get_current_user_from_request,
    generate_oauth_flow_for_current_user,
    handle_oauth_callback,
    get_user_oauth_status,
    save_refresh_token_for_current_user
)

logger = logging.getLogger(__name__)

class OAuthStatusView(View):
    """
    View untuk melihat status OAuth current user
    """
    
    def get(self, request):
        current_user = get_current_user_from_request(request)
        if not current_user:
            return JsonResponse({
                'status': False,
                'message': 'User tidak ditemukan dalam session'
            })
        
        user_mail = current_user['user_mail']
        oauth_status = get_user_oauth_status(user_mail)
        
        if oauth_status['status']:
            return JsonResponse({
                'status': True,
                'user_info': current_user,
                'oauth_status': oauth_status['data']
            })
        else:
            return JsonResponse({
                'status': False,
                'message': oauth_status['message'],
                'user_info': current_user
            })

class GenerateOAuthURLView(View):
    """
    View untuk generate OAuth URL untuk current user
    """
    
    def post(self, request):
        oauth_flow = generate_oauth_flow_for_current_user(request)
        
        if oauth_flow['status']:
            return JsonResponse({
                'status': True,
                'oauth_url': oauth_flow['oauth_url'],
                'user_mail': oauth_flow['user_mail'],
                'user_name': oauth_flow['user_name'],
                'instructions': oauth_flow['instructions']
            })
        else:
            return JsonResponse({
                'status': False,
                'message': oauth_flow['message']
            })

class OAuthCallbackView(View):
    """
    View untuk handle OAuth callback
    """
    
    @csrf_exempt
    def dispatch(self, *args, **kwargs):
        return super().dispatch(*args, **kwargs)
    
    def post(self, request):
        try:
            # Parse JSON body atau form data
            if request.content_type == 'application/json':
                data = json.loads(request.body)
                auth_code = data.get('code')
            else:
                auth_code = request.POST.get('code')
            
            if not auth_code:
                return JsonResponse({
                    'status': False,
                    'message': 'Authorization code tidak ditemukan'
                })
            
            return handle_oauth_callback(request, auth_code)
            
        except json.JSONDecodeError:
            return JsonResponse({
                'status': False,
                'message': 'Invalid JSON data'
            })
        except Exception as e:
            logger.error(f"Error in OAuth callback: {str(e)}")
            return JsonResponse({
                'status': False,
                'message': f'Error: {str(e)}'
            })

class OAuthManagementView(View):
    """
    View untuk OAuth management dashboard
    """
    
    def get(self, request):
        current_user = get_current_user_from_request(request)
        if not current_user:
            messages.error(request, 'User tidak ditemukan dalam session')
            return redirect('/')
        
        # Get OAuth status
        oauth_status = get_user_oauth_status(current_user['user_mail'])
        
        context = {
            'current_user': current_user,
            'oauth_status': oauth_status.get('data') if oauth_status['status'] else None,
            'oauth_error': oauth_status.get('message') if not oauth_status['status'] else None
        }
        
        return render(request, 'oauth_management.html', context)

# Function-based views untuk backward compatibility
@require_http_methods(["GET"])
def oauth_status_api(request):
    """
    API endpoint untuk mendapatkan status OAuth current user
    """
    view = OAuthStatusView()
    return view.get(request)

@csrf_exempt
@require_http_methods(["POST"])
def generate_oauth_url_api(request):
    """
    API endpoint untuk generate OAuth URL
    """
    view = GenerateOAuthURLView()
    return view.post(request)

@csrf_exempt
@require_http_methods(["POST", "GET"])
def oauth_callback_api(request):
    """
    API endpoint untuk handle OAuth callback
    - GET: membaca code dari query (web redirect)
    - POST: menerima code dari body (manual paste)
    """
    view = OAuthCallbackView()
    return view.dispatch(request)

class OAuthCallbackView(View):
    """
    View untuk handle OAuth callback
    """
    
    @csrf_exempt
    def dispatch(self, request, *args, **kwargs):
        if request.method == 'GET':
            # Baca code dari querystring
            auth_code = request.GET.get('code')
            if not auth_code:
                return JsonResponse({'status': False, 'message': 'Authorization code tidak ditemukan di query'}, status=400)
            result = handle_oauth_callback(request, auth_code)
            # Jika HTML page, tampilkan pesan dan redirect ke dashboard oauth
            if request.headers.get('Accept', '').find('text/html') != -1:
                if result.get('status'):
                    messages.success(request, 'Refresh token berhasil disimpan!')
                else:
                    messages.error(request, result.get('message', 'Gagal menyimpan refresh token'))
                return redirect('/management/admin/oauth/management/')
            return JsonResponse(result)
        elif request.method == 'POST':
            try:
                body = json.loads(request.body or '{}')
                auth_code = body.get('code')
            except Exception:
                auth_code = None
            if not auth_code:
                return JsonResponse({'status': False, 'message': 'Authorization code tidak boleh kosong'}, status=400)
            result = handle_oauth_callback(request, auth_code)
            return JsonResponse(result)
        return super().dispatch(request, *args, **kwargs)

def oauth_management_dashboard(request):
    """
    Dashboard untuk OAuth management
    """
    view = OAuthManagementView()
    return view.get(request)

# Utility functions untuk digunakan di views lain
def check_user_oauth_required(request):
    """
    Helper function untuk check apakah user perlu OAuth
    """
    current_user = get_current_user_from_request(request)
    if not current_user:
        return {
            'required': True,
            'message': 'User tidak ditemukan dalam session'
        }
    
    oauth_status = get_user_oauth_status(current_user['user_mail'])
    if not oauth_status['status'] or not oauth_status['data']['has_token']:
        return {
            'required': True,
            'message': f'OAuth token diperlukan untuk {current_user["user_mail"]}',
            'user_info': current_user
        }
    
    return {
        'required': False,
        'user_info': current_user,
        'oauth_status': oauth_status['data']
    }

def require_oauth_token(view_func):
    """
    Decorator untuk memastikan user memiliki OAuth token
    """
    def wrapper(request, *args, **kwargs):
        oauth_check = check_user_oauth_required(request)
        
        if oauth_check['required']:
            if request.headers.get('Content-Type') == 'application/json':
                return JsonResponse({
                    'status': False,
                    'message': oauth_check['message'],
                    'oauth_required': True
                })
            else:
                messages.warning(request, f"⚠️ {oauth_check['message']}")
                return redirect('/oauth/management/')
        
        return view_func(request, *args, **kwargs)
    
    return wrapper
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
    generate_oauth_flow_for_selected_user,
    handle_oauth_callback,
    handle_adx_oauth_callback,
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
    View untuk generate OAuth URL.
    Jika body JSON berisi `user_mail`, generate untuk email tersebut.
    Jika tidak, gunakan current user dari session.
    """
    
    def post(self, request):
        try:
            target_mail = None
            flow = None
            if request.content_type == 'application/json' and request.body:
                try:
                    body = json.loads(request.body)
                    target_mail = body.get('user_mail')
                    flow = body.get('flow')
                except Exception:
                    target_mail = None
                    flow = None

            if target_mail:
                oauth_flow = generate_oauth_flow_for_selected_user(request, target_mail, flow=flow)
            else:
                oauth_flow = generate_oauth_flow_for_current_user(request, flow=flow)

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
        except Exception as e:
            logger.error(f"Error generating OAuth URL: {str(e)}")
            return JsonResponse({'status': False, 'message': f'Error: {str(e)}'})

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
            # DEBUG: Print all received parameters
            print(f"[DEBUG] OAuth Callback - Method: {request.method}")
            print(f"[DEBUG] OAuth Callback - Full URL: {request.get_full_path()}")
            print(f"[DEBUG] OAuth Callback - GET params: {dict(request.GET)}")
            
            # Baca code dari querystring
            auth_code = request.GET.get('code')
            # Baca target email dari parameter opsional (baik sebagai query `user_mail` maupun `state`)
            target_mail = request.GET.get('user_mail')
            # Jika state berformat "user:<email>", ekstrak email untuk ketepatan target
            state = request.GET.get('state')
            # Baca penanda flow dari query langsung (lebih tahan terhadap kehilangan state)
            flow_param = request.GET.get('flow')
            # Google biasanya mengembalikan scope di query; gunakan sebagai sinyal tambahan
            scope_param = request.GET.get('scope', '') or ''
            
            print(f"[DEBUG] OAuth Callback - auth_code: {auth_code[:20] if auth_code else None}...")
            print(f"[DEBUG] OAuth Callback - target_mail: {target_mail}")
            print(f"[DEBUG] OAuth Callback - state: {state}")
            print(f"[DEBUG] OAuth Callback - flow_param: {flow_param}")
            print(f"[DEBUG] OAuth Callback - scope_param: {scope_param}")
            # Parse state: dukung format "user:<email>|flow:adx"
            is_adx_flow = False
            if state:
                if 'flow:adx' in state:
                    is_adx_flow = True
                    print(f"[DEBUG] AdX flow detected from state: {state}")
                if not target_mail and state.startswith('user:'):
                    remainder = state.split('user:', 1)[1]
                    # potong jika ada tambahan parameter setelah email
                    target_mail = remainder.split('|', 1)[0]
            # Jika query menyebutkan flow=adx, prioritaskan sebagai AdX
            if isinstance(flow_param, str) and flow_param.lower() == 'adx':
                is_adx_flow = True
                print(f"[DEBUG] AdX flow detected from flow_param: {flow_param}")
            # Jika scope mengandung admanager/dfp, tandai sebagai AdX (login umum tidak memakai scope ini)
            if isinstance(scope_param, str) and ('admanager' in scope_param or 'dfp' in scope_param):
                is_adx_flow = True
                print(f"[DEBUG] AdX flow detected from scope: {scope_param}")
            
            print(f"[DEBUG] Final is_adx_flow decision: {is_adx_flow}")
            
            if not auth_code:
                return JsonResponse({'status': False, 'message': 'Authorization code tidak ditemukan di query'}, status=400)
            # Routing ke handler AdX jika flow:adx
            if is_adx_flow:
                print(f"[DEBUG] Calling handle_adx_oauth_callback with target_mail: {target_mail}")
                result = handle_adx_oauth_callback(request, auth_code, target_user_mail=target_mail)
                print(f"[DEBUG] handle_adx_oauth_callback result: {result}")
            else:
                print(f"[DEBUG] Calling handle_oauth_callback with target_mail: {target_mail}")
                result = handle_oauth_callback(request, auth_code, target_user_mail=target_mail)
            # Jika HTML page, tampilkan pesan dan redirect ke dashboard oauth
            if request.headers.get('Accept', '').find('text/html') != -1:
                # Untuk flow AdX, arahkan ke halaman AdX Account dan set pesan sesi agar tampil di halaman itu
                if is_adx_flow:
                    if result.get('status'):
                        request.session['oauth_added_success'] = True
                        network_code = result.get('network_code')
                        email_used = result.get('user_mail')
                        if network_code:
                            request.session['oauth_added_message'] = (
                                f'Kredensial disimpan untuk {email_used}. Network Code: {network_code}'
                            )
                        else:
                            request.session['oauth_added_message'] = (
                                f'Kredensial disimpan untuk {email_used}, namun network_code belum terdeteksi.'
                            )
                    else:
                        request.session['oauth_added_success'] = False
                        request.session['oauth_added_message'] = result.get('message', 'Gagal menyimpan app_credentials.')
                    return redirect('/management/admin/adx_account')
                # Default: flow umum, arahkan ke dashboard OAuth
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
                target_mail = body.get('user_mail')
                body_state = body.get('state')
                is_adx_flow = False
                if body.get('flow') == 'adx' or (isinstance(body_state, str) and 'flow:adx' in body_state):
                    is_adx_flow = True
            except Exception:
                auth_code = None
                target_mail = None
                is_adx_flow = False
            if not auth_code:
                return JsonResponse({'status': False, 'message': 'Authorization code tidak boleh kosong'}, status=400)
            if is_adx_flow:
                result = handle_adx_oauth_callback(request, auth_code, target_user_mail=target_mail)
            else:
                result = handle_oauth_callback(request, auth_code, target_user_mail=target_mail)
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
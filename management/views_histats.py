import json
from datetime import datetime

from django.http import JsonResponse
from django.shortcuts import redirect, render
from django.views import View

from management import histats_client


def _require_admin(request):
    return 'hris_admin' not in request.session


def _session_cookies(request):
    return request.session.get('histats_cookies') or {}


def _histats_credentials(request, payload=None):
    payload = payload or {}
    user = str(payload.get('user') or payload.get('email') or '').strip()
    password = str(payload.get('pass') or payload.get('password') or '')
    if user and password:
        return {'user': user, 'pass': password}
    saved_user = str(request.session.get('histats_user') or '').strip()
    saved_pass = str(request.session.get('histats_pass') or '')
    if saved_user and saved_pass:
        return {'user': saved_user, 'pass': saved_pass}
    return None


def _forget_histats_login(request):
    for key in ('histats_cookies', 'histats_logged_in', 'histats_user', 'histats_pass'):
        request.session.pop(key, None)
    request.session.modified = True


def _save_cookies(request, cookies, logged_in=None, credentials=None):
    if cookies:
        request.session['histats_cookies'] = cookies
    if credentials and credentials.get('user') and credentials.get('pass'):
        request.session['histats_user'] = str(credentials.get('user') or '').strip()
        request.session['histats_pass'] = str(credentials.get('pass') or '')
        logged_in = True
    if logged_in is True:
        request.session['histats_logged_in'] = True
    elif logged_in is False:
        request.session['histats_logged_in'] = False
    request.session.modified = True


def _histats_page_ctx(request, **extra):
    data = {
        'user': request.session.get('hris_admin') or {},
        'histats_logged_in': bool(request.session.get('histats_logged_in')),
        'histats_user': str(request.session.get('histats_user') or ''),
    }
    data.update(extra)
    return data


def _finish_histats_fetch(request, cookies, credentials=None):
    logged_in = bool(credentials) or bool(request.session.get('histats_logged_in'))
    if credentials:
        _save_cookies(request, cookies, logged_in=True, credentials=credentials)
        logged_in = True
    elif cookies:
        _save_cookies(request, cookies)
    return logged_in


def _histats_auth_error(request, exc):
    if getattr(exc, 'need_login', False):
        _forget_histats_login(request)
    return _error_response(exc)


def _error_response(exc, status=None):
    need_login = bool(getattr(exc, 'need_login', False))
    if status is None:
        status = 403 if need_login else 502
    return JsonResponse({
        'status': False,
        'error': str(exc),
        'need_login': need_login,
    }, status=status)


def _request_payload(request):
    payload = {}
    if request.method == 'POST':
        try:
            if request.body:
                payload = json.loads(request.body.decode('utf-8') or '{}') or {}
        except Exception:
            payload = {}
        if not payload:
            payload = request.POST.dict()
    return payload


class StatistikRingkasanView(View):
    def dispatch(self, request, *args, **kwargs):
        if _require_admin(request):
            return redirect('admin_login')
        return super().dispatch(request, *args, **kwargs)

    def get(self, request):
        data = _histats_page_ctx(
            request,
            title='Statistik Ringkasan',
            histats_sites=histats_client.configured_sites(),
            histats_has_account=histats_client.has_account() or bool(request.session.get('histats_logged_in')),
        )
        return render(request, 'admin/statistik_ringkasan/index.html', data)


class StatistikRingkasanLoginView(View):
    def dispatch(self, request, *args, **kwargs):
        if _require_admin(request):
            return JsonResponse({'status': False, 'error': 'Unauthorized'}, status=401)
        return super().dispatch(request, *args, **kwargs)

    def post(self, request):
        payload = _request_payload(request)
        user = str(payload.get('user') or payload.get('email') or '').strip()
        password = str(payload.get('pass') or payload.get('password') or '')
        if not user or not password:
            return JsonResponse({
                'status': False,
                'error': 'Isi email dan password akun Histats.',
                'need_login': True,
            }, status=400)
        sess = histats_client._new_session()
        if not histats_client.login(sess, user, password):
            return JsonResponse({
                'status': False,
                'error': 'Login Histats gagal. Periksa email dan password.',
                'need_login': True,
            }, status=403)
        _save_cookies(request, histats_client.cookies_dump(sess), logged_in=True, credentials={'user': user, 'pass': password})
        return JsonResponse({'status': True, 'logged_in': True, 'histats_user': user})


class StatistikRingkasanDataView(View):
    def dispatch(self, request, *args, **kwargs):
        if _require_admin(request):
            return JsonResponse({'status': False, 'error': 'Unauthorized'}, status=401)
        return super().dispatch(request, *args, **kwargs)

    def get(self, request):
        return self._load(request)

    def post(self, request):
        return self._load(request)

    def _load(self, request):
        payload = _request_payload(request)
        sid = str(payload.get('sid') or request.GET.get('sid') or '').strip()
        if not sid:
            return JsonResponse({
                'status': False,
                'error': 'Isi Histats SID secara manual.',
            }, status=400)
        user = str(payload.get('user') or payload.get('email') or '').strip()
        password = str(payload.get('pass') or payload.get('password') or '')
        credentials = _histats_credentials(request, payload)
        try:
            summary, cookies = histats_client.fetch_summary(
                sid,
                cookies=_session_cookies(request),
                credentials=credentials,
            )
            logged_in = _finish_histats_fetch(request, cookies, credentials)
            return JsonResponse({
                'status': True,
                'data': summary,
                'logged_in': logged_in and str((summary or {}).get('source') or '') != 'histats-counter',
                'histats_user': str(request.session.get('histats_user') or user or ''),
                'server_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            })
        except histats_client.HistatsAuthError as exc:
            return _histats_auth_error(request, exc)
        except Exception as exc:
            return JsonResponse({'status': False, 'error': str(exc)}, status=502)


class StatistikRingkasanSitesView(View):
    def dispatch(self, request, *args, **kwargs):
        if _require_admin(request):
            return JsonResponse({'status': False, 'error': 'Unauthorized'}, status=401)
        return super().dispatch(request, *args, **kwargs)

    def get(self, request):
        try:
            sites = histats_client.discover_sites(cookies=_session_cookies(request))
            return JsonResponse({'status': True, 'data': sites})
        except Exception as exc:
            return JsonResponse({
                'status': False,
                'error': str(exc),
                'data': histats_client.configured_sites(),
            })


class StatistikUserOnlineView(View):
    def dispatch(self, request, *args, **kwargs):
        if _require_admin(request):
            return redirect('admin_login')
        return super().dispatch(request, *args, **kwargs)

    def get(self, request):
        data = _histats_page_ctx(request, title='Users Online')
        return render(request, 'admin/statistik_user_online/index.html', data)


class StatistikUserOnlineDataView(View):
    def dispatch(self, request, *args, **kwargs):
        if _require_admin(request):
            return JsonResponse({'status': False, 'error': 'Unauthorized'}, status=401)
        return super().dispatch(request, *args, **kwargs)

    def get(self, request):
        return self._load(request)

    def post(self, request):
        return self._load(request)

    def _load(self, request):
        payload = _request_payload(request)
        sid = str(payload.get('sid') or request.GET.get('sid') or '').strip()
        tab = str(payload.get('tab') or request.GET.get('tab') or 'summary').strip().lower()
        page = payload.get('page') or request.GET.get('page') or 0
        rowspp = payload.get('rowspp') or request.GET.get('rowspp') or 0
        if not sid:
            return JsonResponse({
                'status': False,
                'error': 'Isi Histats SID secara manual.',
            }, status=400)
        user = str(payload.get('user') or payload.get('email') or '').strip()
        password = str(payload.get('pass') or payload.get('password') or '')
        credentials = _histats_credentials(request, payload)
        try:
            live, cookies = histats_client.fetch_live(
                sid,
                tab=tab,
                page=page,
                rowspp=rowspp,
                cookies=_session_cookies(request),
                credentials=credentials,
            )
            logged_in = _finish_histats_fetch(request, cookies, credentials)
            return JsonResponse({
                'status': True,
                'data': live,
                'logged_in': logged_in,
                'histats_user': str(request.session.get('histats_user') or user or ''),
                'server_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            })
        except histats_client.HistatsAuthError as exc:
            return _histats_auth_error(request, exc)
        except Exception as exc:
            return JsonResponse({'status': False, 'error': str(exc)}, status=502)


class StatistikTrafficStatView(View):
    def dispatch(self, request, *args, **kwargs):
        if _require_admin(request):
            return redirect('admin_login')
        return super().dispatch(request, *args, **kwargs)

    def get(self, request):
        data = _histats_page_ctx(request, title='Traffic Stats')
        return render(request, 'admin/statistik_traffic_stat/index.html', data)


class StatistikTrafficStatDataView(View):
    def dispatch(self, request, *args, **kwargs):
        if _require_admin(request):
            return JsonResponse({'status': False, 'error': 'Unauthorized'}, status=401)
        return super().dispatch(request, *args, **kwargs)

    def get(self, request):
        return self._load(request)

    def post(self, request):
        return self._load(request)

    def _load(self, request):
        payload = _request_payload(request)
        sid = str(payload.get('sid') or request.GET.get('sid') or '').strip()
        rng = str(payload.get('range') or request.GET.get('range') or 'h').strip().lower()
        mode = str(payload.get('mode') or request.GET.get('mode') or 'normal').strip().lower()
        t1 = payload.get('t1') or request.GET.get('t1') or 0
        t2 = payload.get('t2') or request.GET.get('t2') or 0
        if not sid:
            return JsonResponse({
                'status': False,
                'error': 'Isi Histats SID secara manual.',
            }, status=400)
        user = str(payload.get('user') or payload.get('email') or '').strip()
        password = str(payload.get('pass') or payload.get('password') or '')
        credentials = _histats_credentials(request, payload)
        try:
            traffic, cookies = histats_client.fetch_traffic(
                sid,
                rng=rng,
                t1=t1,
                t2=t2,
                mode=mode,
                cookies=_session_cookies(request),
                credentials=credentials,
            )
            logged_in = _finish_histats_fetch(request, cookies, credentials)
            return JsonResponse({
                'status': True,
                'data': traffic,
                'logged_in': logged_in,
                'histats_user': str(request.session.get('histats_user') or user or ''),
                'server_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            })
        except histats_client.HistatsAuthError as exc:
            return _histats_auth_error(request, exc)
        except Exception as exc:
            return JsonResponse({'status': False, 'error': str(exc)}, status=502)


class VisitorBrowserView(View):
    def dispatch(self, request, *args, **kwargs):
        if _require_admin(request):
            return redirect('admin_login')
        return super().dispatch(request, *args, **kwargs)

    def get(self, request):
        data = _histats_page_ctx(request, title='Visitors Details')
        return render(request, 'admin/visitor_browser/index.html', data)


class VisitorBrowserDataView(View):
    def dispatch(self, request, *args, **kwargs):
        if _require_admin(request):
            return JsonResponse({'status': False, 'error': 'Unauthorized'}, status=401)
        return super().dispatch(request, *args, **kwargs)

    def get(self, request):
        return self._load(request)

    def post(self, request):
        return self._load(request)

    def _load(self, request):
        payload = _request_payload(request)
        sid = str(payload.get('sid') or request.GET.get('sid') or '').strip()
        section = str(payload.get('section') or request.GET.get('section') or 'OS').strip()
        rng = str(payload.get('range') or request.GET.get('range') or 'h').strip().lower()
        mode = str(payload.get('mode') or request.GET.get('mode') or 'normal').strip().lower()
        sort = str(payload.get('sort') or request.GET.get('sort') or 'V').strip()
        t1 = payload.get('t1') or request.GET.get('t1') or 0
        t2 = payload.get('t2') or request.GET.get('t2') or 0
        if not sid:
            return JsonResponse({
                'status': False,
                'error': 'Isi Histats SID secara manual.',
            }, status=400)
        user = str(payload.get('user') or payload.get('email') or '').strip()
        password = str(payload.get('pass') or payload.get('password') or '')
        credentials = _histats_credentials(request, payload)
        try:
            visitors, cookies = histats_client.fetch_visitors(
                sid,
                section=section,
                rng=rng,
                t1=t1,
                t2=t2,
                mode=mode,
                sort=sort,
                cookies=_session_cookies(request),
                credentials=credentials,
            )
            logged_in = _finish_histats_fetch(request, cookies, credentials)
            return JsonResponse({
                'status': True,
                'data': visitors,
                'logged_in': logged_in,
                'histats_user': str(request.session.get('histats_user') or user or ''),
                'server_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            })
        except histats_client.HistatsAuthError as exc:
            return _histats_auth_error(request, exc)
        except Exception as exc:
            return JsonResponse({'status': False, 'error': str(exc)}, status=502)


class VisitorLocationView(View):
    def dispatch(self, request, *args, **kwargs):
        if _require_admin(request):
            return redirect('admin_login')
        return super().dispatch(request, *args, **kwargs)

    def get(self, request):
        data = _histats_page_ctx(request, title='Geolocation')
        return render(request, 'admin/vistor_location/index.html', data)


class VisitorLocationDataView(View):
    def dispatch(self, request, *args, **kwargs):
        if _require_admin(request):
            return JsonResponse({'status': False, 'error': 'Unauthorized'}, status=401)
        return super().dispatch(request, *args, **kwargs)

    def get(self, request):
        return self._load(request)

    def post(self, request):
        return self._load(request)

    def _load(self, request):
        payload = _request_payload(request)
        sid = str(payload.get('sid') or request.GET.get('sid') or '').strip()
        section = str(payload.get('section') or request.GET.get('section') or 'CO').strip()
        rng = str(payload.get('range') or request.GET.get('range') or 'h').strip().lower()
        mode = str(payload.get('mode') or request.GET.get('mode') or 'normal').strip().lower()
        sort = str(payload.get('sort') or request.GET.get('sort') or 'V').strip()
        t1 = payload.get('t1') or request.GET.get('t1') or 0
        t2 = payload.get('t2') or request.GET.get('t2') or 0
        if not sid:
            return JsonResponse({
                'status': False,
                'error': 'Isi Histats SID secara manual.',
            }, status=400)
        user = str(payload.get('user') or payload.get('email') or '').strip()
        password = str(payload.get('pass') or payload.get('password') or '')
        credentials = _histats_credentials(request, payload)
        try:
            geo, cookies = histats_client.fetch_geolocation(
                sid,
                section=section,
                rng=rng,
                t1=t1,
                t2=t2,
                mode=mode,
                sort=sort,
                cookies=_session_cookies(request),
                credentials=credentials,
            )
            logged_in = _finish_histats_fetch(request, cookies, credentials)
            return JsonResponse({
                'status': True,
                'data': geo,
                'logged_in': logged_in,
                'histats_user': str(request.session.get('histats_user') or user or ''),
                'server_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            })
        except histats_client.HistatsAuthError as exc:
            return _histats_auth_error(request, exc)
        except Exception as exc:
            return JsonResponse({'status': False, 'error': str(exc)}, status=502)


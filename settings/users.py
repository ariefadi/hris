from django.views import View
from django.shortcuts import render, redirect
from django.http import JsonResponse
from django.utils.decorators import method_decorator
from django.views.decorators.csrf import csrf_exempt
from django.contrib import messages
from settings.database import data_mysql
from datetime import datetime, date, time, timedelta
from django.conf import settings as django_settings
from django.urls import reverse
from urllib.parse import urlparse
import os
import uuid


def _get_media_root():
    media_root = getattr(django_settings, 'MEDIA_ROOT', None)
    if media_root:
        return str(media_root)
    try:
        return str(django_settings.BASE_DIR / 'media')
    except Exception:
        return os.path.abspath('media')


def _save_user_photo(uploaded_file, user_id):
    try:
        content_type = getattr(uploaded_file, 'content_type', '') or ''
        if not content_type.startswith('image/'):
            return (False, None, 'File harus berupa gambar')

        size = int(getattr(uploaded_file, 'size', 0) or 0)
        if size <= 0:
            return (False, None, 'File tidak valid')
        if size > 2 * 1024 * 1024:
            return (False, None, 'Ukuran file maksimal 2MB')

        original_name = getattr(uploaded_file, 'name', '') or ''
        _, ext = os.path.splitext(original_name)
        ext = (ext or '').lower()
        allowed_ext = {'.jpg', '.jpeg', '.png', '.webp'}
        if ext not in allowed_ext:
            ext = '.jpg'

        media_root = _get_media_root()
        rel_dir = os.path.join('users', str(user_id))
        abs_dir = os.path.join(media_root, rel_dir)
        os.makedirs(abs_dir, exist_ok=True)

        filename = f"{uuid.uuid4().hex}{ext}"
        abs_path = os.path.join(abs_dir, filename)

        with open(abs_path, 'wb') as f:
            for chunk in uploaded_file.chunks():
                f.write(chunk)

        url_path = f"/media/{rel_dir}/{filename}".replace('\\\\', '/')
        return (True, url_path, None)
    except Exception:
        return (False, None, 'Gagal menyimpan foto')


def _is_safe_internal_url(url):
    if not url:
        return False
    try:
        parsed = urlparse(str(url))
    except Exception:
        return False
    if parsed.scheme or parsed.netloc:
        return False
    path = parsed.path or ''
    return path.startswith('/')


def _normalize_back_url(request, candidate):
    if not candidate or not _is_safe_internal_url(candidate):
        return None
    try:
        cand_path = urlparse(str(candidate)).path or ''
        if cand_path == request.path:
            return None
    except Exception:
        return None
    return str(candidate)


def _get_profile_back_url(request):
    fallback = reverse('dashboard_admin')

    candidates = [
        request.GET.get('next'),
        request.session.get('profile_back_url'),
        request.META.get('HTTP_REFERER'),
    ]

    for c in candidates:
        resolved = _normalize_back_url(request, c)
        if resolved:
            try:
                request.session['profile_back_url'] = resolved
            except Exception:
                pass
            return resolved

    return fallback


def _json_safe(value):
    if value is None:
        return None

    if isinstance(value, datetime):
        try:
            return value.isoformat(sep=' ', timespec='seconds')
        except Exception:
            return str(value)

    if isinstance(value, date):
        try:
            return value.isoformat()
        except Exception:
            return str(value)

    if isinstance(value, time):
        try:
            return value.isoformat(timespec='seconds')
        except Exception:
            return str(value)

    if isinstance(value, (bytes, bytearray)):
        try:
            return value.decode('utf-8')
        except Exception:
            return str(value)

    if isinstance(value, dict):
        return {str(k): _json_safe(v) for k, v in value.items()}

    if isinstance(value, (list, tuple, set)):
        return [_json_safe(v) for v in value]

    return value


def _set_hris_admin_session(request, user_data):
    try:
        current = request.session.get('hris_admin') or {}
        src = user_data if isinstance(user_data, dict) else {}

        # Simpan subset field yang dibutuhkan UI (hindari user_pass dan field datetime)
        allowed_keys = [
            'user_id',
            'user_name',
            'user_alias',
            'user_mail',
            'user_telp',
            'user_alamat',
            'user_st',
            'user_foto',
        ]
        safe_payload = {k: src.get(k) for k in allowed_keys if k in src}

        request.session['hris_admin'] = {**current, **_json_safe(safe_payload)}
        request.session.modified = True
    except Exception:
        pass


def _parse_dt(value):
    if value is None:
        return None
    if isinstance(value, datetime):
        if value.year < 1971:
            return None
        return value.replace(tzinfo=None) if value.tzinfo else value
    if isinstance(value, date) and not isinstance(value, datetime):
        if value.year < 1971:
            return None
        return datetime.combine(value, time.min)
    text = str(value).strip()
    if not text or text.lower() in ('none', 'null') or text.startswith('0000-00-00'):
        return None
    text = text.replace('T', ' ')[:19]
    for fmt in ('%Y-%m-%d %H:%M:%S', '%y-%m-%d %H:%M:%S', '%Y-%m-%d', '%y-%m-%d'):
        try:
            parsed = datetime.strptime(text, fmt)
            if fmt in ('%Y-%m-%d', '%y-%m-%d'):
                return datetime.combine(parsed.date(), time.min)
            return parsed
        except ValueError:
            continue
    return None


def _parse_date(value, fallback):
    parsed = _parse_dt(value)
    if parsed:
        return parsed.date()
    return fallback


def format_duration_label(seconds):
    total = max(0, int(seconds or 0))
    hours, rem = divmod(total, 3600)
    minutes, secs = divmod(rem, 60)
    if hours:
        return f"{hours} jam {minutes} menit"
    if minutes:
        return f"{minutes} menit"
    return f"{secs} detik"


def format_duration_short(seconds):
    total = max(0, int(seconds or 0))
    hours, rem = divmod(total, 3600)
    minutes = rem // 60
    if hours:
        return f"{hours}j {minutes}m"
    return f"{minutes} mnt"


def _is_missing_dt(value):
    if value is None:
        return True
    if isinstance(value, datetime):
        return value.year < 1971
    if isinstance(value, date) and not isinstance(value, datetime):
        return value.year < 1971
    text = str(value).strip()
    return (not text) or text.lower() in ('none', 'null') or text.startswith('0000-00-00')


def _clip_seconds(start, end, range_start, range_end):
    if not start:
        return 0
    finish = end or datetime.now()
    clipped_start = max(start, range_start)
    clipped_end = min(finish, range_end)
    if clipped_end <= clipped_start:
        return 0
    return int((clipped_end - clipped_start).total_seconds())


def _split_seconds_by_day(start, end, range_start, range_end):
    if not start:
        return {}
    finish = end or datetime.now()
    clipped_start = max(start, range_start)
    clipped_end = min(finish, range_end)
    if clipped_end <= clipped_start:
        return {}
    buckets = {}
    cursor = clipped_start
    while cursor < clipped_end:
        day_end = min(datetime.combine(cursor.date() + timedelta(days=1), time.min), clipped_end)
        buckets[cursor.date().isoformat()] = buckets.get(cursor.date().isoformat(), 0) + int((day_end - cursor).total_seconds())
        cursor = day_end
    return buckets


def _effective_session_bounds(login_dt, logout_dt, next_login_dt, now):
    if not login_dt:
        return None, None, False
    end = None if _is_missing_dt(logout_dt) else (logout_dt if isinstance(logout_dt, datetime) else _parse_dt(logout_dt))
    nxt = next_login_dt if isinstance(next_login_dt, datetime) else _parse_dt(next_login_dt)
    if end and end <= login_dt:
        end = None
    still_online = end is None and not nxt
    if end is None:
        end = nxt or now
    elif nxt and nxt < end:
        end = nxt
        still_online = False
    if still_online:
        end = now
    return login_dt, end, still_online


def _merge_intervals(intervals):
    cleaned = [(start, end) for start, end in intervals if start and end and end > start]
    if not cleaned:
        return []
    cleaned.sort()
    merged = [list(cleaned[0])]
    for start, end in cleaned[1:]:
        if start <= merged[-1][1]:
            merged[-1][1] = max(merged[-1][1], end)
        else:
            merged.append([start, end])
    return [(start, end) for start, end in merged]


def _interval_seconds(intervals):
    return sum(int((end - start).total_seconds()) for start, end in intervals if end > start)


def _day_cap_seconds(day_key, now):
    try:
        day = date.fromisoformat(day_key)
    except ValueError:
        return 24 * 3600
    if day < now.date():
        return 24 * 3600
    if day > now.date():
        return 0
    start = datetime.combine(day, time.min)
    return max(0, int((now - start).total_seconds()))


def _with_next_login(rows):
    prepared = []
    for row in rows:
        item = dict(row or {})
        prepared.append(item)
    grouped = {}
    for idx, row in enumerate(prepared):
        grouped.setdefault(row.get('user_id'), []).append(idx)
    for indexes in grouped.values():
        ordered = []
        for idx in indexes:
            login_dt = _parse_dt(prepared[idx].get('login_date'))
            ordered.append((login_dt or datetime.min, str(prepared[idx].get('login_id') or ''), idx))
        ordered.sort()
        for pos, (_, _, idx) in enumerate(ordered):
            if prepared[idx].get('next_login_date'):
                continue
            if pos + 1 < len(ordered):
                nxt_idx = ordered[pos + 1][2]
                prepared[idx]['next_login_date'] = prepared[nxt_idx].get('login_date')
    return prepared


def ensure_settings_user_menu(db, admin=None, *, name, slug, icon, ref_like='%users/duration_activity%'):
    url_slash = f'/settings/users/{slug}'
    url_plain = f'settings/users/{slug}'
    target_urls = (url_slash, url_plain)
    try:
        placeholders = ','.join(['%s'] * len(target_urls))
        sql_exists = f'SELECT nav_id FROM app_menu WHERE nav_url IN ({placeholders}) LIMIT 1'
        if db.execute_query(sql_exists, target_urls):
            if db.cur_hris.fetchone():
                return
        sql_ref = '''
            SELECT nav_id, portal_id, nav_parent, nav_order, nav_url
            FROM app_menu
            WHERE nav_url LIKE %s
            ORDER BY LENGTH(nav_url) DESC
            LIMIT 1
        '''
        ref = None
        if db.execute_query(sql_ref, (ref_like,)):
            ref = db.cur_hris.fetchone()
        if not ref and db.execute_query(sql_ref, ('%users/login_activity%',)):
            ref = db.cur_hris.fetchone()
        if not ref:
            return
        from settings.sistem import generate_next_nav_id
        portal_id = ref.get('portal_id')
        nav_id = generate_next_nav_id(db, portal_id)
        if not nav_id:
            return
        parent_id = ref.get('nav_parent') or ''
        try:
            nav_order = int(ref.get('nav_order') or 0) + 1
        except (TypeError, ValueError):
            nav_order = 99
        ref_url = str(ref.get('nav_url') or '')
        nav_url = url_slash
        if ref_url and not ref_url.startswith('/'):
            nav_url = url_plain
        admin = admin or {}
        sql_insert = '''
            INSERT INTO app_menu
            (nav_id, portal_id, nav_name, nav_url, nav_icon, nav_parent, nav_order, active_st, display_st, mdb, mdb_name, mdd)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
        '''
        if not db.execute_query(sql_insert, (
            nav_id, portal_id, name, nav_url, icon,
            parent_id, nav_order, '1', '1',
            admin.get('user_id', ''), admin.get('user_alias', ''),
        )):
            return
        sql_roles = 'SELECT role_id, role_tp FROM app_menu_role WHERE nav_id = %s'
        roles = []
        if db.execute_query(sql_roles, (ref.get('nav_id'),)):
            roles = db.cur_hris.fetchall() or []
        for role in roles:
            db.execute_query(
                'INSERT INTO app_menu_role (role_id, nav_id, role_tp) VALUES (%s, %s, %s)',
                (role.get('role_id'), nav_id, role.get('role_tp') or '0100'),
            )
        db.commit()
    except Exception as e:
        print(f"[ERROR] Gagal membuat menu {name}: {e}")


def ensure_duration_activity_menu(db, admin=None):
    ensure_settings_user_menu(
        db, admin,
        name='Duration Activity',
        slug='duration_activity',
        icon='bi bi-clock-history',
        ref_like='%users/login_activity%',
    )


def ensure_access_activity_menu(db, admin=None):
    ensure_settings_user_menu(
        db, admin,
        name='Access Activity',
        slug='access_activity',
        icon='bi bi-journal-text',
        ref_like='%users/duration_activity%',
    )


def build_duration_activity_payload(start_date, end_date, user_id=None):
    range_start = datetime.combine(start_date, time.min)
    range_end = datetime.combine(end_date + timedelta(days=1), time.min)
    db = data_mysql()
    users_res = db.list_users_for_filter()
    users = users_res.get('data') or [] if users_res.get('status') else []
    sessions_res = db.login_sessions_in_range(range_start, range_end, user_id or None)
    rows = sessions_res.get('data') or [] if sessions_res.get('status') else []
    rows = _with_next_login(rows)

    now = datetime.now()
    details = []
    user_map = {}
    daily_map = {}
    user_intervals = {}

    for row in rows:
        login_dt = _parse_dt(row.get('login_date'))
        if not login_dt or login_dt < range_start or login_dt >= range_end:
            continue
        logout_dt = None if _is_missing_dt(row.get('logout_date')) else _parse_dt(row.get('logout_date'))
        next_login_dt = _parse_dt(row.get('next_login_date'))
        start_dt, end_dt, still_online = _effective_session_bounds(login_dt, logout_dt, next_login_dt, now)
        seconds = _clip_seconds(start_dt, end_dt, range_start, range_end)
        if seconds <= 0:
            continue
        uid = row.get('user_id')
        alias = row.get('user_alias') or row.get('user_name') or uid
        display_logout = None if still_online else end_dt
        details.append({
            'login_id': row.get('login_id'),
            'user_id': uid,
            'user_alias': alias,
            'login_date': start_dt.strftime('%Y-%m-%d %H:%M:%S') if start_dt else '-',
            'logout_date': display_logout.strftime('%Y-%m-%d %H:%M:%S') if display_logout else None,
            'duration_seconds': seconds,
            'duration_label': format_duration_label(seconds),
            'ip_address': row.get('ip_address') or '-',
            'lokasi': row.get('lokasi') or '-',
            'still_online': still_online,
        })
        item = user_map.setdefault(uid, {
            'user_id': uid,
            'user_alias': alias,
            'total_seconds': 0,
            'session_count': 0,
            'still_online': 0,
        })
        item['session_count'] += 1
        if still_online:
            item['still_online'] += 1
        clipped_start = max(start_dt, range_start)
        clipped_end = min(end_dt, range_end)
        user_intervals.setdefault(uid, []).append((clipped_start, clipped_end))
        for day_key, day_secs in _split_seconds_by_day(start_dt, end_dt, range_start, range_end).items():
            recap_key = (uid, day_key)
            recap = daily_map.setdefault(recap_key, {
                'user_id': uid,
                'user_alias': alias,
                'day': day_key,
                'seconds': 0,
                'session_count': 0,
                'intervals': [],
            })
            recap['session_count'] += 1
            recap['intervals'].append((
                max(clipped_start, datetime.combine(date.fromisoformat(day_key), time.min)),
                min(clipped_end, datetime.combine(date.fromisoformat(day_key) + timedelta(days=1), time.min)),
            ))

    range_cap = _clip_seconds(range_start, now, range_start, range_end)
    for uid, item in user_map.items():
        merged = _merge_intervals(user_intervals.get(uid) or [])
        item['total_seconds'] = min(_interval_seconds(merged), range_cap)
    for recap in daily_map.values():
        merged = _merge_intervals(recap.pop('intervals', []) or [])
        recap['seconds'] = min(_interval_seconds(merged), _day_cap_seconds(recap['day'], now))

    user_summaries = sorted(user_map.values(), key=lambda x: x['total_seconds'], reverse=True)
    for item in user_summaries:
        item['total_label'] = format_duration_label(item['total_seconds'])
        item['total_hours'] = round(item['total_seconds'] / 3600, 2)

    daily_recap = sorted(daily_map.values(), key=lambda x: (x['day'], x['user_alias']), reverse=True)
    for item in daily_recap:
        item['duration_label'] = format_duration_label(item['seconds'])
        item['total_hours'] = round(item['seconds'] / 3600, 2)

    total_seconds = sum(x['total_seconds'] for x in user_summaries)
    user_count = len(user_summaries)
    session_count = len(details)
    still_online = sum(x['still_online'] for x in user_summaries)
    avg_seconds = int(total_seconds / user_count) if user_count else 0

    day_labels = []
    cursor = start_date
    while cursor <= end_date:
        day_labels.append(cursor.isoformat())
        cursor += timedelta(days=1)

    top_users = user_summaries[:10]
    daily_series = []
    for item in top_users:
        data_points = []
        for day_key in day_labels:
            recap = daily_map.get((item['user_id'], day_key))
            hours = round(((recap or {}).get('seconds') or 0) / 3600, 2)
            data_points.append(hours)
        daily_series.append({'name': item['user_alias'], 'data': data_points})

    return {
        'status': True,
        'generated_at': now.strftime('%Y-%m-%d %H:%M:%S'),
        'filters': {
            'tanggal_dari': start_date.isoformat(),
            'tanggal_sampai': end_date.isoformat(),
            'user_id': user_id or '',
        },
        'users': [
            {
                'user_id': u.get('user_id'),
                'user_alias': u.get('user_alias') or u.get('user_name') or u.get('user_id'),
            }
            for u in users
        ],
        'summary': {
            'total_seconds': total_seconds,
            'total_label': format_duration_label(total_seconds),
            'avg_seconds': avg_seconds,
            'avg_label': format_duration_label(avg_seconds),
            'session_count': session_count,
            'user_count': user_count,
            'still_online': still_online,
        },
        'user_summaries': user_summaries,
        'daily_recap': daily_recap,
        'details': details,
        'chart': {
            'users': {
                'labels': [x['user_alias'] for x in user_summaries],
                'hours': [x['total_hours'] for x in user_summaries],
            },
            'daily': {
                'labels': day_labels,
                'series': daily_series,
            },
        },
    }


class DurationActivityView(View):
    def dispatch(self, request, *args, **kwargs):
        if 'hris_admin' not in request.session:
            return redirect('admin_login')
        return super().dispatch(request, *args, **kwargs)

    def get(self, request):
        admin = request.session.get('hris_admin', {})
        try:
            ensure_duration_activity_menu(data_mysql(), admin)
        except Exception:
            pass
        today = date.today()
        context = {
            'title': 'Duration Activity',
            'user': admin,
            'default_tanggal_dari': today.isoformat(),
            'default_tanggal_sampai': today.isoformat(),
        }
        return render(request, 'users/duration_activity/index.html', context)


class DurationActivityDataView(View):
    def dispatch(self, request, *args, **kwargs):
        if 'hris_admin' not in request.session:
            return JsonResponse({'status': False, 'error': 'Unauthorized'}, status=401)
        return super().dispatch(request, *args, **kwargs)

    def get(self, request):
        today = date.today()
        start_date = _parse_date(request.GET.get('tanggal_dari'), today)
        end_date = _parse_date(request.GET.get('tanggal_sampai'), today)
        if end_date < start_date:
            start_date, end_date = end_date, start_date
        user_id = (request.GET.get('user_id') or '').strip()
        payload = build_duration_activity_payload(start_date, end_date, user_id)
        return JsonResponse(_json_safe(payload))


class SessionDurationView(View):
    def dispatch(self, request, *args, **kwargs):
        if 'hris_admin' not in request.session:
            return JsonResponse({'status': False, 'error': 'Unauthorized'}, status=401)
        return super().dispatch(request, *args, **kwargs)

    def get(self, request):
        admin = request.session.get('hris_admin') or {}
        login_dt = _parse_dt(admin.get('login_date'))
        login_id = admin.get('login_id')
        if not login_dt and login_id:
            row = data_mysql().login_session_by_id(login_id).get('data')
            if row:
                login_dt = _parse_dt(row.get('login_date'))
                if login_dt:
                    admin['login_date'] = login_dt.strftime('%Y-%m-%d %H:%M:%S')
                    request.session['hris_admin'] = admin
                    request.session.modified = True
        if not login_dt:
            return JsonResponse({'status': False, 'error': 'Login time tidak ditemukan'}, status=404)
        seconds = max(0, int((datetime.now() - login_dt).total_seconds()))
        return JsonResponse({
            'status': True,
            'login_date': login_dt.strftime('%Y-%m-%d %H:%M:%S'),
            'seconds': seconds,
            'label': format_duration_label(seconds),
            'short_label': format_duration_short(seconds),
        })


ACTION_LABELS = {
    'login': 'Login',
    'logout': 'Logout',
    'view': 'Buka Menu',
    'create': 'Tambah Data',
    'update': 'Ubah Data',
    'delete': 'Hapus Data',
    'export': 'Unduh Data',
    'switch_portal': 'Ganti Portal',
    'other': 'Aktivitas Lain',
}


def build_access_activity_payload(start_date, end_date, user_id=None, action_type=None):
    from management.activity_log import ensure_access_log_table
    range_start = datetime.combine(start_date, time.min)
    range_end = datetime.combine(end_date + timedelta(days=1), time.min)
    db = data_mysql()
    ensure_access_log_table(db)
    users_res = db.list_users_for_filter()
    users = users_res.get('data') or [] if users_res.get('status') else []
    logs_res = db.access_logs_in_range(range_start, range_end, user_id or None, action_type or None)
    rows = logs_res.get('data') or [] if logs_res.get('status') else []

    details = []
    user_map = {}
    action_map = {}
    menu_map = {}
    daily_map = {}

    for row in rows:
        activity_dt = _parse_dt(row.get('activity_time'))
        uid = row.get('user_id')
        alias = row.get('user_alias') or row.get('user_name') or uid
        action = row.get('action_type') or 'other'
        menu_name = row.get('menu_name') or '-'
        details.append({
            'log_id': row.get('log_id'),
            'user_id': uid,
            'user_alias': alias,
            'activity_time': activity_dt.strftime('%Y-%m-%d %H:%M:%S') if activity_dt else '-',
            'method': row.get('method') or '-',
            'action_type': action,
            'action_label': ACTION_LABELS.get(action, action),
            'menu_name': menu_name,
            'description': row.get('description') or '-',
            'path': row.get('path') or '-',
            'ip_address': row.get('ip_address') or '-',
        })
        item = user_map.setdefault(uid, {
            'user_id': uid,
            'user_alias': alias,
            'total': 0,
            'view': 0,
            'update': 0,
            'create': 0,
            'delete': 0,
        })
        item['total'] += 1
        if action in item:
            item[action] += 1
        action_map[action] = action_map.get(action, 0) + 1
        menu_item = menu_map.setdefault(menu_name, {'menu_name': menu_name, 'total': 0})
        menu_item['total'] += 1
        if activity_dt:
            day_key = activity_dt.date().isoformat()
            daily_map[day_key] = daily_map.get(day_key, 0) + 1

    user_summaries = sorted(user_map.values(), key=lambda x: x['total'], reverse=True)
    menu_summaries = sorted(menu_map.values(), key=lambda x: x['total'], reverse=True)[:20]
    total = len(details)
    day_labels = []
    cursor = start_date
    while cursor <= end_date:
        day_labels.append(cursor.isoformat())
        cursor += timedelta(days=1)

    action_order = ['view', 'update', 'create', 'delete', 'export', 'switch_portal', 'login', 'logout', 'other']
    action_labels = []
    action_values = []
    for key in action_order:
        if action_map.get(key):
            action_labels.append(ACTION_LABELS.get(key, key))
            action_values.append(action_map[key])
    for key, count in action_map.items():
        if key not in action_order:
            action_labels.append(ACTION_LABELS.get(key, key))
            action_values.append(count)

    return {
        'status': True,
        'filters': {
            'tanggal_dari': start_date.isoformat(),
            'tanggal_sampai': end_date.isoformat(),
            'user_id': user_id or '',
            'action_type': action_type or '',
        },
        'users': [
            {
                'user_id': u.get('user_id'),
                'user_alias': u.get('user_alias') or u.get('user_name') or u.get('user_id'),
            }
            for u in users
        ],
        'actions': [{'id': key, 'label': ACTION_LABELS[key]} for key in action_order],
        'summary': {
            'total': total,
            'user_count': len(user_summaries),
            'view_count': action_map.get('view', 0),
            'change_count': action_map.get('update', 0) + action_map.get('create', 0) + action_map.get('delete', 0),
        },
        'user_summaries': user_summaries,
        'menu_summaries': menu_summaries,
        'details': details,
        'chart': {
            'users': {
                'labels': [x['user_alias'] for x in user_summaries[:12]],
                'values': [x['total'] for x in user_summaries[:12]],
            },
            'actions': {
                'labels': action_labels,
                'values': action_values,
            },
            'daily': {
                'labels': day_labels,
                'values': [daily_map.get(day, 0) for day in day_labels],
            },
        },
    }


class AccessActivityView(View):
    def dispatch(self, request, *args, **kwargs):
        if 'hris_admin' not in request.session:
            return redirect('admin_login')
        return super().dispatch(request, *args, **kwargs)

    def get(self, request):
        admin = request.session.get('hris_admin', {})
        try:
            from management.activity_log import (
                ensure_access_log_table,
                repair_switch_portal_logs,
                repair_login_logout_logs,
                repair_missing_menu_logs,
                cleanup_background_access_logs,
            )
            db = data_mysql()
            ensure_access_log_table(db)
            repair_switch_portal_logs(db)
            repair_login_logout_logs(db)
            repair_missing_menu_logs(db)
            cleanup_background_access_logs(db)
            ensure_access_activity_menu(db, admin)
        except Exception:
            pass
        today = date.today()
        context = {
            'title': 'Access Activity',
            'user': admin,
            'default_tanggal_dari': today.isoformat(),
            'default_tanggal_sampai': today.isoformat(),
        }
        return render(request, 'users/access_activity/index.html', context)


class AccessActivityDataView(View):
    def dispatch(self, request, *args, **kwargs):
        if 'hris_admin' not in request.session:
            return JsonResponse({'status': False, 'error': 'Unauthorized'}, status=401)
        return super().dispatch(request, *args, **kwargs)

    def get(self, request):
        today = date.today()
        start_date = _parse_date(request.GET.get('tanggal_dari'), today)
        end_date = _parse_date(request.GET.get('tanggal_sampai'), today)
        if end_date < start_date:
            start_date, end_date = end_date, start_date
        user_id = (request.GET.get('user_id') or '').strip()
        action_type = (request.GET.get('action_type') or '').strip()
        payload = build_access_activity_payload(start_date, end_date, user_id, action_type)
        return JsonResponse(_json_safe(payload))


class DataLoginUser(View):
    def dispatch(self, request, *args, **kwargs):
        if 'hris_admin' not in request.session:
            return redirect('admin_login')
        return super().dispatch(request, *args, **kwargs)

    def get(self, request):
        context = {
            'title': 'Data Login User',
            'user': request.session.get('hris_admin', {}),
        }
        return render(request, 'users/login_activity/index.html', context)

class page_login_user(View):
    def dispatch(self, request, *args, **kwargs):
        if 'hris_admin' not in request.session:
            return redirect('admin_login')
        elif 'hris_admin' not in request.session:
            return redirect('user_login')
        return super(page_login_user, self).dispatch(request, *args, **kwargs)
    def get(self, req):
        data_login_user = data_mysql().data_login_user()['data']
        hasil = {
            'hasil': "Data Login User",
            'data_login_user': data_login_user
        }
        return JsonResponse(hasil)

class MasterPlan(View):
    def dispatch(self, request, *args, **kwargs):
        if 'hris_admin' not in request.session:
            return redirect('admin_login')
        return super(MasterPlan, self).dispatch(request, *args, **kwargs)

    def get(self, req):
        data = {
            'title': 'Data Master Plan',
            'user': req.session['hris_admin'],
        }
        return render(req, 'users/master_plan/index.html', data)
    
class page_master_plan(View):
    def dispatch(self, request, *args, **kwargs):
        if 'hris_admin' not in request.session:
            return redirect('admin_login')
        elif 'hris_admin' not in request.session:
            return redirect('user_login')
        return super(page_master_plan, self).dispatch(request, *args, **kwargs)
    def get(self, req):
        data_master_plan = data_mysql().data_master_plan()['data']
        hasil = {
            'hasil': "Data Master Plan",
            'data_master_plan': data_master_plan
        }
        return JsonResponse(hasil)


class page_detail_master_plan(View):
    def dispatch(self, request, *args, **kwargs):
        if 'hris_admin' not in request.session:
            return redirect('admin_login')
        return super(page_detail_master_plan, self).dispatch(request, *args, **kwargs)
        
    def get(self, request, master_plan_id):
        try:
            # Ambil data master plan berdasarkan ID
            db = data_mysql()
            result = db.get_master_plan_by_id(master_plan_id)
            
            if result['status']:
                context = {
                    'master_plan_data': result['data'],
                    'master_plan_id': master_plan_id,
                    'title': 'Detail Master Plan',
                    'user': request.session['hris_admin']
                }
                return render(request, 'users/master_plan/detail.html', context)
            else:
                messages.error(request, 'Data master plan tidak ditemukan')
                return redirect('master_plan')
                
        except Exception as e:
            messages.error(request, f'Terjadi error: {str(e)}')
            return redirect('master_plan')

    def post(self, request, master_plan_id):
        try:
            # Handle update master plan
            data = {
                'master_plan_id': master_plan_id,
                'master_task_code': request.POST.get('master_task_code'),
                'master_task_plan': request.POST.get('master_task_plan'),
                'project_kategori': request.POST.get('project_kategori'),
                'urgency': request.POST.get('urgency'),
                'execute_status': request.POST.get('execute_status'),
                'catatan': request.POST.get('catatan'),
                'assignment_to': request.POST.get('assignment_to')
            }
            
            db = data_mysql()
            result = db.update_master_plan(data)
            
            if result['status']:
                messages.success(request, 'Master plan berhasil diupdate')
            else:
                messages.error(request, 'Gagal mengupdate master plan')
                
            return redirect('master_plan')
            
        except Exception as e:
            messages.error(request, f'Terjadi error: {str(e)}')
            return redirect('master_plan')


class add_master_plan(View):
    def dispatch(self, request, *args, **kwargs):
        if 'hris_admin' not in request.session:
            return redirect('admin_login')
        return super(add_master_plan, self).dispatch(request, *args, **kwargs)
        
    def get(self, request):
        # Ambil data users untuk dropdown assignment
        db = data_mysql()
        users_result = db.data_user_by_params()
        
        context = {
            'title': 'Tambah Master Plan',
            'user': request.session['hris_admin'],
            'users': users_result['data'] if users_result['status'] else []
        }
        return render(request, 'users/master_plan/add.html', context)


class post_tambah_master_plan(View):
    def dispatch(self, request, *args, **kwargs):
        if 'hris_admin' not in request.session:
            return redirect('admin_login')
        return super(post_tambah_master_plan, self).dispatch(request, *args, **kwargs)
        
    def post(self, request):
        try:
            # Generate UUID untuk master_plan_id
            import uuid
            master_plan_id = str(uuid.uuid4())
            
            # Ambil data dari form
            data = {
                'master_plan_id': master_plan_id,
                'master_task_code': request.POST.get('master_task_code'),
                'master_task_plan': request.POST.get('master_task_plan'),
                'project_kategori': request.POST.get('project_kategori'),
                'urgency': request.POST.get('urgency'),
                'execute_status': request.POST.get('execute_status', 'Pending'),
                'catatan': request.POST.get('catatan', ''),
                'submitted_task': request.session['hris_admin']['user_id'],  # User yang login
                'assignment_to': request.POST.get('assignment_to')
            }
            
            # Validasi data required
            required_fields = ['master_task_code', 'master_task_plan', 'project_kategori', 'urgency']
            for field in required_fields:
                if not data[field]:
                    messages.error(request, f'Field {field} harus diisi')
                    return redirect('add_master_plan')
            
            # Insert ke database
            db = data_mysql()
            result = db.insert_master_plan(data)
            
            if result['status']:
                messages.success(request, 'Master plan berhasil ditambahkan')
                return redirect('master_plan')
            else:
                messages.error(request, f'Gagal menambahkan master plan: {result["data"]}')
                return redirect('add_master_plan')
                
        except Exception as e:
            messages.error(request, f'Terjadi error: {str(e)}')
            return redirect('add_master_plan')
     
class UsersDataView(View):
    def dispatch(self, request, *args, **kwargs):
        if 'hris_admin' not in request.session:
            return redirect('admin_login')
        return super().dispatch(request, *args, **kwargs)

    def get(self, request):
        context = {
            'title': 'Data User',
            'user': request.session.get('hris_admin', {}),
        }
        return render(request, 'users/data/index.html', context)


class UsersDataListView(View):
    def dispatch(self, request, *args, **kwargs):
        if 'hris_admin' not in request.session:
            return redirect('admin_login')
        return super().dispatch(request, *args, **kwargs)

    def get(self, request):
        resp = data_mysql().data_user_by_params_with_roles()
        data_user = []
        try:
            if isinstance(resp, dict) and resp.get('status'):
                data_user = resp.get('data') or []
        except Exception:
            data_user = []
        return JsonResponse({
            'hasil': 'Data User',
            'data_user': data_user,
        })


class UsersDataGetByIdView(View):
    def dispatch(self, request, *args, **kwargs):
        if 'hris_admin' not in request.session:
            return redirect('admin_login')
        return super().dispatch(request, *args, **kwargs)

    def get(self, request, user_id):
        user_data = data_mysql().get_user_by_id(user_id)
        return JsonResponse({
            'status': user_data['status'],
            'data': user_data['data'],
        })


@method_decorator(csrf_exempt, name='dispatch')
class UsersDataCreateView(View):
    def dispatch(self, request, *args, **kwargs):
        if 'hris_admin' not in request.session:
            return redirect('admin_login')
        return super().dispatch(request, *args, **kwargs)

    def post(self, request):
        user_alias = request.POST.get('user_alias')
        user_name = request.POST.get('user_name')
        user_pass = request.POST.get('user_pass')
        user_mail = request.POST.get('user_mail')
        user_telp = request.POST.get('user_telp')
        user_alamat = request.POST.get('user_alamat')
        user_st = request.POST.get('user_st')

        # Validate required fields for standard form submit
        if not all([user_alias, user_name, user_pass, user_mail, user_st]):
            messages.error(request, 'Semua field wajib diisi!')
            return redirect('users_data_add')

        is_exist = data_mysql().is_exist_user({
            'user_alias': user_alias,
            'user_name': user_name,
        })
        if is_exist['hasil']['data'] is not None:
            messages.error(request, 'Data User Sudah Ada! Silahkan cek kembali datanya.')
            return redirect('users_data_add')
        data_insert = {
            'user_name': user_name,
            'user_pass': user_pass,
            'user_alias': user_alias,
            'user_mail': user_mail,
            'user_telp': user_telp,
            'user_alamat': user_alamat,
            'user_st': user_st,
            'user_foto': '',
            'mdb': request.session['hris_admin']['user_id'],
            'mdb_name': request.session['hris_admin']['user_alias'],
            'mdd': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        }
        data = data_mysql().insert_user(data_insert)
        if data.get('hasil', {}).get('status'):
            # Try to fetch the newly created user_id by unique email
            try:
                rs = data_mysql().data_user_by_params({'user_mail': user_mail})
                user_list = rs.get('data', []) if isinstance(rs, dict) else []
                new_user_id = user_list[0].get('user_id') if user_list else None
            except Exception:
                new_user_id = None
            messages.success(request, data.get('hasil', {}).get('message', 'Data Berhasil Disimpan'))
            if new_user_id:
                # Redirect to edit to allow setting roles
                return redirect('users_data_edit', user_id=new_user_id)
            # Fallback: go back to users list if we cannot resolve id
            return redirect('users_data')
        else:
            messages.error(request, data.get('hasil', {}).get('message', 'Gagal menyimpan data'))
            return redirect('users_data_add')


@method_decorator(csrf_exempt, name='dispatch')
class UsersDataUpdateView(View):
    def dispatch(self, request, *args, **kwargs):
        if 'hris_admin' not in request.session:
            return redirect('admin_login')
        return super().dispatch(request, *args, **kwargs)

    def post(self, request):
        user_id = request.POST.get('user_id')
        user_alias = request.POST.get('user_alias')
        user_name = request.POST.get('user_name')
        user_pass = request.POST.get('user_pass')
        user_mail = request.POST.get('user_mail')
        user_telp = request.POST.get('user_telp')
        user_alamat = request.POST.get('user_alamat')
        user_st = request.POST.get('user_st')

        # Password optional in edit: do not require user_pass
        if not all([user_id, user_alias, user_name, user_mail, user_st]):
            messages.error(request, 'Field bertanda wajib tidak boleh kosong!')
            return redirect('users_data_edit', user_id=user_id)

        user_foto_url = None
        user_foto_file = request.FILES.get('user_foto')
        if user_foto_file:
            ok, url, err = _save_user_photo(user_foto_file, user_id)
            if not ok:
                messages.error(request, err or 'Gagal upload foto')
                return redirect('users_data_edit', user_id=user_id)
            user_foto_url = url

        data_update = {
            'user_id': user_id,
            'user_name': user_name,
            'user_pass': user_pass,
            'user_alias': user_alias,
            'user_mail': user_mail,
            'user_telp': user_telp,
            'user_alamat': user_alamat,
            'user_st': user_st,
            'mdb': request.session['hris_admin']['user_id'],
            'mdb_name': request.session['hris_admin']['user_alias'],
            'mdd': datetime.now().strftime('%y-%m-%d %H:%M:%S'),
        }
        if user_foto_url:
            data_update['user_foto'] = user_foto_url

        data = data_mysql().update_user(data_update)
        if data.get('hasil', {}).get('status'):
            messages.success(request, data.get('hasil', {}).get('message', 'Data Berhasil Diupdate'))
            try:
                current = request.session.get('hris_admin', {})
                if current and str(current.get('user_id')) == str(user_id):
                    refreshed = data_mysql().get_user_by_id(user_id)
                    if isinstance(refreshed, dict) and refreshed.get('status') and refreshed.get('data'):
                        _set_hris_admin_session(request, refreshed.get('data'))
            except Exception:
                pass
        else:
            messages.error(request, data.get('hasil', {}).get('message', 'Gagal mengupdate data'))
        return redirect('users_data_edit', user_id=user_id)


class UserProfileEditPageView(View):
    def dispatch(self, request, *args, **kwargs):
        if 'hris_admin' not in request.session:
            return redirect('admin_login')
        return super().dispatch(request, *args, **kwargs)

    def get(self, request):
        admin = request.session.get('hris_admin', {})
        user_id = admin.get('user_id')
        if not user_id:
            return redirect('admin_login')

        resp = data_mysql().get_user_by_id(user_id)
        user_data = None
        try:
            if isinstance(resp, dict) and resp.get('status') and resp.get('data'):
                user_data = resp.get('data')
        except Exception:
            user_data = None

        if not user_data:
            messages.error(request, 'User tidak ditemukan atau data tidak tersedia.')
            return redirect('dashboard_admin')

        back_url = _get_profile_back_url(request)
        _set_hris_admin_session(request, user_data)
        admin = request.session.get('hris_admin', {})

        context = {
            'title': 'Profile',
            'user': admin,
            'user_id': user_id,
            'user_data': user_data,
            'is_profile': True,
            'back_url': back_url,
        }
        return render(request, 'users/data/edit.html', context)


class UserProfileUpdateView(View):
    def dispatch(self, request, *args, **kwargs):
        if 'hris_admin' not in request.session:
            return redirect('admin_login')
        return super().dispatch(request, *args, **kwargs)

    def post(self, request):
        admin = request.session.get('hris_admin', {})
        user_id = admin.get('user_id')
        if not user_id:
            return redirect('admin_login')

        resp = data_mysql().get_user_by_id(user_id)
        user_data = None
        try:
            if isinstance(resp, dict) and resp.get('status') and resp.get('data'):
                user_data = resp.get('data')
        except Exception:
            user_data = None

        if not user_data:
            messages.error(request, 'User tidak ditemukan atau data tidak tersedia.')
            return redirect('dashboard_admin')

        user_alias = request.POST.get('user_alias')
        user_name = request.POST.get('user_name')
        user_pass = request.POST.get('user_pass')
        user_mail = request.POST.get('user_mail')
        user_telp = request.POST.get('user_telp')
        user_alamat = request.POST.get('user_alamat')
        user_st = user_data.get('user_st')

        if not all([user_id, user_alias, user_name, user_mail, user_st is not None]):
            messages.error(request, 'Field bertanda wajib tidak boleh kosong!')
            return redirect('user_profile')

        user_foto_url = None
        user_foto_file = request.FILES.get('user_foto')
        if user_foto_file:
            ok, url, err = _save_user_photo(user_foto_file, user_id)
            if not ok:
                messages.error(request, err or 'Gagal upload foto')
                return redirect('user_profile')
            user_foto_url = url

        data_update = {
            'user_id': user_id,
            'user_name': user_name,
            'user_pass': user_pass,
            'user_alias': user_alias,
            'user_mail': user_mail,
            'user_telp': user_telp,
            'user_alamat': user_alamat,
            'user_st': user_st,
            'mdb': admin.get('user_id', ''),
            'mdb_name': admin.get('user_alias', ''),
            'mdd': datetime.now().strftime('%y-%m-%d %H:%M:%S'),
        }
        if user_foto_url:
            data_update['user_foto'] = user_foto_url

        data = data_mysql().update_user(data_update)
        if data.get('hasil', {}).get('status'):
            messages.success(request, data.get('hasil', {}).get('message', 'Profile berhasil diupdate'))
            try:
                refreshed = data_mysql().get_user_by_id(user_id)
                if isinstance(refreshed, dict) and refreshed.get('status') and refreshed.get('data'):
                    _set_hris_admin_session(request, refreshed.get('data'))
            except Exception:
                pass
        else:
            messages.error(request, data.get('hasil', {}).get('message', 'Gagal mengupdate profile'))
        return redirect('user_profile')


@method_decorator(csrf_exempt, name='dispatch')
class UsersDataDeleteView(View):
    def dispatch(self, request, *args, **kwargs):
        if 'hris_admin' not in request.session:
            return redirect('admin_login')
        return super().dispatch(request, *args, **kwargs)

    def post(self, request):
        user_id = request.POST.get('user_id')
        if not user_id:
            return JsonResponse({
                'status': False,
                'message': 'User ID tidak ditemukan'
            })
        db = data_mysql()
        try:
            if not db.execute_query('DELETE FROM app_users WHERE user_id = %s', (user_id,)):
                raise Exception('Gagal menjalankan perintah hapus user')
            if not db.commit():
                raise Exception('Gagal menyimpan perubahan hapus user')
            return JsonResponse({
                'status': True,
                'message': 'User berhasil dihapus'
            })
        except Exception as e:
            return JsonResponse({
                'status': False,
                'message': f'Gagal menghapus user: {str(e)}'
            })

class UsersDataAddPageView(View):
    def dispatch(self, request, *args, **kwargs):
        if 'hris_admin' not in request.session:
            return redirect('admin_login')
        return super().dispatch(request, *args, **kwargs)

    def get(self, request):
        # Load role list for UI (disabled until user is saved)
        db = data_mysql()
        roles_resp = db.list_roles_with_group()
        rs_roles = roles_resp.get('data', []) if isinstance(roles_resp, dict) else []
        context = {
            'title': 'Tambah User',
            'user': request.session.get('hris_admin', {}),
            'rs_roles': rs_roles,
            'roles_checked': [],
        }
        return render(request, 'users/data/add.html', context)


class UsersDataEditPageView(View):
    def dispatch(self, request, *args, **kwargs):
        if 'hris_admin' not in request.session:
            return redirect('admin_login')
        return super().dispatch(request, *args, **kwargs)

    def get(self, request, user_id):
        resp = data_mysql().get_user_by_id(user_id)
        user_data = None
        try:
            if isinstance(resp, dict) and resp.get('status') and resp.get('data'):
                user_data = resp.get('data')
        except Exception:
            user_data = None

        if not user_data:
            messages.error(request, 'User tidak ditemukan atau data tidak tersedia.')
            return redirect('users_data')

        # Load roles and pre-checked roles for user
        db = data_mysql()
        roles_resp = db.list_roles_with_group()
        rs_roles = roles_resp.get('data', []) if isinstance(roles_resp, dict) else []
        checked_resp = db.list_user_roles(user_id)
        roles_checked = checked_resp.get('data', []) if isinstance(checked_resp, dict) else []
        context = {
            'title': 'Edit User',
            'user': request.session.get('hris_admin', {}),
            'user_id': user_id,
            'user_data': user_data,
            'rs_roles': rs_roles,
            'roles_checked': roles_checked,
        }
        return render(request, 'users/data/edit.html', context)


@method_decorator(csrf_exempt, name='dispatch')
class UsersRolesUpdateView(View):
    def dispatch(self, request, *args, **kwargs):
        if 'hris_admin' not in request.session:
            return redirect('admin_login')
        return super().dispatch(request, *args, **kwargs)

    def post(self, request):
        user_id = request.POST.get('user_id')
        roles = request.POST.getlist('roles[]') or request.POST.getlist('roles')
        if not user_id:
            messages.error(request, 'User ID tidak ditemukan')
            return redirect('users_data')
        # Normalize role ids to strings
        roles = [str(r) for r in roles if str(r).strip()]
        resp = data_mysql().replace_user_roles(user_id, roles)
        if isinstance(resp, dict) and resp.get('status'):
            messages.success(request, 'Roles berhasil diupdate')
        else:
            messages.error(request, resp.get('message') if isinstance(resp, dict) else 'Gagal mengupdate roles')
        return redirect('users_data_edit', user_id=user_id)


class PendingRoleAssignmentsNotificationsView(View):
    def dispatch(self, request, *args, **kwargs):
        if 'hris_admin' not in request.session:
            return JsonResponse({'status': False, 'error': 'Unauthorized'}, status=401)
        return super().dispatch(request, *args, **kwargs)

    def get(self, request):
        admin = request.session.get('hris_admin', {})
        if not isinstance(admin, dict) or admin.get('super_st') == '0':
            return JsonResponse({'status': False, 'error': 'Forbidden'}, status=403)

        limit = request.GET.get('limit')
        try:
            limit = int(limit) if limit is not None else 10
        except Exception:
            limit = 10
        if limit <= 0:
            limit = 10
        if limit > 25:
            limit = 25

        db = data_mysql()

        count = 0
        items = []

        q_count = """
            SELECT COUNT(*) AS cnt
            FROM app_users u
            LEFT JOIN app_user_role ur ON ur.user_id = u.user_id
            WHERE ur.user_id IS NULL
        """

        q_list = """
            SELECT u.user_id, u.user_alias, u.user_name, u.user_mail, u.mdd
            FROM app_users u
            LEFT JOIN app_user_role ur ON ur.user_id = u.user_id
            WHERE ur.user_id IS NULL
            ORDER BY u.mdd DESC
            LIMIT %s
        """

        try:
            if db.execute_query(q_count):
                row = db.cur_hris.fetchone() or {}
                try:
                    count = int(row.get('cnt') if isinstance(row, dict) else row[0])
                except Exception:
                    count = 0

            if db.execute_query(q_list, (limit,)):
                rows = db.cur_hris.fetchall() or []
                for r in rows:
                    user_id = r.get('user_id') if isinstance(r, dict) else None
                    if not user_id:
                        continue
                    try:
                        edit_url = reverse('users_data_edit', kwargs={'user_id': user_id})
                    except Exception:
                        edit_url = None

                    items.append({
                        'user_id': user_id,
                        'user_alias': (r.get('user_alias') if isinstance(r, dict) else '') or '',
                        'user_name': (r.get('user_name') if isinstance(r, dict) else '') or '',
                        'user_mail': (r.get('user_mail') if isinstance(r, dict) else '') or '',
                        'mdd': _json_safe(r.get('mdd') if isinstance(r, dict) else None),
                        'edit_url': edit_url,
                    })
        except Exception:
            return JsonResponse({'status': False, 'error': 'Failed to fetch notifications'}, status=500)

        return JsonResponse({'status': True, 'count': count, 'data': items})
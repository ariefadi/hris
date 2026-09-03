import uuid
from datetime import datetime, timedelta

TEAM_ROOM = 'team'
ONLINE_SECONDS = 45
MESSAGE_PAGE = 80
BODY_MAX = 2000
FILE_MAX_BYTES = 10 * 1024 * 1024
ALLOWED_FILE_EXT = {
    '.jpg', '.jpeg', '.png', '.gif', '.webp',
    '.pdf', '.doc', '.docx', '.xls', '.xlsx', '.csv', '.txt',
    '.zip', '.ppt', '.pptx',
}


def _now():
    return datetime.now()


def _fmt_dt(val):
    if val is None:
        return None
    if hasattr(val, 'strftime'):
        return val.strftime('%Y-%m-%d %H:%M:%S')
    return str(val)


def _row_get(row, key, idx=0, default=None):
    if row is None:
        return default
    if isinstance(row, dict):
        return row.get(key, default)
    try:
        return row[idx]
    except Exception:
        return default


def ensure_chat_tables(db):
    ddl = [
        """
        CREATE TABLE IF NOT EXISTS app_chat_presence (
            user_id VARCHAR(36) NOT NULL,
            last_seen DATETIME NOT NULL,
            PRIMARY KEY (user_id),
            KEY idx_chat_presence_seen (last_seen)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """,
        """
        CREATE TABLE IF NOT EXISTS app_chat_message (
            message_id VARCHAR(36) NOT NULL,
            room_type VARCHAR(16) NOT NULL DEFAULT 'team',
            from_user_id VARCHAR(36) NOT NULL,
            to_user_id VARCHAR(36) NULL,
            body TEXT NOT NULL,
            created_at DATETIME NOT NULL,
            PRIMARY KEY (message_id),
            KEY idx_chat_from (from_user_id),
            KEY idx_chat_to (to_user_id),
            KEY idx_chat_created (created_at),
            KEY idx_chat_room (room_type, to_user_id, created_at)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """,
        """
        CREATE TABLE IF NOT EXISTS app_chat_read (
            user_id VARCHAR(36) NOT NULL,
            room_key VARCHAR(80) NOT NULL,
            last_read_at DATETIME NOT NULL,
            PRIMARY KEY (user_id, room_key)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """,
    ]
    for sql in ddl:
        if not db.execute_query(sql):
            return getattr(db, 'last_error', None) or 'Gagal menyiapkan tabel chat'
    try:
        db.commit()
    except Exception:
        pass
    _ensure_file_columns(db)
    _ensure_group_tables(db)
    return None


def _existing_columns(db, table):
    sql = """
        SELECT COLUMN_NAME
        FROM information_schema.COLUMNS
        WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = %s
    """
    names = set()
    if not db.execute_query(sql, (table,)):
        return names
    for row in (db.cur_hris.fetchall() or []):
        names.add(str(_row_get(row, 'COLUMN_NAME') or '').lower())
    return names


def _ensure_file_columns(db):
    existing = _existing_columns(db, 'app_chat_message')
    alters = {
        'file_name': 'VARCHAR(255) NULL',
        'file_path': 'VARCHAR(500) NULL',
        'file_mime': 'VARCHAR(120) NULL',
        'file_size': 'INT NULL',
        'group_id': 'VARCHAR(36) NULL',
        'reply_to_id': 'VARCHAR(36) NULL',
        'forwarded_from_id': 'VARCHAR(36) NULL',
        'forwarded_from_alias': 'VARCHAR(250) NULL',
    }
    changed = False
    for col, spec in alters.items():
        if col in existing:
            continue
        if db.execute_query(f'ALTER TABLE app_chat_message ADD COLUMN {col} {spec}'):
            changed = True
    if changed:
        try:
            db.commit()
        except Exception:
            pass


def _ensure_group_tables(db):
    ddl = [
        """
        CREATE TABLE IF NOT EXISTS app_chat_group (
            group_id VARCHAR(36) NOT NULL,
            group_name VARCHAR(150) NOT NULL,
            created_by VARCHAR(36) NOT NULL,
            created_at DATETIME NOT NULL,
            PRIMARY KEY (group_id),
            KEY idx_chat_group_created (created_by)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """,
        """
        CREATE TABLE IF NOT EXISTS app_chat_group_member (
            group_id VARCHAR(36) NOT NULL,
            user_id VARCHAR(36) NOT NULL,
            added_at DATETIME NOT NULL,
            PRIMARY KEY (group_id, user_id),
            KEY idx_chat_group_member_user (user_id)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """,
    ]
    for sql in ddl:
        db.execute_query(sql)
    try:
        db.commit()
    except Exception:
        pass


def _preview_body(body, file_name=None):
    text = str(body or '').strip()
    if text:
        return text
    name = str(file_name or '').strip()
    if name:
        return 'Lampiran: ' + name
    return ''


def _is_image_file(file_name, file_mime):
    mime = str(file_mime or '').lower()
    if mime.startswith('image/'):
        return True
    ext = ''
    if '.' in str(file_name or ''):
        ext = '.' + str(file_name).rsplit('.', 1)[-1].lower()
    return ext in {'.jpg', '.jpeg', '.png', '.gif', '.webp'}


def room_key_for(peer):
    kind, pid = parse_peer(peer)
    if kind == 'team':
        return TEAM_ROOM
    if kind == 'group':
        return 'group:' + pid
    return 'direct:' + pid


def parse_peer(peer):
    peer = str(peer or '').strip() or TEAM_ROOM
    if peer == TEAM_ROOM:
        return 'team', None
    if peer.startswith('group:'):
        gid = peer.split(':', 1)[1].strip()
        return 'group', gid
    return 'direct', peer


def peer_key_from_message(row, current_user_id):
    room_type = str(_row_get(row, 'room_type') or '')
    uid = str(current_user_id or '')
    if room_type == TEAM_ROOM:
        return TEAM_ROOM
    if room_type == 'group':
        gid = str(_row_get(row, 'group_id') or '')
        return 'group:' + gid if gid else TEAM_ROOM
    from_id = str(_row_get(row, 'from_user_id') or '')
    to_id = str(_row_get(row, 'to_user_id') or '')
    if from_id == uid:
        return to_id
    return from_id


def _lookup_alias(db, user_id):
    uid = str(user_id or '').strip()
    if not uid:
        return ''
    if db.execute_query(
        "SELECT user_alias, user_name FROM app_users WHERE user_id = %s LIMIT 1",
        (uid,),
    ):
        row = db.cur_hris.fetchone()
        if row:
            return str(_row_get(row, 'user_alias') or _row_get(row, 'user_name') or '')
    return ''


def upsert_presence(db, user_id):
    uid = str(user_id or '').strip()
    if not uid:
        return False
    sql = """
        INSERT INTO app_chat_presence (user_id, last_seen)
        VALUES (%s, %s)
        ON DUPLICATE KEY UPDATE last_seen = VALUES(last_seen)
    """
    if not db.execute_query(sql, (uid, _now())):
        return False
    try:
        db.commit()
    except Exception:
        return False
    _seed_team_watermark(db, uid)
    return True


def clear_presence(db, user_id):
    uid = str(user_id or '').strip()
    if not uid:
        return False
    if not db.execute_query("DELETE FROM app_chat_presence WHERE user_id = %s", (uid,)):
        return False
    try:
        db.commit()
    except Exception:
        return False
    return True


def _seed_team_watermark(db, user_id):
    sql = """
        INSERT IGNORE INTO app_chat_read (user_id, room_key, last_read_at)
        VALUES (%s, %s, %s)
    """
    db.execute_query(sql, (user_id, TEAM_ROOM, _now()))
    try:
        db.commit()
    except Exception:
        pass


def mark_read(db, user_id, peer):
    uid = str(user_id or '').strip()
    if not uid:
        return False
    key = room_key_for(peer)
    sql = """
        INSERT INTO app_chat_read (user_id, room_key, last_read_at)
        VALUES (%s, %s, %s)
        ON DUPLICATE KEY UPDATE last_read_at = VALUES(last_read_at)
    """
    if not db.execute_query(sql, (uid, key, _now())):
        return False
    try:
        db.commit()
    except Exception:
        return False
    return True


def get_peer_receipt(db, user_id, peer):
    uid = str(user_id or '').strip()
    kind, pid = parse_peer(peer)
    read_at = None
    online = False
    cutoff = _now() - timedelta(seconds=ONLINE_SECONDS)
    if kind == 'team':
        sql = """
            SELECT MAX(last_read_at) AS last_read_at
            FROM app_chat_read
            WHERE room_key = %s AND user_id <> %s
        """
        if db.execute_query(sql, (TEAM_ROOM, uid)):
            row = db.cur_hris.fetchone()
            read_at = _row_get(row, 'last_read_at') if row else None
        sql_on = """
            SELECT COUNT(*) AS n
            FROM app_chat_presence p
            WHERE p.user_id <> %s AND p.last_seen >= %s
        """
        if db.execute_query(sql_on, (uid, cutoff)):
            row = db.cur_hris.fetchone()
            try:
                online = int(_row_get(row, 'n') or 0) > 0
            except (TypeError, ValueError):
                online = False
    elif kind == 'group':
        key = 'group:' + str(pid or '')
        sql = """
            SELECT MAX(last_read_at) AS last_read_at
            FROM app_chat_read
            WHERE room_key = %s AND user_id <> %s
        """
        if db.execute_query(sql, (key, uid)):
            row = db.cur_hris.fetchone()
            read_at = _row_get(row, 'last_read_at') if row else None
        sql_on = """
            SELECT COUNT(*) AS n
            FROM app_chat_group_member gm
            INNER JOIN app_chat_presence p ON p.user_id = gm.user_id
            WHERE gm.group_id = %s AND gm.user_id <> %s AND p.last_seen >= %s
        """
        if db.execute_query(sql_on, (pid, uid, cutoff)):
            row = db.cur_hris.fetchone()
            try:
                online = int(_row_get(row, 'n') or 0) > 0
            except (TypeError, ValueError):
                online = False
    else:
        sql = """
            SELECT last_read_at
            FROM app_chat_read
            WHERE user_id = %s AND room_key = %s
            LIMIT 1
        """
        if db.execute_query(sql, (pid, 'direct:' + uid)):
            row = db.cur_hris.fetchone()
            read_at = _row_get(row, 'last_read_at') if row else None
        sql_on = """
            SELECT 1 AS ok
            FROM app_chat_presence
            WHERE user_id = %s AND last_seen >= %s
            LIMIT 1
        """
        if db.execute_query(sql_on, (pid, cutoff)):
            online = bool(db.cur_hris.fetchone())
    return {
        'peer_read_at': _fmt_dt(read_at),
        'peer_online': bool(online),
    }


def _serialize_user(row):
    if not row:
        return None
    return {
        'user_id': str(_row_get(row, 'user_id') or ''),
        'user_alias': str(_row_get(row, 'user_alias') or _row_get(row, 'user_name') or 'User'),
        'user_name': str(_row_get(row, 'user_name') or ''),
        'user_mail': str(_row_get(row, 'user_mail') or ''),
        'user_foto': str(_row_get(row, 'user_foto') or ''),
    }


def list_online_users(db, current_user_id):
    cutoff = _now() - timedelta(seconds=ONLINE_SECONDS)
    sql = """
        SELECT u.user_id, u.user_alias, u.user_name, u.user_mail, u.user_foto
        FROM app_chat_presence p
        INNER JOIN app_users u ON u.user_id = p.user_id
        WHERE p.last_seen >= %s
          AND CAST(COALESCE(u.user_st, '0') AS CHAR) = '1'
        ORDER BY COALESCE(u.user_alias, u.user_name) ASC
    """
    rows = []
    if db.execute_query(sql, (cutoff,)):
        rows = db.cur_hris.fetchall() or []
    out = []
    me = str(current_user_id or '')
    for row in rows:
        item = _serialize_user(row)
        if not item or not item['user_id']:
            continue
        item['online'] = True
        item['is_me'] = item['user_id'] == me
        out.append(item)
    return out


def is_group_member(db, user_id, group_id):
    if not user_id or not group_id:
        return False
    sql = """
        SELECT 1 AS ok
        FROM app_chat_group_member
        WHERE group_id = %s AND user_id = %s
        LIMIT 1
    """
    if not db.execute_query(sql, (group_id, user_id)):
        return False
    return bool(db.cur_hris.fetchone())


def get_group(db, group_id):
    gid = str(group_id or '').strip()
    if not gid:
        return None
    sql = """
        SELECT group_id, group_name, created_by, created_at
        FROM app_chat_group
        WHERE group_id = %s
        LIMIT 1
    """
    if not db.execute_query(sql, (gid,)):
        return None
    return db.cur_hris.fetchone()


def list_group_members(db, current_user_id, group_id):
    uid = str(current_user_id or '').strip()
    gid = str(group_id or '').strip()
    if not uid or not gid:
        return None, 'Grup tidak valid'
    if not is_group_member(db, uid, gid):
        return None, 'Anda bukan anggota grup ini'
    group = get_group(db, gid)
    if not group:
        return None, 'Grup tidak ditemukan'
    created_by = str(_row_get(group, 'created_by') or '')
    sql = """
        SELECT u.user_id, u.user_alias, u.user_name, u.user_mail, u.user_foto, gm.added_at
        FROM app_chat_group_member gm
        INNER JOIN app_users u ON u.user_id = gm.user_id
        WHERE gm.group_id = %s
        ORDER BY COALESCE(u.user_alias, u.user_name) ASC
    """
    rows = []
    if db.execute_query(sql, (gid,)):
        rows = db.cur_hris.fetchall() or []
    online_ids = {u['user_id'] for u in list_online_users(db, uid)}
    members = []
    for row in rows:
        item = _serialize_user(row)
        if not item or not item['user_id']:
            continue
        item['online'] = item['user_id'] in online_ids
        item['is_me'] = item['user_id'] == uid
        item['is_creator'] = item['user_id'] == created_by
        members.append(item)
    members.sort(key=lambda m: (
        0 if m.get('is_me') else 1,
        0 if m.get('online') else 1,
        str(m.get('user_alias') or '').lower(),
    ))
    return {
        'peer': 'group:' + gid,
        'group_id': gid,
        'group_name': str(_row_get(group, 'group_name') or 'Grup'),
        'created_by': created_by,
        'member_count': len(members),
        'members': members,
    }, None


def list_directory_users(db, current_user_id):
    uid = str(current_user_id or '').strip()
    sql = """
        SELECT user_id, user_alias, user_name, user_mail, user_foto
        FROM app_users
        WHERE CAST(COALESCE(user_st, '0') AS CHAR) = '1'
          AND user_id <> %s
        ORDER BY COALESCE(user_alias, user_name) ASC
        LIMIT 300
    """
    rows = []
    if db.execute_query(sql, (uid,)):
        rows = db.cur_hris.fetchall() or []
    online_ids = {u['user_id'] for u in list_online_users(db, uid) if not u.get('is_me')}
    out = []
    for row in rows:
        item = _serialize_user(row)
        if not item or not item['user_id']:
            continue
        item['online'] = item['user_id'] in online_ids
        out.append(item)
    return out


def create_group(db, creator_id, name, member_ids):
    uid = str(creator_id or '').strip()
    title = str(name or '').strip()
    if not uid:
        return None, 'User tidak valid'
    if len(title) < 2:
        return None, 'Nama grup minimal 2 karakter'
    if len(title) > 80:
        return None, 'Nama grup maksimal 80 karakter'
    members = []
    seen = set()
    for raw in (member_ids or []):
        mid = str(raw or '').strip()
        if not mid or mid == uid or mid in seen:
            continue
        if not _user_exists(db, mid):
            continue
        seen.add(mid)
        members.append(mid)
    if not members:
        return None, 'Pilih minimal satu anggota selain Anda'
    group_id = str(uuid.uuid4())
    now = _now()
    if not db.execute_query(
        """
        INSERT INTO app_chat_group (group_id, group_name, created_by, created_at)
        VALUES (%s, %s, %s, %s)
        """,
        (group_id, title, uid, now),
    ):
        return None, getattr(db, 'last_error', None) or 'Gagal membuat grup'
    all_members = [uid] + members
    for member in all_members:
        db.execute_query(
            """
            INSERT IGNORE INTO app_chat_group_member (group_id, user_id, added_at)
            VALUES (%s, %s, %s)
            """,
            (group_id, member, now),
        )
    try:
        db.commit()
    except Exception as e:
        return None, str(e)
    return {
        'peer': 'group:' + group_id,
        'group_id': group_id,
        'group_name': title,
        'member_count': len(all_members),
        'created_by': uid,
    }, None


def list_my_groups(db, user_id, unread_map=None):
    uid = str(user_id or '').strip()
    unread_map = unread_map or {}
    sql = """
        SELECT g.group_id, g.group_name, g.created_by,
               (SELECT COUNT(*) FROM app_chat_group_member gm2 WHERE gm2.group_id = g.group_id) AS member_count
        FROM app_chat_group g
        INNER JOIN app_chat_group_member gm ON gm.group_id = g.group_id AND gm.user_id = %s
        ORDER BY g.group_name ASC
    """
    rows = []
    if db.execute_query(sql, (uid,)):
        rows = db.cur_hris.fetchall() or []
    out = []
    for row in rows:
        gid = str(_row_get(row, 'group_id') or '')
        if not gid:
            continue
        preview = _last_group_preview(db, gid)
        out.append({
            'peer': 'group:' + gid,
            'group_id': gid,
            'group_name': str(_row_get(row, 'group_name') or 'Grup'),
            'member_count': int(_row_get(row, 'member_count') or 0),
            'created_by': str(_row_get(row, 'created_by') or ''),
            'unread': int(unread_map.get(gid) or 0),
            'last_body': (preview or {}).get('last_body') or '',
            'last_at': (preview or {}).get('last_at'),
            'last_from': (preview or {}).get('last_from') or '',
        })
    return out


def _last_group_preview(db, group_id):
    sql = """
        SELECT m.body, m.file_name, m.created_at, u.user_alias AS from_alias
        FROM app_chat_message m
        LEFT JOIN app_users u ON u.user_id = m.from_user_id
        WHERE m.room_type = 'group' AND m.group_id = %s
        ORDER BY m.created_at DESC, m.message_id DESC
        LIMIT 1
    """
    if not db.execute_query(sql, (group_id,)):
        return None
    row = db.cur_hris.fetchone()
    if not row:
        return None
    return {
        'last_body': _preview_body(_row_get(row, 'body'), _row_get(row, 'file_name')),
        'last_at': _fmt_dt(_row_get(row, 'created_at')),
        'last_from': str(_row_get(row, 'from_alias') or ''),
    }


def insert_message(db, from_user_id, peer, body, file_meta=None, reply_to_id=None, forwarded_from_id=None, forwarded_from_alias=None):
    uid = str(from_user_id or '').strip()
    text = str(body or '').strip()
    file_meta = file_meta or {}
    if not uid:
        return None, 'User tidak valid'
    if not text and not file_meta.get('file_path'):
        return None, 'Pesan tidak boleh kosong'
    if len(text) > BODY_MAX:
        return None, f'Pesan maksimal {BODY_MAX} karakter'

    kind, pid = parse_peer(peer)
    group_id = None
    to_user_id = None
    if kind == 'team':
        room_type = TEAM_ROOM
    elif kind == 'group':
        room_type = 'group'
        group_id = pid
        if not is_group_member(db, uid, group_id):
            return None, 'Anda bukan anggota grup ini'
    else:
        room_type = 'direct'
        to_user_id = pid
        if to_user_id == uid:
            return None, 'Tidak bisa mengirim ke diri sendiri'
        if not _user_exists(db, to_user_id):
            return None, 'User tidak ditemukan'

    reply_to_id = str(reply_to_id or '').strip() or None
    reply_meta = None
    if reply_to_id:
        src = get_accessible_message(db, uid, reply_to_id)
        if not src:
            return None, 'Pesan yang dibalas tidak ditemukan'
        if room_key_for(peer_key_from_message(src, uid)) != room_key_for(peer):
            return None, 'Balasan harus di percakapan yang sama'
        reply_meta = src

    message_id = str(uuid.uuid4())
    created_at = _now()
    sql = """
        INSERT INTO app_chat_message (
            message_id, room_type, from_user_id, to_user_id, body, created_at,
            file_name, file_path, file_mime, file_size,
            group_id, reply_to_id, forwarded_from_id, forwarded_from_alias
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    params = (
        message_id,
        room_type,
        uid,
        to_user_id,
        text,
        created_at,
        file_meta.get('file_name') or None,
        file_meta.get('file_path') or None,
        file_meta.get('file_mime') or None,
        file_meta.get('file_size'),
        group_id,
        reply_to_id,
        str(forwarded_from_id or '').strip() or None,
        str(forwarded_from_alias or '').strip() or None,
    )
    if not db.execute_query(sql, params):
        return None, getattr(db, 'last_error', None) or 'Gagal menyimpan pesan'
    try:
        db.commit()
    except Exception as e:
        return None, str(e)
    mark_read(db, uid, peer)
    payload = {
        'message_id': message_id,
        'room_type': room_type,
        'from_user_id': uid,
        'to_user_id': to_user_id,
        'body': text,
        'created_at': created_at,
        'from_alias': None,
        'from_foto': None,
        'file_name': file_meta.get('file_name'),
        'file_path': file_meta.get('file_path'),
        'file_mime': file_meta.get('file_mime'),
        'file_size': file_meta.get('file_size'),
        'group_id': group_id,
        'reply_to_id': reply_to_id,
        'forwarded_from_id': str(forwarded_from_id or '').strip() or None,
        'forwarded_from_alias': str(forwarded_from_alias or '').strip() or None,
    }
    if reply_meta:
        payload['reply_body'] = _row_get(reply_meta, 'body')
        payload['reply_from_alias'] = _lookup_alias(db, _row_get(reply_meta, 'from_user_id'))
        payload['reply_file_name'] = _row_get(reply_meta, 'file_name')
    return serialize_message(payload, uid), None


def _user_exists(db, user_id):
    if not db.execute_query(
        "SELECT user_id FROM app_users WHERE user_id = %s AND CAST(COALESCE(user_st, '0') AS CHAR) = '1' LIMIT 1",
        (user_id,),
    ):
        return False
    return bool(db.cur_hris.fetchone())


def serialize_message(row, current_user_id):
    from_id = str(_row_get(row, 'from_user_id') or '')
    message_id = str(_row_get(row, 'message_id') or '')
    file_name = str(_row_get(row, 'file_name') or '')
    file_path = str(_row_get(row, 'file_path') or '')
    file_mime = str(_row_get(row, 'file_mime') or '')
    has_file = bool(file_path or file_name)
    reply_to_id = str(_row_get(row, 'reply_to_id') or '')
    fwd_alias = str(_row_get(row, 'forwarded_from_alias') or '')
    return {
        'message_id': message_id,
        'room_type': str(_row_get(row, 'room_type') or ''),
        'from_user_id': from_id,
        'to_user_id': str(_row_get(row, 'to_user_id') or '') or None,
        'group_id': str(_row_get(row, 'group_id') or '') or None,
        'body': str(_row_get(row, 'body') or ''),
        'created_at': _fmt_dt(_row_get(row, 'created_at')),
        'from_alias': str(_row_get(row, 'from_alias') or ''),
        'from_foto': str(_row_get(row, 'from_foto') or ''),
        'mine': from_id == str(current_user_id or ''),
        'has_file': has_file,
        'file_name': file_name,
        'file_mime': file_mime,
        'file_size': _row_get(row, 'file_size'),
        'file_is_image': _is_image_file(file_name, file_mime) if has_file else False,
        'file_url': ('/management/admin/chat_file/' + message_id) if has_file else '',
        'reply_to_id': reply_to_id or None,
        'reply_body': str(_row_get(row, 'reply_body') or ''),
        'reply_from_alias': str(_row_get(row, 'reply_from_alias') or ''),
        'reply_file_name': str(_row_get(row, 'reply_file_name') or ''),
        'forwarded': bool(_row_get(row, 'forwarded_from_id') or fwd_alias),
        'forwarded_from_alias': fwd_alias,
    }


def get_accessible_message(db, user_id, message_id):
    uid = str(user_id or '').strip()
    mid = str(message_id or '').strip()
    if not uid or not mid:
        return None
    sql = """
        SELECT message_id, room_type, from_user_id, to_user_id, group_id, body,
               file_name, file_path, file_mime, file_size,
               forwarded_from_id, forwarded_from_alias
        FROM app_chat_message
        WHERE message_id = %s
        LIMIT 1
    """
    if not db.execute_query(sql, (mid,)):
        return None
    row = db.cur_hris.fetchone()
    if not row:
        return None
    room_type = str(_row_get(row, 'room_type') or '')
    from_id = str(_row_get(row, 'from_user_id') or '')
    to_id = str(_row_get(row, 'to_user_id') or '')
    if room_type == TEAM_ROOM:
        return row
    if room_type == 'group':
        gid = str(_row_get(row, 'group_id') or '')
        if is_group_member(db, uid, gid):
            return row
        return None
    if from_id == uid or to_id == uid:
        return row
    return None


def forward_message(db, user_id, message_id, to_peer):
    src = get_accessible_message(db, user_id, message_id)
    if not src:
        return None, 'Pesan tidak ditemukan'
    file_meta = None
    if _row_get(src, 'file_path'):
        file_meta = {
            'file_name': _row_get(src, 'file_name'),
            'file_path': _row_get(src, 'file_path'),
            'file_mime': _row_get(src, 'file_mime'),
            'file_size': _row_get(src, 'file_size'),
        }
    alias = _lookup_alias(db, _row_get(src, 'from_user_id')) or 'User'
    return insert_message(
        db,
        user_id,
        to_peer,
        _row_get(src, 'body') or '',
        file_meta=file_meta,
        forwarded_from_id=_row_get(src, 'message_id'),
        forwarded_from_alias=alias,
    )


def save_chat_upload(uploaded_file):
    import os
    import re
    from django.conf import settings

    if not uploaded_file:
        return None, 'File tidak ditemukan'

    original_name = os.path.basename(str(getattr(uploaded_file, 'name', '') or 'lampiran'))
    original_name = re.sub(r'[^A-Za-z0-9._\-\s()]', '_', original_name).strip() or 'lampiran'
    _, ext = os.path.splitext(original_name)
    ext = (ext or '').lower()
    if ext not in ALLOWED_FILE_EXT:
        return None, 'Tipe file tidak didukung'

    try:
        size = int(getattr(uploaded_file, 'size', 0) or 0)
    except (TypeError, ValueError):
        size = 0
    if size <= 0:
        return None, 'File tidak valid'
    if size > FILE_MAX_BYTES:
        return None, 'Ukuran file maksimal 10MB'

    media_root = str(getattr(settings, 'MEDIA_ROOT', '') or os.path.join(str(settings.BASE_DIR), 'media'))
    now = _now()
    rel_dir = os.path.join('chat', now.strftime('%Y'), now.strftime('%m'))
    abs_dir = os.path.join(media_root, rel_dir)
    os.makedirs(abs_dir, exist_ok=True)
    stored_name = f"{uuid.uuid4().hex}{ext}"
    abs_path = os.path.join(abs_dir, stored_name)
    with open(abs_path, 'wb') as handle:
        for chunk in uploaded_file.chunks():
            handle.write(chunk)
    rel_path = os.path.join(rel_dir, stored_name).replace('\\', '/')
    mime = str(getattr(uploaded_file, 'content_type', '') or '')
    return {
        'file_name': original_name[:255],
        'file_path': rel_path,
        'file_mime': mime[:120],
        'file_size': size,
    }, None


def list_messages(db, current_user_id, peer, after_id=None, limit=MESSAGE_PAGE):
    uid = str(current_user_id or '').strip()
    peer = str(peer or TEAM_ROOM).strip() or TEAM_ROOM
    try:
        limit = max(1, min(int(limit or MESSAGE_PAGE), 200))
    except (TypeError, ValueError):
        limit = MESSAGE_PAGE

    after_id = str(after_id or '').strip()
    after_created = None
    if after_id:
        if db.execute_query(
            "SELECT created_at FROM app_chat_message WHERE message_id = %s LIMIT 1",
            (after_id,),
        ):
            found = db.cur_hris.fetchone()
            after_created = _row_get(found, 'created_at') if found else None
        if after_created is None:
            return []

    kind, pid = parse_peer(peer)
    if kind == 'team':
        where = "m.room_type = 'team'"
        params = []
    elif kind == 'group':
        if not is_group_member(db, uid, pid):
            return []
        where = "m.room_type = 'group' AND m.group_id = %s"
        params = [pid]
    else:
        where = """
            m.room_type = 'direct'
            AND (
                (m.from_user_id = %s AND m.to_user_id = %s)
                OR (m.from_user_id = %s AND m.to_user_id = %s)
            )
        """
        params = [uid, pid, pid, uid]

    if after_id and after_created is not None:
        where += " AND (m.created_at > %s OR (m.created_at = %s AND m.message_id > %s))"
        params.extend([after_created, after_created, after_id])
    elif after_id:
        where += " AND m.message_id <> %s"
        params.append(after_id)

    sql = f"""
        SELECT m.message_id, m.room_type, m.from_user_id, m.to_user_id, m.body, m.created_at,
               m.file_name, m.file_path, m.file_mime, m.file_size, m.group_id,
               m.reply_to_id, m.forwarded_from_id, m.forwarded_from_alias,
               u.user_alias AS from_alias, u.user_foto AS from_foto,
               rm.body AS reply_body, rm.file_name AS reply_file_name,
               ru.user_alias AS reply_from_alias
        FROM app_chat_message m
        LEFT JOIN app_users u ON u.user_id = m.from_user_id
        LEFT JOIN app_chat_message rm ON rm.message_id = m.reply_to_id
        LEFT JOIN app_users ru ON ru.user_id = rm.from_user_id
        WHERE {where}
        ORDER BY m.created_at ASC, m.message_id ASC
        LIMIT {int(limit)}
    """
    if after_id:
        rows = []
        if db.execute_query(sql, tuple(params)):
            rows = db.cur_hris.fetchall() or []
    else:
        inner_sql = f"""
            SELECT m.message_id, m.room_type, m.from_user_id, m.to_user_id, m.body, m.created_at,
                   m.file_name, m.file_path, m.file_mime, m.file_size, m.group_id,
                   m.reply_to_id, m.forwarded_from_id, m.forwarded_from_alias,
                   u.user_alias AS from_alias, u.user_foto AS from_foto,
                   rm.body AS reply_body, rm.file_name AS reply_file_name,
                   ru.user_alias AS reply_from_alias
            FROM app_chat_message m
            LEFT JOIN app_users u ON u.user_id = m.from_user_id
            LEFT JOIN app_chat_message rm ON rm.message_id = m.reply_to_id
            LEFT JOIN app_users ru ON ru.user_id = rm.from_user_id
            WHERE {where}
            ORDER BY m.created_at DESC, m.message_id DESC
            LIMIT {int(limit)}
        """
        rows = []
        if db.execute_query(inner_sql, tuple(params)):
            rows = list(db.cur_hris.fetchall() or [])
            rows.reverse()

    return [serialize_message(row, uid) for row in rows]


def _unread_group_map(db, user_id):
    sql = """
        SELECT m.group_id AS peer_id, COUNT(*) AS n
        FROM app_chat_message m
        INNER JOIN app_chat_group_member gm
          ON gm.group_id = m.group_id AND gm.user_id = %s
        LEFT JOIN app_chat_read r
          ON r.user_id = %s AND r.room_key = CONCAT('group:', m.group_id)
        WHERE m.room_type = 'group'
          AND m.from_user_id <> %s
          AND (r.last_read_at IS NULL OR m.created_at > r.last_read_at)
        GROUP BY m.group_id
    """
    out = {}
    if db.execute_query(sql, (user_id, user_id, user_id)):
        for row in (db.cur_hris.fetchall() or []):
            gid = str(_row_get(row, 'peer_id') or '')
            if not gid:
                continue
            try:
                out[gid] = int(_row_get(row, 'n') or 0)
            except (TypeError, ValueError):
                out[gid] = 0
    return out


def _unread_team(db, user_id):
    sql = """
        SELECT COUNT(*) AS n
        FROM app_chat_message m
        LEFT JOIN app_chat_read r
          ON r.user_id = %s AND r.room_key = %s
        WHERE m.room_type = 'team'
          AND m.from_user_id <> %s
          AND (r.last_read_at IS NULL OR m.created_at > r.last_read_at)
    """
    if not db.execute_query(sql, (user_id, TEAM_ROOM, user_id)):
        return 0
    row = db.cur_hris.fetchone()
    try:
        return int(_row_get(row, 'n') or 0)
    except (TypeError, ValueError):
        return 0


def _unread_direct_map(db, user_id):
    sql = """
        SELECT m.from_user_id AS peer_id, COUNT(*) AS n
        FROM app_chat_message m
        LEFT JOIN app_chat_read r
          ON r.user_id = %s AND r.room_key = CONCAT('direct:', m.from_user_id)
        WHERE m.room_type = 'direct'
          AND m.to_user_id = %s
          AND m.from_user_id <> %s
          AND (r.last_read_at IS NULL OR m.created_at > r.last_read_at)
        GROUP BY m.from_user_id
    """
    out = {}
    if db.execute_query(sql, (user_id, user_id, user_id)):
        for row in (db.cur_hris.fetchall() or []):
            peer = str(_row_get(row, 'peer_id') or '')
            if not peer:
                continue
            try:
                out[peer] = int(_row_get(row, 'n') or 0)
            except (TypeError, ValueError):
                out[peer] = 0
    return out


def _last_team_preview(db):
    sql = """
        SELECT m.body, m.file_name, m.created_at, u.user_alias AS from_alias
        FROM app_chat_message m
        LEFT JOIN app_users u ON u.user_id = m.from_user_id
        WHERE m.room_type = 'team'
        ORDER BY m.created_at DESC, m.message_id DESC
        LIMIT 1
    """
    if not db.execute_query(sql):
        return None
    row = db.cur_hris.fetchone()
    if not row:
        return None
    return {
        'last_body': _preview_body(_row_get(row, 'body'), _row_get(row, 'file_name')),
        'last_at': _fmt_dt(_row_get(row, 'created_at')),
        'last_from': str(_row_get(row, 'from_alias') or ''),
    }


def _recent_direct_conversations(db, user_id, unread_map, online_ids):
    rows = _recent_direct_conversations_fallback(db, user_id)

    out = []
    for row in rows:
        peer_id = str(_row_get(row, 'peer_id') or '')
        if not peer_id or peer_id == user_id:
            continue
        out.append({
            'user_id': peer_id,
            'user_alias': str(_row_get(row, 'user_alias') or _row_get(row, 'user_name') or 'User'),
            'user_name': str(_row_get(row, 'user_name') or ''),
            'user_mail': str(_row_get(row, 'user_mail') or ''),
            'user_foto': str(_row_get(row, 'user_foto') or ''),
            'online': peer_id in online_ids,
            'unread': int(unread_map.get(peer_id) or 0),
            'last_body': _preview_body(_row_get(row, 'body'), _row_get(row, 'file_name')),
            'last_at': _fmt_dt(_row_get(row, 'created_at')),
        })
    return out


def _recent_direct_conversations_fallback(db, user_id):
    sql = """
        SELECT
            CASE WHEN m.from_user_id = %s THEN m.to_user_id ELSE m.from_user_id END AS peer_id,
            m.body,
            m.file_name,
            m.created_at,
            u.user_alias,
            u.user_name,
            u.user_mail,
            u.user_foto
        FROM app_chat_message m
        INNER JOIN app_users u ON u.user_id = CASE
            WHEN m.from_user_id = %s THEN m.to_user_id ELSE m.from_user_id
        END
        WHERE m.room_type = 'direct'
          AND (m.from_user_id = %s OR m.to_user_id = %s)
        ORDER BY m.created_at DESC, m.message_id DESC
        LIMIT 200
    """
    rows = []
    if db.execute_query(sql, (user_id, user_id, user_id, user_id)):
        rows = db.cur_hris.fetchall() or []
    seen = set()
    out = []
    for row in rows:
        peer_id = str(_row_get(row, 'peer_id') or '')
        if not peer_id or peer_id in seen:
            continue
        seen.add(peer_id)
        out.append(row)
        if len(out) >= 40:
            break
    return out


def snapshot(db, current_user_id):
    uid = str(current_user_id or '').strip()
    online = list_online_users(db, uid)
    online_ids = {u['user_id'] for u in online if not u.get('is_me')}
    unread_map = _unread_direct_map(db, uid)
    unread_group_map = _unread_group_map(db, uid)
    team_unread = _unread_team(db, uid)
    conversations = _recent_direct_conversations(db, uid, unread_map, online_ids)
    groups = list_my_groups(db, uid, unread_group_map)

    conv_ids = {c['user_id'] for c in conversations}
    for user in online:
        if user.get('is_me'):
            continue
        if user['user_id'] in conv_ids:
            continue
        conversations.append({
            'user_id': user['user_id'],
            'user_alias': user['user_alias'],
            'user_name': user['user_name'],
            'user_mail': user['user_mail'],
            'user_foto': user['user_foto'],
            'online': True,
            'unread': int(unread_map.get(user['user_id']) or 0),
            'last_body': '',
            'last_at': None,
        })

    conversations.sort(key=lambda c: (
        0 if c.get('online') else 1,
        -int(c.get('unread') or 0),
        str(c.get('user_alias') or '').lower(),
    ))

    unread_direct = sum(int(c.get('unread') or 0) for c in conversations)
    unread_groups = sum(int(g.get('unread') or 0) for g in groups)
    others_online = [u for u in online if not u.get('is_me')]
    return {
        'online_count': len(others_online) + 1,
        'online': others_online,
        'conversations': conversations,
        'groups': groups,
        'team': {
            'unread': team_unread,
            **(_last_team_preview(db) or {'last_body': '', 'last_at': None, 'last_from': ''}),
        },
        'unread_total': int(team_unread) + int(unread_direct) + int(unread_groups),
    }

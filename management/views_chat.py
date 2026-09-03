import json
import os

from django.conf import settings
from django.http import FileResponse, Http404, JsonResponse
from django.views import View

from . import chat as chat_db
from .database import data_mysql


def _session_user(request):
    admin = request.session.get('hris_admin') or {}
    user_id = str(admin.get('user_id') or '').strip()
    if not user_id:
        return None
    return admin


def _request_payload(request):
    content_type = (request.content_type or '').lower()
    if 'application/json' in content_type:
        try:
            data = json.loads(request.body.decode('utf-8') or '{}')
            return data if isinstance(data, dict) else {}
        except Exception:
            return {}
    return request.POST


def _unauthorized():
    return JsonResponse({'status': False, 'error': 'Unauthorized'}, status=401)


class ChatHeartbeatView(View):
    def post(self, request):
        user = _session_user(request)
        if not user:
            return _unauthorized()
        db = data_mysql()
        try:
            err = chat_db.ensure_chat_tables(db)
            if err:
                return JsonResponse({'status': False, 'error': err}, status=500)
            chat_db.upsert_presence(db, user['user_id'])
            incoming = _request_payload(request)
            viewing = str(incoming.get('viewing') or '').strip()
            if viewing:
                chat_db.mark_read(db, user['user_id'], viewing)
            payload = chat_db.snapshot(db, user['user_id'])
            payload['me'] = {
                'user_id': user.get('user_id'),
                'user_alias': user.get('user_alias') or user.get('user_name') or 'Saya',
            }
            if viewing:
                payload.update(chat_db.get_peer_receipt(db, user['user_id'], viewing))
            return JsonResponse({'status': True, **payload})
        except Exception as e:
            return JsonResponse({'status': False, 'error': str(e)}, status=500)
        finally:
            db.close()


class ChatMessagesView(View):
    def get(self, request):
        user = _session_user(request)
        if not user:
            return _unauthorized()
        peer = str(request.GET.get('peer') or chat_db.TEAM_ROOM).strip() or chat_db.TEAM_ROOM
        after = str(request.GET.get('after') or '').strip()
        db = data_mysql()
        try:
            err = chat_db.ensure_chat_tables(db)
            if err:
                return JsonResponse({'status': False, 'error': err}, status=500)
            messages = chat_db.list_messages(db, user['user_id'], peer, after_id=after)
            chat_db.mark_read(db, user['user_id'], peer)
            receipt = chat_db.get_peer_receipt(db, user['user_id'], peer)
            return JsonResponse({'status': True, 'peer': peer, 'messages': messages, **receipt})
        except Exception as e:
            return JsonResponse({'status': False, 'error': str(e)}, status=500)
        finally:
            db.close()


class ChatSendView(View):
    def post(self, request):
        user = _session_user(request)
        if not user:
            return _unauthorized()
        uploaded = None
        try:
            uploaded = request.FILES.get('file')
        except Exception:
            uploaded = None
        if uploaded:
            payload = request.POST
        else:
            payload = _request_payload(request)
        peer = str(payload.get('peer') or chat_db.TEAM_ROOM).strip() or chat_db.TEAM_ROOM
        body = payload.get('body') or ''
        file_meta = None
        if uploaded:
            file_meta, file_error = chat_db.save_chat_upload(uploaded)
            if file_error:
                return JsonResponse({'status': False, 'error': file_error}, status=400)
        db = data_mysql()
        try:
            err = chat_db.ensure_chat_tables(db)
            if err:
                return JsonResponse({'status': False, 'error': err}, status=500)
            chat_db.upsert_presence(db, user['user_id'])
            reply_to = str(payload.get('reply_to') or payload.get('reply_to_id') or '').strip() or None
            message, error = chat_db.insert_message(
                db,
                user['user_id'],
                peer,
                body,
                file_meta=file_meta,
                reply_to_id=reply_to,
            )
            if error:
                return JsonResponse({'status': False, 'error': error}, status=400)
            message['from_alias'] = user.get('user_alias') or user.get('user_name') or 'Saya'
            message['mine'] = True
            return JsonResponse({'status': True, 'message': message})
        except Exception as e:
            return JsonResponse({'status': False, 'error': str(e)}, status=500)
        finally:
            db.close()


class ChatReadView(View):
    def post(self, request):
        user = _session_user(request)
        if not user:
            return _unauthorized()
        payload = _request_payload(request)
        peer = str(payload.get('peer') or chat_db.TEAM_ROOM).strip() or chat_db.TEAM_ROOM
        db = data_mysql()
        try:
            err = chat_db.ensure_chat_tables(db)
            if err:
                return JsonResponse({'status': False, 'error': err}, status=500)
            chat_db.mark_read(db, user['user_id'], peer)
            receipt = chat_db.get_peer_receipt(db, user['user_id'], peer)
            return JsonResponse({'status': True, **receipt})
        except Exception as e:
            return JsonResponse({'status': False, 'error': str(e)}, status=500)
        finally:
            db.close()


class ChatFileView(View):
    def get(self, request, message_id):
        user = _session_user(request)
        if not user:
            return _unauthorized()
        db = data_mysql()
        try:
            err = chat_db.ensure_chat_tables(db)
            if err:
                raise Http404()
            row = chat_db.get_accessible_message(db, user['user_id'], message_id)
            if not row:
                raise Http404()
            rel_path = str(row.get('file_path') or '').replace('\\', '/').lstrip('/')
            if not rel_path or '..' in rel_path.split('/'):
                raise Http404()
            media_root = str(getattr(settings, 'MEDIA_ROOT', '') or '')
            abs_path = os.path.abspath(os.path.join(media_root, rel_path))
            chat_root = os.path.abspath(os.path.join(media_root, 'chat'))
            if not abs_path.startswith(chat_root + os.sep) or not os.path.isfile(abs_path):
                raise Http404()
            filename = str(row.get('file_name') or os.path.basename(abs_path))
            mime = str(row.get('file_mime') or '')
            as_attachment = not (mime.startswith('image/') or mime in ('application/pdf',))
            handle = open(abs_path, 'rb')
            response = FileResponse(handle, as_attachment=as_attachment, filename=filename)
            if mime:
                response['Content-Type'] = mime
            return response
        except Http404:
            raise
        except Exception:
            raise Http404()
        finally:
            db.close()


class ChatDirectoryView(View):
    def get(self, request):
        user = _session_user(request)
        if not user:
            return _unauthorized()
        db = data_mysql()
        try:
            err = chat_db.ensure_chat_tables(db)
            if err:
                return JsonResponse({'status': False, 'error': err}, status=500)
            users = chat_db.list_directory_users(db, user['user_id'])
            groups = chat_db.list_my_groups(db, user['user_id'])
            return JsonResponse({'status': True, 'users': users, 'groups': groups})
        except Exception as e:
            return JsonResponse({'status': False, 'error': str(e)}, status=500)
        finally:
            db.close()


class ChatCreateGroupView(View):
    def post(self, request):
        user = _session_user(request)
        if not user:
            return _unauthorized()
        payload = _request_payload(request)
        name = payload.get('name') or payload.get('group_name') or ''
        members = payload.get('members') or payload.get('member_ids') or []
        if isinstance(members, str):
            members = [m.strip() for m in members.split(',') if m.strip()]
        db = data_mysql()
        try:
            err = chat_db.ensure_chat_tables(db)
            if err:
                return JsonResponse({'status': False, 'error': err}, status=500)
            group, error = chat_db.create_group(db, user['user_id'], name, members)
            if error:
                return JsonResponse({'status': False, 'error': error}, status=400)
            return JsonResponse({'status': True, 'group': group})
        except Exception as e:
            return JsonResponse({'status': False, 'error': str(e)}, status=500)
        finally:
            db.close()


class ChatGroupMembersView(View):
    def get(self, request):
        user = _session_user(request)
        if not user:
            return _unauthorized()
        peer = str(request.GET.get('peer') or request.GET.get('group_id') or '').strip()
        kind, gid = chat_db.parse_peer(peer)
        if kind != 'group' or not gid:
            return JsonResponse({'status': False, 'error': 'Grup tidak valid'}, status=400)
        db = data_mysql()
        try:
            err = chat_db.ensure_chat_tables(db)
            if err:
                return JsonResponse({'status': False, 'error': err}, status=500)
            info, error = chat_db.list_group_members(db, user['user_id'], gid)
            if error:
                return JsonResponse({'status': False, 'error': error}, status=400)
            return JsonResponse({'status': True, **info})
        except Exception as e:
            return JsonResponse({'status': False, 'error': str(e)}, status=500)
        finally:
            db.close()


class ChatForwardView(View):
    def post(self, request):
        user = _session_user(request)
        if not user:
            return _unauthorized()
        payload = _request_payload(request)
        message_id = str(payload.get('message_id') or '').strip()
        peer = str(payload.get('peer') or '').strip()
        if not message_id or not peer:
            return JsonResponse({'status': False, 'error': 'Tujuan teruskan tidak valid'}, status=400)
        db = data_mysql()
        try:
            err = chat_db.ensure_chat_tables(db)
            if err:
                return JsonResponse({'status': False, 'error': err}, status=500)
            chat_db.upsert_presence(db, user['user_id'])
            message, error = chat_db.forward_message(db, user['user_id'], message_id, peer)
            if error:
                return JsonResponse({'status': False, 'error': error}, status=400)
            message['from_alias'] = user.get('user_alias') or user.get('user_name') or 'Saya'
            message['mine'] = True
            return JsonResponse({'status': True, 'peer': peer, 'message': message})
        except Exception as e:
            return JsonResponse({'status': False, 'error': str(e)}, status=500)
        finally:
            db.close()

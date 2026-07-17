#!/usr/bin/env python3
"""
Partner BM — webhook receiver untuk integrasi token Facebook HRIS (Opsi 1).

HRIS mengirim POST webhook → service ini terima → partner isi token Facebook →
service otomatis POST balik ke HRIS submit-token API.

Environment:
  PARTNER_WEBHOOK_SECRET   = sama dengan FACEBOOK_PARTNER_WEBHOOK_SECRET di HRIS .env
  HRIS_PARTNER_API_KEY     = sama dengan FACEBOOK_PARTNER_API_KEY di HRIS .env
  PARTNER_FB_ACCESS_TOKEN  = token Facebook EAAG... (dari Graph API Explorer)
  LISTEN_HOST              = default 0.0.0.0
  LISTEN_PORT              = default 8787
  WEBHOOK_PATH             = default /hris/facebook-token-request

Jalankan:
  export PARTNER_WEBHOOK_SECRET=...
  export HRIS_PARTNER_API_KEY=...
  export PARTNER_FB_ACCESS_TOKEN=EAAG...
  python3 scripts/facebook_partner_webhook_server.py

Set di HRIS .env:
  FACEBOOK_PARTNER_WEBHOOK_URL=http://IP_PARTNER:8787/hris/facebook-token-request
  FACEBOOK_PARTNER_WEBHOOK_SECRET=...
  FACEBOOK_PARTNER_API_KEY=...
"""
from __future__ import annotations

import json
import os
import sys
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib import error, request


LISTEN_HOST = os.getenv('LISTEN_HOST', '0.0.0.0')
LISTEN_PORT = int(os.getenv('LISTEN_PORT', '8787'))
WEBHOOK_PATH = os.getenv('WEBHOOK_PATH', '/hris/facebook-token-request')
PARTNER_WEBHOOK_SECRET = os.getenv('PARTNER_WEBHOOK_SECRET', '').strip()
HRIS_PARTNER_API_KEY = os.getenv('HRIS_PARTNER_API_KEY', '').strip()
PARTNER_FB_ACCESS_TOKEN = os.getenv('PARTNER_FB_ACCESS_TOKEN', '').strip()


def submit_token_to_hris(submit_url: str, request_token: str, access_token: str, submitted_by: str = 'partner-bm-webhook'):
    payload = json.dumps({
        'request_token': request_token,
        'access_token': access_token,
        'submitted_by': submitted_by,
    }).encode('utf-8')
    headers = {
        'Content-Type': 'application/json',
        'User-Agent': 'Partner-BM-Webhook/1.0',
    }
    if HRIS_PARTNER_API_KEY:
        headers['X-API-Key'] = HRIS_PARTNER_API_KEY
    req = request.Request(submit_url, data=payload, headers=headers, method='POST')
    with request.urlopen(req, timeout=30) as resp:
        body = resp.read().decode('utf-8', errors='replace')
        return resp.status, body


class PartnerWebhookHandler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):
        sys.stdout.write('[partner-webhook] ' + (fmt % args) + '\n')
        sys.stdout.flush()

    def _read_json(self):
        length = int(self.headers.get('Content-Length') or 0)
        raw = self.rfile.read(length).decode('utf-8') if length else '{}'
        try:
            return json.loads(raw or '{}')
        except json.JSONDecodeError:
            return None

    def _json_response(self, code, payload):
        data = json.dumps(payload).encode('utf-8')
        self.send_response(code)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Content-Length', str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self):
        if self.path.rstrip('/') in ('', '/health', WEBHOOK_PATH.rstrip('/'), WEBHOOK_PATH):
            self._json_response(200, {
                'status': True,
                'service': 'partner-bm-facebook-webhook',
                'webhook_path': WEBHOOK_PATH,
                'fb_token_configured': bool(PARTNER_FB_ACCESS_TOKEN),
            })
            return
        self._json_response(404, {'status': False, 'message': 'Not found'})

    def do_POST(self):
        if self.path != WEBHOOK_PATH:
            self._json_response(404, {'status': False, 'message': 'Not found'})
            return

        if PARTNER_WEBHOOK_SECRET:
            incoming = str(self.headers.get('X-Webhook-Secret') or '').strip()
            if incoming != PARTNER_WEBHOOK_SECRET:
                self._json_response(401, {'status': False, 'message': 'Invalid webhook secret'})
                return

        payload = self._read_json()
        if not isinstance(payload, dict):
            self._json_response(400, {'status': False, 'message': 'Invalid JSON body'})
            return

        if payload.get('event') != 'facebook_token_request':
            self._json_response(400, {'status': False, 'message': 'Unsupported event'})
            return

        submit_url = str(payload.get('submit_url') or '').strip()
        request_token = str(payload.get('request_token') or '').strip()
        account_id = str(payload.get('account_id') or '').strip()
        account_name = str(payload.get('account_name') or '').strip()

        if not submit_url or not request_token:
            self._json_response(400, {'status': False, 'message': 'submit_url and request_token required'})
            return

        if not PARTNER_FB_ACCESS_TOKEN:
            self.log_message(
                'Token request received for %s (%s) — set PARTNER_FB_ACCESS_TOKEN then retry or POST manually to %s',
                account_name or account_id,
                account_id,
                submit_url,
            )
            self._json_response(202, {
                'status': True,
                'message': 'Webhook diterima. Set PARTNER_FB_ACCESS_TOKEN lalu kirim ulang permintaan dari HRIS, '
                           'atau POST access_token manual ke submit_url.',
                'account_id': account_id,
                'account_name': account_name,
            })
            return

        if not PARTNER_FB_ACCESS_TOKEN.startswith('EAA'):
            self._json_response(500, {
                'status': False,
                'message': 'PARTNER_FB_ACCESS_TOKEN harus token Facebook (EAAG...), bukan request_token HRIS.',
            })
            return

        try:
            status, body = submit_token_to_hris(submit_url, request_token, PARTNER_FB_ACCESS_TOKEN)
            self.log_message('HRIS submit-token HTTP %s for %s: %s', status, account_id, body[:200])
            try:
                parsed = json.loads(body)
            except json.JSONDecodeError:
                parsed = {'raw': body}
            self._json_response(200 if 200 <= status < 300 else 502, {
                'status': 200 <= status < 300,
                'message': 'Token dikirim ke HRIS.',
                'hris_status_code': status,
                'hris_response': parsed,
            })
        except error.HTTPError as exc:
            err_body = exc.read().decode('utf-8', errors='replace')
            self.log_message('HRIS submit-token failed HTTP %s: %s', exc.code, err_body[:300])
            self._json_response(502, {
                'status': False,
                'message': f'HRIS submit-token gagal (HTTP {exc.code})',
                'hris_response': err_body,
            })
        except Exception as exc:
            self.log_message('HRIS submit-token error: %s', exc)
            self._json_response(502, {'status': False, 'message': str(exc)})


def main():
    if not HRIS_PARTNER_API_KEY:
        print('WARNING: HRIS_PARTNER_API_KEY belum diset — submit ke HRIS mungkin ditolak.', file=sys.stderr)
    server = HTTPServer((LISTEN_HOST, LISTEN_PORT), PartnerWebhookHandler)
    print(f'Partner BM webhook listening on http://{LISTEN_HOST}:{LISTEN_PORT}{WEBHOOK_PATH}')
    print('Health check: GET /health')
    if PARTNER_FB_ACCESS_TOKEN:
        print('PARTNER_FB_ACCESS_TOKEN: configured (EAAG...)')
    else:
        print('PARTNER_FB_ACCESS_TOKEN: NOT SET — webhook akan terima request tapi belum auto-submit ke HRIS')
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print('\nStopped.')


if __name__ == '__main__':
    main()

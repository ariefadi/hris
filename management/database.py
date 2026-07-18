from ast import If
from typing import Any
from google.auth import credentials
import pymysql.cursors
import json
import os
import re
from django.conf import settings
from datetime import datetime, timedelta
from google_auth_oauthlib.flow import InstalledAppFlow
from random import sample
from argon2 import PasswordHasher, exceptions as argon2_exceptions
from .crypto import sandi

try:
    from dotenv import load_dotenv
except Exception:
    load_dotenv = None

try:
    import requests
except Exception:
    requests = None

_ENV_LOADED = False

# ...existing code...
import pandas as pd
clickhouse_connect_import_error = None
try:
    import clickhouse_connect
except Exception as e:
    clickhouse_connect = None
    clickhouse_connect_import_error = e
from django.db import connection
# ...existing code...

_clickhouse_client = None

def _clickhouse_http_config():
    _ensure_env_loaded()
    host = getattr(settings, 'CH_HOST', None) or os.getenv('CH_HOST') or os.getenv('REPORT_DB_HOST') or os.getenv('DB_REPORT_HOST') or '127.0.0.1'
    raw_port = str(getattr(settings, 'CH_PORT', '') or os.getenv('CH_PORT') or os.getenv('REPORT_DB_PORT') or os.getenv('DB_REPORT_PORT') or '8123').strip()
    try:
        port = int(raw_port)
    except (ValueError, TypeError):
        port = 8123
    user = getattr(settings, 'CH_USER', None) or os.getenv('CH_USER') or os.getenv('REPORT_DB_USER') or os.getenv('DB_REPORT_USER') or 'default'
    password = getattr(settings, 'CH_PASSWORD', None) or os.getenv('CH_PASSWORD') or os.getenv('REPORT_DB_PASSWORD') or os.getenv('DB_REPORT_PASSWORD') or 'hris123456'
    database = getattr(settings, 'CH_DB', None) or os.getenv('CH_DB') or os.getenv('REPORT_DB_NAME') or os.getenv('DB_REPORT_NAME') or 'hris_trendHorizone'
    return host, port, user, password, database

def _clickhouse_http_post(sql_text: str, timeout: int = 120):
    if requests is None:
        raise RuntimeError('The requests library is not installed')
    host, port, user, password, database = _clickhouse_http_config()
    base = f"http://{host}:{port}/"
    params = {}
    if database:
        params['database'] = database
    auth = None
    if user and password is not None:
        auth = (user, password or '')
    elif user:
        auth = (user, '')
    resp = requests.post(
        base,
        params=params,
        auth=auth,
        data=str(sql_text).encode('utf-8'),
        timeout=timeout,
    )
    resp.raise_for_status()
    return resp

def get_clickhouse_client():
    global _clickhouse_client
    if clickhouse_connect is None:
        if clickhouse_connect_import_error is not None:
            raise clickhouse_connect_import_error
        raise ModuleNotFoundError("clickhouse_connect")
    if _clickhouse_client is None:
        _clickhouse_client = clickhouse_connect.get_client(
            host=getattr(settings, 'CH_HOST', '127.0.0.1'),
            port=getattr(settings, 'CH_PORT', 8123),
            username=getattr(settings, 'CH_USER', 'default'),
            password=getattr(settings, 'CH_PASSWORD', 'hris123456'),
            database=getattr(settings, 'CH_DB', 'hris_trendHorizone'),
        )
    return _clickhouse_client

def query_df(sql: str) -> pd.DataFrame:
    if clickhouse_connect is not None:
        client = get_clickhouse_client()
        return client.query_df(sql)
    if requests is None:
        if clickhouse_connect_import_error is not None:
            raise clickhouse_connect_import_error
        raise ModuleNotFoundError("clickhouse_connect")
    host, port, user, password, database = _clickhouse_http_config()
    cur = ClickHouseHttpCursor(host=host, port=port, user=user, password=password, database=database)
    cur.execute(sql)
    return pd.DataFrame(cur.fetchall())

def insert_df(table: str, df: pd.DataFrame) -> None:
    import pandas as pd
    import json
    for col in df.columns:
        # Tangani semua tipe datetime pandas (termasuk timezone-aware)
        if pd.api.types.is_datetime64_any_dtype(df[col]):
            df[col] = df[col].dt.strftime('%Y-%m-%d %H:%M:%S')
        elif len(df) > 0 and isinstance(df[col].iloc[0], pd.Timestamp):
            df[col] = df[col].astype(str)
        # Tangani kolom list/dict agar jadi string JSON
        if df[col].apply(lambda x: isinstance(x, (list, dict))).any():
            df.loc[:, col] = df[col].apply(lambda x: json.dumps(x, ensure_ascii=False) if isinstance(x, (list, dict)) else x)

    if df is None or df.empty:
        return
    if clickhouse_connect is not None:
        client = get_clickhouse_client()
        client.insert_df(table, df)
        return
    if requests is None:
        if clickhouse_connect_import_error is not None:
            raise clickhouse_connect_import_error
        raise ModuleNotFoundError("clickhouse_connect")
    records = df.to_dict(orient='records')
    raw_chunk = str(os.getenv('CH_INSERT_BATCH_SIZE', '5000') or '5000').strip()
    try:
        chunk_size = int(raw_chunk)
    except (ValueError, TypeError):
        chunk_size = 5000
    if chunk_size <= 0:
        chunk_size = 5000
    for i in range(0, len(records), chunk_size):
        chunk = records[i:i + chunk_size]
        lines = [json.dumps(r, ensure_ascii=False, default=str) for r in chunk]
        payload = f"INSERT INTO {table} FORMAT JSONEachRow\n" + "\n".join(lines) + "\n"
        _clickhouse_http_post(payload)

def query_mysql_df(sql: str, params=None) -> pd.DataFrame:
    with connection.cursor() as cursor:
        cursor.execute(sql, params or [])
        columns = [col[0] for col in cursor.description] if cursor.description else []
        rows = cursor.fetchall()
    return pd.DataFrame(rows, columns=columns)
# ...existing code...


def _ensure_env_loaded():
    global _ENV_LOADED
    if _ENV_LOADED:
        return
    _ENV_LOADED = True
    if load_dotenv is None:
        return
    try:
        root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
        env_path = os.path.join(root, '.env')
        if os.path.exists(env_path):
            load_dotenv(env_path, override=False)
    except Exception:
        pass
def _log_debug(message):
    try:
        with open('/tmp/hris_login_debug.log', 'a') as f:
            f.write(str(message) + '\n')
    except Exception:
        pass

def run_sql(sql):
    print(json.dumps(sql, indent=2, sort_keys=True))

class ClickHouseHttpCursor:
    def __init__(self, host, port=8123, user='default', password='', database='', timeout=30):
        self.host = host
        self.port = int(port) if port else 8123
        self.user = user or 'default'
        self.password = password or ''
        self.database = database or ''
        self.timeout = timeout
        self._rows = []

    def close(self):
        self._rows = []

    @property
    def rowcount(self):
        try:
            return len(self._rows or [])
        except Exception:
            return 0

    def _escape(self, v):
        if v is None:
            return 'NULL'
        if isinstance(v, bool):
            return '1' if v else '0'
        if isinstance(v, (int, float)):
            return str(v)
        if isinstance(v, (datetime,)):
            return "'" + v.strftime('%Y-%m-%d %H:%M:%S').replace("'", "''") + "'"
        s = str(v)
        return "'" + s.replace("'", "''") + "'"

    def _substitute_params(self, query, params):
        if not params:
            return query
        if not isinstance(params, (list, tuple)):
            params = (params,)
        parts = query.split('%s')
        if len(parts) == 1:
            return query
        out = [parts[0]]
        for i in range(1, len(parts)):
            pv = params[i - 1] if i - 1 < len(params) else None
            out.append(self._escape(pv))
            out.append(parts[i])
        return ''.join(out)

    def _normalize_sql(self, query):
        q = str(query or '')
        q = q.replace('`', '')
        q = re.sub(r"\bAS\s+'([A-Za-z_][A-Za-z0-9_]*)'", r"AS \1", q, flags=re.IGNORECASE)
        q = re.sub(r"\bDATE\s*\(", "toDate(", q, flags=re.IGNORECASE)
        q = re.sub(r"\bHOUR\s*\(", "toHour(", q, flags=re.IGNORECASE)
        q = re.sub(r"\bNOW\s*\(\s*\)", "now()", q, flags=re.IGNORECASE)
        return q

    def execute(self, query, params=None):
        if requests is None:
            raise RuntimeError('The requests library is not installed')
        q = self._normalize_sql(query)
        q = self._substitute_params(q, params)
        q = q.strip().rstrip(';')
        if ' format ' not in q.lower():
            q = q + '\nFORMAT JSON'

        base = f"http://{self.host}:{self.port}/"
        http_params = {}
        if self.database:
            http_params['database'] = self.database

        auth = None
        if self.user and self.password:
            auth = (self.user, self.password)
        elif self.user:
            auth = (self.user, '')

        resp = requests.post(
            base,
            params=http_params,
            auth=auth,
            data=q.encode('utf-8'),
            timeout=self.timeout,
        )
        resp.raise_for_status()
        js = resp.json()
        self._rows = js.get('data') or []
        return True

    def fetchall(self):
        return list(self._rows or [])

    def fetchone(self):
        rows = self._rows or []
        return rows[0] if rows else None

class data_mysql:
    
    def __init__(self):
        self.db_hris = None
        self.mysql_cur = None
        self.report_cur = None
        self.cur_hris = None
        self.last_error = None
        self.connect()

    def _report_engine(self):
        return str(os.getenv('REPORT_DB_ENGINE', '') or os.getenv('DB_REPORT_ENGINE', '') or '').strip().lower()

    @staticmethod
    def _normalize_fb_account_key(value):
        s = str(value or '').strip().lower()
        if s.startswith('act_'):
            s = s[4:]
        return s

    def _report_tables(self):
        raw = str(os.getenv('REPORT_DB_TABLES', '') or os.getenv('DB_REPORT_TABLES', '') or '').strip()
        if raw:
            tables = [t.strip().lower() for t in raw.split(',') if t.strip()]
            if tables:
                return tables
        return [
            'data_adsense_country',
            'data_adsense_domain',
            'data_adsense_rekap',
            'data_adx_country',
            'data_adx_domain',
            'data_adx_rekap',
            'data_ads_campaign',
            'data_ads_rekap',
            'data_ads_country',
            'log_ads_country',
            'log_adsense_country',
            'log_adx_country',
            'master_account_ads',
            'master_ads'
        ]

    def _extract_query_tables(self, query):
        q = str(query or '')
        hits = re.findall(r'\b(?:from|join)\s+`?([A-Za-z0-9_.]+)`?', q, flags=re.IGNORECASE)
        out = []
        for h in (hits or []):
            t = str(h or '')
            if '.' in t:
                t = t.split('.')[-1]
            t = t.strip().strip('`"').lower()
            if t and t not in out:
                out.append(t)
        return out

    def _should_use_report(self, query):
        engine = self._report_engine()
        if engine not in ('clickhouse', 'ch'):
            return False
        q = str(query or '').lstrip()
        if not q.lower().startswith('select'):
            return False
        tables = self._extract_query_tables(q)
        if not tables:
            return False
        report_tables = set(self._report_tables())
        if any((t not in report_tables) for t in tables):
            return False
        return True

    def _ensure_report_connection(self):
        if self.report_cur:
            return True
        if requests is None:
            raise RuntimeError('The requests library is not installed')
        _ensure_env_loaded()
        host = os.getenv('CH_HOST') or os.getenv('REPORT_DB_HOST') or os.getenv('DB_REPORT_HOST') or os.getenv('DB_HOST') or os.getenv('HRIS_DB_HOST') or '127.0.0.1'
        raw_port = (os.getenv('CH_PORT') or os.getenv('REPORT_DB_PORT') or os.getenv('DB_REPORT_PORT') or '8123').strip()
        try:
            port = int(raw_port)
        except (ValueError, TypeError):
            port = 8123
        user = os.getenv('CH_USER') or os.getenv('REPORT_DB_USER') or os.getenv('DB_REPORT_USER') or 'default'
        password = os.getenv('CH_PASSWORD') or os.getenv('REPORT_DB_PASSWORD') or os.getenv('DB_REPORT_PASSWORD') or 'hris123456'
        database = os.getenv('CH_DB') or os.getenv('REPORT_DB_NAME') or os.getenv('DB_REPORT_NAME') or os.getenv('DB_NAME') or os.getenv('HRIS_DB_NAME') or 'hris_trendHorizone'
        self.report_cur = ClickHouseHttpCursor(host=host, port=port, user=user, password=password, database=database)
        return True

    def connect(self):
        """Membuat koneksi baru ke database"""
        try:
            _ensure_env_loaded()
            host = os.getenv('DB_HOST') or '127.0.0.1'
            raw_port = (os.getenv('DB_PORT') or '').strip()
            if not raw_port:
                raw_port = '3306'
            try:
                port = int(raw_port)
            except (ValueError, TypeError):
                print(f"Invalid HRIS_DB_PORT value '{raw_port}', defaulting to 3306")
                port = 3306
            user = os.getenv('DB_USER') or 'root'
            password = os.getenv('DB_PASSWORD') or 'hris123456'
            database = os.getenv('DB_NAME') or 'hris_trendHorizone'

            self.db_hris = pymysql.connect(
                host=host,
                port=port,
                user=user,
                password=password,
                database=database,
                cursorclass=pymysql.cursors.DictCursor
            )
            self.mysql_cur = self.db_hris.cursor()
            self.cur_hris = self.mysql_cur
            return True
        except pymysql.Error as e:
            print(f"Error connecting to database: {e}")
            return False
            

    def ensure_connection(self):
        """Memastikan koneksi database masih aktif"""
        try:
            self.db_hris.ping(reconnect=True)
        except (pymysql.Error, AttributeError):
            return self.connect()
        return True

    def close(self):
        """Menutup koneksi database"""
        try:
            if self.mysql_cur:
                try:
                    self.mysql_cur.close()
                except Exception:
                    pass
            if self.report_cur:
                try:
                    self.report_cur.close()
                except Exception:
                    pass
            if self.db_hris:
                self.db_hris.close()
        except Exception:
            pass

    def __del__(self):
        """Destructor untuk memastikan koneksi ditutup"""
        self.close()

    def execute_query(self, query, params=None):
        """
        Mengeksekusi query dengan penanganan koneksi yang lebih baik
        """
        try:
            if self._should_use_report(query):
                self._ensure_report_connection()
                self.cur_hris = self.report_cur
                self.cur_hris.execute(query, params)
                return True

            if not self.ensure_connection():
                raise pymysql.Error("Could not establish database connection")

            self.cur_hris = self.mysql_cur
            self.cur_hris.execute(query, params)
            return True
        except Exception as e:
            self.last_error = str(e)
            print(f"Database error: {e}")
            return False

    def commit(self):
        """Commit transaksi dengan penanganan error"""
        try:
            if self.db_hris:
                self.db_hris.commit()
            return True
        except pymysql.Error as e:
            print(f"Error committing transaction: {e}")
            return False

    def fetch_all(self):
        """Mengambil semua baris dari cursor aktif sebagai list of dicts"""
        try:
            if self.cur_hris:
                return self.cur_hris.fetchall()
            return []
        except pymysql.Error as e:
            print(f"Error fetching rows: {e}")
            return []

    def login_admin(self, data):
        # Fetch user by username, then verify password (Argon2 with legacy fallbacks)
        sql = """
                SELECT * FROM `app_users` 
                WHERE `user_name`=%s
                LIMIT 1
              """
        try:
            _log_debug(f"[LOGIN_DEBUG] Attempting login for username={data.get('username')} from DB host={os.getenv('DB_HOST','127.0.0.1')} port={os.getenv('HRIS_DB_PORT','3306')} db={os.getenv('HRIS_DB_NAME','hris_trendHorizone')}")
            if not self.execute_query(sql, (data['username'],)):
                raise pymysql.Error("Failed to execute login query")
            row = self.cur_hris.fetchone()
            _log_debug(f"[LOGIN_DEBUG] Query result exists={bool(row)} for username={data.get('username')}")
            if not row:
                return {"status": True, "data": None}
            try:
                stored_pass = row.get('user_pass') or ''
                ph = PasswordHasher()
                try:
                    ph.verify(stored_pass, data['password'])
                    _log_debug(f"[LOGIN_DEBUG] Argon2 verification SUCCESS for username={data.get('username')}")
                    return {"status": True, "data": row}
                except (argon2_exceptions.VerifyMismatchError, argon2_exceptions.InvalidHash, Exception):
                    pass

                # Legacy plaintext match
                if stored_pass == data['password']:
                    _log_debug(f"[LOGIN_DEBUG] Legacy plaintext match SUCCESS for username={data.get('username')}")
                    # Auto-rehash to Argon2 for security
                    try:
                        new_hash = ph.hash(data['password'])
                        if self.execute_query("UPDATE app_users SET user_pass=%s WHERE user_id=%s", (new_hash, row['user_id'])):
                            self.commit()
                            _log_debug(f"[LOGIN_DEBUG] Auto-rehash applied (plaintext→argon2) for user_id={row['user_id']}")
                        else:
                            _log_debug(f"[LOGIN_DEBUG] Auto-rehash UPDATE failed for user_id={row['user_id']}")
                    except Exception as e:
                        _log_debug(f"[LOGIN_DEBUG] Auto-rehash error for user_id={row.get('user_id')}: {e}")
                    return {"status": True, "data": row}

                # Legacy AES-encrypted match
                try:
                    legacy = sandi()
                    decrypted = legacy.decrypt(stored_pass)
                    if decrypted == data['password']:
                        _log_debug(f"[LOGIN_DEBUG] Legacy AES decrypt match SUCCESS for username={data.get('username')}")
                        # Auto-rehash to Argon2 after AES legacy match
                        try:
                            new_hash = ph.hash(data['password'])
                            if self.execute_query("UPDATE app_users SET user_pass=%s WHERE user_id=%s", (new_hash, row['user_id'])):
                                self.commit()
                                _log_debug(f"[LOGIN_DEBUG] Auto-rehash applied (AES→argon2) for user_id={row['user_id']}")
                            else:
                                _log_debug(f"[LOGIN_DEBUG] Auto-rehash UPDATE failed for user_id={row['user_id']}")
                        except Exception as e:
                            _log_debug(f"[LOGIN_DEBUG] Auto-rehash error for user_id={row.get('user_id')}: {e}")
                        return {"status": True, "data": row}
                except Exception:
                    pass

                _log_debug(f"[LOGIN_DEBUG] All verification methods FAILED for username={data.get('username')}")
                return {"status": True, "data": None}
            except Exception:
                _log_debug(f"[LOGIN_DEBUG] Unexpected error during verification for username={data.get('username')}")
                return {"status": True, "data": None}
        except pymysql.Error as e:
            return {
                "status": False,
                'message': 'Terjadi error {!r}, error nya {}'.format(e, e.args[0])
            }
    
    def insert_user(self, data):
        user_name = data.get('user_name')
        user_pass = data.get('user_pass')
        user_alias = data.get('user_alias')
        user_mail = data.get('user_mail')
        user_st = data.get('user_st')

        if not user_name or not user_pass or not user_alias or not user_mail or user_st is None:
            return {
                'hasil': {
                    'status': False,
                    'message': 'Field user_name, user_pass, user_alias, user_mail, dan user_st wajib diisi'
                }
            }

        try:
            sql_insert = """
                        INSERT INTO app_users
                        (
                            app_users.user_id,
                            app_users.user_name,
                            app_users.user_pass,
                            app_users.user_alias,
                            app_users.user_mail,
                            app_users.user_telp,
                            app_users.user_alamat,
                            app_users.user_st,
                            app_users.user_foto,
                            app_users.mdb,
                            app_users.mdb_name,
                            app_users.mdd
                        )
                    VALUES
                        (
                            UUID(),
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s
                        )
                """

            ph = PasswordHasher()
            hashed_pass = ph.hash(user_pass)

            if not self.execute_query(sql_insert, (
                user_name,
                hashed_pass,
                user_alias,
                user_mail,
                data.get('user_telp'),
                data.get('user_alamat'),
                str(user_st),
                data.get('user_foto') or '',
                data.get('mdb'),
                data.get('mdb_name') or '',
                data.get('mdd') or datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            )):
                raise pymysql.Error('Failed to insert user data')

            if not self.commit():
                raise pymysql.Error('Failed to commit user data')

            hasil = {
                'status': True,
                'message': 'Data Berhasil Disimpan'
            }
        except pymysql.Error as e:
            hasil = {
                'status': False,
                'message': 'Terjadi error {!r}, error nya {}'.format(e, e.args[0] if e.args else e)
            }

        return {'hasil': hasil}

    def update_account_ads(self, data):
        try:
            sql_update = """
                        UPDATE master_account_ads SET
                            account_name = %s,
                            account_email = %s,
                            account_id = %s,
                            app_id = %s,
                            app_secret = %s,
                            access_token = %s,
                            mdb = %s,
                            mdb_name = %s,
                            mdd = %s
                        WHERE account_ads_id = %s
                """
            if not self.execute_query(sql_update, (
                data['account_name'],
                data['account_email'],
                data['account_id'],
                data['app_id'],
                data['app_secret'],
                data['access_token'],
                data['mdb'],
                data['mdb_name'],
                data['mdd'],
                data['account_ads_id']
            )):
                raise pymysql.Error("Failed to update account ads")
            
            if not self.commit():
                raise pymysql.Error("Failed to commit account ads update")
            
            hasil = {
                "status": True,
                "message": "Data Account Ads berhasil diupdate !"
            }
        except pymysql.Error as e:
            hasil = {
                "status": False,
                'message': 'Terjadi error {!r}, error nya {}'.format(e, e.args[0])
            }
        return {'hasil': hasil}

    def update_account_ads_access_token(self, account_ads_id, access_token, mdb=None, mdb_name=None):
        try:
            sql_update = """
                UPDATE master_account_ads SET
                    access_token = %s,
                    mdb = COALESCE(%s, mdb),
                    mdb_name = COALESCE(%s, mdb_name),
                    mdd = %s
                WHERE account_ads_id = %s
            """
            now_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            if not self.execute_query(sql_update, (
                access_token,
                mdb,
                mdb_name,
                now_str,
                account_ads_id,
            )):
                raise pymysql.Error("Failed to update account ads access token")
            if not self.commit():
                raise pymysql.Error("Failed to commit account ads access token update")
            hasil = {
                "status": True,
                "message": "Access token berhasil diperbarui"
            }
        except pymysql.Error as e:
            hasil = {
                "status": False,
                "message": 'Terjadi error {!r}, error nya {}'.format(e, e.args[0] if e.args else e)
            }
        return {'hasil': hasil}

    def delete_account_ads(self, account_ads_id):
        try:
            sql_delete = """
                        DELETE FROM master_account_ads
                        WHERE account_ads_id = %s
                """
            if not self.execute_query(sql_delete, (account_ads_id,)):
                raise pymysql.Error("Failed to delete account ads")

            affected = self.cur_hris.rowcount if self.cur_hris else 0

            if not self.commit():
                raise pymysql.Error("Failed to commit account ads delete")

            if affected <= 0:
                hasil = {
                    "status": False,
                    "message": "Account tidak ditemukan!"
                }
            else:
                hasil = {
                    "status": True,
                    "message": "Account ads berhasil dihapus"
                }
        except pymysql.Error as e:
            hasil = {
                "status": False,
                'message': 'Terjadi error {!r}, error nya {}'.format(e, e.args[0] if e.args else e)
            }
        return {'hasil': hasil}
    
    def insert_login(self, data):
        try:
            sql_insert = """
                        INSERT INTO app_user_login
                        (
                            app_user_login.login_id,
                            app_user_login.user_id,
                            app_user_login.login_date,
                            app_user_login.logout_date,
                            app_user_login.ip_address,
                            app_user_login.user_agent,
                            app_user_login.latitude,
                            app_user_login.longitude,
                            app_user_login.lokasi
                        )
                    VALUES
                        (
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s
                        )
                """
            if not self.execute_query(sql_insert, (
                data['login_id'],
                data['user_id'],
                data['login_date'],
                data['logout_date'],
                data['ip_address'],
                data['user_agent'],
                data['latitude'],
                data['longitude'],
                data.get('lokasi', None)  # Lokasi bisa None jika tidak ada
            )):
                raise pymysql.Error("Failed to insert login data")
            
            if not self.commit():
                raise pymysql.Error("Failed to commit login data")
            
            hasil = {
                "status": True,
                "message": "Data Login Berhasil Disimpan",
                "login_id": data['login_id']
            }
        except pymysql.Error as e:
            hasil = {
                "status": False,
                'message': 'Terjadi error {!r}, error nya {}'.format(e, e.args[0])
            }
        return {'hasil': hasil}

    def update_login(self, data):
        sql = """
            UPDATE app_user_login
            SET app_user_login.logout_date=%s
            WHERE app_user_login.login_id=%s 
            """
        try:
            if not self.execute_query(sql, (
                data['logout_date'],
                data['login_id']
            )):
                raise pymysql.Error("Failed to update login data")
            
            if not self.commit():
                raise pymysql.Error("Failed to commit login update")
            
            hasil = {
                "status": True,
                "message": "Data Login Berhasil Diupdate"
            }
        except pymysql.Error as e:
            hasil = {
                "status": False,
                'message': 'Terjadi error {!r}, error nya {}'.format(e, e.args[0])
            }
        return hasil
    
    def data_login_user(self):
        sql = '''
            SELECT a.login_id, a.user_id, sub.login_day, sub.login_time, a.login_date, 
            a.logout_date, DATE(logout_date) AS logout_day, TIME(logout_date) AS logout_time, 
            a.ip_address, a.user_agent, a.lokasi, b.user_alias
            FROM app_user_login a
            INNER JOIN (
                SELECT user_id, 
                DATE(login_date) AS login_day,
                TIME(login_date) AS login_time,
                MAX(login_date) AS max_login
                FROM app_user_login
                GROUP BY user_id, DATE(login_date)
            ) sub ON sub.user_id = a.user_id 
            AND DATE(a.login_date) = sub.login_day 
            AND a.login_date = sub.max_login
            INNER JOIN app_users b ON b.user_id = a.user_id
            ORDER BY a.login_date DESC
        '''
        try:
            if not self.execute_query(sql):
                raise pymysql.Error("Failed to fetch login data")
            datanya = self.cur_hris.fetchall()
            hasil = {
                "status": True,
                "data": datanya
            }
        except pymysql.Error as e:
            hasil = {
                "status": False,
                'data': 'Terjadi error {!r}, error nya {}'.format(e, e.args[0])
            }
        return hasil

    def data_user_by_params(self, params=None):
        if params and 'user_mail' in params:
            sql='''
                SELECT user_id, user_name, user_alias, 
                user_mail, user_telp, user_alamat, user_st, super_st 
                FROM `app_users`
                WHERE user_mail = %s
                ORDER BY user_alias ASC
            '''
            try:
                if not self.execute_query(sql, (params['user_mail'],)):
                    raise pymysql.Error("Failed to fetch user data")
                datanya = self.cur_hris.fetchall()
                hasil = {
                    "status": True,
                    "data": datanya
                }
            except pymysql.Error as e:
                hasil = {
                    "status": False,
                    'data': 'Terjadi error {!r}, error nya {}'.format(e, e.args[0])
                }
        else:
            sql='''
                SELECT user_id, user_name, user_alias, 
                user_mail, user_telp, user_alamat, user_st 
                FROM `app_users`
                ORDER BY user_alias ASC
            '''
            try:
                if not self.execute_query(sql):
                    raise pymysql.Error("Failed to fetch user data")
                datanya = self.cur_hris.fetchall()
                hasil = {
                    "status": True,
                    "data": datanya
                }
            except pymysql.Error as e:
                hasil = {
                    "status": False,
                    'data': 'Terjadi error {!r}, error nya {}'.format(e, e.args[0])
                }
        return hasil
    
    def get_all_adx_account_data(self):
        """Get user data by user_mail"""
        sql = """
            SELECT a.* FROM app_credentials a
            INNER JOIN data_adx_domain b ON a.account_id = b.account_id
            GROUP BY a.account_id
        """
        try:
            if not self.execute_query(sql,):
                raise pymysql.Error("Failed to fetch user data")
            result = self.cur_hris.fetchall()
            return {
                "status": True,
                "data": result
            }
        except pymysql.Error as e:
            return {
                "status": False,
                'data': f'Terjadi error {e!r}, error nya {e.args[0]}'
            }

    def get_all_adsense_account_data(self):
        """Get user data by user_mail"""
        sql = """
            SELECT a.* FROM app_credentials a
            INNER JOIN data_adsense_domain b ON a.account_id = b.account_id
            GROUP BY a.account_id
        """
        try:
            if not self.execute_query(sql,):
                raise pymysql.Error("Failed to fetch user data")
            result = self.cur_hris.fetchall()
            return {
                "status": True,
                "data": result
            }
        except pymysql.Error as e:
            return {
                "status": False,
                'data': f'Terjadi error {e!r}, error nya {e.args[0]}'
            }

    def get_all_adx_domain_data(self):
        """Get user data by user_mail"""
        sql = """
            SELECT a.account_id, a.data_adx_domain 
            FROM data_adx_domain a
            INNER JOIN app_credentials b ON a.account_id = b.account_id
            GROUP BY a.data_adx_domain
            ORDER BY a.account_id ASC
        """
        try:
            if not self.execute_query(sql,):
                raise pymysql.Error("Failed to fetch user data")
            result = self.cur_hris.fetchall()
            return {
                "status": True,
                "data": result
            }
        except pymysql.Error as e:
            return {
                "status": False,
                'data': f'Terjadi error {e!r}, error nya {e.args[0]}'
            }

    def get_all_adsense_domain_data(self):
        """Get user data by user_mail"""
        sql = """
            SELECT a.account_id, 
            CONCAT(SUBSTRING_INDEX(a.data_adsense_domain, '.', 1), '.com') AS 'data_adsense_domain'
            FROM data_adsense_domain a
            INNER JOIN app_credentials b ON a.account_id = b.account_id
            GROUP BY a.data_adsense_domain
            ORDER BY a.account_id ASC
        """
        try:
            if not self.execute_query(sql,):
                raise pymysql.Error("Failed to fetch user data")
            result = self.cur_hris.fetchall()
            return {
                "status": True,
                "data": result
            }
        except pymysql.Error as e:
            return {
                "status": False,
                'data': f'Terjadi error {e!r}, error nya {e.args[0]}'
            }

    def get_all_domain(self):
        """Get all domain collected"""
        sql = """
            SELECT data_adx_domain FROM data_adx_domain
            GROUP BY data_adx_domain
        """
        try:
            if not self.execute_query(sql,):
                raise pymysql.Error("Failed to fetch domain")
            result = self.cur_hris.fetchall()
            return {
                "status": True,
                "data": result
            }
        except pymysql.Error as e:
            return {
                "status": False,
                'data': f'Terjadi error {e!r}, error nya {e.args[0]}'
            }

    def get_all_adx_account_data_user(self, user_id):
        """Get user data by user_mail"""
        sql = """
            SELECT a.* FROM app_credentials a
            INNER JOIN app_credentials_assign b ON b.account_id = a.account_id
            INNER JOIN data_adx_domain c ON a.account_id = c.account_id
            WHERE b.user_id = %s
            GROUP BY a.account_id
        """
        try:
            if not self.execute_query(sql, (user_id)): 
                raise pymysql.Error("Failed to fetch user data")
            result = self.cur_hris.fetchall()
            return {
                "status": True,
                "data": result
            }
        except pymysql.Error as e:
            return {
                "status": False,
                'data': f'Terjadi error {e!r}, error nya {e.args[0]}'
            }

    def get_all_adsense_account_data_user(self, user_id):
        """Get user data by user_mail"""
        sql = """
            SELECT a.* FROM app_credentials a
            INNER JOIN app_credentials_assign b ON b.account_id = a.account_id
            INNER JOIN data_adsense_domain c ON a.account_id = c.account_id
            WHERE b.user_id = %s
            GROUP BY a.account_id
        """
        try:
            if not self.execute_query(sql, (user_id)): 
                raise pymysql.Error("Failed to fetch user data")
            result = self.cur_hris.fetchall()
            return {
                "status": True,
                "data": result
            }
        except pymysql.Error as e:
            return {
                "status": False,
                'data': f'Terjadi error {e!r}, error nya {e.args[0]}'
            }

    def get_all_adx_domain_data_user(self, user_id):
        """Get user data by user_mail"""
        sql = """
            SELECT a.account_id, a.data_adx_domain 
            FROM data_adx_domain a
            INNER JOIN app_credentials b ON a.account_id = b.account_id
            INNER JOIN app_credentials_assign c ON b.account_id = c.account_id
            WHERE c.user_id = %s
            GROUP BY a.data_adx_domain
            ORDER BY a.account_id ASC
        """
        try:
            if not self.execute_query(sql, (user_id)): 
                raise pymysql.Error("Failed to fetch user data")
            result = self.cur_hris.fetchall()
            return {
                "status": True,
                "data": result
            }
        except pymysql.Error as e:
            return {
                "status": False,
                'data': f'Terjadi error {e!r}, error nya {e.args[0]}'
            }

    def get_all_adsense_domain_data_user(self, user_id):
        """Get user data by user_mail"""
        sql = """
            SELECT a.account_id, a.data_adsense_domain 
            FROM data_adsense_domain a
            INNER JOIN app_credentials b ON a.account_id = b.account_id
            INNER JOIN app_credentials_assign c ON b.account_id = c.account_id
            WHERE c.user_id = %s
            GROUP BY a.data_adsense_domain
            ORDER BY a.account_id ASC
        """
        try:
            if not self.execute_query(sql, (user_id)): 
                raise pymysql.Error("Failed to fetch user data")
            result = self.cur_hris.fetchall()
            return {
                "status": True,
                "data": result
            }
        except pymysql.Error as e:
            return {
                "status": False,
                'data': f'Terjadi error {e!r}, error nya {e.args[0]}'
            }

    def get_user_by_mail(self, user_mail):
        """Get user data by user_mail"""
        sql = """
            SELECT * FROM app_credentials 
            WHERE user_mail = %s
        """
        try:
            if not self.execute_query(sql, (user_mail)):
                raise pymysql.Error("Failed to fetch user data")
            result = self.cur_hris.fetchone()
            return {
                "status": True,
                "data": result
            }
        except pymysql.Error as e:
            return {
                "status": False,
                'data': f'Terjadi error {e!r}, error nya {e.args[0]}'
            }

    def get_user_by_id(self, user_id):
        """Get user data by user_id"""
        sql = """
            SELECT * FROM app_users 
            WHERE user_id = %s
        """
        try:
            if not self.execute_query(sql, (user_id)):
                raise pymysql.Error("Failed to fetch user data")
            result = self.cur_hris.fetchone()
            # Sanitize password field from being exposed
            if isinstance(result, dict) and 'user_pass' in result:
                result['user_pass'] = None
            return {
                "status": True,
                "data": result
            }
        except pymysql.Error as e:
            return {
                "status": False,
                'data': f'Terjadi error {e!r}, error nya {e.args[0]}'
            }
    
    def check_refresh_token(self, user_mail):
        """
        Cek apakah user sudah memiliki refresh token di database
        """
        try:
            sql_select = """
                        SELECT refresh_token FROM user_oauth_credentials
                        WHERE user_mail = %s 
                        AND refresh_token IS NOT NULL
                        AND is_active = 1
                        """
            if not self.execute_query(sql_select, (user_mail,)):
                raise pymysql.Error("Failed to check refresh token")
            result = self.cur_hris.fetchone()
            
            if result:
                hasil = {
                    "status": True,
                    "has_token": True,
                    "refresh_token": result['refresh_token'],
                    "message": f"Refresh token ditemukan untuk {user_mail}"
                }
            else:
                hasil = {
                    "status": True,
                    "has_token": False,
                    "refresh_token": None,
                    "message": f"Refresh token tidak ditemukan untuk {user_mail}"
                }
                
        except Exception as e:
            hasil = {
                "status": False,
                "has_token": False,
                "refresh_token": None,
                "message": f"Error saat cek refresh token: {str(e)}"
            }
            
        return {'hasil': hasil}

    def get_user_by_email(self, user_mail):
        """
        Get user data from app_users table by email
        """
        sql = '''
            SELECT a.user_id, a.user_name, a.user_pass, a.user_alias, 
            a.user_mail, a.user_telp, a.user_alamat, a.user_st,
            b.client_id, b.client_secret, b.refresh_token, b.network_code,
            b.developer_token
            FROM app_users a
            LEFT JOIN app_credentials b ON a.`user_mail` = b.`user_mail`
            WHERE a.user_mail = %s
        '''
        try:
            if not self.execute_query(sql, (user_mail,)):
                raise pymysql.Error("Failed to fetch user data by email")
            datanya = self.cur_hris.fetchone()
            hasil = {
                "status": True,
                "data": datanya
            }
        except pymysql.Error as e:
            hasil = {
                "status": False,
                'data': 'Terjadi error {!r}, error nya {}'.format(e, e.args[0])
            }
        return hasil

    def generate_and_save_refresh_token(self, user_mail):
        """
        Generate refresh token baru menggunakan Google OAuth2 dan simpan ke database
        """
        try:
            # Ambil credentials dari database
            user_data = self.get_user_by_email(user_mail)
            if not user_data['status'] or not user_data['data']:
                return {
                    "status": False,
                    "message": "User tidak ditemukan dalam database"
                }
            user_info = user_data['data']
            client_id = user_info.get('client_id')
            client_secret = user_info.get('client_secret')
            if not client_id or not client_secret:
                return {
                    "status": False,
                    "message": "Client ID atau Client Secret tidak ditemukan di database untuk user ini"
                }
            try:
                # Konfigurasi OAuth2 untuk Google Ad Manager API
                SCOPES = [
                    'https://www.googleapis.com/auth/dfp'
                ]
                # Setup OAuth flow dengan credentials dari database
                client_config = {
                    "installed": {
                        "client_id": client_id,
                        "client_secret": client_secret,
                        "auth_uri": "https://accounts.google.com/o/oauth2/auth",
                        "token_uri": "https://oauth2.googleapis.com/token",
                        "redirect_uris": [
                            "https://kiwipixel.com/accounts/complete/google-oauth2/",
                            "http://127.0.0.1:8000/accounts/complete/google-oauth2/",
                            "http://127.0.0.1:8001/accounts/complete/google-oauth2/",
                            "http://localhost:8000/accounts/complete/google-oauth2/",
                            "http://localhost:8001/accounts/complete/google-oauth2/",
                        ]
                    }
                }
                flow = InstalledAppFlow.from_client_config(client_config, SCOPES)
                # Jalankan OAuth flow
                credentials = flow.run_local_server(port=8000)
                if credentials.refresh_token:
                    # Simpan refresh token ke database
                    update_result = self.update_refresh_token(user_mail, credentials.refresh_token)
                    
                    if update_result['hasil']['status']:
                        hasil = {
                            "status": True,
                            "refresh_token": credentials.refresh_token,
                            "message": f"Refresh token berhasil di-generate dan disimpan untuk {user_mail}"
                        }
                    else:
                        hasil = {
                            "status": False,
                            "refresh_token": credentials.refresh_token,
                            "message": f"Refresh token berhasil di-generate tapi gagal disimpan: {update_result['hasil']['message']}"
                        }
                else:
                    hasil = {
                        "status": False,
                        "refresh_token": None,
                        "message": "Gagal mendapatkan refresh token dari Google OAuth2"
                    }
            except ImportError:
                return {
                    "status": False,
                    "message": "google-auth-oauthlib tidak terinstall. Jalankan: pip install google-auth-oauthlib"
                }
        except ImportError:
            hasil = {
                "status": False,
                "refresh_token": None,
                "message": "google-auth-oauthlib tidak terinstall. Jalankan: pip install google-auth-oauthlib"
            }
        except Exception as e:
            hasil = {
                "status": False,
                "refresh_token": None,
                "message": f"Error saat generate refresh token: {str(e)}"
            }
            
        return {'hasil': hasil}
    
    def generate_refresh_token_from_db_credentials(self, user_mail):
        """
        Generate refresh token menggunakan client_id dan client_secret dari database
        """
        try:
            # Ambil credentials dari database
            user_data = self.get_user_by_email(user_mail)
            if not user_data['status'] or not user_data['data']:
                return {
                    "status": False,
                    "message": "User tidak ditemukan dalam database"
                }
            user_info = user_data['data']
            client_id = user_info.get('client_id')
            client_secret = user_info.get('client_secret')
            if not client_id or not client_secret:
                return {
                    "status": False,
                    "message": "Client ID atau Client Secret tidak ditemukan di database untuk user ini"
                }
            try:
                # Konfigurasi OAuth2 untuk Google Ad Manager API dengan credentials dari database
                SCOPES = [
                    'https://www.googleapis.com/auth/dfp'
                ]
                # Setup OAuth flow dengan credentials dari database
                client_config = {
                    "installed": {
                        "client_id": client_id,
                        "client_secret": client_secret,
                        "auth_uri": "https://accounts.google.com/o/oauth2/auth",
                        "token_uri": "https://oauth2.googleapis.com/token",
                        "redirect_uris": [
                            "https://kiwipixel.com/accounts/complete/google-oauth2/",
                            "http://127.0.0.1:8000/accounts/complete/google-oauth2/",
                            "http://127.0.0.1:8001/accounts/complete/google-oauth2/",
                            "http://localhost:8000/accounts/complete/google-oauth2/",
                            "http://localhost:8001/accounts/complete/google-oauth2/",
                        ]
                    }
                }
                flow = InstalledAppFlow.from_client_config(client_config, SCOPES)
                # Jalankan OAuth flow dengan port dinamis untuk menghindari konflik
                credentials = flow.run_local_server(port=8000)
                refresh_token = credentials.refresh_token
                if refresh_token:
                    # Simpan refresh token ke database
                    save_result = self.update_refresh_token(user_mail, refresh_token)
                    if save_result['status']:
                        return {
                            "status": True,
                            "message": "Refresh token berhasil di-generate dan disimpan",
                            "refresh_token": refresh_token,
                            "timestamp": datetime.now().isoformat()
                        }
                    else:
                        return {
                            "status": False,
                            "message": "Refresh token berhasil di-generate tapi gagal disimpan ke database",
                            "details": save_result.get('message', 'Unknown error')
                        }
                else:
                    return {
                        "status": False,
                        "message": "Gagal mendapatkan refresh token dari Google OAuth2"
                    }
                    
            except ImportError:
                return {
                    "status": False,
                    "message": "google-auth-oauthlib tidak terinstall. Jalankan: pip install google-auth-oauthlib"
                }
            except OSError as os_error:
                if "Address already in use" in str(os_error):
                    return {
                        "status": False,
                        "message": "Port sedang digunakan. Silakan coba lagi dalam beberapa saat atau restart aplikasi.",
                        "details": f"Error: {str(os_error)}"
                    }
                else:
                    return {
                        "status": False,
                        "message": f"Error sistem: {str(os_error)}",
                        "details": "Terjadi error pada sistem operasi"
                    }
            except Exception as oauth_error:
                return {
                    "status": False,
                    "message": f"Error saat OAuth flow: {str(oauth_error)}",
                    "details": "Pastikan client_id dan client_secret valid dan aplikasi OAuth sudah dikonfigurasi dengan benar"
                }
                
        except Exception as e:
            return {
                "status": False,
                "message": f"Error saat generate refresh token: {str(e)}"
            }

    def get_or_generate_refresh_token(self, user_mail):
        """
        Fungsi utama: Cek refresh token di database, jika belum ada maka generate baru
        """
        check_result = self.check_refresh_token(user_mail)
        
        if check_result['hasil']['status'] and check_result['hasil']['has_token']:
            # Refresh token sudah ada
            return {
                'hasil': {
                    "status": True,
                    "action": "existing",
                    "refresh_token": check_result['hasil']['refresh_token'],
                    "message": f"Menggunakan refresh token yang sudah ada untuk {user_mail}"
                }
            }
        else:
            # Refresh token belum ada, generate baru
            generate_result = self.generate_and_save_refresh_token(user_mail)
            
            if generate_result['hasil']['status']:
                return {
                    'hasil': {
                        "status": True,
                        "action": "generated",
                        "refresh_token": generate_result['hasil']['refresh_token'],
                        "message": generate_result['hasil']['message']
                    }
                }
            else:
                return {
                    'hasil': {
                        "status": False,
                        "action": "failed",
                        "refresh_token": None,
                        "message": generate_result['hasil']['message']
                    }
                }

    def data_account_ads_by_params(self, now):
        sql='''
            SELECT a.account_ads_id, a.account_name, c.total_data, a.account_email, a.account_id, 
            a.app_id, a.app_secret, a.access_token, b.user_alias AS 'pemilik_account', c.total_data AS 'total_data_adsense', a.mdd
            FROM `master_account_ads` a
            LEFT JOIN app_users b ON a.account_owner = b.user_id
            LEFT JOIN (
            	SELECT account_ads_id, COUNT(*) AS 'total_data' 
            	FROM master_ads
                WHERE DATE(mdd) >= %s
            	GROUP BY account_ads_id
            )c ON a.account_id = c.account_ads_id
            ORDER BY a.account_name ASC
        '''
        try:
            if not self.execute_query(sql, (now,)):
                raise pymysql.Error("Failed to fetch account ads data")
            datanya = self.cur_hris.fetchall()
            hasil = {
                "status": True,
                "data": datanya
            }
        except pymysql.Error as e:
            hasil = {
                "status": False,
                'data': 'Terjadi error {!r}, error nya {}'.format(e, e.args[0])
            }
        return hasil
    
    def is_exist_account_ads_by_params(self, data):
        sql='''
            SELECT *
            FROM master_account_ads 
            WHERE account_name = %s
            AND account_email = %s
            AND account_id = %s
            AND app_id = %s
        '''
        try:
            if not self.execute_query(sql, (
                data['account_name'],
                data['account_email'],
                data['account_id'],
                data['app_id']
            )):
                raise pymysql.Error("Failed to check account ads existence")
            datanya = self.cur_hris.fetchone()
            hasil = {
                "status": True,
                "data": datanya
            }
        except pymysql.Error as e:
            err_code = None
            err_msg = None
            try:
                if isinstance(getattr(e, 'args', None), (list, tuple)) and len(e.args) >= 2:
                    err_code = e.args[0]
                    err_msg = e.args[1]
                elif isinstance(getattr(e, 'args', None), (list, tuple)) and len(e.args) == 1:
                    err_code = e.args[0]
            except Exception:
                err_code = None
                err_msg = None

            hasil = {
                "status": False,
                'data': 'Terjadi error {!r}, code={}, message={}'.format(e, err_code, err_msg)
            }
        except Exception as e:
            hasil = {
                "status": False,
                'data': 'Terjadi error {!r}'.format(e)
            }
        return {'hasil': hasil}

    def delete_data_ads_campaign_by_date_account(self, account, campaign_id, domain, campaign_name, tanggal):
        try:
            sql_delete = """
                        DELETE FROM data_ads_campaign
                        WHERE account_ads_id = %s
                        AND data_ads_campaign_id = %s
                        AND data_ads_domain = %s
                        AND data_ads_campaign_nm = %s
                        AND data_ads_tanggal = %s
                """
            if not self.execute_query(sql_delete, (account, campaign_id, domain, campaign_name, tanggal)):
                raise pymysql.Error("Failed to delete data ads campaign by date range")

            affected = self.cur_hris.rowcount if self.cur_hris else 0

            if not self.commit():
                raise pymysql.Error("Failed to commit delete data ads campaign by date range")

            hasil = {
                "status": True,
                "message": f"Berhasil menghapus {affected} baris pada rentang tanggal",
                "affected": affected
            }
        except pymysql.Error as e:
            err_code = None
            err_msg = None
            try:
                if isinstance(getattr(e, 'args', None), (list, tuple)) and len(e.args) >= 2:
                    err_code = e.args[0]
                    err_msg = e.args[1]
                elif isinstance(getattr(e, 'args', None), (list, tuple)) and len(e.args) == 1:
                    err_code = e.args[0]
            except Exception:
                err_code = None
                err_msg = None

            hasil = {
                "status": False,
                'data': 'Terjadi error {!r}, code={}, message={}'.format(e, err_code, err_msg)
            }
        return {'hasil': hasil}

    def get_data_ads_country_to_insert_log(self, account, country, domain, campaign, tanggal):
        try:
            sql_select = (
                "SELECT * FROM data_ads_country "
                "WHERE account_ads_id = %s "
                "AND data_ads_country_cd LIKE %s "
                "AND data_ads_domain = %s "
                "AND data_ads_campaign_nm LIKE %s "
                "AND data_ads_country_tanggal LIKE %s "
                "ORDER BY mdd DESC LIMIT 1"
            )
            if not self.execute_query(sql_select, (account, country, domain, campaign, tanggal)):
                raise pymysql.Error("Failed to select data ads country by date range")
            data = self.cur_hris.fetchone()
            if not data:
                return {'hasil': {'status': False, 'message': 'Data ads country tidak ditemukan'}}
            return {'hasil': {'status': True, 'data': data}}
        except pymysql.Error as e:
            return {'hasil': {'status': False, 'message': f"Terjadi error {e}"}}

    def insert_log_ads_country_log(self, data):
        try:
            sql_insert = """
                        INSERT INTO log_ads_country
                        (
                            log_ads_country.account_ads_id,
                            log_ads_country.log_ads_country_cd,
                            log_ads_country.log_ads_country_nm,
                            log_ads_country.log_ads_domain,
                            log_ads_country.log_ads_campaign_id,
                            log_ads_country.log_ads_campaign_nm,
                            log_ads_country.log_ads_country_tanggal,
                            log_ads_country.log_ads_country_spend,
                            log_ads_country.log_ads_country_impresi,
                            log_ads_country.log_ads_country_click,
                            log_ads_country.log_ads_country_reach,
                            log_ads_country.log_ads_country_cpr,
                            log_ads_country.log_ads_country_cpc,
                            log_ads_country.log_ads_country_frekuensi,
                            log_ads_country.log_ads_country_lpv,
                            log_ads_country.log_ads_country_lpv_rate,
                            log_ads_country.mdb,
                            log_ads_country.mdb_name,
                            log_ads_country.mdd
                        )
                    VALUES
                        (
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s
                        )
                """
            if not self.execute_query(sql_insert, (
                data['account_ads_id'],
                data['log_ads_country_cd'],
                data['log_ads_country_nm'],
                data['log_ads_domain'],
                data['log_ads_campaign_id'],
                data['log_ads_campaign_nm'],
                data['log_ads_country_tanggal'],
                data['log_ads_country_spend'],
                data['log_ads_country_impresi'],
                data['log_ads_country_click'],
                data['log_ads_country_reach'],
                data['log_ads_country_cpr'],    
                data['log_ads_country_cpc'],
                data['log_ads_country_frekuensi'],
                data['log_ads_country_lpv'],
                data['log_ads_country_lpv_rate'],
                data['mdb'],
                data['mdb_name'],
                data['mdd']
            )):
                raise pymysql.Error("Failed to insert data ads country log")
            if not self.commit():
                raise pymysql.Error("Failed to commit data ads country log insert")
            
            hasil = {
                "status": True,
                "message": "Data ads country log berhasil ditambahkan"
            }
        except pymysql.Error as e:
            err_code = None
            err_msg = None
            try:
                if isinstance(getattr(e, 'args', None), (list, tuple)) and len(e.args) >= 2:
                    err_code = e.args[0]
                    err_msg = e.args[1]
                elif isinstance(getattr(e, 'args', None), (list, tuple)) and len(e.args) == 1:
                    err_code = e.args[0]
            except Exception:
                err_code = None
                err_msg = None

            hasil = {
                "status": False,
                'data': 'Terjadi error {!r}, code={}, message={}'.format(e, err_code, err_msg)
            }
        return {'hasil': hasil} 

    def insert_log_ads_campaign_log(self, data):
        try:
            sql_insert = """
                        INSERT INTO log_ads_campaign
                        (
                            log_ads_campaign.log_ads_id,
                            log_ads_campaign.account_ads_id,
                            log_ads_campaign.log_ads_domain,
                            log_ads_campaign.log_ads_campaign_id,
                            log_ads_campaign.log_ads_campaign_nm,
                            log_ads_campaign.log_ads_tanggal,
                            log_ads_campaign.log_ads_spend,
                            log_ads_campaign.log_ads_impresi,
                            log_ads_campaign.log_ads_click,
                            log_ads_campaign.log_ads_reach,
                            log_ads_campaign.log_ads_cpr,
                            log_ads_campaign.log_ads_cpc,
                            log_ads_campaign.log_ads_frekuensi,
                            log_ads_campaign.log_ads_lpv,
                            log_ads_campaign.log_ads_lpv_rate,
                            log_ads_campaign.mdb,
                            log_ads_campaign.mdb_name,
                            log_ads_campaign.mdd
                        )
                    VALUES
                        (
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s
                        )
                """
            if not self.execute_query(sql_insert, (
                data['log_ads_id'],
                data['account_ads_id'],
                data['log_ads_domain'],
                data['log_ads_campaign_id'],
                data['log_ads_campaign_nm'],
                data['log_ads_tanggal'],
                data['log_ads_spend'],
                data['log_ads_impresi'],
                data['log_ads_click'],
                data['log_ads_reach'],
                data['log_ads_cpr'],
                data['log_ads_cpc'],
                data['log_ads_frekuensi'],
                data['log_ads_lpv'],
                data['log_ads_lpv_rate'],
                data['mdb'],
                data['mdb_name'],
                data['mdd'],
            )):
                raise pymysql.Error("Failed to insert data ads campaign log")
            if not self.commit():
                raise pymysql.Error("Failed to commit data ads campaign log insert")

            hasil = {
                "status": True,
                "message": "Data ads campaign log berhasil ditambahkan"
            }
        except pymysql.Error as e:
            err_code = None
            err_msg = None
            try:
                if isinstance(getattr(e, 'args', None), (list, tuple)) and len(e.args) >= 2:
                    err_code = e.args[0]
                    err_msg = e.args[1]
                elif isinstance(getattr(e, 'args', None), (list, tuple)) and len(e.args) == 1:
                    err_code = e.args[0]
            except Exception:
                err_code = None
                err_msg = None

            hasil = {
                "status": False,
                'data': 'Terjadi error {!r}, code={}, message={}'.format(e, err_code, err_msg)
            }
        return {'hasil': hasil}

    def delete_data_ads_country_by_date_account(self, account, country, domain, campaign, tanggal):
        try:
            sql_delete = """
                        DELETE FROM data_ads_country
                        WHERE account_ads_id = %s
                        AND data_ads_country_cd = %s
                        AND data_ads_domain = %s
                        AND data_ads_campaign_nm = %s
                        AND data_ads_country_tanggal = %s
                """
            if not self.execute_query(sql_delete, (account, country, domain, campaign, tanggal)): 
                raise pymysql.Error("Failed to delete data ads country by date range")
            affected = self.cur_hris.rowcount if self.cur_hris else 0
            if not self.commit():
                raise pymysql.Error("Failed to commit delete data ads country by date range")
            hasil = {
                "status": True,
                "message": f"Berhasil menghapus {affected} baris pada rentang tanggal",
                "affected": affected
            }
        except pymysql.Error as e:
            err_code = None
            err_msg = None
            try:
                if isinstance(getattr(e, 'args', None), (list, tuple)) and len(e.args) >= 2:
                    err_code = e.args[0]
                    err_msg = e.args[1]
                elif isinstance(getattr(e, 'args', None), (list, tuple)) and len(e.args) == 1:
                    err_code = e.args[0]
            except Exception:
                err_code = None
                err_msg = None

            hasil = {
                "status": False,
                'data': 'Terjadi error {!r}, code={}, message={}'.format(e, err_code, err_msg)
            }
        return {'hasil': hasil}
    
    def insert_account_ads(self, data):
        try:
            sql_insert = """
                        INSERT INTO master_account_ads
                        (
                            master_account_ads.account_ads_id,
                            master_account_ads.account_name,
                            master_account_ads.account_email,
                            master_account_ads.account_id,
                            master_account_ads.app_id,
                            master_account_ads.app_secret,
                            master_account_ads.access_token,
                            master_account_ads.account_owner,
                            master_account_ads.mdb,
                            master_account_ads.mdb_name,
                            master_account_ads.mdd
                        )
                    VALUES
                        (
                            UUID(),
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s
                        )
                """
            if not self.execute_query(sql_insert, (
                data['account_name'],
                data['account_email'],
                data['account_id'],
                data['app_id'],
                data['app_secret'],
                data['access_token'],
                data['account_owner'],
                data['mdb'],
                data['mdb_name'],
                data['mdd']
            )):
                raise pymysql.Error("Failed to insert account ads")
            
            if not self.commit():
                raise pymysql.Error("Failed to commit account ads insert")
            
            hasil = {
                "status": True,
                "message": "Account ads berhasil ditambahkan"
            }
        except pymysql.Error as e:
            hasil = {
                "status": False,
                'data': 'Terjadi error {!r}, error nya {}'.format(e, e.args[0])
            }
        return {'hasil': hasil}

    def master_account_ads(self):
        sql = '''
            SELECT a.account_ads_id, a.account_name, a.account_email, a.account_id, 
            a.app_id, a.app_secret, a.access_token, b.user_alias AS 'pemilik_account'
            FROM `master_account_ads` a
            LEFT JOIN app_users b ON a.account_owner = b.user_id
            ORDER BY a.account_name ASC
        '''
        try:
            self.cur_hris.execute(sql)
            datanya = self.cur_hris.fetchall()
            hasil = {
                "status": True,
                "data": datanya
            }
        except pymysql.Error as e:
            hasil = {
                "status": False,
                'data': 'Terjadi error {!r}, error nya {}'.format(e, e.args[0])
            }
        return hasil

    def get_master_ads_campaign_name_map(self, campaign_ids):
        """
        Get latest campaign_id -> campaign_name by combining:
        - master_ads (master_campaign_id, master_campaign_nm)
        - data_ads_campaign (data_ads_campaign_id, data_ads_campaign_nm)
        """
        try:
            normalized_ids = []
            seen_ids = set()
            for raw_id in (campaign_ids or []):
                cid = str(raw_id or '').strip()
                if cid and cid not in seen_ids:
                    seen_ids.add(cid)
                    normalized_ids.append(cid)

            if not normalized_ids:
                return {"status": True, "data": {}}

            placeholders = ','.join(['%s'] * len(normalized_ids))
            sql = f"""
                SELECT campaign_id, campaign_name, updated_at, source_priority
                FROM (
                    SELECT 
                        CAST(m.master_campaign_id AS CHAR) AS campaign_id,
                        m.master_campaign_nm AS campaign_name,
                        COALESCE(m.mdd, m.master_date_end, m.master_date_start, m.master_date) AS updated_at,
                        2 AS source_priority
                    FROM master_ads m
                    WHERE CAST(m.master_campaign_id AS CHAR) IN ({placeholders})

                    UNION ALL

                    SELECT 
                        CAST(d.data_ads_campaign_id AS CHAR) AS campaign_id,
                        d.data_ads_campaign_nm AS campaign_name,
                        COALESCE(d.mdd, d.data_ads_tanggal) AS updated_at,
                        1 AS source_priority
                    FROM data_ads_campaign d
                    WHERE CAST(d.data_ads_campaign_id AS CHAR) IN ({placeholders})
                ) x
                WHERE COALESCE(campaign_id, '') <> ''
                  AND COALESCE(campaign_name, '') <> ''
                ORDER BY campaign_id ASC, updated_at DESC, source_priority ASC
            """
            params = tuple(normalized_ids + normalized_ids)
            self.cur_hris.execute(sql, params)
            rows = self.cur_hris.fetchall() or []
            name_map = {}
            for row in rows:
                cid = str((row or {}).get('campaign_id') or '').strip()
                cname = str((row or {}).get('campaign_name') or '').strip()
                if not cid:
                    continue
                if cid in name_map:
                    continue
                if cname:
                    name_map[cid] = cname

            return {"status": True, "data": name_map}
        except pymysql.Error as e:
            return {
                "status": False,
                "data": f"Terjadi error {e!r}, error nya {e.args[0] if e.args else e}"
            }

    def get_total_ads_spend_by_domain_keys_and_date(self, domain_keys, start_date, end_date):
        """Sum data_ads_spend where first 2 dot segments of data_ads_domain match domain keys."""
        try:
            def normalize_domain_key(raw_value):
                s = str(raw_value or '').strip().lower()
                if not s:
                    return ''
                parts = [p for p in s.split('.') if p]
                # remove TLD by taking only first 2 labels from the left
                # ex: mavon.missagendalimon.com -> mavon.missagendalimon
                # ex: mavon.missagendalimon -> mavon.missagendalimon
                if len(parts) >= 2:
                    return parts[0] + '.' + parts[1]
                return s

            keys = []
            seen = set()
            for raw in (domain_keys or []):
                key = normalize_domain_key(raw)
                if key and key not in seen:
                    seen.add(key)
                    keys.append(key)

            if not keys:
                return {"status": True, "data": {"total_ad_spend": 0.0}}

            placeholders = ','.join(['%s'] * len(keys))
            sql = f"""
                SELECT COALESCE(SUM(CAST(a.data_ads_spend AS DECIMAL(18,2))), 0) AS total_ad_spend
                FROM data_ads_campaign a
                WHERE DATE(a.data_ads_tanggal) BETWEEN %s AND %s
                  AND LOWER(SUBSTRING_INDEX(a.data_ads_domain, '.', 2)) IN ({placeholders})
            """
            params = [start_date, end_date] + keys
            self.cur_hris.execute(sql, tuple(params))
            row = self.cur_hris.fetchone() or {}
            total = float(row.get('total_ad_spend') or 0)
            return {"status": True, "data": {"total_ad_spend": total}}
        except pymysql.Error as e:
            return {
                "status": False,
                "data": f"Terjadi error {e!r}, error nya {e.args[0] if e.args else e}"
            }

    def get_daily_ads_spend_by_domain_keys_and_date(self, domain_keys, start_date, end_date):
        """Daily sum of data_ads_spend keyed by YYYY-MM-DD date."""
        try:
            def normalize_domain_key(raw_value):
                s = str(raw_value or '').strip().lower()
                if not s:
                    return ''
                parts = [p for p in s.split('.') if p]
                if len(parts) >= 2:
                    return parts[0] + '.' + parts[1]
                return s

            keys = []
            seen = set()
            for raw in (domain_keys or []):
                key = normalize_domain_key(raw)
                if key and key not in seen:
                    seen.add(key)
                    keys.append(key)

            if not keys:
                return {"status": True, "data": {}}

            placeholders = ','.join(['%s'] * len(keys))
            sql = f"""
                SELECT DATE(a.data_ads_tanggal) AS d, COALESCE(SUM(CAST(a.data_ads_spend AS DECIMAL(18,2))), 0) AS v
                FROM data_ads_campaign a
                WHERE DATE(a.data_ads_tanggal) BETWEEN %s AND %s
                  AND LOWER(SUBSTRING_INDEX(a.data_ads_domain, '.', 2)) IN ({placeholders})
                GROUP BY DATE(a.data_ads_tanggal)
                ORDER BY DATE(a.data_ads_tanggal) ASC
            """
            params = [start_date, end_date] + keys
            self.cur_hris.execute(sql, tuple(params))
            rows = self.cur_hris.fetchall() or []
            out = {}
            for row in rows:
                d = str((row or {}).get('d') or '').strip()
                if d:
                    out[d] = float((row or {}).get('v') or 0)
            return {"status": True, "data": out}
        except pymysql.Error as e:
            return {
                "status": False,
                "data": f"Terjadi error {e!r}, error nya {e.args[0] if e.args else e}"
            }

    def get_total_adx_revenue_by_domains_and_date(self, domains, start_date, end_date):
        """Sum data_adx_domain_revenue by selected domains within date range."""
        try:
            def normalize_domain_key(raw_value):
                s = str(raw_value or '').strip().lower()
                if not s:
                    return ''
                parts = [p for p in s.split('.') if p]
                if len(parts) >= 2:
                    return parts[0] + '.' + parts[1]
                return s

            normalized_domains = []
            seen_domains = set()
            for raw in (domains or []):
                domain = normalize_domain_key(raw)
                if domain and domain not in seen_domains:
                    seen_domains.add(domain)
                    normalized_domains.append(domain)

            if not normalized_domains:
                return {"status": True, "data": {"total_revenue": 0.0}}

            placeholders = ','.join(['%s'] * len(normalized_domains))
            sql = f"""
                SELECT COALESCE(SUM(CAST(a.data_adx_domain_revenue AS DECIMAL(18,2))), 0) AS total_revenue
                FROM data_adx_domain a
                WHERE DATE(a.data_adx_domain_tanggal) BETWEEN %s AND %s
                  AND LOWER(SUBSTRING_INDEX(a.data_adx_domain, '.', 2)) IN ({placeholders})
            """
            params = [start_date, end_date] + normalized_domains
            self.cur_hris.execute(sql, tuple(params))
            row = self.cur_hris.fetchone() or {}
            total = float(row.get('total_revenue') or 0)
            return {"status": True, "data": {"total_revenue": total}}
        except pymysql.Error as e:
            return {
                "status": False,
                "data": f"Terjadi error {e!r}, error nya {e.args[0] if e.args else e}"
            }

    def get_daily_adx_revenue_by_domains_and_date(self, domains, start_date, end_date):
        """Daily sum of data_adx_domain_revenue keyed by YYYY-MM-DD date."""
        try:
            def normalize_domain_key(raw_value):
                s = str(raw_value or '').strip().lower()
                if not s:
                    return ''
                parts = [p for p in s.split('.') if p]
                if len(parts) >= 2:
                    return parts[0] + '.' + parts[1]
                return s

            normalized_domains = []
            seen_domains = set()
            for raw in (domains or []):
                domain = normalize_domain_key(raw)
                if domain and domain not in seen_domains:
                    seen_domains.add(domain)
                    normalized_domains.append(domain)

            if not normalized_domains:
                return {"status": True, "data": {}}

            placeholders = ','.join(['%s'] * len(normalized_domains))
            sql = f"""
                SELECT DATE(a.data_adx_domain_tanggal) AS d, COALESCE(SUM(CAST(a.data_adx_domain_revenue AS DECIMAL(18,2))), 0) AS v
                FROM data_adx_domain a
                WHERE DATE(a.data_adx_domain_tanggal) BETWEEN %s AND %s
                  AND LOWER(SUBSTRING_INDEX(a.data_adx_domain, '.', 2)) IN ({placeholders})
                GROUP BY DATE(a.data_adx_domain_tanggal)
                ORDER BY DATE(a.data_adx_domain_tanggal) ASC
            """
            params = [start_date, end_date] + normalized_domains
            self.cur_hris.execute(sql, tuple(params))
            rows = self.cur_hris.fetchall() or []
            out = {}
            for row in rows:
                d = str((row or {}).get('d') or '').strip()
                if d:
                    out[d] = float((row or {}).get('v') or 0)
            return {"status": True, "data": out}
        except pymysql.Error as e:
            return {
                "status": False,
                "data": f"Terjadi error {e!r}, error nya {e.args[0] if e.args else e}"
            }

    def master_domain_ads(self):
        sql = '''
            SELECT 
            rs.account_id, 
            CASE WHEN rs.data_ads_domain = ''
            THEN 'Draft'
            ELSE 
            rs.data_ads_domain
            END AS data_ads_domain
            FROM(
                SELECT a.account_ads_id AS 'account_id', 
                REGEXP_SUBSTR(
                    LOWER(a.data_ads_domain),
                    '^[a-z0-9]+\\.[a-z0-9]+'
                ) AS data_ads_domain
                FROM data_ads_campaign a
                LEFT JOIN master_account_ads b ON a.account_ads_id = b.account_id
                LEFT JOIN app_users c ON b.account_owner = c.user_id
                GROUP BY a.data_ads_domain
            )rs
            GROUP BY rs.data_ads_domain 
        '''
        try:
            self.cur_hris.execute(sql)
            datanya = self.cur_hris.fetchall()
            hasil = {
                "status": True,
                "data": datanya
            }
        except pymysql.Error as e:
            hasil = {
                "status": False,
                'data': 'Terjadi error {!r}, error nya {}'.format(e, e.args[0])
            }
        return hasil

    def get_last_update_ads_traffic_country(self):
        sql = '''
            SELECT MAX(mdd) AS 'last_update'
            FROM `data_ads_country`
        '''
        try:
            self.cur_hris.execute(sql)
            datanya = self.cur_hris.fetchone()
            hasil = {
                "status": True,
                "data": datanya
            }
        except pymysql.Error as e:
            hasil = {
                "status": False,
                'data': 'Terjadi error {!r}, error nya {}'.format(e, e.args[0])
            }
        return hasil

    def get_last_update_ads_traffic_per_domain(self):
        sql = '''
            SELECT MAX(mdd) AS 'last_update'
            FROM `data_ads_campaign`
        '''
        try:
            self.cur_hris.execute(sql)
            datanya = self.cur_hris.fetchone()
            hasil = {
                "status": True,
                "data": datanya
            }
        except pymysql.Error as e:
            hasil = {
                "status": False,
                'data': 'Terjadi error {!r}, error nya {}'.format(e, e.args[0])
            }
        return hasil

    def get_last_update_adx_traffic_country(self):
        sql = '''
            SELECT MAX(mdd) AS 'last_update'
            FROM `data_adx_country`
        '''
        try:
            if not self.execute_query(sql):
                raise pymysql.Error('Failed to fetch last update')
            datanya = self.cur_hris.fetchone() if self.cur_hris else None
            hasil = {
                "status": True,
                "data": datanya
            }
        except pymysql.Error as e:
            hasil = {
                "status": False,
                'data': 'Terjadi error {!r}, error nya {}'.format(e, e.args[0])
            }
        return hasil

    def get_last_update_adsense_traffic_country(self):
        sql = '''
            SELECT MAX(mdd) AS 'last_update'
            FROM `data_adsense_country`
        '''
        try:
            if not self.execute_query(sql):
                raise pymysql.Error('Failed to fetch last update')
            datanya = self.cur_hris.fetchone() if self.cur_hris else None
            hasil = {
                "status": True,
                "data": datanya
            }
        except pymysql.Error as e:
            hasil = {
                "status": False,
                'data': 'Terjadi error {!r}, error nya {}'.format(e, e.args[0])
            }
        return hasil

    def get_last_update_adx_traffic_per_domain(self):
        sql = '''
            SELECT MAX(mdd) AS 'last_update'
            FROM `data_adx_domain`
        '''
        try:
            if not self.execute_query(sql):
                raise pymysql.Error('Failed to fetch last update')
            datanya = self.cur_hris.fetchone() if self.cur_hris else None
            hasil = {
                "status": True,
                "data": datanya
            }
        except pymysql.Error as e:
            hasil = {
                "status": False,
                'data': 'Terjadi error {!r}, error nya {}'.format(e, e.args[0])
            }
        return hasil

    def get_last_update_adsense_traffic_per_domain(self):
        sql = '''
            SELECT MAX(mdd) AS 'last_update'
            FROM `data_adsense_domain`
        '''
        try:
            if not self.execute_query(sql):
                raise pymysql.Error('Failed to fetch last update')
            datanya = self.cur_hris.fetchone() if self.cur_hris else None
            hasil = {
                "status": True,
                "data": datanya
            }
        except pymysql.Error as e:
            hasil = {
                "status": False,
                'data': 'Terjadi error {!r}, error nya {}'.format(e, e.args[0])
            }
        return hasil

    def get_last_update_adx_monitoring_by_params(self, start_date, end_date, selected_account_list=None):
        try:
            if isinstance(selected_account_list, str):
                selected_account_list = [selected_account_list.strip()]
            elif selected_account_list is None:
                selected_account_list = []
            elif isinstance(selected_account_list, (set, tuple)):
                selected_account_list = list(selected_account_list)
            data_account_list = [str(a).strip() for a in selected_account_list if str(a).strip()]

            engine = (self._report_engine() or '').strip().lower()
            use_clickhouse = (engine in ('clickhouse', 'ch'))

            account_col = "toString(b.account_id)" if use_clickhouse else "b.account_id"
            like_conditions_account = " OR ".join([f"{account_col} LIKE %s"] * len(data_account_list))
            like_params_account = [f"{account}%" for account in data_account_list]

            base_sql = [
                "SELECT",
                "\tMAX(b.mdd) AS 'last_update'",
                "FROM data_adx_country b",
                "WHERE",
            ]
            params = []
            if use_clickhouse:
                base_sql.append("toDate(b.data_adx_country_tanggal) BETWEEN toDate(%s) AND toDate(%s)")
            else:
                base_sql.append("b.data_adx_country_tanggal BETWEEN %s AND %s")
            params.extend([start_date, end_date])

            if data_account_list:
                base_sql.append(f"\tAND ({like_conditions_account})")
                params.extend(like_params_account)

            sql = "\n".join(base_sql)
            if not self.execute_query(sql, tuple(params)):
                raise pymysql.Error('Failed to fetch last update')
            datanya = self.cur_hris.fetchone() if self.cur_hris else None
            if not self.commit():
                raise pymysql.Error('Failed to commit last update')
            hasil = {
                "status": True,
                "data": datanya
            }
        except pymysql.Error as e:
            hasil = {
                "status": False,
                'data': 'Terjadi error {!r}, error nya {}'.format(e, e.args[0])
            }
        return hasil

    def get_last_update_adsense_monitoring_by_params(self, start_date, end_date, selected_account_list=None):
        try:
            if isinstance(selected_account_list, str):
                selected_account_list = [selected_account_list.strip()]
            elif selected_account_list is None:
                selected_account_list = []
            elif isinstance(selected_account_list, (set, tuple)):
                selected_account_list = list(selected_account_list)
            data_account_list = [str(a).strip() for a in selected_account_list if str(a).strip()]

            engine = (self._report_engine() or '').strip().lower()
            use_clickhouse = (engine in ('clickhouse', 'ch'))

            account_col = "toString(b.account_id)" if use_clickhouse else "b.account_id"
            like_conditions_account = " OR ".join([f"{account_col} LIKE %s"] * len(data_account_list))
            like_params_account = [f"{account}%" for account in data_account_list]

            base_sql = [
                "SELECT",
                "\tMAX(b.mdd) AS 'last_update'",
                "FROM data_adsense_country b",
                "WHERE",
            ]
            params = []
            if use_clickhouse:
                base_sql.append("toDate(b.data_adsense_country_tanggal) BETWEEN toDate(%s) AND toDate(%s)")
            else:
                base_sql.append("b.data_adsense_country_tanggal BETWEEN %s AND %s")
            params.extend([start_date, end_date])

            if data_account_list:
                base_sql.append(f"\tAND ({like_conditions_account})")
                params.extend(like_params_account)

            sql = "\n".join(base_sql)
            if not self.execute_query(sql, tuple(params)):
                raise pymysql.Error('Failed to fetch last update')
            datanya = self.cur_hris.fetchone() if self.cur_hris else None
            if not self.commit():
                raise pymysql.Error('Failed to commit last update')
            hasil = {
                "status": True,
                "data": datanya
            }
        except pymysql.Error as e:
            hasil = {
                "status": False,
                'data': 'Terjadi error {!r}, error nya {}'.format(e, e.args[0])
            }
        return hasil

    def get_last_update_adx_monitoring_by_domain_params(self, start_date, end_date, selected_account_list=None, selected_domain_list=None):
        try:
            if isinstance(selected_account_list, str):
                selected_account_list = [selected_account_list.strip()]
            elif selected_account_list is None:
                selected_account_list = []
            elif isinstance(selected_account_list, (set, tuple)):
                selected_account_list = list(selected_account_list)
            data_account_list = [str(a).strip() for a in selected_account_list if str(a).strip()]

            if isinstance(selected_domain_list, str):
                selected_domain_list = [selected_domain_list.strip()]
            elif selected_domain_list is None:
                selected_domain_list = []
            elif isinstance(selected_domain_list, (set, tuple)):
                selected_domain_list = list(selected_domain_list)
            data_domain_list = [str(d).strip() for d in selected_domain_list if str(d).strip()]

            engine = (self._report_engine() or '').strip().lower()
            use_clickhouse = (engine in ('clickhouse', 'ch'))

            account_col = "toString(b.account_id)" if use_clickhouse else "b.account_id"
            like_conditions_account = " OR ".join([f"{account_col} LIKE %s"] * len(data_account_list))
            like_params_account = [f"{account}%" for account in data_account_list]

            site_expr = "concat(arrayElement(splitByChar('.', b.data_adx_country_domain), 1), '.', arrayElement(splitByChar('.', b.data_adx_country_domain), 2))" if use_clickhouse else "SUBSTRING_INDEX(b.data_adx_country_domain, '.', 2)"
            like_conditions_domain = " OR ".join([f"(b.data_adsense_country_domain LIKE %s OR {site_expr} LIKE %s)"] * len(data_domain_list))
            like_params_domain = []
            for domain in data_domain_list:
                d = str(domain or '').strip()
                like_params_domain.extend([f"%{d}%", f"%{d}%"])

            base_sql = [
                "SELECT",
                f"\t{site_expr} AS 'site_name',",
                "\tMAX(b.mdd) AS 'last_update'",
                "FROM data_adx_country b",
                "WHERE",
            ]
            params = []
            if use_clickhouse:
                base_sql.append("toDate(b.data_adx_country_tanggal) BETWEEN toDate(%s) AND toDate(%s)")
            else:
                base_sql.append("b.data_adx_country_tanggal BETWEEN %s AND %s")
            params.extend([start_date, end_date])

            if data_account_list:
                base_sql.append(f"\tAND ({like_conditions_account})")
                params.extend(like_params_account)

            if data_domain_list:
                base_sql.append(f"\tAND ({like_conditions_domain})")
                params.extend(like_params_domain)

            base_sql.append(f"GROUP BY {site_expr}")
            base_sql.append(f"ORDER BY {site_expr} ASC")

            sql = "\n".join(base_sql)
            if not self.execute_query(sql, tuple(params)):
                raise pymysql.Error('Failed to fetch last update by domain')
            data = self.fetch_all()
            if not self.commit():
                raise pymysql.Error('Failed to commit last update by domain')
            hasil = {
                "status": True,
                "data": data
            }
        except pymysql.Error as e:
            hasil = {
                "status": False,
                'data': 'Terjadi error {!r}, error nya {}'.format(e, e.args[0])
            }
        return hasil

    def get_last_update_adsense_monitoring_by_domain_params(self, start_date, end_date, selected_account_list=None, selected_domain_list=None):
        try:
            if isinstance(selected_account_list, str):
                selected_account_list = [selected_account_list.strip()]
            elif selected_account_list is None:
                selected_account_list = []
            elif isinstance(selected_account_list, (set, tuple)):
                selected_account_list = list(selected_account_list)
            data_account_list = [str(a).strip() for a in selected_account_list if str(a).strip()]

            if isinstance(selected_domain_list, str):
                selected_domain_list = [selected_domain_list.strip()]
            elif selected_domain_list is None:
                selected_domain_list = []
            elif isinstance(selected_domain_list, (set, tuple)):
                selected_domain_list = list(selected_domain_list)
            data_domain_list = [str(d).strip() for d in selected_domain_list if str(d).strip()]

            engine = (self._report_engine() or '').strip().lower()
            use_clickhouse = (engine in ('clickhouse', 'ch'))

            account_col = "toString(b.account_id)" if use_clickhouse else "b.account_id"
            like_conditions_account = " OR ".join([f"{account_col} LIKE %s"] * len(data_account_list))
            like_params_account = [f"{account}%" for account in data_account_list]

            site_expr = "concat(arrayElement(splitByChar('.', b.data_adsense_country_domain), 1), '.', arrayElement(splitByChar('.', b.data_adsense_country_domain), 2))" if use_clickhouse else "SUBSTRING_INDEX(b.data_adsense_country_domain, '.', 2)"
            like_conditions_domain = " OR ".join([f"{site_expr} LIKE %s"] * len(data_domain_list))
            like_params_domain = [f"%{domain}%" for domain in data_domain_list]

            base_sql = [
                "SELECT",
                f"\t{site_expr} AS 'site_name',",
                "\tMAX(b.mdd) AS 'last_update'",
                "FROM data_adsense_country b",
                "WHERE",
            ]
            params = []
            if use_clickhouse:
                base_sql.append("toDate(b.data_adsense_country_tanggal) BETWEEN toDate(%s) AND toDate(%s)")
            else:
                base_sql.append("b.data_adsense_country_tanggal BETWEEN %s AND %s")
            params.extend([start_date, end_date])

            if data_account_list:
                base_sql.append(f"\tAND ({like_conditions_account})")
                params.extend(like_params_account)

            if data_domain_list:
                base_sql.append(f"\tAND ({like_conditions_domain})")
                params.extend(like_params_domain)

            base_sql.append(f"GROUP BY {site_expr}")
            base_sql.append(f"ORDER BY {site_expr} ASC")

            sql = "\n".join(base_sql)
            if not self.execute_query(sql, tuple(params)):
                raise pymysql.Error('Failed to fetch last update by domain')
            data = self.fetch_all()
            if not self.commit():
                raise pymysql.Error('Failed to commit last update by domain')
            hasil = {
                "status": True,
                "data": data
            }
        except pymysql.Error as e:
            hasil = {
                "status": False,
                'data': 'Terjadi error {!r}, error nya {}'.format(e, e.args[0])
            }
        return hasil

    def master_account_ads_by_id(self, data):
        sql = '''
            SELECT a.account_ads_id, a.account_name, a.account_email, a.account_id, 
            a.app_id, a.app_secret, a.access_token
            FROM `master_account_ads` a
            WHERE a.account_id = %s
        '''
        try:
            self.cur_hris.execute(sql,(
                data['data_account']
            ))
            datanya = self.cur_hris.fetchone()
            hasil = {
                "status": True,
                "data": datanya
            }
        except pymysql.Error as e:
            hasil = {
                "status": False,
                'data': 'Terjadi error {!r}, error nya {}'.format(e, e.args[0])
            }
        return hasil

    def update_master_plan(self, data):
        sql = '''
            UPDATE app_master_plan 
            SET master_task_code = %s, master_task_plan = %s, project_kategori = %s,
                urgency = %s, execute_status = %s, catatan = %s, assignment_to = %s
            WHERE master_plan_id = %s
        '''
        try:
            if not self.execute_query(sql, (
                data['master_task_code'],
                data['master_task_plan'],
                data['project_kategori'],
                data['urgency'],
                data['execute_status'],
                data['catatan'],
                data['assignment_to'],
                data['master_plan_id']
            )):
                raise pymysql.Error("Failed to update master plan")
            
            if not self.commit():
                raise pymysql.Error("Failed to commit master plan update")
            
            hasil = {
                "status": True,
                "data": "Master plan berhasil diupdate"
            }
        except pymysql.Error as e:
            hasil = {
                "status": False,
                'data': 'Terjadi error {!r}, error nya {}'.format(e, e.args[0])
            }
        return hasil

    def master_account_ads_by_params(self, data):
        sql = '''
            SELECT a.account_ads_id, a.account_name, a.account_email, a.account_id, 
            a.app_id, a.app_secret, a.access_token
            FROM `master_account_ads` a
            WHERE a.account_ads_id = %s
        '''
        try:
            self.cur_hris.execute(sql,(
                data['data_account']
            ))
            datanya = self.cur_hris.fetchall()
            hasil = {
                "status": True,
                "data": datanya
            }
        except pymysql.Error as e:
            hasil = {
                "status": False,
                'data': 'Terjadi error {!r}, error nya {}'.format(e, e.args[0])
            }
        return hasil

    # =========================
    # app_credentials (NEW)
    # =========================

    def get_user_credentials(self, user_mail=None):
        """
        Mengambil kredensial OAuth dari database berdasarkan user_id atau user_mail
        """
        sql = '''
            SELECT account_id, account_name, user_mail, client_id,
                   client_secret, refresh_token, network_code,
                   developer_token, is_active
            FROM app_credentials
            WHERE user_mail = %s
            LIMIT 1
        '''
        try:
            if not self.execute_query(sql, (user_mail,)):
                raise pymysql.Error("Failed to fetch OAuth credentials")
            
            result = self.cur_hris.fetchone()
            if not result:
                return {
                    'status': False,
                    'error': f'No credentials found for user (Email: {user_mail})'
                }
            
            return {
                'status': True,
                'data': result
            }
        except pymysql.Error as e:
            return {
                'status': False,
                'error': f'Database error: {str(e)}'
            }
            
    def check_app_credentials_exist(self, user_mail):
        """
        Memeriksa apakah kredensial aplikasi sudah ada untuk user tertentu
        """
        sql = '''
            SELECT COUNT(*) AS total 
            FROM app_credentials 
            WHERE user_mail = %s
        '''
        try:
            if not self.execute_query(sql, (user_mail,)):
                raise pymysql.Error("Failed to check app_credentials existence")

            result = self.cur_hris.fetchone()
            if isinstance(result, dict):
                return result.get('total', 0)
            # Some cursors return tuple
            return result[0] if result else 0
        except pymysql.Error as e:
            return {
                'status': False,
                'error': f'Failed to check app_credentials existence: {str(e)}'
            }

    def insert_app_credentials(self, account_name, user_mail, client_id, client_secret, refresh_token, network_code, developer_token, mdb, mdb_name):
        """
        Insert kredensial aplikasi sesuai skema baru ke tabel app_credentials.
        Kolom: account_name, user_mail, client_id, client_secret, refresh_token,
               network_code, developer_token, is_active, mdb, mdb_name, mdd
        Note: account_id is auto-increment, so it's excluded from INSERT
        """
        # Convert network_code to integer if it's a string, or None if empty/invalid
        processed_network_code = None
        if network_code is not None:
            if isinstance(network_code, str):
                # Remove any non-numeric characters and convert to int
                cleaned_code = ''.join(filter(str.isdigit, network_code))
                if cleaned_code:
                    try:
                        processed_network_code = int(cleaned_code)
                    except (ValueError, TypeError):
                        processed_network_code = None
            elif isinstance(network_code, (int, float)):
                processed_network_code = int(network_code)
        
        sql = '''
            INSERT INTO app_credentials (
                account_name, user_mail, client_id, client_secret, refresh_token,
                network_code, developer_token, is_active, mdb, mdb_name, mdd
            ) VALUES (
                %s, %s, %s, %s, %s,
                %s, %s, '1', %s, %s, NOW()
            )
        '''
        try:
            params = (account_name, user_mail, client_id, client_secret, refresh_token,
                      processed_network_code, developer_token, mdb, mdb_name)
            if not self.execute_query(sql, params):
                raise pymysql.Error("Failed to insert app_credentials")

            # Verifikasi baris yang terpengaruh
            affected = getattr(self.cur_hris, 'rowcount', None)
            if affected is not None and affected <= 0:
                raise pymysql.Error(f"No rows inserted for app_credentials (user_mail={user_mail})")

            if not self.commit():
                raise pymysql.Error("Failed to commit app_credentials insert")

            return {
                'status': True,
                'message': f'Successfully inserted app_credentials for {user_mail}'
            }
        except pymysql.Error as e:
            return {
                'status': False,
                'error': f'Failed to insert app_credentials: {str(e)}'
            }

    def update_app_credentials(self, user_mail, account_name, client_id, client_secret, refresh_token, network_code, developer_token, mdb, mdb_name, is_active='1'):
        """
        Update kredensial aplikasi untuk user tertentu sesuai skema baru.
        Note: account_id is auto-increment primary key, so it's excluded from UPDATE
        """
        # Convert network_code to integer if it's a string, or None if empty/invalid
        processed_network_code = None
        if network_code is not None:
            if isinstance(network_code, str):
                # Remove any non-numeric characters and convert to int
                cleaned_code = ''.join(filter(str.isdigit, network_code))
                if cleaned_code:
                    try:
                        processed_network_code = int(cleaned_code)
                    except (ValueError, TypeError):
                        processed_network_code = None
            elif isinstance(network_code, (int, float)):
                processed_network_code = int(network_code)
        
        sql = '''
            UPDATE app_credentials 
            SET account_name = %s,
                client_id = %s,
                client_secret = %s,
                refresh_token = %s,
                network_code = %s,
                developer_token = %s,
                is_active = %s,
                mdb = %s,
                mdb_name = %s,
                mdd = NOW()
            WHERE user_mail = %s
        '''
        try:
            params = (account_name, client_id, client_secret, refresh_token, processed_network_code,
                      developer_token, is_active, mdb, mdb_name, user_mail)
            if not self.execute_query(sql, params):
                raise pymysql.Error("Failed to update app_credentials")

            # Verifikasi baris yang terpengaruh
            affected = getattr(self.cur_hris, 'rowcount', None)
            if affected is not None and affected <= 0:
                raise pymysql.Error(f"No rows updated for app_credentials (user_mail={user_mail})")

            if not self.commit():
                raise pymysql.Error("Failed to commit app_credentials update")

            return {
                'status': True,
                'message': f'Successfully updated app_credentials for {user_mail}'
            }
        except pymysql.Error as e:
            return {
                'status': False,
                'error': f'Failed to update app_credentials: {str(e)}'
            }

    def update_refresh_token(self, user_mail, refresh_token):
        """
        Update refresh token untuk user tertentu
        """
        sql = '''
            UPDATE app_credentials 
            SET refresh_token = %s
            WHERE user_mail = %s
        '''
        try:
            if not self.execute_query(sql, (refresh_token, user_mail)):
                raise pymysql.Error("Failed to update refresh token")
            
            if not self.commit():
                raise pymysql.Error("Failed to commit refresh token update")
            
            return {
                'status': True,
                'message': f'Successfully updated refresh token for {user_mail}'
            }
        except pymysql.Error as e:
            return {
                'status': False,
                'error': f'Failed to update refresh token: {str(e)}'
            }

    def get_all_app_credentials(self):
        """
        Mengambil semua data dari tabel app_credentials
        """
        sql = '''
            SELECT 
                a.account_id,
                a.account_name,
                a.user_mail,
                a.client_id,
                a.client_secret,
                (
                    SELECT GROUP_CONCAT(u.user_alias SEPARATOR '<br>')
                    FROM app_credentials_assign ca
                    LEFT JOIN app_users u ON ca.user_id = u.user_id
                    WHERE ca.account_id = a.account_id
                ) AS assigned_users,
                a.refresh_token,
                a.network_code,
                a.developer_token,
                a.mcm_revenue_share,
                a.is_active,
                a.mdb,
                a.mdb_name,
                a.mdd
            FROM app_credentials a
            ORDER BY a.mdd DESC
        '''
        try:
            if not self.execute_query(sql):
                raise pymysql.Error("Failed to fetch app_credentials data")
            results = self.cur_hris.fetchall()
            return {
                'status': True,
                'data': results if results else []
            }
        except pymysql.Error as e:
            return {
                'status': False,
                'error': f'Database error: {str(e)}'
            }

    def _normalize_subdomain_key(self, raw_value):
        s = str(raw_value or '').strip().lower()
        if not s:
            return ''
        parts = [p for p in s.split('.') if p]
        if len(parts) >= 2:
            return parts[0] + '.' + parts[1]
        return s

    def get_subdomain_platform_summary_for_accounts(self, account_ids=None):
        """
        Ringkas subdomain per account_id dari data_adx_domain / data_adsense_domain.
        Return: {account_id: [{subdomain, platform_code, platform_label}, ...]}
        """
        account_ids = [str(a).strip() for a in (account_ids or []) if str(a or '').strip()]
        id_clause = ''
        params = ()
        if account_ids:
            placeholders = ','.join(['%s'] * len(account_ids))
            id_clause = f' AND account_id IN ({placeholders})'
            params = tuple(account_ids)

        by_account = {}

        def _touch(account_id, raw_domain, source):
            aid = str(account_id or '').strip()
            key = self._normalize_subdomain_key(raw_domain)
            if not aid or not key:
                return
            bucket = by_account.setdefault(aid, {})
            row = bucket.setdefault(key, {
                'subdomain': key,
                'has_adx': False,
                'has_adsense': False,
            })
            if source == 'adx':
                row['has_adx'] = True
            else:
                row['has_adsense'] = True

        try:
            sql_adx = f"""
                SELECT account_id, data_adx_domain AS raw_domain
                FROM data_adx_domain
                WHERE account_id IS NOT NULL
                  AND TRIM(COALESCE(data_adx_domain, '')) <> ''
                  {id_clause}
                GROUP BY account_id, data_adx_domain
            """
            if not self.execute_query(sql_adx, params if params else None):
                raise pymysql.Error('Failed to fetch adx subdomains')
            for row in (self.cur_hris.fetchall() or []):
                try:
                    account_id = row.get('account_id')
                    raw_domain = row.get('raw_domain')
                except AttributeError:
                    account_id, raw_domain = row[0], row[1]
                _touch(account_id, raw_domain, 'adx')

            sql_adsense = f"""
                SELECT account_id, data_adsense_domain AS raw_domain
                FROM data_adsense_domain
                WHERE account_id IS NOT NULL
                  AND TRIM(COALESCE(data_adsense_domain, '')) <> ''
                  {id_clause}
                GROUP BY account_id, data_adsense_domain
            """
            if not self.execute_query(sql_adsense, params if params else None):
                raise pymysql.Error('Failed to fetch adsense subdomains')
            for row in (self.cur_hris.fetchall() or []):
                try:
                    account_id = row.get('account_id')
                    raw_domain = row.get('raw_domain')
                except AttributeError:
                    account_id, raw_domain = row[0], row[1]
                _touch(account_id, raw_domain, 'adsense')

            data = {}
            for aid, domains in by_account.items():
                items = []
                for key in sorted(domains.keys()):
                    item = domains[key]
                    has_adx = bool(item.get('has_adx'))
                    has_adsense = bool(item.get('has_adsense'))
                    if has_adx and has_adsense:
                        platform_code = 'both'
                        platform_label = 'AdX dan AdSense'
                    elif has_adx:
                        platform_code = 'adx'
                        platform_label = 'AdX'
                    elif has_adsense:
                        platform_code = 'adsense'
                        platform_label = 'AdSense'
                    else:
                        continue
                    items.append({
                        'subdomain': key,
                        'platform_code': platform_code,
                        'platform_label': platform_label,
                    })
                data[aid] = items

            return {'status': True, 'data': data}
        except pymysql.Error as e:
            return {
                'status': False,
                'error': f'Database error: {str(e)}',
                'data': {},
            }
    
    def get_all_app_credentials_user(self, user_id):
        """
        Mengambil semua data dari tabel app_credentials untuk user tertentu     
        """
        sql = '''
            SELECT 
                a.account_id,
                x.user_id,
                a.account_name,
                a.user_mail,
                a.client_id,
                a.client_secret,
                (
                    SELECT GROUP_CONCAT(u.user_alias SEPARATOR '<br>')
                    FROM app_credentials_assign ca
                    LEFT JOIN app_users u ON ca.user_id = u.user_id
                    WHERE ca.account_id = a.account_id
                ) AS assigned_users,
                a.refresh_token,
                a.network_code,
                a.developer_token,
                a.mcm_revenue_share,
                a.is_active,
                a.mdb,
                a.mdb_name,
                a.mdd
            FROM app_credentials a
            INNER JOIN app_credentials_assign x ON a.account_id = x.account_id
            WHERE x.user_id = %s
            ORDER BY a.mdd DESC
        '''
        try:
            if not self.execute_query(sql, (user_id,)): 
                raise pymysql.Error("Failed to fetch app_credentials data")
            results = self.cur_hris.fetchall()
            return {
                'status': True,
                'data': results if results else []
            }
        except pymysql.Error as e:
            return {
                'status': False,
                'error': f'Database error: {str(e)}'
            }

    def assign_account_user(self, params):
        try:
            insert_query = """
                INSERT INTO app_credentials_assign (account_id, user_id, mdb, mdb_name, mdd)
                VALUES (%s, %s, %s, %s, NOW())
            """
            values = (
                params['account_id'],
                params['user_id'],
                params['mdb'],
                params['mdb_name']
            )

            try:
                self.cur_hris.execute(insert_query, values)
                
            except pymysql.Error as e:
                return {
                    'status': False,
                    'message': f'MySQL error: {str(e)}'
                }

            self.commit()
            return {
                'status': True,
                'message': 'Account user berhasil diassign'
            }

        except pymysql.Error as e:
            return {
                'status': False,
                'message': f'Database error: {str(e)}'
            }

    def update_account_name(self, user_mail, new_account_name, new_mcm_revenue_share):
        """Update account name for a specific user"""
        try:
            # Check if user exists
            check_query = "SELECT user_mail FROM app_credentials WHERE user_mail = %s"
            if not self.execute_query(check_query, (user_mail)):
                return {
                    'status': False,
                    'message': 'Database error saat mengecek user'
                }
            
            result = self.cur_hris.fetchone()
            
            if not result:
                return {
                    'status': False,
                    'message': 'User tidak ditemukan'
                }
            
            # Update account name
            update_query = """
                UPDATE app_credentials 
                SET account_name = %s, mcm_revenue_share = %s, mdd = NOW() 
                WHERE user_mail = %s
            """
            
            if not self.execute_query(update_query, (new_account_name, new_mcm_revenue_share, user_mail)):
                return {
                    'status': False,
                    'message': 'Database error saat mengupdate account name'
                }
            
            if not self.commit():
                return {
                    'status': False,
                    'message': 'Gagal menyimpan perubahan'
                }
            
            if self.cur_hris.rowcount > 0:
                return {
                    'status': True,
                    'message': 'Account name berhasil diupdate'
                }

            else:
                return {
                    'status': False,
                    'message': 'Tidak ada data yang diupdate'
                }
                
        except pymysql.Error as e:
            return {
                'status': False,
                'message': f'Database error: {str(e)}'
            }


    def delete_adx_account_credentials(self, user_mail, mdb=None, mdb_name=None):
        try:
            sql_get = "SELECT account_id FROM app_credentials WHERE user_mail = %s"
            if not self.execute_query(sql_get, (user_mail,)):
                return {'status': False, 'message': 'Database error saat mengambil account_id'}

            row = self.cur_hris.fetchone()
            if not row:
                return {'status': False, 'message': 'Kredensial tidak ditemukan'}

            account_id = row.get('account_id') if isinstance(row, dict) else row[0]

            sql_del_assign = "DELETE FROM app_credentials_assign WHERE account_id = %s"
            if not self.execute_query(sql_del_assign, (account_id,)):
                return {'status': False, 'message': 'Database error saat menghapus assignment'}

            sql_del_cred = "DELETE FROM app_credentials WHERE user_mail = %s"
            if not self.execute_query(sql_del_cred, (user_mail,)):
                return {'status': False, 'message': 'Database error saat menghapus kredensial'}

            if not self.commit():
                return {'status': False, 'message': 'Gagal menyimpan perubahan'}

            if getattr(self.cur_hris, 'rowcount', 0) <= 0:
                return {'status': False, 'message': 'Tidak ada data yang dihapus'}

            return {'status': True, 'message': 'Kredensial berhasil dihapus'}

        except pymysql.Error as e:
            return {'status': False, 'message': f'Database error: {str(e)}'}

    # CRON JOB
    def insert_data_master_ads(self, data):
        try:
            sql_insert = """
                        INSERT INTO master_ads
                        (
                            master_ads.master_date,
                            master_ads.account_ads_id,
                            master_ads.master_domain,
                            master_ads.master_campaign_id,
                            master_ads.master_campaign_nm,
                            master_ads.master_budget,
                            master_ads.master_date_start,
                            master_ads.master_date_end,
                            master_ads.master_status,
                            master_ads.mdb,
                            master_ads.mdb_name,
                            master_ads.mdd
                        )
                    VALUES
                        (
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s
                        )
                """
            if not self.execute_query(sql_insert, (
                data['master_date'],
                data['account_ads_id'],
                data['master_domain'],
                data['master_campaign_id'],
                data['master_campaign_nm'],
                data['master_budget'],
                data['master_date_start'],
                data['master_date_end'],
                data['master_status'],
                data['mdb'],
                data['mdb_name'],
                data['mdd']
            )):
                raise pymysql.Error("Failed to insert data master ads campaign")
            if not self.commit():
                raise pymysql.Error("Failed to commit data master ads campaign insert")
            
            hasil = {
                "status": True,
                "message": "Data master ads campaign berhasil ditambahkan"
            }
        except pymysql.Error as e:
            hasil = {
                "status": False,
                'data': 'Terjadi error {!r}, error nya {}'.format(e, e.args[0])
            }
        return {'hasil': hasil}

    def insert_data_ads_campaign(self, data):
        try:
            sql_insert = """
                        INSERT INTO data_ads_campaign
                        (
                            data_ads_campaign.account_ads_id,
                            data_ads_campaign.data_ads_domain,
                            data_ads_campaign.data_ads_campaign_id,
                            data_ads_campaign.data_ads_campaign_nm,
                            data_ads_campaign.data_ads_tanggal,
                            data_ads_campaign.data_ads_spend,
                            data_ads_campaign.data_ads_impresi,
                            data_ads_campaign.data_ads_click,
                            data_ads_campaign.data_ads_reach,
                            data_ads_campaign.data_ads_cpr,
                            data_ads_campaign.data_ads_cpc,
                            data_ads_campaign.data_ads_frekuensi,
                            data_ads_campaign.data_ads_lpv,
                            data_ads_campaign.data_ads_lpv_rate,
                            data_ads_campaign.mdb,
                            data_ads_campaign.mdb_name,
                            data_ads_campaign.mdd
                        )
                    VALUES
                        (
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s
                        )
                """
            if not self.execute_query(sql_insert, (
                data['account_ads_id'],
                data['data_ads_domain'],
                data['data_ads_campaign_id'],
                data['data_ads_campaign_nm'],
                data['data_ads_tanggal'],
                data['data_ads_spend'],
                data['data_ads_impresi'],
                data['data_ads_click'],
                data['data_ads_reach'],
                data['data_ads_cpr'],
                data['data_ads_cpc'],
                data['data_ads_frekuensi'],
                data['data_ads_lpv'],
                data['data_ads_lpv_rate'],
                data['mdb'],
                data['mdb_name'],
                data['mdd']
            )):
                raise pymysql.Error("Failed to insert data ads campaign")
            if not self.commit():
                raise pymysql.Error("Failed to commit data ads campaign insert")
            
            hasil = {
                "status": True,
                "message": "Data ads campaign berhasil ditambahkan"
            }
        except pymysql.Error as e:
            hasil = {
                "status": False,
                'data': 'Terjadi error {!r}, error nya {}'.format(e, e.args[0])
            }
        return {'hasil': hasil}

    def delete_data_ads_rekap(self, account, domain, tahun, bulan, tanggal_tarik):
        try:
            sql_delete = """
                        DELETE FROM data_ads_rekap
                        WHERE account_ads_id = %s
                        AND data_ads_domain = %s
                        AND data_ads_rekap_tahun = %s
                        AND data_ads_rekap_bulan = %s
                        AND data_ads_rekap_tanggal = %s
                """
            if not self.execute_query(sql_delete, (account, domain, tahun, bulan, tanggal_tarik)):
                raise pymysql.Error("Failed to delete data ads rekap")

            affected = self.cur_hris.rowcount if self.cur_hris else 0

            if not self.commit():
                raise pymysql.Error("Failed to commit delete data ads rekap")

            hasil = {
                "status": True,
                "message": f"Berhasil menghapus {affected} baris rekap",
                "affected": affected
            }
        except pymysql.Error as e:
            hasil = {
                "status": False,
                'data': 'Terjadi error {!r}, error nya {}'.format(e, e.args[0] if e.args else e)
            }
        return {'hasil': hasil}

    def insert_data_ads_rekap(self, data):
        try:
            sql_insert = """
                        INSERT INTO data_ads_rekap
                        (
                            data_ads_rekap.data_ads_rekap_id,
                            data_ads_rekap.account_ads_id,
                            data_ads_rekap.data_ads_domain,
                            data_ads_rekap.data_ads_rekap_tahun,
                            data_ads_rekap.data_ads_rekap_bulan,
                            data_ads_rekap.data_ads_rekap_tanggal,
                            data_ads_rekap.data_ads_rekap_spend,
                            data_ads_rekap.data_ads_rekap_impresi,
                            data_ads_rekap.data_ads_rekap_click,
                            data_ads_rekap.data_ads_rekap_reach,
                            data_ads_rekap.data_ads_rekap_cpr,
                            data_ads_rekap.data_ads_rekap_cpc,
                            data_ads_rekap.data_ads_rekap_frekuensi,
                            data_ads_rekap.data_ads_rekap_lpv,
                            data_ads_rekap.data_ads_rekap_lpv_rate,
                            data_ads_rekap.mdb,
                            data_ads_rekap.mdb_name,
                            data_ads_rekap.mdd
                        )
                    VALUES
                        (
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s, %s, %s, %s
                        )
                """
            if not self.execute_query(sql_insert, (
                data['data_ads_rekap_id'],
                data['account_ads_id'],
                data['data_ads_domain'],
                data['data_ads_rekap_tahun'],
                data['data_ads_rekap_bulan'],
                data['data_ads_rekap_tanggal'],
                data['data_ads_rekap_spend'],
                data['data_ads_rekap_impresi'],
                data['data_ads_rekap_click'],
                data['data_ads_rekap_reach'],
                data['data_ads_rekap_cpr'],
                data['data_ads_rekap_cpc'],
                data['data_ads_rekap_frekuensi'],
                data['data_ads_rekap_lpv'],
                data['data_ads_rekap_lpv_rate'],
                data['mdb'],
                data['mdb_name'],
                data['mdd']
            )):
                raise pymysql.Error("Failed to insert data ads rekap")
            if not self.commit():
                raise pymysql.Error("Failed to commit data ads rekap insert")

            hasil = {
                "status": True,
                "message": "Data ads rekap berhasil ditambahkan"
            }
        except pymysql.Error as e:
            hasil = {
                "status": False,
                'data': 'Terjadi error {!r}, error nya {}'.format(e, e.args[0] if e.args else e)
            }
        return {'hasil': hasil}

    def delete_data_adsense_rekap(self, account_id, domain, tahun, bulan, tanggal_tarik):
        try:
            sql_delete = """
                        DELETE FROM data_adsense_rekap
                        WHERE account_id = %s
                        AND data_adsense_rekap_domain = %s
                        AND data_adsense_rekap_tahun = %s
                        AND data_adsense_rekap_bulan = %s
                        AND data_adsense_rekap_tanggal = %s
                """
            if not self.execute_query(sql_delete, (account_id, domain, tahun, bulan, tanggal_tarik)):
                raise pymysql.Error("Failed to delete data adsense rekap")
            affected = self.cur_hris.rowcount if self.cur_hris else 0
            if not self.commit():
                raise pymysql.Error("Failed to commit delete data adsense rekap")
            hasil = {
                "status": True,
                "message": f"Berhasil menghapus {affected} baris rekap AdSense",
                "affected": affected
            }
        except pymysql.Error as e:
            hasil = {
                "status": False,
                'data': 'Terjadi error {!r}, error nya {}'.format(e, e.args[0] if e.args else e)
            }
        return {'hasil': hasil}

    def insert_data_adsense_rekap(self, data):
        try:
            sql_insert = """
                        INSERT INTO data_adsense_rekap
                        (
                            data_adsense_rekap_id,
                            account_id,
                            data_adsense_rekap_tahun,
                            data_adsense_rekap_bulan,
                            data_adsense_rekap_tanggal,
                            data_adsense_rekap_domain,
                            data_adsense_rekap_impresi,
                            data_adsense_rekap_click,
                            data_adsense_rekap_cpc,
                            data_adsense_rekap_ctr,
                            data_adsense_rekap_cpm,
                            data_adsense_rekap_page_views,
                            data_adsense_rekap_page_views_rpm,
                            data_adsense_rekap_ad_requests,
                            data_adsense_rekap_ad_requests_coverage,
                            data_adsense_rekap_active_view_viewability,
                            data_adsense_rekap_active_view_measurability,
                            data_adsense_rekap_active_view_time,
                            data_adsense_rekap_revenue,
                            mdb,
                            mdb_name,
                            mdd
                        )
                    VALUES
                        (
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                        )
                """
            if not self.execute_query(sql_insert, (
                data['data_adsense_rekap_id'],
                data['account_id'],
                data['data_adsense_rekap_tahun'],
                data['data_adsense_rekap_bulan'],
                data['data_adsense_rekap_tanggal'],
                data['data_adsense_rekap_domain'],
                data['data_adsense_rekap_impresi'],
                data['data_adsense_rekap_click'],
                data['data_adsense_rekap_cpc'],
                data['data_adsense_rekap_ctr'],
                data['data_adsense_rekap_cpm'],
                data['data_adsense_rekap_page_views'],
                data['data_adsense_rekap_page_views_rpm'],
                data['data_adsense_rekap_ad_requests'],
                data['data_adsense_rekap_ad_requests_coverage'],
                data['data_adsense_rekap_active_view_viewability'],
                data['data_adsense_rekap_active_view_measurability'],
                data['data_adsense_rekap_active_view_time'],
                data['data_adsense_rekap_revenue'],
                data['mdb'],
                data['mdb_name'],
                data['mdd']
            )):
                raise pymysql.Error("Failed to insert data adsense rekap")
            if not self.commit():
                raise pymysql.Error("Failed to commit data adsense rekap insert")
            hasil = {
                "status": True,
                "message": "Data adsense rekap berhasil ditambahkan"
            }
        except pymysql.Error as e:
            hasil = {
                "status": False,
                'data': 'Terjadi error {!r}, error nya {}'.format(e, e.args[0] if e.args else e)
            }
        return {'hasil': hasil}

    def delete_data_adx_rekap(self, account_id, domain, tahun, bulan, tanggal_tarik):
        try:
            sql_delete = """
                        DELETE FROM data_adx_rekap
                        WHERE account_id = %s
                        AND data_adx_rekap_domain = %s
                        AND data_adx_rekap_tahun = %s
                        AND data_adx_rekap_bulan = %s
                        AND data_adx_rekap_tanggal = %s
                """
            if not self.execute_query(sql_delete, (account_id, domain, tahun, bulan, tanggal_tarik)):
                raise pymysql.Error("Failed to delete data adx rekap")
            affected = self.cur_hris.rowcount if self.cur_hris else 0
            if not self.commit():
                raise pymysql.Error("Failed to commit delete data adx rekap")
            hasil = {
                "status": True,
                "message": f"Berhasil menghapus {affected} baris rekap AdX",
                "affected": affected
            }
        except pymysql.Error as e:
            hasil = {
                "status": False,
                'data': 'Terjadi error {!r}, error nya {}'.format(e, e.args[0] if e.args else e)
            }
        return {'hasil': hasil}

    def insert_data_adx_rekap(self, data):
        try:
            sql_insert = """
                        INSERT INTO data_adx_rekap
                        (
                            data_adx_rekap_id,
                            account_id,
                            data_adx_rekap_tahun,
                            data_adx_rekap_bulan,
                            data_adx_rekap_tanggal,
                            data_adx_rekap_domain,
                            data_adx_rekap_impresi,
                            data_adx_rekap_click,
                            data_adx_rekap_cpc,
                            data_adx_rekap_ctr,
                            data_adx_rekap_cpm,
                            data_adx_rekap_ecpm,
                            data_adx_rekap_total_requests,
                            data_adx_rekap_responses_served,
                            data_adx_rekap_match_rate,
                            data_adx_rekap_fill_rate,
                            data_adx_rekap_active_view_pct_viewable,
                            data_adx_rekap_active_view_avg_time_sec,
                            data_adx_rekap_revenue,
                            mdb,
                            mdb_name,
                            mdd
                        )
                    VALUES
                        (
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                        )
                """
            if not self.execute_query(sql_insert, (
                data['data_adx_rekap_id'],
                data['account_id'],
                data['data_adx_rekap_tahun'],
                data['data_adx_rekap_bulan'],
                data['data_adx_rekap_tanggal'],
                data['data_adx_rekap_domain'],
                data['data_adx_rekap_impresi'],
                data['data_adx_rekap_click'],
                data['data_adx_rekap_cpc'],
                data['data_adx_rekap_ctr'],
                data['data_adx_rekap_cpm'],
                data['data_adx_rekap_ecpm'],
                data['data_adx_rekap_total_requests'],
                data['data_adx_rekap_responses_served'],
                data['data_adx_rekap_match_rate'],
                data['data_adx_rekap_fill_rate'],
                data['data_adx_rekap_active_view_pct_viewable'],
                data['data_adx_rekap_active_view_avg_time_sec'],
                data['data_adx_rekap_revenue'],
                data['mdb'],
                data['mdb_name'],
                data['mdd']
            )):
                raise pymysql.Error("Failed to insert data adx rekap")
            if not self.commit():
                raise pymysql.Error("Failed to commit data adx rekap insert")
            hasil = {
                "status": True,
                "message": "Data adx rekap berhasil ditambahkan"
            }
        except pymysql.Error as e:
            hasil = {
                "status": False,
                'data': 'Terjadi error {!r}, error nya {}'.format(e, e.args[0] if e.args else e)
            }
        return {'hasil': hasil}

    def _normalize_domain_full(self, raw_value):
        s = str(raw_value or '').strip().lower()
        if not s:
            return ''
        s = re.sub(r'^https?://', '', s)
        s = s.split('/')[0].split('?')[0].split('#')[0]
        s = re.sub(r'^www\.', '', s)
        return s

    def _normalize_domain_match_key(self, raw_value):
        s = self._normalize_domain_full(raw_value)
        if not s:
            return ''
        parts = [p for p in s.split('.') if p]
        if len(parts) >= 2:
            return parts[0] + '.' + parts[1]
        return s

    def _domain_match_sql(self, column_expr):
        return (
            f"LOWER(SUBSTRING_INDEX(REPLACE(REPLACE(LOWER(TRIM({column_expr})), 'www.', ''), 'https://', ''), '.', 2)) = %s"
        )

    def _domain_filter_sql(self, column_expr, domain):
        full = self._normalize_domain_full(domain)
        parts = [p for p in full.split('.') if p]
        if len(parts) > 2:
            return (
                f"LOWER(TRIM(REPLACE(REPLACE(LOWER(TRIM({column_expr})), 'www.', ''), 'https://', ''))) = %s",
                [full],
            )
        key = self._normalize_domain_match_key(full)
        return self._domain_match_sql(column_expr), [key]

    def _compare_rekap_metric(self, daily_val, rekap_val):
        daily = float(daily_val or 0)
        rekap = float(rekap_val or 0)
        delta = rekap - daily
        if daily == 0 and rekap == 0:
            return {
                'daily': daily,
                'rekap': rekap,
                'delta': delta,
                'delta_pct': 0.0,
                'status': 'ok',
            }
        if daily == 0:
            return {
                'daily': daily,
                'rekap': rekap,
                'delta': delta,
                'delta_pct': 100.0 if rekap else 0.0,
                'status': 'invalid' if rekap else 'ok',
            }
        delta_pct = (delta / daily) * 100.0
        ad = abs(delta_pct)
        if ad <= 1.0:
            status = 'ok'
        elif ad <= 5.0:
            status = 'warn'
        else:
            status = 'invalid'
        return {
            'daily': daily,
            'rekap': rekap,
            'delta': delta,
            'delta_pct': delta_pct,
            'status': status,
        }

    def _fetch_sum_row(self, sql, params):
        self.cur_hris.execute(sql, tuple(params))
        row = self.cur_hris.fetchone() or {}
        return {k: float(row.get(k) or 0) for k in row.keys()}

    def _domain_fb_filter_sql(self, column_expr, domain):
        """Filter FB ads by 2-level join key — selaras ROI / invalid report list."""
        key = self._normalize_domain_match_key(domain)
        if not key:
            return '1=0', []
        return self._domain_match_sql(column_expr), [key]

    def _domain_clause_for_column(self, column_expr, domain):
        if column_expr == 'data_ads_domain':
            return self._domain_fb_filter_sql(column_expr, domain)
        return self._domain_filter_sql(column_expr, domain)

    def _resolve_rekap_tarik_date(self, table, date_col, year_col, month_col, domain_col, domain, year, month, tanggal_tarik=None):
        if tanggal_tarik:
            return str(tanggal_tarik).strip()
        clause, domain_params = self._domain_clause_for_column(domain_col, domain)
        sql = f"""
            SELECT MAX({date_col}) AS tanggal_tarik
            FROM {table}
            WHERE {year_col} = %s
              AND {month_col} = %s
              AND {clause}
        """
        self.cur_hris.execute(sql, (year, month, *domain_params))
        row = self.cur_hris.fetchone() or {}
        val = row.get('tanggal_tarik')
        if hasattr(val, 'isoformat'):
            return val.isoformat()
        return str(val or '').strip()

    def _list_rekap_tarik_dates(self, table, date_col, year_col, month_col, domain_col, domain, year, month):
        clause, domain_params = self._domain_clause_for_column(domain_col, domain)
        sql = f"""
            SELECT DISTINCT {date_col} AS tanggal_tarik
            FROM {table}
            WHERE {year_col} = %s
              AND {month_col} = %s
              AND {clause}
            ORDER BY {date_col} DESC
        """
        self.cur_hris.execute(sql, (year, month, *domain_params))
        rows = self.cur_hris.fetchall() or []
        out = []
        for row in rows:
            val = row.get('tanggal_tarik')
            if hasattr(val, 'isoformat'):
                out.append(val.isoformat())
            elif val:
                out.append(str(val).strip())
        return out

    def get_rekap_vs_daily_compare(self, domain, year, month, tanggal_tarik=None):
        """Compare monthly recap pulls vs summed daily tables for a domain."""
        import calendar

        try:
            domain_full = self._normalize_domain_full(domain)
            domain_key = self._normalize_domain_match_key(domain)
            if not domain_key:
                return {'status': False, 'data': 'Domain wajib diisi'}

            year = str(year or '').strip()
            month = str(month or '').strip().zfill(2)
            if not year.isdigit() or not month.isdigit():
                return {'status': False, 'data': 'Tahun/bulan tidak valid'}
            month_int = int(month)
            if month_int < 1 or month_int > 12:
                return {'status': False, 'data': 'Bulan tidak valid'}

            last_day = calendar.monthrange(int(year), month_int)[1]
            start_date = f"{year}-{month}-01"
            end_date = f"{year}-{month}-{last_day:02d}"
            ads_clause, ads_params = self._domain_fb_filter_sql('data_ads_domain', domain)
            adsense_daily_clause, adsense_daily_params = self._domain_filter_sql('data_adsense_domain', domain)
            adsense_rekap_clause, adsense_rekap_params = self._domain_filter_sql('data_adsense_rekap_domain', domain)
            adx_daily_clause, adx_daily_params = self._domain_filter_sql('data_adx_domain', domain)
            adx_rekap_clause, adx_rekap_params = self._domain_filter_sql('data_adx_rekap_domain', domain)
            # Legacy names kept for hot-reload safety during partial refactors.
            domain_clause_ads = ads_clause
            domain_clause_adsense_daily = adsense_daily_clause
            domain_clause_adsense_rekap = adsense_rekap_clause
            domain_clause_adx_daily = adx_daily_clause
            domain_clause_adx_rekap = adx_rekap_clause

            def build_metrics(metric_defs, daily_row, rekap_row):
                metrics = []
                summary = {'ok': 0, 'warn': 0, 'invalid': 0, 'missing': 0}
                for item in metric_defs:
                    key = item['key']
                    daily_val = daily_row.get(key, 0)
                    rekap_val = rekap_row.get(key, 0)
                    if not daily_row.get('_has_data') and not rekap_row.get('_has_data'):
                        row = {
                            'key': key,
                            'label': item['label'],
                            'kind': item.get('kind', 'number'),
                            'daily': 0,
                            'rekap': 0,
                            'delta': 0,
                            'delta_pct': 0,
                            'status': 'missing',
                        }
                        summary['missing'] += 1
                    else:
                        row = self._compare_rekap_metric(daily_val, rekap_val)
                        row['key'] = key
                        row['label'] = item['label']
                        row['kind'] = item.get('kind', 'number')
                        summary[row['status']] = summary.get(row['status'], 0) + 1
                    metrics.append(row)
                return metrics, summary

            sections = []
            all_tarik_dates = set()

            # Facebook Ads
            fb_tarik = self._resolve_rekap_tarik_date(
                'data_ads_rekap',
                'data_ads_rekap_tanggal',
                'data_ads_rekap_tahun',
                'data_ads_rekap_bulan',
                'data_ads_domain',
                domain,
                year,
                month,
                tanggal_tarik,
            )
            for d in self._list_rekap_tarik_dates(
                'data_ads_rekap',
                'data_ads_rekap_tanggal',
                'data_ads_rekap_tahun',
                'data_ads_rekap_bulan',
                'data_ads_domain',
                domain,
                year,
                month,
            ):
                all_tarik_dates.add(d)

            fb_daily = {'_has_data': False}
            sql_fb_daily = f"""
                    SELECT
                        COALESCE(SUM(CAST(data_ads_spend AS DECIMAL(18,4))), 0) AS spend,
                        COALESCE(SUM(CAST(data_ads_impresi AS DECIMAL(18,4))), 0) AS impresi,
                        COALESCE(SUM(CAST(data_ads_click AS DECIMAL(18,4))), 0) AS click,
                        COALESCE(SUM(CAST(data_ads_reach AS DECIMAL(18,4))), 0) AS reach,
                        COALESCE(SUM(CAST(data_ads_lpv AS DECIMAL(18,4))), 0) AS lpv
                    FROM data_ads_campaign
                    WHERE DATE(data_ads_tanggal) BETWEEN %s AND %s
                      AND {ads_clause}
                """
            fb_daily = self._fetch_sum_row(sql_fb_daily, [start_date, end_date, *ads_params])
            fb_daily['_has_data'] = any(fb_daily.get(k, 0) for k in ['spend', 'impresi', 'click', 'reach', 'lpv'])

            fb_rekap = {'_has_data': False}
            if fb_tarik:
                sql_fb_rekap = f"""
                    SELECT
                        COALESCE(SUM(CAST(data_ads_rekap_spend AS DECIMAL(18,4))), 0) AS spend,
                        COALESCE(SUM(CAST(data_ads_rekap_impresi AS DECIMAL(18,4))), 0) AS impresi,
                        COALESCE(SUM(CAST(data_ads_rekap_click AS DECIMAL(18,4))), 0) AS click,
                        COALESCE(SUM(CAST(data_ads_rekap_reach AS DECIMAL(18,4))), 0) AS reach,
                        COALESCE(SUM(CAST(data_ads_rekap_lpv AS DECIMAL(18,4))), 0) AS lpv
                    FROM data_ads_rekap
                    WHERE data_ads_rekap_tahun = %s
                      AND data_ads_rekap_bulan = %s
                      AND data_ads_rekap_tanggal = %s
                      AND {ads_clause}
                """
                fb_rekap = self._fetch_sum_row(sql_fb_rekap, [year, month, fb_tarik, *ads_params])
                fb_rekap['_has_data'] = any(fb_rekap.get(k, 0) for k in ['spend', 'impresi', 'click', 'reach', 'lpv'])

            fb_defs = [
                {'key': 'spend', 'label': 'Spend', 'kind': 'money'},
                {'key': 'impresi', 'label': 'Impresi', 'kind': 'number'},
                {'key': 'click', 'label': 'Click', 'kind': 'number'},
                {'key': 'reach', 'label': 'Reach', 'kind': 'number'},
                {'key': 'lpv', 'label': 'LPV', 'kind': 'number'},
            ]
            fb_metrics, fb_summary = build_metrics(fb_defs, fb_daily, fb_rekap)
            sections.append({
                'key': 'facebook_ads',
                'label': 'Facebook Ads',
                'tanggal_tarik': fb_tarik or None,
                'has_daily': bool(fb_daily.get('_has_data')),
                'has_rekap': bool(fb_rekap.get('_has_data')),
                'metrics': fb_metrics,
                'summary': fb_summary,
            })

            # AdSense
            adsense_tarik = self._resolve_rekap_tarik_date(
                'data_adsense_rekap',
                'data_adsense_rekap_tanggal',
                'data_adsense_rekap_tahun',
                'data_adsense_rekap_bulan',
                'data_adsense_rekap_domain',
                domain,
                year,
                month,
                tanggal_tarik,
            )
            for d in self._list_rekap_tarik_dates(
                'data_adsense_rekap',
                'data_adsense_rekap_tanggal',
                'data_adsense_rekap_tahun',
                'data_adsense_rekap_bulan',
                'data_adsense_rekap_domain',
                domain,
                year,
                month,
            ):
                all_tarik_dates.add(d)

            sql_adsense_daily = f"""
                SELECT
                    COALESCE(SUM(CAST(data_adsense_impresi AS DECIMAL(18,4))), 0) AS impresi,
                    COALESCE(SUM(CAST(data_adsense_click AS DECIMAL(18,4))), 0) AS click,
                    COALESCE(SUM(CAST(data_adsense_revenue AS DECIMAL(18,4))), 0) AS revenue,
                    COALESCE(SUM(CAST(data_adsense_page_views AS DECIMAL(18,4))), 0) AS page_views,
                    COALESCE(SUM(CAST(data_adsense_ad_requests AS DECIMAL(18,4))), 0) AS ad_requests
                FROM data_adsense_domain
                WHERE DATE(data_adsense_tanggal) BETWEEN %s AND %s
                  AND {adsense_daily_clause}
            """
            adsense_daily = self._fetch_sum_row(sql_adsense_daily, [start_date, end_date, *adsense_daily_params])
            adsense_daily['_has_data'] = any(
                adsense_daily.get(k, 0) for k in ['impresi', 'click', 'revenue', 'page_views', 'ad_requests']
            )

            adsense_rekap = {'_has_data': False}
            if adsense_tarik:
                sql_adsense_rekap = f"""
                    SELECT
                        COALESCE(SUM(CAST(data_adsense_rekap_impresi AS DECIMAL(18,4))), 0) AS impresi,
                        COALESCE(SUM(CAST(data_adsense_rekap_click AS DECIMAL(18,4))), 0) AS click,
                        COALESCE(SUM(CAST(data_adsense_rekap_revenue AS DECIMAL(18,4))), 0) AS revenue,
                        COALESCE(SUM(CAST(data_adsense_rekap_page_views AS DECIMAL(18,4))), 0) AS page_views,
                        COALESCE(SUM(CAST(data_adsense_rekap_ad_requests AS DECIMAL(18,4))), 0) AS ad_requests
                    FROM data_adsense_rekap
                    WHERE data_adsense_rekap_tahun = %s
                      AND data_adsense_rekap_bulan = %s
                      AND data_adsense_rekap_tanggal = %s
                      AND {adsense_rekap_clause}
                """
                adsense_rekap = self._fetch_sum_row(sql_adsense_rekap, [year, month, adsense_tarik, *adsense_rekap_params])
                adsense_rekap['_has_data'] = any(
                    adsense_rekap.get(k, 0) for k in ['impresi', 'click', 'revenue', 'page_views', 'ad_requests']
                )

            adsense_defs = [
                {'key': 'revenue', 'label': 'Revenue', 'kind': 'money'},
                {'key': 'impresi', 'label': 'Impresi', 'kind': 'number'},
                {'key': 'click', 'label': 'Click', 'kind': 'number'},
                {'key': 'page_views', 'label': 'Page Views', 'kind': 'number'},
                {'key': 'ad_requests', 'label': 'Ad Requests', 'kind': 'number'},
            ]
            adsense_metrics, adsense_summary = build_metrics(adsense_defs, adsense_daily, adsense_rekap)
            sections.append({
                'key': 'adsense',
                'label': 'AdSense',
                'tanggal_tarik': adsense_tarik or None,
                'has_daily': bool(adsense_daily.get('_has_data')),
                'has_rekap': bool(adsense_rekap.get('_has_data')),
                'metrics': adsense_metrics,
                'summary': adsense_summary,
            })

            # AdX
            adx_tarik = self._resolve_rekap_tarik_date(
                'data_adx_rekap',
                'data_adx_rekap_tanggal',
                'data_adx_rekap_tahun',
                'data_adx_rekap_bulan',
                'data_adx_rekap_domain',
                domain,
                year,
                month,
                tanggal_tarik,
            )
            for d in self._list_rekap_tarik_dates(
                'data_adx_rekap',
                'data_adx_rekap_tanggal',
                'data_adx_rekap_tahun',
                'data_adx_rekap_bulan',
                'data_adx_rekap_domain',
                domain,
                year,
                month,
            ):
                all_tarik_dates.add(d)

            sql_adx_daily = f"""
                SELECT
                    COALESCE(SUM(CAST(data_adx_domain_impresi AS DECIMAL(18,4))), 0) AS impresi,
                    COALESCE(SUM(CAST(data_adx_domain_click AS DECIMAL(18,4))), 0) AS click,
                    COALESCE(SUM(CAST(data_adx_domain_revenue AS DECIMAL(18,4))), 0) AS revenue,
                    COALESCE(SUM(CAST(data_adx_domain_total_requests AS DECIMAL(18,4))), 0) AS total_requests,
                    COALESCE(SUM(CAST(data_adx_domain_responses_served AS DECIMAL(18,4))), 0) AS responses_served
                FROM data_adx_domain
                WHERE DATE(data_adx_domain_tanggal) BETWEEN %s AND %s
                  AND {adx_daily_clause}
            """
            adx_daily = self._fetch_sum_row(sql_adx_daily, [start_date, end_date, *adx_daily_params])
            adx_daily['_has_data'] = any(
                adx_daily.get(k, 0) for k in ['impresi', 'click', 'revenue', 'total_requests', 'responses_served']
            )

            adx_rekap = {'_has_data': False}
            if adx_tarik:
                sql_adx_rekap = (
                    "SELECT"
                    " COALESCE(SUM(CAST(data_adx_rekap_impresi AS DECIMAL(18,4))), 0) AS impresi,"
                    " COALESCE(SUM(CAST(data_adx_rekap_click AS DECIMAL(18,4))), 0) AS click,"
                    " COALESCE(SUM(CAST(data_adx_rekap_revenue AS DECIMAL(18,4))), 0) AS revenue,"
                    " COALESCE(SUM(CAST(data_adx_rekap_total_requests AS DECIMAL(18,4))), 0) AS total_requests,"
                    " COALESCE(SUM(CAST(data_adx_rekap_responses_served AS DECIMAL(18,4))), 0) AS responses_served"
                    " FROM data_adx_rekap"
                    " WHERE data_adx_rekap_tahun = %s"
                    " AND data_adx_rekap_bulan = %s"
                    " AND data_adx_rekap_tanggal = %s"
                    " AND " + adx_rekap_clause
                )
                adx_rekap = self._fetch_sum_row(sql_adx_rekap, [year, month, adx_tarik, *adx_rekap_params])
                adx_rekap['_has_data'] = any(
                    adx_rekap.get(k, 0) for k in ['impresi', 'click', 'revenue', 'total_requests', 'responses_served']
                )

            adx_defs = [
                {'key': 'revenue', 'label': 'Revenue', 'kind': 'money'},
                {'key': 'impresi', 'label': 'Impresi', 'kind': 'number'},
                {'key': 'click', 'label': 'Click', 'kind': 'number'},
                {'key': 'total_requests', 'label': 'Requests', 'kind': 'number'},
                {'key': 'responses_served', 'label': 'Responses', 'kind': 'number'},
            ]
            adx_metrics, adx_summary = build_metrics(adx_defs, adx_daily, adx_rekap)
            sections.append({
                'key': 'adx',
                'label': 'AdX',
                'tanggal_tarik': adx_tarik or None,
                'has_daily': bool(adx_daily.get('_has_data')),
                'has_rekap': bool(adx_rekap.get('_has_data')),
                'metrics': adx_metrics,
                'summary': adx_summary,
            })

            overall = {'ok': 0, 'warn': 0, 'invalid': 0, 'missing': 0}
            for sec in sections:
                for k in overall.keys():
                    overall[k] += int((sec.get('summary') or {}).get(k, 0))

            resolved_tarik = str(tanggal_tarik or '').strip() or fb_tarik or adsense_tarik or adx_tarik or ''
            available_tarik = sorted(list(all_tarik_dates), reverse=True)

            return {
                'status': True,
                'data': {
                    'domain': str(domain or '').strip(),
                    'domain_key': domain_key,
                    'domain_full': domain_full,
                    'year': year,
                    'month': month,
                    'period': {'start': start_date, 'end': end_date},
                    'tanggal_tarik': resolved_tarik or None,
                    'available_tarik_dates': available_tarik,
                    'summary': overall,
                    'sections': sections,
                },
            }
        except pymysql.Error as e:
            return {
                'status': False,
                'data': f'Terjadi error {e!r}, error nya {e.args[0] if e.args else e}',
            }

    def _domain_sql_key_expr(self, column_expr):
        return (
            "LOWER(TRIM(REPLACE(REPLACE(LOWER(TRIM("
            + column_expr
            + ")), 'www.', ''), 'https://', '')))"
        )

    def _domain_join_sql_key_expr(self, column_expr):
        """Join key 2-level domain — selaras dengan ROI / menu Tanpa Spends."""
        inner = self._domain_sql_key_expr(column_expr)
        return f"LOWER(SUBSTRING_INDEX({inner}, '.', 2))"

    def _lookup_fb_invalid_row(self, fb_map, domain_name):
        key = self._normalize_domain_match_key(domain_name)
        if not key:
            return None
        return fb_map.get(key)

    def _derive_adx_business_metrics(self, adx_row, fb_row):
        adx_row = adx_row or {}
        fb_row = fb_row or {}
        revenue = float(adx_row.get('revenue') or 0)
        impresi = float(adx_row.get('impresi') or 0)
        click = float(adx_row.get('click') or 0)
        total_requests = float(adx_row.get('total_requests') or 0)
        responses_served = float(adx_row.get('responses_served') or 0)
        spend = float(fb_row.get('spend') or 0)
        fb_click = float(fb_row.get('click') or 0)
        profit = revenue - spend
        roi = ((revenue - spend) / spend * 100.0) if spend > 0 else 0.0
        cpr = (spend / fb_click) if fb_click > 0 else 0.0
        ecpm = (revenue / impresi * 1000.0) if impresi > 0 else 0.0
        ctr = (click / impresi * 100.0) if impresi > 0 else 0.0
        cpc = (revenue / click) if click > 0 else 0.0
        match_rate = (responses_served / total_requests * 100.0) if total_requests > 0 else 0.0
        fill_rate = (impresi / responses_served * 100.0) if responses_served > 0 else 0.0
        return {
            'spend': spend,
            'fb_click': fb_click,
            'profit': profit,
            'roi': roi,
            'cpr': cpr,
            'ecpm': ecpm,
            'ctr': ctr,
            'cpc': cpc,
            'match_rate': match_rate,
            'fill_rate': fill_rate,
        }

    def _build_adx_invalid_metric_rows(self, daily_row, rekap_row, fb_daily_row=None, fb_rekap_row=None):
        adx_defs = [
            {'key': 'revenue', 'label': 'Revenue AdX', 'kind': 'money'},
            {'key': 'impresi', 'label': 'Impresi AdX', 'kind': 'number'},
            {'key': 'click', 'label': 'Click AdX', 'kind': 'number'},
            {'key': 'requests', 'label': 'Requests', 'daily_key': 'total_requests', 'rekap_key': 'total_requests', 'kind': 'number'},
            {'key': 'responses', 'label': 'Responses', 'daily_key': 'responses_served', 'rekap_key': 'responses_served', 'kind': 'number'},
        ]
        fb_defs = [
            {'key': 'spend', 'label': 'Spend FB', 'kind': 'money'},
            {'key': 'click', 'label': 'Click FB', 'kind': 'number'},
            {'key': 'impresi', 'label': 'Impresi FB', 'kind': 'number'},
            {'key': 'lpv', 'label': 'LPV FB', 'kind': 'number'},
        ]
        derived_defs = [
            {'key': 'profit', 'label': 'Profit', 'kind': 'money'},
            {'key': 'roi', 'label': 'ROI', 'kind': 'percent'},
            {'key': 'cpr', 'label': 'CPR', 'kind': 'money'},
            {'key': 'ecpm', 'label': 'eCPM', 'kind': 'money'},
            {'key': 'ctr', 'label': 'CTR AdX', 'kind': 'percent'},
            {'key': 'cpc', 'label': 'CPC AdX', 'kind': 'money'},
            {'key': 'match_rate', 'label': 'Match Rate', 'kind': 'percent'},
            {'key': 'fill_rate', 'label': 'Fill Rate', 'kind': 'percent'},
        ]
        metrics = []
        summary = {'ok': 0, 'warn': 0, 'invalid': 0, 'missing': 0}
        has_adx_daily = bool(daily_row and daily_row.get('_has_data'))
        has_adx_rekap = bool(rekap_row and rekap_row.get('_has_data'))
        has_fb_daily = bool(fb_daily_row and fb_daily_row.get('_has_data'))
        has_fb_rekap = bool(fb_rekap_row and fb_rekap_row.get('_has_data'))
        daily_derived = self._derive_adx_business_metrics(daily_row, fb_daily_row)
        rekap_derived = self._derive_adx_business_metrics(rekap_row, fb_rekap_row)

        def append_metric(item, daily_val, rekap_val, has_daily, has_rekap):
            if not has_daily and not has_rekap:
                row = {
                    'key': item['key'],
                    'label': item['label'],
                    'kind': item.get('kind', 'number'),
                    'daily': 0,
                    'rekap': 0,
                    'delta': 0,
                    'delta_pct': 0,
                    'status': 'missing',
                }
                summary['missing'] += 1
            else:
                row = self._compare_rekap_metric(daily_val, rekap_val)
                row['key'] = item['key']
                row['label'] = item['label']
                row['kind'] = item.get('kind', 'number')
                summary[row['status']] = summary.get(row['status'], 0) + 1
            metrics.append(row)

        for item in adx_defs:
            daily_key = item.get('daily_key') or item['key']
            rekap_key = item.get('rekap_key') or item['key']
            append_metric(
                item,
                float((daily_row or {}).get(daily_key) or 0),
                float((rekap_row or {}).get(rekap_key) or 0),
                has_adx_daily,
                has_adx_rekap,
            )
        for item in fb_defs:
            append_metric(
                item,
                float((fb_daily_row or {}).get(item['key']) or 0),
                float((fb_rekap_row or {}).get(item['key']) or 0),
                has_fb_daily,
                has_fb_rekap,
            )
        for item in derived_defs:
            append_metric(
                item,
                float(daily_derived.get(item['key']) or 0),
                float(rekap_derived.get(item['key']) or 0),
                has_adx_daily or has_fb_daily,
                has_adx_rekap or has_fb_rekap,
            )
        return metrics, summary, daily_derived, rekap_derived

    def _worst_invalid_status(self, summary):
        if int((summary or {}).get('invalid') or 0) > 0:
            return 'invalid'
        if int((summary or {}).get('warn') or 0) > 0:
            return 'warn'
        if int((summary or {}).get('ok') or 0) > 0:
            return 'ok'
        return 'missing'

    def list_adx_rekap_invalid_report(self, year, month, tanggal_tarik=None, status_filter=None, domain_q=None, hide_zero_spend=None):
        """List AdX monthly recap vs daily aggregates for all domains."""
        import calendar

        try:
            year = str(year or '').strip()
            month = str(month or '').strip().zfill(2)
            hide_zero_spend = str(hide_zero_spend or '').strip().lower() in ('1', 'true', 'yes', 'on')
            if not year.isdigit() or not month.isdigit():
                return {'status': False, 'data': 'Tahun/bulan tidak valid'}
            month_int = int(month)
            if month_int < 1 or month_int > 12:
                return {'status': False, 'data': 'Bulan tidak valid'}

            last_day = calendar.monthrange(int(year), month_int)[1]
            start_date = f"{year}-{month}-01"
            end_date = f"{year}-{month}-{last_day:02d}"
            status_filter = str(status_filter or 'all').strip().lower()
            domain_q = str(domain_q or '').strip().lower()

            self.cur_hris.execute(
                """
                SELECT DISTINCT data_adx_rekap_tanggal AS tanggal_tarik
                FROM data_adx_rekap
                WHERE data_adx_rekap_tahun = %s AND data_adx_rekap_bulan = %s
                ORDER BY data_adx_rekap_tanggal DESC
                """,
                (year, month),
            )
            available_tarik = []
            for row in (self.cur_hris.fetchall() or []):
                val = row.get('tanggal_tarik')
                if hasattr(val, 'isoformat'):
                    available_tarik.append(val.isoformat())
                elif val:
                    available_tarik.append(str(val).strip())

            resolved_tarik = str(tanggal_tarik or '').strip()
            if not resolved_tarik:
                resolved_tarik = available_tarik[0] if available_tarik else ''
            if not resolved_tarik:
                return {
                    'status': True,
                    'data': {
                        'year': year,
                        'month': month,
                        'period': {'start': start_date, 'end': end_date},
                        'tanggal_tarik': None,
                        'available_tarik_dates': available_tarik,
                        'summary': {'total': 0, 'invalid': 0, 'warn': 0, 'ok': 0, 'missing': 0},
                        'rows': [],
                    },
                }

            rekap_key_expr = self._domain_sql_key_expr('data_adx_rekap_domain')
            rekap_sql = f"""
                SELECT
                    {rekap_key_expr} AS domain_key,
                    MIN(data_adx_rekap_domain) AS domain,
                    COALESCE(SUM(CAST(data_adx_rekap_revenue AS DECIMAL(18,4))), 0) AS revenue,
                    COALESCE(SUM(CAST(data_adx_rekap_impresi AS DECIMAL(18,4))), 0) AS impresi,
                    COALESCE(SUM(CAST(data_adx_rekap_click AS DECIMAL(18,4))), 0) AS click,
                    COALESCE(SUM(CAST(data_adx_rekap_total_requests AS DECIMAL(18,4))), 0) AS total_requests,
                    COALESCE(SUM(CAST(data_adx_rekap_responses_served AS DECIMAL(18,4))), 0) AS responses_served
                FROM data_adx_rekap
                WHERE data_adx_rekap_tahun = %s
                  AND data_adx_rekap_bulan = %s
                  AND data_adx_rekap_tanggal = %s
                GROUP BY domain_key
            """
            self.cur_hris.execute(rekap_sql, (year, month, resolved_tarik))
            rekap_rows = self.cur_hris.fetchall() or []
            rekap_map = {}
            for row in rekap_rows:
                key = str(row.get('domain_key') or '').strip()
                if not key:
                    continue
                rekap_map[key] = {
                    'domain': str(row.get('domain') or key),
                    'revenue': float(row.get('revenue') or 0),
                    'impresi': float(row.get('impresi') or 0),
                    'click': float(row.get('click') or 0),
                    'total_requests': float(row.get('total_requests') or 0),
                    'responses_served': float(row.get('responses_served') or 0),
                    '_has_data': True,
                }

            daily_key_expr = self._domain_sql_key_expr('data_adx_domain')
            daily_sql = f"""
                SELECT
                    {daily_key_expr} AS domain_key,
                    MIN(data_adx_domain) AS domain,
                    COALESCE(SUM(CAST(data_adx_domain_revenue AS DECIMAL(18,4))), 0) AS revenue,
                    COALESCE(SUM(CAST(data_adx_domain_impresi AS DECIMAL(18,4))), 0) AS impresi,
                    COALESCE(SUM(CAST(data_adx_domain_click AS DECIMAL(18,4))), 0) AS click,
                    COALESCE(SUM(CAST(data_adx_domain_total_requests AS DECIMAL(18,4))), 0) AS total_requests,
                    COALESCE(SUM(CAST(data_adx_domain_responses_served AS DECIMAL(18,4))), 0) AS responses_served
                FROM data_adx_domain
                WHERE DATE(data_adx_domain_tanggal) BETWEEN %s AND %s
                GROUP BY domain_key
            """
            self.cur_hris.execute(daily_sql, (start_date, end_date))
            daily_rows = self.cur_hris.fetchall() or []
            daily_map = {}
            for row in daily_rows:
                key = str(row.get('domain_key') or '').strip()
                if not key:
                    continue
                vals = {
                    'domain': str(row.get('domain') or key),
                    'revenue': float(row.get('revenue') or 0),
                    'impresi': float(row.get('impresi') or 0),
                    'click': float(row.get('click') or 0),
                    'total_requests': float(row.get('total_requests') or 0),
                    'responses_served': float(row.get('responses_served') or 0),
                }
                vals['_has_data'] = any(vals[k] for k in ['revenue', 'impresi', 'click', 'total_requests', 'responses_served'])
                daily_map[key] = vals

            fb_key_expr = self._domain_join_sql_key_expr('data_ads_domain')
            fb_daily_sql = f"""
                SELECT
                    {fb_key_expr} AS domain_key,
                    MIN(data_ads_domain) AS domain,
                    COALESCE(SUM(CAST(data_ads_spend AS DECIMAL(18,4))), 0) AS spend,
                    COALESCE(SUM(CAST(data_ads_click AS DECIMAL(18,4))), 0) AS click,
                    COALESCE(SUM(CAST(data_ads_impresi AS DECIMAL(18,4))), 0) AS impresi,
                    COALESCE(SUM(CAST(data_ads_lpv AS DECIMAL(18,4))), 0) AS lpv
                FROM data_ads_campaign
                WHERE DATE(data_ads_tanggal) BETWEEN %s AND %s
                GROUP BY domain_key
            """
            self.cur_hris.execute(fb_daily_sql, (start_date, end_date))
            fb_daily_rows = self.cur_hris.fetchall() or []
            fb_daily_map = {}
            for row in fb_daily_rows:
                key = str(row.get('domain_key') or '').strip()
                if not key:
                    continue
                vals = {
                    'domain': str(row.get('domain') or key),
                    'spend': float(row.get('spend') or 0),
                    'click': float(row.get('click') or 0),
                    'impresi': float(row.get('impresi') or 0),
                    'lpv': float(row.get('lpv') or 0),
                }
                vals['_has_data'] = any(vals[k] for k in ['spend', 'click', 'impresi', 'lpv'])
                fb_daily_map[key] = vals

            fb_rekap_map = {}
            if resolved_tarik:
                fb_rekap_sql = f"""
                    SELECT
                        {fb_key_expr} AS domain_key,
                        MIN(data_ads_domain) AS domain,
                        COALESCE(SUM(CAST(data_ads_rekap_spend AS DECIMAL(18,4))), 0) AS spend,
                        COALESCE(SUM(CAST(data_ads_rekap_click AS DECIMAL(18,4))), 0) AS click,
                        COALESCE(SUM(CAST(data_ads_rekap_impresi AS DECIMAL(18,4))), 0) AS impresi,
                        COALESCE(SUM(CAST(data_ads_rekap_lpv AS DECIMAL(18,4))), 0) AS lpv
                    FROM data_ads_rekap
                    WHERE data_ads_rekap_tahun = %s
                      AND data_ads_rekap_bulan = %s
                      AND data_ads_rekap_tanggal = %s
                    GROUP BY domain_key
                """
                self.cur_hris.execute(fb_rekap_sql, (year, month, resolved_tarik))
                fb_rekap_rows = self.cur_hris.fetchall() or []
                for row in fb_rekap_rows:
                    key = str(row.get('domain_key') or '').strip()
                    if not key:
                        continue
                    vals = {
                        'domain': str(row.get('domain') or key),
                        'spend': float(row.get('spend') or 0),
                        'click': float(row.get('click') or 0),
                        'impresi': float(row.get('impresi') or 0),
                        'lpv': float(row.get('lpv') or 0),
                    }
                    vals['_has_data'] = any(vals[k] for k in ['spend', 'click', 'impresi', 'lpv'])
                    fb_rekap_map[key] = vals

            all_keys = sorted(set(list(rekap_map.keys()) + list(daily_map.keys())))
            rows_out = []
            summary = {'total': 0, 'invalid': 0, 'warn': 0, 'ok': 0, 'missing': 0}
            totals_adx_daily = {
                '_has_data': False,
                'revenue': 0.0,
                'impresi': 0.0,
                'click': 0.0,
                'total_requests': 0.0,
                'responses_served': 0.0,
            }
            totals_adx_rekap = {
                '_has_data': False,
                'revenue': 0.0,
                'impresi': 0.0,
                'click': 0.0,
                'total_requests': 0.0,
                'responses_served': 0.0,
            }
            totals_fb_daily = {'_has_data': False, 'spend': 0.0, 'click': 0.0, 'impresi': 0.0, 'lpv': 0.0}
            totals_fb_rekap = {'_has_data': False, 'spend': 0.0, 'click': 0.0, 'impresi': 0.0, 'lpv': 0.0}

            def _acc_adx_total(total_row, src_row):
                if not src_row or not src_row.get('_has_data'):
                    return
                total_row['_has_data'] = True
                for metric_key in ['revenue', 'impresi', 'click', 'total_requests', 'responses_served']:
                    total_row[metric_key] += float(src_row.get(metric_key) or 0)

            def _acc_fb_total(total_row, src_row):
                if not src_row or not src_row.get('_has_data'):
                    return
                total_row['_has_data'] = True
                for metric_key in ['spend', 'click', 'impresi', 'lpv']:
                    total_row[metric_key] += float(src_row.get(metric_key) or 0)

            for key in all_keys:
                rekap_row = rekap_map.get(key)
                daily_row = daily_map.get(key)
                if not rekap_row and not daily_row:
                    continue
                domain_name = (rekap_row or daily_row or {}).get('domain') or key
                fb_daily_row = self._lookup_fb_invalid_row(fb_daily_map, domain_name)
                fb_rekap_row = self._lookup_fb_invalid_row(fb_rekap_map, domain_name)
                if domain_q and domain_q not in str(domain_name).lower() and domain_q not in key:
                    continue
                metrics, metric_summary, daily_derived, rekap_derived = self._build_adx_invalid_metric_rows(
                    daily_row or {'_has_data': False},
                    rekap_row or {'_has_data': False},
                    fb_daily_row or {'_has_data': False},
                    fb_rekap_row or {'_has_data': False},
                )
                if hide_zero_spend:
                    spend_val = max(
                        float(daily_derived.get('spend') or 0),
                        float(rekap_derived.get('spend') or 0),
                    )
                    if spend_val <= 0:
                        continue
                row_status = self._worst_invalid_status(metric_summary)
                if status_filter not in ('', 'all') and row_status != status_filter:
                    continue
                revenue_metric = next((m for m in metrics if m.get('key') == 'revenue'), metrics[0] if metrics else {})
                profit_metric = next((m for m in metrics if m.get('key') == 'profit'), {})
                roi_metric = next((m for m in metrics if m.get('key') == 'roi'), {})
                cpr_metric = next((m for m in metrics if m.get('key') == 'cpr'), {})
                _acc_adx_total(totals_adx_daily, daily_row)
                _acc_adx_total(totals_adx_rekap, rekap_row)
                _acc_fb_total(totals_fb_daily, fb_daily_row)
                _acc_fb_total(totals_fb_rekap, fb_rekap_row)
                rows_out.append({
                    'domain': domain_name,
                    'domain_key': key,
                    'status': row_status,
                    'has_daily': bool((daily_row and daily_row.get('_has_data')) or (fb_daily_row and fb_daily_row.get('_has_data'))),
                    'has_rekap': bool((rekap_row and rekap_row.get('_has_data')) or (fb_rekap_row and fb_rekap_row.get('_has_data'))),
                    'daily_revenue': float((daily_row or {}).get('revenue') or 0),
                    'rekap_revenue': float((rekap_row or {}).get('revenue') or 0),
                    'revenue_delta_pct': float(revenue_metric.get('delta_pct') or 0),
                    'daily_spend': float(daily_derived.get('spend') or 0),
                    'rekap_spend': float(rekap_derived.get('spend') or 0),
                    'daily_profit': float(daily_derived.get('profit') or 0),
                    'rekap_profit': float(rekap_derived.get('profit') or 0),
                    'profit_delta_pct': float(profit_metric.get('delta_pct') or 0),
                    'daily_roi': float(daily_derived.get('roi') or 0),
                    'rekap_roi': float(rekap_derived.get('roi') or 0),
                    'roi_delta_pct': float(roi_metric.get('delta_pct') or 0),
                    'daily_cpr': float(daily_derived.get('cpr') or 0),
                    'rekap_cpr': float(rekap_derived.get('cpr') or 0),
                    'cpr_delta_pct': float(cpr_metric.get('delta_pct') or 0),
                    'daily_ecpm': float(daily_derived.get('ecpm') or 0),
                    'rekap_ecpm': float(rekap_derived.get('ecpm') or 0),
                    'daily_impresi': float((daily_row or {}).get('impresi') or 0),
                    'rekap_impresi': float((rekap_row or {}).get('impresi') or 0),
                    'daily_click': float((daily_row or {}).get('click') or 0),
                    'rekap_click': float((rekap_row or {}).get('click') or 0),
                    'has_spend': max(float(daily_derived.get('spend') or 0), float(rekap_derived.get('spend') or 0)) > 0,
                    'metrics': metrics,
                    'summary': metric_summary,
                })
                summary['total'] += 1
                summary[row_status] = summary.get(row_status, 0) + 1

            status_order = {'invalid': 0, 'warn': 1, 'ok': 2, 'missing': 3}
            rows_out.sort(key=lambda r: (status_order.get(r.get('status'), 9), -abs(float(r.get('revenue_delta_pct') or 0))))

            aggregate_metrics, aggregate_summary, aggregate_daily_derived, aggregate_rekap_derived = self._build_adx_invalid_metric_rows(
                totals_adx_daily,
                totals_adx_rekap,
                totals_fb_daily,
                totals_fb_rekap,
            )

            return {
                'status': True,
                'data': {
                    'year': year,
                    'month': month,
                    'period': {'start': start_date, 'end': end_date},
                    'tanggal_tarik': resolved_tarik,
                    'available_tarik_dates': available_tarik,
                    'hide_zero_spend': hide_zero_spend,
                    'summary': summary,
                    'aggregate': {
                        'metrics': aggregate_metrics,
                        'summary': aggregate_summary,
                        'daily_derived': aggregate_daily_derived,
                        'rekap_derived': aggregate_rekap_derived,
                    },
                    'rows': rows_out,
                },
            }
        except pymysql.Error as e:
            return {
                'status': False,
                'data': f'Terjadi error {e!r}, error nya {e.args[0] if e.args else e}',
            }

    def get_adx_invalid_report_domain_detail(self, domain, year, month, tanggal_tarik=None):
        """Daily breakdown for one domain in invalid AdX report."""
        import calendar

        try:
            year = str(year or '').strip()
            month = str(month or '').strip().zfill(2)
            month_int = int(month)
            last_day = calendar.monthrange(int(year), month_int)[1]
            start_date = f"{year}-{month}-01"
            end_date = f"{year}-{month}-{last_day:02d}"

            compare = self.get_rekap_vs_daily_compare(domain, year, month, tanggal_tarik)
            if not compare.get('status'):
                return compare

            compare_data = compare.get('data') or {}
            adx_section = None
            fb_section = None
            for sec in (compare_data.get('sections') or []):
                if sec.get('key') == 'adx':
                    adx_section = sec
                elif sec.get('key') == 'facebook_ads':
                    fb_section = sec

            adx_daily = {'_has_data': bool(adx_section and adx_section.get('has_daily'))}
            adx_rekap = {'_has_data': bool(adx_section and adx_section.get('has_rekap'))}
            fb_daily = {'_has_data': bool(fb_section and fb_section.get('has_daily'))}
            fb_rekap = {'_has_data': bool(fb_section and fb_section.get('has_rekap'))}
            for metric in (adx_section or {}).get('metrics') or []:
                key = metric.get('key')
                if key:
                    adx_daily[key] = float(metric.get('daily') or 0)
                    adx_rekap[key] = float(metric.get('rekap') or 0)
            for metric in (fb_section or {}).get('metrics') or []:
                key = metric.get('key')
                if key:
                    fb_daily[key] = float(metric.get('daily') or 0)
                    fb_rekap[key] = float(metric.get('rekap') or 0)

            metrics, metric_summary, daily_derived, rekap_derived = self._build_adx_invalid_metric_rows(
                adx_daily,
                adx_rekap,
                fb_daily,
                fb_rekap,
            )

            clause, params = self._domain_fb_filter_sql('data_ads_domain', domain)
            adx_clause, adx_params = self._domain_filter_sql('data_adx_domain', domain)
            daily_sql = f"""
                SELECT
                    DATE(data_adx_domain_tanggal) AS tanggal,
                    COALESCE(SUM(CAST(data_adx_domain_revenue AS DECIMAL(18,4))), 0) AS revenue,
                    COALESCE(SUM(CAST(data_adx_domain_impresi AS DECIMAL(18,4))), 0) AS impresi,
                    COALESCE(SUM(CAST(data_adx_domain_click AS DECIMAL(18,4))), 0) AS click,
                    COALESCE(SUM(CAST(data_adx_domain_total_requests AS DECIMAL(18,4))), 0) AS total_requests,
                    COALESCE(SUM(CAST(data_adx_domain_responses_served AS DECIMAL(18,4))), 0) AS responses_served
                FROM data_adx_domain
                WHERE DATE(data_adx_domain_tanggal) BETWEEN %s AND %s
                  AND {adx_clause}
                GROUP BY DATE(data_adx_domain_tanggal)
                ORDER BY DATE(data_adx_domain_tanggal) ASC
            """
            self.cur_hris.execute(daily_sql, [start_date, end_date, *adx_params])
            adx_by_date = {}
            for row in (self.cur_hris.fetchall() or []):
                tanggal = row.get('tanggal')
                if hasattr(tanggal, 'isoformat'):
                    tanggal = tanggal.isoformat()
                date_key = str(tanggal or '')
                adx_by_date[date_key] = {
                    'revenue': float(row.get('revenue') or 0),
                    'impresi': float(row.get('impresi') or 0),
                    'click': float(row.get('click') or 0),
                    'total_requests': float(row.get('total_requests') or 0),
                    'responses_served': float(row.get('responses_served') or 0),
                }

            fb_daily_sql = f"""
                SELECT
                    DATE(data_ads_tanggal) AS tanggal,
                    COALESCE(SUM(CAST(data_ads_spend AS DECIMAL(18,4))), 0) AS spend,
                    COALESCE(SUM(CAST(data_ads_click AS DECIMAL(18,4))), 0) AS fb_click,
                    COALESCE(SUM(CAST(data_ads_impresi AS DECIMAL(18,4))), 0) AS fb_impresi,
                    COALESCE(SUM(CAST(data_ads_lpv AS DECIMAL(18,4))), 0) AS lpv
                FROM data_ads_campaign
                WHERE DATE(data_ads_tanggal) BETWEEN %s AND %s
                  AND {clause}
                GROUP BY DATE(data_ads_tanggal)
                ORDER BY DATE(data_ads_tanggal) ASC
            """
            self.cur_hris.execute(fb_daily_sql, [start_date, end_date, *params])
            fb_by_date = {}
            for row in (self.cur_hris.fetchall() or []):
                tanggal = row.get('tanggal')
                if hasattr(tanggal, 'isoformat'):
                    tanggal = tanggal.isoformat()
                date_key = str(tanggal or '')
                fb_by_date[date_key] = {
                    'spend': float(row.get('spend') or 0),
                    'fb_click': float(row.get('fb_click') or 0),
                    'fb_impresi': float(row.get('fb_impresi') or 0),
                    'lpv': float(row.get('lpv') or 0),
                }

            all_dates = sorted(set(list(adx_by_date.keys()) + list(fb_by_date.keys())))
            daily_breakdown = []
            for date_key in all_dates:
                adx_vals = adx_by_date.get(date_key) or {}
                fb_vals = fb_by_date.get(date_key) or {}
                derived = self._derive_adx_business_metrics(adx_vals, {
                    'spend': fb_vals.get('spend'),
                    'click': fb_vals.get('fb_click'),
                })
                daily_breakdown.append({
                    'date': date_key,
                    'revenue': float(adx_vals.get('revenue') or 0),
                    'impresi': float(adx_vals.get('impresi') or 0),
                    'click': float(adx_vals.get('click') or 0),
                    'total_requests': float(adx_vals.get('total_requests') or 0),
                    'responses_served': float(adx_vals.get('responses_served') or 0),
                    'spend': float(fb_vals.get('spend') or 0),
                    'fb_click': float(fb_vals.get('fb_click') or 0),
                    'lpv': float(fb_vals.get('lpv') or 0),
                    'profit': float(derived.get('profit') or 0),
                    'roi': float(derived.get('roi') or 0),
                    'cpr': float(derived.get('cpr') or 0),
                    'ecpm': float(derived.get('ecpm') or 0),
                })

            return {
                'status': True,
                'data': {
                    'domain': str(domain or '').strip(),
                    'year': year,
                    'month': month,
                    'period': {'start': start_date, 'end': end_date},
                    'tanggal_tarik': compare_data.get('tanggal_tarik'),
                    'adx': {
                        'metrics': metrics,
                        'summary': metric_summary,
                        'daily_derived': daily_derived,
                        'rekap_derived': rekap_derived,
                    },
                    'facebook_ads': fb_section or {},
                    'daily_breakdown': daily_breakdown,
                },
            }
        except pymysql.Error as e:
            return {
                'status': False,
                'data': f'Terjadi error {e!r}, error nya {e.args[0] if e.args else e}',
            }

    def _derive_adsense_business_metrics(self, adsense_row, fb_row):
        adsense_row = adsense_row or {}
        fb_row = fb_row or {}
        revenue = float(adsense_row.get('revenue') or 0)
        impresi = float(adsense_row.get('impresi') or 0)
        click = float(adsense_row.get('click') or 0)
        page_views = float(adsense_row.get('page_views') or 0)
        ad_requests = float(adsense_row.get('ad_requests') or 0)
        spend = float(fb_row.get('spend') or 0)
        fb_click = float(fb_row.get('click') or 0)
        profit = revenue - spend
        roi = ((revenue - spend) / spend * 100.0) if spend > 0 else 0.0
        cpr = (spend / fb_click) if fb_click > 0 else 0.0
        ecpm = (revenue / impresi * 1000.0) if impresi > 0 else 0.0
        rpm = (revenue / page_views * 1000.0) if page_views > 0 else 0.0
        ctr = (click / impresi * 100.0) if impresi > 0 else 0.0
        cpc = (revenue / click) if click > 0 else 0.0
        coverage = (impresi / ad_requests * 100.0) if ad_requests > 0 else 0.0
        return {
            'spend': spend,
            'fb_click': fb_click,
            'profit': profit,
            'roi': roi,
            'cpr': cpr,
            'ecpm': ecpm,
            'rpm': rpm,
            'ctr': ctr,
            'cpc': cpc,
            'coverage': coverage,
        }

    def _build_adsense_invalid_metric_rows(self, daily_row, rekap_row, fb_daily_row=None, fb_rekap_row=None):
        adsense_defs = [
            {'key': 'revenue', 'label': 'Revenue AdSense', 'kind': 'money'},
            {'key': 'impresi', 'label': 'Impresi AdSense', 'kind': 'number'},
            {'key': 'click', 'label': 'Click AdSense', 'kind': 'number'},
            {'key': 'page_views', 'label': 'Page Views', 'kind': 'number'},
            {'key': 'ad_requests', 'label': 'Ad Requests', 'kind': 'number'},
        ]
        fb_defs = [
            {'key': 'spend', 'label': 'Spend FB', 'kind': 'money'},
            {'key': 'click', 'label': 'Click FB', 'kind': 'number'},
            {'key': 'impresi', 'label': 'Impresi FB', 'kind': 'number'},
            {'key': 'lpv', 'label': 'LPV FB', 'kind': 'number'},
        ]
        derived_defs = [
            {'key': 'profit', 'label': 'Profit', 'kind': 'money'},
            {'key': 'roi', 'label': 'ROI', 'kind': 'percent'},
            {'key': 'cpr', 'label': 'CPR', 'kind': 'money'},
            {'key': 'ecpm', 'label': 'eCPM', 'kind': 'money'},
            {'key': 'rpm', 'label': 'RPM', 'kind': 'money'},
            {'key': 'ctr', 'label': 'CTR AdSense', 'kind': 'percent'},
            {'key': 'cpc', 'label': 'CPC AdSense', 'kind': 'money'},
            {'key': 'coverage', 'label': 'Coverage', 'kind': 'percent'},
        ]
        metrics = []
        summary = {'ok': 0, 'warn': 0, 'invalid': 0, 'missing': 0}
        has_adsense_daily = bool(daily_row and daily_row.get('_has_data'))
        has_adsense_rekap = bool(rekap_row and rekap_row.get('_has_data'))
        has_fb_daily = bool(fb_daily_row and fb_daily_row.get('_has_data'))
        has_fb_rekap = bool(fb_rekap_row and fb_rekap_row.get('_has_data'))
        daily_derived = self._derive_adsense_business_metrics(daily_row, fb_daily_row)
        rekap_derived = self._derive_adsense_business_metrics(rekap_row, fb_rekap_row)

        def append_metric(item, daily_val, rekap_val, has_daily, has_rekap):
            if not has_daily and not has_rekap:
                row = {
                    'key': item['key'],
                    'label': item['label'],
                    'kind': item.get('kind', 'number'),
                    'daily': 0,
                    'rekap': 0,
                    'delta': 0,
                    'delta_pct': 0,
                    'status': 'missing',
                }
                summary['missing'] += 1
            else:
                row = self._compare_rekap_metric(daily_val, rekap_val)
                row['key'] = item['key']
                row['label'] = item['label']
                row['kind'] = item.get('kind', 'number')
                summary[row['status']] = summary.get(row['status'], 0) + 1
            metrics.append(row)

        for item in adsense_defs:
            append_metric(
                item,
                float((daily_row or {}).get(item['key']) or 0),
                float((rekap_row or {}).get(item['key']) or 0),
                has_adsense_daily,
                has_adsense_rekap,
            )
        for item in fb_defs:
            append_metric(
                item,
                float((fb_daily_row or {}).get(item['key']) or 0),
                float((fb_rekap_row or {}).get(item['key']) or 0),
                has_fb_daily,
                has_fb_rekap,
            )
        for item in derived_defs:
            append_metric(
                item,
                float(daily_derived.get(item['key']) or 0),
                float(rekap_derived.get(item['key']) or 0),
                has_adsense_daily or has_fb_daily,
                has_adsense_rekap or has_fb_rekap,
            )
        return metrics, summary, daily_derived, rekap_derived

    def list_adsense_rekap_invalid_report(self, year, month, tanggal_tarik=None, status_filter=None, domain_q=None, hide_zero_spend=None):
        """List AdSense monthly recap vs daily aggregates for all domains."""
        import calendar

        try:
            year = str(year or '').strip()
            month = str(month or '').strip().zfill(2)
            hide_zero_spend = str(hide_zero_spend or '').strip().lower() in ('1', 'true', 'yes', 'on')
            if not year.isdigit() or not month.isdigit():
                return {'status': False, 'data': 'Tahun/bulan tidak valid'}
            month_int = int(month)
            if month_int < 1 or month_int > 12:
                return {'status': False, 'data': 'Bulan tidak valid'}

            last_day = calendar.monthrange(int(year), month_int)[1]
            start_date = f"{year}-{month}-01"
            end_date = f"{year}-{month}-{last_day:02d}"
            status_filter = str(status_filter or 'all').strip().lower()
            domain_q = str(domain_q or '').strip().lower()

            self.cur_hris.execute(
                """
                SELECT DISTINCT data_adsense_rekap_tanggal AS tanggal_tarik
                FROM data_adsense_rekap
                WHERE data_adsense_rekap_tahun = %s AND data_adsense_rekap_bulan = %s
                ORDER BY data_adsense_rekap_tanggal DESC
                """,
                (year, month),
            )
            available_tarik = []
            for row in (self.cur_hris.fetchall() or []):
                val = row.get('tanggal_tarik')
                if hasattr(val, 'isoformat'):
                    available_tarik.append(val.isoformat())
                elif val:
                    available_tarik.append(str(val).strip())

            resolved_tarik = str(tanggal_tarik or '').strip()
            if not resolved_tarik:
                resolved_tarik = available_tarik[0] if available_tarik else ''
            if not resolved_tarik:
                return {
                    'status': True,
                    'data': {
                        'year': year,
                        'month': month,
                        'period': {'start': start_date, 'end': end_date},
                        'tanggal_tarik': None,
                        'available_tarik_dates': available_tarik,
                        'summary': {'total': 0, 'invalid': 0, 'warn': 0, 'ok': 0, 'missing': 0},
                        'rows': [],
                    },
                }

            rekap_key_expr = self._domain_sql_key_expr('data_adsense_rekap_domain')
            rekap_sql = f"""
                SELECT
                    {rekap_key_expr} AS domain_key,
                    MIN(data_adsense_rekap_domain) AS domain,
                    COALESCE(SUM(CAST(data_adsense_rekap_revenue AS DECIMAL(18,4))), 0) AS revenue,
                    COALESCE(SUM(CAST(data_adsense_rekap_impresi AS DECIMAL(18,4))), 0) AS impresi,
                    COALESCE(SUM(CAST(data_adsense_rekap_click AS DECIMAL(18,4))), 0) AS click,
                    COALESCE(SUM(CAST(data_adsense_rekap_page_views AS DECIMAL(18,4))), 0) AS page_views,
                    COALESCE(SUM(CAST(data_adsense_rekap_ad_requests AS DECIMAL(18,4))), 0) AS ad_requests
                FROM data_adsense_rekap
                WHERE data_adsense_rekap_tahun = %s
                  AND data_adsense_rekap_bulan = %s
                  AND data_adsense_rekap_tanggal = %s
                GROUP BY domain_key
            """
            self.cur_hris.execute(rekap_sql, (year, month, resolved_tarik))
            rekap_map = {}
            for row in (self.cur_hris.fetchall() or []):
                key = str(row.get('domain_key') or '').strip()
                if not key:
                    continue
                rekap_map[key] = {
                    'domain': str(row.get('domain') or key),
                    'revenue': float(row.get('revenue') or 0),
                    'impresi': float(row.get('impresi') or 0),
                    'click': float(row.get('click') or 0),
                    'page_views': float(row.get('page_views') or 0),
                    'ad_requests': float(row.get('ad_requests') or 0),
                    '_has_data': True,
                }

            daily_key_expr = self._domain_sql_key_expr('data_adsense_domain')
            daily_sql = f"""
                SELECT
                    {daily_key_expr} AS domain_key,
                    MIN(data_adsense_domain) AS domain,
                    COALESCE(SUM(CAST(data_adsense_revenue AS DECIMAL(18,4))), 0) AS revenue,
                    COALESCE(SUM(CAST(data_adsense_impresi AS DECIMAL(18,4))), 0) AS impresi,
                    COALESCE(SUM(CAST(data_adsense_click AS DECIMAL(18,4))), 0) AS click,
                    COALESCE(SUM(CAST(data_adsense_page_views AS DECIMAL(18,4))), 0) AS page_views,
                    COALESCE(SUM(CAST(data_adsense_ad_requests AS DECIMAL(18,4))), 0) AS ad_requests
                FROM data_adsense_domain
                WHERE DATE(data_adsense_tanggal) BETWEEN %s AND %s
                GROUP BY domain_key
            """
            self.cur_hris.execute(daily_sql, (start_date, end_date))
            daily_map = {}
            for row in (self.cur_hris.fetchall() or []):
                key = str(row.get('domain_key') or '').strip()
                if not key:
                    continue
                vals = {
                    'domain': str(row.get('domain') or key),
                    'revenue': float(row.get('revenue') or 0),
                    'impresi': float(row.get('impresi') or 0),
                    'click': float(row.get('click') or 0),
                    'page_views': float(row.get('page_views') or 0),
                    'ad_requests': float(row.get('ad_requests') or 0),
                }
                vals['_has_data'] = any(vals[k] for k in ['revenue', 'impresi', 'click', 'page_views', 'ad_requests'])
                daily_map[key] = vals

            fb_key_expr = self._domain_join_sql_key_expr('data_ads_domain')
            fb_daily_sql = f"""
                SELECT
                    {fb_key_expr} AS domain_key,
                    MIN(data_ads_domain) AS domain,
                    COALESCE(SUM(CAST(data_ads_spend AS DECIMAL(18,4))), 0) AS spend,
                    COALESCE(SUM(CAST(data_ads_click AS DECIMAL(18,4))), 0) AS click,
                    COALESCE(SUM(CAST(data_ads_impresi AS DECIMAL(18,4))), 0) AS impresi,
                    COALESCE(SUM(CAST(data_ads_lpv AS DECIMAL(18,4))), 0) AS lpv
                FROM data_ads_campaign
                WHERE DATE(data_ads_tanggal) BETWEEN %s AND %s
                GROUP BY domain_key
            """
            self.cur_hris.execute(fb_daily_sql, (start_date, end_date))
            fb_daily_map = {}
            for row in (self.cur_hris.fetchall() or []):
                key = str(row.get('domain_key') or '').strip()
                if not key:
                    continue
                vals = {
                    'domain': str(row.get('domain') or key),
                    'spend': float(row.get('spend') or 0),
                    'click': float(row.get('click') or 0),
                    'impresi': float(row.get('impresi') or 0),
                    'lpv': float(row.get('lpv') or 0),
                }
                vals['_has_data'] = any(vals[k] for k in ['spend', 'click', 'impresi', 'lpv'])
                fb_daily_map[key] = vals

            fb_rekap_map = {}
            if resolved_tarik:
                fb_rekap_sql = f"""
                    SELECT
                        {fb_key_expr} AS domain_key,
                        MIN(data_ads_domain) AS domain,
                        COALESCE(SUM(CAST(data_ads_rekap_spend AS DECIMAL(18,4))), 0) AS spend,
                        COALESCE(SUM(CAST(data_ads_rekap_click AS DECIMAL(18,4))), 0) AS click,
                        COALESCE(SUM(CAST(data_ads_rekap_impresi AS DECIMAL(18,4))), 0) AS impresi,
                        COALESCE(SUM(CAST(data_ads_rekap_lpv AS DECIMAL(18,4))), 0) AS lpv
                    FROM data_ads_rekap
                    WHERE data_ads_rekap_tahun = %s
                      AND data_ads_rekap_bulan = %s
                      AND data_ads_rekap_tanggal = %s
                    GROUP BY domain_key
                """
                self.cur_hris.execute(fb_rekap_sql, (year, month, resolved_tarik))
                for row in (self.cur_hris.fetchall() or []):
                    key = str(row.get('domain_key') or '').strip()
                    if not key:
                        continue
                    vals = {
                        'domain': str(row.get('domain') or key),
                        'spend': float(row.get('spend') or 0),
                        'click': float(row.get('click') or 0),
                        'impresi': float(row.get('impresi') or 0),
                        'lpv': float(row.get('lpv') or 0),
                    }
                    vals['_has_data'] = any(vals[k] for k in ['spend', 'click', 'impresi', 'lpv'])
                    fb_rekap_map[key] = vals

            all_keys = sorted(set(list(rekap_map.keys()) + list(daily_map.keys())))
            rows_out = []
            summary = {'total': 0, 'invalid': 0, 'warn': 0, 'ok': 0, 'missing': 0}
            totals_adsense_daily = {
                '_has_data': False,
                'revenue': 0.0,
                'impresi': 0.0,
                'click': 0.0,
                'page_views': 0.0,
                'ad_requests': 0.0,
            }
            totals_adsense_rekap = {
                '_has_data': False,
                'revenue': 0.0,
                'impresi': 0.0,
                'click': 0.0,
                'page_views': 0.0,
                'ad_requests': 0.0,
            }
            totals_fb_daily = {'_has_data': False, 'spend': 0.0, 'click': 0.0, 'impresi': 0.0, 'lpv': 0.0}
            totals_fb_rekap = {'_has_data': False, 'spend': 0.0, 'click': 0.0, 'impresi': 0.0, 'lpv': 0.0}

            def _acc_adsense_total(total_row, src_row):
                if not src_row or not src_row.get('_has_data'):
                    return
                total_row['_has_data'] = True
                for metric_key in ['revenue', 'impresi', 'click', 'page_views', 'ad_requests']:
                    total_row[metric_key] += float(src_row.get(metric_key) or 0)

            def _acc_fb_total_adsense(total_row, src_row):
                if not src_row or not src_row.get('_has_data'):
                    return
                total_row['_has_data'] = True
                for metric_key in ['spend', 'click', 'impresi', 'lpv']:
                    total_row[metric_key] += float(src_row.get(metric_key) or 0)

            for key in all_keys:
                rekap_row = rekap_map.get(key)
                daily_row = daily_map.get(key)
                if not rekap_row and not daily_row:
                    continue
                domain_name = (rekap_row or daily_row or {}).get('domain') or key
                fb_daily_row = self._lookup_fb_invalid_row(fb_daily_map, domain_name)
                fb_rekap_row = self._lookup_fb_invalid_row(fb_rekap_map, domain_name)
                if domain_q and domain_q not in str(domain_name).lower() and domain_q not in key:
                    continue
                metrics, metric_summary, daily_derived, rekap_derived = self._build_adsense_invalid_metric_rows(
                    daily_row or {'_has_data': False},
                    rekap_row or {'_has_data': False},
                    fb_daily_row or {'_has_data': False},
                    fb_rekap_row or {'_has_data': False},
                )
                if hide_zero_spend:
                    spend_val = max(
                        float(daily_derived.get('spend') or 0),
                        float(rekap_derived.get('spend') or 0),
                    )
                    if spend_val <= 0:
                        continue
                row_status = self._worst_invalid_status(metric_summary)
                if status_filter not in ('', 'all') and row_status != status_filter:
                    continue
                revenue_metric = next((m for m in metrics if m.get('key') == 'revenue'), metrics[0] if metrics else {})
                profit_metric = next((m for m in metrics if m.get('key') == 'profit'), {})
                roi_metric = next((m for m in metrics if m.get('key') == 'roi'), {})
                cpr_metric = next((m for m in metrics if m.get('key') == 'cpr'), {})
                _acc_adsense_total(totals_adsense_daily, daily_row)
                _acc_adsense_total(totals_adsense_rekap, rekap_row)
                _acc_fb_total_adsense(totals_fb_daily, fb_daily_row)
                _acc_fb_total_adsense(totals_fb_rekap, fb_rekap_row)
                rows_out.append({
                    'domain': domain_name,
                    'domain_key': key,
                    'status': row_status,
                    'has_daily': bool((daily_row and daily_row.get('_has_data')) or (fb_daily_row and fb_daily_row.get('_has_data'))),
                    'has_rekap': bool((rekap_row and rekap_row.get('_has_data')) or (fb_rekap_row and fb_rekap_row.get('_has_data'))),
                    'daily_revenue': float((daily_row or {}).get('revenue') or 0),
                    'rekap_revenue': float((rekap_row or {}).get('revenue') or 0),
                    'revenue_delta_pct': float(revenue_metric.get('delta_pct') or 0),
                    'daily_spend': float(daily_derived.get('spend') or 0),
                    'rekap_spend': float(rekap_derived.get('spend') or 0),
                    'daily_profit': float(daily_derived.get('profit') or 0),
                    'rekap_profit': float(rekap_derived.get('profit') or 0),
                    'profit_delta_pct': float(profit_metric.get('delta_pct') or 0),
                    'daily_roi': float(daily_derived.get('roi') or 0),
                    'rekap_roi': float(rekap_derived.get('roi') or 0),
                    'roi_delta_pct': float(roi_metric.get('delta_pct') or 0),
                    'daily_cpr': float(daily_derived.get('cpr') or 0),
                    'rekap_cpr': float(rekap_derived.get('cpr') or 0),
                    'cpr_delta_pct': float(cpr_metric.get('delta_pct') or 0),
                    'daily_rpm': float(daily_derived.get('rpm') or 0),
                    'rekap_rpm': float(rekap_derived.get('rpm') or 0),
                    'daily_ecpm': float(daily_derived.get('ecpm') or 0),
                    'rekap_ecpm': float(rekap_derived.get('ecpm') or 0),
                    'daily_impresi': float((daily_row or {}).get('impresi') or 0),
                    'rekap_impresi': float((rekap_row or {}).get('impresi') or 0),
                    'daily_click': float((daily_row or {}).get('click') or 0),
                    'rekap_click': float((rekap_row or {}).get('click') or 0),
                    'daily_page_views': float((daily_row or {}).get('page_views') or 0),
                    'rekap_page_views': float((rekap_row or {}).get('page_views') or 0),
                    'has_spend': max(float(daily_derived.get('spend') or 0), float(rekap_derived.get('spend') or 0)) > 0,
                    'metrics': metrics,
                    'summary': metric_summary,
                })
                summary['total'] += 1
                summary[row_status] = summary.get(row_status, 0) + 1

            status_order = {'invalid': 0, 'warn': 1, 'ok': 2, 'missing': 3}
            rows_out.sort(key=lambda r: (status_order.get(r.get('status'), 9), -abs(float(r.get('revenue_delta_pct') or 0))))

            aggregate_metrics, aggregate_summary, aggregate_daily_derived, aggregate_rekap_derived = self._build_adsense_invalid_metric_rows(
                totals_adsense_daily,
                totals_adsense_rekap,
                totals_fb_daily,
                totals_fb_rekap,
            )

            return {
                'status': True,
                'data': {
                    'year': year,
                    'month': month,
                    'period': {'start': start_date, 'end': end_date},
                    'tanggal_tarik': resolved_tarik,
                    'available_tarik_dates': available_tarik,
                    'hide_zero_spend': hide_zero_spend,
                    'summary': summary,
                    'aggregate': {
                        'metrics': aggregate_metrics,
                        'summary': aggregate_summary,
                        'daily_derived': aggregate_daily_derived,
                        'rekap_derived': aggregate_rekap_derived,
                    },
                    'rows': rows_out,
                },
            }
        except pymysql.Error as e:
            return {
                'status': False,
                'data': f'Terjadi error {e!r}, error nya {e.args[0] if e.args else e}',
            }

    def get_adsense_invalid_report_domain_detail(self, domain, year, month, tanggal_tarik=None):
        """Daily breakdown for one domain in invalid AdSense report."""
        import calendar

        try:
            year = str(year or '').strip()
            month = str(month or '').strip().zfill(2)
            month_int = int(month)
            last_day = calendar.monthrange(int(year), month_int)[1]
            start_date = f"{year}-{month}-01"
            end_date = f"{year}-{month}-{last_day:02d}"

            compare = self.get_rekap_vs_daily_compare(domain, year, month, tanggal_tarik)
            if not compare.get('status'):
                return compare

            compare_data = compare.get('data') or {}
            adsense_section = None
            fb_section = None
            for sec in (compare_data.get('sections') or []):
                if sec.get('key') == 'adsense':
                    adsense_section = sec
                elif sec.get('key') == 'facebook_ads':
                    fb_section = sec

            adsense_daily = {'_has_data': bool(adsense_section and adsense_section.get('has_daily'))}
            adsense_rekap = {'_has_data': bool(adsense_section and adsense_section.get('has_rekap'))}
            fb_daily = {'_has_data': bool(fb_section and fb_section.get('has_daily'))}
            fb_rekap = {'_has_data': bool(fb_section and fb_section.get('has_rekap'))}
            for metric in (adsense_section or {}).get('metrics') or []:
                key = metric.get('key')
                if key:
                    adsense_daily[key] = float(metric.get('daily') or 0)
                    adsense_rekap[key] = float(metric.get('rekap') or 0)
            for metric in (fb_section or {}).get('metrics') or []:
                key = metric.get('key')
                if key:
                    fb_daily[key] = float(metric.get('daily') or 0)
                    fb_rekap[key] = float(metric.get('rekap') or 0)

            metrics, metric_summary, daily_derived, rekap_derived = self._build_adsense_invalid_metric_rows(
                adsense_daily,
                adsense_rekap,
                fb_daily,
                fb_rekap,
            )

            adsense_clause, adsense_params = self._domain_filter_sql('data_adsense_domain', domain)
            fb_clause, fb_params = self._domain_fb_filter_sql('data_ads_domain', domain)
            daily_sql = f"""
                SELECT
                    DATE(data_adsense_tanggal) AS tanggal,
                    COALESCE(SUM(CAST(data_adsense_revenue AS DECIMAL(18,4))), 0) AS revenue,
                    COALESCE(SUM(CAST(data_adsense_impresi AS DECIMAL(18,4))), 0) AS impresi,
                    COALESCE(SUM(CAST(data_adsense_click AS DECIMAL(18,4))), 0) AS click,
                    COALESCE(SUM(CAST(data_adsense_page_views AS DECIMAL(18,4))), 0) AS page_views,
                    COALESCE(SUM(CAST(data_adsense_ad_requests AS DECIMAL(18,4))), 0) AS ad_requests
                FROM data_adsense_domain
                WHERE DATE(data_adsense_tanggal) BETWEEN %s AND %s
                  AND {adsense_clause}
                GROUP BY DATE(data_adsense_tanggal)
                ORDER BY DATE(data_adsense_tanggal) ASC
            """
            self.cur_hris.execute(daily_sql, [start_date, end_date, *adsense_params])
            adsense_by_date = {}
            for row in (self.cur_hris.fetchall() or []):
                tanggal = row.get('tanggal')
                if hasattr(tanggal, 'isoformat'):
                    tanggal = tanggal.isoformat()
                date_key = str(tanggal or '')
                adsense_by_date[date_key] = {
                    'revenue': float(row.get('revenue') or 0),
                    'impresi': float(row.get('impresi') or 0),
                    'click': float(row.get('click') or 0),
                    'page_views': float(row.get('page_views') or 0),
                    'ad_requests': float(row.get('ad_requests') or 0),
                }

            fb_daily_sql = f"""
                SELECT
                    DATE(data_ads_tanggal) AS tanggal,
                    COALESCE(SUM(CAST(data_ads_spend AS DECIMAL(18,4))), 0) AS spend,
                    COALESCE(SUM(CAST(data_ads_click AS DECIMAL(18,4))), 0) AS fb_click,
                    COALESCE(SUM(CAST(data_ads_impresi AS DECIMAL(18,4))), 0) AS fb_impresi,
                    COALESCE(SUM(CAST(data_ads_lpv AS DECIMAL(18,4))), 0) AS lpv
                FROM data_ads_campaign
                WHERE DATE(data_ads_tanggal) BETWEEN %s AND %s
                  AND {fb_clause}
                GROUP BY DATE(data_ads_tanggal)
                ORDER BY DATE(data_ads_tanggal) ASC
            """
            self.cur_hris.execute(fb_daily_sql, [start_date, end_date, *fb_params])
            fb_by_date = {}
            for row in (self.cur_hris.fetchall() or []):
                tanggal = row.get('tanggal')
                if hasattr(tanggal, 'isoformat'):
                    tanggal = tanggal.isoformat()
                date_key = str(tanggal or '')
                fb_by_date[date_key] = {
                    'spend': float(row.get('spend') or 0),
                    'fb_click': float(row.get('fb_click') or 0),
                    'fb_impresi': float(row.get('fb_impresi') or 0),
                    'lpv': float(row.get('lpv') or 0),
                }

            all_dates = sorted(set(list(adsense_by_date.keys()) + list(fb_by_date.keys())))
            daily_breakdown = []
            for date_key in all_dates:
                adsense_vals = adsense_by_date.get(date_key) or {}
                fb_vals = fb_by_date.get(date_key) or {}
                derived = self._derive_adsense_business_metrics(adsense_vals, {
                    'spend': fb_vals.get('spend'),
                    'click': fb_vals.get('fb_click'),
                })
                daily_breakdown.append({
                    'date': date_key,
                    'revenue': float(adsense_vals.get('revenue') or 0),
                    'impresi': float(adsense_vals.get('impresi') or 0),
                    'click': float(adsense_vals.get('click') or 0),
                    'page_views': float(adsense_vals.get('page_views') or 0),
                    'ad_requests': float(adsense_vals.get('ad_requests') or 0),
                    'spend': float(fb_vals.get('spend') or 0),
                    'fb_click': float(fb_vals.get('fb_click') or 0),
                    'lpv': float(fb_vals.get('lpv') or 0),
                    'profit': float(derived.get('profit') or 0),
                    'roi': float(derived.get('roi') or 0),
                    'cpr': float(derived.get('cpr') or 0),
                    'rpm': float(derived.get('rpm') or 0),
                    'ecpm': float(derived.get('ecpm') or 0),
                })

            return {
                'status': True,
                'data': {
                    'domain': str(domain or '').strip(),
                    'year': year,
                    'month': month,
                    'period': {'start': start_date, 'end': end_date},
                    'tanggal_tarik': compare_data.get('tanggal_tarik'),
                    'adsense': {
                        'metrics': metrics,
                        'summary': metric_summary,
                        'daily_derived': daily_derived,
                        'rekap_derived': rekap_derived,
                    },
                    'facebook_ads': fb_section or {},
                    'daily_breakdown': daily_breakdown,
                },
            }
        except pymysql.Error as e:
            return {
                'status': False,
                'data': f'Terjadi error {e!r}, error nya {e.args[0] if e.args else e}',
            }

    def _derive_fb_invalid_metrics(self, fb_row):
        fb_row = fb_row or {}
        spend = float(fb_row.get('spend') or 0)
        impresi = float(fb_row.get('impresi') or 0)
        click = float(fb_row.get('click') or 0)
        reach = float(fb_row.get('reach') or 0)
        lpv = float(fb_row.get('lpv') or 0)
        ctr = (click / impresi * 100.0) if impresi > 0 else 0.0
        cpc = (spend / click) if click > 0 else 0.0
        cpr = (spend / lpv) if lpv > 0 else cpc
        frekuensi = (impresi / reach) if reach > 0 else 0.0
        lpv_rate = (lpv / click * 100.0) if click > 0 else 0.0
        return {
            'spend': spend,
            'impresi': impresi,
            'click': click,
            'reach': reach,
            'lpv': lpv,
            'ctr': ctr,
            'cpc': cpc,
            'cpr': cpr,
            'frekuensi': frekuensi,
            'lpv_rate': lpv_rate,
        }

    def _build_ads_invalid_metric_rows(self, daily_row, rekap_row):
        raw_defs = [
            {'key': 'spend', 'label': 'Spend', 'kind': 'money'},
            {'key': 'impresi', 'label': 'Impresi', 'kind': 'number'},
            {'key': 'click', 'label': 'Click', 'kind': 'number'},
            {'key': 'reach', 'label': 'Reach', 'kind': 'number'},
            {'key': 'lpv', 'label': 'LPV', 'kind': 'number'},
        ]
        derived_defs = [
            {'key': 'ctr', 'label': 'CTR', 'kind': 'percent'},
            {'key': 'cpc', 'label': 'CPC', 'kind': 'money'},
            {'key': 'cpr', 'label': 'CPR', 'kind': 'money'},
            {'key': 'frekuensi', 'label': 'Frequency', 'kind': 'number'},
            {'key': 'lpv_rate', 'label': 'LPV Rate', 'kind': 'percent'},
        ]
        metrics = []
        summary = {'ok': 0, 'warn': 0, 'invalid': 0, 'missing': 0}
        has_daily = bool(daily_row and daily_row.get('_has_data'))
        has_rekap = bool(rekap_row and rekap_row.get('_has_data'))
        daily_derived = self._derive_fb_invalid_metrics(daily_row)
        rekap_derived = self._derive_fb_invalid_metrics(rekap_row)

        def append_metric(item, daily_val, rekap_val):
            if not has_daily and not has_rekap:
                row = {
                    'key': item['key'],
                    'label': item['label'],
                    'kind': item.get('kind', 'number'),
                    'daily': 0,
                    'rekap': 0,
                    'delta': 0,
                    'delta_pct': 0,
                    'status': 'missing',
                }
                summary['missing'] += 1
            else:
                row = self._compare_rekap_metric(daily_val, rekap_val)
                row['key'] = item['key']
                row['label'] = item['label']
                row['kind'] = item.get('kind', 'number')
                summary[row['status']] = summary.get(row['status'], 0) + 1
            metrics.append(row)

        for item in raw_defs:
            append_metric(
                item,
                float((daily_row or {}).get(item['key']) or 0),
                float((rekap_row or {}).get(item['key']) or 0),
            )
        for item in derived_defs:
            append_metric(
                item,
                float(daily_derived.get(item['key']) or 0),
                float(rekap_derived.get(item['key']) or 0),
            )
        return metrics, summary, daily_derived, rekap_derived

    def list_ads_rekap_invalid_report(self, year, month, tanggal_tarik=None, status_filter=None, domain_q=None):
        """List Facebook Ads monthly recap vs daily aggregates for all domains."""
        import calendar

        try:
            year = str(year or '').strip()
            month = str(month or '').strip().zfill(2)
            if not year.isdigit() or not month.isdigit():
                return {'status': False, 'data': 'Tahun/bulan tidak valid'}
            month_int = int(month)
            if month_int < 1 or month_int > 12:
                return {'status': False, 'data': 'Bulan tidak valid'}

            last_day = calendar.monthrange(int(year), month_int)[1]
            start_date = f"{year}-{month}-01"
            end_date = f"{year}-{month}-{last_day:02d}"
            status_filter = str(status_filter or 'all').strip().lower()
            domain_q = str(domain_q or '').strip().lower()

            self.cur_hris.execute(
                """
                SELECT DISTINCT data_ads_rekap_tanggal AS tanggal_tarik
                FROM data_ads_rekap
                WHERE data_ads_rekap_tahun = %s AND data_ads_rekap_bulan = %s
                ORDER BY data_ads_rekap_tanggal DESC
                """,
                (year, month),
            )
            available_tarik = []
            for row in (self.cur_hris.fetchall() or []):
                val = row.get('tanggal_tarik')
                if hasattr(val, 'isoformat'):
                    available_tarik.append(val.isoformat())
                elif val:
                    available_tarik.append(str(val).strip())

            resolved_tarik = str(tanggal_tarik or '').strip()
            if not resolved_tarik:
                resolved_tarik = available_tarik[0] if available_tarik else ''
            if not resolved_tarik:
                return {
                    'status': True,
                    'data': {
                        'year': year,
                        'month': month,
                        'period': {'start': start_date, 'end': end_date},
                        'tanggal_tarik': None,
                        'available_tarik_dates': available_tarik,
                        'summary': {'total': 0, 'invalid': 0, 'warn': 0, 'ok': 0, 'missing': 0},
                        'aggregate': {'metrics': [], 'summary': {}, 'daily_derived': {}, 'rekap_derived': {}},
                        'rows': [],
                    },
                }

            domain_key_expr = self._domain_join_sql_key_expr('data_ads_domain')
            rekap_sql = f"""
                SELECT
                    {domain_key_expr} AS domain_key,
                    MIN(data_ads_domain) AS domain,
                    COALESCE(SUM(CAST(data_ads_rekap_spend AS DECIMAL(18,4))), 0) AS spend,
                    COALESCE(SUM(CAST(data_ads_rekap_impresi AS DECIMAL(18,4))), 0) AS impresi,
                    COALESCE(SUM(CAST(data_ads_rekap_click AS DECIMAL(18,4))), 0) AS click,
                    COALESCE(SUM(CAST(data_ads_rekap_reach AS DECIMAL(18,4))), 0) AS reach,
                    COALESCE(SUM(CAST(data_ads_rekap_lpv AS DECIMAL(18,4))), 0) AS lpv
                FROM data_ads_rekap
                WHERE data_ads_rekap_tahun = %s
                  AND data_ads_rekap_bulan = %s
                  AND data_ads_rekap_tanggal = %s
                GROUP BY domain_key
            """
            self.cur_hris.execute(rekap_sql, (year, month, resolved_tarik))
            rekap_map = {}
            for row in (self.cur_hris.fetchall() or []):
                key = str(row.get('domain_key') or '').strip()
                if not key:
                    continue
                vals = {
                    'domain': str(row.get('domain') or key),
                    'spend': float(row.get('spend') or 0),
                    'impresi': float(row.get('impresi') or 0),
                    'click': float(row.get('click') or 0),
                    'reach': float(row.get('reach') or 0),
                    'lpv': float(row.get('lpv') or 0),
                }
                vals['_has_data'] = any(vals[k] for k in ['spend', 'impresi', 'click', 'reach', 'lpv'])
                rekap_map[key] = vals

            daily_sql = f"""
                SELECT
                    {domain_key_expr} AS domain_key,
                    MIN(data_ads_domain) AS domain,
                    COALESCE(SUM(CAST(data_ads_spend AS DECIMAL(18,4))), 0) AS spend,
                    COALESCE(SUM(CAST(data_ads_impresi AS DECIMAL(18,4))), 0) AS impresi,
                    COALESCE(SUM(CAST(data_ads_click AS DECIMAL(18,4))), 0) AS click,
                    COALESCE(SUM(CAST(data_ads_reach AS DECIMAL(18,4))), 0) AS reach,
                    COALESCE(SUM(CAST(data_ads_lpv AS DECIMAL(18,4))), 0) AS lpv
                FROM data_ads_campaign
                WHERE DATE(data_ads_tanggal) BETWEEN %s AND %s
                GROUP BY domain_key
            """
            self.cur_hris.execute(daily_sql, (start_date, end_date))
            daily_map = {}
            for row in (self.cur_hris.fetchall() or []):
                key = str(row.get('domain_key') or '').strip()
                if not key:
                    continue
                vals = {
                    'domain': str(row.get('domain') or key),
                    'spend': float(row.get('spend') or 0),
                    'impresi': float(row.get('impresi') or 0),
                    'click': float(row.get('click') or 0),
                    'reach': float(row.get('reach') or 0),
                    'lpv': float(row.get('lpv') or 0),
                }
                vals['_has_data'] = any(vals[k] for k in ['spend', 'impresi', 'click', 'reach', 'lpv'])
                daily_map[key] = vals

            all_keys = sorted(set(list(rekap_map.keys()) + list(daily_map.keys())))
            rows_out = []
            summary = {'total': 0, 'invalid': 0, 'warn': 0, 'ok': 0, 'missing': 0}
            totals_daily = {'_has_data': False, 'spend': 0.0, 'impresi': 0.0, 'click': 0.0, 'reach': 0.0, 'lpv': 0.0}
            totals_rekap = {'_has_data': False, 'spend': 0.0, 'impresi': 0.0, 'click': 0.0, 'reach': 0.0, 'lpv': 0.0}

            def _acc_fb_total(total_row, src_row):
                if not src_row or not src_row.get('_has_data'):
                    return
                total_row['_has_data'] = True
                for metric_key in ['spend', 'impresi', 'click', 'reach', 'lpv']:
                    total_row[metric_key] += float(src_row.get(metric_key) or 0)

            for key in all_keys:
                rekap_row = rekap_map.get(key)
                daily_row = daily_map.get(key)
                if not rekap_row and not daily_row:
                    continue
                domain_name = (rekap_row or daily_row or {}).get('domain') or key
                if domain_q and domain_q not in str(domain_name).lower() and domain_q not in key:
                    continue
                metrics, metric_summary, daily_derived, rekap_derived = self._build_ads_invalid_metric_rows(
                    daily_row or {'_has_data': False},
                    rekap_row or {'_has_data': False},
                )
                row_status = self._worst_invalid_status(metric_summary)
                if status_filter not in ('', 'all') and row_status != status_filter:
                    continue
                revenue_metric = next((m for m in metrics if m.get('key') == 'spend'), metrics[0] if metrics else {})
                _acc_fb_total(totals_daily, daily_row)
                _acc_fb_total(totals_rekap, rekap_row)
                rows_out.append({
                    'domain': domain_name,
                    'domain_key': key,
                    'status': row_status,
                    'has_daily': bool(daily_row and daily_row.get('_has_data')),
                    'has_rekap': bool(rekap_row and rekap_row.get('_has_data')),
                    'daily_spend': float((daily_row or {}).get('spend') or 0),
                    'rekap_spend': float((rekap_row or {}).get('spend') or 0),
                    'spend_delta_pct': float(revenue_metric.get('delta_pct') or 0),
                    'metrics': metrics,
                    'summary': metric_summary,
                })
                summary['total'] += 1
                summary[row_status] = summary.get(row_status, 0) + 1

            status_order = {'invalid': 0, 'warn': 1, 'ok': 2, 'missing': 3}
            rows_out.sort(key=lambda r: (status_order.get(r.get('status'), 9), -abs(float(r.get('spend_delta_pct') or 0))))

            aggregate_metrics, aggregate_summary, aggregate_daily_derived, aggregate_rekap_derived = self._build_ads_invalid_metric_rows(
                totals_daily,
                totals_rekap,
            )

            return {
                'status': True,
                'data': {
                    'year': year,
                    'month': month,
                    'period': {'start': start_date, 'end': end_date},
                    'tanggal_tarik': resolved_tarik,
                    'available_tarik_dates': available_tarik,
                    'summary': summary,
                    'aggregate': {
                        'metrics': aggregate_metrics,
                        'summary': aggregate_summary,
                        'daily_derived': aggregate_daily_derived,
                        'rekap_derived': aggregate_rekap_derived,
                    },
                    'rows': rows_out,
                },
            }
        except pymysql.Error as e:
            return {
                'status': False,
                'data': f'Terjadi error {e!r}, error nya {e.args[0] if e.args else e}',
            }

    def get_ads_invalid_report_domain_detail(self, domain, year, month, tanggal_tarik=None):
        """Daily breakdown for one domain in invalid Facebook Ads report."""
        import calendar

        try:
            year = str(year or '').strip()
            month = str(month or '').strip().zfill(2)
            month_int = int(month)
            last_day = calendar.monthrange(int(year), month_int)[1]
            start_date = f"{year}-{month}-01"
            end_date = f"{year}-{month}-{last_day:02d}"

            compare = self.get_rekap_vs_daily_compare(domain, year, month, tanggal_tarik)
            if not compare.get('status'):
                return compare

            compare_data = compare.get('data') or {}
            fb_section = None
            for sec in (compare_data.get('sections') or []):
                if sec.get('key') == 'facebook_ads':
                    fb_section = sec
                    break

            fb_daily = {'_has_data': bool(fb_section and fb_section.get('has_daily'))}
            fb_rekap = {'_has_data': bool(fb_section and fb_section.get('has_rekap'))}
            for metric in (fb_section or {}).get('metrics') or []:
                key = metric.get('key')
                if key:
                    fb_daily[key] = float(metric.get('daily') or 0)
                    fb_rekap[key] = float(metric.get('rekap') or 0)

            metrics, metric_summary, daily_derived, rekap_derived = self._build_ads_invalid_metric_rows(
                fb_daily,
                fb_rekap,
            )

            clause, params = self._domain_fb_filter_sql('data_ads_domain', domain)
            daily_sql = f"""
                SELECT
                    DATE(data_ads_tanggal) AS tanggal,
                    COALESCE(SUM(CAST(data_ads_spend AS DECIMAL(18,4))), 0) AS spend,
                    COALESCE(SUM(CAST(data_ads_impresi AS DECIMAL(18,4))), 0) AS impresi,
                    COALESCE(SUM(CAST(data_ads_click AS DECIMAL(18,4))), 0) AS click,
                    COALESCE(SUM(CAST(data_ads_reach AS DECIMAL(18,4))), 0) AS reach,
                    COALESCE(SUM(CAST(data_ads_lpv AS DECIMAL(18,4))), 0) AS lpv
                FROM data_ads_campaign
                WHERE DATE(data_ads_tanggal) BETWEEN %s AND %s
                  AND {clause}
                GROUP BY DATE(data_ads_tanggal)
                ORDER BY DATE(data_ads_tanggal) ASC
            """
            self.cur_hris.execute(daily_sql, [start_date, end_date, *params])
            daily_breakdown = []
            for row in (self.cur_hris.fetchall() or []):
                tanggal = row.get('tanggal')
                if hasattr(tanggal, 'isoformat'):
                    tanggal = tanggal.isoformat()
                date_key = str(tanggal or '')
                vals = {
                    'spend': float(row.get('spend') or 0),
                    'impresi': float(row.get('impresi') or 0),
                    'click': float(row.get('click') or 0),
                    'reach': float(row.get('reach') or 0),
                    'lpv': float(row.get('lpv') or 0),
                }
                derived = self._derive_fb_invalid_metrics(vals)
                daily_breakdown.append({
                    'date': date_key,
                    'spend': vals['spend'],
                    'impresi': vals['impresi'],
                    'click': vals['click'],
                    'reach': vals['reach'],
                    'lpv': vals['lpv'],
                    'ctr': float(derived.get('ctr') or 0),
                    'cpc': float(derived.get('cpc') or 0),
                    'cpr': float(derived.get('cpr') or 0),
                    'frekuensi': float(derived.get('frekuensi') or 0),
                    'lpv_rate': float(derived.get('lpv_rate') or 0),
                })

            return {
                'status': True,
                'data': {
                    'domain': str(domain or '').strip(),
                    'year': year,
                    'month': month,
                    'period': {'start': start_date, 'end': end_date},
                    'tanggal_tarik': compare_data.get('tanggal_tarik'),
                    'facebook_ads': {
                        'metrics': metrics,
                        'summary': metric_summary,
                        'daily_derived': daily_derived,
                        'rekap_derived': rekap_derived,
                    },
                    'daily_breakdown': daily_breakdown,
                },
            }
        except pymysql.Error as e:
            return {
                'status': False,
                'data': f'Terjadi error {e!r}, error nya {e.args[0] if e.args else e}',
            }

    def insert_data_ads_country(self, data):
        try:
            sql_insert = """
                        INSERT INTO data_ads_country
                        (
                            data_ads_country.account_ads_id,
                            data_ads_country.data_ads_country_cd,
                            data_ads_country.data_ads_country_nm,
                            data_ads_country.data_ads_domain,
                            data_ads_country.data_ads_campaign_id,
                            data_ads_country.data_ads_campaign_nm,
                            data_ads_country.data_ads_country_tanggal,
                            data_ads_country.data_ads_country_spend,
                            data_ads_country.data_ads_country_impresi,
                            data_ads_country.data_ads_country_click,
                            data_ads_country.data_ads_country_reach,
                            data_ads_country.data_ads_country_cpr,
                            data_ads_country.data_ads_country_cpc,
                            data_ads_country.data_ads_country_frekuensi,
                            data_ads_country.data_ads_country_lpv,
                            data_ads_country.data_ads_country_lpv_rate,
                            data_ads_country.mdb,
                            data_ads_country.mdb_name,
                            data_ads_country.mdd
                        )
                    VALUES
                        (
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s
                        )
                """
            if not self.execute_query(sql_insert, (
                data['account_ads_id'],
                data['data_ads_country_cd'],
                data['data_ads_country_nm'],
                data['data_ads_domain'],
                data['data_ads_campaign_id'],
                data['data_ads_campaign_nm'],
                data['data_ads_country_tanggal'],
                data['data_ads_country_spend'],
                data['data_ads_country_impresi'],
                data['data_ads_country_click'],
                data['data_ads_country_reach'],
                data['data_ads_country_cpr'],
                data['data_ads_country_cpc'],
                data['data_ads_country_frekuensi'],
                data['data_ads_country_lpv'],
                data['data_ads_country_lpv_rate'],
                data['mdb'],
                data['mdb_name'],
                data['mdd']
            )):
                raise pymysql.Error("Failed to insert data ads country")
            if not self.commit():
                raise pymysql.Error("Failed to commit data ads country insert")
            
            hasil = {
                "status": True,
                "message": "Data ads country berhasil ditambahkan"
            }
        except pymysql.Error as e:
            hasil = {
                "status": False,
                'data': 'Terjadi error {!r}, error nya {}'.format(e, e.args[0])
            }
        return {'hasil': hasil}

    def get_data_adx_country_to_insert_log(self, account_id, tanggal, code_negara, site_name):
        try:
            if not self.ensure_connection():
                raise pymysql.Error("Could not establish database connection")
            self.cur_hris = self.mysql_cur

            sql_exact = (
                "SELECT * FROM data_adx_country "
                "WHERE account_id = %s "
                "AND data_adx_country_tanggal LIKE %s "
                "AND data_adx_country_cd = %s "
                "AND data_adx_country_domain = %s "
                "ORDER BY mdd DESC LIMIT 1"
            )
            self.mysql_cur.execute(sql_exact, (account_id, f"{tanggal}%", code_negara, site_name))
            data = self.mysql_cur.fetchone()

            if (not data) and site_name:
                sql_like = (
                    "SELECT * FROM data_adx_country "
                    "WHERE account_id = %s "
                    "AND data_adx_country_tanggal LIKE %s "
                    "AND data_adx_country_cd = %s "
                    "AND data_adx_country_domain LIKE %s "
                    "ORDER BY mdd DESC LIMIT 1"
                )
                self.mysql_cur.execute(sql_like, (account_id, f"{tanggal}%", code_negara, f"%{site_name}%"))
                data = self.mysql_cur.fetchone()

            if not data:
                return {'hasil': {'status': False, 'message': 'Data adx country tidak ditemukan'}}
            return {'hasil': {'status': True, 'data': data}}
        except pymysql.Error as e:
            return {'hasil': {'status': False, 'message': f"Terjadi error {e}"}}

    def insert_log_adx_country_log(self, data):
        try:
            sql_insert = """
                        INSERT INTO log_adx_country
                        (
                            log_adx_country.account_id,
                            log_adx_country.log_adx_country_tanggal,
                            log_adx_country.log_adx_country_cd,
                            log_adx_country.log_adx_country_nm,
                            log_adx_country.log_adx_country_domain,
                            log_adx_country.log_adx_country_impresi,
                            log_adx_country.log_adx_country_click,
                            log_adx_country.log_adx_country_ctr,
                            log_adx_country.log_adx_country_cpc,
                            log_adx_country.log_adx_country_cpm,
                            log_adx_country.log_adx_country_ecpm,
                            log_adx_country.log_adx_country_total_requests,
                            log_adx_country.log_adx_country_responses_served,
                            log_adx_country.log_adx_country_match_rate,
                            log_adx_country.log_adx_country_fill_rate,
                            log_adx_country.log_adx_country_active_view_pct_viewable,
                            log_adx_country.log_adx_country_active_view_avg_time_sec,
                            log_adx_country.log_adx_country_revenue,
                            log_adx_country.mdb,
                            log_adx_country.mdb_name,
                            log_adx_country.mdd
                        )
                    VALUES
                        (
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s
                        )
                """
            if not self.execute_query(sql_insert, (
                data['account_id'],
                data['log_adx_country_tanggal'],
                data['log_adx_country_cd'],
                data['log_adx_country_nm'],
                data['log_adx_country_domain'],
                data['log_adx_country_impresi'],
                data['log_adx_country_click'],
                data['log_adx_country_ctr'],
                data['log_adx_country_cpc'],
                data['log_adx_country_cpm'],
                data['log_adx_country_ecpm'],
                data['log_adx_country_total_requests'],
                data['log_adx_country_responses_served'],
                data['log_adx_country_match_rate'],
                data['log_adx_country_fill_rate'],
                data['log_adx_country_active_view_pct_viewable'],
                data['log_adx_country_active_view_avg_time_sec'],
                data['log_adx_country_revenue'],
                data['mdb'],
                data['mdb_name'],
                data['mdd']
            )):
                raise pymysql.Error("Failed to insert data adx country log")
            if not self.commit():
                raise pymysql.Error("Failed to commit data adx country log insert")
            
            hasil = {
                "status": True,
                "message": "Data adx country log berhasil ditambahkan"
            }
        except pymysql.Error as e:
            hasil = {
                "status": False,
                'data': 'Terjadi error {!r}, error nya {}'.format(e, e.args[0])
            }
        return {'hasil': hasil}

    def insert_log_adx_domain_log(self, data):
        try:
            sql_insert = """
                        INSERT INTO log_adx_domain
                        (
                            log_adx_domain.log_adx_domain_id,
                            log_adx_domain.account_id,
                            log_adx_domain.log_adx_domain_tanggal,
                            log_adx_domain.log_adx_domain,
                            log_adx_domain.log_adx_domain_impresi,
                            log_adx_domain.log_adx_domain_click,
                            log_adx_domain.log_adx_domain_cpc,
                            log_adx_domain.log_adx_domain_ctr,
                            log_adx_domain.log_adx_domain_cpm,
                            log_adx_domain.log_adx_domain_ecpm,
                            log_adx_domain.log_adx_domain_total_requests,
                            log_adx_domain.log_adx_domain_responses_served,
                            log_adx_domain.log_adx_domain_match_rate,
                            log_adx_domain.log_adx_domain_fill_rate,
                            log_adx_domain.log_adx_domain_active_view_pct_viewable,
                            log_adx_domain.log_adx_domain_active_view_avg_time_sec,
                            log_adx_domain.log_adx_domain_revenue,
                            log_adx_domain.mdb,
                            log_adx_domain.mdb_name,
                            log_adx_domain.mdd
                        )
                    VALUES
                        (
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s
                        )
                """
            if not self.execute_query(sql_insert, (
                data['log_adx_domain_id'],
                data['account_id'],
                data['log_adx_domain_tanggal'],
                data['log_adx_domain'],
                data['log_adx_domain_impresi'],
                data['log_adx_domain_click'],
                data['log_adx_domain_cpc'],
                data['log_adx_domain_ctr'],
                data['log_adx_domain_cpm'],
                data['log_adx_domain_ecpm'],
                data['log_adx_domain_total_requests'],
                data['log_adx_domain_responses_served'],
                data['log_adx_domain_match_rate'],
                data['log_adx_domain_fill_rate'],
                data['log_adx_domain_active_view_pct_viewable'],
                data['log_adx_domain_active_view_avg_time_sec'],
                data['log_adx_domain_revenue'],
                data['mdb'],
                data['mdb_name'],
                data['mdd'],
            )):
                raise pymysql.Error("Failed to insert data adx domain log")
            if not self.commit():
                raise pymysql.Error("Failed to commit data adx domain log insert")

            hasil = {
                "status": True,
                "message": "Data adx domain log berhasil ditambahkan"
            }
        except pymysql.Error as e:
            hasil = {
                "status": False,
                'data': 'Terjadi error {!r}, error nya {}'.format(e, e.args[0])
            }
        return {'hasil': hasil}

    def insert_log_adsense_country_log(self, data):
        try:
            if not self.ensure_connection():
                raise pymysql.Error("Could not establish database connection")
            self.cur_hris = self.mysql_cur

            sql_insert = """
                        INSERT INTO log_adsense_country
                        (
                            log_adsense_country.account_id,
                            log_adsense_country.log_adsense_country_tanggal,
                            log_adsense_country.log_adsense_country_cd,
                            log_adsense_country.log_adsense_country_nm,
                            log_adsense_country.log_adsense_country_domain,
                            log_adsense_country.log_adsense_country_impresi,
                            log_adsense_country.log_adsense_country_click,
                            log_adsense_country.log_adsense_country_cpc,
                            log_adsense_country.log_adsense_country_ctr,
                            log_adsense_country.log_adsense_country_cpm,
                            log_adsense_country.log_adsense_country_page_views,
                            log_adsense_country.log_adsense_country_page_views_rpm,
                            log_adsense_country.log_adsense_country_ad_requests,
                            log_adsense_country.log_adsense_country_ad_requests_coverage,
                            log_adsense_country.log_adsense_country_active_view_viewability,
                            log_adsense_country.log_adsense_country_active_view_measurability,
                            log_adsense_country.log_adsense_country_active_view_time,
                            log_adsense_country.log_adsense_country_revenue,
                            log_adsense_country.mdb,
                            log_adsense_country.mdb_name,
                            log_adsense_country.mdd
                        )
                    VALUES
                        (
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s
                        )
                """

            self.mysql_cur.execute(sql_insert, (
                data['account_id'],
                data['log_adsense_country_tanggal'],
                data['log_adsense_country_cd'],
                data['log_adsense_country_nm'],
                data['log_adsense_country_domain'],
                data['log_adsense_country_impresi'],
                data['log_adsense_country_click'],
                data['log_adsense_country_cpc'],
                data['log_adsense_country_ctr'],
                data['log_adsense_country_cpm'],
                data['log_adsense_country_page_views'],
                data['log_adsense_country_page_views_rpm'],
                data['log_adsense_country_ad_requests'],
                data['log_adsense_country_ad_requests_coverage'],
                data['log_adsense_country_active_view_viewability'],
                data['log_adsense_country_active_view_measurability'],
                data['log_adsense_country_active_view_time'],
                data['log_adsense_country_revenue'],
                data['mdb'],
                data['mdb_name'],
                data['mdd'],
            ))

            if not self.commit():
                raise pymysql.Error("Failed to commit data adsense country log insert")

            hasil = {
                "status": True,
                "message": "Data adsense country log berhasil ditambahkan",
            }
        except pymysql.Error as e:
            err_code = None
            err_msg = None
            try:
                if isinstance(getattr(e, 'args', None), (list, tuple)) and len(e.args) >= 2:
                    err_code = e.args[0]
                    err_msg = e.args[1]
                elif isinstance(getattr(e, 'args', None), (list, tuple)) and len(e.args) == 1:
                    err_code = e.args[0]
            except Exception:
                err_code = None
                err_msg = None

            hasil = {
                "status": False,
                'data': 'Terjadi error {!r}, code={}, message={}'.format(e, err_code, err_msg),
            }
        return {'hasil': hasil}


    def insert_log_adsense_domain_log(self, data):
        try:
            if not self.ensure_connection():
                raise pymysql.Error("Could not establish database connection")
            self.cur_hris = self.mysql_cur

            sql_insert = """
                        INSERT INTO log_adsense_domain
                        (
                            log_adsense_domain.log_adsense_id,
                            log_adsense_domain.account_id,
                            log_adsense_domain.log_adsense_tanggal,
                            log_adsense_domain.log_adsense_domain,
                            log_adsense_domain.log_adsense_impresi,
                            log_adsense_domain.log_adsense_click,
                            log_adsense_domain.log_adsense_cpc,
                            log_adsense_domain.log_adsense_ctr,
                            log_adsense_domain.log_adsense_cpm,
                            log_adsense_domain.log_adsense_page_views,
                            log_adsense_domain.log_adsense_page_views_rpm,
                            log_adsense_domain.log_adsense_ad_requests,
                            log_adsense_domain.log_adsense_ad_requests_coverage,
                            log_adsense_domain.log_adsense_active_view_viewability,
                            log_adsense_domain.log_adsense_active_view_measurability,
                            log_adsense_domain.log_adsense_active_view_time,
                            log_adsense_domain.log_adsense_revenue,
                            log_adsense_domain.mdb,
                            log_adsense_domain.mdb_name,
                            log_adsense_domain.mdd
                        )
                    VALUES
                        (
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s
                        )
                """

            self.mysql_cur.execute(sql_insert, (
                data['log_adsense_id'],
                data['account_id'],
                data['log_adsense_tanggal'],
                data['log_adsense_domain'],
                data['log_adsense_impresi'],
                data['log_adsense_click'],
                data['log_adsense_cpc'],
                data['log_adsense_ctr'],
                data['log_adsense_cpm'],
                data.get('log_adsense_page_views', 0),
                data.get('log_adsense_page_views_rpm', 0),
                data.get('log_adsense_ad_requests', 0),
                data.get('log_adsense_ad_requests_coverage', 0),
                data.get('log_adsense_active_view_viewability', 0),
                data.get('log_adsense_active_view_measurability', 0),
                data.get('log_adsense_active_view_time', 0),
                data['log_adsense_revenue'],
                data['mdb'],
                data['mdb_name'],
                data['mdd'],
            ))

            if not self.commit():
                raise pymysql.Error("Failed to commit data adsense domain log insert")

            hasil = {
                "status": True,
                "message": "Data adsense domain log berhasil ditambahkan",
            }
        except pymysql.Error as e:
            err_code = None
            err_msg = None
            try:
                if isinstance(getattr(e, 'args', None), (list, tuple)) and len(e.args) >= 2:
                    err_code = e.args[0]
                    err_msg = e.args[1]
                elif isinstance(getattr(e, 'args', None), (list, tuple)) and len(e.args) == 1:
                    err_code = e.args[0]
            except Exception:
                err_code = None
                err_msg = None

            hasil = {
                "status": False,
                'data': 'Terjadi error {!r}, code={}, message={}'.format(e, err_code, err_msg),
            }
        return {'hasil': hasil}


    def delete_data_adx_country_by_date(self, account_id, tanggal, code_negara, site_name):
        try:
            sql_delete = (
                "DELETE FROM data_adx_country WHERE account_id = %s AND data_adx_country_tanggal LIKE %s AND data_adx_country_cd = %s AND data_adx_country_domain LIKE %s"
            )
            if not self.execute_query(sql_delete, (account_id, f"{tanggal}%", code_negara, site_name)): 
                raise pymysql.Error("Failed to delete data adx country by date range")
            affected_rows = self.cur_hris.rowcount if hasattr(self, 'cur_hris') and self.cur_hris else 0
            if not self.commit():
                raise pymysql.Error("Failed to commit data adx country delete")
            hasil = {
                "status": True,
                "message": "Data adx country berhasil dihapus",
                "affected": affected_rows
            }
        except pymysql.Error as e:
            hasil = {
                "status": False,
                'data': 'Terjadi error {!r}, error nya {}'.format(e, e.args[0])
            }
        return {'hasil': hasil}

    def insert_data_adx_country(self, data):
        try:
            sql_insert = """
                        INSERT INTO data_adx_country
                        (
                            data_adx_country.account_id,
                            data_adx_country.data_adx_country_tanggal,
                            data_adx_country.data_adx_country_cd,
                            data_adx_country.data_adx_country_nm,
                            data_adx_country.data_adx_country_domain,
                            data_adx_country.data_adx_country_impresi,
                            data_adx_country.data_adx_country_click,
                            data_adx_country.data_adx_country_ctr,
                            data_adx_country.data_adx_country_cpc,
                            data_adx_country.data_adx_country_cpm,
                            data_adx_country.data_adx_country_ecpm,
                            data_adx_country.data_adx_country_total_requests,
                            data_adx_country.data_adx_country_responses_served,
                            data_adx_country.data_adx_country_match_rate,
                            data_adx_country.data_adx_country_fill_rate,
                            data_adx_country.data_adx_country_active_view_pct_viewable,
                            data_adx_country.data_adx_country_active_view_avg_time_sec,
                            data_adx_country.data_adx_country_revenue,
                            data_adx_country.mdb,
                            data_adx_country.mdb_name,
                            data_adx_country.mdd
                        )
                    VALUES
                        (
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s
                        )
                """
            if not self.execute_query(sql_insert, (
                data['account_id'],
                data['data_adx_country_tanggal'],
                data['data_adx_country_cd'],
                data['data_adx_country_nm'],
                data['data_adx_country_domain'],
                data['data_adx_country_impresi'],
                data['data_adx_country_click'],
                data['data_adx_country_ctr'],
                data['data_adx_country_cpc'],
                data['data_adx_country_cpm'],
                data['data_adx_country_ecpm'],
                data['data_adx_country_total_requests'],
                data['data_adx_country_response_served'],
                data['data_adx_country_match_rate'],
                data['data_adx_country_fill_rate'],
                data['data_adx_country_active_view_pct_viewable'],
                data['data_adx_country_active_view_avg_time_sec'],
                data['data_adx_country_revenue'],
                data['mdb'],
                data['mdb_name'],
                data['mdd']
            )):
                raise pymysql.Error("Failed to insert data adx country")
            if not self.commit():
                raise pymysql.Error("Failed to commit data adx country insert")

            hasil = {
                "status": True,
                "message": "Data adx country berhasil ditambahkan"
            }
        except pymysql.Error as e:
            hasil = {
                "status": False,
                'data': 'Terjadi error {!r}, error nya {}'.format(e, e.args[0])
            }
        return {'hasil': hasil}

    def delete_data_adx_domain_by_date_account(self, account_id, start_date, site_name):
        try:
            sql_delete = (
                "DELETE FROM data_adx_domain WHERE account_id = %s AND data_adx_domain_tanggal LIKE %s AND data_adx_domain LIKE %s"
            )
            if not self.execute_query(sql_delete, (account_id, f"{start_date}%", site_name)):
                raise pymysql.Error("Failed to delete data adx domain by date range")
            affected_rows = self.cur_hris.rowcount if hasattr(self, 'cur_hris') and self.cur_hris else 0
            if not self.commit():
                raise pymysql.Error("Failed to commit data adx delete")
            hasil = {
                "status": True,
                "message": "Data adx domain berhasil dihapus",
                "affected": affected_rows
            }
        except pymysql.Error as e:
            err_code = None
            err_msg = None
            try:
                if isinstance(getattr(e, 'args', None), (list, tuple)) and len(e.args) >= 2:
                    err_code = e.args[0]
                    err_msg = e.args[1]
                elif isinstance(getattr(e, 'args', None), (list, tuple)) and len(e.args) == 1:
                    err_code = e.args[0]
            except Exception:
                err_code = None
                err_msg = None

            hasil = {
                "status": False,
                'data': 'Terjadi error {!r}, code={}, message={}'.format(e, err_code, err_msg)
            }
        return {'hasil': hasil}

    def insert_data_adx_domain(self, data):
        try:
            self.last_error = None

            sql_insert_with_id = """
                        INSERT INTO data_adx_domain
                        (
                            data_adx_domain.data_adx_domain_id,
                            data_adx_domain.account_id,
                            data_adx_domain.data_adx_domain_tanggal,
                            data_adx_domain.data_adx_domain,
                            data_adx_domain.data_adx_domain_impresi,
                            data_adx_domain.data_adx_domain_click,
                            data_adx_domain.data_adx_domain_cpc,
                            data_adx_domain.data_adx_domain_ctr,
                            data_adx_domain.data_adx_domain_cpm,
                            data_adx_domain.data_adx_domain_ecpm,
                            data_adx_domain.data_adx_domain_total_requests,
                            data_adx_domain.data_adx_domain_responses_served,
                            data_adx_domain.data_adx_domain_match_rate,
                            data_adx_domain.data_adx_domain_fill_rate,
                            data_adx_domain.data_adx_domain_active_view_pct_viewable,
                            data_adx_domain.data_adx_domain_active_view_avg_time_sec,
                            data_adx_domain.data_adx_domain_revenue,
                            data_adx_domain.mdb,
                            data_adx_domain.mdb_name,
                            data_adx_domain.mdd
                        )
                    VALUES
                        (
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s
                        )
                """

            sql_insert_no_id = """
                        INSERT INTO data_adx_domain
                        (
                            data_adx_domain.account_id,
                            data_adx_domain.data_adx_domain_tanggal,
                            data_adx_domain.data_adx_domain,
                            data_adx_domain.data_adx_domain_impresi,
                            data_adx_domain.data_adx_domain_click,
                            data_adx_domain.data_adx_domain_cpc,
                            data_adx_domain.data_adx_domain_ctr,
                            data_adx_domain.data_adx_domain_cpm,
                            data_adx_domain.data_adx_domain_ecpm,
                            data_adx_domain.data_adx_domain_total_requests,
                            data_adx_domain.data_adx_domain_responses_served,
                            data_adx_domain.data_adx_domain_match_rate,
                            data_adx_domain.data_adx_domain_fill_rate,
                            data_adx_domain.data_adx_domain_active_view_pct_viewable,
                            data_adx_domain.data_adx_domain_active_view_avg_time_sec,
                            data_adx_domain.data_adx_domain_revenue,
                            data_adx_domain.mdb,
                            data_adx_domain.mdb_name,
                            data_adx_domain.mdd
                        )
                    VALUES
                        (
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s
                        )
                """

            params_with_id = (
                data.get('data_adx_domain_id'),
                data.get('account_id'),
                data.get('data_adx_domain_tanggal'),
                data.get('data_adx_domain'),
                data.get('data_adx_domain_impresi'),
                data.get('data_adx_domain_click'),
                data.get('data_adx_domain_cpc'),
                data.get('data_adx_domain_ctr'),
                data.get('data_adx_domain_cpm'),
                data.get('data_adx_domain_ecpm'),
                data.get('data_adx_domain_total_requests'),
                data.get('data_adx_domain_responses_served'),
                data.get('data_adx_domain_match_rate'),
                data.get('data_adx_domain_fill_rate'),
                data.get('data_adx_domain_active_view_pct_viewable'),
                data.get('data_adx_domain_active_view_avg_time_sec'),
                data.get('data_adx_domain_revenue'),
                data.get('mdb'),
                data.get('mdb_name'),
                data.get('mdd')
            )

            ok = self.execute_query(sql_insert_with_id, params_with_id)
            if not ok:
                last = str(self.last_error or '')
                if 'data_adx_domain_id' in last and 'Unknown column' in last:
                    params_no_id = params_with_id[1:]
                    ok = self.execute_query(sql_insert_no_id, params_no_id)

            if not ok:
                raise pymysql.Error(self.last_error or 'Failed to insert data adx domain')

            if not self.commit():
                raise pymysql.Error("Failed to commit data adx domain insert")
            hasil = {
                "status": True,
                "message": "Data adx domain berhasil ditambahkan"
            }
        except pymysql.Error as e:
            err_code = None
            err_msg = None
            try:
                if isinstance(getattr(e, 'args', None), (list, tuple)) and len(e.args) >= 2:
                    err_code = e.args[0]
                    err_msg = e.args[1]
                elif isinstance(getattr(e, 'args', None), (list, tuple)) and len(e.args) == 1:
                    err_code = e.args[0]
            except Exception:
                err_code = None
                err_msg = None

            hasil = {
                "status": False,
                'data': 'Terjadi error {!r}, code={}, message={}'.format(e, err_code, err_msg)
            }
        return {'hasil': hasil}
        
    def delete_data_adsense_country_by_date_range(self, start_date, end_date):
        try:
            sql = (
                "DELETE FROM data_adsense WHERE DATE(data_adsense_tanggal) BETWEEN %s AND %s"
            )
            if not self.execute_query(sql, (start_date, end_date)):
                raise pymysql.Error("Failed to delete data_adsense by date range")
            deleted = self.cur_hris.rowcount if hasattr(self, 'cur_hris') and self.cur_hris else 0
            if not self.commit():
                raise pymysql.Error("Failed to commit delete data_adsense by date range")
            hasil = {
                "status": True,
                "message": "Berhasil menghapus data adsense dalam rentang tanggal",
                "deleted": deleted
            }
        except pymysql.Error as e:
            hasil = {
                "status": False,
                'data': 'Terjadi error {!r}, error nya {}'.format(e, e.args[0])
            }
        return {'hasil': hasil}

    def insert_data_adsense_country(self, data):
        try:
            sql_insert = """
                        INSERT INTO data_adsense_country
                        (
                            data_adsense_country.account_id,
                            data_adsense_country.data_adsense_country_tanggal,
                            data_adsense_country.data_adsense_country_cd,
                            data_adsense_country.data_adsense_country_nm,
                            data_adsense_country.data_adsense_country_domain,
                            data_adsense_country.data_adsense_country_impresi,
                            data_adsense_country.data_adsense_country_click,
                            data_adsense_country.data_adsense_country_cpc,
                            data_adsense_country.data_adsense_country_ctr,
                            data_adsense_country.data_adsense_country_cpm,
                            data_adsense_country.data_adsense_country_page_views,
                            data_adsense_country.data_adsense_country_page_views_rpm,
                            data_adsense_country.data_adsense_country_ad_requests,
                            data_adsense_country.data_adsense_country_ad_requests_coverage,
                            data_adsense_country.data_adsense_country_active_view_viewability,
                            data_adsense_country.data_adsense_country_active_view_measurability,
                            data_adsense_country.data_adsense_country_active_view_time,
                            data_adsense_country.data_adsense_country_revenue,
                            data_adsense_country.mdb,
                            data_adsense_country.mdb_name,
                            data_adsense_country.mdd
                        )
                    VALUES
                        (
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s
                        )
                """
            if not self.execute_query(sql_insert, (
                data['account_id'],
                data['data_adsense_country_tanggal'],
                data['data_adsense_country_cd'],
                data['data_adsense_country_nm'],
                data['data_adsense_country_domain'],
                data['data_adsense_country_impresi'],
                data['data_adsense_country_click'],
                data['data_adsense_country_cpc'],
                data['data_adsense_country_ctr'],
                data['data_adsense_country_cpm'],
                data['data_adsense_country_page_views'],
                data['data_adsense_country_page_views_rpm'],
                data['data_adsense_country_ad_requests'],
                data['data_adsense_country_ad_requests_coverage'],
                data['data_adsense_country_active_view_viewability'],
                data['data_adsense_country_active_view_measurability'],
                data['data_adsense_country_active_view_time'],
                data['data_adsense_country_revenue'],
                data['mdb'],
                data['mdb_name'],
                data['mdd']
            )):
                raise pymysql.Error("Failed to insert data adsense country")
            if not self.commit():
                raise pymysql.Error("Failed to commit data adsense country insert")

            hasil = {
                "status": True,
                "message": "Data adsense country berhasil ditambahkan"
            }
        except pymysql.Error as e:
            hasil = {
                "status": False,
                'data': 'Terjadi error {!r}, error nya {}'.format(e, e.args[0])
            }
        return {'hasil': hasil}

    def get_all_adsense_traffic_account_by_params(self, start_date, end_date, account_list = None, selected_domain_list = None, force_clickhouse: bool = False):
        try:
            if isinstance(account_list, str):
                account_list = [account_list.strip()]
            elif account_list is None:
                account_list = []
            elif isinstance(account_list, (set, tuple)):
                account_list = list(account_list)
            data_account_list = [str(a).strip() for a in account_list if str(a).strip()]

            engine = (self._report_engine() or '').strip().lower()
            use_clickhouse = bool(force_clickhouse) or (engine in ('clickhouse', 'ch'))

            if isinstance(selected_domain_list, str):
                selected_domain_list = [selected_domain_list.strip()]
            elif selected_domain_list is None:
                selected_domain_list = []
            elif isinstance(selected_domain_list, (set, tuple)):
                selected_domain_list = list(selected_domain_list)
            data_domain_list = [str(d).strip() for d in selected_domain_list if str(d).strip()]
            expanded_domain_tokens = []
            for _d in data_domain_list:
                s = str(_d or '').strip().lower().strip('.')
                if not s:
                    continue
                parts = [p for p in s.split('.') if p]
                cands = [s]
                if len(parts) >= 2:
                    cands.append('.'.join(parts[:2]))
                    cands.append('.'.join(parts[-2:]))
                for c in cands:
                    if c and c not in expanded_domain_tokens:
                        expanded_domain_tokens.append(c)

            params = [start_date, end_date]
            if use_clickhouse:
                account_col = "toString(b.account_id)"
                base_sql = [
                    "SELECT",
                    "\tb.account_id AS account_id,",
                    "\tifNull(any(a.account_name), '') AS account_name,",
                    "\tifNull(any(a.user_mail), '') AS user_mail,",
                    "\ttoDate(b.data_adsense_country_tanggal) AS date,",
                    "\tb.data_adsense_country_domain AS site_name,",
                    "\tb.data_adsense_country_cd AS country_code,",
                    "\tSUM(b.data_adsense_country_impresi) AS impressions_adsense,",
                    "\tSUM(b.data_adsense_country_click) AS clicks_adsense,",
                    "\tSUM(b.data_adsense_country_page_views) AS page_views,",
                    "\tSUM(b.data_adsense_country_ad_requests) AS ad_requests,",
                    "\tSUM(COALESCE(b.data_adsense_country_ad_requests_coverage, 0) * COALESCE(b.data_adsense_country_ad_requests, 0)) AS ad_requests_coverage_weighted_sum,",
                    "\tSUM(COALESCE(b.data_adsense_country_active_view_viewability, 0) * COALESCE(b.data_adsense_country_impresi, 0)) AS active_view_viewability_weighted_sum,",
                    "\tSUM(COALESCE(b.data_adsense_country_active_view_measurability, 0) * COALESCE(b.data_adsense_country_impresi, 0)) AS active_view_measurability_weighted_sum,",
                    "\tSUM(COALESCE(b.data_adsense_country_active_view_time, 0) * COALESCE(b.data_adsense_country_impresi, 0)) AS active_view_time_weighted_sum,",
                    "\tCASE WHEN SUM(b.data_adsense_country_click) > 0 THEN ROUND(SUM(b.data_adsense_country_revenue) / SUM(b.data_adsense_country_click), 0) ELSE 0 END AS cpc_adsense,",
                    "\tCASE WHEN SUM(b.data_adsense_country_impresi) > 0 THEN ROUND((SUM(b.data_adsense_country_revenue) / SUM(b.data_adsense_country_impresi)) * 1000) ELSE 0 END AS ecpm,",
                    "\tSUM(b.data_adsense_country_revenue) AS revenue",
                    "FROM data_adsense_country b",
                    "LEFT JOIN app_credentials a ON toString(a.account_id) = toString(b.account_id)",
                    "WHERE",
                    "\ttoDate(b.data_adsense_country_tanggal) BETWEEN toDate(%s) AND toDate(%s)",
                ]
                if data_account_list:
                    like_conditions_account = " OR ".join([f"{account_col} LIKE %s"] * len(data_account_list))
                    base_sql.append(f"\tAND ({like_conditions_account})")
                    params.extend([f"%{account}%" for account in data_account_list])
                if data_domain_list:
                    like_conditions_domain = " OR ".join(["b.data_adsense_country_domain LIKE %s"] * len(data_domain_list))
                    base_sql.append(f"\tAND ({like_conditions_domain})")
                    params.extend([f"%{domain}%" for domain in data_domain_list])
                base_sql.append("GROUP BY b.account_id, toDate(b.data_adsense_country_tanggal), b.data_adsense_country_domain, b.data_adsense_country_cd")
                base_sql.append("ORDER BY date ASC")
            else:
                like_conditions_account = " OR ".join(["b.account_id LIKE %s"] * len(data_account_list))
                like_conditions_domain = " OR ".join(["b.data_adsense_country_domain LIKE %s"] * len(data_domain_list))
                base_sql = [
                    "SELECT",
                    "\ta.account_id, a.account_name, a.user_mail,",
                    "\tb.data_adsense_country_tanggal AS 'date',",
                    "\tb.data_adsense_country_domain AS 'site_name',",
                    "\tb.data_adsense_country_cd AS 'country_code',",
                    "\tSUM(b.data_adsense_country_impresi) AS 'impressions_adsense',",
                    "\tSUM(b.data_adsense_country_click) AS 'clicks_adsense',",
                    "\tSUM(b.data_adsense_country_page_views) AS 'page_views',",
                    "\tSUM(b.data_adsense_country_ad_requests) AS 'ad_requests',",
                    "\tSUM(COALESCE(b.data_adsense_country_ad_requests_coverage, 0) * COALESCE(b.data_adsense_country_ad_requests, 0)) AS 'ad_requests_coverage_weighted_sum',",
                    "\tSUM(COALESCE(b.data_adsense_country_active_view_viewability, 0) * COALESCE(b.data_adsense_country_impresi, 0)) AS 'active_view_viewability_weighted_sum',",
                    "\tSUM(COALESCE(b.data_adsense_country_active_view_measurability, 0) * COALESCE(b.data_adsense_country_impresi, 0)) AS 'active_view_measurability_weighted_sum',",
                    "\tSUM(COALESCE(b.data_adsense_country_active_view_time, 0) * COALESCE(b.data_adsense_country_impresi, 0)) AS 'active_view_time_weighted_sum',",
                    "\tCASE WHEN SUM(b.data_adsense_country_click) > 0 THEN ROUND(SUM(b.data_adsense_country_revenue) / SUM(b.data_adsense_country_click), 0) ELSE 0 END AS 'cpc_adsense',",
                    "\tCASE WHEN SUM(b.data_adsense_country_impresi) > 0 THEN ROUND((SUM(b.data_adsense_country_revenue) / SUM(b.data_adsense_country_impresi)) * 1000) ELSE 0 END AS 'ecpm',",
                    "\tSUM(b.data_adsense_country_revenue) AS 'revenue'",
                    "FROM app_credentials a",
                    "INNER JOIN data_adsense_country b ON a.account_id = b.account_id",
                    "WHERE",
                    "\tDATE(b.data_adsense_country_tanggal) BETWEEN %s AND %s",
                ]
                if data_account_list:
                    base_sql.append(f"\tAND ({like_conditions_account})")
                    params.extend([f"%{account}%" for account in data_account_list])
                if data_domain_list:
                    base_sql.append(f"\tAND ({like_conditions_domain})")
                    params.extend([f"%{domain}%" for domain in data_domain_list])
                base_sql.append("GROUP BY b.data_adsense_country_tanggal, a.account_id, b.data_adsense_country_domain, b.data_adsense_country_cd")
                base_sql.append("ORDER BY b.data_adsense_country_tanggal ASC")

            sql = "\n".join(base_sql)
            if use_clickhouse:
                self._ensure_report_connection()
                self.cur_hris = self.report_cur
                self.cur_hris.execute(sql, tuple(params))
                data = self.fetch_all()

                if isinstance(data, list) and data:
                    try:
                        account_ids = []
                        seen_ids = set()
                        for r in data:
                            aid = str((r or {}).get('account_id') or '').strip()
                            if aid and aid not in seen_ids:
                                seen_ids.add(aid)
                                account_ids.append(aid)
                        if account_ids:
                            placeholders = ','.join(['%s'] * len(account_ids))
                            sql_map = f"SELECT account_id, account_ads_id, account_name FROM master_account_ads WHERE account_id IN ({placeholders}) OR account_ads_id IN ({placeholders})"
                            if self.execute_query(sql_map, tuple(account_ids + account_ids)):
                                rows_map = self.fetch_all() or []
                                name_map = {}
                                for x in rows_map:
                                    if not isinstance(x, dict):
                                        continue
                                    nm = str((x or {}).get('account_name') or '').strip()
                                    if not nm:
                                        continue
                                    k1 = str((x or {}).get('account_id') or '').strip()
                                    k2 = str((x or {}).get('account_ads_id') or '').strip()
                                    if k1:
                                        name_map[k1] = nm
                                        name_map[k1.lower().replace('act_', '', 1)] = nm
                                    if k2:
                                        name_map[k2] = nm
                                        name_map[k2.lower().replace('act_', '', 1)] = nm
                                for r in data:
                                    if not isinstance(r, dict):
                                        continue
                                    if str(r.get('account_name') or '').strip():
                                        continue
                                    aid = str(r.get('account_id') or '').strip()
                                    norm_aid = aid.lower().replace('act_', '', 1)
                                    r['account_name'] = name_map.get(aid) or name_map.get(norm_aid) or ''
                    except Exception:
                        pass
            else:
                if not self.execute_query(sql, tuple(params)):
                    raise pymysql.Error("Failed to get all adsense traffic account by params")
                data = self.fetch_all()
                if not self.commit():
                    raise pymysql.Error("Failed to commit get all adsense traffic account by params")
            hasil = {
                "status": True,
                "message": "Data adsense traffic account berhasil diambil",
                "data": data
            }
        except pymysql.Error as e:
            hasil = {
                "status": False,
                'data': 'Terjadi error {!r}, error nya {}'.format(e, e.args[0])
            }
        return {'hasil': hasil}

    def get_all_adsense_traffic_country_by_params(self, start_date, end_date, selected_account_list = None, countries_list = None, selected_domain_list = None, force_clickhouse: bool = False):
        try:
            # --- 0. Pastikan selected_account_list adalah list string
            if isinstance(selected_account_list, str):
                selected_account_list = [selected_account_list.strip()]
            elif selected_account_list is None:
                selected_account_list = []
            elif isinstance(selected_account_list, (set, tuple)):
                selected_account_list = list(selected_account_list)
            data_account_list = [str(a).strip() for a in selected_account_list if str(a).strip()]

            engine = (self._report_engine() or '').strip().lower()
            use_clickhouse = bool(force_clickhouse) or (engine in ('clickhouse', 'ch'))

            like_account_col = "toString(b.account_id)" if use_clickhouse else "a.account_id"
            like_conditions_account = " OR ".join([f"{like_account_col} LIKE %s"] * len(data_account_list))
            like_params_account = [f"%{account}%" for account in data_account_list]

            if isinstance(selected_domain_list, str):
                selected_domain_list = [selected_domain_list.strip()]
            elif selected_domain_list is None:
                selected_domain_list = []
            elif isinstance(selected_domain_list, (set, tuple)):
                selected_domain_list = list(selected_domain_list)
            data_domain_list = [str(d).strip() for d in selected_domain_list if str(d).strip()]
            like_conditions_domain = " OR ".join(["b.data_adsense_country_domain LIKE %s"] * len(data_domain_list))
            like_params_domain = [f"%{domain}%" for domain in data_domain_list]

            params = []
            if use_clickhouse:
                base_sql = [
                    "SELECT",
                    "\tifNull(any(toString(b.account_id)), '') AS account_id,",
                    "\tifNull(any(a.account_name), '') AS account_name,",
                    "\tifNull(any(a.user_mail), '') AS user_mail,",
                    "\tb.data_adsense_country_nm AS country_name,",
                    "\tb.data_adsense_country_cd AS country_code,",
                    "\tSUM(b.data_adsense_country_impresi) AS impressions,",
                    "\tSUM(b.data_adsense_country_click) AS clicks,",
                    "\tSUM(b.data_adsense_country_page_views) AS page_views,",
                    "\tCASE WHEN SUM(b.data_adsense_country_page_views) > 0 THEN ROUND(SUM(COALESCE(b.data_adsense_country_page_views_rpm, 0) * COALESCE(b.data_adsense_country_page_views, 0)) / SUM(b.data_adsense_country_page_views), 2) ELSE 0 END AS page_views_rpm,",
                    "\tSUM(b.data_adsense_country_ad_requests) AS ad_requests,",
                    "\tCASE WHEN SUM(b.data_adsense_country_ad_requests) > 0 THEN ROUND(SUM(COALESCE(b.data_adsense_country_ad_requests_coverage, 0) * COALESCE(b.data_adsense_country_ad_requests, 0)) / SUM(b.data_adsense_country_ad_requests), 2) ELSE 0 END AS ad_requests_coverage,",
                    "\tCASE WHEN SUM(b.data_adsense_country_impresi) > 0 THEN ROUND(SUM(COALESCE(b.data_adsense_country_active_view_viewability, 0) * COALESCE(b.data_adsense_country_impresi, 0)) / SUM(b.data_adsense_country_impresi), 2) ELSE 0 END AS active_view_viewability,",
                    "\tCASE WHEN SUM(b.data_adsense_country_impresi) > 0 THEN ROUND(SUM(COALESCE(b.data_adsense_country_active_view_measurability, 0) * COALESCE(b.data_adsense_country_impresi, 0)) / SUM(b.data_adsense_country_impresi), 2) ELSE 0 END AS active_view_measurability,",
                    "\tCASE WHEN SUM(b.data_adsense_country_impresi) > 0 THEN ROUND(SUM(COALESCE(b.data_adsense_country_active_view_time, 0) * COALESCE(b.data_adsense_country_impresi, 0)) / SUM(b.data_adsense_country_impresi), 2) ELSE 0 END AS active_view_time,",
                    "\tCASE WHEN SUM(b.data_adsense_country_impresi) > 0 THEN ROUND((SUM(b.data_adsense_country_click) / SUM(b.data_adsense_country_impresi)) * 100, 2) ELSE 0 END AS ctr,",
                    "\tCASE WHEN SUM(b.data_adsense_country_impresi) > 0 THEN ROUND((SUM(b.data_adsense_country_revenue) / SUM(b.data_adsense_country_impresi)) * 1000) ELSE 0 END AS ecpm,",
                    "\tCASE WHEN SUM(b.data_adsense_country_click) > 0 THEN ROUND(SUM(b.data_adsense_country_revenue) / SUM(b.data_adsense_country_click), 0) ELSE 0 END AS cpc,",
                    "\tSUM(b.data_adsense_country_revenue) AS revenue",
                    "FROM data_adsense_country b",
                    "LEFT JOIN app_credentials a ON toString(a.account_id) = toString(b.account_id)",
                    "WHERE",
                    "\ttoDate(b.data_adsense_country_tanggal) BETWEEN toDate(%s) AND toDate(%s)",
                ]
            else:
                base_sql = [
                    "SELECT",
                    "\ta.account_id, a.account_name, a.user_mail,",
                    "\tb.data_adsense_country_nm AS country_name,",
                    "\tb.data_adsense_country_cd AS country_code,",
                    "\tSUM(b.data_adsense_country_impresi) AS impressions,",
                    "\tSUM(b.data_adsense_country_click) AS clicks,",
                    "\tSUM(b.data_adsense_country_page_views) AS page_views,",
                    "\tCASE WHEN SUM(b.data_adsense_country_page_views) > 0",
                    "\t\tTHEN ROUND(SUM(COALESCE(b.data_adsense_country_page_views_rpm, 0) * COALESCE(b.data_adsense_country_page_views, 0)) / SUM(b.data_adsense_country_page_views), 2)",
                    "\t\tELSE 0 END AS page_views_rpm,",
                    "\tSUM(b.data_adsense_country_ad_requests) AS ad_requests,",
                    "\tCASE WHEN SUM(b.data_adsense_country_ad_requests) > 0",
                    "\t\tTHEN ROUND(SUM(COALESCE(b.data_adsense_country_ad_requests_coverage, 0) * COALESCE(b.data_adsense_country_ad_requests, 0)) / SUM(b.data_adsense_country_ad_requests), 2)",
                    "\t\tELSE 0 END AS ad_requests_coverage,",
                    "\tCASE WHEN SUM(b.data_adsense_country_impresi) > 0",
                    "\t\tTHEN ROUND(SUM(COALESCE(b.data_adsense_country_active_view_viewability, 0) * COALESCE(b.data_adsense_country_impresi, 0)) / SUM(b.data_adsense_country_impresi), 2)",
                    "\t\tELSE 0 END AS active_view_viewability,",
                    "\tCASE WHEN SUM(b.data_adsense_country_impresi) > 0",
                    "\t\tTHEN ROUND(SUM(COALESCE(b.data_adsense_country_active_view_measurability, 0) * COALESCE(b.data_adsense_country_impresi, 0)) / SUM(b.data_adsense_country_impresi), 2)",
                    "\t\tELSE 0 END AS active_view_measurability,",
                    "\tCASE WHEN SUM(b.data_adsense_country_impresi) > 0",
                    "\t\tTHEN ROUND(SUM(COALESCE(b.data_adsense_country_active_view_time, 0) * COALESCE(b.data_adsense_country_impresi, 0)) / SUM(b.data_adsense_country_impresi), 2)",
                    "\t\tELSE 0 END AS active_view_time,",
                    "\tCASE WHEN SUM(b.data_adsense_country_impresi) > 0",
                    "\t\tTHEN ROUND((SUM(b.data_adsense_country_click) / SUM(b.data_adsense_country_impresi)) * 100, 2)",
                    "\t\tELSE 0 END AS ctr,",
                    "\tCASE WHEN SUM(b.data_adsense_country_impresi) > 0",
                    "\t\tTHEN ROUND((SUM(b.data_adsense_country_revenue) / SUM(b.data_adsense_country_impresi)) * 1000)",
                    "\t\tELSE 0 END AS ecpm,",
                    "\tCASE WHEN SUM(b.data_adsense_country_click) > 0",
                    "\t\tTHEN ROUND(SUM(b.data_adsense_country_revenue) / SUM(b.data_adsense_country_click), 0)",
                    "\t\tELSE 0 END AS cpc,",
                    "\tSUM(b.data_adsense_country_revenue) AS revenue",
                    "FROM app_credentials a",
                    "INNER JOIN data_adsense_country b ON a.account_id = b.account_id",
                    "WHERE",
                    "\tb.data_adsense_country_tanggal BETWEEN %s AND %s",
                ]
            params.extend([start_date, end_date])
            # Normalize selected_account_list and apply account filter
            if data_account_list:
                base_sql.append(f"\tAND ({like_conditions_account})")
                params.extend(like_params_account)
            if data_domain_list:
                base_sql.append(f"\tAND ({like_conditions_domain})")
                params.extend(like_params_domain)
            # Normalize countries_list and apply country code filter
            country_codes = []
            if countries_list:
                if isinstance(countries_list, str):
                    country_codes = [c.strip() for c in countries_list.split(',') if c.strip()]
                elif isinstance(countries_list, (list, tuple)):
                    country_codes = [str(c).strip() for c in countries_list if str(c).strip()]
            if country_codes:
                placeholders = ','.join(['%s'] * len(country_codes))
                base_sql.append(f"AND b.data_adsense_country_cd IN ({placeholders})")
                params.extend(country_codes)
            base_sql.append("GROUP BY b.data_adsense_country_cd, b.data_adsense_country_nm")
            base_sql.append("ORDER BY revenue DESC")
            sql = "\n".join(base_sql)
            if use_clickhouse:
                self._ensure_report_connection()
                self.cur_hris = self.report_cur
                self.cur_hris.execute(sql, tuple(params))
                data_rows = self.fetch_all()
            else:
                if not self.execute_query(sql, tuple(params)):
                    raise pymysql.Error("Failed to get all adsense traffic country by params")
                data_rows = self.fetch_all()
                if not self.commit():
                    raise pymysql.Error("Failed to commit get all adsense traffic country by params")
            # Build summary
            total_impressions = sum((row.get('impressions') or 0) for row in data_rows) if data_rows else 0
            total_clicks = sum((row.get('clicks') or 0) for row in data_rows) if data_rows else 0
            total_revenue = sum((row.get('revenue') or 0) for row in data_rows) if data_rows else 0.0
            total_ctr_ratio = (float(total_clicks) / float(total_impressions)) if total_impressions else 0.0
            return {
                "status": True,
                "message": "Data adsense traffic country berhasil diambil",
                "data": data_rows,
                "summary": {
                    "total_impressions": total_impressions,
                    "total_clicks": total_clicks,
                    "total_revenue": total_revenue,
                    "total_ctr": total_ctr_ratio
                }
            }
        except pymysql.Error as e:
            return {
                "status": False,
                "error": f"Terjadi error {e!r}, error nya {e.args[0]}"
            }


    def insert_data_adsense_domain(self, data):
        try:
            sql_insert = """
                        INSERT INTO data_adsense_domain
                        (
                            account_id,
                            data_adsense_tanggal,
                            data_adsense_domain,
                            data_adsense_impresi,
                            data_adsense_click,
                            data_adsense_ctr,
                            data_adsense_cpc,
                            data_adsense_cpm,
                            data_adsense_page_views,
                            data_adsense_page_views_rpm,
                            data_adsense_ad_requests,
                            data_adsense_ad_requests_coverage,
                            data_adsense_active_view_viewability,
                            data_adsense_active_view_measurability,
                            data_adsense_active_view_time,
                            data_adsense_revenue,
                            mdb,
                            mdb_name,
                            mdd
                        )
                    VALUES
                        (
                            %s, 
                            %s, 
                            %s, 
                            %s, 
                            %s, 
                            %s, 
                            %s, 
                            %s, 
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s, 
                            %s, 
                            %s
                        )   
                """
            if not self.execute_query(sql_insert, (
                data['account_id'],
                data['data_adsense_tanggal'],
                data['data_adsense_domain'],
                data['data_adsense_impresi'],
                data['data_adsense_click'],
                data['data_adsense_ctr'],
                data['data_adsense_cpc'],
                data['data_adsense_cpm'],
                data.get('data_adsense_page_views', 0),
                data.get('data_adsense_page_views_rpm', 0),
                data.get('data_adsense_ad_requests', 0),
                data.get('data_adsense_ad_requests_coverage', 0),
                data.get('data_adsense_active_view_viewability', 0),
                data.get('data_adsense_active_view_measurability', 0),
                data.get('data_adsense_active_view_time', 0),
                data['data_adsense_revenue'],
                data['mdb'],
                data['mdb_name'],
                data['mdd']
            )):
                raise pymysql.Error("Failed to insert data adsense domain")
            if not self.commit():
                raise pymysql.Error("Failed to commit data adsense domain insert")

            hasil = {
                "status": True,
                "message": "Data adsense domain berhasil ditambahkan"
            }
        except pymysql.Error as e:
            hasil = {
                "status": False,
                'data': 'Terjadi error {!r}, error nya {}'.format(e, e.args[0])
            }
        return {'hasil': hasil}
        
    def get_all_adx_traffic_account_by_params(self, start_date, end_date, account_list = None, selected_domain_list = None, force_clickhouse: bool = False):
        try:
            if isinstance(account_list, str):
                account_list = [account_list.strip()]
            elif account_list is None:
                account_list = []
            elif isinstance(account_list, (set, tuple)):
                account_list = list(account_list)
            data_account_list = [str(a).strip() for a in account_list if str(a).strip()]

            engine = (self._report_engine() or '').strip().lower()
            use_clickhouse = engine in ('clickhouse', 'ch')

            if isinstance(selected_domain_list, str):
                selected_domain_list = [selected_domain_list.strip()]
            elif selected_domain_list is None:
                selected_domain_list = []
            elif isinstance(selected_domain_list, (set, tuple)):
                selected_domain_list = list(selected_domain_list)
            data_domain_list = [str(d).strip() for d in selected_domain_list if str(d).strip()]

            account_tokens = []
            for a in (data_account_list or []):
                v = str(a or '').strip()
                if not v:
                    continue
                account_tokens.append(v)
                if v.lower().startswith('act_'):
                    account_tokens.append(v[4:])
                else:
                    account_tokens.append(f"act_{v}")
            account_tokens = list(dict.fromkeys([x for x in account_tokens if x]))

            if use_clickhouse:
                # ClickHouse query (pakai report cursor langsung, tanpa commit)
                params = [start_date, end_date]
                account_col = "toString(b.account_id)"
                base_sql = [
                    "SELECT",
                    "\tb.account_id AS account_id,",
                    "\t'' AS account_name,",
                    "\t'' AS user_mail,",
                    "\ttoDate(b.data_adx_country_tanggal) AS date,",
                    "\tb.data_adx_country_domain AS site_name,",
                    "\tb.data_adx_country_cd AS country_code,",
                    "\tSUM(b.data_adx_country_impresi) AS impressions_adx,",
                    "\tSUM(b.data_adx_country_click) AS clicks_adx,",
                    "\tCASE WHEN SUM(b.data_adx_country_click) > 0 THEN ROUND(SUM(b.data_adx_country_revenue) / SUM(b.data_adx_country_click), 0) ELSE 0 END AS cpc_adx,",
                    "\tCASE WHEN SUM(b.data_adx_country_impresi) > 0 THEN ROUND((SUM(b.data_adx_country_revenue) / SUM(b.data_adx_country_impresi)) * 1000) ELSE 0 END AS ecpm,",
                    "\tSUM(b.data_adx_country_total_requests) AS total_requests,",
                    "\tSUM(b.data_adx_country_responses_served) AS responses_served,",
                    "\tCASE WHEN SUM(b.data_adx_country_impresi) > 0 THEN ROUND(SUM(b.data_adx_country_active_view_pct_viewable * b.data_adx_country_impresi) / SUM(b.data_adx_country_impresi), 2) ELSE 0 END AS active_view_pct_viewable,",
                    "\tCASE WHEN SUM(b.data_adx_country_impresi) > 0 THEN ROUND(SUM(b.data_adx_country_active_view_avg_time_sec * b.data_adx_country_impresi) / SUM(b.data_adx_country_impresi), 2) ELSE 0 END AS active_view_avg_time_sec,",
                    "\tSUM(b.data_adx_country_revenue) AS revenue",
                    "FROM data_adx_country b",
                    "WHERE",
                    "\ttoDate(b.data_adx_country_tanggal) BETWEEN toDate(%s) AND toDate(%s)",
                ]
                if account_tokens:
                    like_conditions_account = " OR ".join([f"{account_col} LIKE %s"] * len(account_tokens))
                    base_sql.append(f"\tAND ({like_conditions_account})")
                    params.extend([f"%{account}%" for account in account_tokens])
                if data_domain_list:
                    like_conditions_domain = " OR ".join(["lowerUTF8(b.data_adx_country_domain) LIKE lowerUTF8(%s)"] * len(data_domain_list))
                    base_sql.append(f"\tAND ({like_conditions_domain})")
                    params.extend([f"%{domain}%" for domain in data_domain_list])
                base_sql.append("GROUP BY b.account_id, toDate(b.data_adx_country_tanggal), b.data_adx_country_domain, b.data_adx_country_cd")
                base_sql.append("ORDER BY date ASC")

                sql = "\n".join(base_sql)
                self._ensure_report_connection()
                self.cur_hris = self.report_cur
                self.cur_hris.execute(sql, tuple(params))
                data = self.fetch_all()
            else:
                # MySQL query
                params = [start_date, end_date]
                like_conditions_account = " OR ".join(["a.account_id LIKE %s"] * len(data_account_list))
                like_conditions_domain = " OR ".join(["b.data_adx_country_domain LIKE %s"] * len(data_domain_list))
                base_sql = [
                    "SELECT",
                    "\ta.account_id, a.account_name, a.user_mail,",
                    "\tb.data_adx_country_tanggal AS 'date',",
                    "\tb.data_adx_country_domain AS 'site_name',",
                    "\tb.data_adx_country_cd AS 'country_code',",
                    "\tSUM(b.data_adx_country_impresi) AS 'impressions_adx',",
                    "\tSUM(b.data_adx_country_click) AS 'clicks_adx',",
                    "\tCASE WHEN SUM(b.data_adx_country_click) > 0 THEN ROUND(SUM(b.data_adx_country_revenue) / SUM(b.data_adx_country_click), 0) ELSE 0 END AS 'cpc_adx',",
                    "\tCASE WHEN SUM(b.data_adx_country_impresi) > 0 THEN ROUND((SUM(b.data_adx_country_revenue) / SUM(b.data_adx_country_impresi)) * 1000) ELSE 0 END AS 'ecpm',",
                    "\tSUM(b.data_adx_country_total_requests) AS 'total_requests',",
                    "\tSUM(b.data_adx_country_responses_served) AS 'responses_served',",
                    "\tCASE WHEN SUM(b.data_adx_country_impresi) > 0 THEN ROUND(SUM(b.data_adx_country_active_view_pct_viewable * b.data_adx_country_impresi) / SUM(b.data_adx_country_impresi), 2) ELSE 0 END AS 'active_view_pct_viewable',",
                    "\tCASE WHEN SUM(b.data_adx_country_impresi) > 0 THEN ROUND(SUM(b.data_adx_country_active_view_avg_time_sec * b.data_adx_country_impresi) / SUM(b.data_adx_country_impresi), 2) ELSE 0 END AS 'active_view_avg_time_sec',",
                    "\tSUM(b.data_adx_country_revenue) AS 'revenue'",
                    "FROM app_credentials a",
                    "INNER JOIN data_adx_country b ON a.account_id = b.account_id",
                    "WHERE",
                    "\tb.data_adx_country_tanggal BETWEEN %s AND %s",
                ]
                if data_account_list:
                    base_sql.append(f"\tAND ({like_conditions_account})")
                    params.extend([f"%{account}%" for account in data_account_list])
                if data_domain_list:
                    base_sql.append(f"\tAND ({like_conditions_domain})")
                    params.extend([f"%{domain}%" for domain in data_domain_list])
                base_sql.append("GROUP BY a.account_id, a.account_name, a.user_mail, b.data_adx_country_tanggal, b.data_adx_country_domain, b.data_adx_country_cd")
                base_sql.append("ORDER BY b.data_adx_country_tanggal ASC")

                sql = "\n".join(base_sql)
                if not self.execute_query(sql, tuple(params)):
                    raise pymysql.Error(f"Failed to get all adx traffic account by params: {self.last_error}")
                data = self.fetch_all()
                if not self.commit():
                    raise pymysql.Error("Failed to commit get all adx traffic account by params")
            hasil = {
                "status": True,
                "message": "Data adx traffic account berhasil diambil",
                "data": data
            }
        except pymysql.Error as e:
            hasil = {
                "status": False,
                'data': 'Terjadi error {!r}, error nya {}'.format(e, e.args[0])
            }
        return {'hasil': hasil}

    def get_all_rekap_adsense_traffic_account_by_params(self, start_date, end_date, account_list = None, selected_domain_list = None):
        try:
            if isinstance(account_list, str):
                account_list = [account_list.strip()]
            elif account_list is None:
                account_list = []
            elif isinstance(account_list, (set, tuple)):
                account_list = list(account_list)
            data_account_list = [str(a).strip() for a in account_list if str(a).strip()]

            engine = (self._report_engine() or '').strip().lower()
            use_clickhouse = engine in ('clickhouse', 'ch')

            if isinstance(selected_domain_list, str):
                selected_domain_list = [selected_domain_list.strip()]
            elif selected_domain_list is None:
                selected_domain_list = []
            elif isinstance(selected_domain_list, (set, tuple)):
                selected_domain_list = list(selected_domain_list)
            data_domain_list = [str(d).strip() for d in selected_domain_list if str(d).strip()]
            expanded_domain_tokens = []
            for _d in data_domain_list:
                s = str(_d or '').strip().lower().strip('.')
                if not s:
                    continue
                parts = [p for p in s.split('.') if p]
                cands = [s]
                if len(parts) >= 2:
                    cands.append('.'.join(parts[:2]))
                    cands.append('.'.join(parts[-2:]))
                for c in cands:
                    if c and c not in expanded_domain_tokens:
                        expanded_domain_tokens.append(c)

            params = [start_date, end_date]

            if use_clickhouse:
                site_expr = "concat(arrayElement(splitByChar('.', b.data_adsense_country_domain), 1), '.', arrayElement(splitByChar('.', b.data_adsense_country_domain), 2))"
                base_sql = [
                    "SELECT",
                    "\tb.account_id AS account_id,",
                    "\t'' AS account_name,",
                    "\t'' AS user_mail,",
                    "\ttoDate(b.data_adsense_country_tanggal) AS date,",
                    f"\t{site_expr} AS site_name,",
                    "\tb.data_adsense_country_cd AS country_code,",
                    "\tSUM(b.data_adsense_country_impresi) AS impressions_adsense,",
                    "\tSUM(b.data_adsense_country_click) AS clicks_adsense,",
                    "\tCASE WHEN SUM(b.data_adsense_country_click) > 0 THEN ROUND(SUM(b.data_adsense_country_revenue) / SUM(b.data_adsense_country_click), 0) ELSE 0 END AS cpc_adx,",
                    "\tCASE WHEN SUM(b.data_adsense_country_impresi) > 0 THEN ROUND((SUM(b.data_adsense_country_revenue) / SUM(b.data_adsense_country_impresi)) * 1000) ELSE 0 END AS ecpm,",
                    "\tSUM(b.data_adsense_country_revenue) AS revenue",
                    "FROM data_adsense_country b",
                    "WHERE",
                    "\tb.data_adsense_country_tanggal BETWEEN %s AND %s",
                ]
                if data_account_list:
                    like_conditions_account = " OR ".join(["b.account_id LIKE %s"] * len(data_account_list))
                    base_sql.append(f"\tAND ({like_conditions_account})")
                    params.extend([f"%{account}%" for account in data_account_list])
                if expanded_domain_tokens:
                    like_conditions_domain = " OR ".join([f"(b.data_adsense_country_domain LIKE %s OR {site_expr} LIKE %s)"] * len(expanded_domain_tokens))
                    base_sql.append(f"\tAND ({like_conditions_domain})")
                    for domain in expanded_domain_tokens:
                        d = str(domain or '').strip()
                        params.extend([f"%{d}%", f"%{d}%"]) 
                base_sql.append(f"GROUP BY b.account_id, toDate(b.data_adsense_country_tanggal), {site_expr}, b.data_adsense_country_cd")
                base_sql.append("ORDER BY date ASC")
            else:
                like_conditions_account = " OR ".join(["a.account_id LIKE %s"] * len(data_account_list))
                like_params_account = [f"%{account}%" for account in data_account_list]
                like_conditions_domain = " OR ".join(["(b.data_adsense_country_domain LIKE %s OR SUBSTRING_INDEX(b.data_adsense_country_domain, '.', 2) LIKE %s)"] * len(expanded_domain_tokens))
                like_params_domain = []
                for domain in expanded_domain_tokens:
                    d = str(domain or '').strip()
                    like_params_domain.extend([f"%{d}%", f"%{d}%"]) 
                base_sql = [
                    "SELECT",
                    "\ta.account_id, a.account_name, a.user_mail,",
                    "\tb.data_adsense_country_tanggal AS 'date',",
                    "\tSUBSTRING_INDEX(b.data_adsense_country_domain, '.', 2) AS 'site_name',",
                    "\tb.data_adsense_country_cd AS 'country_code',",
                    "\tSUM(b.data_adsense_country_impresi) AS 'impressions_adsense',",
                    "\tSUM(b.data_adsense_country_click) AS 'clicks_adsense',",
                    "\tCASE WHEN SUM(b.data_adsense_country_click) > 0 THEN ROUND(SUM(b.data_adsense_country_revenue) / SUM(b.data_adsense_country_click), 0) ELSE 0 END AS 'cpc_adx',",
                    "\tCASE WHEN SUM(b.data_adsense_country_impresi) > 0 THEN ROUND((SUM(b.data_adsense_country_revenue) / SUM(b.data_adsense_country_impresi)) * 1000) ELSE 0 END AS 'ecpm',",
                    "\tSUM(b.data_adsense_country_revenue) AS 'revenue'",
                    "FROM app_credentials a",
                    "INNER JOIN data_adsense_country b ON a.account_id = b.account_id",
                    "WHERE",
                    "\tb.data_adsense_country_tanggal BETWEEN %s AND %s",
                ]
                if data_account_list:
                    base_sql.append(f"\tAND ({like_conditions_account})")
                    params.extend(like_params_account)
                if data_domain_list:
                    base_sql.append(f"\tAND ({like_conditions_domain})")
                    params.extend(like_params_domain)
                base_sql.append("GROUP BY b.data_adsense_country_tanggal, SUBSTRING_INDEX(b.data_adsense_country_domain, '.', 2), b.data_adsense_country_cd")
                base_sql.append("ORDER BY b.data_adsense_country_tanggal ASC")

            sql = "\n".join(base_sql)
            if not self.execute_query(sql, tuple(params)):
                raise pymysql.Error("Failed to get all adsense traffic account by params")
            data = self.fetch_all()
            if not self.commit():
                raise pymysql.Error("Failed to commit get all adsense traffic account by params")
            hasil = {
                "status": True,
                "message": "Data adsense traffic account berhasil diambil",
                "data": data
            }
        except pymysql.Error as e:
            hasil = {
                "status": False,
                'data': 'Terjadi error {!r}, error nya {}'.format(e, e.args[0])
            }
        return {'hasil': hasil}

    def get_all_adx_monitoring_account_by_params(self, start_date, end_date, selected_account_list = None, selected_domain_list = None):
        try:
            # --- 0. Pastikan selected_account_list adalah list string
            if isinstance(selected_account_list, str):
                selected_account_list = [selected_account_list.strip()]
            elif selected_account_list is None:
                selected_account_list = []
            elif isinstance(selected_account_list, (set, tuple)):
                selected_account_list = list(selected_account_list)
            data_account_list = [str(a).strip() for a in selected_account_list if str(a).strip()]

            engine = (self._report_engine() or '').strip().lower()
            use_clickhouse = engine in ('clickhouse', 'ch')

            account_col = "toString(b.account_id)" if use_clickhouse else "b.account_id"
            like_conditions_account = " OR ".join([f"{account_col} LIKE %s"] * len(data_account_list))
            like_params_account = [f"{account}%" for account in data_account_list]

            # --- 1. Pastikan selected_domain_list adalah list string
            if isinstance(selected_domain_list, str):
                selected_domain_list = [selected_domain_list.strip()]
            elif selected_domain_list is None:
                selected_domain_list = []
            elif isinstance(selected_domain_list, (set, tuple)):
                selected_domain_list = list(selected_domain_list)
            data_domain_list = [str(d).strip() for d in selected_domain_list if str(d).strip()]

            # --- 2. Buat kondisi LIKE untuk setiap domain
            like_conditions_domain = " OR ".join(["b.data_adx_country_domain LIKE %s"] * len(data_domain_list))
            like_params_domain = [f"%{domain}%" for domain in data_domain_list]

            if use_clickhouse:
                base_sql = [
                    "SELECT",
                    "\ttoDate(b.data_adx_country_tanggal) AS 'date',",
                    "\tb.data_adx_country_domain AS 'site_name',",
                    "\tb.data_adx_country_cd AS 'country_code',",
                    "\tSUM(b.data_adx_country_impresi) AS impressions,",
                    "\tSUM(b.data_adx_country_click) AS clicks,",
                    "\tif(SUM(b.data_adx_country_impresi) > 0, ROUND((SUM(b.data_adx_country_click) / SUM(b.data_adx_country_impresi)) * 100, 4), 0) AS ctr,",
                    "\tif(SUM(b.data_adx_country_impresi) > 0, ROUND((SUM(b.data_adx_country_revenue) / SUM(b.data_adx_country_impresi)) * 1000, 4), 0) AS ecpm,",
                    "\tif(SUM(b.data_adx_country_click) > 0, ROUND(SUM(b.data_adx_country_revenue) / SUM(b.data_adx_country_click), 4), 0) AS cpc,",
                    "\tSUM(b.data_adx_country_total_requests) AS total_requests,",
                    "\tSUM(b.data_adx_country_responses_served) AS responses_served,",
                    "\tif(SUM(b.data_adx_country_total_requests) > 0, ROUND((SUM(b.data_adx_country_responses_served) / SUM(b.data_adx_country_total_requests)) * 100, 4), 0) AS match_rate,",
                    "\tif(SUM(b.data_adx_country_responses_served) > 0, ROUND((SUM(b.data_adx_country_impresi) / SUM(b.data_adx_country_responses_served)) * 100, 4), 0) AS fill_rate,",
                    "\tif(SUM(b.data_adx_country_impresi) > 0, ROUND(SUM(b.data_adx_country_active_view_pct_viewable * b.data_adx_country_impresi) / SUM(b.data_adx_country_impresi), 4), 0) AS active_view_pct_viewable,",
                    "\tif(SUM(b.data_adx_country_impresi) > 0, ROUND(SUM(b.data_adx_country_active_view_avg_time_sec * b.data_adx_country_impresi) / SUM(b.data_adx_country_impresi), 4), 0) AS active_view_avg_time_sec,",
                    "\tSUM(b.data_adx_country_revenue) AS 'revenue'",
                    "FROM data_adx_country b",
                    "WHERE",
                ]
                params = []
                base_sql.append("toDate(b.data_adx_country_tanggal) BETWEEN toDate(%s) AND toDate(%s)")
                params.extend([start_date, end_date])
            else:
                base_sql = [
                    "SELECT",
                    "\tb.data_adx_country_tanggal AS 'date',",
                    "\tb.data_adx_country_domain AS 'site_name',",
                    "\tb.data_adx_country_cd AS 'country_code',",
                    "\tSUM(b.data_adx_country_impresi) AS impressions,",
                    "\tSUM(b.data_adx_country_click) AS clicks,",
                    "\tCASE WHEN SUM(b.data_adx_country_impresi) > 0",
                    "\t\tTHEN ROUND((SUM(b.data_adx_country_click) / SUM(b.data_adx_country_impresi)) * 100, 4)",
                    "\t\tELSE 0 END AS ctr,",
                    "\tCASE WHEN SUM(b.data_adx_country_impresi) > 0",
                    "\t\tTHEN ROUND((SUM(b.data_adx_country_revenue) / SUM(b.data_adx_country_impresi)) * 1000, 4)",
                    "\t\tELSE 0 END AS ecpm,",
                    "\tCASE WHEN SUM(b.data_adx_country_click) > 0",
                    "\t\tTHEN ROUND(SUM(b.data_adx_country_revenue) / SUM(b.data_adx_country_click), 4)",
                    "\t\tELSE 0 END AS cpc,",
                    "\tSUM(b.data_adx_country_total_requests) AS total_requests,",
                    "\tSUM(b.data_adx_country_responses_served) AS responses_served,",
                    "\tCASE WHEN SUM(b.data_adx_country_total_requests) > 0",
                    "\t\tTHEN ROUND((SUM(b.data_adx_country_responses_served) / SUM(b.data_adx_country_total_requests)) * 100, 4)",
                    "\t\tELSE 0 END AS match_rate,",
                    "\tCASE WHEN SUM(b.data_adx_country_responses_served) > 0",
                    "\t\tTHEN ROUND((SUM(b.data_adx_country_impresi) / SUM(b.data_adx_country_responses_served)) * 100, 4)",
                    "\t\tELSE 0 END AS fill_rate,",
                    "\tCASE WHEN SUM(b.data_adx_country_impresi) > 0",
                    "\t\tTHEN ROUND(SUM(b.data_adx_country_active_view_pct_viewable * b.data_adx_country_impresi) / SUM(b.data_adx_country_impresi), 4)",
                    "\t\tELSE 0 END AS active_view_pct_viewable,",
                    "\tCASE WHEN SUM(b.data_adx_country_impresi) > 0",
                    "\t\tTHEN ROUND(SUM(b.data_adx_country_active_view_avg_time_sec * b.data_adx_country_impresi) / SUM(b.data_adx_country_impresi), 4)",
                    "\t\tELSE 0 END AS active_view_avg_time_sec,",
                    "\tSUM(b.data_adx_country_revenue) AS 'revenue'",
                    "FROM data_adx_country b",
                    "WHERE",
                ]
                params = []
                base_sql.append("b.data_adx_country_tanggal BETWEEN %s AND %s")
                params.extend([start_date, end_date])

            if data_account_list:
                base_sql.append(f"\tAND ({like_conditions_account})")
                params.extend(like_params_account)

            if data_domain_list:
                base_sql.append(f"\tAND ({like_conditions_domain})")
                params.extend(like_params_domain)

            if use_clickhouse:
                base_sql.append("GROUP BY toDate(b.data_adx_country_tanggal), b.data_adx_country_domain, b.data_adx_country_cd")
                base_sql.append("ORDER BY toDate(b.data_adx_country_tanggal) ASC, b.data_adx_country_domain ASC")
            else:
                base_sql.append("GROUP BY b.data_adx_country_tanggal, b.data_adx_country_domain, b.data_adx_country_cd")
                base_sql.append("ORDER BY b.data_adx_country_tanggal ASC, b.data_adx_country_domain ASC")

            sql = "\n".join(base_sql)
            if not self.execute_query(sql, tuple(params)):
                raise pymysql.Error("Failed to get all adx monitoring account by params")
            data = self.fetch_all()
            if not self.commit():
                raise pymysql.Error("Failed to commit get all adx monitoring account by params")
            hasil = {
                "status": True,
                "message": "Data adx monitoring account berhasil diambil",
                "data": data
            }
        except pymysql.Error as e:
            hasil = {
                "status": False,
                'data': 'Terjadi error {!r}, error nya {}'.format(e, e.args[0])
            }
        return {'hasil': hasil}

    def get_all_adsense_monitoring_account_by_params(self, start_date, end_date, selected_account_list = None, selected_domain_list = None):
        try:
            # --- 0. Pastikan selected_account_list adalah list string
            if isinstance(selected_account_list, str):
                selected_account_list = [selected_account_list.strip()]
            elif selected_account_list is None:
                selected_account_list = []
            elif isinstance(selected_account_list, (set, tuple)):
                selected_account_list = list(selected_account_list)
            data_account_list = [str(a).strip() for a in selected_account_list if str(a).strip()]

            engine = (self._report_engine() or '').strip().lower()
            use_clickhouse = engine in ('clickhouse', 'ch')

            account_col = "toString(b.account_id)" if use_clickhouse else "b.account_id"
            like_conditions_account = " OR ".join([f"{account_col} LIKE %s"] * len(data_account_list))
            like_params_account = [f"{account}%" for account in data_account_list]

            # --- 1. Pastikan selected_domain_list adalah list string
            if isinstance(selected_domain_list, str):
                selected_domain_list = [selected_domain_list.strip()]
            elif selected_domain_list is None:
                selected_domain_list = []
            elif isinstance(selected_domain_list, (set, tuple)):
                selected_domain_list = list(selected_domain_list)
            data_domain_list = [str(d).strip() for d in selected_domain_list if str(d).strip()]
            expanded_domain_tokens = []
            for _d in data_domain_list:
                s = str(_d or '').strip().lower().strip('.')
                if not s:
                    continue
                parts = [p for p in s.split('.') if p]
                cands = [s]
                if len(parts) >= 2:
                    cands.append('.'.join(parts[:2]))
                    cands.append('.'.join(parts[-2:]))
                for c in cands:
                    if c and c not in expanded_domain_tokens:
                        expanded_domain_tokens.append(c)

            site_expr = "concat(arrayElement(splitByChar('.', b.data_adsense_country_domain), 1), '.', arrayElement(splitByChar('.', b.data_adsense_country_domain), 2))" if use_clickhouse else "SUBSTRING_INDEX(b.data_adsense_country_domain, '.', 2)"
            date_expr = "toDate(b.data_adsense_country_tanggal)" if use_clickhouse else "DATE(b.data_adsense_country_tanggal)"

            like_conditions_domain = " OR ".join([f"(b.data_adsense_country_domain LIKE %s OR {site_expr} LIKE %s)"] * len(expanded_domain_tokens))
            like_params_domain = []
            for domain in expanded_domain_tokens:
                d = str(domain or '').strip()
                like_params_domain.extend([f"%{d}%", f"%{d}%"])

            base_sql = [
                "SELECT",
                f"\t{date_expr} AS 'date',",
                f"\t{site_expr} AS 'site_name',",
                "\tb.data_adsense_country_cd AS 'country_code',",
                "\tSUM(b.data_adsense_country_impresi) AS 'impressions',",
                "\tSUM(b.data_adsense_country_click) AS 'clicks',",
                "\tSUM(b.data_adsense_country_page_views) AS 'page_views',",
                "\tSUM(b.data_adsense_country_ad_requests) AS 'ad_requests',",
                "\tSUM(COALESCE(b.data_adsense_country_ad_requests_coverage, 0) * COALESCE(b.data_adsense_country_ad_requests, 0)) AS 'ad_requests_coverage_weighted_sum',",
                "\tSUM(COALESCE(b.data_adsense_country_active_view_viewability, 0) * COALESCE(b.data_adsense_country_impresi, 0)) AS 'active_view_viewability_weighted_sum',",
                "\tSUM(COALESCE(b.data_adsense_country_active_view_measurability, 0) * COALESCE(b.data_adsense_country_impresi, 0)) AS 'active_view_measurability_weighted_sum',",
                "\tSUM(COALESCE(b.data_adsense_country_active_view_time, 0) * COALESCE(b.data_adsense_country_impresi, 0)) AS 'active_view_time_weighted_sum',",
                "\tSUM(b.data_adsense_country_revenue) AS 'revenue'",
                "FROM data_adsense_country b",
                "WHERE",
            ]
            params = []
            if use_clickhouse:
                base_sql.append("toDate(b.data_adsense_country_tanggal) BETWEEN toDate(%s) AND toDate(%s)")
            else:
                base_sql.append("b.data_adsense_country_tanggal BETWEEN %s AND %s")
            params.extend([start_date, end_date])

            if data_account_list:
                base_sql.append(f"\tAND ({like_conditions_account})")
                params.extend(like_params_account)

            if data_domain_list:
                base_sql.append(f"\tAND ({like_conditions_domain})")
                params.extend(like_params_domain)

            base_sql.append(f"GROUP BY {date_expr}, {site_expr}, b.data_adsense_country_cd")
            base_sql.append(f"ORDER BY {date_expr} ASC, {site_expr} ASC")

            sql = "\n".join(base_sql)
            if not self.execute_query(sql, tuple(params)):
                raise pymysql.Error("Failed to get all adsense monitoring account by params")
            data = self.fetch_all()
            if not self.commit():
                raise pymysql.Error("Failed to commit get all adsense monitoring account by params")
            hasil = {
                "status": True,
                "message": "Data adsense monitoring account berhasil diambil",
                "data": data
            }
        except pymysql.Error as e:
            hasil = {
                "status": False,
                'data': 'Terjadi error {!r}, error nya {}'.format(e, e.args[0])
            }
        return {'hasil': hasil}


    def get_all_adx_traffic_country_by_params(self, start_date, end_date, selected_account_list = None, selected_domain_list = None, countries_list = None, force_clickhouse: bool = False):
        try:
            # --- 0. Pastikan selected_account_list adalah list string
            if isinstance(selected_account_list, str):
                selected_account_list = [selected_account_list.strip()]
            elif selected_account_list is None:
                selected_account_list = []
            elif isinstance(selected_account_list, (set, tuple)):
                selected_account_list = list(selected_account_list)
            data_account_list = [str(a).strip() for a in selected_account_list if str(a).strip()]

            engine = (self._report_engine() or '').strip().lower()
            use_clickhouse = bool(force_clickhouse) or (engine in ('clickhouse', 'ch'))

            account_tokens = []
            for a in (data_account_list or []):
                v = str(a or '').strip()
                if not v:
                    continue
                account_tokens.append(v)
                if v.lower().startswith('act_'):
                    account_tokens.append(v[4:])
                else:
                    account_tokens.append(f"act_{v}")
            account_tokens = list(dict.fromkeys([x for x in account_tokens if x]))

            like_account_col = "toString(b.account_id)" if use_clickhouse else "b.account_id"
            like_conditions_account = " OR ".join([f"{like_account_col} LIKE %s"] * len(account_tokens))
            like_params_account = [f"%{account}%" for account in account_tokens] 
            # --- 1. Pastikan selected_domain_list adalah list string
            if isinstance(selected_domain_list, str):
                selected_domain_list = [selected_domain_list.strip()]
            elif selected_domain_list is None:
                selected_domain_list = []
            elif isinstance(selected_domain_list, (set, tuple)):
                selected_domain_list = list(selected_domain_list)
            data_domain_list = [str(d).strip() for d in selected_domain_list if str(d).strip()]
            # --- 2. Buat kondisi LIKE untuk setiap domain
            like_conditions_domain = " OR ".join(["b.data_adx_country_domain LIKE %s"] * len(data_domain_list))
            like_params_domain = [f"%{domain}%" for domain in data_domain_list] 
            base_sql = [
                "SELECT",
                "\tb.data_adx_country_nm AS country_name,",
                "\tb.data_adx_country_cd AS country_code,",
                "\tSUM(b.data_adx_country_impresi) AS impressions,",
                "\tSUM(b.data_adx_country_click) AS clicks,",
                "\tCASE WHEN SUM(b.data_adx_country_impresi) > 0",
                "\t\tTHEN ROUND((SUM(b.data_adx_country_click) / SUM(b.data_adx_country_impresi)) * 100, 2)",
                "\t\tELSE 0 END AS ctr,",
                "\tCASE WHEN SUM(b.data_adx_country_impresi) > 0",
                "\t\tTHEN ROUND((SUM(b.data_adx_country_revenue) / SUM(b.data_adx_country_impresi)) * 1000, 2)",
                "\t\tELSE 0 END AS ecpm,",
                "\tCASE WHEN SUM(b.data_adx_country_click) > 0",
                "\t\tTHEN ROUND(SUM(b.data_adx_country_revenue) / SUM(b.data_adx_country_click), 2)",
                "\t\tELSE 0 END AS cpc,",
                "\tSUM(b.data_adx_country_total_requests) AS total_requests,",
                "\tSUM(b.data_adx_country_responses_served) AS responses_served,",
                "\tCASE WHEN SUM(b.data_adx_country_total_requests) > 0",
                "\t\tTHEN ROUND((SUM(b.data_adx_country_responses_served) / SUM(b.data_adx_country_total_requests)) * 100, 2)",
                "\t\tELSE 0 END AS match_rate,",
                "\tCASE WHEN SUM(b.data_adx_country_responses_served) > 0",
                "\t\tTHEN ROUND((SUM(b.data_adx_country_impresi) / SUM(b.data_adx_country_responses_served)) * 100, 2)",
                "\t\tELSE 0 END AS fill_rate,",
                "\tCASE WHEN SUM(b.data_adx_country_impresi) > 0",
                "\t\tTHEN ROUND(SUM(b.data_adx_country_active_view_pct_viewable * b.data_adx_country_impresi) / SUM(b.data_adx_country_impresi), 2)",
                "\t\tELSE 0 END AS active_view_pct_viewable,",
                "\tCASE WHEN SUM(b.data_adx_country_impresi) > 0",
                "\t\tTHEN ROUND(SUM(b.data_adx_country_active_view_avg_time_sec * b.data_adx_country_impresi) / SUM(b.data_adx_country_impresi), 2)",
                "\t\tELSE 0 END AS active_view_avg_time_sec,",
                "\tSUM(b.data_adx_country_revenue) AS revenue",
                "FROM data_adx_country b",
                "WHERE",
            ]
            params = []
            # Date range
            base_sql.append("b.data_adx_country_tanggal BETWEEN %s AND %s")
            params.extend([start_date, end_date])
            # Normalize selected_account_list and apply account filter
            if data_account_list:
                base_sql.append(f"\tAND ({like_conditions_account})")
                params.extend(like_params_account)
            # Normalize selected_sites (CSV string or list) and apply domain filter
            if data_domain_list:
                base_sql.append(f"\tAND ({like_conditions_domain})")
                params.extend(like_params_domain)
            # Normalize countries_list and apply country code filter
            country_codes = []
            if countries_list:
                if isinstance(countries_list, str):
                    country_codes = [c.strip() for c in countries_list.split(',') if c.strip()]
                elif isinstance(countries_list, (list, tuple)):
                    country_codes = [str(c).strip() for c in countries_list if str(c).strip()]
            if country_codes:
                placeholders = ','.join(['%s'] * len(country_codes))
                base_sql.append(f"AND b.data_adx_country_cd IN ({placeholders})")
                params.extend(country_codes)
            base_sql.append("GROUP BY b.data_adx_country_cd, b.data_adx_country_nm")
            base_sql.append("ORDER BY revenue DESC")
            sql = "\n".join(base_sql)
            data_rows = []
            if use_clickhouse:
                try:
                    self._ensure_report_connection()
                    self.cur_hris = self.report_cur
                    self.cur_hris.execute(sql, tuple(params))
                    data_rows = self.fetch_all()
                except Exception as ch_err:
                    # Fallback ke MySQL jika ClickHouse error (mis. HTTP 500)
                    if not self.ensure_connection():
                        raise pymysql.Error(f"Failed to get all adx traffic country by params: {ch_err}")
                    self.cur_hris = self.mysql_cur
                    self.cur_hris.execute(sql, tuple(params))
                    data_rows = self.fetch_all()
                    if not self.commit():
                        raise pymysql.Error("Failed to commit get all adx traffic country by params")
            else:
                if not self.execute_query(sql, tuple(params)):
                    raise pymysql.Error(f"Failed to get all adx traffic country by params: {self.last_error}")
                data_rows = self.fetch_all()
                if not self.commit():
                    raise pymysql.Error("Failed to commit get all adx traffic country by params")
            # Build summary
            total_impressions = sum((row.get('impressions') or 0) for row in data_rows) if data_rows else 0
            total_clicks = sum((row.get('clicks') or 0) for row in data_rows) if data_rows else 0
            total_revenue = sum((row.get('revenue') or 0) for row in data_rows) if data_rows else 0.0
            total_requests = sum((row.get('total_requests') or 0) for row in data_rows) if data_rows else 0
            responses_served = sum((row.get('responses_served') or 0) for row in data_rows) if data_rows else 0

            total_ctr_ratio = (float(total_clicks) / float(total_impressions)) if total_impressions else 0.0
            match_rate = (float(responses_served) / float(total_requests) * 100.0) if total_requests else 0.0
            fill_rate = (float(total_impressions) / float(responses_served) * 100.0) if responses_served else 0.0

            active_view_pct_viewable = 0.0
            active_view_avg_time_sec = 0.0
            if total_impressions:
                active_view_pct_viewable = sum((float(row.get('active_view_pct_viewable') or 0.0) * float(row.get('impressions') or 0)) for row in data_rows) / float(total_impressions)
                active_view_avg_time_sec = sum((float(row.get('active_view_avg_time_sec') or 0.0) * float(row.get('impressions') or 0)) for row in data_rows) / float(total_impressions)

            return {
                "status": True,
                "message": "Data adx traffic country berhasil diambil",
                "data": data_rows,
                "summary": {
                    "total_impressions": total_impressions,
                    "total_clicks": total_clicks,
                    "total_revenue": total_revenue,
                    "total_ctr": total_ctr_ratio,
                    "total_requests": total_requests,
                    "responses_served": responses_served,
                    "match_rate": round(match_rate, 2),
                    "fill_rate": round(fill_rate, 2),
                    "active_view_pct_viewable": round(active_view_pct_viewable, 2),
                    "active_view_avg_time_sec": round(active_view_avg_time_sec, 2)
                }
            }
        except pymysql.Error as e:
            return {
                "status": False,
                "error": f"Terjadi error {e!r}, error nya {e.args[0]}"
            }

    def get_all_adx_monitoring_country_by_params(self, start_date, end_date, selected_account = None, selected_domain_list = None, countries_list = None):
        try:
            # --- 1. Pastikan selected_domain_list adalah list string
            if isinstance(selected_domain_list, str):
                selected_domain_list = [selected_domain_list.strip()]
            elif selected_domain_list is None:
                selected_domain_list = []
            elif isinstance(selected_domain_list, (set, tuple)):
                selected_domain_list = list(selected_domain_list)
            data_domain_list = [str(d).strip() for d in selected_domain_list if str(d).strip()]
            # --- 2. Buat kondisi LIKE untuk setiap domain
            like_conditions = " OR ".join(["b.data_adx_country_domain LIKE %s"] * len(data_domain_list))
            like_params = [f"%{domain}%" for domain in data_domain_list] 
            base_sql = [
                "SELECT",
                "\ta.account_id, a.account_name, a.user_mail,",
                "\tb.data_adx_country_domain AS 'site_name',",
                "\tb.data_adx_country_nm AS country_name,",
                "\tb.data_adx_country_cd AS country_code,",
                "\tSUM(b.data_adx_country_revenue) AS revenue",
                "FROM app_credentials a",
                "INNER JOIN data_adx_country b ON a.account_id = b.account_id",
                "WHERE",
            ]
            params = []
            # Date range
            base_sql.append("b.data_adx_country_tanggal BETWEEN %s AND %s")
            params.extend([start_date, end_date])
            # Normalize selected_account and apply account filter
            if selected_account:
                base_sql.append(f"\tAND a.account_id LIKE %s")
                params.append(f"{selected_account}%")
            # Normalize selected_sites (CSV string or list) and apply domain filter
            if data_domain_list:
                base_sql.append(f"\tAND ({like_conditions})")
                params.extend(like_params)
            # Normalize countries_list and apply country code filter
            country_codes = []
            if countries_list:
                if isinstance(countries_list, str):
                    country_codes = [c.strip() for c in countries_list.split(',') if c.strip()]
                elif isinstance(countries_list, (list, tuple)):
                    country_codes = [str(c).strip() for c in countries_list if str(c).strip()]
            if country_codes:
                placeholders = ','.join(['%s'] * len(country_codes))
                base_sql.append(f"AND b.data_adx_country_cd IN ({placeholders})")
                params.extend(country_codes)
            base_sql.append("GROUP BY b.data_adx_country_domain, b.data_adx_country_cd, b.data_adx_country_nm")
            base_sql.append("ORDER BY revenue DESC")
            sql = "\n".join(base_sql)
            if not self.execute_query(sql, tuple(params)):
                raise pymysql.Error("Failed to get all adx traffic country by params")
            data_rows = self.fetch_all()
            if not self.commit():
                raise pymysql.Error("Failed to commit get all adx traffic country by params")
            # Build summary
            total_revenue = sum((row.get('revenue') or 0) for row in data_rows) if data_rows else 0.0
            return {
                "status": True,
                "message": "Data adx monitoring country berhasil diambil",
                "data": data_rows,
                "summary": {
                    "total_revenue": total_revenue
                }
            }
        except pymysql.Error as e:
            return {
                "status": False,
                "error": f"Terjadi error {e!r}, error nya {e.args[0]}"
            }

    def fetch_ads_campaign_list(self, ads_id):
        try:
            base_sql = [
                "SELECT",
                "\tDISTINCT data_ads_domain AS 'site_name'",
                "FROM",
                "\tdata_ads_campaign",
                "WHERE account_ads_id = %s",
            ]
            params = [ads_id]
            if not self.execute_query("\n".join(base_sql), tuple(params)):
                raise pymysql.Error("Failed to get ads sites list by params")
            data_rows = self.fetch_all()
            if not self.commit():
                raise pymysql.Error("Failed to commit get ads sites list by params")
            hasil = {
                "status": True,
                "message": "Data ads traffic campaign berhasil diambil",
                "data": [row['site_name'] for row in data_rows]
            }
        except pymysql.Error as e:
            hasil = {
                "status": False,
                "error": f"Terjadi error {e!r}, error nya {e.args[0]}"
            }
        return {'hasil': hasil}

    def fetch_ads_sites_list(self, ads_id):
        try:
            if isinstance(ads_id, str):
                ads_id = [s.strip() for s in ads_id.split(",") if s.strip()]
            elif ads_id is None:
                ads_id = []
            elif isinstance(ads_id, (set, tuple)):
                ads_id = list(ads_id)
            if not ads_id:
                raise ValueError("ads_id is required and cannot be empty")
            like_conditions = " OR ".join(["a.account_ads_id LIKE %s"] * len(ads_id))
            like_params = [f"%{d}%" for d in ads_id]
            base_sql = [
                "SELECT",
                "\trs.account_id,",
                "\tCASE",
                "\t\tWHEN rs.data_ads_domain = ''",
                "\t\tTHEN 'Draft'",
                "\t\tELSE rs.data_ads_domain",
                "\tEND AS site_name",
                "FROM (",
                "\tSELECT",
                "\t\ta.account_ads_id AS 'account_id',",
                "\t\tREGEXP_SUBSTR(LOWER(a.data_ads_domain), '^[a-z0-9]+\\.[a-z0-9]+') AS data_ads_domain",
                "\tFROM data_ads_campaign a",
                "\tLEFT JOIN master_account_ads b ON a.account_ads_id = b.account_id",
                "\tLEFT JOIN app_users c ON b.account_owner = c.user_id",
                f"WHERE {like_conditions}",
                "\tGROUP BY a.data_ads_domain",
                ") rs",
                "GROUP BY site_name"
            ]
            params = like_params
            if not self.execute_query("\n".join(base_sql), tuple(params)):
                raise pymysql.Error("Failed to get ads sites list by params")
            data_rows = self.fetch_all()
            if not self.commit():
                raise pymysql.Error("Failed to commit get ads sites list by params")
            hasil = {
                "status": True,
                "message": "Data ads traffic campaign berhasil diambil",
                "data": [row['site_name'] for row in data_rows]
            }
        except pymysql.Error as e:
            hasil = {
                "status": False,
                "error": f"Terjadi error {e!r}, error nya {e.args[0]}"
            }
        return {'hasil': hasil}

    def fetch_ads_account_list(self, selected_domain_list):
        try:
            if isinstance(selected_domain_list, str):
                selected_domain_list = [s.strip() for s in selected_domain_list.split(",") if s.strip()]
            elif selected_domain_list is None:
                selected_domain_list = []
            elif isinstance(selected_domain_list, (set, tuple)):
                selected_domain_list = list(selected_domain_list)
            if not selected_domain_list:
                raise ValueError("selected_domain_list is required and cannot be empty")
            like_conditions = " OR ".join(["b.data_ads_domain LIKE %s"] * len(selected_domain_list))
            like_params = [f"%{d}%" for d in selected_domain_list]
            base_sql = [
                "SELECT",
                "\trs.account_id,",
                "\trs.account_name,",
                "\tCASE",
                "\t\tWHEN rs.site_name = ''",
                "\t\tTHEN 'Draft'",
                "\t\tELSE rs.site_name",
                "\tEND AS site_name",
                "FROM (",
                "\tSELECT DISTINCT",
                "\t\ta.account_id AS 'account_id',",
                "\t\ta.account_name AS 'account_name',",
                "\t\tREGEXP_SUBSTR(LOWER(b.data_ads_domain), '^[a-z0-9]+\\.[a-z0-9]+') AS site_name",
                "\tFROM master_account_ads a",
                "\tINNER JOIN data_ads_campaign b ON a.account_id = b.account_ads_id",
                f"WHERE {like_conditions}",
                "\tGROUP BY account_id, site_name",
                ") rs",
                "GROUP BY rs.account_id, rs.site_name"
            ]
            params = like_params
            if not self.execute_query("\n".join(base_sql), tuple(params)):
                raise pymysql.Error("Failed to get ads sites list by params")
            data_rows = self.fetch_all()
            if not self.commit():
                raise pymysql.Error("Failed to commit get ads account list by params")
            hasil = {
                "status": True,
                "message": "Data ads account campaign berhasil diambil",
                "data": [row for row in data_rows]
            }
        except pymysql.Error as e:
            hasil = {
                "status": False,
                "error": f"Terjadi error {e!r}, error nya {e.args[0]}"
            }
        return {'hasil': hasil}

    def fetch_country_ads_list(self, tanggal_dari, tanggal_sampai, selected_account, selected_domain_list):
        try:
            # --- 1. Normalisasi input ---
            if isinstance(selected_domain_list, str):
                selected_domain_list = [selected_domain_list.strip()]
            elif selected_domain_list is None:
                selected_domain_list = []
            elif isinstance(selected_domain_list, (set, tuple)):
                selected_domain_list = list(selected_domain_list)
            elif not isinstance(selected_domain_list, list):
                selected_domain_list = []

            # --- 2. Sanitasi ---
            data_domain_list = [
                str(d).strip()
                for d in selected_domain_list
                if d is not None and str(d).strip()
            ]

            # --- 3. Buat LIKE clause hanya jika ada domain ---
            if data_domain_list:
                like_conditions = " OR ".join(["data_ads_domain LIKE %s"] * len(data_domain_list))
                like_clause = f"\tAND ({like_conditions})"
                like_params = [f"%{domain}%" for domain in data_domain_list]
            else:
                like_clause = ""      # tidak menambah filter domain
                like_params = []      # tidak menambah parameter

            base_sql = [
                "SELECT",
                "\tdata_ads_country_cd AS 'country_code',",
                "\tdata_ads_country_nm AS 'country_name'",
                "FROM",
                "\tdata_ads_country",
                "WHERE data_ads_country_tanggal BETWEEN %s AND %s",
                "\tAND account_ads_id LIKE %s",
                f"{like_clause}",
                "GROUP BY data_ads_country_cd, data_ads_country_nm",
                "ORDER BY data_ads_country_cd ASC",
            ]

            params = [tanggal_dari, tanggal_sampai, f"{selected_account}%"] + like_params
            sql = "\n".join(base_sql)

            if not self.execute_query(sql, tuple(params)):
                raise pymysql.Error("Failed to get all country list by params")

            data = self.fetch_all()
            if not self.commit():
                raise pymysql.Error("Failed to commit get all country list by params")

            hasil = {
                "status": True,
                "message": "Data country list berhasil diambil",
                "data": data
            }

        except pymysql.Error as e:
            hasil = {
                "status": False,
                "data": f"Terjadi error {e!r}, error nya {e.args[0]}"
            }
        except Exception as e:
            hasil = {
                "status": "error",
                "message": "Gagal mengambil data negara.",
                "error": str(e)
            }

        return {"hasil": hasil}

    def get_all_adx_roi_country_hourly_by_params(self, start_date, end_date, selected_domain_list = None):
        try:
            # --- 1. Pastikan selected_domain_list adalah list string
            if isinstance(selected_domain_list, str):
                selected_domain_list = [selected_domain_list.strip()]
            elif selected_domain_list is None:
                selected_domain_list = []
            elif isinstance(selected_domain_list, (set, tuple)):
                selected_domain_list = list(selected_domain_list)
            data_domain_list = [str(d).strip() for d in selected_domain_list if str(d).strip()]
            # --- 2. Buat kondisi LIKE untuk setiap domain
            like_conditions_domain = " OR ".join(["a.log_adx_country_domain LIKE %s"] * len(data_domain_list))
            like_params_domain = [f"%{domain}%" for domain in data_domain_list] 
            base_sql = [
                "SELECT",
                "    DATE(a.log_adx_country_tanggal) AS date,",
                "    HOUR(a.mdd) AS hour,",
                "    a.mdd AS time,",
                "    a.log_adx_country_cd AS country_code,",
                "    a.log_adx_country_nm AS country_name,",
                "    a.log_adx_country_domain,",
                "    a.log_adx_country_impresi AS impressions,",
                "    a.log_adx_country_click AS clicks,",
                "    a.log_adx_country_revenue AS revenue",
                "FROM log_adx_country a",
                "JOIN (",
                "    SELECT",
                "        log_adx_country_cd,",
                "        log_adx_country_domain,",
                "        HOUR(mdd) AS jam,",
                "        MAX(mdd) AS max_time",
                "    FROM log_adx_country",
                "    WHERE log_adx_country_tanggal BETWEEN %s AND %s",
                "    GROUP BY HOUR(mdd), log_adx_country_cd, log_adx_country_domain",
                ") b ON a.log_adx_country_cd = b.log_adx_country_cd",
                "   AND a.log_adx_country_domain = b.log_adx_country_domain",
                "   AND a.mdd = b.max_time",
                "WHERE a.log_adx_country_tanggal BETWEEN %s AND %s",
                "AND a.log_adx_country_domain NOT IN ('(Not applicable)')",
            ]
            params = [start_date, end_date, start_date, end_date]
            if data_domain_list:
                base_sql.append(f"\tAND ({like_conditions_domain})")
                params.extend(like_params_domain)
            base_sql.append("ORDER BY hour ASC, a.log_adx_country_nm ASC")
            sql = "\n".join(base_sql)
            if not self.execute_query(sql, tuple(params)):
                raise pymysql.Error("Failed to get hourly AdX country logs by params")
            data_rows = self.fetch_all()
            return {
                "status": True,
                "message": "Hourly AdX country logs berhasil diambil",
                "data": data_rows
            }
        except pymysql.Error as e:
            return {"status": False, "error": f"Terjadi error {e!r}, error nya {e.args[0]}"}
        except Exception as e:
            return {"status": False, "error": str(e)}

    def get_all_adx_country_hourly_by_params(self, tanggal):
        try:
            sql = "\n".join([
                "SELECT",
                "    toHour(mdd) AS hour,",
                "    log_adx_country_cd AS country_code,",
                "    log_adx_country_domain,",
                "    argMax(log_adx_country_impresi, mdd) AS impressions,",
                "    argMax(log_adx_country_click, mdd) AS clicks,",
                "    argMax(log_adx_country_revenue, mdd) AS revenue",
                "FROM log_adx_country",
                "WHERE toDate(log_adx_country_tanggal) = toDate(%s)",
                "AND log_adx_country_domain NOT IN ('(Not applicable)')",
                "GROUP BY hour, country_code, log_adx_country_domain",
                "ORDER BY hour ASC",
            ])
            params = [tanggal]

            self._ensure_report_connection()
            self.cur_hris = self.report_cur
            self.cur_hris.execute(sql, tuple(params))
            data_rows = self.fetch_all() or []
            
            return {
                "status": True,
                "message": "Hourly AdX country logs berhasil diambil",
                "data": data_rows
            }
        except Exception as e:
            return {"status": False, "error": f"Terjadi error {e!r}"}

    def get_all_adsense_country_hourly_by_params(self, tanggal):
        """Selalu ambil data hourly AdSense dari ClickHouse (tanpa fallback MySQL)."""
        try:
            sql = "\n".join([
                    "WITH",
                    "    lower(log_adsense_country_domain) AS d,",
                    "    cutToFirstSignificantSubdomain(d) AS root,",
                    "    arrayElement(splitByChar('.', d), 1) AS first_label",
                    "SELECT",
                    "    toHour(mdd) AS hour,",
                    "    log_adsense_country_cd AS country_code,",
                    "    log_adsense_country_domain,",
                    "    argMax(log_adsense_country_impresi, mdd) AS impressions,",
                    "    argMax(log_adsense_country_click, mdd) AS clicks,",
                    "    argMax(log_adsense_country_revenue, mdd) AS revenue",
                    "FROM log_adsense_country",
                    "WHERE toDate(log_adsense_country_tanggal) = toDate(%s)",
                    "  AND (",
                    "        (d != root AND first_label != 'www')",
                    "        OR",
                    "        (d = root AND position(root, '-') > 0)",
                    "      )",
                    "GROUP BY hour, country_code, log_adsense_country_domain",
                    "ORDER BY hour ASC",
                ])
            params = [tanggal]
            self._ensure_report_connection()
            self.cur_hris = self.report_cur
            self.cur_hris.execute(sql, tuple(params))
            data_rows = self.fetch_all() or []

            return {
                "status": True,
                "message": "Hourly AdSense country logs berhasil diambil",
                "data": data_rows,
            }
        except Exception as e:
            return {"status": False, "error": f"Terjadi error {e!r}"}

    def get_all_adsense_country_hourly_range_by_params(self, start_date, end_date, selected_domain_list=None):
        try:
            if isinstance(selected_domain_list, str):
                data_domain_list = [s.strip() for s in selected_domain_list.split(',') if s.strip()]
            elif isinstance(selected_domain_list, (list, set, tuple)):
                data_domain_list = [str(s).strip() for s in selected_domain_list if str(s).strip()]
            else:
                data_domain_list = []

            like_clause = ''
            like_params = []
            if data_domain_list:
                like_conditions = " OR ".join(["log_adsense_country_domain LIKE %s"] * len(data_domain_list))
                like_clause = f"AND ({like_conditions})"
                like_params = [f"%{d}%" for d in data_domain_list]
            sql = "\n".join([
                "SELECT",
                "    toDate(log_adsense_country_tanggal) AS date,",
                "    toHour(mdd) AS hour,",
                "    log_adsense_country_cd AS country_code,",
                "    log_adsense_country_nm AS country_name,",
                "    log_adsense_country_domain,",
                "    argMax(log_adsense_country_impresi, mdd) AS impressions,",
                "    argMax(log_adsense_country_click, mdd) AS clicks,",
                "    argMax(log_adsense_country_revenue, mdd) AS revenue",
                "FROM log_adsense_country",
                "WHERE toDate(log_adsense_country_tanggal) BETWEEN toDate(%s) AND toDate(%s)",
                "AND log_adsense_country_domain NOT IN ('(Not applicable)')",
                like_clause,
                "GROUP BY date, hour, country_code, country_name, log_adsense_country_domain",
                "ORDER BY date ASC, hour ASC",
            ])

            params = [start_date, end_date] + like_params

            self._ensure_report_connection()
            self.cur_hris = self.report_cur
            self.cur_hris.execute(sql, tuple(params))
            data_rows = self.fetch_all() or []

            return {
                "status": True,
                "message": "Hourly AdSense country logs berhasil diambil",
                "data": data_rows,
            }
        except Exception as e:
            return {"status": False, "error": f"Terjadi error {e!r}"}

    def get_all_adx_roi_country_hourly_logs_by_params(self, target_date, selected_domain_list=None):
        try:
            # Normalize domain list (optional)
            if isinstance(selected_domain_list, (list, set, tuple)):
                data_domain_list = [str(d).strip() for d in selected_domain_list if str(d).strip()]
            elif isinstance(selected_domain_list, str):
                data_domain_list = [selected_domain_list.strip()] if selected_domain_list.strip() else []
            else:
                data_domain_list = []
            like_conditions_domain = " OR ".join(["log_adx_country_domain LIKE %s"] * len(data_domain_list))
            like_params_domain = [f"%{domain}%" for domain in data_domain_list]
            base_sql = [
                "SELECT",
                "\tDATE(log_adx_country_tanggal) AS date,",
                "\tHOUR(mdd) AS hour,",
                "\tlog_adx_country_cd AS country_code,",
                "\tlog_adx_country_nm AS country_name,",
                "\tSUM(log_adx_country_impresi) AS impressions,",
                "\tSUM(log_adx_country_click) AS clicks,",
                "\tSUM(log_adx_country_revenue) AS revenue",
                "FROM log_adx_country a",
                "WHERE log_adx_country_tanggal = %s",
            ]
            params = [target_date]
            if data_domain_list:
                base_sql.append(f"\tAND ({like_conditions_domain})")
                params.extend(like_params_domain)
            base_sql.append("GROUP BY date, hour, log_adx_country_cd, log_adx_country_nm")
            base_sql.append("ORDER BY hour ASC, country_name ASC")
            sql = "\n".join(base_sql)
            if not self.execute_query(sql, tuple(params)):
                raise pymysql.Error("Failed to get hourly AdX country logs by params")
            data_rows = self.fetch_all()
            return {
                "status": True,
                "message": "Hourly AdX country logs berhasil diambil",
                "data": data_rows
            }
        except pymysql.Error as e:
            return {"status": False, "error": f"Terjadi error {e!r}, error nya {e.args[0]}"}
        except Exception as e:
            return {"status": False, "error": str(e)}

    def get_all_ads_roi_country_hourly_by_params(self, start_date, end_date, data_sub_domain=None):
        try:
            # --- 1. Pastikan selected_domain_list adalah list string
            if isinstance(data_sub_domain, str):
                data_sub_domain = [s.strip() for s in data_sub_domain.split(",") if s.strip()]
            elif data_sub_domain is None:
                data_sub_domain = []
            elif isinstance(data_sub_domain, (set, tuple)):
                data_sub_domain = list(data_sub_domain)
            if not data_sub_domain:
                raise ValueError("data_sub_domain is required and cannot be empty")
            # like_conditions = " OR ".join(["SUBSTRING_INDEX(a.log_ads_domain, '.', 2) LIKE %s"] * len(data_sub_domain))
            like_conditions = " OR ".join(["a.log_ads_domain LIKE %s"] * len(data_sub_domain))
            like_clause = f"\tAND ({like_conditions})" if like_conditions else ""
            like_params = [f"%{d}%" for d in data_sub_domain]
            # =========================
            # QUERY BARU (pakai subquery rs)
            # =========================
            base_sql = [
                "SELECT",
                "\trs.hour,",
                "\tSUM(rs.spend) AS spend,",
                "\tSUM(rs.impressions) AS impressions,",
                "\tSUM(rs.clicks) AS clicks",
                "FROM (",
                    "\tSELECT",
                    "\t\tHOUR(a.mdd) AS hour,",
                    "\t\tSUM(a.log_ads_country_spend) AS spend,",
                    "\t\tSUM(a.log_ads_country_impresi) AS impressions,",
                    "\t\tSUM(a.log_ads_country_click) AS clicks",
                    "\tFROM log_ads_country a",
                    "\tINNER JOIN (",
                        "\t\tSELECT",
                        "\t\t\tlog_ads_country_cd,",
                        "\t\t\tHOUR(mdd) AS jam,",
                        "\t\t\tMAX(mdd) AS max_time",
                        "\t\tFROM log_ads_country",
                        "\t\tWHERE log_ads_country_tanggal BETWEEN %s AND %s",
                        "\t\tGROUP BY HOUR(mdd), log_ads_domain, log_ads_country_cd",
                    "\t) b",
                    "\tON a.log_ads_country_cd = b.log_ads_country_cd",
                    "\tAND HOUR(a.mdd) = b.jam",
                    "\tAND a.mdd = b.max_time",
                    "\tWHERE a.log_ads_country_tanggal BETWEEN %s AND %s",
                    f"{like_clause}",
                    "\tGROUP BY HOUR(a.mdd)",
                ") rs",
                "GROUP BY rs.hour",
                "ORDER BY rs.hour ASC"
            ]
            params = [start_date, end_date, start_date, end_date] + like_params
            sql = "\n".join(base_sql)
            if not self.execute_query(sql, tuple(params)):
                raise pymysql.Error("Failed to get hourly Ads country logs by params")
            data = self.fetch_all()
            hasil = {
                "status": True,
                "message": "Hourly Ads country logs berhasil diambil",
                "data": data
            }

        except Exception as e:
            hasil = {"status": False, "data": f"Terjadi error {e!r}"}

        return {"hasil": hasil}

    def get_all_ads_country_hourly_by_params(self, tanggal, data_sub_domain=None):
        try:
            # Normalisasi domain
            if isinstance(data_sub_domain, str):
                domains = [d.strip() for d in data_sub_domain.split(",") if d.strip()]
            elif isinstance(data_sub_domain, (list, tuple, set)):
                domains = list(data_sub_domain)
            else:
                domains = []

            domains = [str(d).strip() for d in (domains or []) if str(d).strip()]
            if not domains:
                raise ValueError("data_sub_domain is required")

            # Buat clause startsWith
            starts_clause_ch = " OR ".join(["startsWith(log_ads_domain, %s)"] * len(domains))
            # Query: last record per hour (menit terakhir)
            sql_ch = f"""
            SELECT
                toHour(lacs.mdd) AS hour,
                SUM(lacs.log_ads_country_spend) AS spend,
                SUM(lacs.log_ads_country_impresi) AS impressions,
                SUM(lacs.log_ads_country_click) AS clicks
            FROM hris_trendHorizone.log_ads_country AS lacs
            WHERE toDate(lacs.log_ads_country_tanggal) = toDate(%s)
            AND ({starts_clause_ch})
            GROUP BY hour
            ORDER BY hour;
            """

            # Params: tanggal untuk CTE + tanggal untuk main query + domains
            params_tuple = tuple([tanggal] + domains)

            self._ensure_report_connection()
            self.cur_hris = self.report_cur
            self.cur_hris.execute(sql_ch, params_tuple)
            data = self.fetch_all() or []

            return {
                "hasil": {
                    "status": True,
                    "message": "Hourly Ads country logs berhasil diambil",
                    "data": data,
                }
            }
        except Exception as e:
            return {"hasil": {"status": False, "data": str(e)}}

    def get_all_ads_roi_country_hourly_logs_by_params(self, target_date, data_sub_domain=None):
        try:
            # Normalize domain list
            if isinstance(data_sub_domain, (list, set, tuple)):
                domain_list = [str(s).strip() for s in data_sub_domain if str(s).strip()]
            elif isinstance(data_sub_domain, str):
                domain_list = [s.strip() for s in data_sub_domain.split(",") if s.strip()]
            else:
                domain_list = []
            like_clause = ""
            like_params = []
            if domain_list:
                # log_ads_domain menyimpan 'you.example.xxx' (tanpa TLD asli),
                # maka gunakan pola LIKE berbasis domain tanpa TLD: 'you.example.%'
                patterns = []
                for d in domain_list:
                    parts = d.split('.')
                    # buang segmen TLD terakhir jika ada
                    if len(parts) >= 2:
                        base = ".".join(parts[:-1]) + "."
                    else:
                        base = d + "."
                    patterns.append(base + "%")
                like_conditions = " OR ".join(["log_ads_domain LIKE %s"] * len(patterns))
                like_clause = f"\tAND ({like_conditions})"
                like_params = patterns
            base_sql = [
                "SELECT",
                "\tDATE(log_ads_country_tanggal) AS 'date',",
                "\tHOUR(mdd) AS 'hour',",
                "\tlog_ads_country_cd AS 'country_code',",
                "\tlog_ads_country_nm AS 'country_name',",
                "\tSUM(log_ads_country_spend) AS 'spend',",
                "\tSUM(log_ads_country_impresi) AS 'impressions',",
                "\tSUM(log_ads_country_click) AS 'clicks',",
                "\tROUND(AVG(log_ads_country_cpr), 0) AS 'cpr'",
                "FROM log_ads_country",
                "WHERE log_ads_country_tanggal = %s",
            ]
            params = [target_date] + like_params
            if like_clause:
                base_sql.append(like_clause)
            base_sql.append("GROUP BY date, hour, log_ads_country_cd, log_ads_country_nm")
            base_sql.append("ORDER BY hour ASC, country_name ASC")
            sql = "\n".join(base_sql)
            if not self.execute_query(sql, tuple(params)):
                raise pymysql.Error("Failed to get hourly Ads country logs by params")
            data = self.fetch_all()
            hasil = {"status": True, "message": "Hourly Ads country logs berhasil diambil", "data": data}
        except Exception as e:
            hasil = {"status": False, "data": f"Terjadi error {e!r}"}
        return {"hasil": hasil}


    def get_all_ads_domain_by_active(self, data_account, tanggal_dari, tanggal_sampai, data_sub_domain = None):   
        try:
            base_sql = [
                "SELECT",
                "\ta.account_id, a.account_name, a.account_email,",
                "\tb.data_ads_tanggal AS 'date',",
                "\tb.data_ads_domain AS 'domain',",
                "\tb.data_ads_campaign_nm AS 'campaign',",
                "\tb.data_ads_spend AS 'spend',",
                "\tb.data_ads_impresi AS 'impressions',",
                "\tb.data_ads_click AS 'clicks',",
                "\tb.data_ads_reach AS 'reach',",
                "\tb.data_ads_cpr AS 'cpr',",
                "\tb.data_ads_cpc AS 'cpc'",
                "FROM",
                "\tmaster_account_ads a",
                "INNER JOIN data_ads_campaign b ON a.account_id = b.account_ads_id",
                "WHERE a.account_ads_id = %s",
                "AND b.data_ads_tanggal BETWEEN %s AND %s",
                "AND b.data_ads_domain LIKE %s",
            ]

            # Tangani filter domain: jika None/kosong/list, gunakan wildcard '%'
            if isinstance(data_sub_domain, (list, tuple)):
                search_domain = '%'
            else:
                raw_domain = (data_sub_domain or '').strip()
                search_domain = '%' if raw_domain in ('', '%') else f"%{raw_domain}%"
            params = [data_account, tanggal_dari, tanggal_sampai, search_domain]
            base_sql.append("ORDER BY b.data_ads_domain ASC, b.data_ads_tanggal ASC")
            sql = "\n".join(base_sql)
            if not self.execute_query(sql, tuple(params)):
                raise pymysql.Error("Failed to get all adx traffic account by params")
            data = self.fetch_all()
            if not self.commit():
                raise pymysql.Error("Failed to commit get all adx traffic account by params")
            hasil = {
                "status": True,
                "message": "Data ads traffic campaign berhasil diambil",
                "data": data
            }
        except pymysql.Error as e:
            hasil = {
                "status": False,
                'data': 'Terjadi error {!r}, error nya {}'.format(e, e.args[0])
            }
        return {'hasil': hasil}

    def get_all_ads_traffic_campaign_by_params(self, tanggal_dari, tanggal_sampai, selected_account_list = None, selected_domain_list = None):   
        try:
            if isinstance(selected_account_list, str):
                selected_account_list = [selected_account_list.strip()]
            elif selected_account_list is None:
                selected_account_list = []
            elif isinstance(selected_account_list, (set, tuple)):
                selected_account_list = list(selected_account_list)
            selected_account_list = [
                str(a).strip() for a in selected_account_list
                if str(a).strip() and str(a).strip() != '%'
            ]

            if isinstance(selected_domain_list, str):
                selected_domain_list = [selected_domain_list.strip()]
            elif selected_domain_list is None:
                selected_domain_list = []
            elif isinstance(selected_domain_list, (set, tuple)):
                selected_domain_list = list(selected_domain_list)
            selected_domain_list = [
                str(d).strip() for d in selected_domain_list
                if str(d).strip() and str(d).strip() != '%'
            ]

            engine = (self._report_engine() or '').strip().lower()
            use_clickhouse = engine in ('clickhouse', 'ch')

            params = [tanggal_dari, tanggal_sampai]
            if use_clickhouse:
                account_col = "replaceRegexpAll(lowerUTF8(toString(b.account_ads_id)), '^act_', '')"
                base_sql = [
                    "SELECT",
                    "\ttoString(b.account_ads_id) AS account_id,",
                    "\tifNull(any(a.account_name), '') AS account_name,",
                    "\tifNull(any(a.account_email), '') AS account_email,",
                    "\ttoDate(b.data_ads_tanggal) AS date,",
                    "\tb.data_ads_domain AS domain,",
                    "\tb.data_ads_campaign_nm AS campaign,",
                    "\tSUM(b.data_ads_spend) AS spend,",
                    "\tSUM(b.data_ads_click) AS clicks,",
                    "\tSUM(b.data_ads_impresi) AS impressions,",
                    "\tSUM(b.data_ads_reach) AS reach,",
                    "\tROUND(AVG(b.data_ads_frekuensi), 2) AS frequency,",
                    "\tSUM(b.data_ads_lpv) AS lpv,",
                    "\tROUND(AVG(b.data_ads_lpv_rate), 2) AS lpv_rate,",
                    "\tROUND(AVG(b.data_ads_cpr), 0) AS cpr,",
                    "\tCASE WHEN SUM(b.data_ads_click) > 0 THEN ROUND((SUM(b.data_ads_spend) / SUM(b.data_ads_click)), 0) ELSE 0 END AS cpc",
                    "FROM data_ads_campaign b",
                    "LEFT JOIN master_account_ads a ON (",
                    "\treplaceRegexpAll(lowerUTF8(toString(a.account_id)), '^act_', '') = replaceRegexpAll(lowerUTF8(toString(b.account_ads_id)), '^act_', '')",
                    "\tOR replaceRegexpAll(lowerUTF8(toString(a.account_ads_id)), '^act_', '') = replaceRegexpAll(lowerUTF8(toString(b.account_ads_id)), '^act_', '')",
                    ")",
                    "WHERE",
                    "\ttoDate(b.data_ads_tanggal) BETWEEN toDate(%s) AND toDate(%s)",
                ]
                if selected_account_list:
                    like_conditions_account = " OR ".join([f"{account_col} LIKE %s"] * len(selected_account_list))
                    base_sql.append(f"\tAND ({like_conditions_account})")
                    params.extend([
                        f"%{self._normalize_fb_account_key(account)}%"
                        for account in selected_account_list
                    ])
                if selected_domain_list:
                    like_conditions_domain = " OR ".join(
                        ["positionCaseInsensitive(b.data_ads_domain, %s) > 0"] * len(selected_domain_list)
                    )
                    base_sql.append(f"\tAND ({like_conditions_domain})")
                    params.extend([str(domain) for domain in selected_domain_list])
                base_sql.append("GROUP BY toString(b.account_ads_id), toDate(b.data_ads_tanggal), b.data_ads_domain, b.data_ads_campaign_nm")
                base_sql.append("ORDER BY date, domain, campaign")
            else:
                like_conditions_domain = " OR ".join(["b.data_ads_domain LIKE %s"] * len(selected_domain_list))
                base_sql = [
                    "SELECT",
                    "\trs.account_id, rs.account_name, rs.account_email,",
                    "\trs.date, rs.domain, rs.campaign,",
                    "\tSUM(rs.spend) AS 'spend',",
                    "\tSUM(rs.clicks) AS 'clicks',",
                    "\tSUM(rs.impressions) AS 'impressions',",
                    "\tSUM(rs.reach) AS 'reach',",
                    "\tROUND(AVG(rs.frequency), 2) AS 'frequency',",
                    "\tSUM(rs.lpv) AS 'lpv',",
                    "\tROUND(AVG(rs.lpv_rate), 2) AS 'lpv_rate',",
                    "\tROUND(AVG(rs.cpr), 0) AS 'cpr',",
                    "\tCASE WHEN SUM(rs.clicks) > 0 THEN ROUND((SUM(rs.spend) / SUM(rs.clicks)), 0) ELSE 0 END AS 'cpc'",
                    "FROM (",
                        "\tSELECT",
                        "\t\ta.account_id, a.account_name, a.account_email,",
                        "\t\tb.data_ads_tanggal AS 'date',",
                        "\t\tb.data_ads_domain AS 'domain',",
                        "\t\tb.data_ads_campaign_nm AS 'campaign',",
                        "\t\tb.data_ads_spend AS 'spend',",
                        "\t\tb.data_ads_impresi AS 'impressions',",
                        "\t\tb.data_ads_click AS 'clicks',",
                        "\t\tb.data_ads_reach AS 'reach',",
                        "\t\tb.data_ads_frekuensi AS 'frequency',",
                        "\t\tb.data_ads_lpv AS 'lpv',",
                        "\t\tb.data_ads_lpv_rate AS 'lpv_rate',",
                        "\t\tb.data_ads_cpr AS 'cpr'",
                        "\tFROM master_account_ads a",
                        "\tINNER JOIN data_ads_campaign b ON a.account_id = b.account_ads_id",
                        "\tWHERE",
                        "\tb.data_ads_tanggal BETWEEN %s AND %s",
                ]
                if selected_account_list:
                    account_match_parts = []
                    for account in selected_account_list:
                        norm = self._normalize_fb_account_key(account)
                        account_match_parts.append(
                            "("
                            "REPLACE(LOWER(b.account_ads_id), 'act_', '') LIKE %s "
                            "OR REPLACE(LOWER(a.account_id), 'act_', '') LIKE %s "
                            "OR REPLACE(LOWER(CAST(a.account_ads_id AS CHAR)), 'act_', '') LIKE %s"
                            ")"
                        )
                        params.extend([f"%{norm}%", f"%{norm}%", f"%{norm}%"])
                    base_sql.append(f"\tAND ({' OR '.join(account_match_parts)})")
                if selected_domain_list:
                    base_sql.append(f"\tAND ({like_conditions_domain})")
                    params.extend([f"%{domain}%" for domain in selected_domain_list])
                base_sql.append(") rs")
                base_sql.append("GROUP BY rs.account_id, rs.account_name, rs.account_email, rs.date, rs.domain, rs.campaign")
                base_sql.append("ORDER BY rs.date, rs.domain, rs.campaign")

            sql = "\n".join(base_sql)
            if use_clickhouse:
                self._ensure_report_connection()
                self.cur_hris = self.report_cur
                self.cur_hris.execute(sql, tuple(params))
                data = self.fetch_all()
            else:
                if not self.execute_query(sql, tuple(params)):
                    raise pymysql.Error("Failed to get all adx traffic account by params")
                data = self.fetch_all()
                if not self.commit():
                    raise pymysql.Error("Failed to commit get all adx traffic account by params")

            hasil = {
                "status": True,
                "message": "Data ads traffic campaign berhasil diambil",
                "data": data
            }
        except pymysql.Error as e:
            hasil = {
                "status": False,
                'data': 'Terjadi error {!r}, error nya {}'.format(e, e.args[0])
            }
        return {'hasil': hasil}

    def get_monitoring_campaign_facebook_by_params(self, tanggal_dari, tanggal_sampai, selected_account_list=None, selected_domain_list=None):
        try:
            if isinstance(selected_account_list, str):
                selected_account_list = [selected_account_list.strip()]
            elif selected_account_list is None:
                selected_account_list = []
            elif isinstance(selected_account_list, (set, tuple)):
                selected_account_list = list(selected_account_list)
            selected_account_list = [str(a).strip() for a in selected_account_list if str(a).strip() and str(a).strip() != '%']

            if isinstance(selected_domain_list, str):
                selected_domain_list = [selected_domain_list.strip()]
            elif selected_domain_list is None:
                selected_domain_list = []
            elif isinstance(selected_domain_list, (set, tuple)):
                selected_domain_list = list(selected_domain_list)
            selected_domain_list = [str(d).strip() for d in selected_domain_list if str(d).strip() and str(d).strip() != '%']

            base_sql = [
                "SELECT",
                "\trs.account_name,",
                "\trs.campaign,",
                "\trs.spend,",
                "\trs.daily_budget,",
                "\trs.campaign_status,",
                "\tCASE",
                "\t\tWHEN UPPER(rs.campaign_status) = 'PAUSED' THEN 'Paused'",
                "\t\tWHEN rs.spend > rs.daily_budget THEN 'Overspend'",
                "\t\tELSE 'Normal'",
                "\tEND AS remark",
                "FROM (",
                "\tSELECT",
                "\t\ta.account_name AS account_name,",
                "\t\tb.data_ads_campaign_nm AS campaign,",
                "\t\tSUM(b.data_ads_country_spend) AS spend,",
                "\t\tCOALESCE(MAX(m.master_budget), 0) AS daily_budget,",
                "\t\tCOALESCE(SUBSTRING_INDEX(GROUP_CONCAT(m.master_status ORDER BY m.master_date DESC, m.mdd DESC SEPARATOR ','), ',', 1), 'UNKNOWN') AS campaign_status",
                "\tFROM master_account_ads a",
                "\tINNER JOIN data_ads_country b ON a.account_id = b.account_ads_id",
                "\tLEFT JOIN master_ads m ON m.account_ads_id = b.account_ads_id",
                "\t\tAND m.master_campaign_nm = b.data_ads_campaign_nm",
                "\t\tAND m.master_date = (",
                "\t\t\tSELECT MAX(m2.master_date)",
                "\t\t\tFROM master_ads m2",
                "\t\t\tWHERE m2.account_ads_id = b.account_ads_id",
                "\t\t\tAND m2.master_campaign_nm = b.data_ads_campaign_nm",
                "\t\t)",
                "\tWHERE b.data_ads_country_tanggal BETWEEN %s AND %s",
            ]
            params = [tanggal_dari, tanggal_sampai]

            if selected_account_list:
                acc_like = " OR ".join(["b.account_ads_id LIKE %s"] * len(selected_account_list))
                base_sql.append(f"\tAND ({acc_like})")
                params.extend([f"%{a}%" for a in selected_account_list])

            if selected_domain_list:
                dom_like = " OR ".join(["b.data_ads_domain LIKE %s"] * len(selected_domain_list))
                base_sql.append(f"\tAND ({dom_like})")
                params.extend([f"%{d}%" for d in selected_domain_list])

            base_sql.extend([
                "\tGROUP BY a.account_name, b.data_ads_campaign_nm",
                ") rs",
                "WHERE rs.spend > rs.daily_budget OR UPPER(rs.campaign_status) = 'PAUSED'",
                "ORDER BY rs.spend DESC, rs.account_name ASC",
            ])

            sql = "\n".join(base_sql)
            if not self.execute_query(sql, tuple(params)):
                raise pymysql.Error("Failed to get monitoring campaign facebook by params")
            data = self.fetch_all()
            if not self.commit():
                raise pymysql.Error("Failed to commit monitoring campaign facebook by params")

            hasil = {
                "status": True,
                "message": "Monitoring campaign facebook berhasil diambil",
                "data": data,
            }
        except pymysql.Error as e:
            hasil = {
                "status": False,
                'data': 'Terjadi error {!r}, error nya {}'.format(e, e.args[0])
            }
        return {'hasil': hasil}

    def get_monitoring_campaign_facebook_by_params(self, tanggal_dari, tanggal_sampai, selected_account_list=None, selected_domain_list=None):
        try:
            if isinstance(selected_account_list, str):
                selected_account_list = [selected_account_list.strip()]
            elif selected_account_list is None:
                selected_account_list = []
            elif isinstance(selected_account_list, (set, tuple)):
                selected_account_list = list(selected_account_list)
            selected_account_list = [str(a).strip() for a in selected_account_list if str(a).strip() and str(a).strip() != '%']

            if isinstance(selected_domain_list, str):
                selected_domain_list = [selected_domain_list.strip()]
            elif selected_domain_list is None:
                selected_domain_list = []
            elif isinstance(selected_domain_list, (set, tuple)):
                selected_domain_list = list(selected_domain_list)
            selected_domain_list = [str(d).strip() for d in selected_domain_list if str(d).strip() and str(d).strip() != '%']

            base_sql = [
                "SELECT",
                "\trs.account_name,",
                "\trs.campaign,",
                "\trs.spend,",
                "\trs.daily_budget,",
                "\trs.campaign_status,",
                "\tCASE",
                "\t\tWHEN UPPER(rs.campaign_status) = 'PAUSED' THEN 'Paused'",
                "\t\tWHEN rs.spend > rs.daily_budget THEN 'Overspend'",
                "\t\tELSE 'Normal'",
                "\tEND AS remark",
                "FROM (",
                "\tSELECT",
                "\t\ta.account_name AS account_name,",
                "\t\tb.data_ads_campaign_nm AS campaign,",
                "\t\tSUM(b.data_ads_country_spend) AS spend,",
                "\t\tCOALESCE(MAX(m.master_budget), 0) AS daily_budget,",
                "\t\tCOALESCE(SUBSTRING_INDEX(GROUP_CONCAT(m.master_status ORDER BY m.master_date DESC, m.mdd DESC SEPARATOR ','), ',', 1), 'UNKNOWN') AS campaign_status",
                "\tFROM master_account_ads a",
                "\tINNER JOIN data_ads_country b ON a.account_id = b.account_ads_id",
                "\tLEFT JOIN master_ads m ON m.account_ads_id = b.account_ads_id",
                "\t\tAND m.master_campaign_nm = b.data_ads_campaign_nm",
                "\t\tAND m.master_date = (",
                "\t\t\tSELECT MAX(m2.master_date)",
                "\t\t\tFROM master_ads m2",
                "\t\t\tWHERE m2.account_ads_id = b.account_ads_id",
                "\t\t\tAND m2.master_campaign_nm = b.data_ads_campaign_nm",
                "\t\t)",
                "\tWHERE b.data_ads_country_tanggal BETWEEN %s AND %s",
            ]
            params = [tanggal_dari, tanggal_sampai]

            if selected_account_list:
                acc_like = " OR ".join(["b.account_ads_id LIKE %s"] * len(selected_account_list))
                base_sql.append(f"\tAND ({acc_like})")
                params.extend([f"%{a}%" for a in selected_account_list])

            if selected_domain_list:
                dom_like = " OR ".join(["b.data_ads_domain LIKE %s"] * len(selected_domain_list))
                base_sql.append(f"\tAND ({dom_like})")
                params.extend([f"%{d}%" for d in selected_domain_list])

            base_sql.extend([
                "\tGROUP BY a.account_name, b.data_ads_campaign_nm",
                ") rs",
                "WHERE rs.spend > rs.daily_budget OR UPPER(rs.campaign_status) = 'PAUSED'",
                "ORDER BY rs.spend DESC, rs.account_name ASC",
            ])

            sql = "\n".join(base_sql)
            if not self.execute_query(sql, tuple(params)):
                raise pymysql.Error("Failed to get monitoring campaign facebook by params")
            data = self.fetch_all()
            if not self.commit():
                raise pymysql.Error("Failed to commit monitoring campaign facebook by params")

            hasil = {
                "status": True,
                "message": "Monitoring campaign facebook berhasil diambil",
                "data": data,
            }
        except pymysql.Error as e:
            hasil = {
                "status": False,
                'data': 'Terjadi error {!r}, error nya {}'.format(e, e.args[0])
            }
        return {'hasil': hasil}

    def get_all_ads_traffic_country_by_params(self, tanggal_dari, tanggal_sampai, selected_account_list, selected_domain_list, countries_list = None):
        try:
            # --- 1. Pastikan selected_account_list adalah list string
            if isinstance(selected_account_list, str):
                selected_account_list = [selected_account_list.strip()]
            elif selected_account_list is None:
                selected_account_list = []
            elif isinstance(selected_account_list, (set, tuple)):
                selected_account_list = list(selected_account_list)
            selected_account_list = [str(a).strip() for a in selected_account_list if str(a).strip()]
            engine = (self._report_engine() or '').strip().lower()
            use_clickhouse = engine in ('clickhouse', 'ch')
            account_like_col = "replaceRegexpAll(lowerUTF8(toString(b.account_ads_id)), '^act_', '')" if use_clickhouse else "b.account_ads_id"
            like_conditions_account = " OR ".join([f"{account_like_col} LIKE %s"] * len(selected_account_list))
            like_params_account = [f"%{str(account).strip().lower().removeprefix('act_')}%" if use_clickhouse else f"%{account}%" for account in selected_account_list] 
            # --- 2. Pastikan selected_domain_list adalah list string
            if isinstance(selected_domain_list, str):
                selected_domain_list = [selected_domain_list.strip()]
            elif selected_domain_list is None:
                selected_domain_list = []
            elif isinstance(selected_domain_list, (set, tuple)):
                selected_domain_list = list(selected_domain_list)
            data_domain_list = [str(d).strip() for d in selected_domain_list if str(d).strip()]
            like_conditions_domain = " OR ".join(["b.data_ads_domain LIKE %s"] * len(data_domain_list))
            like_params_domain = [f"%{domain}%" for domain in data_domain_list] 
            if use_clickhouse:
                base_sql = [
                    "SELECT",
                    "\tb.data_ads_country_nm AS country_name,",
                    "\tb.data_ads_country_cd AS country_code,",
                    "\tSUM(b.data_ads_country_spend) AS spend,",
                    "\tSUM(b.data_ads_country_impresi) AS impressions,",
                    "\tSUM(b.data_ads_country_click) AS clicks,",
                    "\tSUM(b.data_ads_country_reach) AS reach,",
                    "\tROUND(AVG(b.data_ads_country_cpr), 0) AS cpr,",
                    "\tCASE WHEN SUM(b.data_ads_country_click) > 0 THEN ROUND((SUM(b.data_ads_country_spend)/SUM(b.data_ads_country_click)), 0) ELSE 0 END AS cpc",
                    "FROM data_ads_country b",
                    "WHERE",
                ]
            else:
                base_sql = [
                    "SELECT",
                    "\trs.account_id, rs.account_name, rs.account_email,",
                    "\trs.country_name, rs.country_code, rs.domain, rs.campaign,",
                    "\tSUM(rs.spend) AS 'spend',",
                    "\tSUM(rs.impressions) AS 'impressions',",
                    "\tSUM(rs.clicks) AS 'clicks',",
                    "\tSUM(rs.reach) AS 'reach',",
                    "\tROUND(AVG(rs.cpr), 0) AS 'cpr',",
                    "\tROUND((SUM(rs.spend)/SUM(rs.clicks)), 0) AS 'cpc'",
                    "FROM (",
                        "\tSELECT",
                        "\t\ta.account_id, a.account_name, a.account_email,",
                        "\t\tb.data_ads_country_tanggal AS 'date',",
                        "\t\tb.data_ads_country_cd AS 'country_code',",
                        "\t\tb.data_ads_country_nm AS 'country_name',",
                        "\t\tb.data_ads_domain AS 'domain',",
                        "\t\tb.data_ads_campaign_nm AS 'campaign',",
                        "\t\tb.data_ads_country_spend AS 'spend',",
                        "\t\tb.data_ads_country_impresi AS 'impressions',",
                        "\t\tb.data_ads_country_click AS 'clicks',",
                        "\t\tb.data_ads_country_reach AS 'reach',",
                        "\t\tb.data_ads_country_cpr AS 'cpr'",
                        "\tFROM master_account_ads a",
                        "\tINNER JOIN data_ads_country b ON a.account_id = b.account_ads_id",
                        "\tWHERE",
                ]
            params = []
            if use_clickhouse:
                base_sql.append("\ttoDate(b.data_ads_country_tanggal) BETWEEN toDate(%s) AND toDate(%s)")
            else:
                base_sql.append("b.data_ads_country_tanggal BETWEEN %s AND %s")
            params.extend([tanggal_dari, tanggal_sampai])
            if selected_account_list:
                base_sql.append(f"\tAND ({like_conditions_account})")
                params.extend(like_params_account)
            if selected_domain_list:
                base_sql.append(f"\tAND ({like_conditions_domain})")
                params.extend(like_params_domain)
            # Normalize countries_list and apply country code filter
            country_codes = []
            if countries_list:
                if isinstance(countries_list, str):
                    country_codes = [c.strip() for c in countries_list.split(',') if c.strip()]
                elif isinstance(countries_list, (list, tuple)):
                    country_codes = [str(c).strip() for c in countries_list if str(c).strip()]
            if country_codes:
                placeholders = ','.join(['%s'] * len(country_codes))
                base_sql.append(f"AND b.data_ads_country_cd IN ({placeholders})")
                params.extend(country_codes)
            if use_clickhouse:
                base_sql.append("GROUP BY b.data_ads_country_cd, b.data_ads_country_nm")
                base_sql.append("ORDER BY b.data_ads_country_nm ASC")
            else:
                base_sql.append(") rs")
                base_sql.append("GROUP BY rs.country_code")
                base_sql.append("ORDER BY rs.country_name ASC")
            sql = "\n".join(base_sql)
            if use_clickhouse:
                self._ensure_report_connection()
                self.cur_hris = self.report_cur
                self.cur_hris.execute(sql, tuple(params))
                data_rows = self.fetch_all()
            else:
                if not self.execute_query(sql, tuple(params)):
                    raise pymysql.Error("Failed to get all adx traffic country by params")
                data_rows = self.fetch_all()
                if not self.commit():
                    raise pymysql.Error("Failed to commit get all adx traffic country by params")
            # Build summary
            total_impressions = sum((row.get('impressions') or 0) for row in data_rows) if data_rows else 0
            total_clicks = sum((row.get('clicks') or 0) for row in data_rows) if data_rows else 0
            total_revenue = sum((row.get('revenue') or 0) for row in data_rows) if data_rows else 0.0
            return {
                "status": True,
                "message": "Data ads traffic country berhasil diambil",
                "data": data_rows,
                "summary": {
                    "total_impressions": total_impressions,
                    "total_clicks": total_clicks,
                    "total_revenue": total_revenue,
                }
            }
        except pymysql.Error as e:
            return {
                "status": False,
                "error": f"Terjadi error {e!r}, error nya {e.args[0]}"
            }

    def fetch_user_sites_list(self, user_mail, tanggal_dari, tanggal_sampai):   
        try:
            if isinstance(user_mail, str):
                user_mail = [s.strip() for s in user_mail.split(",") if s.strip()]
            elif user_mail is None:
                user_mail = []
            elif isinstance(user_mail, (set, tuple)):
                user_mail = list(user_mail)
            if not user_mail:
                raise ValueError("user_mail is required and cannot be empty")
            like_conditions = " OR ".join(["a.user_mail LIKE %s"] * len(user_mail))
            like_params = [f"%{d}%" for d in user_mail]
            base_sql = [
                "SELECT",
                "\tb.data_adx_domain AS 'site_name'",
                "FROM",
                "\tapp_credentials a",
                "INNER JOIN data_adx_domain b ON a.account_id = b.account_id",
                "WHERE b.data_adx_domain_tanggal BETWEEN %s AND %s",
                f"\tAND ({like_conditions})",
            ]
            params = [tanggal_dari, tanggal_sampai] + like_params 
            base_sql.append("GROUP BY b.data_adx_domain")
            sql = "\n".join(base_sql)
            if not self.execute_query(sql, tuple(params)):
                raise pymysql.Error("Failed to get all adx traffic account by params")
            data = self.fetch_all()
            if not self.commit():
                raise pymysql.Error("Failed to commit get all adx traffic account by params")
            hasil = {
                "status": True,
                "message": "Data ads traffic campaign berhasil diambil",
                "data": [row['site_name'] for row in data]
            }
        except pymysql.Error as e:
            hasil = {
                "status": False,
                'data': 'Terjadi error {!r}, error nya {}'.format(e, e.args[0])
            }
        return {'hasil': hasil}

    def fetch_user_adsense_sites_list(self, tanggal_dari, tanggal_sampai, selected_account_list):   
        try:
            if isinstance(selected_account_list, str):
                selected_account_list = [s.strip() for s in selected_account_list.split(",") if s.strip()]
            elif selected_account_list is None:
                selected_account_list = []
            elif isinstance(selected_account_list, (set, tuple)):
                selected_account_list = list(selected_account_list)
            if not selected_account_list:
                raise ValueError("selected_account_list is required and cannot be empty")
            like_conditions = " OR ".join(["a.account_id LIKE %s"] * len(selected_account_list))
            like_params = [f"%{d}%" for d in selected_account_list]
            base_sql = [
                "SELECT",
                "\tCONCAT(SUBSTRING_INDEX(b.data_adsense_domain, '.', 1), '.com') AS 'site_name'",
                "FROM",
                "\tapp_credentials a",
                "INNER JOIN data_adsense_domain b ON a.account_id = b.account_id",
                "WHERE b.data_adsense_tanggal BETWEEN %s AND %s",
                f"\tAND ({like_conditions})",
            ]
            params = [tanggal_dari, tanggal_sampai] + like_params 
            base_sql.append("GROUP BY b.data_adsense_domain")
            sql = "\n".join(base_sql)
            if not self.execute_query(sql, tuple(params)):
                raise pymysql.Error("Failed to get all adsense traffic account by params")
            data = self.fetch_all()
            if not self.commit():
                raise pymysql.Error("Failed to commit get all adx traffic account by params")
            hasil = {
                "status": True,
                "message": "Data adsense traffic campaign berhasil diambil",
                "data": [row['site_name'] for row in data]
            }
        except pymysql.Error as e:
            hasil = {
                "status": False,
                'data': 'Terjadi error {!r}, error nya {}'.format(e, e.args[0])
            }
        return {'hasil': hasil}

    def fetch_account_list_by_domain(self, selected_domain_list, tanggal_dari, tanggal_sampai):   
        try:
            if isinstance(selected_domain_list, str):
                selected_domain_list = [s.strip() for s in selected_domain_list.split(",") if s.strip()]
            elif selected_domain_list is None:
                selected_domain_list = []
            elif isinstance(selected_domain_list, (set, tuple)):
                selected_domain_list = list(selected_domain_list)
            if not selected_domain_list:
                raise ValueError("selected_domain_list is required and cannot be empty")
            like_conditions = " OR ".join(["b.data_adx_domain LIKE %s"] * len(selected_domain_list))
            like_params = [f"%{d}%" for d in selected_domain_list]
            base_sql = [
                "SELECT",
                "\tb.account_id AS 'account_id', a.mcm_revenue_share AS 'mcm_revenue_share', a.account_name AS 'account_name'",
                "FROM",
                "\tapp_credentials a",
                "INNER JOIN data_adx_domain b ON a.account_id = b.account_id",
                "WHERE b.data_adx_domain_tanggal BETWEEN %s AND %s",
                f"\tAND ({like_conditions})",
            ]
            params = [tanggal_dari, tanggal_sampai] + like_params 
            base_sql.append("GROUP BY b.account_id")
            sql = "\n".join(base_sql)
            if not self.execute_query(sql, tuple(params)):
                raise pymysql.Error("Failed to get all adx traffic account by params")
            data = self.fetch_all()
            if not self.commit():
                raise pymysql.Error("Failed to commit get all adx traffic account by params")
            hasil = {
                "status": True,
                "message": "Data ads traffic account berhasil diambil",
                "data": [{'account_id': row['account_id'], 'mcm_revenue_share': row['mcm_revenue_share'], 'account_name': row['account_name']} for row in data]
            }
        except pymysql.Error as e:
            hasil = {
                "status": False,
                'data': 'Terjadi error {!r}, error nya {}'.format(e, e.args[0])
            }
        return {'hasil': hasil}

    def fetch_adsense_account_list_by_domain(self, selected_domain_list, tanggal_dari, tanggal_sampai):   
        try:
            if isinstance(selected_domain_list, str):
                selected_domain_list = [s.strip() for s in selected_domain_list.split(",") if s.strip()]
            elif selected_domain_list is None:
                selected_domain_list = []
            elif isinstance(selected_domain_list, (set, tuple)):
                selected_domain_list = list(selected_domain_list)
            if not selected_domain_list:
                raise ValueError("selected_domain_list is required and cannot be empty")
            like_conditions = " OR ".join(["CONCAT(SUBSTRING_INDEX(b.data_adsense_domain, '.', 2), '.com') LIKE %s"] * len(selected_domain_list))
            like_params = [f"%{d}%" for d in selected_domain_list]
            base_sql = [
                "SELECT",
                "\tb.account_id AS 'account_id', a.account_name AS 'account_name'",
                "FROM",
                "\tapp_credentials a",
                "INNER JOIN data_adsense_domain b ON a.account_id = b.account_id",
                "WHERE b.data_adsense_tanggal BETWEEN %s AND %s",
                f"\tAND ({like_conditions})",
            ]
            params = [tanggal_dari, tanggal_sampai] + like_params 
            base_sql.append("GROUP BY b.account_id")
            sql = "\n".join(base_sql)
            if not self.execute_query(sql, tuple(params)):
                raise pymysql.Error("Failed to get all adx traffic account by params")
            data = self.fetch_all()
            if not self.commit():
                raise pymysql.Error("Failed to commit get all adx traffic account by params")
            hasil = {
                "status": True,
                "message": "Data ads traffic account berhasil diambil",
                "data": [{'account_id': row['account_id'], 'account_name': row['account_name']} for row in data]
            }
        except pymysql.Error as e:
            hasil = {
                "status": False,
                'data': 'Terjadi error {!r}, error nya {}'.format(e, e.args[0])
            }
        return {'hasil': hasil}

    def fetch_user_mail_by_account(self, account_id_list):
        try:
            if isinstance(account_id_list, str):
                account_id_list = [s.strip() for s in account_id_list.split(",") if s.strip()]
            elif account_id_list is None:
                account_id_list = []
            elif isinstance(account_id_list, (set, tuple)):
                account_id_list = list(account_id_list)
            if not account_id_list:
                raise ValueError("account_id_list is required and cannot be empty")

            like_conditions = " OR ".join(["a.account_id LIKE %s"] * len(account_id_list))
            like_params = [f"%{d}%" for d in account_id_list]
            base_sql = [
                "SELECT",
                "\ta.user_mail",
                "FROM",
                "\tapp_credentials a",
                f"\tWHERE ({like_conditions})",
            ]
            params = like_params
            sql = "\n".join(base_sql)
            if not self.execute_query(sql, tuple(params)):
                raise pymysql.Error("Failed to get user mail by account id")
            data = self.fetch_all()
            if not self.commit():
                raise pymysql.Error("Failed to commit get user mail by account id")
            user_mail = [row['user_mail'] for row in data] if data else None
        except pymysql.Error as e:
            user_mail = None
        return user_mail

    def fetch_user_sites_id_list(self, tanggal_dari, tanggal_sampai, selected_account_list):   
        try:
            if isinstance(selected_account_list, str):
                selected_account_list = [s.strip() for s in selected_account_list.split(",") if s.strip()]
            elif selected_account_list is None:
                selected_account_list = []
            elif isinstance(selected_account_list, (set, tuple)):
                selected_account_list = list(selected_account_list)
            if not selected_account_list:
                raise ValueError("selected_account_list is required and cannot be empty")

            engine = (self._report_engine() or '').lower()
            use_clickhouse = engine in ('clickhouse', 'ch')

            like_col = "b.account_id" if use_clickhouse else "a.account_id"
            like_conditions = " OR ".join([f"{like_col} LIKE %s"] * len(selected_account_list))
            like_params = [f"%{d}%" for d in selected_account_list]

            if use_clickhouse:
                base_sql = [
                    "SELECT",
                    "\tb.data_adx_domain AS 'site_name'",
                    "FROM data_adx_domain b",
                    "WHERE toDate(b.data_adx_domain_tanggal) BETWEEN toDate(%s) AND toDate(%s)",
                    f"\tAND ({like_conditions})",
                    "GROUP BY b.data_adx_domain",
                ]
            else:
                base_sql = [
                    "SELECT",
                    "\tb.data_adx_domain AS 'site_name'",
                    "FROM",
                    "\tapp_credentials a",
                    "INNER JOIN data_adx_domain b ON a.account_id = b.account_id",
                    "WHERE b.data_adx_domain_tanggal BETWEEN %s AND %s",
                    f"\tAND ({like_conditions})",
                    "GROUP BY b.data_adx_domain",
                ]

            params = [tanggal_dari, tanggal_sampai] + like_params
            sql = "\n".join(base_sql)
            if not self.execute_query(sql, tuple(params)):
                raise pymysql.Error("Failed to get all adx traffic account by params")
            data = self.fetch_all()
            if not self.commit():
                raise pymysql.Error("Failed to commit get all adx traffic account by params")
            hasil = {
                "status": True,
                "message": "Data ads traffic campaign berhasil diambil",
                "data": [row['site_name'] for row in data]
            }
        except pymysql.Error as e:
            hasil = {
                "status": False,
                'data': 'Terjadi error {!r}, error nya {}'.format(e, e.args[0])
            }
        return {'hasil': hasil}

    def fetch_user_sites_adsense_id_list(self, tanggal_dari, tanggal_sampai, selected_account_list):   
        try:
            if isinstance(selected_account_list, str):
                selected_account_list = [s.strip() for s in selected_account_list.split(",") if s.strip()]
            elif selected_account_list is None:
                selected_account_list = []
            elif isinstance(selected_account_list, (set, tuple)):
                selected_account_list = list(selected_account_list)
            if not selected_account_list:
                raise ValueError("selected_account_list is required and cannot be empty")

            engine = (self._report_engine() or '').strip().lower()
            use_clickhouse = engine in ('clickhouse', 'ch')

            if use_clickhouse:
                like_col = "toString(b.account_id)"
                site_expr = "concat(arrayElement(splitByChar('.', b.data_adsense_domain), 1), '.', arrayElement(splitByChar('.', b.data_adsense_domain), 2))"
            else:
                like_col = "a.account_id"
                site_expr = "SUBSTRING_INDEX(b.data_adsense_domain, '.', 2)"

            like_conditions = " OR ".join([f"{like_col} LIKE %s"] * len(selected_account_list))
            like_params = [f"%{d}%" for d in selected_account_list]

            if use_clickhouse:
                base_sql = [
                    "SELECT",
                    f"\t{site_expr} AS 'site_name'",
                    "FROM",
                    "\tdata_adsense_domain b",
                    "WHERE toDate(b.data_adsense_tanggal) BETWEEN toDate(%s) AND toDate(%s)",
                    f"\tAND ({like_conditions})",
                    f"GROUP BY {site_expr}",
                ]
            else:
                base_sql = [
                    "SELECT",
                    "\tSUBSTRING_INDEX(b.data_adsense_domain, '.', 2) AS 'site_name'",
                    "FROM",
                    "\tapp_credentials a",
                    "INNER JOIN data_adsense_domain b ON a.account_id = b.account_id",
                    "WHERE b.data_adsense_tanggal BETWEEN %s AND %s",
                    f"\tAND ({like_conditions})",
                    "GROUP BY b.data_adsense_domain",
                ]

            params = [tanggal_dari, tanggal_sampai] + like_params
            sql = "\n".join(base_sql)
            if not self.execute_query(sql, tuple(params)):
                raise pymysql.Error("Failed to get all adsense traffic account by params")
            data = self.fetch_all()
            if not self.commit():
                raise pymysql.Error("Failed to commit get all adsense traffic account by params")
            hasil = {
                "status": True,
                "message": "Data adsense traffic campaign berhasil diambil",
                "data": [row['site_name'] for row in data]
            }
        except pymysql.Error as e:
            hasil = {
                "status": False,
                'data': 'Terjadi error {!r}, error nya {}'.format(e, e.args[0])
            }
        return {'hasil': hasil}

    def get_all_ads_roi_traffic_campaign_by_params(self, start_date_formatted, end_date_formatted, data_sub_domain=None):
        try:
            # --- 1. Pastikan data_sub_domain adalah list string
            if isinstance(data_sub_domain, str):
                data_sub_domain = [s.strip() for s in data_sub_domain.split(",") if s.strip()]
            elif data_sub_domain is None:
                data_sub_domain = []
            elif isinstance(data_sub_domain, (set, tuple)):
                data_sub_domain = list(data_sub_domain)
            if not data_sub_domain:
                raise ValueError("data_sub_domain is required and cannot be empty")
            # --- 2. Buat kondisi LIKE untuk tiap domain
            like_conditions = " OR ".join(["CONCAT(SUBSTRING_INDEX(b.data_ads_domain, '.', 2), '.com') LIKE %s"] * len(data_sub_domain))
            like_params = [f"%{d}%" for d in data_sub_domain]  # tambahkan % supaya match '.com'
            # --- 3. Susun query
            engine = (self._report_engine() or '').lower()
            use_clickhouse = engine in ('clickhouse', 'ch')

            if use_clickhouse:
                domain_expr = "concat(arrayElement(splitByChar('.', b.data_ads_domain), 1), '.', arrayElement(splitByChar('.', b.data_ads_domain), 2), '.com')"
                like_conditions_ch = " OR ".join([f"{domain_expr} LIKE %s"] * len(data_sub_domain))
                base_sql = [
                    "SELECT",
                    "\t'' AS 'account_id',",
                    "\t'' AS 'account_name',",
                    "\t'' AS 'account_email',",
                    "\ttoDate(b.data_ads_country_tanggal) AS 'date',",
                    f"\t{domain_expr} AS domain,",
                    "\t'' AS 'campaign',",
                    "\tb.data_ads_country_cd AS 'country_code',",
                    "\tSUM(b.data_ads_country_spend) AS 'spend',",
                    "\tSUM(b.data_ads_country_click) AS 'clicks_fb',",
                    "\tSUM(b.data_ads_country_impresi) AS 'impressions_fb',",
                    "\tROUND(AVG(b.data_ads_country_cpr), 0) AS 'cpr',",
                    "\tCASE WHEN SUM(b.data_ads_country_click) > 0 THEN ROUND(SUM(b.data_ads_country_spend) / SUM(b.data_ads_country_click), 0) ELSE 0 END AS 'cpc_fb'",
                    "FROM data_ads_country b",
                    "WHERE toDate(b.data_ads_country_tanggal) BETWEEN toDate(%s) AND toDate(%s)",
                    f"\tAND ({like_conditions_ch})",
                    f"GROUP BY toDate(b.data_ads_country_tanggal), {domain_expr}, b.data_ads_country_cd",
                    "ORDER BY toDate(b.data_ads_country_tanggal) ASC",
                ]
                params = [start_date_formatted, end_date_formatted] + like_params
                sql = "\n".join(base_sql)
            else:
                base_sql = [
                    "SELECT",
                    "\trs.account_id, rs.account_name, rs.account_email,",
                    "\trs.date, rs.domain, rs.campaign, rs.country_code,",
                    "\tSUM(rs.spend) AS 'spend',",
                    "\tSUM(rs.clicks_fb) AS 'clicks_fb',",
                    "\tSUM(rs.impressions_fb) AS 'impressions_fb',",
                    "\tROUND(AVG(rs.cpr), 0) AS 'cpr',",
                    "\tROUND((SUM(rs.spend)/SUM(rs.clicks_fb)), 0) AS 'cpc_fb'",
                    "FROM (",
                        "\tSELECT",
                        "\t\ta.account_id, a.account_name, a.account_email,",
                        "\t\tb.data_ads_country_tanggal AS 'date',",
                        "\t\tb.data_ads_country_cd AS 'country_code',",
                        "\t\tb.data_ads_country_nm AS 'country_name',",
                        "\t\tSUBSTRING_INDEX(b.data_ads_domain, '.', 2) AS domain,",
                        "\t\tb.data_ads_campaign_nm AS 'campaign',",
                        "\t\tb.data_ads_country_spend AS 'spend',",
                        "\t\tb.data_ads_country_impresi AS 'impressions_fb',",
                        "\t\tb.data_ads_country_click AS 'clicks_fb',",
                        "\t\tb.data_ads_country_cpr AS 'cpr'",
                        "\tFROM master_account_ads a",
                        "\tINNER JOIN data_ads_country b ON a.account_id = b.account_ads_id",
                        "\tWHERE b.data_ads_country_tanggal BETWEEN %s AND %s",
                        f"\tAND ({like_conditions})",
                    ") rs",
                    "GROUP BY rs.date, rs.domain, rs.country_code",
                    "ORDER BY rs.account_id, rs.date",
                ]
                params = [start_date_formatted, end_date_formatted] + like_params
                sql = "\n".join(base_sql)
            if not self.execute_query(sql, tuple(params)):
                raise pymysql.Error("Failed to get all ads roi traffic campaign by params")
            data = self.fetch_all()
            if not self.commit():
                raise pymysql.Error("Failed to commit get all ads roi traffic campaign by params")
            hasil = {
                "status": True,
                "message": "Data ads traffic campaign berhasil diambil",
                "data": data
            }

        except pymysql.Error as e:
            hasil = {
                "status": False,
                "data": f"Terjadi error {e!r}, error nya {e.args[0]}"
            }
        except Exception as e:
            hasil = {
                "status": False,
                "data": f"Terjadi error {e}"
            }

        return {"hasil": hasil}

    def get_all_ads_adsense_roi_traffic_campaign_by_params( self, start_date_formatted, end_date_formatted, data_sub_domain=None ):
        try:
            # --- 1. Normalize input
            if isinstance(data_sub_domain, str):
                domains = [d.strip() for d in data_sub_domain.split(",") if d.strip()]
            elif isinstance(data_sub_domain, (list, tuple, set)):
                domains = list(data_sub_domain)
            else:
                domains = []
            domains = [str(d).strip() for d in (domains or []) if str(d).strip()]
            if not domains:
                raise ValueError("data_sub_domain is required")

            engine = (self._report_engine() or '').strip().lower()
            use_clickhouse = engine in ('clickhouse', 'ch')

            if use_clickhouse:
                domain_1 = "arrayElement(splitByChar('.', b.data_ads_domain), 1)"
                domain_2 = "concat(arrayElement(splitByChar('.', b.data_ads_domain), 1), '.', arrayElement(splitByChar('.', b.data_ads_domain), 2))"
                cond_parts = []
                like_params = []
                for d in domains:
                    d = str(d or '').strip()
                    if not d:
                        continue
                    d_first = d.split('.')[0] if '.' in d else d
                    cond_parts.append(
                        f"(positionCaseInsensitive(b.data_ads_domain, %s) > 0 "
                        f"OR positionCaseInsensitive({domain_2}, %s) > 0 "
                        f"OR positionCaseInsensitive({domain_1}, %s) > 0)"
                    )
                    like_params.extend([d, d, d_first])
                like_clause = f"AND ({' OR '.join(cond_parts)})" if cond_parts else ""
                query = f"""
                SELECT
                    rs.account_id,
                    any(rs.account_name) AS account_name,
                    any(rs.account_email) AS account_email,
                    rs.date,
                    rs.domain,
                    rs.country_code,
                    sum(rs.spend) AS spend,
                    sum(rs.clicks_fb) AS clicks_fb,
                    sum(rs.impressions_fb) AS impressions_fb,
                    round(avg(rs.cpr), 0) AS cpr,
                    if(sum(rs.clicks_fb) = 0, 0,
                        round(sum(rs.spend) / sum(rs.clicks_fb), 0)
                    ) AS cpc_fb
                FROM
                (
                    SELECT
                        a.account_id,
                        a.account_name,
                        a.account_email,
                        b.data_ads_country_tanggal AS date,
                        b.data_ads_country_cd AS country_code,
                        b.data_ads_country_nm AS country_name,
                        arrayStringConcat(
                            arraySlice(splitByChar('.', b.data_ads_domain), 1, 2),
                            '.'
                        ) AS domain,
                        b.data_ads_country_spend AS spend,
                        b.data_ads_country_impresi AS impressions_fb,
                        b.data_ads_country_click AS clicks_fb,
                        b.data_ads_country_cpr AS cpr
                    FROM hris_trendHorizone.master_account_ads a
                    INNER JOIN hris_trendHorizone.data_ads_country b ON a.account_id = b.account_ads_id
                    WHERE b.data_ads_country_tanggal BETWEEN toDate(%s) AND toDate(%s)
                    {like_clause}
                ) rs
                GROUP BY
                    rs.account_id,
                    rs.date,
                    rs.domain,
                    rs.country_code
                ORDER BY
                    rs.account_id,
                    rs.date
                """
                params_tuple = tuple([start_date_formatted, end_date_formatted] + like_params)
                self._ensure_report_connection()
                self.cur_hris = self.report_cur
                self.cur_hris.execute(query, params_tuple)
                data = self.fetch_all() or []
            else:
                like_conditions = " OR ".join([
                    "(b.data_ads_domain LIKE %s OR SUBSTRING_INDEX(b.data_ads_domain, '.', 2) LIKE %s OR SUBSTRING_INDEX(b.data_ads_domain, '.', 1) LIKE %s)"
                ] * len(domains))
                like_params = []
                for d in domains:
                    d = str(d or '').strip()
                    if not d:
                        continue
                    d_first = d.split('.')[0] if '.' in d else d
                    like_params.extend([f"%{d}%", f"%{d}%", f"%{d_first}%"])
                base_sql = [
                    "SELECT",
                    "\trs.account_id, rs.account_name, rs.account_email,",
                    "\trs.date, SUBSTRING_INDEX(rs.domain, '.', -2) AS 'domain', rs.country_code,",
                    "\tSUM(rs.spend) AS 'spend',",
                    "\tSUM(rs.clicks_fb) AS 'clicks_fb',",
                    "\tSUM(rs.impressions_fb) AS 'impressions_fb',",
                    "\tROUND(AVG(rs.cpr), 0) AS 'cpr',",
                    "\tROUND((SUM(rs.spend)/SUM(rs.clicks_fb)), 0) AS 'cpc_fb'",
                    "FROM (",
                    "\tSELECT",
                    "\t\ta.account_id, a.account_name, a.account_email,",
                    "\t\tb.data_ads_country_tanggal AS 'date',",
                    "\t\tb.data_ads_country_cd AS 'country_code',",
                    "\t\tb.data_ads_country_nm AS 'country_name',",
                    "\t\tCONCAT(SUBSTRING_INDEX(b.data_ads_domain, '.', 2), '.com') AS domain,",
                    "\t\tb.data_ads_country_spend AS 'spend',",
                    "\t\tb.data_ads_country_impresi AS 'impressions_fb',",
                    "\t\tb.data_ads_country_click AS 'clicks_fb',",
                    "\t\tb.data_ads_country_cpr AS 'cpr'",
                    "\tFROM master_account_ads a",
                    "\tINNER JOIN data_ads_country b ON a.account_id = b.account_ads_id",
                    "\tWHERE b.data_ads_country_tanggal BETWEEN %s AND %s",
                    f"\tAND ({like_conditions})",
                    ") rs",
                    "GROUP BY rs.date, rs.country_code, SUBSTRING_INDEX(rs.domain, '.', -2)",
                    "ORDER BY rs.account_id, rs.date",
                ]
                params = [start_date_formatted, end_date_formatted] + like_params
                sql = "\n".join(base_sql)
                if not self.execute_query(sql, tuple(params)):
                    raise pymysql.Error("Failed to get all ads adsense roi traffic campaign by params")
                data = self.fetch_all() or []
                if not self.commit():
                    raise pymysql.Error("Failed to commit get all ads adsense roi traffic campaign by params")

            hasil = {
                "status": True,
                "message": "Data ads traffic campaign berhasil diambil",
                "data": data
            }
        except Exception as e:
            hasil = {
                "status": False,
                "data": f"Terjadi error {e!r}"
            }
        return {"hasil": hasil}

    def get_all_ads_roi_monitoring_campaign_by_params(self, start_date_formatted, end_date_formatted, data_sub_domain=None):
        try:
            # --- 1. Pastikan data_sub_domain adalah list string
            if isinstance(data_sub_domain, str):
                data_sub_domain = [s.strip() for s in data_sub_domain.split(",") if s.strip()]
            elif data_sub_domain is None:
                data_sub_domain = []
            elif isinstance(data_sub_domain, (set, tuple)):
                data_sub_domain = list(data_sub_domain)
            if not data_sub_domain:
                raise ValueError("data_sub_domain is required and cannot be empty")
            # --- 2. Buat kondisi LIKE untuk setiap domain
            like_conditions = " OR ".join(["b.data_ads_domain LIKE %s"] * len(data_sub_domain))
            like_clause = f"\tAND ({like_conditions})" if like_conditions else ""
            like_params = [f"%{d}%" for d in data_sub_domain]
            # --- 3. Susun query
            engine = (self._report_engine() or '').strip().lower()
            use_clickhouse = engine in ('clickhouse', 'ch')

            if use_clickhouse:
                domain_expr = "concat(arrayElement(splitByChar('.', b.data_ads_domain), 1), '.', arrayElement(splitByChar('.', b.data_ads_domain), 2), '.com')"
                base_sql = [
                    "SELECT",
                    "\trs.date,",
                    "\trs.account_id, rs.account_name,",
                    "\trs.domain, rs.country_code,",
                    "\tSUM(rs.spend) AS spend,",
                    "\tSUM(rs.impressions) AS impressions,",
                    "\tSUM(rs.clicks) AS clicks,",
                    "\tCASE WHEN SUM(rs.impressions) > 0 THEN ROUND((SUM(rs.clicks) / SUM(rs.impressions)) * 100, 4) ELSE 0 END AS ctr,",
                    "\tCASE WHEN SUM(rs.clicks) > 0 THEN ROUND(SUM(rs.spend) / SUM(rs.clicks), 4) ELSE 0 END AS cpc,",
                    "\tCASE WHEN SUM(rs.impressions) > 0 THEN ROUND((SUM(rs.spend) / SUM(rs.impressions)) * 1000, 4) ELSE 0 END AS cpm",
                    "FROM (",
                        "\tSELECT",
                        "\t\ttoDate(b.data_ads_country_tanggal) AS date,",
                        "\t\tb.account_ads_id AS account_id,",
                        "\t\t'' AS account_name,",
                        "\t\tb.data_ads_domain AS domain_raw,",
                        f"\t\t{domain_expr} AS domain,",
                        "\t\tb.data_ads_country_cd AS country_code,",
                        "\t\tb.data_ads_country_spend AS spend,",
                        "\t\tb.data_ads_country_impresi AS impressions,",
                        "\t\tb.data_ads_country_click AS clicks",
                        "\tFROM data_ads_country b",
                        "\tWHERE toDate(b.data_ads_country_tanggal) BETWEEN toDate(%s) AND toDate(%s)",
                        f"{like_clause}",
                    ") rs",
                    "GROUP BY",
                    "\trs.date,",
                    "\trs.account_id,",
                    "\trs.account_name,",
                    "\trs.domain,",
                    "\trs.country_code"
                ]
            else:
                base_sql = [
                    "SELECT",
                    "\trs.date,",
                    "\trs.account_id, rs.account_name,",
                    "\trs.domain, rs.country_code,",
                    "\tSUM(rs.spend) AS spend,",
                    "\tSUM(rs.impressions) AS impressions,",
                    "\tSUM(rs.clicks) AS clicks,",
                    "\tCASE WHEN SUM(rs.impressions) > 0 THEN ROUND((SUM(rs.clicks) / SUM(rs.impressions)) * 100, 4) ELSE 0 END AS ctr,",
                    "\tCASE WHEN SUM(rs.clicks) > 0 THEN ROUND(SUM(rs.spend) / SUM(rs.clicks), 4) ELSE 0 END AS cpc,",
                    "\tCASE WHEN SUM(rs.impressions) > 0 THEN ROUND((SUM(rs.spend) / SUM(rs.impressions)) * 1000, 4) ELSE 0 END AS cpm",
                    "FROM (",
                        "\tSELECT",
                        "\t\tb.data_ads_country_tanggal AS date,",
                        "\t\ta.account_id, a.account_name,",
                        "\t\tb.data_ads_domain AS domain_raw,",
                        "\t\tCONCAT(SUBSTRING_INDEX(b.data_ads_domain, '.', 2), '.com') AS domain,",
                        "\t\tb.data_ads_country_cd AS country_code,",
                        "\t\tb.data_ads_country_spend AS spend,",
                        "\t\tb.data_ads_country_impresi AS impressions,",
                        "\t\tb.data_ads_country_click AS clicks",
                        "\tFROM master_account_ads a",
                        "\tINNER JOIN data_ads_country b ON a.account_id = b.account_ads_id",
                        "\tWHERE b.data_ads_country_tanggal BETWEEN %s AND %s",
                        f"{like_clause}",
                    ") rs",
                    "GROUP BY",
                    "\trs.date,",
                    "\trs.account_id,",
                    "\trs.account_name,",
                    "\trs.domain,",
                    "\trs.country_code"
                ]

            # --- 4. Gabungkan parameter
            params = [start_date_formatted, end_date_formatted] + like_params
            # --- 5. Eksekusi query
            sql = "\n".join(base_sql)
            if not self.execute_query(sql, tuple(params)):
                raise pymysql.Error("Failed to get all ads roi traffic campaign by params")
            data = self.fetch_all()

            if use_clickhouse and isinstance(data, list) and data:
                try:
                    account_ids = []
                    seen_ids = set()
                    for r in data:
                        aid = str((r or {}).get('account_id') or '').strip()
                        if aid and aid not in seen_ids:
                            seen_ids.add(aid)
                            account_ids.append(aid)
                    if account_ids:
                        placeholders = ','.join(['%s'] * len(account_ids))
                        sql_map = f"SELECT account_id, account_name FROM master_account_ads WHERE account_id IN ({placeholders})"
                        if self.execute_query(sql_map, tuple(account_ids)):
                            rows_map = self.fetch_all() or []
                            name_map = {str((x or {}).get('account_id') or '').strip(): str((x or {}).get('account_name') or '').strip() for x in (rows_map or [])}
                            for r in data:
                                if not isinstance(r, dict):
                                    continue
                                aid = str(r.get('account_id') or '').strip()
                                if aid and not str(r.get('account_name') or '').strip():
                                    r['account_name'] = name_map.get(aid, '')
                except Exception:
                    pass

            if not self.commit():
                raise pymysql.Error("Failed to commit get all ads roi traffic campaign by params")
            hasil = {
                "status": True,
                "message": "Data ads traffic campaign berhasil diambil",
                "data": data
            }
        except pymysql.Error as e:
            hasil = {
                "status": False,
                "data": f"Terjadi error {e!r}, error nya {e.args[0]}"
            }
        except Exception as e:
            hasil = {
                "status": False,
                "data": f"Terjadi error {e}"
            }

        return {"hasil": hasil}

    def get_all_adsense_roi_monitoring_campaign_by_params(self, start_date_formatted, end_date_formatted, data_sub_domain=None):
        try:
            if isinstance(data_sub_domain, str):
                data_sub_domain = [s.strip() for s in data_sub_domain.split(",") if s.strip()]
            elif data_sub_domain is None:
                data_sub_domain = []
            elif isinstance(data_sub_domain, (set, tuple)):
                data_sub_domain = list(data_sub_domain)
            if not data_sub_domain:
                raise ValueError("data_sub_domain is required and cannot be empty")

            engine = (self._report_engine() or '').strip().lower()
            use_clickhouse = engine in ('clickhouse', 'ch')

            if use_clickhouse:
                domain_1 = "arrayElement(splitByChar('.', b.data_ads_domain), 1)"
                domain_2 = "concat(arrayElement(splitByChar('.', b.data_ads_domain), 1), '.', arrayElement(splitByChar('.', b.data_ads_domain), 2))"

                cond_parts = []
                like_params = []
                for d in (data_sub_domain or []):
                    d = str(d or '').strip()
                    if not d:
                        continue
                    d_first = d.split('.')[0] if '.' in d else d
                    cond_parts.append(f"(positionCaseInsensitive(b.data_ads_domain, %s) > 0 OR positionCaseInsensitive({domain_2}, %s) > 0 OR positionCaseInsensitive({domain_1}, %s) > 0)")
                    like_params.extend([d, d, d_first])

                like_clause = f"\tAND ({' OR '.join(cond_parts)})" if cond_parts else ""

                base_sql = [
                    "SELECT",
                    "\tany(rs.account_id) AS account_id,",
                    "\t'' AS account_name,",
                    "\trs.domain AS 'domain', rs.country_code,",
                    "\tSUM(rs.spend) AS 'spend'",
                    "FROM (",
                        "\tSELECT",
                        "\t\tb.account_ads_id AS account_id,",
                        f"\t\t{domain_2} AS domain,",
                        "\t\tb.data_ads_country_cd AS 'country_code',",
                        "\t\tb.data_ads_country_spend AS 'spend'",
                        "\tFROM data_ads_country b",
                        "\tWHERE toDate(b.data_ads_country_tanggal) BETWEEN toDate(%s) AND toDate(%s)",
                        f"{like_clause}",
                    ") rs",
                    "GROUP BY rs.domain, rs.country_code"
                ]
                params = [start_date_formatted, end_date_formatted] + like_params
            else:
                like_conditions = " OR ".join([
                    "(b.data_ads_domain LIKE %s OR SUBSTRING_INDEX(b.data_ads_domain, '.', 2) LIKE %s OR SUBSTRING_INDEX(b.data_ads_domain, '.', 1) LIKE %s)"
                ] * len(data_sub_domain))
                like_clause = f"\tAND ({like_conditions})" if like_conditions else ""
                like_params = []
                for d in data_sub_domain:
                    d = str(d or '').strip()
                    if not d:
                        continue
                    d_first = d.split('.')[0] if '.' in d else d
                    like_params.extend([f"%{d}%", f"%{d}%", f"%{d_first}%"])
                base_sql = [
                    "SELECT",
                    "\trs.account_id, rs.account_name,",
                    "\trs.domain AS 'domain', rs.country_code,",
                    "\tSUM(rs.spend) AS 'spend'",
                    "FROM (",
                        "\tSELECT",
                        "\t\ta.account_id, a.account_name,",
                        "\t\tb.data_ads_domain AS 'domain_raw',",
                        "\t\tSUBSTRING_INDEX(b.data_ads_domain, '.', 2) AS domain,",
                        "\t\tb.data_ads_country_cd AS 'country_code',",
                        "\t\tb.data_ads_country_spend AS 'spend'",
                        "\tFROM master_account_ads a",
                        "\tINNER JOIN data_ads_country b ON a.account_id = b.account_ads_id",
                        "\tWHERE b.data_ads_country_tanggal BETWEEN %s AND %s",
                        f"{like_clause}",
                    ") rs",
                    "GROUP BY rs.domain, rs.country_code"
                ]
                params = [start_date_formatted, end_date_formatted] + like_params

            sql = "\n".join(base_sql)
            if not self.execute_query(sql, tuple(params)):
                raise pymysql.Error("Failed to get all ads roi traffic campaign by params")
            data = self.fetch_all()

            if use_clickhouse and isinstance(data, list) and data:
                try:
                    account_ids = []
                    seen_ids = set()
                    for r in data:
                        aid = str((r or {}).get('account_id') or '').strip()
                        if aid and aid not in seen_ids:
                            seen_ids.add(aid)
                            account_ids.append(aid)
                    if account_ids:
                        placeholders = ','.join(['%s'] * len(account_ids))
                        sql_map = f"SELECT account_id, account_name FROM master_account_ads WHERE account_id IN ({placeholders})"
                        if self.execute_query(sql_map, tuple(account_ids)):
                            rows_map = self.fetch_all() or []
                            name_map = {str((x or {}).get('account_id') or '').strip(): str((x or {}).get('account_name') or '').strip() for x in (rows_map or [])}
                            for r in data:
                                if not isinstance(r, dict):
                                    continue
                                aid = str(r.get('account_id') or '').strip()
                                if aid and not str(r.get('account_name') or '').strip():
                                    r['account_name'] = name_map.get(aid, '')
                except Exception:
                    pass

            if not self.commit():
                raise pymysql.Error("Failed to commit get all ads roi traffic campaign by params")
            hasil = {
                "status": True,
                "message": "Data ads traffic campaign berhasil diambil",
                "data": data
            }
        except pymysql.Error as e:
            hasil = {
                "status": False,
                "data": f"Terjadi error {e!r}, error nya {e.args[0]}"
            }
        except Exception as e:
            hasil = {
                "status": False,
                "data": f"Terjadi error {e}"
            }

        return {"hasil": hasil}

    def get_all_ads_roi_traffic_country_by_params(self, start_date_formatted, end_date_formatted, data_sub_domain=None, countries_list=None):
        try:
            # --- 1. Siapkan data_sub_domain sebagai list
            if isinstance(data_sub_domain, str):
                data_sub_domain = [s.strip() for s in data_sub_domain.split(",") if s.strip()]
            elif data_sub_domain is None:
                data_sub_domain = []
            elif isinstance(data_sub_domain, (set, tuple)):
                data_sub_domain = list(data_sub_domain)
            if not data_sub_domain:
                raise ValueError("data_sub_domain is required and cannot be empty")
            # --- 2. Buat kondisi LIKE untuk tiap domain
            like_conditions = " OR ".join(["b.data_ads_domain LIKE %s"] * len(data_sub_domain))
            like_params = [f"%{d}%" for d in data_sub_domain]  # tambahkan % supaya match '.com'
            sql_parts = [
                "SELECT",
                "\trs.account_id, rs.account_name, rs.account_email,",
                "\trs.country_name, rs.country_code, rs.domain, rs.campaign,",
                "\tSUM(rs.spend) AS 'spend',",
                "\tSUM(rs.clicks) AS 'clicks',",
                "\tSUM(rs.impressions) AS 'impressions',",
                "\tROUND(AVG(rs.cpr), 0) AS 'cpr',",
                "\tROUND((SUM(rs.spend)/SUM(rs.clicks)), 0) AS 'cpc'",
                "FROM (",
                    "\tSELECT",
                    "\t\ta.account_id, a.account_name, a.account_email,",
                    "\t\tb.data_ads_country_tanggal AS 'date',",
                    "\t\tb.data_ads_country_cd AS 'country_code',",
                    "\t\tb.data_ads_country_nm AS 'country_name',",
                    "\t\tb.data_ads_domain AS 'domain',",
                    "\t\tb.data_ads_campaign_nm AS 'campaign',",
                    "\t\tb.data_ads_country_spend AS 'spend',",
                    "\t\tb.data_ads_country_impresi AS 'impressions',",
                    "\t\tb.data_ads_country_click AS 'clicks',",
                    "\t\tb.data_ads_country_cpr AS 'cpr'",
                    "\tFROM master_account_ads a",
                    "\tINNER JOIN data_ads_country b ON a.account_id = b.account_ads_id",
                    "\tWHERE b.data_ads_country_tanggal BETWEEN %s AND %s",
                    f"\tAND ({like_conditions})",
            ]
            params = [start_date_formatted, end_date_formatted] + like_params
            # --- 3. Filter countries jika ada
            country_codes = []
            if countries_list:
                if isinstance(countries_list, str):
                    country_codes = [c.strip() for c in countries_list.split(',') if c.strip()]
                elif isinstance(countries_list, (list, tuple)):
                    country_codes = [str(c).strip() for c in countries_list if str(c).strip()]
            if country_codes:
                placeholders = ','.join(['%s'] * len(country_codes))
                sql_parts.append(f"AND b.data_ads_country_cd IN ({placeholders})")
                params.extend(country_codes)
            sql_parts.append(") rs")
            sql_parts.append("GROUP BY rs.country_code")
            sql_parts.append("ORDER BY rs.country_name ASC")
            sql = "\n".join(sql_parts)
            # --- 4. Eksekusi query
            if not self.execute_query(sql, tuple(params)):
                raise pymysql.Error("Failed to get all ads roi traffic campaign by params")
            data = self.fetch_all()
            if not self.commit():
                raise pymysql.Error("Failed to commit get all ads roi traffic campaign by params")
            hasil = {
                "status": True,
                "message": "Data ads traffic campaign berhasil diambil",
                "data": data
            }
        except Exception as e:
            hasil = {
                "status": False,
                "data": f"Terjadi error {e!r}"
            }
        return {"hasil": hasil}

    def get_all_ads_monitoring_country_by_params(self, start_date_formatted, end_date_formatted, data_sub_domain=None, countries_list=None):
        try:
            # --- 1. Siapkan data_sub_domain sebagai list
            if isinstance(data_sub_domain, str):
                data_sub_domain = [s.strip() for s in data_sub_domain.split(",") if s.strip()]
            elif data_sub_domain is None:
                data_sub_domain = []
            elif isinstance(data_sub_domain, (set, tuple)):
                data_sub_domain = list(data_sub_domain)
            if not data_sub_domain:
                raise ValueError("data_sub_domain is required and cannot be empty")
            # --- 2. Buat kondisi LIKE untuk tiap domain
            like_conditions = " OR ".join(["b.data_ads_domain LIKE %s"] * len(data_sub_domain))
            like_clause = f"\tAND ({like_conditions})" if like_conditions else ""
            like_params = [f"%{d}%" for d in data_sub_domain]  # tambahkan % supaya match '.com'
            sql_parts = [
                "SELECT",
                "\trs.account_id, rs.account_name, rs.domain,",
                "\trs.country_name, rs.country_code,",
                "\tSUM(rs.spend) AS 'spend'",
                "FROM (",
                    "\tSELECT",
                    "\t\ta.account_id, a.account_name,",
                    "\t\tb.data_ads_domain AS 'domain',",
                    "\t\tb.data_ads_country_cd AS 'country_code',",
                    "\t\tb.data_ads_country_nm AS 'country_name',",
                    "\t\tb.data_ads_country_spend AS 'spend'",
                    "\tFROM master_account_ads a",
                    "\tINNER JOIN data_ads_country b ON a.account_id = b.account_ads_id",
                    "\tWHERE b.data_ads_country_tanggal BETWEEN %s AND %s",
                     f"{like_clause}",
            ]
            params = [start_date_formatted, end_date_formatted] + like_params
            # --- 3. Filter countries jika ada
            country_codes = []
            if countries_list:
                if isinstance(countries_list, str):
                    country_codes = [c.strip() for c in countries_list.split(',') if c.strip()]
                elif isinstance(countries_list, (list, tuple)):
                    country_codes = [str(c).strip() for c in countries_list if str(c).strip()]
            if country_codes:
                placeholders = ','.join(['%s'] * len(country_codes))
                sql_parts.append(f"AND b.data_ads_country_cd IN ({placeholders})")
                params.extend(country_codes)
            sql_parts.append(") rs")
            sql_parts.append("GROUP BY rs.domain, rs.country_code")
            sql_parts.append("ORDER BY rs.country_name ASC")
            sql = "\n".join(sql_parts)
            # --- 4. Eksekusi query
            if not self.execute_query(sql, tuple(params)):
                raise pymysql.Error("Failed to get all ads roi traffic campaign by params")
            data = self.fetch_all()
            if not self.commit():
                raise pymysql.Error("Failed to commit get all ads roi traffic campaign by params")
            hasil = {
                "status": True,
                "message": "Data ads traffic campaign berhasil diambil",
                "data": data
            }
        except Exception as e:
            hasil = {
                "status": False,
                "data": f"Terjadi error {e!r}"
            }
        return {"hasil": hasil}


    def fetch_country_list(self, start_date, end_date, selected_account=None, selected_domain_list=None):
        try:
            # --- 1. Pastikan selected_domain_list adalah list string
            if isinstance(selected_domain_list, str):
                selected_domain_list = [selected_domain_list.strip()]
            elif selected_domain_list is None:
                selected_domain_list = []
            elif isinstance(selected_domain_list, (set, tuple)):
                selected_domain_list = list(selected_domain_list)
            data_domain_list = [str(d).strip() for d in selected_domain_list if str(d).strip()]
            # --- 2. Buat kondisi LIKE untuk setiap domain (kolom AdX)
            like_conditions = " OR ".join(["b.data_adx_country_domain LIKE %s"] * len(data_domain_list))
            like_params = [f"%{domain}%" for domain in data_domain_list] 
            base_sql = [
                "SELECT",
                "\tb.data_adx_country_cd AS 'country_code',",
                "\tb.data_adx_country_nm AS 'country_name'",
                "FROM",
                "\tapp_credentials a",
                "INNER JOIN data_adx_country b ON a.account_id = b.account_id",
                "\tWHERE",
            ]
            params = []
            base_sql.append("b.data_adx_country_tanggal BETWEEN %s AND %s")
            params.extend([start_date, end_date])
            account_list = []
            if isinstance(selected_account, str):
                account_list = [s.strip() for s in selected_account.split(',') if s.strip()]
            elif isinstance(selected_account, (list, tuple, set)):
                account_list = [str(s).strip() for s in selected_account if str(s).strip()]

            if account_list:
                account_tokens = []
                for a in account_list:
                    account_tokens.append(a)
                    if a.lower().startswith('act_'):
                        account_tokens.append(a[4:])
                    else:
                        account_tokens.append(f"act_{a}")
                account_tokens = list(dict.fromkeys([x for x in account_tokens if x]))
                like_conditions_account = " OR ".join(["b.account_id LIKE %s"] * len(account_tokens))
                base_sql.append(f"\tAND ({like_conditions_account})")
                params.extend([f"%{a}%" for a in account_tokens])
            if data_domain_list:
                base_sql.append(f"\tAND ({like_conditions})")
                params.extend(like_params)
            base_sql.append("GROUP BY b.data_adx_country_cd, b.data_adx_country_nm")
            base_sql.append("ORDER BY b.data_adx_country_cd ASC")
            sql = "\n".join(base_sql)
            if not self.execute_query(sql, tuple(params)):
                raise pymysql.Error("Failed to get all country list by params")
            data = self.fetch_all()
            if not self.commit():
                raise pymysql.Error("Failed to commit get all country list by params")
            hasil = {
                "status": True,
                "message": "Data country list berhasil diambil",
                "data": data
            }
        except pymysql.Error as e:
            hasil = {
                "status": False,
                "data": f"Terjadi error {e!r}, error nya {e.args[0]}"
            }
        except Exception as e:
            hasil = {
                "status": "error",
                "message": "Gagal mengambil data negara.",
                "error": str(e)
            }
        return {"hasil": hasil}

    def fetch_country_list_adsense(self, start_date, end_date, selected_account=None):
        try:
            base_sql = [
                "SELECT",
                "\tb.data_adsense_country_cd AS 'country_code',",
                "\tb.data_adsense_country_nm AS 'country_name'",
                "FROM",
                "\tapp_credentials a",
                "INNER JOIN data_adsense_country b ON a.account_id = b.account_id",
                "\tWHERE",
            ]
            params = []
            base_sql.append("b.data_adsense_country_tanggal BETWEEN %s AND %s")
            params.extend([start_date, end_date])
            if selected_account:
                base_sql.append(f"\tAND b.account_id LIKE %s")
                params.append(f"{selected_account}%")
            base_sql.append("GROUP BY b.data_adsense_country_cd, b.data_adsense_country_nm")
            base_sql.append("ORDER BY b.data_adsense_country_cd ASC")
            sql = "\n".join(base_sql)
            if not self.execute_query(sql, tuple(params)):
                raise pymysql.Error("Failed to get all adsense country list by params")
            data = self.fetch_all()
            if not self.commit():
                raise pymysql.Error("Failed to commit get all adsense country list by params")
            hasil = {
                "status": True,
                "message": "Data adsense country list berhasil diambil",
                "data": data
            }
        except pymysql.Error as e:
            hasil = {
                "status": False,
                "data": f"Terjadi error {e!r}, error nya {e.args[0]}"
            }
        except Exception as e:
            hasil = {
                "status": "error",
                "message": "Gagal mengambil data negara.",
                "error": str(e)
            }
        return {"hasil": hasil}

    def get_all_adx_roi_country_detail_by_params(self, start_date, end_date, selected_account_list = None, selected_domain_list = None, countries_list = None):
        try:
            if isinstance(selected_account_list, str):
                selected_account_list = [selected_account_list.strip()]
            elif selected_account_list is None:
                selected_account_list = []
            elif isinstance(selected_account_list, (set, tuple)):
                selected_account_list = list(selected_account_list)
            data_account_list = [str(a).strip() for a in selected_account_list if str(a).strip()]

            if isinstance(selected_domain_list, str):
                selected_domain_list = [selected_domain_list.strip()]
            elif selected_domain_list is None:
                selected_domain_list = []
            elif isinstance(selected_domain_list, (set, tuple)):
                selected_domain_list = list(selected_domain_list)

            def _norm_domain(v):
                s = str(v).strip()
                if not s:
                    return ''
                parts = [p for p in s.split('.') if p]
                return '.'.join(parts[:2]) if len(parts) >= 2 else s

            data_domain_list = []
            for d in selected_domain_list:
                nd = _norm_domain(d)
                if nd:
                    data_domain_list.append(nd)

            engine = (self._report_engine() or '').strip().lower()
            use_clickhouse = engine in ('clickhouse', 'ch')
            account_col = "toString(b.account_id)" if use_clickhouse else "b.account_id"
            site_expr = "concat(arrayElement(splitByChar('.', b.data_adx_country_domain), 1), '.', arrayElement(splitByChar('.', b.data_adx_country_domain), 2))" if use_clickhouse else "SUBSTRING_INDEX(b.data_adx_country_domain, '.', 2)"
            like_conditions_account = " OR ".join([f"{account_col} LIKE %s"] * len(data_account_list))
            like_params_account = [f"%{account}%" for account in data_account_list]
            like_conditions_domain = " OR ".join([f"{site_expr} LIKE %s"] * len(data_domain_list))
            like_params_domain = [f"%{domain}%" for domain in data_domain_list]
            if use_clickhouse:
                base_sql = [
                    "SELECT",
                    "\ttoDate(b.data_adx_country_tanggal) AS 'date',",
                    "\tb.data_adx_country_domain AS 'site_name',",
                    "\tb.data_adx_country_cd AS 'country_code',",
                    "\tb.data_adx_country_nm AS 'country_name',",
                    "\tSUM(b.data_adx_country_impresi) AS impressions,",
                    "\tSUM(b.data_adx_country_click) AS clicks,",
                    "\tCASE WHEN SUM(b.data_adx_country_impresi) > 0",
                    "\t\tTHEN ROUND((SUM(b.data_adx_country_click) / SUM(b.data_adx_country_impresi)) * 100, 2)",
                    "\t\tELSE 0 END AS ctr,",
                    "\tCASE WHEN SUM(b.data_adx_country_impresi) > 0",
                    "\t\tTHEN ROUND((SUM(b.data_adx_country_revenue) / SUM(b.data_adx_country_impresi)) * 1000)",
                    "\t\tELSE 0 END AS ecpm,",
                    "\tCASE WHEN SUM(b.data_adx_country_click) > 0",
                    "\t\tTHEN ROUND(SUM(b.data_adx_country_revenue) / SUM(b.data_adx_country_click), 0)",
                    "\t\tELSE 0 END AS cpc,",
                    "\tSUM(b.data_adx_country_revenue) AS 'revenue'",
                    "FROM data_adx_country b",
                    "WHERE",
                ]
            else:
                base_sql = [
                    "SELECT",
                    "\tb.data_adx_country_tanggal AS 'date',",
                    "\tb.data_adx_country_domain AS 'site_name',",
                    "\tb.data_adx_country_cd AS 'country_code',",
                    "\tb.data_adx_country_nm AS 'country_name',",
                    "\tSUM(b.data_adx_country_impresi) AS impressions,",
                    "\tSUM(b.data_adx_country_click) AS clicks,",
                    "\tCASE WHEN SUM(b.data_adx_country_impresi) > 0",
                    "\t\tTHEN ROUND((SUM(b.data_adx_country_click) / SUM(b.data_adx_country_impresi)) * 100, 2)",
                    "\t\tELSE 0 END AS ctr,",
                    "\tCASE WHEN SUM(b.data_adx_country_impresi) > 0",
                    "\t\tTHEN ROUND((SUM(b.data_adx_country_revenue) / SUM(b.data_adx_country_impresi)) * 1000)",
                    "\t\tELSE 0 END AS ecpm,",
                    "\tCASE WHEN SUM(b.data_adx_country_click) > 0",
                    "\t\tTHEN ROUND(SUM(b.data_adx_country_revenue) / SUM(b.data_adx_country_click), 0)",
                    "\t\tELSE 0 END AS cpc,",
                    "\tSUM(b.data_adx_country_revenue) AS 'revenue'",
                    "FROM app_credentials a",
                    "INNER JOIN data_adx_country b ON a.account_id = b.account_id",
                    "WHERE",
                ]

            params = []
            if use_clickhouse:
                base_sql.append("toDate(b.data_adx_country_tanggal) BETWEEN toDate(%s) AND toDate(%s)")
            else:
                base_sql.append("b.data_adx_country_tanggal BETWEEN %s AND %s")
            params.extend([start_date, end_date])

            if data_account_list:
                base_sql.append(f"\tAND ({like_conditions_account})")
                params.extend(like_params_account)

            if data_domain_list:
                base_sql.append(f"\tAND ({like_conditions_domain})")
                params.extend(like_params_domain)

            country_codes = []
            if countries_list:
                if isinstance(countries_list, str):
                    country_codes = [c.strip() for c in countries_list.split(',') if c.strip()]
                elif isinstance(countries_list, (list, tuple)):
                    country_codes = [str(c).strip() for c in countries_list if str(c).strip()]
            if country_codes:
                placeholders = ','.join(['%s'] * len(country_codes))
                base_sql.append(f"AND b.data_adx_country_cd IN ({placeholders})")
                params.extend(country_codes)

            if use_clickhouse:
                base_sql.append("GROUP BY toDate(b.data_adx_country_tanggal), b.data_adx_country_domain, b.data_adx_country_cd, b.data_adx_country_nm")
                base_sql.append("ORDER BY toDate(b.data_adx_country_tanggal) ASC")
            else:
                base_sql.append("GROUP BY b.data_adx_country_tanggal, b.data_adx_country_domain, b.data_adx_country_cd, b.data_adx_country_nm")
                base_sql.append("ORDER BY b.data_adx_country_tanggal ASC")
            sql = "\n".join(base_sql)

            if not self.execute_query(sql, tuple(params)):
                raise pymysql.Error("Failed to get adx country detail by params")
            data_rows = self.fetch_all()
            if not self.commit():
                raise pymysql.Error("Failed to commit adx country detail by params")
            return {
                "status": True,
                "message": "Detail AdX country berhasil diambil",
                "data": data_rows
            }
        except pymysql.Error as e:
            return {"status": False, "error": f"Terjadi error {e!r}, error nya {e.args[0]}"}
        # ... existing code ...

    def get_all_adsense_rekap_country_detail_by_params(self, start_date, end_date, selected_account_list = None, selected_domain_list = None, countries_list = None):
        # ... existing code ...
        try:
            if isinstance(selected_account_list, str):
                selected_account_list = [selected_account_list.strip()]
            elif selected_account_list is None:
                selected_account_list = []
            elif isinstance(selected_account_list, (set, tuple)):
                selected_account_list = list(selected_account_list)
            data_account_list = [str(a).strip() for a in selected_account_list if str(a).strip()]
            like_conditions_account = " OR ".join(["b.account_id LIKE %s"] * len(data_account_list))
            like_params_account = [f"%{account}%" for account in data_account_list]
            if isinstance(selected_domain_list, str):
                selected_domain_list = [selected_domain_list.strip()]
            elif selected_domain_list is None:
                selected_domain_list = []
            elif isinstance(selected_domain_list, (set, tuple)):
                selected_domain_list = list(selected_domain_list)
            data_domain_list = [str(d).strip() for d in selected_domain_list if str(d).strip()]
            like_conditions_domain = " OR ".join(["CONCAT(SUBSTRING_INDEX(b.data_adsense_country_domain, '.', 2), '.com') LIKE %s"] * len(data_domain_list))
            like_params_domain = [f"%{domain}%" for domain in data_domain_list]

            engine = (self._report_engine() or '').strip().lower()
            use_clickhouse = engine in ('clickhouse', 'ch')

            like_account_col = "b.account_id" if use_clickhouse else "a.account_id"
            like_conditions_account = " OR ".join([f"{like_account_col} LIKE %s"] * len(data_account_list))
            like_params_account = [f"%{account}%" for account in data_account_list]

            if use_clickhouse:
                site_expr = "concat(arrayElement(splitByChar('.', b.data_adsense_country_domain), 1), '.', arrayElement(splitByChar('.', b.data_adsense_country_domain), 2))"
                domain_filter_expr = f"concat({site_expr}, '.com')"
                like_conditions_domain = " OR ".join([f"{domain_filter_expr} LIKE %s"] * len(data_domain_list))
                like_params_domain = [f"%{domain}%" for domain in data_domain_list]
                base_sql = [
                    "SELECT",
                    "\ttoDate(b.data_adsense_country_tanggal) AS 'date',",
                    f"\t{site_expr} AS 'site_name',",
                    "\tb.data_adsense_country_cd AS 'country_code',",
                    "\tb.data_adsense_country_nm AS 'country_name',",
                    "\tSUM(b.data_adsense_country_impresi) AS impressions,",
                    "\tSUM(b.data_adsense_country_click) AS clicks,",
                    "\tCASE WHEN SUM(b.data_adsense_country_impresi) > 0",
                    "\t\tTHEN ROUND((SUM(b.data_adsense_country_click) / SUM(b.data_adsense_country_impresi)) * 100, 2)",
                    "\t\tELSE 0 END AS ctr,",
                    "\tCASE WHEN SUM(b.data_adsense_country_impresi) > 0",
                    "\t\tTHEN ROUND((SUM(b.data_adsense_country_revenue) / SUM(b.data_adsense_country_impresi)) * 1000)",
                    "\t\tELSE 0 END AS ecpm,",
                    "\tCASE WHEN SUM(b.data_adsense_country_click) > 0",
                    "\t\tTHEN ROUND(SUM(b.data_adsense_country_revenue) / SUM(b.data_adsense_country_click), 0)",
                    "\t\tELSE 0 END AS cpc,",
                    "\tSUM(b.data_adsense_country_revenue) AS 'revenue'",
                    "FROM data_adsense_country b",
                    "WHERE",
                ]
            else:
                like_conditions_domain = " OR ".join(["CONCAT(SUBSTRING_INDEX(b.data_adsense_country_domain, '.', 2), '.com') LIKE %s"] * len(data_domain_list))
                like_params_domain = [f"%{domain}%" for domain in data_domain_list]
                base_sql = [
                    "SELECT",
                    "\tb.data_adsense_country_tanggal AS 'date',",
                    "\tSUBSTRING_INDEX(b.data_adsense_country_domain, '.', 2) AS 'site_name',",
                    "\tb.data_adsense_country_cd AS 'country_code',",
                    "\tb.data_adsense_country_nm AS 'country_name',",
                    "\tSUM(b.data_adsense_country_impresi) AS impressions,",
                    "\tSUM(b.data_adsense_country_click) AS clicks,",
                    "\tCASE WHEN SUM(b.data_adsense_country_impresi) > 0",
                    "\t\tTHEN ROUND((SUM(b.data_adsense_country_click) / SUM(b.data_adsense_country_impresi)) * 100, 2)",
                    "\t\tELSE 0 END AS ctr,",
                    "\tCASE WHEN SUM(b.data_adsense_country_impresi) > 0",
                    "\t\tTHEN ROUND((SUM(b.data_adsense_country_revenue) / SUM(b.data_adsense_country_impresi)) * 1000)",
                    "\t\tELSE 0 END AS ecpm,",
                    "\tCASE WHEN SUM(b.data_adsense_country_click) > 0",
                    "\t\tTHEN ROUND(SUM(b.data_adsense_country_revenue) / SUM(b.data_adsense_country_click), 0)",
                    "\t\tELSE 0 END AS cpc,",
                    "\tSUM(b.data_adsense_country_revenue) AS 'revenue'",
                    "FROM app_credentials a",
                    "INNER JOIN data_adsense_country b ON a.account_id = b.account_id",
                    "WHERE",
                ]

            params = []
            if use_clickhouse:
                base_sql.append("toDate(b.data_adsense_country_tanggal) BETWEEN toDate(%s) AND toDate(%s)")
            else:
                base_sql.append("b.data_adsense_country_tanggal BETWEEN %s AND %s")
            params.extend([start_date, end_date])

            if data_account_list:
                base_sql.append(f"\tAND ({like_conditions_account})")
                params.extend(like_params_account)

            if data_domain_list:
                base_sql.append(f"\tAND ({like_conditions_domain})")
                params.extend(like_params_domain)

            country_codes = []
            if countries_list:
                if isinstance(countries_list, str):
                    country_codes = [c.strip() for c in countries_list.split(',') if c.strip()]
                elif isinstance(countries_list, (list, tuple)):
                    country_codes = [str(c).strip() for c in countries_list if str(c).strip()]
            if country_codes:
                placeholders = ','.join(['%s'] * len(country_codes))
                base_sql.append(f"AND b.data_adsense_country_cd IN ({placeholders})")
                params.extend(country_codes)

            if use_clickhouse:
                base_sql.append(f"GROUP BY toDate(b.data_adsense_country_tanggal), {site_expr}, b.data_adsense_country_cd, b.data_adsense_country_nm")
                base_sql.append("ORDER BY toDate(b.data_adsense_country_tanggal) ASC")
            else:
                base_sql.append("GROUP BY b.data_adsense_country_tanggal, SUBSTRING_INDEX(b.data_adsense_country_domain, '.', 2), b.data_adsense_country_cd, b.data_adsense_country_nm")
                base_sql.append("ORDER BY b.data_adsense_country_tanggal ASC")
            sql = "\n".join(base_sql)

            if not self.execute_query(sql, tuple(params)):
                raise pymysql.Error("Failed to get adsense country detail by params")
            data_rows = self.fetch_all()
            if not self.commit():
                raise pymysql.Error("Failed to commit adsense country detail by params")
            return {
                "status": True,
                "message": "Detail AdSense country berhasil diambil",
                "data": data_rows
            }
        except pymysql.Error as e:
            return {"status": False, "error": f"Terjadi error {e!r}, error nya {e.args[0]}"}
        # ... existing code ...

    def get_all_ads_roi_country_detail_by_params(self, start_date_formatted, end_date_formatted, data_sub_domain=None, countries_list=None):
        # ... existing code ...
        try:
            if isinstance(data_sub_domain, str):
                data_sub_domain = [s.strip() for s in data_sub_domain.split(",") if s.strip()]
            elif data_sub_domain is None:
                data_sub_domain = []
            elif isinstance(data_sub_domain, (set, tuple)):
                data_sub_domain = list(data_sub_domain)
            if not data_sub_domain:
                raise ValueError("data_sub_domain is required and cannot be empty")

            like_conditions = " OR ".join(["b.data_ads_domain LIKE %s"] * len(data_sub_domain))
            like_params = [f"%{d}%" for d in data_sub_domain]

            engine = (self._report_engine() or '').lower()
            use_clickhouse = engine in ('clickhouse', 'ch')

            if use_clickhouse:
                sql_parts = [
                    "SELECT",
                    "\ttoDate(b.data_ads_country_tanggal) AS 'date',",
                    "\tb.data_ads_country_cd AS 'country_code',",
                    "\tb.data_ads_country_nm AS 'country_name',",
                    "\tb.data_ads_domain AS 'domain',",
                    "\tSUM(b.data_ads_country_spend) AS 'spend',",
                    "\tSUM(b.data_ads_country_click) AS 'clicks',",
                    "\tSUM(b.data_ads_country_impresi) AS 'impressions',",
                    "\tROUND(AVG(b.data_ads_country_cpr), 0) AS 'cpr'",
                    "FROM data_ads_country b",
                    "WHERE toDate(b.data_ads_country_tanggal) BETWEEN toDate(%s) AND toDate(%s)",
                    f"\tAND ({like_conditions})",
                ]
            else:
                sql_parts = [
                    "SELECT",
                    "\tb.data_ads_country_tanggal AS 'date',",
                    "\tb.data_ads_country_cd AS 'country_code',",
                    "\tb.data_ads_country_nm AS 'country_name',",
                    "\tb.data_ads_domain AS 'domain',",
                    "\tSUM(b.data_ads_country_spend) AS 'spend',",
                    "\tSUM(b.data_ads_country_click) AS 'clicks',",
                    "\tSUM(b.data_ads_country_impresi) AS 'impressions',",
                    "\tROUND(AVG(b.data_ads_country_cpr), 0) AS 'cpr'",
                    "FROM master_account_ads a",
                    "INNER JOIN data_ads_country b ON a.account_id = b.account_ads_id",
                    "WHERE b.data_ads_country_tanggal BETWEEN %s AND %s",
                    f"\tAND ({like_conditions})",
                ]

            params = [start_date_formatted, end_date_formatted] + like_params

            country_codes = []
            if countries_list:
                if isinstance(countries_list, str):
                    country_codes = [c.strip() for c in countries_list.split(',') if c.strip()]
                elif isinstance(countries_list, (list, tuple)):
                    country_codes = [str(c).strip() for c in countries_list if str(c).strip()]
            if country_codes:
                placeholders = ','.join(['%s'] * len(country_codes))
                sql_parts.append(f"AND b.data_ads_country_cd IN ({placeholders})")
                params.extend(country_codes)

            if use_clickhouse:
                sql_parts.append("GROUP BY toDate(b.data_ads_country_tanggal), b.data_ads_country_cd, b.data_ads_country_nm, b.data_ads_domain")
                sql_parts.append("ORDER BY toDate(b.data_ads_country_tanggal) ASC")
            else:
                sql_parts.append("GROUP BY b.data_ads_country_tanggal, b.data_ads_country_cd, b.data_ads_country_nm, b.data_ads_domain")
                sql_parts.append("ORDER BY b.data_ads_country_tanggal ASC")
            sql = "\n".join(sql_parts)

            if not self.execute_query(sql, tuple(params)):
                raise pymysql.Error("Failed to get ads country detail by params")
            data = self.fetch_all()
            if not self.commit():
                raise pymysql.Error("Failed to commit ads country detail by params")

            hasil = {"status": True, "message": "Detail Ads country berhasil diambil", "data": data}
        except Exception as e:
            hasil = {"status": False, "data": f"Terjadi error {e!r}"}
        return {"hasil": hasil}

    def get_all_ads_roi_country_detail_adsense_by_params(self, start_date_formatted, end_date_formatted, data_sub_domain=None, countries_list=None):
        # ... existing code ...
        try:
            if isinstance(data_sub_domain, str):
                data_sub_domain = [s.strip() for s in data_sub_domain.split(",") if s.strip()]
            elif data_sub_domain is None:
                data_sub_domain = []
            elif isinstance(data_sub_domain, (set, tuple)):
                data_sub_domain = list(data_sub_domain)
            if not data_sub_domain:
                raise ValueError("data_sub_domain is required and cannot be empty")
            like_conditions = " OR ".join(["SUBSTRING_INDEX(b.data_ads_domain, '.', 2) LIKE %s"] * len(data_sub_domain))
            like_params = [f"%{d}%" for d in data_sub_domain]
            sql_parts = [
                "SELECT",
                "\tb.data_ads_country_tanggal AS 'date',",
                "\tb.data_ads_country_cd AS 'country_code',",
                "\tb.data_ads_country_nm AS 'country_name',",
                "\tSUBSTRING_INDEX(b.data_ads_domain, '.', 2) AS 'domain',",
                "\tSUM(b.data_ads_country_spend) AS 'spend',",
                "\tSUM(b.data_ads_country_click) AS 'clicks',",
                "\tSUM(b.data_ads_country_impresi) AS 'impressions',",
                "\tROUND(AVG(b.data_ads_country_cpr), 0) AS 'cpr'",
                "FROM master_account_ads a",
                "INNER JOIN data_ads_country b ON a.account_id = b.account_ads_id",
                "WHERE b.data_ads_country_tanggal BETWEEN %s AND %s",
                f"\tAND ({like_conditions})",
            ]
            params = [start_date_formatted, end_date_formatted] + like_params
            country_codes = []
            if countries_list:
                if isinstance(countries_list, str):
                    country_codes = [c.strip() for c in countries_list.split(',') if c.strip()]
                elif isinstance(countries_list, (list, tuple)):
                    country_codes = [str(c).strip() for c in countries_list if str(c).strip()]
            if country_codes:
                placeholders = ','.join(['%s'] * len(country_codes))
                sql_parts.append(f"AND b.data_ads_country_cd IN ({placeholders})")
                params.extend(country_codes)
            sql_parts.append("GROUP BY b.data_ads_country_tanggal, b.data_ads_country_cd, b.data_ads_country_nm, SUBSTRING_INDEX(b.data_ads_domain, '.', 2)")
            sql_parts.append("ORDER BY b.data_ads_country_tanggal ASC")
            sql = "\n".join(sql_parts)
            if not self.execute_query(sql, tuple(params)):
                raise pymysql.Error("Failed to get ads country detail by params")
            data = self.fetch_all()
            if not self.commit():
                raise pymysql.Error("Failed to commit ads country detail by params")
            hasil = {"status": True, "message": "Detail Ads country berhasil diambil", "data": data}
        except Exception as e:
            hasil = {"status": False, "data": f"Terjadi error {e!r}"}
        return {"hasil": hasil}


    def get_all_adx_country_detail_by_params(self, start_date, end_date, selected_account_list = None, selected_domain_list = None, countries_list = None):
        try:
             # --- 0. Pastikan selected_account_list adalah list string
            if isinstance(selected_account_list, str):
                selected_account_list = [selected_account_list.strip()]
            elif selected_account_list is None:
                selected_account_list = []
            elif isinstance(selected_account_list, (set, tuple)):
                selected_account_list = list(selected_account_list)
            data_account_list = [str(a).strip() for a in selected_account_list if str(a).strip()]

            engine = (self._report_engine() or '').strip().lower()
            use_clickhouse = engine in ('clickhouse', 'ch')

            like_account_col = "b.account_id" if use_clickhouse else "a.account_id"
            like_conditions_account = " OR ".join([f"{like_account_col} LIKE %s"] * len(data_account_list))
            like_params_account = [f"%{account}%" for account in data_account_list] 

            if isinstance(selected_domain_list, str):
                selected_domain_list = [selected_domain_list.strip()]
            elif selected_domain_list is None:
                selected_domain_list = []
            elif isinstance(selected_domain_list, (set, tuple)):
                selected_domain_list = list(selected_domain_list)
            data_domain_list = [str(d).strip() for d in selected_domain_list if str(d).strip()]
            like_conditions_domain = " OR ".join(["b.data_adx_country_domain LIKE %s"] * len(data_domain_list))
            like_params_domain = [f"%{domain}%" for domain in data_domain_list]

            if use_clickhouse:
                base_sql = [
                    "SELECT",
                    "\ttoDate(b.data_adx_country_tanggal) AS 'date',",
                    "\tb.data_adx_country_domain AS 'site_name',",
                    "\tb.data_adx_country_cd AS 'country_code',",
                    "\tb.data_adx_country_nm AS 'country_name',",
                    "\tSUM(b.data_adx_country_revenue) AS 'revenue'",
                    "FROM data_adx_country b",
                    "WHERE",
                ]
                params = []
                base_sql.append("toDate(b.data_adx_country_tanggal) BETWEEN toDate(%s) AND toDate(%s)")
                params.extend([start_date, end_date])
            else:
                base_sql = [
                    "SELECT",
                    "\tb.data_adx_country_tanggal AS 'date',",
                    "\tb.data_adx_country_domain AS 'site_name',",
                    "\tb.data_adx_country_cd AS 'country_code',",
                    "\tb.data_adx_country_nm AS 'country_name',",
                    "\tSUM(b.data_adx_country_revenue) AS 'revenue'",
                    "FROM app_credentials a",
                    "INNER JOIN data_adx_country b ON a.account_id = b.account_id",
                    "WHERE",
                ]
                params = []
                base_sql.append("b.data_adx_country_tanggal BETWEEN %s AND %s")
                params.extend([start_date, end_date])

            if data_account_list:
                base_sql.append(f"\tAND ({like_conditions_account})")
                params.extend(like_params_account)

            if data_domain_list:
                base_sql.append(f"\tAND ({like_conditions_domain})")
                params.extend(like_params_domain)

            country_codes = []
            if countries_list:
                if isinstance(countries_list, str):
                    country_codes = [c.strip() for c in countries_list.split(',') if c.strip()]
                elif isinstance(countries_list, (list, tuple)):
                    country_codes = [str(c).strip() for c in countries_list if str(c).strip()]
            if country_codes:
                placeholders = ','.join(['%s'] * len(country_codes))
                base_sql.append(f"AND b.data_adx_country_cd IN ({placeholders})")
                params.extend(country_codes)

            if use_clickhouse:
                base_sql.append("GROUP BY toDate(b.data_adx_country_tanggal), b.data_adx_country_domain, b.data_adx_country_cd, b.data_adx_country_nm")
                base_sql.append("ORDER BY toDate(b.data_adx_country_tanggal) ASC")
            else:
                base_sql.append("GROUP BY b.data_adx_country_tanggal, b.data_adx_country_domain, b.data_adx_country_cd, b.data_adx_country_nm")
                base_sql.append("ORDER BY b.data_adx_country_tanggal ASC")
            sql = "\n".join(base_sql)

            if not self.execute_query(sql, tuple(params)):
                raise pymysql.Error("Failed to get adx country detail by params")
            data_rows = self.fetch_all()
            if not self.commit():
                raise pymysql.Error("Failed to commit adx country detail by params")
            return {
                "status": True,
                "message": "Detail AdX country berhasil diambil",
                "data": data_rows
            }
        except pymysql.Error as e:
            return {"status": False, "error": f"Terjadi error {e!r}, error nya {e.args[0]}"}
        # ... existing code ...

    def get_all_log_adx_country_detail_by_params(self, start_date, end_date, selected_account_list=None, selected_domain_list=None, countries_list=None):
        try:
            if isinstance(selected_account_list, str):
                selected_account_list = [selected_account_list.strip()]
            elif selected_account_list is None:
                selected_account_list = []
            elif isinstance(selected_account_list, (set, tuple)):
                selected_account_list = list(selected_account_list)
            data_account_list = [str(a).strip() for a in selected_account_list if str(a).strip()]

            if isinstance(selected_domain_list, str):
                selected_domain_list = [selected_domain_list.strip()]
            elif selected_domain_list is None:
                selected_domain_list = []
            elif isinstance(selected_domain_list, (set, tuple)):
                selected_domain_list = list(selected_domain_list)
            data_domain_list = [str(d).strip() for d in selected_domain_list if str(d).strip()]

            engine = (self._report_engine() or '').strip().lower()
            use_clickhouse = engine in ('clickhouse', 'ch')

            like_account_col = "b.account_id"
            like_conditions_account = " OR ".join([f"{like_account_col} LIKE %s"] * len(data_account_list))
            like_params_account = [f"%{account}%" for account in data_account_list]

            like_conditions_domain = " OR ".join(["b.log_adx_country_domain LIKE %s"] * len(data_domain_list))
            like_params_domain = [f"%{domain}%" for domain in data_domain_list]

            if use_clickhouse:
                base_sql = [
                    "SELECT",
                    "\ttoDate(b.log_adx_country_tanggal) AS 'date',",
                    "\tb.log_adx_country_domain AS 'site_name',",
                    "\tb.log_adx_country_cd AS 'country_code',",
                    "\tb.log_adx_country_nm AS 'country_name',",
                    "\tSUM(b.log_adx_country_revenue) AS 'revenue'",
                    "FROM log_adx_country b",
                    "WHERE",
                    "\ttoDate(b.log_adx_country_tanggal) BETWEEN toDate(%s) AND toDate(%s)",
                ]
            else:
                base_sql = [
                    "SELECT",
                    "\tb.log_adx_country_tanggal AS 'date',",
                    "\tb.log_adx_country_domain AS 'site_name',",
                    "\tb.log_adx_country_cd AS 'country_code',",
                    "\tb.log_adx_country_nm AS 'country_name',",
                    "\tSUM(b.log_adx_country_revenue) AS 'revenue'",
                    "FROM log_adx_country b",
                    "WHERE",
                    "\tb.log_adx_country_tanggal BETWEEN %s AND %s",
                ]

            params = [start_date, end_date]

            if data_account_list:
                base_sql.append(f"\tAND ({like_conditions_account})")
                params.extend(like_params_account)

            if data_domain_list:
                base_sql.append(f"\tAND ({like_conditions_domain})")
                params.extend(like_params_domain)

            country_codes = []
            if countries_list:
                if isinstance(countries_list, str):
                    country_codes = [c.strip() for c in countries_list.split(',') if c.strip()]
                elif isinstance(countries_list, (list, tuple)):
                    country_codes = [str(c).strip() for c in countries_list if str(c).strip()]
            if country_codes:
                placeholders = ','.join(['%s'] * len(country_codes))
                base_sql.append(f"AND b.log_adx_country_cd IN ({placeholders})")
                params.extend(country_codes)

            if use_clickhouse:
                base_sql.append("GROUP BY toDate(b.log_adx_country_tanggal), b.log_adx_country_domain, b.log_adx_country_cd, b.log_adx_country_nm")
                base_sql.append("ORDER BY toDate(b.log_adx_country_tanggal) ASC")
            else:
                base_sql.append("GROUP BY b.log_adx_country_tanggal, b.log_adx_country_domain, b.log_adx_country_cd, b.log_adx_country_nm")
                base_sql.append("ORDER BY b.log_adx_country_tanggal ASC")

            sql = "\n".join(base_sql)
            if not self.execute_query(sql, tuple(params)):
                raise pymysql.Error("Failed to get log adx country detail by params")
            data_rows = self.fetch_all()
            if not self.commit():
                raise pymysql.Error("Failed to commit log adx country detail by params")
            return {
                "status": True,
                "message": "Detail log AdX country berhasil diambil",
                "data": data_rows,
            }
        except pymysql.Error as e:
            return {"status": False, "error": f"Terjadi error {e!r}, error nya {e.args[0]}"}
        except Exception as e:
            return {"status": False, "error": f"Terjadi error {e!r}"}

    def get_all_log_adsense_country_detail_by_params(self, start_date, end_date, selected_account_list=None, selected_domain_list=None, countries_list=None):
        try:
            if isinstance(selected_account_list, str):
                selected_account_list = [selected_account_list.strip()]
            elif selected_account_list is None:
                selected_account_list = []
            elif isinstance(selected_account_list, (set, tuple)):
                selected_account_list = list(selected_account_list)
            data_account_list = [str(a).strip() for a in selected_account_list if str(a).strip()]

            if isinstance(selected_domain_list, str):
                selected_domain_list = [selected_domain_list.strip()]
            elif selected_domain_list is None:
                selected_domain_list = []
            elif isinstance(selected_domain_list, (set, tuple)):
                selected_domain_list = list(selected_domain_list)
            data_domain_list = [str(d).strip() for d in selected_domain_list if str(d).strip()]

            engine = (self._report_engine() or '').strip().lower()
            use_clickhouse = engine in ('clickhouse', 'ch')

            like_account_col = "b.account_id"
            like_conditions_account = " OR ".join([f"{like_account_col} LIKE %s"] * len(data_account_list))
            like_params_account = [f"%{account}%" for account in data_account_list]

            like_conditions_domain = " OR ".join(["b.log_adsense_country_domain LIKE %s"] * len(data_domain_list))
            like_params_domain = [f"%{domain}%" for domain in data_domain_list]

            site_expr = (
                "concat(arrayElement(splitByChar('.', b.log_adsense_country_domain), 1), '.', arrayElement(splitByChar('.', b.log_adsense_country_domain), 2))"
                if use_clickhouse
                else "SUBSTRING_INDEX(b.log_adsense_country_domain, '.', 2)"
            )

            if use_clickhouse:
                base_sql = [
                    "SELECT",
                    "\ttoDate(b.log_adsense_country_tanggal) AS 'date',",
                    f"\t{site_expr} AS 'site_name',",
                    "\tb.log_adsense_country_cd AS 'country_code',",
                    "\tb.log_adsense_country_nm AS 'country_name',",
                    "\tSUM(b.log_adsense_country_revenue) AS 'revenue'",
                    "FROM log_adsense_country b",
                    "WHERE",
                    "\ttoDate(b.log_adsense_country_tanggal) BETWEEN toDate(%s) AND toDate(%s)",
                ]
            else:
                base_sql = [
                    "SELECT",
                    "\tb.log_adsense_country_tanggal AS 'date',",
                    f"\t{site_expr} AS 'site_name',",
                    "\tb.log_adsense_country_cd AS 'country_code',",
                    "\tb.log_adsense_country_nm AS 'country_name',",
                    "\tSUM(b.log_adsense_country_revenue) AS 'revenue'",
                    "FROM log_adsense_country b",
                    "WHERE",
                    "\tb.log_adsense_country_tanggal BETWEEN %s AND %s",
                ]

            params = [start_date, end_date]

            if data_account_list:
                base_sql.append(f"\tAND ({like_conditions_account})")
                params.extend(like_params_account)

            if data_domain_list:
                base_sql.append(f"\tAND ({like_conditions_domain})")
                params.extend(like_params_domain)

            country_codes = []
            if countries_list:
                if isinstance(countries_list, str):
                    country_codes = [c.strip() for c in countries_list.split(',') if c.strip()]
                elif isinstance(countries_list, (list, tuple)):
                    country_codes = [str(c).strip() for c in countries_list if str(c).strip()]
            if country_codes:
                placeholders = ','.join(['%s'] * len(country_codes))
                base_sql.append(f"AND b.log_adsense_country_cd IN ({placeholders})")
                params.extend(country_codes)

            if use_clickhouse:
                base_sql.append(f"GROUP BY toDate(b.log_adsense_country_tanggal), {site_expr}, b.log_adsense_country_cd, b.log_adsense_country_nm")
                base_sql.append("ORDER BY toDate(b.log_adsense_country_tanggal) ASC")
            else:
                base_sql.append(f"GROUP BY b.log_adsense_country_tanggal, {site_expr}, b.log_adsense_country_cd, b.log_adsense_country_nm")
                base_sql.append("ORDER BY b.log_adsense_country_tanggal ASC")

            sql = "\n".join(base_sql)
            if not self.execute_query(sql, tuple(params)):
                raise pymysql.Error("Failed to get log adsense country detail by params")
            data_rows = self.fetch_all()
            if not self.commit():
                raise pymysql.Error("Failed to commit log adsense country detail by params")
            return {
                "status": True,
                "message": "Detail log AdSense country berhasil diambil",
                "data": data_rows,
            }
        except pymysql.Error as e:
            return {"status": False, "error": f"Terjadi error {e!r}, error nya {e.args[0]}"}
        except Exception as e:
            return {"status": False, "error": f"Terjadi error {e!r}"}

    def get_all_log_ads_country_detail_by_params(self, start_date_formatted, end_date_formatted, data_sub_domain=None, countries_list=None):
        try:
            if isinstance(data_sub_domain, str):
                data_sub_domain = [s.strip() for s in data_sub_domain.split(",") if s.strip()]
            elif data_sub_domain is None:
                data_sub_domain = []
            elif isinstance(data_sub_domain, (set, tuple)):
                data_sub_domain = list(data_sub_domain)

            uniq = []
            seen = set()
            for d in (data_sub_domain or []):
                s = str(d or '').strip()
                if not s or s in seen:
                    continue
                seen.add(s)
                uniq.append(s)
            data_sub_domain = uniq

            if not data_sub_domain:
                raise ValueError("data_sub_domain is required and cannot be empty")

            try:
                batch_size = int(os.getenv('ROI_FB_DOMAIN_BATCH', '80') or 80)
            except Exception:
                batch_size = 80
            if batch_size < 1:
                batch_size = 80

            engine = (self._report_engine() or '').strip().lower()
            use_clickhouse = engine in ('clickhouse', 'ch')

            country_codes = []
            if countries_list:
                if isinstance(countries_list, str):
                    country_codes = [c.strip() for c in countries_list.split(',') if c.strip()]
                elif isinstance(countries_list, (list, tuple)):
                    country_codes = [str(c).strip() for c in countries_list if str(c).strip()]

            def run_one(domains):
                like_conditions = " OR ".join(["b.log_ads_domain LIKE %s"] * len(domains))
                like_params = [f"%{d}%" for d in domains]

                if use_clickhouse:
                    sql_parts = [
                        "SELECT",
                        "\ttoDate(b.log_ads_country_tanggal) AS 'date',",
                        "\tb.log_ads_country_cd AS 'country_code',",
                        "\tb.log_ads_country_nm AS 'country_name',",
                        "\tb.log_ads_domain AS 'domain',",
                        "\tSUM(b.log_ads_country_spend) AS 'spend',",
                        "\tSUM(b.log_ads_country_click) AS 'clicks',",
                        "\tSUM(b.log_ads_country_impresi) AS 'impressions',",
                        "\tROUND(AVG(b.log_ads_country_cpr), 0) AS 'cpr'",
                        "FROM log_ads_country b",
                        "WHERE toDate(b.log_ads_country_tanggal) BETWEEN toDate(%s) AND toDate(%s)",
                        f"\tAND ({like_conditions})",
                    ]
                else:
                    sql_parts = [
                        "SELECT",
                        "\tb.log_ads_country_tanggal AS 'date',",
                        "\tb.log_ads_country_cd AS 'country_code',",
                        "\tb.log_ads_country_nm AS 'country_name',",
                        "\tb.log_ads_domain AS 'domain',",
                        "\tSUM(b.log_ads_country_spend) AS 'spend',",
                        "\tSUM(b.log_ads_country_click) AS 'clicks',",
                        "\tSUM(b.log_ads_country_impresi) AS 'impressions',",
                        "\tROUND(AVG(b.log_ads_country_cpr), 0) AS 'cpr'",
                        "FROM log_ads_country b",
                        "WHERE b.log_ads_country_tanggal BETWEEN %s AND %s",
                        f"\tAND ({like_conditions})",
                    ]

                params = [start_date_formatted, end_date_formatted] + like_params

                if country_codes:
                    placeholders = ','.join(['%s'] * len(country_codes))
                    sql_parts.append(f"AND b.log_ads_country_cd IN ({placeholders})")
                    params.extend(country_codes)

                sql_parts.append("GROUP BY b.log_ads_country_tanggal, b.log_ads_country_cd, b.log_ads_country_nm, b.log_ads_domain")
                sql_parts.append("ORDER BY b.log_ads_country_tanggal ASC")
                sql = "\n".join(sql_parts)

                if not self.execute_query(sql, tuple(params)):
                    raise pymysql.Error("Failed to get log ads country detail by params")
                return self.fetch_all() or []

            if len(data_sub_domain) > batch_size:
                out = {}
                order = []
                for i in range(0, len(data_sub_domain), batch_size):
                    batch = data_sub_domain[i:i + batch_size]
                    batch_rows = run_one(batch) or []
                    for r in (batch_rows or []):
                        if not isinstance(r, dict):
                            continue
                        k = (
                            str(r.get('date') or '').strip(),
                            str(r.get('country_code') or '').strip(),
                            str(r.get('country_name') or '').strip(),
                            str(r.get('domain') or '').strip(),
                        )
                        cur = out.get(k)
                        if not cur:
                            out[k] = r
                            order.append(k)
                            continue
                        for f in ('spend', 'clicks', 'impressions'):
                            try:
                                cur[f] = max(float(cur.get(f) or 0), float(r.get(f) or 0))
                            except Exception:
                                pass
                        if cur.get('cpr') in (None, '') and r.get('cpr') not in (None, ''):
                            cur['cpr'] = r.get('cpr')
                data = [out[k] for k in order]
            else:
                data = run_one(data_sub_domain) or []

            if not self.commit():
                raise pymysql.Error("Failed to commit log ads country detail by params")

            hasil = {"status": True, "message": "Detail log Ads country berhasil diambil", "data": data}
        except pymysql.Error as e:
            hasil = {"status": False, "data": f"Terjadi error {e!r}, error nya {e.args[0]}"}
        except Exception as e:
            hasil = {"status": False, "data": f"Terjadi error {e!r}"}
        return {"hasil": hasil}

    def get_all_adsense_country_detail_by_params(self, start_date, end_date, selected_account_list = None, selected_domain_list = None, countries_list = None):
        # ... existing code ...
        try:
             # --- 0. Pastikan selected_account_list adalah list string
            if isinstance(selected_account_list, str):
                selected_account_list = [selected_account_list.strip()]
            elif selected_account_list is None:
                selected_account_list = []
            elif isinstance(selected_account_list, (set, tuple)):
                selected_account_list = list(selected_account_list)
            data_account_list = [str(a).strip() for a in selected_account_list if str(a).strip()]

            engine = (self._report_engine() or '').strip().lower()
            use_clickhouse = engine in ('clickhouse', 'ch')

            like_account_col = "b.account_id" if use_clickhouse else "a.account_id"
            like_conditions_account = " OR ".join([f"{like_account_col} LIKE %s"] * len(data_account_list))
            like_params_account = [f"%{account}%" for account in data_account_list]

            if isinstance(selected_domain_list, str):
                selected_domain_list = [selected_domain_list.strip()]
            elif selected_domain_list is None:
                selected_domain_list = []
            elif isinstance(selected_domain_list, (set, tuple)):
                selected_domain_list = list(selected_domain_list)
            data_domain_list = [str(d).strip() for d in selected_domain_list if str(d).strip()]
            like_conditions_domain = " OR ".join(["b.data_adsense_country_domain LIKE %s"] * len(data_domain_list))
            like_params_domain = [f"%{domain}%" for domain in data_domain_list]

            if use_clickhouse:
                base_sql = [
                    "SELECT",
                    "\ttoDate(b.data_adsense_country_tanggal) AS 'date',",
                    "\tSUBSTRING_INDEX(b.data_adsense_country_domain, '.', 2) AS 'site_name',",
                    "\tb.data_adsense_country_cd AS 'country_code',",
                    "\tb.data_adsense_country_nm AS 'country_name',",
                    "\tSUM(b.data_adsense_country_revenue) AS 'revenue'",
                    "FROM data_adsense_country b",
                    "WHERE",
                    "\ttoDate(b.data_adsense_country_tanggal) BETWEEN toDate(%s) AND toDate(%s)",
                ]
            else:
                base_sql = [
                    "SELECT",
                    "\tb.data_adsense_country_tanggal AS 'date',",
                    "\tSUBSTRING_INDEX(b.data_adsense_country_domain, '.', 2) AS 'site_name',",
                    "\tb.data_adsense_country_cd AS 'country_code',",
                    "\tb.data_adsense_country_nm AS 'country_name',",
                    "\tSUM(b.data_adsense_country_revenue) AS 'revenue'",
                    "FROM app_credentials a",
                    "INNER JOIN data_adsense_country b ON a.account_id = b.account_id",
                    "WHERE",
                    "\tb.data_adsense_country_tanggal BETWEEN %s AND %s",
                ]

            params = [start_date, end_date]
            if data_account_list:
                base_sql.append(f"\tAND ({like_conditions_account})")
                params.extend(like_params_account)
            if data_domain_list:
                base_sql.append(f"\tAND ({like_conditions_domain})")
                params.extend(like_params_domain)
            country_codes = []
            if countries_list:
                if isinstance(countries_list, str):
                    country_codes = [c.strip() for c in countries_list.split(',') if c.strip()]
                elif isinstance(countries_list, (list, tuple)):
                    country_codes = [str(c).strip() for c in countries_list if str(c).strip()]
            if country_codes:
                placeholders = ','.join(['%s'] * len(country_codes))
                base_sql.append(f"AND b.data_adsense_country_cd IN ({placeholders})")
                params.extend(country_codes)

            if use_clickhouse:
                base_sql.append("GROUP BY toDate(b.data_adsense_country_tanggal), SUBSTRING_INDEX(b.data_adsense_country_domain, '.', 2), b.data_adsense_country_cd, b.data_adsense_country_nm")
                base_sql.append("ORDER BY toDate(b.data_adsense_country_tanggal) ASC")
            else:
                base_sql.append("GROUP BY b.data_adsense_country_tanggal, SUBSTRING_INDEX(b.data_adsense_country_domain, '.', 2), b.data_adsense_country_cd, b.data_adsense_country_nm")
                base_sql.append("ORDER BY b.data_adsense_country_tanggal ASC")
            sql = "\n".join(base_sql)

            if not self.execute_query(sql, tuple(params)):
                raise pymysql.Error("Failed to get adsense country detail by params")   
            data_rows = self.fetch_all()
            if not self.commit():
                raise pymysql.Error("Failed to commit adsense country detail by params")
            return {
                "status": True,
                "message": "Detail AdSense country berhasil diambil",
                "data": data_rows
            }
        except pymysql.Error as e:
            return {"status": False, "error": f"Terjadi error {e!r}, error nya {e.args[0]}"}
        # ... existing code ...

    def get_all_ads_country_detail_by_params(self, start_date_formatted, end_date_formatted, data_sub_domain=None, countries_list=None):
        # ... existing code ...
        try:
            if isinstance(data_sub_domain, str):
                data_sub_domain = [s.strip() for s in data_sub_domain.split(",") if s.strip()]
            elif data_sub_domain is None:
                data_sub_domain = []
            elif isinstance(data_sub_domain, (set, tuple)):
                data_sub_domain = list(data_sub_domain)
            if not data_sub_domain:
                raise ValueError("data_sub_domain is required and cannot be empty")

            like_conditions = " OR ".join(["b.data_ads_domain LIKE %s"] * len(data_sub_domain))
            like_params = [f"%{d}%" for d in data_sub_domain]

            engine = (self._report_engine() or '').strip().lower()
            use_clickhouse = engine in ('clickhouse', 'ch')

            if use_clickhouse:
                sql_parts = [
                    "SELECT",
                    "\ttoDate(b.data_ads_country_tanggal) AS 'date',",
                    "\tb.data_ads_country_cd AS 'country_code',",
                    "\tb.data_ads_country_nm AS 'country_name',",
                    "\tb.data_ads_domain AS 'domain',",
                    "\tSUM(b.data_ads_country_spend) AS 'spend',",
                    "\tSUM(b.data_ads_country_click) AS 'clicks',",
                    "\tSUM(b.data_ads_country_impresi) AS 'impressions',",
                    "\tROUND(AVG(b.data_ads_country_cpr), 0) AS 'cpr'",
                    "FROM data_ads_country b",
                    "WHERE toDate(b.data_ads_country_tanggal) BETWEEN toDate(%s) AND toDate(%s)",
                    f"\tAND ({like_conditions})",
                ]
            else:
                sql_parts = [
                    "SELECT",
                    "\tb.data_ads_country_tanggal AS 'date',",
                    "\tb.data_ads_country_cd AS 'country_code',",
                    "\tb.data_ads_country_nm AS 'country_name',",
                    "\tb.data_ads_domain AS 'domain',",
                    "\tSUM(b.data_ads_country_spend) AS 'spend',",
                    "\tSUM(b.data_ads_country_click) AS 'clicks',",
                    "\tSUM(b.data_ads_country_impresi) AS 'impressions',",
                    "\tROUND(AVG(b.data_ads_country_cpr), 0) AS 'cpr'",
                    "FROM master_account_ads a",
                    "INNER JOIN data_ads_country b ON a.account_id = b.account_ads_id",
                    "WHERE b.data_ads_country_tanggal BETWEEN %s AND %s",
                    f"\tAND ({like_conditions})",
                ]

            params = [start_date_formatted, end_date_formatted] + like_params

            country_codes = []
            if countries_list:
                if isinstance(countries_list, str):
                    country_codes = [c.strip() for c in countries_list.split(',') if c.strip()]
                elif isinstance(countries_list, (list, tuple)):
                    country_codes = [str(c).strip() for c in countries_list if str(c).strip()]
            if country_codes:
                placeholders = ','.join(['%s'] * len(country_codes))
                sql_parts.append(f"AND b.data_ads_country_cd IN ({placeholders})")
                params.extend(country_codes)

            sql_parts.append("GROUP BY b.data_ads_country_tanggal, b.data_ads_country_cd, b.data_ads_country_nm, b.data_ads_domain")
            sql_parts.append("ORDER BY b.data_ads_country_tanggal ASC")
            sql = "\n".join(sql_parts)

            if not self.execute_query(sql, tuple(params)):
                raise pymysql.Error("Failed to get ads country detail by params")
            data = self.fetch_all()
            if not self.commit():
                raise pymysql.Error("Failed to commit ads country detail by params")

            hasil = {"status": True, "message": "Detail Ads country berhasil diambil", "data": data}
        except Exception as e:
            hasil = {"status": False, "data": f"Terjadi error {e!r}"}
        return {"hasil": hasil}
    def get_all_ads_adsense_country_detail_by_params(self, start_date_formatted, end_date_formatted, data_sub_domain=None, countries_list=None):
        try:
            if isinstance(data_sub_domain, str):
                data_sub_domain = [s.strip() for s in data_sub_domain.split(",") if s.strip()]
            elif isinstance(data_sub_domain, (set, tuple)):
                data_sub_domain = list(data_sub_domain)
            elif data_sub_domain is None:
                data_sub_domain = []
            if not data_sub_domain:
                raise ValueError("data_sub_domain is required and cannot be empty")

            engine = (self._report_engine() or '').strip().lower()
            use_clickhouse = engine in ('clickhouse', 'ch')

            domain_expr = "concat(arrayElement(splitByChar('.', b.data_ads_domain), 1), '.', arrayElement(splitByChar('.', b.data_ads_domain), 2))" if use_clickhouse else "SUBSTRING_INDEX(b.data_ads_domain, '.', 2)"

            like_conditions = " OR ".join([f"{domain_expr} LIKE %s"] * len(data_sub_domain))
            like_params = [f"%{d}%" for d in data_sub_domain]

            country_codes = []
            if countries_list:
                if isinstance(countries_list, str):
                    country_codes = [c.strip() for c in countries_list.split(',') if c.strip()]
                elif isinstance(countries_list, (list, tuple)):
                    country_codes = [str(c).strip() for c in countries_list if str(c).strip()]

            country_sql = ""
            country_params = []
            if country_codes:
                placeholders = ",".join(["%s"] * len(country_codes))
                country_sql = f" AND b.data_ads_country_cd IN ({placeholders})"
                country_params = country_codes

            if use_clickhouse:
                sql_parts = [
                    "SELECT",
                    "\ttoDate(b.data_ads_country_tanggal) AS date,",
                    "\tb.data_ads_country_cd AS country_code,",
                    "\tb.data_ads_country_nm AS country_name,",
                    f"\t{domain_expr} AS domain,",
                    "\tSUM(b.data_ads_country_spend) AS spend,",
                    "\tSUM(b.data_ads_country_click) AS clicks,",
                    "\tSUM(b.data_ads_country_impresi) AS impressions,",
                    "\tROUND(AVG(b.data_ads_country_cpr), 0) AS cpr",
                    "FROM data_ads_country b",
                    "WHERE toDate(b.data_ads_country_tanggal) BETWEEN toDate(%s) AND toDate(%s)",
                    f"\tAND ({like_conditions})",
                    f"\t{country_sql}",
                    "GROUP BY",
                    "\ttoDate(b.data_ads_country_tanggal),",
                    "\tb.data_ads_country_cd,",
                    "\tb.data_ads_country_nm,",
                    f"\t{domain_expr}",
                    "ORDER BY date ASC"
                ]
                sql = "\n".join(sql_parts)
                params = [start_date_formatted, end_date_formatted] + like_params + country_params
            else:
                sql_parts = [
                    "SELECT",
                    "\trs.date,",
                    "\trs.country_code,",
                    "\trs.country_name,",
                    "\trs.domain AS domain,",
                    "\tSUM(rs.spend) AS spend,",
                    "\tSUM(rs.clicks) AS clicks,",
                    "\tSUM(rs.impressions) AS impressions,",
                    "\tROUND(AVG(rs.cpr), 0) AS cpr",
                    "FROM ("
                ]
                sql_parts.extend([
                        "\tSELECT",
                        "\t\tb.data_ads_country_tanggal AS date,",
                        "\t\tb.data_ads_country_cd AS country_code,",
                        "\t\tb.data_ads_country_nm AS country_name,",
                        "\t\tSUBSTRING_INDEX(b.data_ads_domain, '.', 2) AS domain,",
                        "\t\tSUM(b.data_ads_country_spend) AS spend,",
                        "\t\tSUM(b.data_ads_country_click) AS clicks,",
                        "\t\tSUM(b.data_ads_country_impresi) AS impressions,",
                        "\t\tROUND(AVG(b.data_ads_country_cpr), 0) AS cpr",
                        "\tFROM master_account_ads a",
                        "\tINNER JOIN data_ads_country b ON a.account_id = b.account_ads_id",
                        "\tWHERE b.data_ads_country_tanggal BETWEEN %s AND %s",
                        f"\t\tAND ({like_conditions})",
                        f"\t\t{country_sql}",
                        "\tGROUP BY",
                        "\t\tb.data_ads_country_tanggal,",
                        "\t\tb.data_ads_country_cd,",
                        "\t\tb.data_ads_country_nm,",
                        "\t\tSUBSTRING_INDEX(b.data_ads_domain, '.', -2)",
                    ") rs",
                    "GROUP BY",
                    "\trs.date,",
                    "\trs.country_code,",
                    "\trs.country_name,",
                    "\trs.domain",
                    "ORDER BY rs.date ASC"
                ])
                sql = "\n".join(sql_parts)
                params = [start_date_formatted, end_date_formatted] + like_params + country_params

            if not self.execute_query(sql, tuple(params)):
                raise pymysql.Error("Failed to get adsense country detail by params")
            data = self.fetch_all()
            if not self.commit():
                raise pymysql.Error("Failed to commit adsense country detail by params")
            return {
                "hasil": {
                    "status": True,
                    "message": "Detail Adsense country berhasil diambil",
                    "data": data
                }
            }
        except Exception as e:
            return {
                "hasil": {
                    "status": False,
                    "data": f"Terjadi error {e!r}"
                }
            }

    def get_all_rekapitulasi_adx_monitoring_account_by_params(self, start_date, end_date, past_start_date, past_end_date, selected_account_list = None, selected_domain_list = None):
        try:
            # --- 0. Pastikan selected_account_list adalah list string
            if isinstance(selected_account_list, str):
                selected_account_list = [selected_account_list.strip()]
            elif selected_account_list is None:
                selected_account_list = []
            elif isinstance(selected_account_list, (set, tuple)):
                selected_account_list = list(selected_account_list)
            data_account_list = [str(a).strip() for a in selected_account_list if str(a).strip()]
            like_conditions_account_now = " OR ".join(["a.account_id LIKE %s"] * len(data_account_list))
            like_params_account_now = [f"{account}%" for account in data_account_list] 
            like_conditions_account_last = " OR ".join(["a.account_id LIKE %s"] * len(data_account_list))
            like_params_account_last = [f"{account}%" for account in data_account_list] 
            # --- 1. Pastikan selected_domain_list adalah list string
            if isinstance(selected_domain_list, str):
                selected_domain_list = [selected_domain_list.strip()]
            elif selected_domain_list is None:
                selected_domain_list = []
            elif isinstance(selected_domain_list, (set, tuple)):
                selected_domain_list = list(selected_domain_list)
            data_domain_list = [str(d).strip() for d in selected_domain_list if str(d).strip()]

            engine = (self._report_engine() or '').strip().lower()
            use_clickhouse = engine in ('clickhouse', 'ch')

            if use_clickhouse:
                params_now = [start_date, end_date]
                sql_now = [
                    "SELECT",
                    "\tb.account_id AS account_id,",
                    "\tSUM(b.data_adx_country_revenue) AS pendapatan",
                    "FROM data_adx_country b",
                    "WHERE toDate(b.data_adx_country_tanggal) BETWEEN toDate(%s) AND toDate(%s)",
                ]
                if data_account_list:
                    like_account_ch = " OR ".join(["toString(b.account_id) LIKE %s"] * len(data_account_list))
                    sql_now.append(f"AND ({like_account_ch})")
                    params_now.extend([f"{account}%" for account in data_account_list])
                if data_domain_list:
                    like_domain_ch = " OR ".join(["b.data_adx_country_domain LIKE %s"] * len(data_domain_list))
                    sql_now.append(f"AND ({like_domain_ch})")
                    params_now.extend([f"%{domain}%" for domain in data_domain_list])
                sql_now.append("GROUP BY b.account_id")

                if not self.execute_query("\n".join(sql_now), tuple(params_now)):
                    raise pymysql.Error("Failed to get all adx monitoring account (now) by params")
                now_rows = self.fetch_all() or []

                params_last = [past_start_date, past_end_date]
                sql_last = [
                    "SELECT",
                    "\tb.account_id AS account_id,",
                    "\tSUM(b.data_adx_country_revenue) AS pendapatan",
                    "FROM data_adx_country b",
                    "WHERE toDate(b.data_adx_country_tanggal) BETWEEN toDate(%s) AND toDate(%s)",
                ]
                if data_account_list:
                    like_account_ch = " OR ".join(["toString(b.account_id) LIKE %s"] * len(data_account_list))
                    sql_last.append(f"AND ({like_account_ch})")
                    params_last.extend([f"{account}%" for account in data_account_list])
                if data_domain_list:
                    like_domain_ch = " OR ".join(["b.data_adx_country_domain LIKE %s"] * len(data_domain_list))
                    sql_last.append(f"AND ({like_domain_ch})")
                    params_last.extend([f"%{domain}%" for domain in data_domain_list])
                sql_last.append("GROUP BY b.account_id")

                if not self.execute_query("\n".join(sql_last), tuple(params_last)):
                    raise pymysql.Error("Failed to get all adx monitoring account (last) by params")
                last_rows = self.fetch_all() or []

                last_map = {}
                for r in last_rows:
                    aid = str((r or {}).get('account_id') or '').strip()
                    if not aid:
                        continue
                    try:
                        last_map[aid] = float((r or {}).get('pendapatan') or 0)
                    except Exception:
                        last_map[aid] = 0.0

                data = []
                account_ids = []
                for r in now_rows:
                    aid = str((r or {}).get('account_id') or '').strip()
                    if not aid:
                        continue
                    try:
                        now_val = float((r or {}).get('pendapatan') or 0)
                    except Exception:
                        now_val = 0.0
                    last_val = float(last_map.get(aid) or 0.0)
                    data.append({
                        'account_id': aid,
                        'account_name': '',
                        'pendapatan_last': last_val,
                        'pendapatan_now': now_val,
                        'pendapatan_selisih': round(now_val - last_val),
                        'pendapatan_persen': round(((now_val - last_val) / now_val * 100) if now_val else 0, 2),
                    })
                    account_ids.append(aid)

                if account_ids:
                    try:
                        placeholders = ','.join(['%s'] * len(account_ids))
                        sql_name = f"SELECT account_id, MAX(account_name) AS account_name FROM app_credentials WHERE account_id IN ({placeholders}) GROUP BY account_id"
                        if self.execute_query(sql_name, tuple(account_ids)):
                            name_rows = self.fetch_all() or []
                            name_map = {str((n or {}).get('account_id') or '').strip(): ((n or {}).get('account_name') or '') for n in name_rows}
                            for row in data:
                                row['account_name'] = name_map.get(str(row.get('account_id') or '').strip(), '')
                    except Exception:
                        pass

                return {'hasil': {"status": True, "message": "Data adx monitoring account berhasil diambil", "data": data}}

            # --- 2. Buat kondisi LIKE untuk setiap domain
            like_conditions_domain_now = " OR ".join(["b.data_adx_country_domain LIKE %s"] * len(data_domain_list))
            like_params_domain_now = [f"%{domain}%" for domain in data_domain_list] 
            like_conditions_domain_last = " OR ".join(["b.data_adx_country_domain LIKE %s"] * len(data_domain_list))
            like_params_domain_last = [f"%{domain}%" for domain in data_domain_list] 
            base_sql = [
                "SELECT",
                "\tnow.account_id,",
                "\tnow.account_name,",
                "\tCOALESCE(last.pendapatan, 0) AS pendapatan_last,",
                "\tnow.pendapatan AS pendapatan_now,",
                "\tROUND(now.pendapatan - COALESCE(last.pendapatan, 0)) AS pendapatan_selisih,",
                "\tROUND(("
                "\t\t(now.pendapatan - COALESCE(last.pendapatan, 0))"
                "\t\t/ NULLIF(now.pendapatan, 0)"
                "\t) * 100, 2) AS pendapatan_persen",
                "FROM (",
                "\tSELECT rs.account_id, rs.account_name, SUM(rs.revenue) AS pendapatan",
                "\tFROM (",
                "\t\tSELECT",
                "\t\t\ta.account_id, a.account_name,",
                "\t\t\tb.data_adx_country_domain,",
                "\t\t\tb.data_adx_country_cd,",
                "\t\t\tSUM(b.data_adx_country_revenue) AS revenue",
                "\t\tFROM app_credentials a",
                "\t\tINNER JOIN data_adx_country b ON a.account_id = b.account_id",
                "\t\tWHERE b.data_adx_country_tanggal BETWEEN %s AND %s",
            ]
            params = [start_date, end_date]
            # filter account NOW
            if data_account_list:
                base_sql.append(f"\t\tAND ({like_conditions_account_now})")
                params.extend(like_params_account_now)
            # filter domain NOW
            if data_domain_list:
                base_sql.append(f"\t\tAND ({like_conditions_domain_now})")
                params.extend(like_params_domain_now)
            base_sql.extend([
                "\t\tGROUP BY a.account_id, a.account_name, b.data_adx_country_domain, b.data_adx_country_cd",
                "\t) rs",
                "\tGROUP BY rs.account_id, rs.account_name",
                ") now",
                "LEFT JOIN (",
                "\tSELECT rs.account_id, rs.account_name, SUM(rs.revenue) AS pendapatan",
                "\tFROM (",
                "\t\tSELECT",
                "\t\t\ta.account_id, a.account_name,",
                "\t\t\tb.data_adx_country_domain,",
                "\t\t\tb.data_adx_country_cd,",
                "\t\t\tSUM(b.data_adx_country_revenue) AS revenue",
                "\t\tFROM app_credentials a",
                "\t\tINNER JOIN data_adx_country b ON a.account_id = b.account_id",
                "\t\tWHERE b.data_adx_country_tanggal BETWEEN %s AND %s",
            ])
            params.extend([past_start_date, past_end_date])
            # filter account LAST
            if data_account_list:
                base_sql.append(f"\t\tAND ({like_conditions_account_last})")
                params.extend(like_params_account_last)
            # filter domain LAST
            if data_domain_list:
                base_sql.append(f"\t\tAND ({like_conditions_domain_last})")
                params.extend(like_params_domain_last)
            base_sql.extend([
                "\t\tGROUP BY a.account_id, a.account_name, b.data_adx_country_domain, b.data_adx_country_cd",
                "\t) rs",
                "\tGROUP BY rs.account_id, rs.account_name",
                ") last ON now.account_id = last.account_id",
            ])
            sql = "\n".join(base_sql)
            if not self.execute_query(sql, tuple(params)):
                raise pymysql.Error("Failed to get all adx monitoring account by params")  
            data = self.fetch_all()
            if not self.commit():
                raise pymysql.Error("Failed to commit get all adx monitoring account by params")
            hasil = {
                "status": True,
                "message": "Data adx monitoring account berhasil diambil",
                "data": data
            }
        except pymysql.Error as e:
            hasil = {
                "status": False,
                'data': 'Terjadi error {!r}, error nya {}'.format(e, e.args[0])
            }
        return {'hasil': hasil}

    def get_all_rekapitulasi_adsense_monitoring_account_by_params(self, start_date, end_date, past_start_date, past_end_date, selected_account_list = None):
        try:
            if isinstance(selected_account_list, str):
                selected_account_list = [selected_account_list.strip()]
            elif selected_account_list is None:
                selected_account_list = []
            elif isinstance(selected_account_list, (set, tuple)):
                selected_account_list = list(selected_account_list)
            data_account_list = [str(a).strip() for a in selected_account_list if str(a).strip()]

            engine = (self._report_engine() or '').strip().lower()
            use_clickhouse = engine in ('clickhouse', 'ch')

            # =========================
            # CLICKHOUSE PATH (earning_adsense)
            # pendapatan now/last dari ClickHouse, account_name dari MySQL app_credentials
            # =========================
            if use_clickhouse:
                params_now = [start_date, end_date]
                sql_now = [
                    "SELECT",
                    "\tb.account_id AS account_id,",
                    "\tSUM(b.data_adsense_country_revenue) AS pendapatan",
                    "FROM data_adsense_country b",
                    "WHERE toDate(b.data_adsense_country_tanggal) BETWEEN toDate(%s) AND toDate(%s)",
                ]
                if data_account_list:
                    like_conditions_account = " OR ".join(["b.account_id LIKE %s"] * len(data_account_list))
                    sql_now.append(f"AND ({like_conditions_account})")
                    params_now.extend([f"{account}%" for account in data_account_list])
                sql_now.append("GROUP BY b.account_id")

                if not self.execute_query("\n".join(sql_now), tuple(params_now)):
                    raise pymysql.Error("Failed to get all adsense monitoring account (now) by params")
                now_rows = self.fetch_all() or []

                params_last = [past_start_date, past_end_date]
                sql_last = [
                    "SELECT",
                    "\tb.account_id AS account_id,",
                    "\tSUM(b.data_adsense_country_revenue) AS pendapatan",
                    "FROM data_adsense_country b",
                    "WHERE toDate(b.data_adsense_country_tanggal) BETWEEN toDate(%s) AND toDate(%s)",
                ]
                if data_account_list:
                    like_conditions_account = " OR ".join(["b.account_id LIKE %s"] * len(data_account_list))
                    sql_last.append(f"AND ({like_conditions_account})")
                    params_last.extend([f"{account}%" for account in data_account_list])
                sql_last.append("GROUP BY b.account_id")

                if not self.execute_query("\n".join(sql_last), tuple(params_last)):
                    raise pymysql.Error("Failed to get all adsense monitoring account (last) by params")
                last_rows = self.fetch_all() or []

                last_map = {}
                for r in last_rows:
                    aid = str((r or {}).get('account_id') or '').strip()
                    if not aid:
                        continue
                    try:
                        last_map[aid] = float((r or {}).get('pendapatan') or 0)
                    except Exception:
                        last_map[aid] = 0.0

                data = []
                account_ids = []
                for r in now_rows:
                    aid = str((r or {}).get('account_id') or '').strip()
                    if not aid:
                        continue
                    try:
                        now_val = float((r or {}).get('pendapatan') or 0)
                    except Exception:
                        now_val = 0.0
                    last_val = float(last_map.get(aid) or 0.0)
                    selisih = round(now_val - last_val)
                    persen = round(((now_val - last_val) / now_val * 100) if now_val else 0, 2)
                    data.append({
                        'account_id': aid,
                        'account_name': '',
                        'pendapatan_last': last_val,
                        'pendapatan_now': now_val,
                        'pendapatan_selisih': selisih,
                        'pendapatan_persen': persen,
                    })
                    account_ids.append(aid)

                # Enrich account_name from MySQL (app_credentials)
                if account_ids:
                    try:
                        placeholders = ','.join(['%s'] * len(account_ids))
                        sql_name = f"SELECT account_id, MAX(account_name) AS account_name FROM app_credentials WHERE account_id IN ({placeholders}) GROUP BY account_id"
                        if self.execute_query(sql_name, tuple(account_ids)):
                            name_rows = self.fetch_all() or []
                            name_map = {str((n or {}).get('account_id') or '').strip(): ((n or {}).get('account_name') or '') for n in name_rows}
                            for row in data:
                                aid = str(row.get('account_id') or '').strip()
                                row['account_name'] = name_map.get(aid, '')
                    except Exception:
                        pass

                hasil = {
                    "status": True,
                    "message": "Data adsense monitoring account berhasil diambil",
                    "data": data
                }
                return {'hasil': hasil}

            # =========================
            # MYSQL PATH (existing)
            # =========================
            like_conditions_account_now = " OR ".join(["a.account_id LIKE %s"] * len(data_account_list))
            like_params_account_now = [f"{account}%" for account in data_account_list]
            like_conditions_account_last = " OR ".join(["a.account_id LIKE %s"] * len(data_account_list))
            like_params_account_last = [f"{account}%" for account in data_account_list]
            base_sql = [
                "SELECT",
                "\tnow.account_id,",
                "\tnow.account_name,",
                "\tCOALESCE(last.pendapatan, 0) AS pendapatan_last,",
                "\tnow.pendapatan AS pendapatan_now,",
                "\tROUND(now.pendapatan - COALESCE(last.pendapatan, 0)) AS pendapatan_selisih,",
                "\tROUND((",
                "\t\t(now.pendapatan - COALESCE(last.pendapatan, 0))",
                "\t\t/ NULLIF(now.pendapatan, 0)",
                "\t) * 100, 2) AS pendapatan_persen",
                "FROM (",
                "\tSELECT rs.account_id, rs.account_name, SUM(rs.revenue) AS pendapatan",
                "\tFROM (",
                "\t\tSELECT",
                "\t\t\ta.account_id, a.account_name,",
                "\t\t\tb.data_adsense_country_domain,",
                "\t\t\tb.data_adsense_country_cd,",
                "\t\t\tSUM(b.data_adsense_country_revenue) AS revenue",
                "\t\tFROM app_credentials a",
                "\t\tINNER JOIN data_adsense_country b ON a.account_id = b.account_id",
                "\t\tWHERE b.data_adsense_country_tanggal BETWEEN %s AND %s",
            ]
            params = [start_date, end_date]
            if data_account_list:
                base_sql.append(f"\t\tAND ({like_conditions_account_now})")
                params.extend(like_params_account_now)
            base_sql.extend([
                "\t\tGROUP BY a.account_id, a.account_name, b.data_adsense_country_domain, b.data_adsense_country_cd",
                "\t) rs",
                "\tGROUP BY rs.account_id, rs.account_name",
                ") now",
                "LEFT JOIN (",
                "\tSELECT rs.account_id, rs.account_name, SUM(rs.revenue) AS pendapatan",
                "\tFROM (",
                "\t\tSELECT",
                "\t\t\ta.account_id, a.account_name,",
                "\t\t\tb.data_adsense_country_domain,",
                "\t\t\tb.data_adsense_country_cd,",
                "\t\t\tSUM(b.data_adsense_country_revenue) AS revenue",
                "\t\tFROM app_credentials a",
                "\t\tINNER JOIN data_adsense_country b ON a.account_id = b.account_id",
                "\t\tWHERE b.data_adsense_country_tanggal BETWEEN %s AND %s",
            ])
            params.extend([past_start_date, past_end_date])
            if data_account_list:
                base_sql.append(f"\t\tAND ({like_conditions_account_last})")
                params.extend(like_params_account_last)
            base_sql.extend([
                "\t\tGROUP BY a.account_id, a.account_name, b.data_adsense_country_cd",
                "\t) rs",
                "\tGROUP BY rs.account_id, rs.account_name",
                ") last ON now.account_id = last.account_id",
            ])
            sql = "\n".join(base_sql)
            if not self.execute_query(sql, tuple(params)):
                raise pymysql.Error("Failed to get all adsense monitoring account by params")
            data = self.fetch_all()
            if not self.commit():
                raise pymysql.Error("Failed to commit get all adsense monitoring account by params")
            hasil = {
                "status": True,
                "message": "Data adsense monitoring account berhasil diambil",
                "data": data
            }
        except Exception as e:
            hasil = {
                "status": False,
                'data': 'Terjadi error {!r}'.format(e)
            }
        return {'hasil': hasil}


    def get_all_data_meta_adx_adsense_country_detail_by_params(self, tanggal):
        """
        Jalankan query ClickHouse untuk mengambil data meta/adx/adsense per domain/country.
        """
        try:
            sql = f"""
            		SELECT
                    toHour(toTimeZone(now(), 'Asia/Jakarta')) AS run_hour,
                    formatDateTime(toTimeZone(now(), 'Asia/Jakarta'), '%H:%i:%S') AS run_time,
                    m.domain AS domain,
                    lower(m.domain) || '|' || lower(m.campaign_name) || '|' || upper(m.country_cd) AS entity_key,
                    m.country_cd AS country_code,
                    m.country_name AS country_name,
                    m.date AS date,
                    /* ================= META ================= */
                    m.spend AS meta_spend,
                    m.meta_budget AS meta_daily_budget,
                    m.campaign_name AS meta_campaign,
                    m.cpc AS meta_cpc,
                    m.clicks AS meta_clicks,
                    m.lpv AS meta_lpv,
                    m.lpv_rate AS meta_lpv_rate,
                    m.frekuensi AS meta_frequency,
                    /* ================= ADX ================= */
                    round(m.lpv_weight * ifNull(a.revenue,0),0) AS adx_revenue,
                    round(m.lpv_weight * ifNull(a.impressions,0),0) AS adx_impressions,
                    round(m.lpv_weight * ifNull(a.clicks,0),0) AS adx_clicks,
                    ifNull(a.ecpm,0) AS adx_ecpm,
                    ifNull(a.cpc,0) AS adx_cpc,
                    round(m.lpv_weight * ifNull(a.requests,0),0) AS adx_requests,
                    round(m.lpv_weight * ifNull(a.responses_served,0),0) AS adx_responses_served,
                    ifNull(a.match_rate,0) AS adx_match_rate,
                    ifNull(a.fill_rate,0) AS adx_fill_rate,
                    ifNull(a.active_view_pct_viewable,0) AS adx_active_view_pct_viewable,
                    ifNull(a.active_view_avg_time_sec,0) AS adx_active_view_avg_time_sec,
                    /* ================= ADSENSE ================= */
                    round(m.lpv_weight * ifNull(s.revenue,0),0) AS adsense_estimated_earnings,
                    round(m.lpv_weight * ifNull(s.page_views,0),0) AS adsense_page_views,
                    round(m.lpv_weight * ifNull(s.clicks,0),0) AS adsense_clicks,
                    ifNull(s.cpc,0) AS adsense_cost_per_click,
                    ifNull(s.page_views_rpm,0) AS adsense_page_views_rpm,
                    round(m.lpv_weight * ifNull(s.requests,0),0) AS adsense_ad_requests,
                    ifNull(s.requests_coverage,0) AS adsense_ad_requests_coverage,
                    round(m.lpv_weight * ifNull(s.impressions,0),0) AS adsense_impressions,
                    ifNull(s.active_view_viewability,0) AS adsense_active_view_viewability,
                    ifNull(s.active_view_measurability,0) AS adsense_active_view_measurability,
                    ifNull(s.active_view_time,0) AS adsense_active_view_time,
                    /* ================= TOTAL ================= */
                    round(
                        m.lpv_weight *
                        (ifNull(a.revenue,0)+ifNull(s.revenue,0)),
                    0) AS total_revenue,
                    round(
                        (m.lpv_weight *
                        (ifNull(a.revenue,0)+ifNull(s.revenue,0)))
                        - m.spend,
                    0) AS profit,
                    if(
                        m.spend>0,
                        round(
                            (m.lpv_weight *
                            (ifNull(a.revenue,0)+ifNull(s.revenue,0)))
                            / m.spend,
                        3),
                        0
                    ) AS roas
                FROM
                (
                    /* ================= META SOURCE ================= */
                    SELECT
                        base.*,
                        base.lpv /
                        nullIf(
                            sum(base.lpv) OVER (
                                PARTITION BY
                                    base.date,
                                    base.domain,
                                    base.country_cd
                            ),0
                        ) AS lpv_weight
                    FROM
                    (
                        WITH lower(
                                arrayStringConcat(
                                    arraySlice(splitByChar('.',data_ads_domain),1,2),
                                    '.'
                                )
                            ) AS domain
                        SELECT
                            domain,
                            upper(a.data_ads_country_cd) AS country_cd,
                            upper(a.data_ads_country_nm) AS country_name,
                            toDate(a.data_ads_country_tanggal) AS date,
                            lower(a.data_ads_campaign_nm) AS campaign_name,
                            argMax(a.data_ads_country_spend,a.mdd) AS spend,
                            toFloat64(argMax(b.master_budget,a.mdd)) AS meta_budget,
                            argMax(a.data_ads_country_cpc,a.mdd) AS cpc,
                            argMax(a.data_ads_country_click,a.mdd) AS clicks,
                            argMax(a.data_ads_country_lpv,a.mdd) AS lpv,
                            argMax(a.data_ads_country_lpv_rate,a.mdd) AS lpv_rate,
                            argMax(a.data_ads_country_frekuensi,a.mdd) AS frekuensi
                        FROM hris_trendHorizone.data_ads_country a
                        INNER JOIN (
                            SELECT
                                master_date,
                                lower(arrayStringConcat(
                                    arraySlice(splitByChar('.',master_domain),1,2),'.'
                                )) AS master_domain,
                                max(master_budget) AS master_budget
                            FROM hris_trendHorizone.master_ads
                            GROUP BY master_date, master_domain
                        ) b
                        ON domain = b.master_domain
                        AND toDate(a.data_ads_country_tanggal) = b.master_date
                        WHERE a.data_ads_country_tanggal >= '{tanggal}'
                        GROUP BY
                            domain,
                            country_cd,
                            country_name,
                            date,
                            campaign_name
                    ) base
                ) m

                /* ================= ADX ================= */
                LEFT JOIN
                (
                    SELECT
                        lower(arrayStringConcat(arraySlice(splitByChar('.', data_adx_country_domain),1,2),'.')) AS domain,
                        upper(data_adx_country_cd) AS country_cd,
                        toDate(data_adx_country_tanggal) AS date,
                        argMax(data_adx_country_revenue,data_adx_country_tanggal) AS revenue,
                        argMax(data_adx_country_impresi,data_adx_country_tanggal) AS impressions,
                        argMax(data_adx_country_click,data_adx_country_tanggal) AS clicks,
                        argMax(data_adx_country_ecpm,data_adx_country_tanggal) AS ecpm,
                        argMax(data_adx_country_cpc,data_adx_country_tanggal) AS cpc,
                        argMax(data_adx_country_total_requests,data_adx_country_tanggal) AS requests,
                        argMax(data_adx_country_responses_served,data_adx_country_tanggal) AS responses_served,
                        argMax(data_adx_country_match_rate,data_adx_country_tanggal) AS match_rate,
                        argMax(data_adx_country_fill_rate,data_adx_country_tanggal) AS fill_rate,
                        argMax(data_adx_country_active_view_pct_viewable,data_adx_country_tanggal) AS active_view_pct_viewable,
                        argMax(data_adx_country_active_view_avg_time_sec,data_adx_country_tanggal) AS active_view_avg_time_sec
                    FROM hris_trendHorizone.data_adx_country
                    WHERE data_adx_country_tanggal >= '{tanggal}'
                    GROUP BY domain,country_cd,date
                ) a
                ON m.domain=a.domain
                AND m.country_cd=a.country_cd
                AND m.date=a.date

                /* ================= ADSENSE ================= */
                LEFT JOIN
                (
                    SELECT
                        lower(arrayStringConcat(arraySlice(splitByChar('.', data_adsense_country_domain),1,2),'.')) AS domain,
                        upper(data_adsense_country_cd) AS country_cd,
                        toDate(data_adsense_country_tanggal) AS date,
                        argMax(data_adsense_country_revenue,data_adsense_country_tanggal) AS revenue,
                        argMax(data_adsense_country_page_views,data_adsense_country_tanggal) AS page_views,
                        argMax(data_adsense_country_click,data_adsense_country_tanggal) AS clicks,
                        argMax(data_adsense_country_cpc,data_adsense_country_tanggal) AS cpc,
                        argMax(data_adsense_country_page_views_rpm,data_adsense_country_tanggal) AS page_views_rpm,
                        argMax(data_adsense_country_ad_requests,data_adsense_country_tanggal) AS requests,
                        argMax(data_adsense_country_ad_requests_coverage,data_adsense_country_tanggal) AS requests_coverage,
                        argMax(data_adsense_country_impresi,data_adsense_country_tanggal) AS impressions,
                        argMax(data_adsense_country_active_view_viewability,data_adsense_country_tanggal) AS active_view_viewability,
                        argMax(data_adsense_country_active_view_measurability,data_adsense_country_tanggal) AS active_view_measurability,
                        argMax(data_adsense_country_active_view_time,data_adsense_country_tanggal) AS active_view_time
                    FROM hris_trendHorizone.data_adsense_country
                    WHERE data_adsense_country_tanggal >= '{tanggal}'
                    GROUP BY domain,country_cd,date
                ) s
                ON m.domain=s.domain
                AND m.country_cd=s.country_cd
                AND m.date=s.date
                WHERE m.domain IS NOT NULL
                AND m.spend>0
                AND (
                    ifNull(a.revenue,0)>0
                    OR ifNull(s.revenue,0)>0
                )
                ORDER BY m.date DESC,m.domain ASC
            """
            # Gunakan koneksi ClickHouse
            if clickhouse_connect is None:
                raise RuntimeError('clickhouse_connect library is not installed')
            client = get_clickhouse_client()
            result = client.query(sql)
            rows = result.result_rows if hasattr(result, 'result_rows') else result.result_set
            cols = result.column_names if hasattr(result, 'column_names') else []
            data_rows = [dict(zip(cols, row)) for row in rows]
            return {
                "status": True,
                "message": "Data berhasil diambil",
                "total_rows": len(data_rows),
                "data": data_rows
            }
        except Exception as e:
            return {
                "status": False,
                "error": f"Terjadi error: {str(e)}"
            }

    def get_monitoring_domain_campaign_breakdown_by_params(self, start_date, end_date, site_name):
        try:
            domain = str(site_name or '').strip().lower()
            if not domain:
                return {"status": False, "error": "site_name wajib diisi", "data": []}

            sql = """
            SELECT
                ifNull(nullIf(m.campaign_name, ''), 'unknown_campaign') AS campaign,
                round(sum(m.spend), 0) AS spend,
                round(sum(m.lpv_weight * (ifNull(a.revenue, 0) + ifNull(s.revenue, 0))), 0) AS revenue,
                round(sum(m.lpv_weight * (ifNull(a.revenue, 0) + ifNull(s.revenue, 0))) - sum(m.spend), 0) AS net_profit,
                if(sum(m.spend) > 0,
                   round(((sum(m.lpv_weight * (ifNull(a.revenue, 0) + ifNull(s.revenue, 0))) - sum(m.spend)) / sum(m.spend)) * 100, 2),
                   0) AS roi
            FROM
            (
                SELECT
                    base.*,
                    base.lpv / nullIf(sum(base.lpv) OVER (PARTITION BY base.date, base.domain, base.country_cd), 0) AS lpv_weight
                FROM
                (
                    SELECT
                        lower(arrayStringConcat(arraySlice(splitByChar('.', a.log_ads_domain), 1, 2), '.')) AS domain,
                        upper(a.log_ads_country_cd) AS country_cd,
                        toDate(a.log_ads_country_tanggal) AS date,
                        lower(a.log_ads_campaign_nm) AS campaign_name,
                        argMax(a.log_ads_country_spend, a.mdd) AS spend,
                        argMax(a.log_ads_country_lpv, a.mdd) AS lpv
                    FROM hris_trendHorizone.log_ads_country a
                    WHERE toDate(a.log_ads_country_tanggal) BETWEEN toDate(%s) AND toDate(%s)
                      AND lower(arrayStringConcat(arraySlice(splitByChar('.', a.log_ads_domain), 1, 2), '.')) = lower(%s)
                    GROUP BY domain, country_cd, date, campaign_name
                ) base
            ) m
            LEFT JOIN
            (
                SELECT lower(arrayStringConcat(arraySlice(splitByChar('.', data_adx_country_domain), 1, 2), '.')) AS domain,
                       upper(data_adx_country_cd) AS country_cd,
                       toDate(data_adx_country_tanggal) AS date,
                       argMax(data_adx_country_revenue, data_adx_country_tanggal) AS revenue
                FROM hris_trendHorizone.data_adx_country
                WHERE toDate(data_adx_country_tanggal) BETWEEN toDate(%s) AND toDate(%s)
                GROUP BY domain, country_cd, date
            ) a ON m.domain = a.domain AND m.country_cd = a.country_cd AND m.date = a.date
            LEFT JOIN
            (
                SELECT lower(arrayStringConcat(arraySlice(splitByChar('.', data_adsense_country_domain), 1, 2), '.')) AS domain,
                       upper(data_adsense_country_cd) AS country_cd,
                       toDate(data_adsense_country_tanggal) AS date,
                       argMax(data_adsense_country_revenue, data_adsense_country_tanggal) AS revenue
                FROM hris_trendHorizone.data_adsense_country
                WHERE toDate(data_adsense_country_tanggal) BETWEEN toDate(%s) AND toDate(%s)
                GROUP BY domain, country_cd, date
            ) s ON m.domain = s.domain AND m.country_cd = s.country_cd AND m.date = s.date
            GROUP BY campaign
            ORDER BY spend DESC
            """

            params = (start_date, end_date, domain, start_date, end_date, start_date, end_date)
            self._ensure_report_connection()
            self.cur_hris = self.report_cur
            self.cur_hris.execute(sql, params)
            rows = self.fetch_all() or []
            return {"status": True, "data": rows}
        except Exception as e:
            return {"status": False, "error": f"Terjadi error: {e}", "data": []}

    def get_monitoring_country_subdomain_campaign_breakdown_by_params(self, start_date, end_date, country_code, selected_domain_list=None):
        try:
            cc = str(country_code or '').strip().upper()
            if cc == 'TU':
                cc = 'TR'
            if not cc:
                return {"status": False, "error": "country_code wajib diisi", "data": []}

            domains = []
            for d in (selected_domain_list or []):
                s = str(d or '').strip().lower()
                if not s:
                    continue
                ps = [x for x in s.split('.') if x]
                if len(ps) >= 2:
                    s = '.'.join(ps[:2])
                if s and s not in domains:
                    domains.append(s)

            domain_filter_sql = ''
            domain_params = []
            if domains:
                domain_filter_sql = ' AND lower(arrayStringConcat(arraySlice(splitByChar(\'.\', a.log_ads_domain), 1, 2), \'\.\')) IN (' + ','.join(['%s'] * len(domains)) + ') '
                domain_params = domains

            sql = f"""
            SELECT
                m.domain AS subdomain,
                ifNull(nullIf(m.campaign_name, ''), 'unknown_campaign') AS campaign,
                round(sum(m.spend), 0) AS spend,
                round(sum(m.lpv_weight * (ifNull(a.revenue, 0) + ifNull(s.revenue, 0))), 0) AS revenue,
                round(sum(m.lpv_weight * (ifNull(a.revenue, 0) + ifNull(s.revenue, 0))) - sum(m.spend), 0) AS net_profit,
                if(sum(m.spend) > 0,
                   round(((sum(m.lpv_weight * (ifNull(a.revenue, 0) + ifNull(s.revenue, 0))) - sum(m.spend)) / sum(m.spend)) * 100, 2),
                   0) AS roi
            FROM
            (
                SELECT
                    base.*,
                    base.lpv / nullIf(sum(base.lpv) OVER (PARTITION BY base.date, base.domain, base.country_cd), 0) AS lpv_weight
                FROM
                (
                    SELECT
                        lower(arrayStringConcat(arraySlice(splitByChar('.', a.log_ads_domain), 1, 2), '.')) AS domain,
                        upper(a.log_ads_country_cd) AS country_cd,
                        toDate(a.log_ads_country_tanggal) AS date,
                        lower(a.log_ads_campaign_nm) AS campaign_name,
                        argMax(a.log_ads_country_spend, a.mdd) AS spend,
                        argMax(a.log_ads_country_lpv, a.mdd) AS lpv
                    FROM hris_trendHorizone.log_ads_country a
                    WHERE toDate(a.log_ads_country_tanggal) BETWEEN toDate(%s) AND toDate(%s)
                      AND upper(a.log_ads_country_cd) IN (%s, if(%s='TR','TU',''))
                      {domain_filter_sql}
                    GROUP BY domain, country_cd, date, campaign_name
                ) base
            ) m
            LEFT JOIN
            (
                SELECT lower(arrayStringConcat(arraySlice(splitByChar('.', data_adx_country_domain), 1, 2), '.')) AS domain,
                       upper(data_adx_country_cd) AS country_cd,
                       toDate(data_adx_country_tanggal) AS date,
                       argMax(data_adx_country_revenue, data_adx_country_tanggal) AS revenue
                FROM hris_trendHorizone.data_adx_country
                WHERE toDate(data_adx_country_tanggal) BETWEEN toDate(%s) AND toDate(%s)
                GROUP BY domain, country_cd, date
            ) a ON m.domain = a.domain AND m.country_cd = a.country_cd AND m.date = a.date
            LEFT JOIN
            (
                SELECT lower(arrayStringConcat(arraySlice(splitByChar('.', data_adsense_country_domain), 1, 2), '.')) AS domain,
                       upper(data_adsense_country_cd) AS country_cd,
                       toDate(data_adsense_country_tanggal) AS date,
                       argMax(data_adsense_country_revenue, data_adsense_country_tanggal) AS revenue
                FROM hris_trendHorizone.data_adsense_country
                WHERE toDate(data_adsense_country_tanggal) BETWEEN toDate(%s) AND toDate(%s)
                GROUP BY domain, country_cd, date
            ) s ON m.domain = s.domain AND m.country_cd = s.country_cd AND m.date = s.date
            GROUP BY subdomain, campaign
            ORDER BY spend DESC
            """

            params = [start_date, end_date, cc, cc] + domain_params + [start_date, end_date, start_date, end_date]
            self._ensure_report_connection()
            self.cur_hris = self.report_cur
            self.cur_hris.execute(sql, tuple(params))
            rows = self.fetch_all() or []
            return {"status": True, "data": rows}
        except Exception as e:
            return {"status": False, "error": f"Terjadi error: {e}", "data": []}

    def get_all_data_meta_adx_adsense_country_detail_by_params_log(self, tanggal):
        """
        Jalankan query ClickHouse untuk mengambil data meta/adx/adsense per domain/country.
        """
        try:
            sql = f"""
            		SELECT
                    m.run_date AS run_date,
                    m.run_hour AS run_hour,
                    m.run_time AS run_time,
                    m.domain AS domain,
                    concat(lower(m.domain),'|',lower(m.campaign_name),'|',upper(m.country_cd)) AS entity_key,
                    m.country_cd AS country_code,
                    m.country_name AS country_name,
                    m.date AS date,
                    -- ================= META =================
                    m.spend AS meta_spend,
                    m.meta_budget AS meta_daily_budget,
                    m.campaign_name AS meta_campaign,
                    m.cpc AS meta_cpc,
                    m.clicks AS meta_clicks,
                    m.lpv AS meta_lpv,
                    m.lpv_rate AS meta_lpv_rate,
                    m.frekuensi AS meta_frequency,
                    -- ================= ADX (LPV ATTRIBUTION) =================
                    round(
                        (
                            m.lpv /
                            nullIf(
                                sum(m.lpv) OVER (
                                    PARTITION BY
                                        m.date,
                                        m.domain,
                                        m.country_cd,
                                        m.run_hour
                                ),
                            0)
                        ) * ifNull(a.revenue,0),
                    0) AS adx_revenue,
                    round(lpv_weight * ifNull(a.impressions,0),0) AS adx_impressions,
                    round(lpv_weight * ifNull(a.clicks,0),0) AS adx_clicks,
                    ifNull(a.ecpm,0) AS adx_ecpm,
                    ifNull(a.cpc,0) AS adx_cpc,
                    round(lpv_weight * ifNull(a.requests,0),0) AS adx_requests,
                    round(lpv_weight * ifNull(a.responses_served,0),0) AS adx_responses_served,
                    ifNull(a.match_rate,0) AS adx_match_rate,
                    ifNull(a.fill_rate,0) AS adx_fill_rate,
                    ifNull(a.active_view_pct_viewable,0) AS adx_active_view_pct_viewable,
                    ifNull(a.active_view_avg_time_sec,0) AS adx_active_view_avg_time_sec,
                    -- ================= ADSENSE =================
                    round(
                    (
                        m.lpv /
                        nullIf(
                            sum(m.lpv) OVER (
                                PARTITION BY
                                    m.date,
                                    m.domain,
                                    m.country_cd,
                                    m.run_hour
                            ),
                        0)
                    ) * ifNull(s.revenue,0),
                    0) AS adsense_estimated_earnings,
                    round(lpv_weight * ifNull(s.page_views,0),0) AS adsense_page_views,
                    round(lpv_weight * ifNull(s.clicks,0),0) AS adsense_clicks,
                    ifNull(s.cpc,0) AS adsense_cost_per_click,
                    ifNull(s.page_views_rpm,0) AS adsense_page_views_rpm,
                    round(lpv_weight * ifNull(s.requests,0),0) AS adsense_ad_requests,
                    ifNull(s.requests_coverage,0) AS adsense_ad_requests_coverage,
                    round(lpv_weight * ifNull(s.impressions,0),0) AS adsense_impressions,
                    ifNull(s.active_view_viewability,0) AS adsense_active_view_viewability,
                    ifNull(s.active_view_measurability,0) AS adsense_active_view_measurability,
                    ifNull(s.active_view_time,0) AS adsense_active_view_time,
                    -- ================= TOTAL =================
                    round(
                        (
                            (
                                m.lpv /
                                nullIf(
                                    sum(m.lpv) OVER (
                                        PARTITION BY
                                            m.date,
                                            m.domain,
                                            m.country_cd,
                                            m.run_hour
                                    ),
                                0)
                            ) * ifNull(a.revenue,0)
                        ) + ifNull(s.revenue,0),
                    0) AS total_revenue,
                    round(
                        (
                            (
                                (
                                    m.lpv /
                                    nullIf(
                                        sum(m.lpv) OVER (
                                            PARTITION BY
                                                m.date,
                                                m.domain,
                                                m.country_cd,
                                                m.run_hour
                                        ),
                                    0)
                                ) * ifNull(a.revenue,0)
                            ) + ifNull(s.revenue,0)
                        ) - m.spend,
                    0) AS profit,
                    if(
                        m.spend > 0,
                        round(
                            (
                                (
                                    (
                                        m.lpv /
                                        nullIf(
                                            sum(m.lpv) OVER (
                                                PARTITION BY
                                                    m.date,
                                                    m.domain,
                                                    m.country_cd,
                                                    m.run_hour
                                            ),
                                        0)
                                    ) * ifNull(a.revenue,0)
                                ) + ifNull(s.revenue,0)
                            ) / m.spend,
                        0),
                        0
                    ) AS roas
                FROM
                (
                    SELECT
                        m.*,

                        m.lpv /
                        nullIf(
                            sum(m.lpv) OVER (
                                PARTITION BY
                                    m.date,
                                    m.domain,
                                    m.country_cd,
                                    m.run_hour
                            ),
                        0) AS lpv_weight

                    FROM
                    (
                        -- META SOURCE (QUERY m KAMU)
                        WITH lower(
                                arrayStringConcat(
                                    arraySlice(splitByChar('.',a.log_ads_domain),1,2),
                                    '.'
                                )
                            ) AS domain
                        
                        SELECT
                            toDate(a.mdd) AS run_date,
                            toHour(toTimeZone(a.mdd,'Asia/Jakarta')) AS run_hour,
                            formatDateTime(toTimeZone(max(a.mdd),'Asia/Jakarta'),'%H:%i:%S') AS run_time,
                            domain,
                            upper(a.log_ads_country_cd) AS country_cd,
                            upper(a.log_ads_country_nm) AS country_name,
                            toDate(a.log_ads_country_tanggal) AS date,
                            lower(a.log_ads_campaign_nm) AS campaign_name,
                            argMax(a.log_ads_country_spend,a.mdd) AS spend,
                            toFloat64(argMax(b.master_budget,a.mdd)) AS meta_budget,
                            argMax(a.log_ads_country_cpc,a.mdd) AS cpc,
                            argMax(a.log_ads_country_click,a.mdd) AS clicks,
                            argMax(a.log_ads_country_lpv,a.mdd) AS lpv,
                            argMax(a.log_ads_country_lpv_rate,a.mdd) AS lpv_rate,
                            argMax(a.log_ads_country_frekuensi,a.mdd) AS frekuensi
                        FROM hris_trendHorizone.log_ads_country a
                        INNER JOIN (
                            SELECT
                                master_date,
                                lower(arrayStringConcat(
                                    arraySlice(splitByChar('.',master_domain),1,2),'.'
                                )) AS domain,
                                max(master_budget) AS master_budget
                            FROM hris_trendHorizone.master_ads
                            GROUP BY master_date, domain
                        ) b
                        ON domain = b.domain
                        WHERE a.log_ads_country_tanggal = '{tanggal}'
                        GROUP BY
                            domain,
                            country_cd,
                            country_name,
                            date,
                            campaign_name,
                            run_date,
                            run_hour
                    ) m
                ) m
                -- ================= ADX =================
                LEFT JOIN
                (
                    SELECT
                        lower(arrayStringConcat(arraySlice(splitByChar('.',log_adx_country_domain),1,2),'.')) AS domain,
                        upper(log_adx_country_cd) AS country_cd,
                        toDate(log_adx_country_tanggal) AS date,
                        toHour(toTimeZone(mdd,'Asia/Jakarta')) AS run_hour,
                        argMax(log_adx_country_revenue,mdd) AS revenue,
                        argMax(log_adx_country_impresi,mdd) AS impressions,
                        argMax(log_adx_country_click,mdd) AS clicks,
                        argMax(log_adx_country_ecpm,mdd) AS ecpm,
                        argMax(log_adx_country_cpc,mdd) AS cpc,
                        argMax(log_adx_country_total_requests,mdd) AS requests,
                        argMax(log_adx_country_responses_served,mdd) AS responses_served,
                        argMax(log_adx_country_match_rate,mdd) AS match_rate,
                        argMax(log_adx_country_fill_rate,mdd) AS fill_rate,
                        argMax(log_adx_country_active_view_pct_viewable,mdd) AS active_view_pct_viewable,
                        argMax(log_adx_country_active_view_avg_time_sec,mdd) AS active_view_avg_time_sec
                    FROM hris_trendHorizone.log_adx_country
                    WHERE log_adx_country_tanggal = '{tanggal}'
                    GROUP BY domain,country_cd,date,run_hour
                ) a
                ON m.domain=a.domain
                AND m.country_cd=a.country_cd
                AND m.date=a.date
                AND m.run_hour=a.run_hour
                -- ================= ADSENSE =================
                LEFT JOIN
                (
                    SELECT
                        lower(arrayStringConcat(arraySlice(splitByChar('.',log_adsense_country_domain),1,2),'.')) AS domain,
                        upper(log_adsense_country_cd) AS country_cd,
                        toDate(log_adsense_country_tanggal) AS date,
                        toHour(toTimeZone(mdd,'Asia/Jakarta')) AS run_hour,
                        argMax(log_adsense_country_revenue,mdd) AS revenue,
                        argMax(log_adsense_country_page_views,mdd) AS page_views,
                        argMax(log_adsense_country_click,mdd) AS clicks,
                        argMax(log_adsense_country_cpc,mdd) AS cpc,
                        argMax(log_adsense_country_page_views_rpm,mdd) AS page_views_rpm,
                        argMax(log_adsense_country_ad_requests,mdd) AS requests,
                        argMax(log_adsense_country_ad_requests_coverage,mdd) AS requests_coverage,
                        argMax(log_adsense_country_impresi,mdd) AS impressions,
                        argMax(log_adsense_country_active_view_viewability,mdd) AS active_view_viewability,
                        argMax(log_adsense_country_active_view_measurability,mdd) AS active_view_measurability,
                        argMax(log_adsense_country_active_view_time,mdd) AS active_view_time
                    FROM hris_trendHorizone.log_adsense_country
                    WHERE log_adsense_country_tanggal = '{tanggal}'
                    GROUP BY domain,country_cd,date,run_hour
                ) s
                ON m.domain=s.domain
                AND m.country_cd=s.country_cd
                AND m.date=s.date
                AND m.run_hour=s.run_hour
                WHERE m.spend > 0
                ORDER BY m.date DESC, m.domain, m.country_cd
            """
            # Gunakan koneksi ClickHouse
            if clickhouse_connect is None:
                raise RuntimeError('clickhouse_connect library is not installed')
            client = get_clickhouse_client()
            result = client.query(sql)
            rows = result.result_rows if hasattr(result, 'result_rows') else result.result_set
            cols = result.column_names if hasattr(result, 'column_names') else []
            data_rows = [dict(zip(cols, row)) for row in rows]
            return {
                "status": True,
                "message": "Data berhasil diambil",
                "total_rows": len(data_rows),
                "data": data_rows
            }
        except Exception as e:
            return {
                "status": False,
                "error": f"Terjadi error: {str(e)}"
            }

    def insert_bulk_fact_domain(self, rows):
        """
        Bulk insert ke ClickHouse menggunakan clickhouse_connect.
        """
        try:
            if not rows:
                return {"status": True, "message": "No data to insert"}
            from .engine_utils import ensure_uuid
            import pandas as pd
            from zoneinfo import ZoneInfo
            from datetime import datetime, time as dt_time
            now = datetime.now(ZoneInfo("Asia/Jakarta"))
            # =========================
            # CONFIG
            # =========================
            SOURCE_MODE_MIXED = "MIXED"
            SOURCE_MODE_ADSENSE_ONLY = "ADSENSE_ONLY"
            SOURCE_MODE_ADX_ONLY = "ADX_ONLY"
            SCORABLE_JOIN_STATUSES = {
                "OK",
                "SOURCE_ONLY_NO_META",
                "SOURCE_ONLY_NO_META_ADSENSE",
                "SOURCE_ONLY_NO_META_ADX",
            }
            # =========================
            # HELPERS
            # =========================
            def compute_mapped_revenue_source(mapped_mode,
                                            adsense_active,
                                            adx_active):

                if mapped_mode == SOURCE_MODE_MIXED:
                    return SOURCE_MODE_MIXED
                if adsense_active and adx_active:
                    return SOURCE_MODE_MIXED
                if mapped_mode == SOURCE_MODE_ADSENSE_ONLY and not adx_active:
                    return SOURCE_MODE_ADSENSE_ONLY
                if mapped_mode == SOURCE_MODE_ADX_ONLY and not adsense_active:
                    return SOURCE_MODE_ADX_ONLY
                if adsense_active:
                    return SOURCE_MODE_ADSENSE_ONLY
                if adx_active:
                    return SOURCE_MODE_ADX_ONLY
                return mapped_mode or SOURCE_MODE_MIXED

            def _to_float(value, default=0.0):
                try:
                    if value is None:
                        return float(default)
                    v = float(value)
                    if pd.isna(v):
                        return float(default)
                    return float(v)
                except Exception:
                    return float(default)

            def _to_str(value, default=""):
                try:
                    if value is None or pd.isna(value):
                        return default
                except Exception:
                    pass
                if isinstance(value, (bytes, bytearray)):
                    try:
                        s = value.decode("utf-8", errors="ignore")
                    except Exception:
                        s = str(value)
                else:
                    s = str(value)
                s = s.strip()
                m = re.match(r"^[bB][\"\'](.*)[\"\']$", s)
                if m:
                    s = m.group(1).strip()
                return s or default

            def _derive_site(row):
                s = _to_str(row.get("domain")) or _to_str(row.get("site"))
                if s:
                    return s.lower()
                ek = _to_str(row.get("entity_key"))
                return (ek.split("|", 1)[0] if "|" in ek else ek).lower() or "unknown"

            def compute_join_status(row):
                meta_active = _to_float(row.get("meta_spend", 0)) > 0
                adsense_active = _to_float(row.get("adsense_estimated_earnings", 0)) > 0
                adx_active = _to_float(row.get("adx_revenue", 0)) > 0
                if meta_active and (adsense_active or adx_active):
                    return "OK"
                if not meta_active and adsense_active and adx_active:
                    return "SOURCE_ONLY_NO_META"
                if not meta_active and adsense_active:
                    return "SOURCE_ONLY_NO_META_ADSENSE"
                if not meta_active and adx_active:
                    return "SOURCE_ONLY_NO_META_ADX"
                return "DATA_INCOMPLETE"

            def _to_uint(value, default=0):
                try:
                    v = _to_float(value, default)
                    if pd.isna(v):
                        return int(default)
                    return int(max(0, v))
                except Exception:
                    return int(default)

            # =========================
            # BUILD DATAFRAME
            # =========================
            data = []
            for row in rows:
                adsense_active = _to_float(row.get("adsense_estimated_earnings", 0)) > 0
                adx_active = _to_float(row.get("adx_revenue", 0)) > 0
                mapped_revenue_source = compute_mapped_revenue_source(
                    mapped_mode=row.get("mapped_revenue_source"),
                    adsense_active=adsense_active,
                    adx_active=adx_active,
                )
                join_status = compute_join_status(row)
                # 🚨 skip data non-scorable
                if join_status not in SCORABLE_JOIN_STATUSES:
                    continue
                run_hour = _to_uint(row.get("run_hour", 0))
                if run_hour > 23:
                    run_hour = 23
                run_time_raw = row.get("run_time")
                run_time = str(run_time_raw).strip() if run_time_raw is not None else ""
                if " " in run_time:
                    run_time = run_time.split(" ")[-1]
                if len(run_time) == 5:
                    run_time = f"{run_time}:00"
                if not run_time:
                    run_time = f"{run_hour:02d}:00:00"

                date_raw = row.get("run_date") or row.get("date")
                dt = pd.to_datetime(date_raw, errors="coerce")
                safe_date = dt.date() if pd.notna(dt) else now.date()
                run_time_dt = pd.to_datetime(f"{safe_date.isoformat()} {run_time}", errors="coerce")
                if pd.isna(run_time_dt):
                    run_time_dt = datetime.combine(safe_date, dt_time(hour=run_hour, minute=0, second=0))

                data.append({
                    "batch_id": _to_str(ensure_uuid()),
                    "run_time": run_time_dt,
                    "run_date": safe_date,
                    "run_hour": run_hour,
                    "site": _derive_site(row),
                    "entity_key": _to_str(row.get("entity_key")).upper(),
                    "country_code": _to_str(row.get("country_code")).upper(),
                    "country_name": _to_str(row.get("country_name")).upper(),
                    "date": safe_date,
                    # ⭐ GENERATED FIELDS
                    "mapped_revenue_source": _to_str(mapped_revenue_source).upper(),
                    "join_status": _to_str(join_status).upper(),
                    # META
                    "meta_spend": _to_float(row.get("meta_spend", 0)),
                    "meta_daily_budget": _to_float(row.get("meta_daily_budget", 0)),
                    "meta_campaign": _to_str(row.get("meta_campaign")).upper(),
                    "meta_cpc": _to_float(row.get("meta_cpc", 0)),
                    "meta_clicks": _to_uint(row.get("meta_clicks", 0)),
                    "meta_lpv": _to_uint(row.get("meta_lpv", 0)),
                    "meta_lpv_rate": _to_float(row.get("meta_lpv_rate", 0)),
                    "meta_frequency": _to_float(row.get("meta_frequency", 0)),
                    # ADX
                    "adx_revenue": _to_float(row.get("adx_revenue", 0)),
                    "adx_impressions": _to_uint(row.get("adx_impressions", 0)),
                    "adx_clicks": _to_uint(row.get("adx_clicks", 0)),
                    "adx_ecpm": _to_float(row.get("adx_ecpm", 0)),
                    "adx_cpc": _to_float(row.get("adx_cpc", 0)),
                    "adx_requests": _to_uint(row.get("adx_requests", 0)),
                    "adx_responses_served": _to_uint(row.get("adx_responses_served", 0)),
                    "adx_match_rate": _to_float(row.get("adx_match_rate", 0)),
                    "adx_fill_rate": _to_float(row.get("adx_fill_rate", 0)),
                    "adx_active_view_pct_viewable": _to_float(row.get("adx_active_view_pct_viewable", 0)),
                    "adx_active_view_avg_time_sec": _to_float(row.get("adx_active_view_avg_time_sec", 0)),
                    # ADSENSE
                    "adsense_estimated_earnings": _to_float(row.get("adsense_estimated_earnings", 0)),
                    "adsense_page_views": _to_uint(row.get("adsense_page_views", 0)),
                    "adsense_clicks": _to_uint(row.get("adsense_clicks", 0)),
                    "adsense_cost_per_click": _to_float(row.get("adsense_cost_per_click", 0)),
                    "adsense_page_views_rpm": _to_float(row.get("adsense_page_views_rpm", 0)),
                    "adsense_ad_requests": _to_uint(row.get("adsense_ad_requests", 0)),
                    "adsense_impressions": _to_uint(row.get("adsense_impressions", 0)),
                    "adsense_active_view_viewability": _to_uint(row.get("adsense_active_view_viewability", 0)),
                    "adsense_active_view_measurability": _to_float(row.get("adsense_active_view_measurability", 0)),
                    "adsense_active_view_time": _to_float(row.get("adsense_active_view_time", 0)),
                    # SUMMARY
                    "total_revenue": _to_float(row.get("total_revenue", 0)),
                    "profit": _to_float(row.get("profit", 0)),
                    "roas": _to_float(row.get("roas", 0)),
                    "mdd": now,
                })
            if not data:
                return {"status": True, "message": "No scorable rows"}
            df = pd.DataFrame(data)
            # =========================
            # INSERT CLICKHOUSE
            # =========================
            if clickhouse_connect is None:
                raise RuntimeError("clickhouse_connect library is not installed")
            client = get_clickhouse_client()
            client.insert_df("hris_trendHorizone.fact_join_hourly", df)
            return {
                "status": True,
                "message": f"Inserted {len(df)} rows"
            }
        except Exception as e:
            return {
                "status": False,
                "error": str(e)
            }
        
    def delete_fact_join_hourly_by_date(self, tanggal):
        """
        Menghapus data dari ClickHouse fact_join_hourly berdasarkan tanggal.
        """
        try:
            if clickhouse_connect is None:
                raise RuntimeError('clickhouse_connect library is not installed')
            client = get_clickhouse_client()
            sql = f"DELETE FROM hris_trendHorizone.fact_join_hourly WHERE date = toDate('{tanggal}')"
            client.command(sql)
            return {"status": True, "message": f"Deleted data for date {tanggal}"}
        except Exception as e:
            return {"status": False, "error": str(e)}

    def get_fact_join_hourly_active_days_map(self, end_date, sites=None):
        """
        Mengambil umur domain (hari aktif) dari fact_join_hourly per site.
        """
        try:
            try:
                end_ymd = datetime.strptime(str(end_date), "%Y-%m-%d").strftime("%Y-%m-%d")
            except Exception:
                end_ymd = datetime.now().strftime("%Y-%m-%d")

            normalized_sites = []
            seen = set()
            for site in (sites or []):
                s = str(site or "").strip().lower()
                if not s or s == "unknown" or s in seen:
                    continue
                seen.add(s)
                normalized_sites.append(s)

            if not normalized_sites:
                return {"status": True, "data": {}}

            where_parts = []
            for s in normalized_sites:
                esc = str(s).replace("'", "''")
                where_parts.append(f"lower(site) = '{esc}'")
                where_parts.append(f"lower(site) LIKE '%.{esc}'")
                where_parts.append(f"lower(site) LIKE '{esc}.%'")
                where_parts.append(f"lower(site) LIKE '%.{esc}.%'")
            where_sql = " OR ".join(where_parts) if where_parts else "1 = 0"
            sql = f"""
                SELECT
                    lower(site) AS site_key,
                    min(toDate(date)) AS first_seen_date,
                    max(toDate(date)) AS last_seen_date
                FROM hris_trendHorizone.fact_join_hourly
                WHERE date <= toDate('{end_ymd}')
                  AND site != ''
                  AND ({where_sql})
                GROUP BY site_key
            """
            try:
                df = query_df(sql)
            except Exception:
                # Fallback ke HTTP cursor jika driver clickhouse_connect bermasalah.
                host, port, user, password, database = _clickhouse_http_config()
                cur = ClickHouseHttpCursor(host=host, port=port, user=user, password=password, database=database)
                cur.execute(sql)
                rows = cur.fetchall() or []
                df = pd.DataFrame(rows)
            first_last_by_requested = {}
            if df is not None and not df.empty:
                for _, row in df.iterrows():
                    site_key = str(row.get("site_key") or "").strip().lower()
                    if not site_key:
                        continue
                    first_seen = row.get("first_seen_date")
                    last_seen = row.get("last_seen_date")
                    for req in normalized_sites:
                        if (
                            site_key == req
                            or site_key.endswith("." + req)
                            or site_key.startswith(req + ".")
                            or ("." + req + ".") in site_key
                            or req.endswith("." + site_key)
                        ):
                            current = first_last_by_requested.get(req)
                            if current is None:
                                first_last_by_requested[req] = {"first_seen": first_seen, "last_seen": last_seen}
                            else:
                                cur_first = current.get("first_seen")
                                cur_last = current.get("last_seen")
                                if first_seen is not None and (cur_first is None or first_seen < cur_first):
                                    current["first_seen"] = first_seen
                                if last_seen is not None and (cur_last is None or last_seen > cur_last):
                                    current["last_seen"] = last_seen
            result = {}
            for req in normalized_sites:
                pair = first_last_by_requested.get(req) or {}
                fs = pair.get("first_seen")
                ls = pair.get("last_seen")
                if fs is None or ls is None:
                    result[req] = 0
                    continue
                try:
                    fs_dt = fs if hasattr(fs, "year") else datetime.strptime(str(fs)[:10], "%Y-%m-%d").date()
                    ls_dt = ls if hasattr(ls, "year") else datetime.strptime(str(ls)[:10], "%Y-%m-%d").date()
                    result[req] = max((ls_dt - fs_dt).days, 0)
                except Exception:
                    result[req] = 0
            return {"status": True, "data": result}
        except Exception as e:
            return {"status": False, "error": str(e), "data": {}}

    def _report_account_domain_key_sql(self, column_expr):
        return self._domain_join_sql_key_expr(column_expr)

    def _report_account_normalize_campaign_domain(self, raw_value):
        """Normalisasi domain dari campaign FB (buang suffix .ADX/.DISP/.display)."""
        s = self._normalize_domain_full(raw_value)
        if not s:
            return ''
        for suffix in ('.adx', '.disp', '.display'):
            if s.endswith(suffix):
                s = s[: -len(suffix)]
                break
        return self._normalize_domain_match_key(s)

    def _report_account_fetch_email_cred_ids(self):
        """Map email app_credentials -> account_id."""
        sql = """
            SELECT LOWER(TRIM(ac.user_mail)) AS user_mail, ac.account_id
            FROM app_credentials ac
            WHERE COALESCE(ac.is_active, '1') = '1'
              AND TRIM(COALESCE(ac.user_mail, '')) <> ''
        """
        self.cur_hris.execute(sql)
        out = {}
        for row in (self.cur_hris.fetchall() or []):
            email = str(row.get('user_mail') or '').strip().lower()
            cid = str(row.get('account_id') or '').strip()
            if email and cid:
                out.setdefault(email, set()).add(cid)
        return out

    def _report_account_cred_ids_for_account(self, acct, owner_cred_map, email_cred_map):
        cred_ids = set()
        owner_id = str(acct.get('account_owner') or '').strip()
        if owner_id:
            cred_ids |= set(owner_cred_map.get(owner_id) or set())
        email = str(acct.get('account_email') or '').strip().lower()
        if email:
            cred_ids |= set(email_cred_map.get(email) or set())
        return cred_ids

    def _report_account_fuzzy_adx_keys(self, domain_key, adx_map):
        domain_key = str(domain_key or '').strip()
        if not domain_key:
            return set()
        keys = {domain_key}
        prefix = domain_key + '.'
        for candidate in (adx_map or {}).keys():
            if candidate == domain_key:
                continue
            if candidate.startswith(prefix) or domain_key.startswith(candidate + '.'):
                keys.add(candidate)
        return keys

    def _report_account_lookup_adx_map_amount(self, domain_key, adx_map, adx_by_cred, cred_ids):
        domain_key = str(domain_key or '').strip()
        if not domain_key:
            return 0.0
        cred_ids = [str(c).strip() for c in (cred_ids or []) if str(c).strip()]
        total = 0.0
        for fk in self._report_account_fuzzy_adx_keys(domain_key, adx_map):
            if cred_ids:
                scoped = 0.0
                for cid in cred_ids:
                    scoped += float((adx_by_cred.get(cid) or {}).get(fk) or 0)
                if scoped == 0.0:
                    scoped = float(adx_map.get(fk) or 0)
                total += scoped
            else:
                total += float(adx_map.get(fk) or 0)
        return total

    def _report_account_register_domain_revenue(self, global_map, by_cred_map, raw_domain, rev, account_id=None):
        dk = self._normalize_domain_match_key(raw_domain)
        full = self._normalize_domain_full(raw_domain)
        keys = set()
        if dk:
            keys.add(dk)
        if full and full != dk:
            keys.add(full)
        if not keys:
            return
        rev = float(rev or 0)
        cid = str(account_id or '').strip()
        for k in keys:
            global_map[k] = global_map.get(k, 0.0) + rev
            if cid:
                bucket = by_cred_map.setdefault(cid, {})
                bucket[k] = bucket.get(k, 0.0) + rev

    def _report_account_fetch_adx_revenue_by_account(self, start_date, end_date, account_keys=None):
        """Sum AdX revenue per FB account via domain join key (selaras invalid report)."""
        fb_key = self._report_account_fb_key_sql('c.account_ads_id')
        fb_dom = self._domain_join_sql_key_expr('c.data_ads_domain')
        adx_dom = self._domain_join_sql_key_expr('d.data_adx_domain')
        camp_where = (
            "DATE(c.data_ads_tanggal) BETWEEN %s AND %s"
            " AND TRIM(COALESCE(c.data_ads_domain, '')) <> ''"
        )
        params = [start_date, end_date]
        if account_keys:
            keys = [str(k).strip() for k in account_keys if str(k).strip()]
            if keys:
                placeholders = ','.join(['%s'] * len(keys))
                camp_where += f" AND {fb_key} IN ({placeholders})"
                params.extend(keys)
        params.extend([start_date, end_date])
        sql = f"""
            SELECT camps.account_key,
                   COALESCE(SUM(adx.adx_rev), 0) AS adx_revenue
            FROM (
                SELECT DISTINCT {fb_key} AS account_key, {fb_dom} AS domain_key,
                       DATE(c.data_ads_tanggal) AS d
                FROM data_ads_campaign c
                WHERE {camp_where}
            ) camps
            INNER JOIN (
                SELECT {adx_dom} AS domain_key, DATE(d.data_adx_domain_tanggal) AS d,
                       COALESCE(SUM(CAST(d.data_adx_domain_revenue AS DECIMAL(18,4))), 0) AS adx_rev
                FROM data_adx_domain d
                WHERE DATE(d.data_adx_domain_tanggal) BETWEEN %s AND %s
                GROUP BY domain_key, d
            ) adx ON adx.domain_key = camps.domain_key AND adx.d = camps.d
            GROUP BY camps.account_key
        """
        self.cur_hris.execute(sql, tuple(params))
        out = {}
        for row in (self.cur_hris.fetchall() or []):
            k = str(row.get('account_key') or '').strip()
            if k:
                out[k] = float(row.get('adx_revenue') or 0)
        return out

    def _report_account_fetch_adx_revenue_map_for_account(self, start_date, end_date, account_key):
        fb_key = self._report_account_fb_key_sql('c.account_ads_id')
        fb_dom = self._domain_join_sql_key_expr('c.data_ads_domain')
        adx_dom = self._domain_join_sql_key_expr('d.data_adx_domain')
        sql = f"""
            SELECT camps.domain_key,
                   COALESCE(SUM(adx.adx_rev), 0) AS adx_revenue
            FROM (
                SELECT DISTINCT {fb_dom} AS domain_key, DATE(c.data_ads_tanggal) AS d
                FROM data_ads_campaign c
                WHERE DATE(c.data_ads_tanggal) BETWEEN %s AND %s
                  AND {fb_key} = %s
                  AND TRIM(COALESCE(c.data_ads_domain, '')) <> ''
            ) camps
            INNER JOIN (
                SELECT {adx_dom} AS domain_key, DATE(d.data_adx_domain_tanggal) AS d,
                       COALESCE(SUM(CAST(d.data_adx_domain_revenue AS DECIMAL(18,4))), 0) AS adx_rev
                FROM data_adx_domain d
                WHERE DATE(d.data_adx_domain_tanggal) BETWEEN %s AND %s
                GROUP BY domain_key, d
            ) adx ON adx.domain_key = camps.domain_key AND adx.d = camps.d
            GROUP BY camps.domain_key
        """
        self.cur_hris.execute(sql, (start_date, end_date, account_key, start_date, end_date))
        out = {}
        for row in (self.cur_hris.fetchall() or []):
            k = str(row.get('domain_key') or '').strip()
            if k:
                out[k] = float(row.get('adx_revenue') or 0)
        return out

    def _report_account_fetch_adx_revenue_daily_for_accounts(self, start_date, end_date, account_keys=None):
        fb_key = self._report_account_fb_key_sql('c.account_ads_id')
        fb_dom = self._domain_join_sql_key_expr('c.data_ads_domain')
        adx_dom = self._domain_join_sql_key_expr('d.data_adx_domain')
        camp_where = (
            "DATE(c.data_ads_tanggal) BETWEEN %s AND %s"
            " AND TRIM(COALESCE(c.data_ads_domain, '')) <> ''"
        )
        params = [start_date, end_date]
        if account_keys:
            keys = [str(k).strip() for k in account_keys if str(k).strip()]
            if keys:
                placeholders = ','.join(['%s'] * len(keys))
                camp_where += f" AND {fb_key} IN ({placeholders})"
                params.extend(keys)
        params.extend([start_date, end_date])
        sql = f"""
            SELECT camps.d AS d, camps.account_key,
                   COALESCE(SUM(adx.adx_rev), 0) AS adx_revenue
            FROM (
                SELECT DISTINCT {fb_key} AS account_key, {fb_dom} AS domain_key,
                       DATE(c.data_ads_tanggal) AS d
                FROM data_ads_campaign c
                WHERE {camp_where}
            ) camps
            INNER JOIN (
                SELECT {adx_dom} AS domain_key, DATE(d.data_adx_domain_tanggal) AS d,
                       COALESCE(SUM(CAST(d.data_adx_domain_revenue AS DECIMAL(18,4))), 0) AS adx_rev
                FROM data_adx_domain d
                WHERE DATE(d.data_adx_domain_tanggal) BETWEEN %s AND %s
                GROUP BY domain_key, d
            ) adx ON adx.domain_key = camps.domain_key AND adx.d = camps.d
            GROUP BY camps.d, camps.account_key
            ORDER BY camps.d ASC
        """
        self.cur_hris.execute(sql, tuple(params))
        out = {}
        for row in (self.cur_hris.fetchall() or []):
            d = row.get('d')
            if hasattr(d, 'isoformat'):
                d = d.isoformat()
            else:
                d = str(d or '')[:10]
            ak = str(row.get('account_key') or '').strip()
            if not d or not ak:
                continue
            out.setdefault(d, {})
            out[d][ak] = float(row.get('adx_revenue') or 0)
        return out

    def _report_account_fetch_owner_cred_ids(self):
        """Map app_users.user_id -> set(app_credentials.account_id) untuk scope revenue AdX/AdSense."""
        sql = """
            SELECT ca.user_id, ca.account_id
            FROM app_credentials_assign ca
            INNER JOIN app_credentials ac ON ac.account_id = ca.account_id
            WHERE COALESCE(ac.is_active, '1') = '1'
        """
        self.cur_hris.execute(sql)
        out = {}
        for row in (self.cur_hris.fetchall() or []):
            uid = str(row.get('user_id') or '').strip()
            cid = str(row.get('account_id') or '').strip()
            if uid and cid:
                out.setdefault(uid, set()).add(cid)
        return out

    def _report_account_build_revenue_maps(self, table, date_col, revenue_col, domain_col, start_date, end_date):
        """
        Agregasi revenue per domain_key (Python normalize, selaras ROI/invalid report).
        Return: (global_map, by_cred_map)
        """
        sql = f"""
            SELECT account_id, {domain_col} AS raw_domain,
                   COALESCE(SUM(CAST({revenue_col} AS DECIMAL(18,4))), 0) AS revenue
            FROM {table}
            WHERE DATE({date_col}) BETWEEN %s AND %s
            GROUP BY account_id, raw_domain
        """
        self.cur_hris.execute(sql, (start_date, end_date))
        global_map = {}
        by_cred_map = {}
        for row in (self.cur_hris.fetchall() or []):
            self._report_account_register_domain_revenue(
                global_map, by_cred_map, row.get('raw_domain'), row.get('revenue'), row.get('account_id')
            )
        return global_map, by_cred_map

    def _report_account_build_revenue_daily_maps(self, table, date_col, revenue_col, domain_col, start_date, end_date):
        sql = f"""
            SELECT DATE({date_col}) AS d, account_id, {domain_col} AS raw_domain,
                   COALESCE(SUM(CAST({revenue_col} AS DECIMAL(18,4))), 0) AS revenue
            FROM {table}
            WHERE DATE({date_col}) BETWEEN %s AND %s
            GROUP BY d, account_id, raw_domain
            ORDER BY d ASC
        """
        self.cur_hris.execute(sql, (start_date, end_date))
        global_out = {}
        by_cred_out = {}
        for row in (self.cur_hris.fetchall() or []):
            d = row.get('d')
            if hasattr(d, 'isoformat'):
                d = d.isoformat()
            else:
                d = str(d or '')[:10]
            if not d:
                continue
            rev = float(row.get('revenue') or 0)
            cid = str(row.get('account_id') or '').strip()
            keys = set()
            dk = self._normalize_domain_match_key(row.get('raw_domain'))
            full = self._normalize_domain_full(row.get('raw_domain'))
            if dk:
                keys.add(dk)
            if full and full != dk:
                keys.add(full)
            if not keys:
                continue
            global_out.setdefault(d, {})
            for k in keys:
                global_out[d][k] = global_out[d].get(k, 0.0) + rev
                if cid:
                    by_cred_out.setdefault(cid, {})
                    by_cred_out[cid].setdefault(d, {})
                    by_cred_out[cid][d][k] = by_cred_out[cid][d].get(k, 0.0) + rev
        return global_out, by_cred_out

    def _report_account_sum_adx_revenue(self, domain_keys, adx_global, adx_by_cred, cred_ids):
        domains = [str(d).strip() for d in (domain_keys or []) if str(d).strip()]
        if not domains:
            return 0.0
        all_fk = set()
        for dk in domains:
            all_fk |= self._report_account_fuzzy_adx_keys(dk, adx_global)
        total = 0.0
        for fk in all_fk:
            total += self._report_account_lookup_adx_map_amount(fk, adx_global, adx_by_cred, cred_ids)
        return total

    def _report_account_sum_adx_revenue_daily(self, date_key, domain_keys, adx_daily_global, adx_daily_by_cred, cred_ids):
        domains = [str(d).strip() for d in (domain_keys or []) if str(d).strip()]
        if not domains:
            return 0.0
        day_global = adx_daily_global.get(date_key) or {}
        all_fk = set()
        for dk in domains:
            all_fk |= self._report_account_fuzzy_adx_keys(dk, day_global)
        total = 0.0
        for fk in all_fk:
            cred_ids_list = [str(c).strip() for c in (cred_ids or []) if str(c).strip()]
            if cred_ids_list:
                scoped = 0.0
                for cid in cred_ids_list:
                    scoped += float(((adx_daily_by_cred.get(cid) or {}).get(date_key) or {}).get(fk) or 0)
                if scoped == 0.0:
                    scoped = float(day_global.get(fk) or 0)
                total += scoped
            else:
                total += float(day_global.get(fk) or 0)
        return total

    def _report_account_build_rekap_revenue_maps(self, table, domain_col, revenue_col, year, month, tanggal_tarik):
        if table == 'data_adsense_rekap':
            sql = f"""
                SELECT account_id, {domain_col} AS raw_domain,
                       COALESCE(SUM(CAST({revenue_col} AS DECIMAL(18,4))), 0) AS revenue
                FROM {table}
                WHERE data_adsense_rekap_tahun = %s
                  AND data_adsense_rekap_bulan = %s
                  AND data_adsense_rekap_tanggal = %s
                GROUP BY account_id, raw_domain
            """
        else:
            sql = f"""
                SELECT account_id, {domain_col} AS raw_domain,
                       COALESCE(SUM(CAST({revenue_col} AS DECIMAL(18,4))), 0) AS revenue
                FROM {table}
                WHERE data_adx_rekap_tahun = %s
                  AND data_adx_rekap_bulan = %s
                  AND data_adx_rekap_tanggal = %s
                GROUP BY account_id, raw_domain
            """
        self.cur_hris.execute(sql, (str(year), str(month).zfill(2), tanggal_tarik))
        global_map = {}
        by_cred_map = {}
        for row in (self.cur_hris.fetchall() or []):
            self._report_account_register_domain_revenue(
                global_map, by_cred_map, row.get('raw_domain'), row.get('revenue'), row.get('account_id')
            )
        return global_map, by_cred_map

    def _report_account_fb_key_sql(self, column_expr):
        return f"REPLACE(LOWER(TRIM({column_expr})), 'act_', '')"

    def _report_account_fetch_accounts(self, account_q=None):
        params = []
        sql = [
            "SELECT account_ads_id, account_id, account_name, account_email, account_owner",
            "FROM master_account_ads",
            "WHERE 1=1",
        ]
        q = str(account_q or '').strip()
        if q:
            sql.append("AND (account_name LIKE %s OR account_email LIKE %s OR CAST(account_id AS CHAR) LIKE %s)")
            like = f"%{q}%"
            params.extend([like, like, like])
        sql.append("ORDER BY account_name ASC")
        self.cur_hris.execute("\n".join(sql), tuple(params))
        rows = self.cur_hris.fetchall() or []
        accounts = []
        for row in rows:
            acct_key = self._normalize_fb_account_key(row.get('account_id') or row.get('account_ads_id'))
            if not acct_key:
                continue
            accounts.append({
                'account_ads_id': row.get('account_ads_id'),
                'account_id': row.get('account_id'),
                'account_key': acct_key,
                'account_name': str(row.get('account_name') or '').strip() or str(row.get('account_id') or '-'),
                'account_email': str(row.get('account_email') or '').strip(),
                'account_owner': row.get('account_owner'),
            })
        return accounts

    def search_report_account_suggest(self, q, limit=20):
        q = str(q or '').strip()
        if len(q) < 3:
            return {'status': True, 'data': []}
        try:
            limit = max(1, min(int(limit or 20), 50))
            like = f"%{q}%"
            sql = """
                SELECT account_ads_id, account_id, account_name, account_email
                FROM master_account_ads
                WHERE account_name LIKE %s
                   OR account_email LIKE %s
                   OR CAST(account_id AS CHAR) LIKE %s
                ORDER BY account_name ASC
                LIMIT %s
            """
            self.cur_hris.execute(sql, (like, like, like, limit))
            rows = self.cur_hris.fetchall() or []
            data = []
            seen = set()
            for row in rows:
                name = str(row.get('account_name') or '').strip()
                if not name or name.lower() in seen:
                    continue
                seen.add(name.lower())
                email = str(row.get('account_email') or '').strip()
                data.append({
                    'account_ads_id': row.get('account_ads_id'),
                    'account_id': row.get('account_id'),
                    'account_name': name,
                    'account_email': email,
                })
            return {'status': True, 'data': data}
        except Exception as e:
            return {'status': False, 'data': str(e)}

    def _report_account_list_rekap_tarik_dates(self, year, month):
        self.cur_hris.execute(
            """
            SELECT DISTINCT data_ads_rekap_tanggal AS tanggal_tarik
            FROM data_ads_rekap
            WHERE data_ads_rekap_tahun = %s AND data_ads_rekap_bulan = %s
            ORDER BY data_ads_rekap_tanggal DESC
            """,
            (str(year), str(month).zfill(2)),
        )
        out = []
        for row in (self.cur_hris.fetchall() or []):
            val = row.get('tanggal_tarik')
            if hasattr(val, 'isoformat'):
                out.append(val.isoformat())
            elif val:
                out.append(str(val).strip())
        return out

    def _report_account_fetch_spend_by_account(self, start_date, end_date):
        key_expr = self._report_account_fb_key_sql('b.account_ads_id')
        sql = f"""
            SELECT {key_expr} AS account_key,
                   COALESCE(SUM(CAST(b.data_ads_spend AS DECIMAL(18,4))), 0) AS spend
            FROM data_ads_campaign b
            WHERE DATE(b.data_ads_tanggal) BETWEEN %s AND %s
            GROUP BY account_key
        """
        self.cur_hris.execute(sql, (start_date, end_date))
        out = {}
        for row in (self.cur_hris.fetchall() or []):
            k = str(row.get('account_key') or '').strip()
            if k:
                out[k] = float(row.get('spend') or 0)
        return out

    def _report_account_fetch_spend_daily(self, start_date, end_date, account_keys=None):
        key_expr = self._report_account_fb_key_sql('b.account_ads_id')
        sql = [
            f"SELECT DATE(b.data_ads_tanggal) AS d, {key_expr} AS account_key,",
            "COALESCE(SUM(CAST(b.data_ads_spend AS DECIMAL(18,4))), 0) AS spend",
            "FROM data_ads_campaign b",
            "WHERE DATE(b.data_ads_tanggal) BETWEEN %s AND %s",
        ]
        params = [start_date, end_date]
        keys = [str(k).strip() for k in (account_keys or []) if str(k).strip()]
        if keys:
            placeholders = ','.join(['%s'] * len(keys))
            sql.append(f"AND {key_expr} IN ({placeholders})")
            params.extend(keys)
        sql.append("GROUP BY d, account_key ORDER BY d ASC")
        self.cur_hris.execute("\n".join(sql), tuple(params))
        out = {}
        for row in (self.cur_hris.fetchall() or []):
            d = row.get('d')
            if hasattr(d, 'isoformat'):
                d = d.isoformat()
            else:
                d = str(d or '')[:10]
            k = str(row.get('account_key') or '').strip()
            if not d or not k:
                continue
            out.setdefault(d, {})
            out[d][k] = float(row.get('spend') or 0)
        return out

    def _report_account_fetch_domain_map(self, start_date, end_date):
        key_expr = self._report_account_fb_key_sql('b.account_ads_id')
        sql = f"""
            SELECT DISTINCT {key_expr} AS account_key, b.data_ads_domain AS raw_domain
            FROM data_ads_campaign b
            WHERE DATE(b.data_ads_tanggal) BETWEEN %s AND %s
              AND b.data_ads_domain IS NOT NULL
              AND TRIM(b.data_ads_domain) <> ''
        """
        self.cur_hris.execute(sql, (start_date, end_date))
        account_domains = {}
        domain_accounts = {}
        for row in (self.cur_hris.fetchall() or []):
            ak = str(row.get('account_key') or '').strip()
            dk = self._report_account_normalize_campaign_domain(row.get('raw_domain'))
            if not ak or not dk:
                continue
            account_domains.setdefault(ak, set()).add(dk)
            domain_accounts.setdefault(dk, set()).add(ak)
        return account_domains, domain_accounts

    def _report_account_fetch_revenue_by_domain(self, table, date_col, revenue_col, domain_col, start_date, end_date):
        dom_expr = self._report_account_domain_key_sql(domain_col)
        sql = f"""
            SELECT {dom_expr} AS domain_key,
                   COALESCE(SUM(CAST({revenue_col} AS DECIMAL(18,4))), 0) AS revenue
            FROM {table}
            WHERE DATE({date_col}) BETWEEN %s AND %s
            GROUP BY domain_key
        """
        self.cur_hris.execute(sql, (start_date, end_date))
        out = {}
        for row in (self.cur_hris.fetchall() or []):
            k = str(row.get('domain_key') or '').strip()
            if k:
                out[k] = float(row.get('revenue') or 0)
        return out

    def _report_account_fetch_revenue_daily_by_domain(self, table, date_col, revenue_col, domain_col, start_date, end_date):
        dom_expr = self._report_account_domain_key_sql(domain_col)
        sql = f"""
            SELECT DATE({date_col}) AS d, {dom_expr} AS domain_key,
                   COALESCE(SUM(CAST({revenue_col} AS DECIMAL(18,4))), 0) AS revenue
            FROM {table}
            WHERE DATE({date_col}) BETWEEN %s AND %s
            GROUP BY d, domain_key
            ORDER BY d ASC
        """
        self.cur_hris.execute(sql, (start_date, end_date))
        out = {}
        for row in (self.cur_hris.fetchall() or []):
            d = row.get('d')
            if hasattr(d, 'isoformat'):
                d = d.isoformat()
            else:
                d = str(d or '')[:10]
            k = str(row.get('domain_key') or '').strip()
            if not d or not k:
                continue
            out.setdefault(d, {})
            out[d][k] = out[d].get(k, 0.0) + float(row.get('revenue') or 0)
        return out

    def _report_account_fetch_rekap_spend_by_account(self, year, month, tanggal_tarik):
        key_expr = self._report_account_fb_key_sql('account_ads_id')
        sql = f"""
            SELECT {key_expr} AS account_key,
                   COALESCE(SUM(CAST(data_ads_rekap_spend AS DECIMAL(18,4))), 0) AS spend
            FROM data_ads_rekap
            WHERE data_ads_rekap_tahun = %s
              AND data_ads_rekap_bulan = %s
              AND data_ads_rekap_tanggal = %s
            GROUP BY account_key
        """
        self.cur_hris.execute(sql, (str(year), str(month).zfill(2), tanggal_tarik))
        out = {}
        for row in (self.cur_hris.fetchall() or []):
            k = str(row.get('account_key') or '').strip()
            if k:
                out[k] = float(row.get('spend') or 0)
        return out

    def _report_account_fetch_rekap_revenue_by_domain(self, table, domain_col, revenue_col, year, month, tanggal_tarik):
        dom_expr = self._report_account_domain_key_sql(domain_col)
        sql = f"""
            SELECT {dom_expr} AS domain_key,
                   COALESCE(SUM(CAST({revenue_col} AS DECIMAL(18,4))), 0) AS revenue
            FROM {table}
            WHERE data_adx_rekap_tahun = %s AND data_adx_rekap_bulan = %s AND data_adx_rekap_tanggal = %s
            GROUP BY domain_key
        """
        if table == 'data_adsense_rekap':
            sql = f"""
                SELECT {dom_expr} AS domain_key,
                       COALESCE(SUM(CAST({revenue_col} AS DECIMAL(18,4))), 0) AS revenue
                FROM {table}
                WHERE data_adsense_rekap_tahun = %s
                  AND data_adsense_rekap_bulan = %s
                  AND data_adsense_rekap_tanggal = %s
                GROUP BY domain_key
            """
        self.cur_hris.execute(sql, (str(year), str(month).zfill(2), tanggal_tarik))
        out = {}
        for row in (self.cur_hris.fetchall() or []):
            k = str(row.get('domain_key') or '').strip()
            if k:
                out[k] = float(row.get('revenue') or 0)
        return out

    def _report_account_sum_revenue_for_account(self, domain_keys, revenue_map):
        total = 0.0
        for dk in (domain_keys or []):
            total += float(revenue_map.get(dk) or 0)
        return total

    def _report_account_build_row_metrics(self, spend, revenue, subdomain_count):
        profit = revenue - spend
        roi = ((profit / spend) * 100.0) if spend > 0 else 0.0
        return {
            'subdomain_count': int(subdomain_count or 0),
            'spend': round(spend, 2),
            'revenue': round(revenue, 2),
            'profit': round(profit, 2),
            'roi': round(roi, 2),
        }

    def _report_account_compare_block(self, daily_val, rekap_val):
        cmp = self._compare_rekap_metric(daily_val, rekap_val)
        return {
            'daily': round(float(cmp.get('daily') or 0), 2),
            'rekap': round(float(cmp.get('rekap') or 0), 2),
            'delta': round(float(cmp.get('delta') or 0), 2),
            'delta_pct': round(float(cmp.get('delta_pct') or 0), 2),
            'status': cmp.get('status') or 'missing',
        }

    def list_report_account_summary(
        self,
        start_date,
        end_date,
        account_q=None,
        compare_rekap=False,
        rekap_year=None,
        rekap_month=None,
        rekap_tanggal_tarik=None,
    ):
        import calendar
        from datetime import datetime as dt

        try:
            start_date = str(start_date or '').strip()[:10]
            end_date = str(end_date or '').strip()[:10]
            if not start_date or not end_date:
                return {'status': False, 'data': 'Rentang tanggal wajib diisi'}

            accounts = self._report_account_fetch_accounts(account_q)
            if not accounts:
                return {
                    'status': True,
                    'data': {
                        'period': {'start': start_date, 'end': end_date},
                        'compare_rekap': bool(compare_rekap),
                        'rekap': None,
                        'summary': self._report_account_build_row_metrics(0, 0, 0),
                        'rows': [],
                        'chart': [],
                    },
                }

            account_keys = [a['account_key'] for a in accounts]
            account_domains, _domain_accounts = self._report_account_fetch_domain_map(start_date, end_date)
            owner_cred_map = self._report_account_fetch_owner_cred_ids()
            email_cred_map = self._report_account_fetch_email_cred_ids()
            spend_map = self._report_account_fetch_spend_by_account(start_date, end_date)
            adx_by_account = self._report_account_fetch_adx_revenue_by_account(start_date, end_date, account_keys)
            adx_rev_map, adx_rev_by_cred = self._report_account_build_revenue_maps(
                'data_adx_domain', 'data_adx_domain_tanggal', 'data_adx_domain_revenue', 'data_adx_domain', start_date, end_date
            )
            adsense_rev_map, _adsense_rev_by_cred = self._report_account_build_revenue_maps(
                'data_adsense_domain', 'data_adsense_tanggal', 'data_adsense_revenue', 'data_adsense_domain', start_date, end_date
            )

            rekap_spend_map = {}
            rekap_adx_map = {}
            rekap_adsense_map = {}
            resolved_tarik = None
            available_tarik = []
            if compare_rekap and rekap_year and rekap_month:
                available_tarik = self._report_account_list_rekap_tarik_dates(rekap_year, rekap_month)
                resolved_tarik = str(rekap_tanggal_tarik or '').strip() or (available_tarik[0] if available_tarik else '')
                if resolved_tarik:
                    rekap_spend_map = self._report_account_fetch_rekap_spend_by_account(rekap_year, rekap_month, resolved_tarik)
                    rekap_adx_map, rekap_adx_by_cred = self._report_account_build_rekap_revenue_maps(
                        'data_adx_rekap', 'data_adx_rekap_domain', 'data_adx_rekap_revenue',
                        rekap_year, rekap_month, resolved_tarik,
                    )
                    rekap_adsense_map, _rekap_adsense_by_cred = self._report_account_build_rekap_revenue_maps(
                        'data_adsense_rekap', 'data_adsense_rekap_domain', 'data_adsense_rekap_revenue',
                        rekap_year, rekap_month, resolved_tarik,
                    )

            rows_out = []
            totals = {'subdomain_count': 0, 'spend': 0.0, 'revenue': 0.0, 'profit': 0.0}
            totals_rekap = {'subdomain_count': 0, 'spend': 0.0, 'revenue': 0.0, 'profit': 0.0}

            spend_daily = self._report_account_fetch_spend_daily(start_date, end_date, account_keys)
            adx_daily_by_account = self._report_account_fetch_adx_revenue_daily_for_accounts(
                start_date, end_date, account_keys
            )
            adsense_daily, _adsense_daily_by_cred = self._report_account_build_revenue_daily_maps(
                'data_adsense_domain', 'data_adsense_tanggal', 'data_adsense_revenue', 'data_adsense_domain', start_date, end_date
            )

            for acct in accounts:
                ak = acct['account_key']
                cred_ids = self._report_account_cred_ids_for_account(acct, owner_cred_map, email_cred_map)
                domains = set(account_domains.get(ak, set()))
                for cid in cred_ids:
                    domains |= set((adx_rev_by_cred.get(cid) or {}).keys())
                active_domains = set()
                account_spend = float(spend_map.get(ak) or 0)
                for dk in domains:
                    adx_hint = self._report_account_lookup_adx_map_amount(
                        dk, adx_rev_map, adx_rev_by_cred, cred_ids
                    )
                    if (
                        adx_hint > 0
                        or float(adsense_rev_map.get(dk) or 0) > 0
                        or account_spend > 0
                    ):
                        active_domains.add(dk)
                if not active_domains and domains:
                    active_domains = set(domains)

                adx_rev = float(adx_by_account.get(ak) or 0)
                if adx_rev <= 0 and active_domains:
                    adx_rev = self._report_account_sum_adx_revenue(
                        active_domains, adx_rev_map, adx_rev_by_cred, cred_ids
                    )
                adsense_rev = self._report_account_sum_revenue_for_account(active_domains, adsense_rev_map)
                revenue = adx_rev + adsense_rev
                spend = float(spend_map.get(ak) or 0)
                metrics = self._report_account_build_row_metrics(spend, revenue, len(active_domains))

                row = {
                    'account_ads_id': acct.get('account_ads_id'),
                    'account_id': acct.get('account_id'),
                    'account_key': ak,
                    'account_name': acct.get('account_name'),
                    'adx_revenue': round(adx_rev, 2),
                    'adsense_revenue': round(adsense_rev, 2),
                    **metrics,
                }

                if compare_rekap and resolved_tarik:
                    rekap_adx = self._report_account_sum_adx_revenue(active_domains, rekap_adx_map, rekap_adx_by_cred, cred_ids)
                    rekap_adsense = self._report_account_sum_revenue_for_account(active_domains, rekap_adsense_map)
                    rekap_revenue = rekap_adx + rekap_adsense
                    rekap_spend = float(rekap_spend_map.get(ak) or 0)
                    rekap_metrics = self._report_account_build_row_metrics(rekap_spend, rekap_revenue, len(active_domains))
                    row['compare'] = {
                        'spend': self._report_account_compare_block(metrics['spend'], rekap_metrics['spend']),
                        'revenue': self._report_account_compare_block(metrics['revenue'], rekap_metrics['revenue']),
                        'profit': self._report_account_compare_block(metrics['profit'], rekap_metrics['profit']),
                        'roi': self._report_account_compare_block(metrics['roi'], rekap_metrics['roi']),
                        'subdomain_count': self._report_account_compare_block(metrics['subdomain_count'], rekap_metrics['subdomain_count']),
                        'rekap_adx_revenue': round(rekap_adx, 2),
                        'rekap_adsense_revenue': round(rekap_adsense, 2),
                    }
                    totals_rekap['subdomain_count'] += rekap_metrics['subdomain_count']
                    totals_rekap['spend'] += rekap_metrics['spend']
                    totals_rekap['revenue'] += rekap_metrics['revenue']
                    totals_rekap['profit'] += rekap_metrics['profit']

                totals['subdomain_count'] += metrics['subdomain_count']
                totals['spend'] += metrics['spend']
                totals['revenue'] += metrics['revenue']
                totals['profit'] += metrics['profit']
                rows_out.append(row)

            rows_out.sort(key=lambda r: float(r.get('revenue') or 0), reverse=True)

            chart = []
            try:
                d0 = dt.strptime(start_date, '%Y-%m-%d').date()
                d1 = dt.strptime(end_date, '%Y-%m-%d').date()
                cur = d0
                while cur <= d1:
                    ds = cur.isoformat()
                    day_spend = 0.0
                    day_revenue = 0.0
                    for ak in account_keys:
                        day_spend += float((spend_daily.get(ds) or {}).get(ak) or 0)
                        day_revenue += float((adx_daily_by_account.get(ds) or {}).get(ak) or 0)
                        acct = next((a for a in accounts if a.get('account_key') == ak), None)
                        cred_ids = self._report_account_cred_ids_for_account(
                            acct or {}, owner_cred_map, email_cred_map
                        )
                        domains = set(account_domains.get(ak, set()))
                        for cid in cred_ids:
                            domains |= set((adx_rev_by_cred.get(cid) or {}).keys())
                        for dk in domains:
                            day_revenue += float((adsense_daily.get(ds) or {}).get(dk) or 0)
                    chart.append({
                        'date': ds,
                        'spend': round(day_spend, 2),
                        'revenue': round(day_revenue, 2),
                        'profit': round(day_revenue - day_spend, 2),
                    })
                    cur += timedelta(days=1)
            except Exception:
                chart = []

            summary = self._report_account_build_row_metrics(
                totals['spend'], totals['revenue'], totals['subdomain_count']
            )
            summary_block = {'daily': summary}
            if compare_rekap and resolved_tarik:
                rekap_summary = self._report_account_build_row_metrics(
                    totals_rekap['spend'], totals_rekap['revenue'], totals_rekap['subdomain_count']
                )
                summary_block['rekap'] = rekap_summary
                summary_block['compare'] = {
                    'spend': self._report_account_compare_block(summary['spend'], rekap_summary['spend']),
                    'revenue': self._report_account_compare_block(summary['revenue'], rekap_summary['revenue']),
                    'profit': self._report_account_compare_block(summary['profit'], rekap_summary['profit']),
                    'roi': self._report_account_compare_block(summary['roi'], rekap_summary['roi']),
                    'subdomain_count': self._report_account_compare_block(summary['subdomain_count'], rekap_summary['subdomain_count']),
                }

            return {
                'status': True,
                'data': {
                    'period': {'start': start_date, 'end': end_date},
                    'compare_rekap': bool(compare_rekap and resolved_tarik),
                    'rekap': {
                        'year': str(rekap_year or ''),
                        'month': str(rekap_month or '').zfill(2),
                        'tanggal_tarik': resolved_tarik,
                        'available_tarik_dates': available_tarik,
                    } if compare_rekap else None,
                    'summary': summary_block,
                    'rows': rows_out,
                    'chart': chart,
                },
            }
        except Exception as e:
            return {'status': False, 'data': str(e)}

    def _report_account_resolve_source_labels(self, adx_rev, adsense_rev, spend=0):
        labels = []
        if float(adx_rev or 0) > 0:
            labels.append('AdX')
        if float(adsense_rev or 0) > 0:
            labels.append('AdSense')
        if float(spend or 0) > 0 and not labels:
            labels.append('FB Ads')
        return labels

    def search_report_account_domain_suggest(self, account_key, q, start_date, end_date, limit=20):
        account_key = self._normalize_fb_account_key(account_key)
        q = str(q or '').strip().lower()
        if not account_key or len(q) < 2:
            return {'status': True, 'data': []}
        try:
            limit = max(1, min(int(limit or 20), 50))
            account_domains, _ = self._report_account_fetch_domain_map(start_date, end_date)
            domains = sorted(account_domains.get(account_key, set()))
            data = []
            for dk in domains:
                if q in dk.lower():
                    data.append({'domain': dk})
                    if len(data) >= limit:
                        break
            return {'status': True, 'data': data}
        except Exception as e:
            return {'status': False, 'data': str(e)}

    def get_report_account_detail(
        self,
        account_key,
        start_date,
        end_date,
        compare_rekap=False,
        rekap_year=None,
        rekap_month=None,
        rekap_tanggal_tarik=None,
        domain_q=None,
    ):
        try:
            account_key = self._normalize_fb_account_key(account_key)
            if not account_key:
                return {'status': False, 'data': 'Account tidak valid'}

            accounts = self._report_account_fetch_accounts()
            acct = next((a for a in accounts if a.get('account_key') == account_key), None)
            if not acct:
                return {'status': False, 'data': 'Account tidak ditemukan'}

            account_domains, _ = self._report_account_fetch_domain_map(start_date, end_date)
            owner_cred_map = self._report_account_fetch_owner_cred_ids()
            email_cred_map = self._report_account_fetch_email_cred_ids()
            cred_ids = self._report_account_cred_ids_for_account(acct, owner_cred_map, email_cred_map)
            adx_by_domain = self._report_account_fetch_adx_revenue_map_for_account(start_date, end_date, account_key)

            adx_rev_map, adx_rev_by_cred = self._report_account_build_revenue_maps(
                'data_adx_domain', 'data_adx_domain_tanggal', 'data_adx_domain_revenue', 'data_adx_domain', start_date, end_date
            )
            adsense_rev_map, _adsense_rev_by_cred = self._report_account_build_revenue_maps(
                'data_adsense_domain', 'data_adsense_tanggal', 'data_adsense_revenue', 'data_adsense_domain', start_date, end_date
            )

            domains = set(account_domains.get(account_key, set()))
            for cid in cred_ids:
                domains |= set((adx_rev_by_cred.get(cid) or {}).keys())
            domains = sorted(domains)

            key_expr = self._report_account_fb_key_sql('b.account_ads_id')
            sql = f"""
                SELECT b.data_ads_domain AS raw_domain,
                       COALESCE(SUM(CAST(b.data_ads_spend AS DECIMAL(18,4))), 0) AS spend
                FROM data_ads_campaign b
                WHERE DATE(b.data_ads_tanggal) BETWEEN %s AND %s
                  AND {key_expr} = %s
                GROUP BY raw_domain
            """
            self.cur_hris.execute(sql, (start_date, end_date, account_key))
            spend_by_domain = {}
            for row in (self.cur_hris.fetchall() or []):
                dk = self._report_account_normalize_campaign_domain(row.get('raw_domain'))
                if dk:
                    spend_by_domain[dk] = spend_by_domain.get(dk, 0.0) + float(row.get('spend') or 0)

            rekap_maps = {}
            resolved_tarik = None
            available_tarik = []
            if compare_rekap and rekap_year and rekap_month:
                available_tarik = self._report_account_list_rekap_tarik_dates(rekap_year, rekap_month)
                resolved_tarik = str(rekap_tanggal_tarik or '').strip() or (available_tarik[0] if available_tarik else '')
                if resolved_tarik:
                    rekap_adx_map, rekap_adx_by_cred = self._report_account_build_rekap_revenue_maps(
                        'data_adx_rekap', 'data_adx_rekap_domain', 'data_adx_rekap_revenue',
                        rekap_year, rekap_month, resolved_tarik,
                    )
                    rekap_adsense_map, _rekap_adsense_by_cred = self._report_account_build_rekap_revenue_maps(
                        'data_adsense_rekap', 'data_adsense_rekap_domain', 'data_adsense_rekap_revenue',
                        rekap_year, rekap_month, resolved_tarik,
                    )
                    rekap_maps = {
                        'spend': {},
                        'adx': rekap_adx_map,
                        'adx_by_cred': rekap_adx_by_cred,
                        'adsense': rekap_adsense_map,
                    }
                    fb_key_expr = self._report_account_fb_key_sql('account_ads_id')
                    rekap_fb_sql = f"""
                        SELECT data_ads_domain AS raw_domain,
                               COALESCE(SUM(CAST(data_ads_rekap_spend AS DECIMAL(18,4))), 0) AS spend
                        FROM data_ads_rekap
                        WHERE data_ads_rekap_tahun = %s
                          AND data_ads_rekap_bulan = %s
                          AND data_ads_rekap_tanggal = %s
                          AND {fb_key_expr} = %s
                        GROUP BY raw_domain
                    """
                    self.cur_hris.execute(
                        rekap_fb_sql,
                        (str(rekap_year), str(rekap_month).zfill(2), resolved_tarik, account_key),
                    )
                    for row in (self.cur_hris.fetchall() or []):
                        dk = self._normalize_domain_match_key(row.get('raw_domain'))
                        if dk:
                            rekap_maps['spend'][dk] = rekap_maps['spend'].get(dk, 0.0) + float(row.get('spend') or 0)

            all_domains = sorted(set(domains) | set(spend_by_domain.keys()) | set(adx_by_domain.keys()))
            domain_filter = str(domain_q or '').strip().lower()
            if domain_filter:
                all_domains = [dk for dk in all_domains if domain_filter in dk.lower()]

            detail_rows = []
            totals = {'spend': 0.0, 'revenue': 0.0, 'profit': 0.0, 'adx_revenue': 0.0, 'adsense_revenue': 0.0}
            totals_rekap = {'spend': 0.0, 'revenue': 0.0, 'profit': 0.0}
            for dk in all_domains:
                adx_rev = float(adx_by_domain.get(dk) or 0)
                if adx_rev <= 0:
                    adx_rev = self._report_account_sum_adx_revenue([dk], adx_rev_map, adx_rev_by_cred, cred_ids)
                adsense_rev = float(adsense_rev_map.get(dk) or 0)
                spend = float(spend_by_domain.get(dk) or 0)
                revenue = adx_rev + adsense_rev
                metrics = self._report_account_build_row_metrics(spend, revenue, 1)
                source_labels = self._report_account_resolve_source_labels(adx_rev, adsense_rev, spend)
                item = {
                    'domain': dk,
                    'adx_revenue': round(adx_rev, 2),
                    'adsense_revenue': round(adsense_rev, 2),
                    'source_labels': source_labels,
                    'source_label': ' + '.join(source_labels) if source_labels else '-',
                    **metrics,
                }
                if compare_rekap and resolved_tarik:
                    r_adx = self._report_account_sum_adx_revenue(
                        [dk],
                        rekap_maps.get('adx') or {},
                        rekap_maps.get('adx_by_cred') or {},
                        cred_ids,
                    )
                    r_adsense = float(rekap_maps.get('adsense', {}).get(dk) or 0)
                    r_spend = float(rekap_maps.get('spend', {}).get(dk) or 0)
                    r_rev = r_adx + r_adsense
                    r_metrics = self._report_account_build_row_metrics(r_spend, r_rev, 1)
                    item['compare'] = {
                        'spend': self._report_account_compare_block(metrics['spend'], r_metrics['spend']),
                        'revenue': self._report_account_compare_block(metrics['revenue'], r_metrics['revenue']),
                        'profit': self._report_account_compare_block(metrics['profit'], r_metrics['profit']),
                        'roi': self._report_account_compare_block(metrics['roi'], r_metrics['roi']),
                    }
                if spend > 0 or revenue > 0 or dk in domains:
                    detail_rows.append(item)
                    totals['spend'] += spend
                    totals['revenue'] += revenue
                    totals['profit'] += metrics['profit']
                    totals['adx_revenue'] += adx_rev
                    totals['adsense_revenue'] += adsense_rev
                    if compare_rekap and resolved_tarik:
                        totals_rekap['spend'] += r_spend
                        totals_rekap['revenue'] += r_rev
                        totals_rekap['profit'] += r_metrics['profit']

            detail_rows.sort(key=lambda r: float(r.get('revenue') or 0), reverse=True)

            summary = self._report_account_build_row_metrics(
                totals['spend'], totals['revenue'], len(detail_rows)
            )
            summary['adx_revenue'] = round(totals['adx_revenue'], 2)
            summary['adsense_revenue'] = round(totals['adsense_revenue'], 2)
            summary_block = {'daily': summary}
            if compare_rekap and resolved_tarik:
                rekap_summary = self._report_account_build_row_metrics(
                    totals_rekap['spend'], totals_rekap['revenue'], len(detail_rows)
                )
                summary_block['rekap'] = rekap_summary
                summary_block['compare'] = {
                    'spend': self._report_account_compare_block(summary['spend'], rekap_summary['spend']),
                    'revenue': self._report_account_compare_block(summary['revenue'], rekap_summary['revenue']),
                    'profit': self._report_account_compare_block(summary['profit'], rekap_summary['profit']),
                    'roi': self._report_account_compare_block(summary['roi'], rekap_summary['roi']),
                }

            chart = []
            spend_daily = self._report_account_fetch_spend_daily(start_date, end_date, [account_key])
            adx_daily_by_account = self._report_account_fetch_adx_revenue_daily_for_accounts(
                start_date, end_date, [account_key]
            )
            adsense_daily, _adsense_daily_by_cred = self._report_account_build_revenue_daily_maps(
                'data_adsense_domain', 'data_adsense_tanggal', 'data_adsense_revenue', 'data_adsense_domain', start_date, end_date
            )
            domain_set = set(all_domains)
            try:
                from datetime import datetime as dt
                d0 = dt.strptime(start_date, '%Y-%m-%d').date()
                d1 = dt.strptime(end_date, '%Y-%m-%d').date()
                cur = d0
                while cur <= d1:
                    ds = cur.isoformat()
                    day_spend = float((spend_daily.get(ds) or {}).get(account_key) or 0)
                    day_revenue = float((adx_daily_by_account.get(ds) or {}).get(account_key) or 0)
                    for dk in domain_set:
                        day_revenue += float((adsense_daily.get(ds) or {}).get(dk) or 0)
                    chart.append({
                        'date': ds,
                        'spend': round(day_spend, 2),
                        'revenue': round(day_revenue, 2),
                        'profit': round(day_revenue - day_spend, 2),
                    })
                    cur += timedelta(days=1)
            except Exception:
                chart = []

            return {
                'status': True,
                'data': {
                    'account': acct,
                    'period': {'start': start_date, 'end': end_date},
                    'compare_rekap': bool(compare_rekap and resolved_tarik),
                    'rekap': {
                        'year': str(rekap_year or ''),
                        'month': str(rekap_month).zfill(2) if rekap_month else '',
                        'tanggal_tarik': resolved_tarik,
                        'available_tarik_dates': available_tarik if compare_rekap else [],
                    } if compare_rekap else None,
                    'summary': summary_block,
                    'chart': chart,
                    'rows': detail_rows,
                },
            }
        except Exception as e:
            return {'status': False, 'data': str(e)}

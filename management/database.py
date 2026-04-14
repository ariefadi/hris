
from ast import If
from typing import Any
from google.auth import credentials
import pymysql.cursors
import json
import os
import re
from django.conf import settings
from datetime import datetime
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

    def _report_tables(self):
        raw = str(os.getenv('REPORT_DB_TABLES', '') or os.getenv('DB_REPORT_TABLES', '') or '').strip()
        if raw:
            tables = [t.strip().lower() for t in raw.split(',') if t.strip()]
            if tables:
                return tables
        return [
            'data_adsense_country',
            'data_adsense_domain',
            'data_adx_country',
            'data_adx_domain',
            'data_ads_campaign',
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
        password = os.getenv('CH_PASSWORD') or os.getenv('REPORT_DB_PASSWORD') or os.getenv('DB_REPORT_PASSWORD') or ''
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
            password = os.getenv('DB_PASSWORD') or ''
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

    def delete_data_ads_campaign_by_date_account(self, account, domain, campaign_name, tanggal):
        try:
            sql_delete = """
                        DELETE FROM data_ads_campaign
                        WHERE account_ads_id = %s
                        AND data_ads_domain = %s
                        AND data_ads_campaign_nm = %s
                        AND data_ads_tanggal = %s
                """
            if not self.execute_query(sql_delete, (account, domain, campaign_name, tanggal)):
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
                            %s
                        )
                """
            if not self.execute_query(sql_insert, (
                data['account_ads_id'],
                data['log_ads_country_cd'],
                data['log_ads_country_nm'],
                data['log_ads_domain'],
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
                            %s
                        )
                """
            if not self.execute_query(sql_insert, (
                data['log_ads_id'],
                data['account_ads_id'],
                data['log_ads_domain'],
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
            use_clickhouse = engine in ('clickhouse', 'ch')

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
            use_clickhouse = engine in ('clickhouse', 'ch')

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
            use_clickhouse = engine in ('clickhouse', 'ch')

            account_col = "toString(b.account_id)" if use_clickhouse else "b.account_id"
            like_conditions_account = " OR ".join([f"{account_col} LIKE %s"] * len(data_account_list))
            like_params_account = [f"{account}%" for account in data_account_list]

            site_expr = "concat(arrayElement(splitByChar('.', b.data_adx_country_domain), 1), '.', arrayElement(splitByChar('.', b.data_adx_country_domain), 2))" if use_clickhouse else "SUBSTRING_INDEX(b.data_adx_country_domain, '.', 2)"
            like_conditions_domain = " OR ".join([f"{site_expr} LIKE %s"] * len(data_domain_list))
            like_params_domain = [f"%{domain}%" for domain in data_domain_list]

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
            use_clickhouse = engine in ('clickhouse', 'ch')

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
                            %s
                        )
                """
            if not self.execute_query(sql_insert, (
                data['master_date'],
                data['account_ads_id'],
                data['master_domain'],
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
                            %s
                        )
                """
            if not self.execute_query(sql_insert, (
                data['account_ads_id'],
                data['data_ads_domain'],
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

    def insert_data_ads_country(self, data):
        try:
            sql_insert = """
                        INSERT INTO data_ads_country
                        (
                            data_ads_country.account_ads_id,
                            data_ads_country.data_ads_country_cd,
                            data_ads_country.data_ads_country_nm,
                            data_ads_country.data_ads_domain,
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
                            %s
                        )
                """
            if not self.execute_query(sql_insert, (
                data['account_ads_id'],
                data['data_ads_country_cd'],
                data['data_ads_country_nm'],
                data['data_ads_domain'],
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
                            log_adx_country.log_adx_country_cpc,
                            log_adx_country.log_adx_country_ctr,
                            log_adx_country.log_adx_country_cpm,
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
                data['log_adx_country_cpc'],
                data['log_adx_country_ctr'],
                data['log_adx_country_cpm'],
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

    def get_all_adsense_traffic_account_by_params(self, start_date, end_date, account_list = None):
        try:
            # --- 1. Pastikan account_list adalah list string
            if isinstance(account_list, str):
                account_list = [account_list.strip()]
            elif account_list is None:
                account_list = []
            elif isinstance(account_list, (set, tuple)):
                account_list = list(account_list)
            data_account_list = [str(a).strip() for a in account_list if str(a).strip()]
            # --- 2. Buat kondisi LIKE untuk setiap domain
            like_conditions_account = " OR ".join(["b.account_id LIKE %s"] * len(data_account_list))
            like_params_account = [f"%{account}%" for account in data_account_list] 
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
            ]    
            params = []
            base_sql.append("DATE(b.data_adsense_country_tanggal) BETWEEN %s AND %s")
            params.extend([start_date, end_date])
            # Normalize selected_account and apply account filter
            if data_account_list:
                base_sql.append(f"\tAND ({like_conditions_account})")
                params.extend(like_params_account)
            # Normalize selected_sites (CSV string or list) and apply domain filter
            base_sql.append("GROUP BY b.data_adsense_country_tanggal, a.account_id, b.data_adsense_country_domain, b.data_adsense_country_cd")
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

    def get_all_adsense_traffic_country_by_params(self, start_date, end_date, selected_account_list = None, countries_list = None):
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
            ]
            params = []
            # Date range
            base_sql.append("b.data_adsense_country_tanggal BETWEEN %s AND %s")
            params.extend([start_date, end_date])
            # Normalize selected_account_list and apply account filter
            if data_account_list:
                base_sql.append(f"\tAND ({like_conditions_account})")
                params.extend(like_params_account)
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
        
    def get_all_adx_traffic_account_by_params(self, start_date, end_date, account_list = None, selected_domain_list = None):
        try:
            # --- 1. Pastikan account_list adalah list string
            if isinstance(account_list, str):
                account_list = [account_list.strip()]
            elif account_list is None:
                account_list = []
            elif isinstance(account_list, (set, tuple)):
                account_list = list(account_list)
            data_account_list = [str(a).strip() for a in account_list if str(a).strip()]
            # --- 2. Buat kondisi LIKE untuk setiap domain
            like_conditions_account = " OR ".join(["a.account_id LIKE %s"] * len(data_account_list))
            like_params_account = [f"%{account}%" for account in data_account_list] 
            # --- 2. Pastikan selected_domain_list adalah list string
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
            ]    
            params = []
            base_sql.append("b.data_adx_country_tanggal BETWEEN %s AND %s")
            params.extend([start_date, end_date])
            # Normalize selected_account and apply account filter
            if data_account_list:
                base_sql.append(f"\tAND ({like_conditions_account})")
                params.extend(like_params_account)
            # Normalize selected_sites (CSV string or list) and apply domain filter
            if data_domain_list:
                base_sql.append(f"\tAND ({like_conditions_domain})")
                params.extend(like_params_domain)
            base_sql.append("GROUP BY a.account_id, a.account_name, a.user_mail, b.data_adx_country_tanggal, b.data_adx_country_domain, b.data_adx_country_cd")
            base_sql.append("ORDER BY b.data_adx_country_tanggal ASC")
            sql = "\n".join(base_sql)
            if not self.execute_query(sql, tuple(params)):
                raise pymysql.Error("Failed to get all adx traffic account by params")  
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
                if data_domain_list:
                    like_conditions_domain = " OR ".join([f"{site_expr} LIKE %s"] * len(data_domain_list))
                    base_sql.append(f"\tAND ({like_conditions_domain})")
                    params.extend([f"%{domain}%" for domain in data_domain_list])
                base_sql.append(f"GROUP BY b.account_id, toDate(b.data_adsense_country_tanggal), {site_expr}, b.data_adsense_country_cd")
                base_sql.append("ORDER BY date ASC")
            else:
                like_conditions_account = " OR ".join(["a.account_id LIKE %s"] * len(data_account_list))
                like_params_account = [f"%{account}%" for account in data_account_list]
                like_conditions_domain = " OR ".join(["SUBSTRING_INDEX(b.data_adsense_country_domain, '.', 2) LIKE %s"] * len(data_domain_list))
                like_params_domain = [f"%{domain}%" for domain in data_domain_list]
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

            site_expr = "concat(arrayElement(splitByChar('.', b.data_adsense_country_domain), 1), '.', arrayElement(splitByChar('.', b.data_adsense_country_domain), 2))" if use_clickhouse else "SUBSTRING_INDEX(b.data_adsense_country_domain, '.', 2)"
            date_expr = "toDate(b.data_adsense_country_tanggal)" if use_clickhouse else "DATE(b.data_adsense_country_tanggal)"

            like_conditions_domain = " OR ".join([f"{site_expr} LIKE %s"] * len(data_domain_list))
            like_params_domain = [f"%{domain}%" for domain in data_domain_list]

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


    def get_all_adx_traffic_country_by_params(self, start_date, end_date, selected_account_list = None, selected_domain_list = None, countries_list = None):
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
                base_sql.append(f"AND b.data_adsense_country_cd IN ({placeholders})")
                params.extend(country_codes)
            base_sql.append("GROUP BY b.data_adx_country_cd, b.data_adx_country_nm")
            base_sql.append("ORDER BY revenue DESC")
            sql = "\n".join(base_sql)
            if not self.execute_query(sql, tuple(params)):
                raise pymysql.Error("Failed to get all adx traffic country by params")
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
                "SELECT",
                "    toHour(mdd) AS hour,",
                "    log_adsense_country_cd AS country_code,",
                "    log_adsense_country_domain,",
                "    argMax(log_adsense_country_impresi, mdd) AS impressions,",
                "    argMax(log_adsense_country_click, mdd) AS clicks,",
                "    argMax(log_adsense_country_revenue, mdd) AS revenue",
                "FROM log_adsense_country",
                "WHERE toDate(log_adsense_country_tanggal) = toDate(%s)",
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
            # Gunakan LIKE tanpa wildcard jika domain sama persis dengan TLD (AdX)
            like_conditions_domain = " OR ".join(["log_adx_country_domain LIKE %s"] * len(data_domain_list))
            like_params_domain = [domain for domain in data_domain_list]
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
            # --- 1. Pastikan selected_account_list adalah list string
            if isinstance(selected_account_list, str):
                selected_account_list = [selected_account_list.strip()]
            elif selected_account_list is None:
                selected_account_list = []
            elif isinstance(selected_account_list, (set, tuple)):
                selected_account_list = list(selected_account_list)
            selected_account_list = [str(a).strip() for a in selected_account_list if str(a).strip()]
            like_conditions_account = " OR ".join(["b.account_ads_id LIKE %s"] * len(selected_account_list))
            like_params_account = [f"%{account}%" for account in selected_account_list] 
            # --- 2. Pastikan selected_domain_list adalah list string
            if isinstance(selected_domain_list, str):
                selected_domain_list = [selected_domain_list.strip()]
            elif selected_domain_list is None:
                selected_domain_list = []
            elif isinstance(selected_domain_list, (set, tuple)):
                selected_domain_list = list(selected_domain_list)
            selected_domain_list = [str(d).strip() for d in selected_domain_list if str(d).strip()]
            # --- 3. Buat kondisi LIKE untuk setiap domain
            like_conditions_domain = " OR ".join(["b.data_ads_domain LIKE %s"] * len(selected_domain_list))
            like_params_domain = [f"%{domain}%" for domain in selected_domain_list] 
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
            ]
            params = []
            base_sql.append("b.data_ads_tanggal BETWEEN %s AND %s")
            params.extend([tanggal_dari, tanggal_sampai])
            if selected_account_list:
                base_sql.append(f"\tAND ({like_conditions_account})")
                params.extend(like_params_account)
            if selected_domain_list:
                base_sql.append(f"\tAND ({like_conditions_domain})")
                params.extend(like_params_domain)
            base_sql.append(") rs")
            base_sql.append("GROUP BY rs.account_id, rs.account_name, rs.account_email, rs.date, rs.domain, rs.campaign")
            base_sql.append("ORDER BY rs.date, rs.domain, rs.campaign")
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
            like_conditions_account = " OR ".join(["b.account_ads_id LIKE %s"] * len(selected_account_list))
            like_params_account = [f"%{account}%" for account in selected_account_list] 
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
            base_sql.append(") rs")
            base_sql.append("GROUP BY rs.country_code")
            base_sql.append("ORDER BY rs.country_name ASC")
            sql = "\n".join(base_sql)
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
            # Buat clause startsWith
            starts_clause_ch = " OR ".join(["startsWith(b.data_ads_domain, %s)"] * len(domains))
            # --- 3. Query ClickHouse (no subquery, optimized)
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
                WHERE b.data_ads_country_tanggal BETWEEN toDate('{start_date_formatted}') AND toDate('{end_date_formatted}')
                AND ({starts_clause_ch})
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
            # Params: tanggal untuk CTE + tanggal untuk main query + domains
            params_tuple = tuple([start_date_formatted, end_date_formatted] + domains)  
            self._ensure_report_connection()
            self.cur_hris = self.report_cur
            self.cur_hris.execute(query, params_tuple)
            data = self.fetch_all() or []
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
            # --- 2. Buat kondisi LIKE untuk setiap domain
            like_conditions = " OR ".join(["b.data_ads_domain LIKE %s"] * len(data_domain_list))
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
            if selected_account:
                base_sql.append(f"\tAND b.account_id LIKE %s")
                params.append(f"{selected_account}%")
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



from ast import If
from typing import Any
from google.auth import credentials
import pymysql.cursors
import json
import os
from django.conf import settings
from datetime import datetime
from google_auth_oauthlib.flow import InstalledAppFlow
from random import sample
from argon2 import PasswordHasher, exceptions as argon2_exceptions
from .crypto import sandi
def _log_debug(message):
    try:
        with open('/tmp/hris_login_debug.log', 'a') as f:
            f.write(str(message) + '\n')
    except Exception:
        pass

def run_sql(sql):
    print(json.dumps(sql, indent=2, sort_keys=True))

class data_mysql:
    
    def __init__(self):
        self.db_hris = None
        self.cur_hris = None
        self.connect()

    def connect(self):
        """Membuat koneksi baru ke database"""
        try:
            # Use the same environment variables as Django settings for consistency
            host = os.getenv('DB_HOST', '127.0.0.1')
            # Use the same port as Django (3307, not 3307)
            raw_port = os.getenv('DB_PORT', '').strip()
            if not raw_port:
                raw_port = '3306'
            try:
                port = int(raw_port)
            except (ValueError, TypeError):
                print(f"Invalid HRIS_DB_PORT value '{raw_port}', defaulting to 3307")
                port = 3306
            user = os.getenv('DB_USER', 'root')
            password = os.getenv('DB_PASSWORD', 'hris123456')
            database = os.getenv('DB_NAME', 'hris_trendHorizone')

            self.db_hris = pymysql.connect(
                host=host,
                port=port,
                user=user,
                password=password,
                database=database,
                cursorclass=pymysql.cursors.DictCursor
            )
            self.cur_hris = self.db_hris.cursor()
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
            if self.cur_hris:
                self.cur_hris.close()
            if self.db_hris:
                self.db_hris.close()
        except pymysql.Error:
            pass

    def __del__(self):
        """Destructor untuk memastikan koneksi ditutup"""
        self.close()

    def execute_query(self, query, params=None):
        """
        Mengeksekusi query dengan penanganan koneksi yang lebih baik
        """
        try:
            if not self.ensure_connection():
                raise pymysql.Error("Could not establish database connection")
            
            self.cur_hris.execute(query, params)
            return True
        except pymysql.Error as e:
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
            _log_debug(f"[LOGIN_DEBUG] Attempting login for username={data.get('username')} from DB host={os.getenv('DB_HOST','127.0.0.1')} port={os.getenv('HRIS_DB_PORT','3307')} db={os.getenv('HRIS_DB_NAME','hris_trendHorizone')}")
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
            hasil = {
                "status": False,
                'data': 'Terjadi error {!r}, error nya {}'.format(e, e.args[0])
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
            hasil = {
                "status": False,
                'data': 'Terjadi error {!r}, error nya {}'.format(e, e.args[0])
            }
        return {'hasil': hasil}

    def get_data_ads_country_to_insert_log(self, account, country, domain, campaign, tanggal):
        try:
            sql_select = (
                "SELECT * FROM data_ads_country WHERE account_ads_id = %s AND data_ads_country_cd LIKE %s AND data_ads_domain = %s AND data_ads_campaign_nm LIKE %s AND data_ads_country_tanggal LIKE %s"
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
            hasil = {
                "status": False,
                'data': 'Terjadi error {!r}, error nya {}'.format(e, e.args[0])
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
            hasil = {
                "status": False,
                'data': 'Terjadi error {!r}, error nya {}'.format(e, e.args[0])
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

    def get_last_update_adsense_traffic_country(self):
        sql = '''
            SELECT MAX(mdd) AS 'last_update'
            FROM `data_adsense_country`
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

    def get_last_update_adx_traffic_per_domain(self):
        sql = '''
            SELECT MAX(mdd) AS 'last_update'
            FROM `data_adx_domain`
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

    def get_last_update_adsense_traffic_per_domain(self):
        sql = '''
            SELECT MAX(mdd) AS 'last_update'
            FROM `data_adsense_domain`
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

    def update_account_name(self, user_mail, new_account_name):
        """Update account name for a specific user"""
        try:
            # Check if user exists
            check_query = "SELECT user_mail FROM app_credentials WHERE user_mail = %s"
            if not self.execute_query(check_query, (user_mail,)):
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
                SET account_name = %s, mdd = NOW() 
                WHERE user_mail = %s
            """
            
            if not self.execute_query(update_query, (new_account_name, user_mail)):
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
            sql_select = (
                "SELECT * FROM data_adx_country WHERE account_id = %s AND data_adx_country_tanggal LIKE %s AND data_adx_country_cd = %s AND data_adx_country_domain LIKE %s"
            )
            if not self.execute_query(sql_select, (account_id, f"{tanggal}%", code_negara, site_name)):
                raise pymysql.Error("Failed to select data adx country by date range")
            data = self.cur_hris.fetchone()
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
            hasil = {
                "status": False,
                'data': 'Terjadi error {!r}, error nya {}'.format(e, e.args[0])
            }
        return {'hasil': hasil}

    def insert_data_adx_domain(self, data):
        try:
            sql_insert = """
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
                            %s
                        )
                """
            if not self.execute_query(sql_insert, (
                data['account_id'],
                data['data_adx_domain_tanggal'],
                data['data_adx_domain'],
                data['data_adx_domain_impresi'],
                data['data_adx_domain_click'],
                data['data_adx_domain_cpc'],
                data['data_adx_domain_ctr'],
                data['data_adx_domain_cpm'],
                data['data_adx_domain_revenue'],
                data['mdb'],
                data['mdb_name'],
                data['mdd']
            )):
                raise pymysql.Error("Failed to insert data adx domain")
            if not self.commit():
                raise pymysql.Error("Failed to commit data adx domain insert")
            hasil = {
                "status": True,
                "message": "Data adx domain berhasil ditambahkan"
            }
        except pymysql.Error as e:
            hasil = {
                "status": False,
                'data': 'Terjadi error {!r}, error nya {}'.format(e, e.args[0])
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
                            data_adsense_country.data_adsense_country_ctr,
                            data_adsense_country.data_adsense_country_cpc,
                            data_adsense_country.data_adsense_country_cpm,
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
                data['data_adsense_country_ctr'],
                data['data_adsense_country_cpc'],
                data['data_adsense_country_cpm'],
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
            like_conditions_account = " OR ".join(["a.account_id LIKE %s"] * len(data_account_list))
            like_params_account = [f"%{account}%" for account in data_account_list] 
            base_sql = [
                "SELECT",
                "\ta.account_id, a.account_name, a.user_mail,",
                "\tb.data_adsense_country_tanggal AS 'date',",
                "\tb.data_adsense_country_domain AS 'site_name',",
                "\tb.data_adsense_country_cd AS 'country_code',",
                "\tSUM(b.data_adsense_country_impresi) AS 'impressions_adsense',",
                "\tSUM(b.data_adsense_country_click) AS 'clicks_adsense',",
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
            like_conditions_account = " OR ".join(["a.account_id LIKE %s"] * len(data_account_list))
            like_params_account = [f"%{account}%" for account in data_account_list] 
            base_sql = [
                "SELECT",
                "\ta.account_id, a.account_name, a.user_mail,",
                "\tb.data_adsense_country_nm AS country_name,",
                "\tb.data_adsense_country_cd AS country_code,",
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
            base_sql.append("GROUP BY b.data_adx_country_tanggal, b.data_adx_country_domain, b.data_adx_country_cd")
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
            like_conditions_domain = " OR ".join(["CONCAT(SUBSTRING_INDEX(b.data_adsense_country_domain, '.', 2), '.com') LIKE %s"] * len(data_domain_list))
            like_params_domain = [f"%{domain}%" for domain in data_domain_list] 
            base_sql = [
                "SELECT",
                "\ta.account_id, a.account_name, a.user_mail,",
                "\tb.data_adsense_country_tanggal AS 'date',",
                "\tCONCAT(SUBSTRING_INDEX(b.data_adsense_country_domain, '.', 2), '.com') AS 'site_name',",
                "\tb.data_adsense_country_cd AS 'country_code',",
                "\tSUM(b.data_adsense_country_impresi) AS 'impressions_adsense',",
                "\tSUM(b.data_adsense_country_click) AS 'clicks_adsense',",
                "\tCASE WHEN SUM(b.data_adsense_country_click) > 0 THEN ROUND(SUM(b.data_adsense_country_revenue) / SUM(b.data_adsense_country_click), 0) ELSE 0 END AS 'cpc_adx',",
                "\tCASE WHEN SUM(b.data_adsense_country_impresi) > 0 THEN ROUND((SUM(b.data_adsense_country_revenue) / SUM(b.data_adsense_country_impresi)) * 1000) ELSE 0 END AS 'ecpm',",
                "\tSUM(b.data_adsense_country_revenue) AS 'revenue'",
                "FROM app_credentials a",
                "INNER JOIN data_adsense_country b ON a.account_id = b.account_id",
                "WHERE",
            ]    
            params = []
            base_sql.append("b.data_adsense_country_tanggal BETWEEN %s AND %s")
            params.extend([start_date, end_date])
            # Normalize selected_account and apply account filter
            if data_account_list:
                base_sql.append(f"\tAND ({like_conditions_account})")
                params.extend(like_params_account)
            # Normalize selected_sites (CSV string or list) and apply domain filter
            if data_domain_list:
                base_sql.append(f"\tAND ({like_conditions_domain})")
                params.extend(like_params_domain)
            base_sql.append("GROUP BY b.data_adsense_country_tanggal, CONCAT(SUBSTRING_INDEX(b.data_adsense_country_domain, '.', 2), '.com'), b.data_adsense_country_cd")
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
            like_conditions_account = " OR ".join(["a.account_id LIKE %s"] * len(data_account_list))
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
            base_sql = [
                "SELECT",
                "\tb.data_adx_country_domain AS 'site_name',",
                "\tb.data_adx_country_cd AS 'country_code',",
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
            
            base_sql.append("GROUP BY b.data_adx_country_domain, b.data_adx_country_cd")
            base_sql.append("ORDER BY b.data_adx_country_domain ASC")
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
            like_conditions_account = " OR ".join(["a.account_id LIKE %s"] * len(data_account_list))
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
            like_conditions_domain = " OR ".join(["CONCAT(SUBSTRING_INDEX(b.data_adsense_country_domain, '.', 2), '.com') LIKE %s"] * len(data_domain_list))
            like_params_domain = [f"%{domain}%" for domain in data_domain_list] 
            base_sql = [
                "SELECT",
                "\tCONCAT(SUBSTRING_INDEX(b.data_adsense_country_domain, '.', 1), '.com') AS 'site_name',",
                "\tb.data_adsense_country_cd AS 'country_code',",
                "\tSUM(b.data_adsense_country_revenue) AS 'revenue'",
                "FROM app_credentials a",
                "INNER JOIN data_adsense_country b ON a.account_id = b.account_id",
                "WHERE",
            ]
            params = []
            base_sql.append("b.data_adsense_country_tanggal BETWEEN %s AND %s")
            params.extend([start_date, end_date])
            # Normalize selected_account and apply account filter
            if data_account_list:
                base_sql.append(f"\tAND ({like_conditions_account})")
                params.extend(like_params_account)
            # Normalize selected_sites (CSV string or list) and apply domain filter
            if data_domain_list:
                base_sql.append(f"\tAND ({like_conditions_domain})")
                params.extend(like_params_domain)
            base_sql.append("GROUP BY b.data_adsense_country_domain, b.data_adsense_country_cd")
            base_sql.append("ORDER BY b.data_adsense_country_domain ASC")
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
            like_conditions_account = " OR ".join(["a.account_id LIKE %s"] * len(data_account_list))
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
                "\ta.account_id, a.account_name, a.user_mail,",
                "\tb.data_adx_country_nm AS country_name,",
                "\tb.data_adx_country_cd AS country_code,",
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
                "\tSUM(b.data_adx_country_revenue) AS revenue",
                "FROM app_credentials a",
                "INNER JOIN data_adx_country b ON a.account_id = b.account_id",
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
            total_ctr_ratio = (float(total_clicks) / float(total_impressions)) if total_impressions else 0.0
            return {
                "status": True,
                "message": "Data adx traffic country berhasil diambil",
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
                "\tROUND(AVG(rs.cpr), 0) AS 'cpr',",
                "\tROUND((SUM(rs.spend)/SUM(rs.clicks)), 0) AS 'cpc'",
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
            base_sql.append("GROUP BY rs.account_name, rs.date, rs.domain")
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
                "\tb.account_id AS 'account_id', a.account_name AS 'account_name'",
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
                "data": [{'account_id': row['account_id'], 'account_name': row['account_name']} for row in data]
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
            like_conditions = " OR ".join(["a.account_id LIKE %s"] * len(selected_account_list))
            like_params = [f"%{d}%" for d in selected_account_list]
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
                    "\t\tCONCAT(SUBSTRING_INDEX(b.data_ads_domain, '.', 2), '.com') AS domain,",
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
            # --- 4. Gabungkan parameter
            params = [start_date_formatted, end_date_formatted] + like_params
            # --- 5. Eksekusi query
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

    def get_all_ads_adsense_roi_traffic_campaign_by_params(self, start_date_formatted, end_date_formatted, data_sub_domain=None):
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
            # --- 4. Gabungkan parameter
            params = [start_date_formatted, end_date_formatted] + like_params
            # --- 5. Eksekusi query
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
            print(f"like_params: {like_params}") 
            # --- 3. Susun query
            base_sql = [
                "SELECT",
                "\trs.account_id, rs.account_name,",
                "\trs.domain, rs.country_code,",
                "\tSUM(rs.spend) AS 'spend'",
                "FROM (",
                    "\tSELECT",
                    "\t\ta.account_id, a.account_name,",
                    "\t\tb.data_ads_domain AS 'domain_raw',",
                    "\t\tCONCAT(SUBSTRING_INDEX(b.data_ads_domain, '.', 2), '.com') AS domain,",
                    "\t\tb.data_ads_country_cd AS 'country_code',",
                    "\t\tb.data_ads_country_spend AS 'spend'",
                    "\tFROM master_account_ads a",
                    "\tINNER JOIN data_ads_country b ON a.account_id = b.account_ads_id",
                    "\tWHERE b.data_ads_country_tanggal BETWEEN %s AND %s",
                    f"{like_clause}",
                ") rs",
                "GROUP BY rs.domain, rs.country_code"
            ]
            # --- 4. Gabungkan parameter
            params = [start_date_formatted, end_date_formatted] + like_params
            # --- 5. Eksekusi query
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

    def get_all_adsense_roi_monitoring_campaign_by_params(self, start_date_formatted, end_date_formatted, data_sub_domain=None):
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
            print(f"like_params: {like_params}") 
            # --- 3. Susun query
            base_sql = [
                "SELECT",
                "\trs.account_id, rs.account_name,",
                "\tSUBSTRING_INDEX(rs.domain, '.', -2) AS 'domain', rs.country_code,",
                "\tSUM(rs.spend) AS 'spend'",
                "FROM (",
                    "\tSELECT",
                    "\t\ta.account_id, a.account_name,",
                    "\t\tb.data_ads_domain AS 'domain_raw',",
                    "\t\tCONCAT(SUBSTRING_INDEX(b.data_ads_domain, '.', 2), '.com') AS domain,",
                    "\t\tb.data_ads_country_cd AS 'country_code',",
                    "\t\tb.data_ads_country_spend AS 'spend'",
                    "\tFROM master_account_ads a",
                    "\tINNER JOIN data_ads_country b ON a.account_id = b.account_ads_id",
                    "\tWHERE b.data_ads_country_tanggal BETWEEN %s AND %s",
                    f"{like_clause}",
                ") rs",
                "GROUP BY rs.domain, rs.country_code"
            ]
            # --- 4. Gabungkan parameter
            params = [start_date_formatted, end_date_formatted] + like_params
            # --- 5. Eksekusi query
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
            like_conditions_domain = " OR ".join(["b.data_adx_country_domain LIKE %s"] * len(data_domain_list))
            like_params_domain = [f"%{domain}%" for domain in data_domain_list]

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

            base_sql = [
                "SELECT",
                "\tb.data_adsense_country_tanggal AS 'date',",
                "\tCONCAT(SUBSTRING_INDEX(b.data_adsense_country_domain, '.', 2), '.com') AS 'site_name',",
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

            base_sql.append("GROUP BY b.data_adsense_country_tanggal, CONCAT(SUBSTRING_INDEX(b.data_adsense_country_domain, '.', 2), '.com'), b.data_adsense_country_cd, b.data_adsense_country_nm")
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
            like_conditions = " OR ".join(["CONCAT(SUBSTRING_INDEX(b.data_ads_domain, '.', 2), '.com') LIKE %s"] * len(data_sub_domain))
            like_params = [f"%{d}%" for d in data_sub_domain]
            sql_parts = [
                "SELECT",
                "\tb.data_ads_country_tanggal AS 'date',",
                "\tb.data_ads_country_cd AS 'country_code',",
                "\tb.data_ads_country_nm AS 'country_name',",
                "\tCONCAT(SUBSTRING_INDEX(b.data_ads_domain, '.', 2), '.com') AS 'domain',",
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
            sql_parts.append("GROUP BY b.data_ads_country_tanggal, b.data_ads_country_cd, b.data_ads_country_nm, CONCAT(SUBSTRING_INDEX(b.data_ads_domain, '.', 2), '.com')")
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
            like_conditions_account = " OR ".join(["a.account_id LIKE %s"] * len(data_account_list))
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
            like_conditions_account = " OR ".join(["a.account_id LIKE %s"] * len(data_account_list))
            like_params_account = [f"%{account}%" for account in data_account_list] 
            if isinstance(selected_domain_list, str):
                selected_domain_list = [selected_domain_list.strip()]
            elif selected_domain_list is None:
                selected_domain_list = []
            elif isinstance(selected_domain_list, (set, tuple)):
                selected_domain_list = list(selected_domain_list)
            data_domain_list = [str(d).strip() for d in selected_domain_list if str(d).strip()]
            like_conditions_domain = " OR ".join(["CONCAT(SUBSTRING_INDEX(b.data_adsense_country_domain, '.', 1), '.com') LIKE %s"] * len(data_domain_list))
            like_params_domain = [f"%{domain}%" for domain in data_domain_list]
            base_sql = [
                "SELECT",
                "\tb.data_adsense_country_tanggal AS 'date',",
                "\tCONCAT(SUBSTRING_INDEX(b.data_adsense_country_domain, '.', 1), '.com') AS 'site_name',",
                "\tb.data_adsense_country_cd AS 'country_code',",
                "\tb.data_adsense_country_nm AS 'country_name',",
                "\tSUM(b.data_adsense_country_revenue) AS 'revenue'",
                "FROM app_credentials a",
                "INNER JOIN data_adsense_country b ON a.account_id = b.account_id",
                "WHERE",
            ]
            params = []
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

            base_sql.append("GROUP BY b.data_adsense_country_tanggal, CONCAT(SUBSTRING_INDEX(b.data_adsense_country_domain, '.', 1), '.com'), b.data_adsense_country_cd, b.data_adsense_country_nm")
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
            # -----------------------------
            # Normalize domain filter
            # -----------------------------
            if isinstance(data_sub_domain, str):
                data_sub_domain = [s.strip() for s in data_sub_domain.split(",") if s.strip()]
            elif isinstance(data_sub_domain, (set, tuple)):
                data_sub_domain = list(data_sub_domain)
            elif data_sub_domain is None:
                data_sub_domain = []
            if not data_sub_domain:
                raise ValueError("data_sub_domain is required and cannot be empty")
            like_conditions = " OR ".join( ["CONCAT(SUBSTRING_INDEX(b.data_ads_domain, '.', 2), '.com') LIKE %s"] * len(data_sub_domain))
            like_params = [f"%{d}%" for d in data_sub_domain]
            # -----------------------------
            # Normalize country filter
            # -----------------------------
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
            # -----------------------------
            # SQL Query
            # -----------------------------
            sql_parts = [
                "SELECT",
                "\trs.date,",
                "\trs.country_code,",
                "\trs.country_name,",
                "\tSUBSTRING_INDEX(rs.domain, '.', -2) AS domain,",
                "\tSUM(rs.spend) AS spend,",
                "\tSUM(rs.clicks) AS clicks,",
                "\tSUM(rs.impressions) AS impressions,",
                "\tROUND(AVG(rs.cpr), 0) AS cpr",
                "FROM (",
                    "\tSELECT",
                    "\t\tb.data_ads_country_tanggal AS date,",
                    "\t\tb.data_ads_country_cd AS country_code,",
                    "\t\tb.data_ads_country_nm AS country_name,",
                    "\t\tCONCAT(SUBSTRING_INDEX(b.data_ads_domain, '.', 2), '.com') AS domain,",
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
                    "\t\tCONCAT(SUBSTRING_INDEX(b.data_ads_domain, '.', 2), '.com')",
                ") rs",
                "GROUP BY",
                "\trs.date,",
                "\trs.country_code,",
                "\trs.country_name,",
                "\tSUBSTRING_INDEX(rs.domain, '.', -2)",
                "ORDER BY rs.date ASC"
            ]
            sql = "\n".join(sql_parts)
            params = ( [start_date_formatted, end_date_formatted] + like_params + country_params)
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
                "\t\t\tb.data_adsense_country_domain,",
                "\t\t\tb.data_adsense_country_cd,",
                "\t\t\tSUM(b.data_adsense_country_revenue) AS revenue",
                "\t\tFROM app_credentials a",
                "\t\tINNER JOIN data_adsense_country b ON a.account_id = b.account_id",
                "\t\tWHERE b.data_adsense_country_tanggal BETWEEN %s AND %s",
            ]
            params = [start_date, end_date]
            # filter account NOW
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
            # filter account LAST
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
        except pymysql.Error as e:
            hasil = {
                "status": False,
                'data': 'Terjadi error {!r}, error nya {}'.format(e, e.args[0])
            }
        return {'hasil': hasil}


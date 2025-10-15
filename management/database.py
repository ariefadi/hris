from random import sample
import pymysql.cursors
from django.conf import settings
import json

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
            self.db_hris = pymysql.connect(
                host='127.0.0.1',
                port=3306,
                user='root',
                password='hris123456',
                database='hris_trendHorizone',
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

    def login_admin(self, data):
        sql = """
              SELECT * FROM `app_users` 
              WHERE `user_name`=%s 
              AND `user_pass`=%s
              """
        try:
            if not self.execute_query(sql, (data['username'], data['password'])):
                raise pymysql.Error("Failed to execute login query")
            result = self.cur_hris.fetchone()
            hasil = {
                "status": True,
                "data": result
            }
        except pymysql.Error as e:
            hasil = {
                "status": False,
                'message': 'Terjadi error {!r}, error nya {}'.format(e, e.args[0])
            }
        return hasil
    
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
    
    def data_user_by_params(self, params=None):
        if params and 'user_mail' in params:
            sql='''
                SELECT user_id, user_name, user_pass, user_alias, 
                user_mail, user_telp, user_alamat, user_st 
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
                SELECT user_id, user_name, user_pass, user_alias, 
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

    def is_exist_user(self, data):
        sql='''
            SELECT *
            FROM app_users 
            WHERE user_alias = %s
            AND user_name = %s
            AND user_pass = %s
        '''
        try:
            if not self.execute_query(sql, (
                data['user_alias'],
                data['user_name'],
                data['user_pass']
            )):
                raise pymysql.Error("Failed to check user existence")
            datanya = self.cur_hris.fetchone()
            hasil = {
                "status": True,
                "data": datanya
            }
        except pymysql.Error as e:
            hasil = {
                "status": False,
                'message': 'Terjadi error {!r}, error nya {}'.format(e, e.args[0])
            }
        return {'hasil': hasil}
    
    def insert_user(self, data):
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
            if not self.execute_query(sql_insert, (
                data['user_name'],
                data['user_pass'],
                data['user_alias'],
                data['user_mail'],
                data['user_telp'],
                data['user_alamat'],
                data['user_st'],
                data['user_foto'],
                data['mdb'],
                data['mdb_name'],
                data['mdd'],
            )):
                raise pymysql.Error("Failed to insert user data")
            
            if not self.commit():
                raise pymysql.Error("Failed to commit user data")
            
            hasil = {
                "status": True,
                "message": "Data Berhasil Disimpan"
            }

        except pymysql.Error as e:
            hasil = {
                "status": False,
                'message': 'Terjadi error {!r}, error nya {}'.format(e, e.args[0])
            }
        return {'hasil': hasil}
    
    def get_user_by_mail(self, user_mail):
        """Get user data by user_mail"""
        sql = """
            SELECT * FROM app_oauth_credentials 
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
            return {
                "status": True,
                "data": result
            }
        except pymysql.Error as e:
            return {
                "status": False,
                'data': f'Terjadi error {e!r}, error nya {e.args[0]}'
            }
    
    def update_user(self, data):
        try:
            sql_update = """
                        UPDATE app_users SET
                            user_name = %s,
                            user_pass = %s,
                            user_alias = %s,
                            user_mail = %s,
                            user_telp = %s,
                            user_alamat = %s,
                            user_st = %s,
                            mdb = %s,
                            mdb_name = %s,
                            mdd = %s
                        WHERE user_id = %s
                """
            if not self.execute_query(sql_update, (
                data['user_name'],
                data['user_pass'],
                data['user_alias'],
                data['user_mail'],
                data['user_telp'],
                data['user_alamat'],
                data['user_st'],
                data['mdb'],
                data['mdb_name'],
                data['mdd'],
                data['user_id']
            )):
                raise pymysql.Error("Failed to update user data")
            
            if not self.commit():
                raise pymysql.Error("Failed to commit user update")
            
            hasil = {
                "status": True,
                "message": "Data Berhasil Diupdate"
            }

        except pymysql.Error as e:
            hasil = {
                "status": False,
                'message': 'Terjadi error {!r}, error nya {}'.format(e, e.args[0])
            }
        return {'hasil': hasil}
    
    def check_refresh_token(self, user_mail):
        """
        Cek apakah user sudah memiliki refresh token di database
        """
        try:
            sql_select = """
                        SELECT refresh_token FROM user_oauth_credentials
                        WHERE user_mail = %s AND refresh_token IS NOT NULL
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
            b.google_oauth2_client_id AS 'client_id', b.google_oauth2_client_secret AS 'client_secret',
            b.google_ads_refresh_token AS 'refresh_token', b.google_ad_manager_network_code AS 'network_code',
            b.developer_token
            FROM app_users a
            LEFT JOIN app_oauth_credentials b ON a.`user_mail` = b.`user_mail`
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
            from google_auth_oauthlib.flow import InstalledAppFlow
            import os
            from django.conf import settings
            
            # Ambil credentials dari settings Django
            client_id = getattr(settings, 'SOCIAL_AUTH_GOOGLE_OAUTH2_KEY', None)
            client_secret = getattr(settings, 'SOCIAL_AUTH_GOOGLE_OAUTH2_SECRET', None)
            
            if not client_id or not client_secret:
                hasil = {
                    "status": False,
                    "refresh_token": None,
                    "message": "Google OAuth2 credentials tidak ditemukan di settings"
                }
                return {'hasil': hasil}
            
            # Konfigurasi OAuth2 untuk Google Ads API
            SCOPES = ['https://www.googleapis.com/auth/adwords']
            
            # Setup OAuth flow
            flow = InstalledAppFlow.from_client_config(
                {
                    "installed": {
                        "client_id": client_id,
                        "client_secret": client_secret,
                        "auth_uri": "https://accounts.google.com/o/oauth2/auth",
                        "token_uri": "https://oauth2.googleapis.com/token",
                        "redirect_uris": ["urn:ietf:wg:oauth:2.0:oob", "http://localhost"]
                    }
                },
                SCOPES
            )
            
            # Jalankan OAuth flow
            credentials = flow.run_local_server(port=8000, open_browser=True)
            
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
            # Debug logging
            print(f"DEBUG generate_refresh_token_from_db_credentials - Email parameter: {user_mail}")
            
            # Ambil credentials dari database
            user_data = self.get_user_by_email(user_mail)
            print(f"DEBUG generate_refresh_token_from_db_credentials - get_user_by_email result: {user_data}")
            
            if not user_data['status'] or not user_data['data']:
                print(f"DEBUG generate_refresh_token_from_db_credentials - User not found or error")
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
                from google_auth_oauthlib.flow import InstalledAppFlow
                from datetime import datetime
                
                # Konfigurasi OAuth2 untuk Google Ads API dengan credentials dari database
                SCOPES = [
                    'https://www.googleapis.com/auth/adwords',
                    'https://www.googleapis.com/auth/dfp'
                ]
                
                # Setup OAuth flow dengan credentials dari database
                client_config = {
                    "installed": {
                        "client_id": client_id,
                        "client_secret": client_secret,
                        "auth_uri": "https://accounts.google.com/o/oauth2/auth",
                        "token_uri": "https://oauth2.googleapis.com/token",
                        "redirect_uris": ["http://localhost", "http://127.0.0.1", "http://localhost:8080", "http://127.0.0.1:8080"]
                    }
                }
                
                flow = InstalledAppFlow.from_client_config(client_config, SCOPES)
                
                # Jalankan OAuth flow dengan port dinamis untuk menghindari konflik
                credentials = flow.run_local_server(port=0)
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
    
    def data_master_plan(self):
        sql = '''
            SELECT a.`master_plan_id`, DATE(a.master_plan_date) AS task_date, 
            TIME(a.master_plan_date) AS task_time, a.`master_task_code`, a.`master_task_plan`,
            b.user_alias AS 'submit_task', c.user_alias AS 'assign_task', a.`project_kategori`,
            a.`urgency`, a.`execute_status`, a.`catatan`
            FROM app_master_plan a
            LEFT JOIN app_users b ON a.submitted_task = b.user_id
            LEFT JOIN app_users c ON a.assignment_to = c.user_id
            ORDER BY DATE(a.master_plan_date) DESC
        '''
        try:
            if not self.execute_query(sql):
                raise pymysql.Error("Failed to fetch master plan data")
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

    def get_master_plan_by_id(self, master_plan_id):
        sql = '''
            SELECT a.`master_plan_id`, DATE(a.master_plan_date) AS task_date, 
            TIME(a.master_plan_date) AS task_time, a.`master_task_code`, a.`master_task_plan`,
            b.user_alias AS 'submit_task', c.user_alias AS 'assign_task', a.`project_kategori`,
            a.`urgency`, a.`execute_status`, a.`catatan`, a.`submitted_task`, a.`assignment_to`
            FROM app_master_plan a
            LEFT JOIN app_users b ON a.submitted_task = b.user_id
            LEFT JOIN app_users c ON a.assignment_to = c.user_id
            WHERE a.master_plan_id = %s
        '''
        try:
            if not self.execute_query(sql, (master_plan_id,)):
                raise pymysql.Error("Failed to fetch master plan by ID")
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

    def insert_master_plan(self, data):
        sql = '''
            INSERT INTO app_master_plan 
            (master_plan_id, master_plan_date, master_task_code, master_task_plan, 
             project_kategori, urgency, execute_status, catatan, submitted_task, assignment_to)
            VALUES (%s, NOW(), %s, %s, %s, %s, %s, %s, %s, %s)
        '''
        try:
            if not self.execute_query(sql, (
                data['master_plan_id'],
                data['master_task_code'],
                data['master_task_plan'],
                data['project_kategori'],
                data['urgency'],
                data['execute_status'],
                data['catatan'],
                data['submitted_task'],
                data['assignment_to']
            )):
                raise pymysql.Error("Failed to insert master plan")
            
            if not self.commit():
                raise pymysql.Error("Failed to commit master plan insert")
            
            hasil = {
                "status": True,
                "data": "Master plan berhasil ditambahkan"
            }
        except pymysql.Error as e:
            hasil = {
                "status": False,
                'data': 'Terjadi error {!r}, error nya {}'.format(e, e.args[0])
            }
        return hasil

    def data_account_ads_by_params(self):
        sql='''
            SELECT a.account_ads_id, a.account_name, a.account_email, a.account_id, 
            a.app_id, a.app_secret, a.access_token, b.user_alias AS 'pemilik_account', a.mdd
            FROM `master_account_ads` a
            LEFT JOIN app_users b ON a.account_owner = b.user_id
            ORDER BY a.account_name ASC
        '''
        try:
            if not self.execute_query(sql):
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

    def master_account_ads_by_id(self, data):
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
    
    def check_refresh_token(self, user_mail):
        """
        Cek apakah user sudah memiliki refresh token di database
        """
        try:
            sql_select = """
                        SELECT refresh_token FROM user_oauth_credentials
                        WHERE user_mail = %s AND refresh_token IS NOT NULL
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

    def generate_and_save_refresh_token(self, user_mail):
        """
        Generate refresh token baru menggunakan Google OAuth2 dan simpan ke database
        """
        try:
            from google_auth_oauthlib.flow import InstalledAppFlow
            import os
            from django.conf import settings
            
            # Ambil credentials dari settings Django
            client_id = getattr(settings, 'SOCIAL_AUTH_GOOGLE_OAUTH2_KEY', None)
            client_secret = getattr(settings, 'SOCIAL_AUTH_GOOGLE_OAUTH2_SECRET', None)
            
            if not client_id or not client_secret:
                hasil = {
                    "status": False,
                    "refresh_token": None,
                    "message": "Google OAuth2 credentials tidak ditemukan di settings"
                }
                return {'hasil': hasil}
            
            # Konfigurasi OAuth2 untuk Google Ads API
            SCOPES = ['https://www.googleapis.com/auth/adwords']
            
            # Setup OAuth flow
            flow = InstalledAppFlow.from_client_config(
                {
                    "installed": {
                        "client_id": client_id,
                        "client_secret": client_secret,
                        "auth_uri": "https://accounts.google.com/o/oauth2/auth",
                        "token_uri": "https://oauth2.googleapis.com/token",
                        "redirect_uris": ["urn:ietf:wg:oauth:2.0:oob", "http://localhost"]
                    }
                },
                SCOPES
            )
            
            # Jalankan OAuth flow
            credentials = flow.run_local_server(port=8000, open_browser=True)
            
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
            # Debug logging
            print(f"DEBUG generate_refresh_token_from_db_credentials - Email parameter: {user_mail}")
            
            # Ambil credentials dari database
            user_data = self.get_user_by_email(user_mail)
            print(f"DEBUG generate_refresh_token_from_db_credentials - get_user_by_email result ok: {user_data}")
            
            if not user_data['status'] or not user_data['data']:
                print(f"DEBUG generate_refresh_token_from_db_credentials - User not found or error")
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
                from google_auth_oauthlib.flow import InstalledAppFlow
                from datetime import datetime
                
                # Konfigurasi OAuth2 untuk Google Ads API dengan credentials dari database
                SCOPES = [
                    'https://www.googleapis.com/auth/adwords',
                    'https://www.googleapis.com/auth/dfp'
                ]
                
                # Setup OAuth flow dengan credentials dari database
                client_config = {
                    "installed": {
                        "client_id": client_id,
                        "client_secret": client_secret,
                        "auth_uri": "https://accounts.google.com/o/oauth2/auth",
                        "token_uri": "https://oauth2.googleapis.com/token",
                        "redirect_uris": ["http://localhost", "http://127.0.0.1", "http://localhost:8080", "http://127.0.0.1:8080"]
                    }
                }
                
                flow = InstalledAppFlow.from_client_config(client_config, SCOPES)
                
                # Jalankan OAuth flow dengan port dinamis untuk menghindari konflik
                credentials = flow.run_local_server(port=0)
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
    
    def data_master_plan(self):
        sql = '''
            SELECT a.`master_plan_id`, DATE(a.master_plan_date) AS task_date, 
            TIME(a.master_plan_date) AS task_time, a.`master_task_code`, a.`master_task_plan`,
            b.user_alias AS 'submit_task', c.user_alias AS 'assign_task', a.`project_kategori`,
            a.`urgency`, a.`execute_status`, a.`catatan`
            FROM app_master_plan a
            LEFT JOIN app_users b ON a.submitted_task = b.user_id
            LEFT JOIN app_users c ON a.assignment_to = c.user_id
            ORDER BY DATE(a.master_plan_date) DESC
        '''
        try:
            if not self.execute_query(sql):
                raise pymysql.Error("Failed to fetch master plan data")
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

    def get_master_plan_by_id(self, master_plan_id):
        sql = '''
            SELECT a.`master_plan_id`, DATE(a.master_plan_date) AS task_date, 
            TIME(a.master_plan_date) AS task_time, a.`master_task_code`, a.`master_task_plan`,
            b.user_alias AS 'submit_task', c.user_alias AS 'assign_task', a.`project_kategori`,
            a.`urgency`, a.`execute_status`, a.`catatan`, a.`submitted_task`, a.`assignment_to`
            FROM app_master_plan a
            LEFT JOIN app_users b ON a.submitted_task = b.user_id
            LEFT JOIN app_users c ON a.assignment_to = c.user_id
            WHERE a.master_plan_id = %s
        '''
        try:
            if not self.execute_query(sql, (master_plan_id,)):
                raise pymysql.Error("Failed to fetch master plan by ID")
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

    def insert_master_plan(self, data):
        sql = '''
            INSERT INTO app_master_plan 
            (master_plan_id, master_plan_date, master_task_code, master_task_plan, 
             project_kategori, urgency, execute_status, catatan, submitted_task, assignment_to)
            VALUES (%s, NOW(), %s, %s, %s, %s, %s, %s, %s, %s)
        '''
        try:
            if not self.execute_query(sql, (
                data['master_plan_id'],
                data['master_task_code'],
                data['master_task_plan'],
                data['project_kategori'],
                data['urgency'],
                data['execute_status'],
                data['catatan'],
                data['submitted_task'],
                data['assignment_to']
            )):
                raise pymysql.Error("Failed to insert master plan")
            
            if not self.commit():
                raise pymysql.Error("Failed to commit master plan insert")
            
            hasil = {
                "status": True,
                "data": "Master plan berhasil ditambahkan"
            }
        except pymysql.Error as e:
            hasil = {
                "status": False,
                'data': 'Terjadi error {!r}, error nya {}'.format(e, e.args[0])
            }
        return hasil

    def data_account_ads_by_params(self):
        sql='''
            SELECT a.account_ads_id, a.account_name, a.account_email, a.account_id, 
            a.app_id, a.app_secret, a.access_token, b.user_alias AS 'pemilik_account', a.mdd
            FROM `master_account_ads` a
            LEFT JOIN app_users b ON a.account_owner = b.user_id
            ORDER BY a.account_name ASC
        '''
        try:
            if not self.execute_query(sql):
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

    def master_account_ads_by_id(self, data):
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

    def get_user_oauth_credentials(self, user_mail=None):
        """
        Mengambil kredensial OAuth dari database berdasarkan user_id atau user_mail
        """
        sql = '''
            SELECT user_id, user_mail, google_oauth2_client_id, google_oauth2_client_secret,
                   google_ads_client_id, google_ads_client_secret, google_ads_refresh_token,
                   google_ad_manager_network_code, developer_token
            FROM app_oauth_credentials
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

    def update_oauth_credentials(self, user_mail, client_id, client_secret, ads_client_id, ads_client_secret, network_code):
        """
        Update kredensial OAuth (client_id dan client_secret) untuk user tertentu
        """
        sql = '''
            UPDATE app_oauth_credentials 
            SET google_oauth2_client_id = %s,
                google_oauth2_client_secret = %s,
                google_ads_client_id = %s,
                google_ads_client_secret = %s,
                google_ad_manager_network_code = %s,
                updated_at = NOW()
            WHERE user_mail = %s
        '''
        try:
            if not self.execute_query(sql, (client_id, client_secret, ads_client_id, ads_client_secret, network_code, user_mail)):
                raise pymysql.Error("Failed to update OAuth credentials")
            
            if not self.commit():
                raise pymysql.Error("Failed to commit OAuth credentials update")
            
            return {
                'status': True,
                'message': f'Successfully updated OAuth credentials for {user_mail}'
            }
        except pymysql.Error as e:
            return {
                'status': False,
                'error': f'Failed to update OAuth credentials: {str(e)}'
            }

    def update_refresh_token(self, user_mail, refresh_token):
        """
        Update refresh token untuk user tertentu
        """
        sql = '''
            UPDATE app_oauth_credentials 
            SET google_ads_refresh_token = %s
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

    def check_refresh_token(self, user_mail):
        """
        Cek apakah user sudah memiliki refresh token di database
        """
        sql = '''
            SELECT google_ads_refresh_token
            FROM app_oauth_credentials
            WHERE user_mail = %s
            AND is_active = 1
        '''
        try:
            if not self.execute_query(sql, (user_mail,)):
                raise pymysql.Error("Failed to check refresh token")
            result = self.cur_hris.fetchone()
            if not result:
                return {
                    'hasil': {
                        'status': False,
                        'has_token': False,
                        'message': f'User not found: {user_mail}'
                    }
                }
            
            has_token = bool(result['google_ads_refresh_token'])
            return {
                'hasil': {
                    'status': True,
                    'has_token': has_token,
                    'refresh_token': result['google_ads_refresh_token'] if has_token else None,
                    'message': 'Refresh token found' if has_token else 'No refresh token'
                }
            }
        except pymysql.Error as e:
            return {
                'hasil': {
                    'status': False,
                    'has_token': False,
                    'message': f'Database error: {str(e)}'
                }
            }

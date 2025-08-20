from random import sample
import pymysql.cursors
from django.conf import settings
import json

def run_sql(sql):
    print(json.dumps(sql, indent=2, sort_keys=True))

class data_mysql:
    
    hris_trendHorizone = ('127.0.0.1', 'root', '', 'hris_trendHorizone')

    def __init__(self):
        db_hris = pymysql.connect(
            host='127.0.0.1',
            user='root',
            password='',
            database='hris_trendHorizone',
            cursorclass=pymysql.cursors.DictCursor  # ⬅️ ini penting
        )
        self.cur_hris = db_hris.cursor()  # sekarang otomatis DictCursor
        self.comit_hris = db_hris

    def login_admin(self, data):
        sql = """
              SELECT * FROM `app_users` 
              WHERE `user_name`=%s 
              AND `user_pass`=%s
              """
        try:
            self.cur_hris.execute(sql, (data['username'], data['password']))
            result = self.cur_hris.fetchone()
            hasil = {
                "status": True,
                "data": result
            }
        except pymysql.Error as e:
            hasil = {
                "status": False,
                'data': 'Terjadi error {!r}, error nya {}'.format(e, e.args[0])
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
                            mub = %s,
                            mub_name = %s,
                            mud = %s
                        WHERE account_ads_id = %s
                """
            self.cur_hris.execute(sql_update, (
                data['account_name'],
                data['account_email'],
                data['account_id'],
                data['app_id'],
                data['app_secret'],
                data['access_token'],
                data['mub'],
                data['mub_name'],
                data['mud'],
                data['account_ads_id']
            ))
            self.con_hris.commit()
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
            self.cur_hris.execute(sql_insert, (
                data['login_id'],
                data['user_id'],
                data['login_date'],
                data['logout_date'],
                data['ip_address'],
                data['user_agent'],
                data['latitude'],
                data['longitude'],
                data.get('lokasi', None)  # Lokasi bisa None jika tidak ada
            ))
            self.comit_hris.commit()
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
            self.cur_hris.execute(sql, (
                data['logout_date'],
                data['login_id']
            ))
            self.comit_hris.commit()
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
    
    def data_user_by_params(self):
        sql='''
            SELECT user_id, user_name, user_pass, user_alias, 
            user_mail, user_telp, user_alamat, user_st 
            FROM `app_users`
            ORDER BY user_alias ASC
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

    def is_exist_user(self, data):
        sql='''
            SELECT *
            FROM app_users 
            WHERE user_alias = %s
            AND user_name = %s
            AND user_pass = %s
        '''
        try:
            self.cur_hris.execute(sql,(
                data['user_alias'],
                data['user_name'],
                data['user_pass']
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
            self.cur_hris.execute(sql_insert, (
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
            ))
            self.comit_hris.commit()
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
    
    def get_user_by_id(self, user_id):
        sql = '''
            SELECT user_id, user_name, user_pass, user_alias, 
            user_mail, user_telp, user_alamat, user_st 
            FROM `app_users`
            WHERE user_id = %s
        '''
        try:
            self.cur_hris.execute(sql, (user_id,))
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
            self.cur_hris.execute(sql_update, (
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
            ))
            self.comit_hris.commit()
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



    def data_account_ads_by_params(self):
        sql='''
            SELECT a.account_ads_id, a.account_name, a.account_email, a.account_id, 
            a.app_id, a.app_secret, a.access_token, b.user_alias AS 'pemilik_account', a.mdd
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
            self.cur_hris.execute(sql,(
                data['account_name'],
                data['account_email'],
                data['account_id'],
                data['app_id']
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
            self.cur_hris.execute(sql_insert, (
                data['account_name'],
                data['account_email'],
                data['account_id'],
                data['app_id'],
                data['app_secret'],
                data['access_token'],
                data['account_owner'],
                data['mdb'],
                data['mdb_name'],
                data['mdd'],
            ))
            self.comit_hris.commit()
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
    
    def master_account_ads(self):
        sql='''
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
        sql='''
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
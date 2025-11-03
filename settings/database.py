"""
settings.database
------------------
Separation layer for the Settings app to access database utilities
while reusing the same connection logic defined in management.database.

This keeps a single source of truth for DB connections and shared helpers
and allows adding Settings-specific queries here without duplicating code.
"""

# Reuse the data_mysql implementation from management.database
from management.database import data_mysql as ManagementDB
from argon2 import PasswordHasher, exceptions as argon2_exceptions
import pymysql

# Settings-specialized subclass: add settings-specific queries here
class SettingsDB(ManagementDB):
    def data_login_user(self):
        sql = '''
            SELECT 
                a.login_id,
                a.user_id,
                DATE(a.login_date) AS login_day,
                TIME(a.login_date) AS login_time,
                a.login_date,
                a.logout_date,
                DATE(a.logout_date) AS logout_day,
                TIME(a.logout_date) AS logout_time,
                a.ip_address,
                a.user_agent,
                a.lokasi,
                b.user_alias
            FROM app_user_login a
            INNER JOIN app_users b ON b.user_id = a.user_id
            WHERE a.login_date >= DATE_SUB(NOW(), INTERVAL 7 DAY)
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

    def is_exist_user(self, data):
        # Check duplicate user by alias and username (ignore password)
        sql='''
            SELECT *
            FROM app_users 
            WHERE user_alias = %s
            AND user_name = %s
        '''
        try:
            if not self.execute_query(sql, (
                data.get('user_alias'),
                data.get('user_name')
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
            # Hash password with Argon2 before storing
            ph = PasswordHasher()
            hashed_pass = ph.hash(data['user_pass'])
            if not self.execute_query(sql_insert, (
                data['user_name'],
                hashed_pass,
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

    def list_roles_with_group(self):
        """Return all roles with their group info"""
        sql = '''
            SELECT r.role_id, r.role_nm, r.role_desc, g.group_name
            FROM app_role r
            LEFT JOIN app_group g ON g.group_id = r.group_id
            ORDER BY COALESCE(g.group_name, ''), COALESCE(r.role_nm, '')
        '''
        try:
            if not self.execute_query(sql):
                raise pymysql.Error("Failed to fetch roles")
            results = self.cur_hris.fetchall() or []
            return { 'status': True, 'data': results }
        except pymysql.Error as e:
            return { 'status': False, 'message': f'Database error: {str(e)}' }

    def list_user_roles(self, user_id):
        """Return list of role_ids currently assigned to a user"""
        sql = 'SELECT role_id FROM app_user_role WHERE user_id = %s'
        try:
            if not self.execute_query(sql, (user_id,)):
                raise pymysql.Error("Failed to fetch user roles")
            rows = self.cur_hris.fetchall() or []
            role_ids = []
            for row in rows:
                try:
                    role_ids.append(row['role_id'] if isinstance(row, dict) else row[0])
                except Exception:
                    pass
            return { 'status': True, 'data': role_ids }
        except pymysql.Error as e:
            return { 'status': False, 'message': f'Database error: {str(e)}' }

    def replace_user_roles(self, user_id, role_ids):
        """Replace all roles for a user with the provided list"""
        try:
            # Delete existing roles for user
            if not self.execute_query('DELETE FROM app_user_role WHERE user_id = %s', (user_id,)):
                raise pymysql.Error("Failed to delete existing user roles")
            # Insert new roles
            role_ids = role_ids or []
            for rid in role_ids:
                if not self.execute_query(
                    """
                    INSERT INTO app_user_role (user_id, role_id, role_default, role_display)
                    VALUES (%s, %s, '2', '1')
                    """,
                    (user_id, rid)
                ):
                    raise pymysql.Error(f"Failed to insert role {rid}")
            if not self.commit():
                raise pymysql.Error("Failed to commit user roles change")
            return { 'status': True, 'message': 'Roles updated' }
        except pymysql.Error as e:
            return { 'status': False, 'message': f'Database error: {str(e)}' }
    
    def update_user(self, data):
        try:
            # Build dynamic SQL depending on whether password should be updated
            update_fields = [
                'user_name = %s',
                'user_alias = %s',
                'user_mail = %s',
                'user_telp = %s',
                'user_alamat = %s',
                'user_st = %s',
                'mdb = %s',
                'mdb_name = %s',
                'mdd = %s'
            ]
            params = [
                data['user_name'],
                data['user_alias'],
                data['user_mail'],
                data['user_telp'],
                data['user_alamat'],
                data['user_st'],
                data['mdb'],
                data['mdb_name'],
                data['mdd']
            ]
            pw = data.get('user_pass')
            if pw is not None and str(pw).strip() != '':
                ph = PasswordHasher()
                hashed_pw = ph.hash(str(pw))
                update_fields.insert(1, 'user_pass = %s')
                params.insert(1, hashed_pw)
            sql_update = f"""
                        UPDATE app_users SET
                            {', '.join(update_fields)}
                        WHERE user_id = %s
                """
            params.append(data['user_id'])
            if not self.execute_query(sql_update, tuple(params)):
                raise pymysql.Error("Failed to update user data")
            if not self.commit():
                raise pymysql.Error("Failed to commit user update")
            hasil = {"status": True, "message": "Data Berhasil Diupdate"}
        except pymysql.Error as e:
            hasil = {
                "status": False,
                'message': 'Terjadi error {!r}, error nya {}'.format(e, e.args[0])
            }
        return {'hasil': hasil}
    
# Re-export with the same name so existing code can import from settings.database
data_mysql = SettingsDB
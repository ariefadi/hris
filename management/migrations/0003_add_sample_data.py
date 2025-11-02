# Generated migration for sample data
import uuid
from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ('management', '0002_add_user_management_tables'),
    ]

    operations = [
        # Insert sample admin user
        migrations.RunSQL(
            """
            INSERT INTO `app_users` (
                `user_id`, 
                `user_name`, 
                `user_pass`, 
                `user_alias`, 
                `user_mail`, 
                `user_telp`, 
                `user_alamat`, 
                `user_st`, 
                `user_foto`, 
                `mdb`, 
                `mdb_name`, 
                `mdd`
            ) VALUES (
                UUID(), 
                'admin', 
                'admin', 
                'System Administrator', 
                'admin@example.com', 
                '081234567890', 
                'Jakarta, Indonesia', 
                '1', 
                'default_avatar.png', 
                UUID(), 
                'System Setup', 
                NOW()
            )
            """,
            reverse_sql="DELETE FROM `app_users` WHERE `user_name` = 'admin'"
        ),
    ]
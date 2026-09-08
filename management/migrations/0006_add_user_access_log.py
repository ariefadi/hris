from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ('management', '0005_add_live_chat_tables'),
    ]

    operations = [
        migrations.RunSQL(
            """
            CREATE TABLE IF NOT EXISTS app_user_access_log (
                log_id VARCHAR(36) NOT NULL,
                user_id VARCHAR(36) NOT NULL,
                login_id VARCHAR(36) DEFAULT NULL,
                activity_time DATETIME NOT NULL,
                method VARCHAR(10) NOT NULL,
                action_type VARCHAR(20) NOT NULL,
                path VARCHAR(500) NOT NULL,
                nav_id VARCHAR(36) DEFAULT NULL,
                menu_name VARCHAR(255) DEFAULT NULL,
                description VARCHAR(500) NOT NULL,
                ip_address VARCHAR(100) DEFAULT NULL,
                user_agent VARCHAR(500) DEFAULT NULL,
                status_code SMALLINT DEFAULT NULL,
                is_ajax TINYINT(1) NOT NULL DEFAULT 0,
                PRIMARY KEY (log_id),
                KEY idx_access_user_time (user_id, activity_time),
                KEY idx_access_time (activity_time),
                KEY idx_access_action (action_type),
                KEY idx_access_nav (nav_id)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """,
            reverse_sql="DROP TABLE IF EXISTS app_user_access_log",
        ),
    ]

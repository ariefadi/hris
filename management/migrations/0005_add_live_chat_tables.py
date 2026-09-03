from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ('management', '0004_merge_20251104_2320'),
        ('management', '0004_merge_20251105_0027'),
    ]

    operations = [
        migrations.RunSQL(
            """
            CREATE TABLE IF NOT EXISTS app_chat_presence (
                user_id VARCHAR(36) NOT NULL,
                last_seen DATETIME NOT NULL,
                PRIMARY KEY (user_id),
                KEY idx_chat_presence_seen (last_seen)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """,
            reverse_sql="DROP TABLE IF EXISTS app_chat_presence",
        ),
        migrations.RunSQL(
            """
            CREATE TABLE IF NOT EXISTS app_chat_message (
                message_id VARCHAR(36) NOT NULL,
                room_type VARCHAR(16) NOT NULL DEFAULT 'team',
                from_user_id VARCHAR(36) NOT NULL,
                to_user_id VARCHAR(36) NULL,
                body TEXT NOT NULL,
                created_at DATETIME NOT NULL,
                PRIMARY KEY (message_id),
                KEY idx_chat_from (from_user_id),
                KEY idx_chat_to (to_user_id),
                KEY idx_chat_created (created_at),
                KEY idx_chat_room (room_type, to_user_id, created_at)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """,
            reverse_sql="DROP TABLE IF EXISTS app_chat_message",
        ),
        migrations.RunSQL(
            """
            CREATE TABLE IF NOT EXISTS app_chat_read (
                user_id VARCHAR(36) NOT NULL,
                room_key VARCHAR(80) NOT NULL,
                last_read_at DATETIME NOT NULL,
                PRIMARY KEY (user_id, room_key)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """,
            reverse_sql="DROP TABLE IF EXISTS app_chat_read",
        ),
    ]

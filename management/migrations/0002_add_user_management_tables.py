# Generated migration for user management tables
import uuid
from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ('management', '0001_initial'),
    ]

    operations = [
        # Create app_group table
        migrations.RunSQL(
            """
            CREATE TABLE `app_group` (
                `group_id` varchar(2) NOT NULL,
                `group_name` varchar(50) DEFAULT NULL,
                `group_desc` varchar(100) DEFAULT NULL,
                `mdb` varchar(36) DEFAULT NULL,
                `mdb_name` varchar(50) DEFAULT NULL,
                `mdd` datetime DEFAULT NULL,
                PRIMARY KEY (`group_id`)
            ) ENGINE=InnoDB DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci
            """,
            # reverse_sql="DROP TABLE IF EXISTS `app_group`"
        ),
        
        # Create app_role table (requires app_group table to exist)
        migrations.RunSQL(
            """
            CREATE TABLE `app_role` (
                `role_id` varchar(5) NOT NULL,
                `group_id` varchar(2) DEFAULT NULL,
                `role_nm` varchar(100) DEFAULT NULL,
                `role_desc` varchar(100) DEFAULT NULL,
                `default_page` varchar(50) DEFAULT NULL,
                `mdb` varchar(36) DEFAULT NULL,
                `mdb_name` varchar(50) DEFAULT NULL,
                `mdd` datetime DEFAULT NULL,
                PRIMARY KEY (`role_id`),
                KEY `group_id` (`group_id`),
                CONSTRAINT `app_role_ibfk_1` FOREIGN KEY (`group_id`) REFERENCES `app_group` (`group_id`) ON UPDATE CASCADE
            ) ENGINE=InnoDB DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci
            """,
            # reverse_sql="DROP TABLE IF EXISTS `app_role`"
        ),
        
        # Create app_users table
        migrations.RunSQL(
            """
            CREATE TABLE `app_users` (
                `user_id` varchar(36) NOT NULL DEFAULT '',
                `user_name` varchar(100) DEFAULT NULL,
                `user_pass` varchar(100) DEFAULT NULL,
                `user_alias` varchar(250) DEFAULT NULL,
                `user_mail` char(50) DEFAULT NULL,
                `user_telp` char(13) DEFAULT NULL,
                `user_alamat` varchar(500) DEFAULT NULL,
                `user_st` enum('0','1') DEFAULT NULL,
                `user_foto` varchar(255) DEFAULT NULL,
                `mdb` varchar(36) DEFAULT NULL,
                `mdb_name` varchar(250) DEFAULT NULL,
                `mdd` datetime DEFAULT NULL,
                PRIMARY KEY (`user_id`)
            ) ENGINE=InnoDB DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci
            """,
            # reverse_sql="DROP TABLE IF EXISTS `app_users`"
        ),
        
        # Create app_user_role table (requires app_role and app_users table to exist)
        migrations.RunSQL(
            """
            CREATE TABLE `app_user_role` (
                `user_id` varchar(36) NOT NULL,
                `role_id` varchar(5) NOT NULL,
                `role_default` enum('1','2') DEFAULT '2',
                `role_display` enum('1','0') DEFAULT '1',
                PRIMARY KEY (`user_id`,`role_id`),
                KEY `role_id` (`role_id`),
                CONSTRAINT `app_user_role_ibfk_1` FOREIGN KEY (`user_id`) REFERENCES `app_users` (`user_id`) ON DELETE CASCADE ON UPDATE CASCADE,
                CONSTRAINT `app_user_role_ibfk_2` FOREIGN KEY (`role_id`) REFERENCES `app_role` (`role_id`) ON DELETE CASCADE ON UPDATE CASCADE
            ) ENGINE=InnoDB DEFAULT CHARSET=latin1
            """,
            # reverse_sql="DROP TABLE IF EXISTS `app_user_role`"
        ),
        
        # Create app_user_login table
        migrations.RunSQL(
            """
            CREATE TABLE `app_user_login` (
                `login_id` varchar(36) NOT NULL DEFAULT '',
                `user_id` varchar(36) DEFAULT NULL,
                `login_date` datetime DEFAULT NULL,
                `logout_date` datetime DEFAULT NULL,
                `ip_address` varchar(100) DEFAULT NULL,
                `user_agent` text DEFAULT NULL,
                `latitude` varchar(100) DEFAULT NULL,
                `longitude` varchar(100) DEFAULT NULL,
                `lokasi` text DEFAULT NULL,
                PRIMARY KEY (`login_id`)
            ) ENGINE=InnoDB DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci
            """,
            # reverse_sql="DROP TABLE IF EXISTS `app_user_login`"
        ),
        
        # Update app_oauth_credentials table to match the new schema (with conditional column additions)
        migrations.RunSQL(
            """
            SET @sql = '';
            SELECT COUNT(*) INTO @col_exists FROM information_schema.columns 
            WHERE table_schema = DATABASE() AND table_name = 'app_oauth_credentials' AND column_name = 'google_ads_client_id';
            SET @sql = IF(@col_exists = 0, 'ALTER TABLE `app_oauth_credentials` ADD COLUMN `google_ads_client_id` varchar(255) NOT NULL DEFAULT \'\' AFTER `google_oauth2_client_secret`;', 'SELECT "Column google_ads_client_id already exists";');
            PREPARE stmt FROM @sql;
            EXECUTE stmt;
            DEALLOCATE PREPARE stmt;
            """,
            reverse_sql="SELECT 1"
        ),
        
        migrations.RunSQL(
            """
            SET @sql = '';
            SELECT COUNT(*) INTO @col_exists FROM information_schema.columns 
            WHERE table_schema = DATABASE() AND table_name = 'app_oauth_credentials' AND column_name = 'google_ads_client_secret';
            SET @sql = IF(@col_exists = 0, 'ALTER TABLE `app_oauth_credentials` ADD COLUMN `google_ads_client_secret` varchar(255) NOT NULL DEFAULT \'\' AFTER `google_ads_client_id`;', 'SELECT "Column google_ads_client_secret already exists";');
            PREPARE stmt FROM @sql;
            EXECUTE stmt;
            DEALLOCATE PREPARE stmt;
            """,
            reverse_sql="SELECT 1"
        ),
        
        migrations.RunSQL(
            """
            SET @sql = '';
            SELECT COUNT(*) INTO @col_exists FROM information_schema.columns 
            WHERE table_schema = DATABASE() AND table_name = 'app_oauth_credentials' AND column_name = 'developer_token';
            SET @sql = IF(@col_exists = 0, 'ALTER TABLE `app_oauth_credentials` ADD COLUMN `developer_token` varchar(255) DEFAULT NULL AFTER `google_ad_manager_network_code`;', 'SELECT "Column developer_token already exists";');
            PREPARE stmt FROM @sql;
            EXECUTE stmt;
            DEALLOCATE PREPARE stmt;
            """,
            reverse_sql="SELECT 1"
        ),
        
        migrations.RunSQL(
            """
            ALTER TABLE `app_oauth_credentials` 
            MODIFY COLUMN `user_id` varchar(36) DEFAULT NULL,
            MODIFY COLUMN `user_mail` char(50) DEFAULT NULL,
            MODIFY COLUMN `google_oauth2_client_id` varchar(255) NOT NULL,
            MODIFY COLUMN `google_oauth2_client_secret` varchar(255) NOT NULL,
            MODIFY COLUMN `google_ads_refresh_token` varchar(500) NOT NULL DEFAULT '',
            MODIFY COLUMN `google_ad_manager_network_code` bigint(50) DEFAULT NULL,
            MODIFY COLUMN `is_active` tinyint(1) DEFAULT 1,
            MODIFY COLUMN `created_at` timestamp NOT NULL DEFAULT current_timestamp(),
            MODIFY COLUMN `updated_at` timestamp NOT NULL DEFAULT current_timestamp() ON UPDATE current_timestamp()
            """,
            reverse_sql="SELECT 1"
        ),
        
        # Create app_master_plan table
        migrations.RunSQL(
            """
            CREATE TABLE `app_master_plan` (
                `master_plan_id` varchar(36) NOT NULL DEFAULT '',
                `master_plan_date` datetime DEFAULT NULL,
                `master_task_code` char(10) DEFAULT NULL,
                `master_task_plan` text DEFAULT NULL,
                `submitted_task` varchar(36) DEFAULT NULL,
                `assignment_to` varchar(36) DEFAULT NULL,
                `project_kategori` enum('finance','infra','softdev','ads','publishing','operations','other') DEFAULT NULL,
                `urgency` enum('P0','P1','P2') DEFAULT NULL,
                `execute_status` enum('in-progress','review','done') DEFAULT NULL,
                `catatan` text DEFAULT NULL,
                `mdb` varchar(36) DEFAULT NULL,
                `mdb_name` varchar(250) DEFAULT NULL,
                `mdd` datetime DEFAULT NULL,
                PRIMARY KEY (`master_plan_id`)
            ) ENGINE=InnoDB DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci
            """,
            # reverse_sql="DROP TABLE IF EXISTS `app_master_plan`"
        ),
        
        # Create sessions table
        migrations.RunSQL(
            """
            CREATE TABLE `sessions` (
                `id` varchar(255) NOT NULL,
                `user_id` bigint(20) unsigned DEFAULT NULL,
                `ip_address` varchar(45) DEFAULT NULL,
                `user_agent` text DEFAULT NULL,
                `payload` longtext NOT NULL,
                `last_activity` int(11) NOT NULL,
                PRIMARY KEY (`id`),
                KEY `sessions_user_id_index` (`user_id`),
                KEY `sessions_last_activity_index` (`last_activity`)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_unicode_ci
            """,
            # reverse_sql="DROP TABLE IF EXISTS `sessions`"
        ),
        
        # Create app_menu table
        migrations.RunSQL(
            """
            CREATE TABLE IF NOT EXISTS `app_menu` (
                `nav_id` varchar(10) NOT NULL,
                `portal_id` varchar(2) DEFAULT NULL,
                `nav_name` varchar(100) DEFAULT NULL,
                `nav_url` varchar(255) DEFAULT NULL,
                `nav_icon` varchar(50) DEFAULT NULL,
                `nav_parent` varchar(10) DEFAULT NULL,
                `nav_order` int(11) DEFAULT NULL,
                `active_st` enum('1','0') DEFAULT '1',
                `display_st` enum('1','0') DEFAULT '1',
                `nav_icon` varchar(50) DEFAULT NULL,
                `mdb` varchar(36) DEFAULT NULL,
                `mdb_name` varchar(50) DEFAULT NULL,
                `mdd` datetime DEFAULT NULL,
                PRIMARY KEY (`nav_id`),
                KEY `portal_id` (`portal_id`),
                CONSTRAINT `app_menu_ibfk_1` FOREIGN KEY (`portal_id`) REFERENCES `app_portal` (`portal_id`) ON UPDATE CASCADE
            ) ENGINE=InnoDB DEFAULT CHARSET=latin1
            """,
            # reverse_sql="DROP TABLE IF EXISTS `app_menu`"
        ),
        
        # Create app_menu_role table (requires app_menu and app_role table to exist)
        migrations.RunSQL(
            """
            CREATE TABLE `app_role_menu` (
                `role_id` varchar(5) NOT NULL,
                `nav_id` varchar(10) NOT NULL,
                `role_tp` varchar(4) NOT NULL DEFAULT '1111',
                PRIMARY KEY (`nav_id`,`role_id`),
                KEY `role_id` (`role_id`),
                CONSTRAINT `app_menu_role_ibfk_1` FOREIGN KEY (`role_id`) REFERENCES `app_role` (`role_id`) ON DELETE CASCADE ON UPDATE CASCADE,
                CONSTRAINT `app_menu_role_ibfk_2` FOREIGN KEY (`nav_id`) REFERENCES `app_menu` (`nav_id`) ON DELETE CASCADE ON UPDATE CASCADE
            ) ENGINE=InnoDB DEFAULT CHARSET=latin1
            """,
            # reverse_sql="DROP TABLE IF EXISTS `app_role_menu`"
        ),
        
        # Create app_portal table
        migrations.RunSQL(
            """
            CREATE TABLE `app_portal` (
                `portal_id` varchar(2) NOT NULL,
                `portal_nm` varchar(50) DEFAULT NULL,
                `portal_title` varchar(50) DEFAULT NULL,
                `portal_icon` varchar(100) DEFAULT NULL,
                `portal_logo` varchar(100) DEFAULT NULL,
                `site_title` varchar(100) DEFAULT NULL,
                `site_desc` varchar(100) DEFAULT NULL,
                `meta_desc` varchar(255) DEFAULT NULL,
                `meta_keyword` varchar(255) DEFAULT NULL,
                `create_by` varchar(10) DEFAULT NULL,
                `create_by_name` varchar(50) DEFAULT NULL,
                `create_date` datetime DEFAULT NULL,
                `mdb` varchar(36) DEFAULT NULL,
                `mdb_name` varchar(50) DEFAULT NULL,
                `mdd` datetime DEFAULT NULL,
                PRIMARY KEY (`portal_id`)
            ) ENGINE=InnoDB DEFAULT CHARSET=latin1
            """,
            # reverse_sql="DROP TABLE IF EXISTS `app_portal`"
        ),

        # migration for app_credentials table
        migrations.RunSQL(
            sql="""
            CREATE TABLE `app_credentials` (
                `account_id` int(11) unsigned NOT NULL AUTO_INCREMENT,
                `account_name` varchar(255) DEFAULT NULL,
                `user_mail` varchar(50) DEFAULT NULL,
                `client_id` varchar(255) DEFAULT NULL,
                `client_secret` varchar(255) DEFAULT NULL,
                `refresh_token` varchar(255) DEFAULT NULL,
                `network_code` bigint(20) unsigned DEFAULT NULL,
                `developer_token` varchar(255) DEFAULT NULL,
                `is_active` enum('1','0') DEFAULT NULL,
                `mdb` varchar(36) DEFAULT NULL,
                `mdb_name` varchar(250) DEFAULT NULL,
                `mdd` datetime DEFAULT NULL,
                PRIMARY KEY (`account_id`)
            ) ENGINE=InnoDB AUTO_INCREMENT=9 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;
            """,
            # reverse_sql="DROP TABLE IF EXISTS `app_credentials`;"
        ),

        # create media partner table
        migrations.RunSQL(
            sql="""
                CREATE TABLE `data_media_partner` (
                `partner_id` int NOT NULL AUTO_INCREMENT,
                `partner_name` varchar(100) DEFAULT NULL,
                `partner_contact` varchar(20) DEFAULT NULL,
                `partner_domain` varchar(255) DEFAULT NULL,
                `partner_region` varchar(100) DEFAULT NULL,
                `request_date` datetime DEFAULT NULL,
                `pic` varchar(100) DEFAULT NULL,
                `mdb` varchar(36) DEFAULT NULL,
                `mdb_name` varchar(50) DEFAULT NULL,
                `mdd` datetime DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (`partner_id`)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
            """,
            # reverse_sql="DROP TABLE IF EXISTS `data_media_partner`;"
        ),
    ]
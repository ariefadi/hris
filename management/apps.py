from django.apps import AppConfig
from .db_features_patch import (
    force_disable_mysql_returning_class,
    disable_returning_on_mysql,
    monkey_patch_mysql_insert_drop_returning,
    monkey_patch_insert_execute_sql_safe,
)

class ManagementConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'management'
    
    def ready(self):
        try:
            from management.googleads_patch_v2 import apply_googleads_patches
            success = apply_googleads_patches()
            if success:
                print("GoogleAds comprehensive patches applied successfully")
            else:
                print("GoogleAds patches failed to apply - using fallback error handling")
        except Exception as e:
            print(f"GoogleAds patch import failed: {e} - using fallback error handling")
        try:
            # Patch class-level flags first (affects future connections)
            force_disable_mysql_returning_class()
            # Patch any existing connection features
            disable_returning_on_mysql()
            # Compiler safety patches
            monkey_patch_mysql_insert_drop_returning()
            monkey_patch_insert_execute_sql_safe()
            print("[PATCH] MySQL returning disabled and compiler normalized for INSERT")
        except Exception as e:
            print(f"[PATCH] DB features patch failed: {e}")
from django.apps import AppConfig

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
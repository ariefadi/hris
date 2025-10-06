from django.db import models
import uuid

class AppOAuthCredentials(models.Model):
    id = models.UUIDField(primary_key=True, default=uuid.uuid4)
    user_id = models.CharField(max_length=255)
    user_mail = models.EmailField(max_length=255)
    google_oauth2_client_id = models.CharField(max_length=255)
    google_oauth2_client_secret = models.CharField(max_length=255)
    google_ads_client_id = models.CharField(max_length=255, null=True, blank=True)
    google_ads_client_secret = models.CharField(max_length=255, null=True, blank=True)
    google_ads_refresh_token = models.TextField(null=True, blank=True)
    google_ad_manager_network_code = models.CharField(max_length=50, null=True, blank=True)
    is_active = models.BooleanField(default=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = 'app_oauth_credentials'
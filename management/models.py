from django.db import models
import uuid

class AppOAuthCredentials(models.Model):
    account_id = models.IntegerField(primary_key=True, max_length=11)
    account_name = models.CharField(max_length=255)
    user_mail = models.EmailField(max_length=255)
    client_id = models.CharField(max_length=255)
    client_secret = models.CharField(max_length=255)
    refresh_token = models.TextField(null=True, blank=True)
    network_code = models.CharField(max_length=50, null=True, blank=True)
    is_active = models.BooleanField(default=True)
    mdb = models.CharField(max_length=36)
    mdb_name = models.CharField(max_length=255)
    mdd = models.DateTimeField(auto_now=True)
    class Meta:
        db_table = 'app_credentials'

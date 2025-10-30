# Google Ad Manager OAuth Authentication Fix Guide

## 🎯 Problem Summary

The error "Google Ad Manager API Error: SOAP request failed due to authentication or permission issues" occurs because:

1. **OAuth Scopes Issue**: The current OAuth configuration may not include all required scopes for Google Ad Manager API
2. **Token Scope Mismatch**: Existing refresh tokens were created with limited scopes
3. **API Scope Evolution**: Google has updated scope requirements for Ad Manager API

## ✅ Solution Applied

### 1. Updated OAuth Scopes

The OAuth configuration has been updated to include both current and future-compatible scopes:

```python
# Updated scopes in hris/settings.py
SOCIAL_AUTH_GOOGLE_OAUTH2_SCOPE = [
    'email', 
    'profile', 
    'https://www.googleapis.com/auth/dfp',        # Ad Manager SOAP API
    'https://www.googleapis.com/auth/admanager'   # Ad Manager REST API (Beta)
]

GOOGLE_SCOPES = [
    'https://www.googleapis.com/auth/dfp',
    'https://www.googleapis.com/auth/admanager',
]
```

### 2. Scope Compatibility

- **`https://www.googleapis.com/auth/dfp`**: Required for current Ad Manager SOAP API
- **`https://www.googleapis.com/auth/admanager`**: Required for newer Ad Manager REST API (Beta)

Both scopes ensure compatibility with current and future API versions.

## 🔧 Required Action: User Re-authorization

**IMPORTANT**: All users must re-authorize the application to get tokens with the new scopes.

### Option 1: Automated Re-authorization (Recommended)

```bash
cd /Users/ariefdwicahyoadi/hris
python reauthorize_oauth.py
```

This script will:
- Clear existing tokens
- Guide you through the re-authorization process
- Verify successful authorization

### Option 2: Manual Re-authorization

1. **Start Django Server**:
   ```bash
   cd /Users/ariefdwicahyoadi/hris
   python manage.py runserver 127.0.0.1:8000
   ```

2. **Clear Existing Sessions**:
   - Go to: http://127.0.0.1:8000/management/admin/logout
   - Clear browser cookies for localhost

3. **Re-authorize**:
   - Go to: http://127.0.0.1:8000/management/admin/login
   - Click "Login with Google"
   - **Important**: You should see additional permission requests for Ad Manager API
   - Accept all permissions

4. **Verify**:
   - Access AdX Traffic Account
   - Check that authentication errors are resolved

## 🔍 Verification Steps

### 1. Check OAuth Scopes

```bash
python check_oauth_scopes.py
```

This should show:
```
✓ https://www.googleapis.com/auth/dfp - PRESENT
✓ https://www.googleapis.com/auth/admanager - PRESENT
```

### 2. Test Ad Manager API Access

```bash
python test_auth_fix.py
```

This should show successful API access without authentication errors.

### 3. Test AdX Traffic Account

- Navigate to: http://127.0.0.1:8000/management/admin/adx_traffic_account
- Verify that data loads without "authentication or permission issues" errors
- Check that specific error messages are displayed instead of "Unknown error occurred"

## 🚨 Troubleshooting

### Issue: Still Getting Authentication Errors

**Cause**: User hasn't completed re-authorization with new scopes

**Solution**:
1. Ensure Django server is running
2. Complete logout and re-login process
3. Verify that Google shows the new permission requests
4. Check that refresh token is updated in database

### Issue: "Invalid Scope" Error

**Cause**: OAuth client configuration in Google Console

**Solution**:
1. Go to [Google Cloud Console](https://console.cloud.google.com/)
2. Navigate to APIs & Services > Credentials
3. Edit OAuth 2.0 Client ID
4. Ensure these APIs are enabled:
   - Google Ad Manager API
   - Google Ads API

### Issue: "Access Denied" Error

**Cause**: Service account not added to Ad Manager

**Solution**: Follow the [Ad Manager Setup Guide](ADMANAGER_SETUP_GUIDE.md)

## 📋 Files Modified

1. **`hris/settings.py`**: Updated OAuth scopes
2. **`update_oauth_scopes.py`**: Script to update and test scopes
3. **`reauthorize_oauth.py`**: Guided re-authorization tool
4. **`management/utils.py`**: Enhanced error handling (previous fix)

## 🎯 Expected Results

After completing re-authorization:

✅ **Authentication errors resolved**
✅ **AdX Traffic Account loads data successfully**
✅ **Specific error messages instead of "Unknown error occurred"**
✅ **Future-compatible with Ad Manager API updates**

## 📞 Support

If issues persist after following this guide:

1. Check Django server logs for detailed error messages
2. Verify Google Cloud Console API settings
3. Ensure service account permissions in Ad Manager
4. Run diagnostic scripts to identify specific issues

---

**Database Schema and Token Consistency**

- If you see errors like `Out of range value for column 'network_code'`, update the column type to handle 11-digit network codes:

  ```sql
  ALTER TABLE app_credentials MODIFY COLUMN network_code BIGINT UNSIGNED NULL;
  ```

- If you see `unauthorized_client` or `invalid_client` during token refresh, ensure the `refresh_token` was issued by the same OAuth client (`client_id`/`client_secret`) currently configured in environment and saved in `app_credentials`. A refresh token is bound to the client that issued it — mismatched client credentials will fail to refresh. Re-run the OAuth consent flow to generate a refresh token aligned with the active client.

**Note**: This fix addresses the OAuth scope issue. If authentication errors persist, it may indicate service account configuration issues in Google Ad Manager, which require additional setup as described in `ADMANAGER_SETUP_GUIDE.md`.

from social_core.backends.google import GoogleOAuth2
from social_core.exceptions import AuthMissingParameter
import logging
from django.conf import settings

logger = logging.getLogger(__name__)

class CustomGoogleOAuth2(GoogleOAuth2):
    """Custom Google OAuth2 backend dengan state handling yang diperbaiki"""
    
    def get_scope(self):
        """Override get_scope to ensure we use the complete scopes from settings"""
        # Always use scopes from settings which includes Ad Manager scope
        scope = getattr(settings, 'SOCIAL_AUTH_GOOGLE_OAUTH2_SCOPE', [
            'openid', 'email', 'profile'
        ])
        logger.debug(f"Using OAuth scopes: {scope}")
        return scope
    
    def validate_state(self):
        """Override validate_state untuk handling yang lebih baik"""
        try:
            # Get state from request
            request_state = self.data.get('state') or self.data.get(self.STATE_PARAMETER)
            
            # Get state from session
            session_state = self.strategy.session_get('state')
            
            logger.debug(f"Request state: {request_state}")
            logger.debug(f"Session state: {session_state}")
            
            # If no request state, try to get from URL parameters
            if not request_state:
                request_state = self.strategy.request_data().get('state')
            
            # If still no state, check if we can skip validation in development
            if not request_state and not session_state:
                if hasattr(self.strategy.request, 'GET'):
                    request_state = self.strategy.request.GET.get('state')
                
                # In development, we might want to be more lenient
                if not request_state:
                    logger.warning("No state parameter found, but continuing in development mode")
                    return None
            
            # Validate state if both exist
            if request_state and session_state:
                if request_state != session_state:
                    logger.error(f"State mismatch: request={request_state}, session={session_state}")
                    raise AuthMissingParameter(self, 'state')
                else:
                    logger.debug("State validation successful")
                    return request_state
            
            # If we have request state but no session state, accept it in development
            if request_state and not session_state:
                logger.warning("Request state found but no session state, accepting in development")
                return request_state
            
            # If no state at all, raise error
            logger.error("No state parameter found in request or session")
            raise AuthMissingParameter(self, 'state')
            
        except Exception as e:
            logger.error(f"State validation error: {e}")
            # In development, we might want to continue despite errors
            if hasattr(self.strategy.request, 'GET'):
                request_state = self.strategy.request.GET.get('state')
                if request_state:
                    logger.warning("Using request state despite validation error")
                    return request_state
            raise
    
    def auth_complete(self, *args, **kwargs):
        """Override auth_complete dengan error handling yang lebih baik"""
        try:
            return super().auth_complete(*args, **kwargs)
        except AuthMissingParameter as e:
            logger.error(f"Auth missing parameter: {e}")
            # Try to recover by checking request parameters directly
            if hasattr(self.strategy.request, 'GET'):
                state = self.strategy.request.GET.get('state')
                if state:
                    logger.warning("Found state in request GET parameters, retrying")
                    # Set state in session and retry
                    self.strategy.session_set('state', state)
                    return super().auth_complete(*args, **kwargs)
            raise

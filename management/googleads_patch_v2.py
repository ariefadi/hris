"""Comprehensive patch for GoogleAds library compatibility issues"""

import os
import tempfile

try:
    import googleads
    import googleads.common
    from googleads import ad_manager
except Exception:
    googleads = None
    ad_manager = None

import yaml
from django.conf import settings
from .database import data_mysql  # Mengubah import path

# Store original methods
_original_load_from_storage = (
    getattr(getattr(googleads, "common", None), "LoadFromStorage", None) if googleads else None
)
_original_make_soap_request = None

def get_user_credentials(user_mail):
    """Get user's Google Ad Manager credentials from database"""
    try:
        db = data_mysql()
        sql = """
            SELECT client_id, client_secret, refresh_token, network_code, developer_token
            FROM app_credentials
            WHERE user_mail = %s
            LIMIT 1
        """

        if not db.execute_query(sql, (user_mail,)):
            return {
                'status': False,
                'error': 'Failed to query app_credentials'
            }
        user_data = db.cur_hris.fetchone()
        
        if not user_data:
            return {
                'status': False,
                'error': f'No credentials found for user: {user_mail}'
            }
            
        # Validate required fields
        required_fields = ['client_id', 'client_secret', 'refresh_token', 'network_code']
        missing_fields = [field for field in required_fields if not user_data.get(field)]
        
        if missing_fields:
            return {
                'status': False,
                'error': f'Missing required credentials: {", ".join(missing_fields)}'
            }
            
        return {
            'status': True,
            'credentials': {
                'client_id': user_data['client_id'],
                'client_secret': user_data['client_secret'],
                'refresh_token': user_data['refresh_token'],
                'network_code': user_data['network_code'],
                'developer_token': user_data.get('developer_token', '')
            }
        }
    except Exception as e:
        return {
            'status': False,
            'error': f'Database error: {str(e)}'
        }

def patched_load_from_storage(*args, **kwargs):
    """Patched LoadFromStorage to handle encoding issues and use user credentials"""
    try:
        # Get user_mail from request/session if available
        from django.http import HttpRequest
        from threading import current_thread
        request = getattr(current_thread(), 'request', None)
        
        user_mail = None
        if isinstance(request, HttpRequest):
            user_id = request.session.get('hris_admin', {}).get('user_id')
            if user_id:
                # Get user_mail from database
                user_data = data_mysql().get_user_by_id(user_id)
                if user_data['status'] and user_data['data']:
                    user_mail = user_data['data'].get('user_mail')

        if user_mail:
            # Get user credentials from database
            creds_result = get_user_credentials(user_mail)
            if creds_result['status']:
                credentials = creds_result['credentials']
                
                # Create config dictionary with user credentials
                config = {
                    'ad_manager': {
                        'application_name': 'AdX Manager Dashboard',
                        'network_code': credentials['network_code'],
                        'client_id': credentials['client_id'],
                        'client_secret': credentials['client_secret'],
                        'refresh_token': credentials['refresh_token']
                    }
                }
                
                if credentials.get('developer_token'):
                    config['ad_manager']['developer_token'] = credentials['developer_token']
                
                # Write config to temp file
                with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False, encoding='utf-8') as temp_file:
                    yaml.safe_dump(config, temp_file, default_flow_style=False, allow_unicode=True)
                
                try:
                    # Call original method with temp config file
                    return _original_load_from_storage(temp_file.name)
                finally:
                    # Clean up temp file
                    if os.path.exists(temp_file.name):
                        os.unlink(temp_file.name)
        
        # Extract path from args or kwargs for fallback
        path = None
        if args:
            path = args[0]
        elif 'path' in kwargs:
            path = kwargs['path']
        
        if not path or not os.path.exists(path):
            # Fallback to original method if no valid path
            return _original_load_from_storage(*args, **kwargs)
        
        # Read and validate YAML content
        with open(path, 'r', encoding='utf-8') as f:
            yaml_content = f.read()
        
        # Parse YAML to ensure it's valid
        config = yaml.safe_load(yaml_content)
        
        if config is None or not isinstance(config, dict):
            return _original_load_from_storage(*args, **kwargs)
        
        # Ensure all string values are properly encoded
        if 'ad_manager' in config:
            ad_manager_config = config['ad_manager']
            
            # Convert all credential values to strings and strip whitespace
            for key in ['developer_token', 'client_id', 'client_secret', 'refresh_token']:
                if key in ad_manager_config:
                    value = ad_manager_config[key]
                    if isinstance(value, (int, float)):
                        ad_manager_config[key] = str(value)
                    elif isinstance(value, str):
                        ad_manager_config[key] = value.strip()
            
            # Ensure network_code is integer
            network_code = None
            if 'network_code' in ad_manager_config:
                try:
                    network_code = ad_manager_config['network_code']
                    if isinstance(network_code, str):
                        # Remove any non-numeric characters
                        clean_code = ''.join(filter(str.isdigit, network_code))
                        if clean_code:
                            network_code = int(clean_code)
                        else:
                            # Use network code from settings if available
                            network_code = getattr(settings, 'GOOGLE_AD_MANAGER_NETWORK_CODE', None)
                    else:
                        network_code = int(network_code)
                except (ValueError, TypeError):
                    # Use network code from settings if available
                    network_code = getattr(settings, 'GOOGLE_AD_MANAGER_NETWORK_CODE', None)
            
            if not network_code:
                raise Exception("Network code not found in settings")
            ad_manager_config['network_code'] = network_code
            
            # Write corrected YAML back to a new temp file
            corrected_yaml = tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False, encoding='utf-8')
            yaml.safe_dump(config, corrected_yaml, default_flow_style=False, allow_unicode=True)
            corrected_yaml.close()
            
            try:
                # Update args to use corrected file path
                new_args = list(args)
                if new_args:
                    new_args[0] = corrected_yaml.name
                else:
                    kwargs['path'] = corrected_yaml.name
                
                # Call original method with corrected file
                result = _original_load_from_storage(*new_args, **kwargs)
                return result
            finally:
                # Clean up temp file
                if os.path.exists(corrected_yaml.name):
                    os.unlink(corrected_yaml.name)
        
        # Fallback to original method
        return _original_load_from_storage(*args, **kwargs)
        
    except Exception as e:
        print(f"[PATCH] LoadFromStorage patch failed: {e}")
        if _original_load_from_storage is None:
            raise
        try:
            return _original_load_from_storage(*args, **kwargs)
        except Exception:
            raise e

def patched_get_current_network(self):
    """Patched getCurrentNetwork to handle TypeError"""
    try:
        # Try original method first
        return self._original_get_current_network()
    except TypeError as e:
        if "argument should be integer or bytes-like object, not 'str'" in str(e):
            print(f"[PATCH] getCurrentNetwork TypeError caught, using fallback")
            # Return a mock network object with required attributes
            class MockNetwork:
                def __init__(self):
                    network_code = getattr(settings, 'GOOGLE_AD_MANAGER_NETWORK_CODE', None)
                    if not network_code:
                        raise Exception("Network code not found in settings")
                    self.network_code = network_code
                    self.display_name = "AdX Manager Dashboard"
                    self.currency_code = "USD"
                    self.time_zone = "America/New_York"
            
            return MockNetwork()
        else:
            raise e
    except Exception as e:
        print(f"[PATCH] getCurrentNetwork failed with: {e}")
        # Return mock network for any other errors
        class MockNetwork:
            def __init__(self):
                network_code = getattr(settings, 'GOOGLE_AD_MANAGER_NETWORK_CODE', None)
                if not network_code:
                    raise Exception("Network code not found in settings")
                self.network_code = network_code
                self.display_name = "AdX Manager Dashboard"
                self.currency_code = "USD"
                self.time_zone = "America/New_York"
        
        return MockNetwork()

def apply_make_soap_request_patch(client):
    """Apply comprehensive patch for XML parsing issues in MakeSoapRequest"""
    import zeep.exceptions
    import googleads.errors
    
    # Patch the MakeSoapRequest method in the client's service classes
    for service_name in ['NetworkService', 'InventoryService', 'UserService', 'ReportService']:
        try:
            service = client.GetService(service_name, version='v202508')
            if hasattr(service, 'MakeSoapRequest'):
                original_method = service.MakeSoapRequest
                
                def create_patched_method(orig_method):
                    def patched_make_soap_request(method_name, args):
                        """Patched MakeSoapRequest to handle XML parsing issues"""
                        try:
                            packed_args = service._PackArguments(method_name, args)
                            soap_headers = service._GetSoapHeaders()
                            soap_service_method = getattr(service.zeep_client.service, method_name)
                            
                            return soap_service_method(
                                *packed_args, _soapheaders=soap_headers)['body']['rval']
                                
                        except zeep.exceptions.Fault as e:
                            error_list = ()
                            if e.detail is not None:
                                try:
                                    # Handle the XML parsing issue by converting to string first
                                    detail_str = str(e.detail) if not isinstance(e.detail, str) else e.detail
                                    
                                    # Try to find the ApiExceptionFault in the detail
                                    namespace = service._GetBindingNamespace()
                                    fault_element_name = f'{{{namespace}}}ApiExceptionFault'
                                    
                                    # Use lxml to parse the detail safely
                                    import lxml.etree as etree
                                    if isinstance(e.detail, str):
                                        detail_element = etree.fromstring(detail_str.encode('utf-8'))
                                    else:
                                        detail_element = e.detail
                                    
                                    underlying_exception = detail_element.find(fault_element_name)
                                    
                                    if underlying_exception is not None:
                                        fault_type = service.zeep_client.get_element(fault_element_name)
                                        fault = fault_type.parse(underlying_exception, service.zeep_client.wsdl.types)
                                        error_list = fault.errors or error_list
                                        
                                except (TypeError, AttributeError, ValueError) as parse_error:
                                    print(f"[PATCH] XML parsing error handled: {parse_error}")
                                    # If parsing fails, continue with empty error_list
                                    pass
                                    
                            raise googleads.errors.GoogleAdsServerFault(
                                e.detail, errors=error_list, message=e.message)
                        except Exception as other_error:
                            print(f"[PATCH] Other error in MakeSoapRequest: {other_error}")
                            raise
                    
                    return patched_make_soap_request
                
                # Apply the patch
                service.MakeSoapRequest = create_patched_method(original_method)
                print(f"[PATCH] Successfully patched MakeSoapRequest for {service_name}")
                
        except Exception as e:
            print(f"[PATCH] Could not patch {service_name}: {e}")
            continue

def patch_get_current_network(client):
    """Patch getCurrentNetwork to handle TypeError"""
    original_get_current_network = client.GetService('NetworkService').getCurrentNetwork
    
    def patched_get_current_network(*args, **kwargs):
        """Patched getCurrentNetwork to handle TypeError"""
        try:
            return original_get_current_network(*args, **kwargs)
        except TypeError as e:
            print(f"[PATCH] getCurrentNetwork patch handling TypeError: {e}")
            # Get user credentials from current request if available
            from threading import current_thread
            request = getattr(current_thread(), 'request', None)
            if request:
                user_id = request.session.get('hris_admin', {}).get('user_id')
                if user_id:
                    user_data = data_mysql().get_user_by_id(user_id)
                    if user_data['status'] and user_data['data']:
                        user_mail = user_data['data'].get('user_mail')
                        if user_mail:
                            creds = get_user_credentials(user_mail)
                            if creds['status']:
                                return {'networkCode': creds['credentials']['network_code']}
            
            # Fallback to settings if available
            network_code = getattr(settings, 'GOOGLE_AD_MANAGER_NETWORK_CODE', None)
            if not network_code:
                raise Exception("Network code not found in settings")
            return {'networkCode': network_code}
        except Exception as e:
            print(f"[PATCH] getCurrentNetwork patch handling error: {e}")
            raise

    client.GetService('NetworkService').getCurrentNetwork = patched_get_current_network

def patch_get_all_networks(client):
    """Patch getAllNetworks to handle TypeError"""
    original_get_all_networks = client.GetService('NetworkService').getAllNetworks
    
    def patched_get_all_networks(*args, **kwargs):
        """Patched getAllNetworks to handle TypeError"""
        try:
            return original_get_all_networks(*args, **kwargs)
        except TypeError as e:
            print(f"[PATCH] getAllNetworks patch handling TypeError: {e}")
            # Get user credentials from current request if available
            from threading import current_thread
            request = getattr(current_thread(), 'request', None)
            if request:
                user_id = request.session.get('hris_admin', {}).get('user_id')
                if user_id:
                    user_data = data_mysql().get_user_by_id(user_id)
                    if user_data['status'] and user_data['data']:
                        user_mail = user_data['data'].get('user_mail')
                        if user_mail:
                            creds = get_user_credentials(user_mail)
                            if creds['status']:
                                return [{'networkCode': creds['credentials']['network_code']}]
            
            # Fallback to settings if available
            network_code = getattr(settings, 'GOOGLE_AD_MANAGER_NETWORK_CODE', None)
            if not network_code:
                raise Exception("Network code not found in settings")
            return [{'networkCode': network_code}]
        except Exception as e:
            print(f"[PATCH] getAllNetworks patch handling error: {e}")
            raise
    
    client.GetService('NetworkService').getAllNetworks = patched_get_all_networks

def patch_run_report_job(client):
    """Patch runReportJob in ReportService"""
    original_run_report_job = client.GetService('ReportService').runReportJob
    
    def patched_run_report_job(*args, **kwargs):
        """Patched runReportJob to handle various errors"""
        try:
            return original_run_report_job(*args, **kwargs)
        except Exception as e:
            print(f"[PATCH] runReportJob patch handling error: {e}")
            if "network code" in str(e).lower():
                # Try to get network code from settings
                network_code = getattr(settings, 'GOOGLE_AD_MANAGER_NETWORK_CODE', None)
                if not network_code:
                    raise Exception("Network code not found in settings")
                # Update report query with correct network code
                if args and isinstance(args[0], dict):
                    args[0]['networkCode'] = network_code
                    return original_run_report_job(*args, **kwargs)
            raise
    
    client.GetService('ReportService').runReportJob = patched_run_report_job

def patch_get_report_job_status(client):
    """Patch getReportJobStatus in ReportService"""
    original_get_report_job_status = client.GetService('ReportService').getReportJobStatus
    
    def patched_get_report_job_status(*args, **kwargs):
        """Patched getReportJobStatus to handle various errors"""
        try:
            return original_get_report_job_status(*args, **kwargs)
        except Exception as e:
            print(f"[PATCH] getReportJobStatus patch handling error: {e}")
            if "network code" in str(e).lower():
                # Try to get network code from settings
                network_code = getattr(settings, 'GOOGLE_AD_MANAGER_NETWORK_CODE', None)
                if not network_code:
                    raise Exception("Network code not found in settings")
                # Update job ID with correct network code
                if args and isinstance(args[0], dict):
                    args[0]['networkCode'] = network_code
                    return original_get_report_job_status(*args, **kwargs)
            raise
    
    client.GetService('ReportService').getReportJobStatus = patched_get_report_job_status

def patch_download_report(client):
    """Patch DownloadReportToString in GetDataDownloader"""
    try:
        data_downloader = client.GetDataDownloader()
        if not hasattr(data_downloader, 'DownloadReportToString'):
            print("[PATCH] DataDownloader does not have DownloadReportToString method, skipping patch")
            return
            
        original_download_report = data_downloader.DownloadReportToString
        
        def patched_download_report(*args, **kwargs):
            """Patched DownloadReportToString to handle various errors"""
            try:
                return original_download_report(*args, **kwargs)
            except Exception as e:
                print(f"[PATCH] DownloadReportToString patch handling error: {e}")
                if "network code" in str(e).lower():
                    # Try to get network code from settings
                    network_code = getattr(settings, 'GOOGLE_AD_MANAGER_NETWORK_CODE', None)
                    if not network_code:
                        raise Exception("Network code not found in settings")
                    # Update report job ID with correct network code
                    if args and isinstance(args[0], dict):
                        args[0]['networkCode'] = network_code
                        return original_download_report(*args, **kwargs)
                raise
        
        data_downloader.DownloadReportToString = patched_download_report
        print("[PATCH] Successfully patched DataDownloader.DownloadReportToString")
    except Exception as e:
        print(f"[PATCH] Failed to patch DataDownloader: {e}")

def patch_user_service(client):
    """Patch UserService methods to handle data type conversion errors"""
    try:
        user_service = client.GetService('UserService', version='v202508')
        if not hasattr(user_service, 'getUsersByStatement'):
            print("[PATCH] UserService does not have getUsersByStatement method, skipping patch")
            return
            
        original_get_users = user_service.getUsersByStatement
        
        def patched_get_users(*args, **kwargs):
            """Patched getUsersByStatement to handle data type conversion errors"""
            try:
                print(f"[PATCH] getUsersByStatement called with args: {len(args)} kwargs: {len(kwargs)}")
                result = original_get_users(*args, **kwargs)
                print(f"[PATCH] getUsersByStatement successful")
                return result
            except Exception as e:
                print(f"[PATCH] getUsersByStatement patch handling error: {e}")
                if "argument should be integer or bytes-like object, not 'str'" in str(e):
                    print(f"[PATCH] getUsersByStatement suppressed SOAP encoding error: {e}")
                    # Return empty result for this specific error
                    return {'results': [], 'totalResultSetSize': 0}
                print(f"[PATCH] getUsersByStatement caught unexpected error: {e}")
                # For any other exception, return empty result to prevent crashes
                return {'results': [], 'totalResultSetSize': 0}
        
        user_service.getUsersByStatement = patched_get_users
        print("[PATCH] Successfully patched UserService.getUsersByStatement")
    except Exception as e:
        print(f"[PATCH] Failed to patch UserService: {e}")

def patch_inventory_service(client):
    """Patch InventoryService methods to handle data type conversion errors"""
    try:
        inventory_service = client.GetService('InventoryService', version='v202508')
        if not hasattr(inventory_service, 'getAdUnitsByStatement'):
            print("[PATCH] InventoryService does not have getAdUnitsByStatement method, skipping patch")
            return
            
        original_get_ad_units = inventory_service.getAdUnitsByStatement
        
        def patched_get_ad_units(*args, **kwargs):
            """Patched getAdUnitsByStatement to handle data type conversion errors"""
            try:
                print(f"[PATCH] getAdUnitsByStatement called with args: {len(args)} kwargs: {len(kwargs)}")
                result = original_get_ad_units(*args, **kwargs)
                print(f"[PATCH] getAdUnitsByStatement successful")
                return result
            except TypeError as e:
                if "argument should be integer or bytes-like object, not 'str'" in str(e):
                    print(f"[PATCH] getAdUnitsByStatement suppressed SOAP encoding error: {e}")
                    # Return empty result for this specific error
                    return {'results': [], 'totalResultSetSize': 0}
                else:
                    print(f"[PATCH] getAdUnitsByStatement re-raising non-SOAP error: {e}")
                    raise
            except Exception as e:
                print(f"[PATCH] getAdUnitsByStatement caught unexpected error: {e}")
                # For any other exception, return empty result to prevent crashes
                return {'results': [], 'totalResultSetSize': 0}
        
        inventory_service.getAdUnitsByStatement = patched_get_ad_units
        print("[PATCH] Successfully patched InventoryService.getAdUnitsByStatement")
    except Exception as e:
        print(f"[PATCH] Failed to patch InventoryService: {e}")

def apply_all_patches(client):
    """Apply all patches to the client"""
    apply_make_soap_request_patch(client)
    patch_get_current_network(client)
    patch_get_all_networks(client)
    patch_run_report_job(client)
    patch_get_report_job_status(client)
    patch_download_report(client)
    patch_user_service(client)
    patch_inventory_service(client)

def apply_googleads_patches(create_test_client=False):
    """Apply all GoogleAds library patches"""
    if googleads is None or ad_manager is None or _original_load_from_storage is None:
        return False

    try:
        # Replace original LoadFromStorage with patched version
        googleads.common.LoadFromStorage = patched_load_from_storage
        print("[PATCH] Applied LoadFromStorage patch")

        if create_test_client:
            client = ad_manager.AdManagerClient.LoadFromStorage()
            apply_all_patches(client)
            print("[PATCH] Applied all AdManagerClient patches")

        return True
    except Exception as e:
        print(f"[PATCH] Failed to apply GoogleAds patches: {e}")
        return False
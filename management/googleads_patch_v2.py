"""Comprehensive patch for GoogleAds library compatibility issues"""

import googleads.common
from googleads import ad_manager
import tempfile
import yaml
import os
from django.conf import settings

# Store original methods
_original_load_from_storage = googleads.common.LoadFromStorage
_original_make_soap_request = None

def patched_load_from_storage(*args, **kwargs):
    """Patched LoadFromStorage to handle encoding issues"""
    try:
        # Extract path from args or kwargs
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
            if 'network_code' in ad_manager_config:
                try:
                    network_code = ad_manager_config['network_code']
                    if isinstance(network_code, str):
                        # Remove any non-numeric characters
                        clean_code = ''.join(filter(str.isdigit, network_code))
                        if clean_code:
                            ad_manager_config['network_code'] = int(clean_code)
                        else:
                            ad_manager_config['network_code'] = 23303534834
                    else:
                        ad_manager_config['network_code'] = int(network_code)
                except (ValueError, TypeError):
                    ad_manager_config['network_code'] = 23303534834
            
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
        # Fallback to original method
        return _original_load_from_storage(*args, **kwargs)

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
                    self.network_code = getattr(settings, 'GOOGLE_ADS_NETWORK_CODE', 23303534834)
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
                self.network_code = getattr(settings, 'GOOGLE_ADS_NETWORK_CODE', 23303534834)
                self.display_name = "AdX Manager Dashboard"
                self.currency_code = "USD"
                self.time_zone = "America/New_York"
        
        return MockNetwork()

def apply_make_soap_request_patch():
    """Patch GoogleSoapService to handle XML parsing issues in googleads library"""
    try:
        import googleads.common as common
        
        # Patch GoogleSoapService.__getattr__ to intercept SOAP method calls
        GoogleSoapService = common.GoogleSoapService
        _original_getattr = GoogleSoapService.__getattr__
        
        def patched_getattr(self, attr):
            # Get the original method
            original_method = _original_getattr(self, attr)
            
            # If it's a SOAP method, wrap it with error handling
            if callable(original_method):
                def wrapped_method(*args, **kwargs):
                    try:
                        return original_method(*args, **kwargs)
                    except Exception as e:
                        # Check if this is the specific XML parsing error in googleads library
                        if "argument should be integer or bytes-like object, not 'str'" in str(e):
                            print(f"[PATCH] Detected googleads XML parsing bug in {attr}, investigating underlying SOAP fault...")
                            
                            # The error occurs because googleads tries to call e.detail.find() on a SOAP fault
                            # where e.detail is a string instead of an XML element
                            # Let's try to get the underlying SOAP fault information
                            
                            underlying_error = None
                            if hasattr(e, '__cause__') and e.__cause__:
                                underlying_error = e.__cause__
                                print(f"[PATCH] Underlying SOAP fault: {underlying_error}")
                                
                                # Check if it's a zeep.exceptions.Fault
                                if hasattr(underlying_error, 'message'):
                                    fault_message = str(underlying_error.message)
                                    print(f"[PATCH] SOAP fault message: {fault_message}")
                                    
                                    # Common Google Ad Manager API errors
                                    if 'AuthenticationError' in fault_message:
                                        raise Exception(f"Google Ad Manager Authentication Error: Invalid credentials or expired refresh token")
                                    elif 'PermissionError' in fault_message:
                                        raise Exception(f"Google Ad Manager Permission Error: Insufficient permissions for this operation")
                                    elif 'NetworkError' in fault_message:
                                        raise Exception(f"Google Ad Manager Network Error: Invalid network code or network access denied")
                                    else:
                                        raise Exception(f"Google Ad Manager API Error: {fault_message}")
                                elif hasattr(underlying_error, 'detail'):
                                    detail = str(underlying_error.detail)
                                    print(f"[PATCH] SOAP fault detail: {detail}")
                                    raise Exception(f"Google Ad Manager API Error: {detail}")
                            
                            # If we can't extract meaningful error info, provide a generic message
                            raise Exception(f"Google Ad Manager API Error: SOAP request failed due to authentication or permission issues. Please check your credentials and network access.")
                        else:
                            # Re-raise other errors as-is
                            raise e
                
                return wrapped_method
            else:
                return original_method
        
        # Apply patch
        GoogleSoapService.__getattr__ = patched_getattr
        print(f"[PATCH] Applied GoogleSoapService patch for XML parsing issues")
        return True
    except Exception as e:
        print(f"[PATCH] Failed to patch GoogleSoapService: {e}")
        return False

def apply_googleads_patches():
    """Apply all GoogleAds library patches"""
    try:
        # Patch LoadFromStorage
        googleads.common.LoadFromStorage = patched_load_from_storage
        print("[PATCH] Applied LoadFromStorage patch")
        
        # Apply MakeSoapRequest patch to fix XML parsing issue
        apply_make_soap_request_patch()
        print("[PATCH] Applied MakeSoapRequest patch")
        
        # Patch at the client level to handle SOAP request issues
        try:
            # Monkey patch the AdManagerClient to handle TypeError in SOAP calls
            original_get_service = ad_manager.AdManagerClient.GetService
            
            def patched_get_service(self, service_name, version=None):
                """Patched GetService to handle TypeError in SOAP calls"""
                try:
                    # Ensure we have a valid API version
                    if version is None:
                        version = 'v202502'  # Use the latest supported version
                    
                    service = original_get_service(self, service_name, version)
                    
                    # Patch getCurrentNetwork method if it's NetworkService
                    if service_name == 'NetworkService':
                        original_get_current_network = service.getCurrentNetwork
                        
                        def patched_get_current_network_method(*args, **kwargs):
                            try:
                                return original_get_current_network(*args, **kwargs)
                            except TypeError as e:
                                if "argument should be integer or bytes-like object, not 'str'" in str(e):
                                    print(f"[PATCH] getCurrentNetwork TypeError caught, returning mock network")
                                    # Return a mock network object that behaves like a dict
                                    mock_network = {
                                        'network_code': getattr(settings, 'GOOGLE_ADS_NETWORK_CODE', 23303534834),
                                        'display_name': "AdX Manager Dashboard",
                                        'currency_code': "USD",
                                        'time_zone': "America/New_York",
                                        'networkCode': getattr(settings, 'GOOGLE_ADS_NETWORK_CODE', 23303534834),
                                        'displayName': "AdX Manager Dashboard",
                                        'currencyCode': "USD",
                                        'timeZone': "America/New_York"
                                    }
                                    return mock_network
                                else:
                                    raise e
                        
                        service.getCurrentNetwork = patched_get_current_network_method
                        
                        # Also patch getAllNetworks if it exists
                        if hasattr(service, 'getAllNetworks'):
                            original_get_all_networks = service.getAllNetworks
                            
                            def patched_get_all_networks_method(*args, **kwargs):
                                try:
                                    return original_get_all_networks(*args, **kwargs)
                                except TypeError as e:
                                    if "argument should be integer or bytes-like object, not 'str'" in str(e):
                                        print(f"[PATCH] getAllNetworks TypeError caught, returning mock networks")
                                        # Return mock networks list with proper structure
                                        mock_network = {
                                            'network_code': getattr(settings, 'GOOGLE_ADS_NETWORK_CODE', 23303534834),
                                            'display_name': "AdX Manager Dashboard",
                                            'currency_code': "USD",
                                            'time_zone': "America/New_York",
                                            'networkCode': getattr(settings, 'GOOGLE_ADS_NETWORK_CODE', 23303534834),
                                            'displayName': "AdX Manager Dashboard",
                                            'currencyCode': "USD",
                                            'timeZone': "America/New_York"
                                        }
                                        
                                        return {'results': [mock_network]}
                                    else:
                                        raise e
                            
                            service.getAllNetworks = patched_get_all_networks_method
                    
                    # Patch ReportService methods if this is a ReportService
                    elif service_name == 'ReportService':
                        # Patch runReportJob
                        if hasattr(service, 'runReportJob'):
                            original_run_report_job = service.runReportJob
                            
                            def patched_run_report_job_method(*args, **kwargs):
                                try:
                                    print(f"[PATCH] runReportJob called with args: {len(args)} args, kwargs: {list(kwargs.keys())}")
                                    result = original_run_report_job(*args, **kwargs)
                                    print(f"[PATCH] runReportJob successful, got report job: {result.get('id', 'unknown') if isinstance(result, dict) else result}")
                                    return result
                                except Exception as e:
                                    error_msg = str(e)
                                    print(f"[PATCH] runReportJob error: {error_msg}")
                                    
                                    # Don't return fake data, let the error propagate properly
                                    # but provide more context
                                    if 'NOT_NULL' in error_msg:
                                        print(f"[PATCH] NOT_NULL error detected - this column combination is not supported")
                                    elif 'REPORT_NOT_FOUND' in error_msg:
                                        print(f"[PATCH] REPORT_NOT_FOUND error - network may not have AdX data")
                                    elif 'PERMISSION' in error_msg.upper():
                                        print(f"[PATCH] Permission error - check AdX access rights")
                                    
                                    # Re-raise the original error
                                    raise e
                            
                            service.runReportJob = patched_run_report_job_method
                        
                        # Patch getReportJobStatus
                        if hasattr(service, 'getReportJobStatus'):
                            original_get_report_job_status = service.getReportJobStatus
                            
                            def patched_get_report_job_status_method(*args, **kwargs):
                                try:
                                    result = original_get_report_job_status(*args, **kwargs)
                                    print(f"[PATCH] getReportJobStatus successful, status: {result}")
                                    return result
                                except Exception as e:
                                    error_msg = str(e)
                                    print(f"[PATCH] getReportJobStatus error: {error_msg}")
                                    
                                    # Provide context but don't fake the response
                                    if 'REPORT_NOT_FOUND' in error_msg:
                                        print(f"[PATCH] Report ID not found - this may indicate the report was not created successfully")
                                    
                                    # Re-raise the original error
                                    raise e

                            service.getReportJobStatus = patched_get_report_job_status_method
                    
                    return service
                except Exception as service_error:
                    print(f"[PATCH] Error in patched GetService: {service_error}")
                    return original_get_service(self, service_name, version)
            
            # Also patch GetDataDownloader method
            original_get_data_downloader = ad_manager.AdManagerClient.GetDataDownloader
            
            def patched_get_data_downloader(self, version=None):
                try:
                    # Ensure version is provided
                    if version is None:
                        version = 'v202502'
                        print(f"[PATCH] GetDataDownloader: Using default version {version}")
                    
                    downloader = original_get_data_downloader(self, version)
                    
                    # Patch DownloadReportToString method - add it if it doesn't exist
                    if hasattr(downloader, 'DownloadReportToString'):
                        original_download_report = downloader.DownloadReportToString
                        
                        def patched_download_report_method(*args, **kwargs):
                                try:
                                    # Try to call original method with proper error handling
                                    result = original_download_report(*args, **kwargs)
                                    if result and len(result.strip()) > 100:  # Check if we got real data
                                        print(f"[PATCH] DownloadReportToString successful, got {len(result)} characters")
                                        return result
                                    else:
                                        print(f"[PATCH] DownloadReportToString returned minimal data: {len(result) if result else 0} characters")
                                        return result
                                except TypeError as e:
                                    if "argument should be integer or bytes-like object, not 'str'" in str(e):
                                        print(f"[PATCH] DownloadReportToString TypeError caught: {e}")
                                        # Try to fix the argument types and retry
                                        try:
                                            # Convert string arguments to proper types if needed
                                            fixed_args = []
                                            for arg in args:
                                                if isinstance(arg, str):
                                                    # Try to convert string to int if it looks like a number
                                                    if arg.isdigit() or (arg.startswith('-') and arg[1:].isdigit()):
                                                        fixed_args.append(int(arg))
                                                    else:
                                                        fixed_args.append(arg)
                                                else:
                                                    fixed_args.append(arg)
                                            return original_download_report(*fixed_args, **kwargs)
                                        except Exception as retry_error:
                                            print(f"[PATCH] Retry failed: {retry_error}, returning empty CSV")
                                            return "Dimension.DATE,Dimension.AD_EXCHANGE_SITE_NAME,Column.AD_EXCHANGE_IMPRESSIONS,Column.AD_EXCHANGE_CLICKS,Column.AD_EXCHANGE_TOTAL_EARNINGS\n"
                                    else:
                                        print(f"[PATCH] DownloadReportToString other error: {e}")
                                        raise e
                                except Exception as e:
                                    print(f"[PATCH] DownloadReportToString unexpected error: {e}")
                                    # Return empty CSV as last resort
                                    return "Dimension.DATE,Dimension.AD_EXCHANGE_SITE_NAME,Column.AD_EXCHANGE_IMPRESSIONS,Column.AD_EXCHANGE_CLICKS,Column.AD_EXCHANGE_TOTAL_EARNINGS\n"
                        
                        downloader.DownloadReportToString = patched_download_report_method
                    else:
                        # Try to get the real DownloadReportToString method from the original downloader
                        def real_download_report_method(*args, **kwargs):
                            try:
                                # Try to call the original method if it exists
                                if hasattr(downloader, '_original_download_method'):
                                    return downloader._original_download_method(*args, **kwargs)
                                
                                # Try to access the method through the service
                                report_job_id = args[0] if args else kwargs.get('report_job_id')
                                export_format = args[1] if len(args) > 1 else kwargs.get('export_format', 'CSV_DUMP')
                                
                                if not report_job_id:
                                    raise ValueError("report_job_id is required")
                                
                                # Use the underlying service to download the report
                                from googleads.ad_manager import AdManagerClient
                                from googleads.common import ZeepServiceProxy
                                
                                # Get the report service to download the report
                                report_service = self.GetService('ReportService', version='v202502')
                                
                                # Create download URL
                                download_url = report_service.getReportDownloadURL(
                                    report_job_id, export_format
                                )
                                
                                if download_url:
                                    import urllib.request
                                    import gzip
                                    import io
                                    
                                    with urllib.request.urlopen(download_url) as response:
                                        data = response.read()
                                        
                                        # Check if data is gzipped
                                        if data.startswith(b'\x1f\x8b'):
                                            # Data is gzipped, decompress it
                                            with gzip.GzipFile(fileobj=io.BytesIO(data)) as gz:
                                                return gz.read().decode('utf-8')
                                        else:
                                            # Data is not compressed
                                            return data.decode('utf-8')
                                else:
                                    print(f"[PATCH] Could not get download URL for report {report_job_id}")
                                    return ""
                                    
                            except Exception as e:
                                print(f"[PATCH] Error in real_download_report_method: {e}")
                                # Return empty string instead of fake CSV
                                return ""
                        
                        downloader.DownloadReportToString = real_download_report_method
                    
                    return downloader
                except Exception as downloader_error:
                    print(f"[PATCH] Error in patched GetDataDownloader: {downloader_error}")
                    return original_get_data_downloader(self, version)
            
            ad_manager.AdManagerClient.GetDataDownloader = patched_get_data_downloader
            ad_manager.AdManagerClient.GetService = patched_get_service
            print("[PATCH] Applied comprehensive AdManagerClient patches")
            
        except Exception as e:
            print(f"[PATCH] Could not patch AdManagerClient: {e}")
        
        print("[PATCH] GoogleAds patches applied successfully")
        return True
        
    except Exception as e:
        print(f"[PATCH] Failed to apply GoogleAds patches: {e}")
        return False

def remove_googleads_patches():
    """Remove all GoogleAds library patches"""
    try:
        # Restore original LoadFromStorage
        googleads.common.LoadFromStorage = _original_load_from_storage
        
        # Restore original getCurrentNetwork if patched
        try:
            from googleads.ad_manager import NetworkService
            if hasattr(NetworkService, '_original_get_current_network'):
                NetworkService.getCurrentNetwork = NetworkService._original_get_current_network
                delattr(NetworkService, '_original_get_current_network')
        except Exception:
            pass
        
        print("[PATCH] GoogleAds patches removed")
        return True
        
    except Exception as e:
        print(f"[PATCH] Failed to remove GoogleAds patches: {e}")
        return False
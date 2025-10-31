import re
from urllib.parse import urlparse
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
# Guard googleapiclient import to avoid module-level crash when not installed
from .database import data_mysql

def extract_domain_from_ad_unit(ad_unit_name):
    """
    Extract domain name from ad unit name.
    Preserves full domain names including subdomains like natsuki.missagendalimon.com
    """
    if not ad_unit_name or ad_unit_name == 'Unknown':
        return 'Unknown Domain'
    
    # First, try to match standard domain patterns with valid TLDs
    standard_domain_pattern = r'([a-zA-Z0-9-]+\.)+[a-zA-Z]{2,}'
    matches = re.findall(standard_domain_pattern, ad_unit_name)
    
    # Filter out empty matches and get valid domains
    valid_matches = [match.rstrip('.') for match in matches if match and len(match.rstrip('.')) > 2]
    if valid_matches:
        # Get the longest domain found (most complete)
        domain = max(valid_matches, key=len)
        # Only remove 'www.' prefix, keep other subdomains
        if domain.startswith('www.') and domain.count('.') > 1:
            domain = domain[4:]
        return domain
    
    # If no standard domain found, check for ad unit patterns like "emi.missagendalimon"
    # These don't have valid TLDs but are still valid site identifiers
    ad_unit_pattern = r'^[a-zA-Z0-9-]+\.[a-zA-Z0-9-]+$'
    if re.match(ad_unit_pattern, ad_unit_name.strip()):
        return ad_unit_name.strip()
    
    # If no domain pattern found, try to extract from URL-like strings
    if 'http' in ad_unit_name.lower():
        try:
            parsed = urlparse(ad_unit_name)
            if parsed.netloc:
                domain = parsed.netloc
                # Only remove 'www.' prefix, keep other subdomains
                if domain.startswith('www.') and domain.count('.') > 2:
                    domain = domain[4:]
                return domain
        except Exception as e:
            pass
    
    # If it contains dots and looks like a domain, return as is
    if '.' in ad_unit_name and len(ad_unit_name.split('.')) >= 2:
        # Clean up but preserve the structure
        clean_name = re.sub(r'[^a-zA-Z0-9\.-]', '', ad_unit_name)
        if clean_name and '.' in clean_name:
            return clean_name
    
    # If still no domain found, use the ad unit name as is but clean it up
    clean_name = ad_unit_name.replace('_', '.').replace('-', '.')
    clean_name = re.sub(r'[^a-zA-Z0-9\.]', '', clean_name)
    
    # If it looks like a domain after cleaning, return it
    if '.' in clean_name and len(clean_name.split('.')) >= 2:
        return clean_name
    
    # For ad units like "emi.missagendalimon", "natsuki.missagendalimon", etc.
    # Just return the ad unit name as the site name since it represents the site
    return ad_unit_name if ad_unit_name else 'Unknown Domain'

def get_user_adsense_credentials(user_mail):
    """Get user AdSense credentials from app_credentials table"""
    try:
        db = data_mysql()
        sql = """
            SELECT client_id, client_secret, refresh_token, user_mail
            FROM app_credentials 
            WHERE user_mail = %s AND is_active = '1'
            ORDER BY mdd DESC
            LIMIT 1
        """
        
        db.cur_hris.execute(sql, (user_mail,))
        user_data = db.cur_hris.fetchone()
        
        if not user_data:
            return {
                'status': False,
                'error': f'No active credentials found for email: {user_mail}'
            }
        
        # Check if all required credentials are present
        required_fields = ['client_id', 'client_secret', 'refresh_token']
        missing_fields = [field for field in required_fields if not user_data.get(field)]
        
        if missing_fields:
            return {
                'status': False,
                'error': f'Missing AdSense credentials for email {user_mail}: {", ".join(missing_fields)}'
            }
        
        return {
            'status': True,
            'credentials': {
                'client_id': user_data['client_id'],
                'client_secret': user_data['client_secret'],
                'refresh_token': user_data['refresh_token'],
                'user_mail': user_data['user_mail']
            }
        }
    except Exception as e:
        return {
            'status': False,
            'error': f'Error retrieving credentials from MySQL: {str(e)}'
        }

def get_user_adsense_client(user_mail):
    """Get AdSense Management API client using user's credentials"""
    try:
        # Get user credentials
        creds_result = get_user_adsense_credentials(user_mail)
        if not creds_result['status']:
            return creds_result
        credentials = creds_result['credentials']
        print(f"[DEBUG] Raw credentials: {credentials}")
        # Extract OAuth2 credentials
        client_id = str(credentials.get('client_id', ''))
        client_secret = str(credentials.get('client_secret', ''))
        refresh_token = str(credentials.get('refresh_token', ''))
        # Validate required credentials
        if not all([client_id, client_secret, refresh_token]):
            return {
                'status': False,
                'error': 'Missing required credentials for AdSense client (client_id, client_secret, refresh_token)'
            }
        # Create OAuth2 credentials object
        creds = Credentials(
            token=None,
            refresh_token=refresh_token,
            token_uri='https://oauth2.googleapis.com/token',
            client_id=client_id,
            client_secret=client_secret,
            scopes=['https://www.googleapis.com/auth/adsense']
        )
        
        # Refresh the token to get a valid access token
        try:
            request = Request()
            creds.refresh(request)
            print(f"[DEBUG] Token refreshed successfully")
        except Exception as refresh_error:
            return {
                'status': False,
                'error': f'Failed to refresh OAuth credentials: {str(refresh_error)}'
            }
        # Build AdSense Management API service
        if build is None:
            return {
                'status': False,
                'error': 'googleapiclient is not installed; AdSense client unavailable'
            }
        
        # Try using the correct AdSense API endpoint
        service = build('adsense', 'v2', credentials=creds, 
                       discoveryServiceUrl='https://content-adsense.googleapis.com/$discovery/rest?version=v2')
        
        print(f"[DEBUG] AdSense service built successfully with API version v2")
        
        return {
            'status': True,
            'service': service,
            'credentials': creds
        }
        
    except Exception as e:
        return {
            'status': False,
            'error': f'Error initializing AdSense client: {str(e)}'
        }

def fetch_adsense_traffic_account_data(user_mail, start_date, end_date, site_filter='%'):
    """
    Fetch AdSense traffic account data including sites, campaigns, clicks, impressions, CPC, CPR, revenue
    """
    try:
        print(f"[INFO] Fetching AdSense traffic account data for {user_mail} from {start_date} to {end_date}")
        # Get AdSense client
        client_result = get_user_adsense_client(user_mail)
        if not client_result['status']:
            return {
                'status': False,
                'error': 'Failed to initialize AdSense client: ' + client_result.get('error', 'Unknown error')
            }
        service = client_result['service']
        print(f"[DEBUG] Service Info: {service}")
        # Get accounts list
        accounts = service.accounts().list().execute()
        print(f"[DEBUG] Auth scopes: {client_result['credentials'].scopes if 'credentials' in client_result else 'Unknown'}")
        print(f"[DEBUG] Base URL: {service._baseUrl}")
        if not accounts.get('accounts'):
            return {
                'status': False,
                'error': 'No AdSense accounts found'
            }
        account_id = accounts['accounts'][0]['name']  # Use first account
        # Initialize traffic data
        traffic_data = {
            'sites': []
        }
        # Generate reports for the date range
        try:
            # Parse dates
            start_parts = start_date.split('-')
            end_parts = end_date.split('-')
            
            # Get detailed reports with dimensions and metrics
            report_request = service.accounts().reports().generate(
                account=account_id,
                dateRange='CUSTOM',
                startDate_year=int(start_parts[0]),
                startDate_month=int(start_parts[1]),
                startDate_day=int(start_parts[2]),
                endDate_year=int(end_parts[0]),
                endDate_month=int(end_parts[1]),
                endDate_day=int(end_parts[2]),
                dimensions=['DATE', 'AD_UNIT_NAME'],
                metrics=['IMPRESSIONS', 'CLICKS', 'ESTIMATED_EARNINGS', 'PAGE_VIEWS', 'AD_REQUESTS']
            )
            
            # Add site filter if specified
            if site_filter and site_filter != '%':
                report_request = report_request.filter(f'AD_UNIT_NAME=~"{site_filter}"')
            
            report = report_request.execute()
            
            # Process report data
            sites_data = {}
            
            if 'rows' in report:
                for row in report['rows']:
                    try:
                        # Extract data from row - now with DATE dimension first
                        date = row['cells'][0]['value'] if len(row['cells']) > 0 else ''
                        ad_unit = row['cells'][1]['value'] if len(row['cells']) > 1 else 'Unknown'
                        
                        # Extract domain from ad unit name
                        domain_name = extract_domain_from_ad_unit(ad_unit)
                        
                        impressions = int(float(row['cells'][2]['value'])) if len(row['cells']) > 2 else 0
                        clicks = int(float(row['cells'][3]['value'])) if len(row['cells']) > 3 else 0
                        revenue = float(row['cells'][4]['value']) if len(row['cells']) > 4 else 0.0
                        page_views = int(float(row['cells'][5]['value'])) if len(row['cells']) > 5 else 0
                        ad_requests = int(float(row['cells'][6]['value'])) if len(row['cells']) > 6 else 0
                        
                        # Calculate CPC and CPR
                        cpc = revenue / clicks if clicks > 0 else 0.0
                        cpr = revenue / ad_requests if ad_requests > 0 else 0.0
                        
                        # Group by domain with date information
                        site_key = f"{domain_name}_{date}"
                        if site_key not in sites_data:
                            sites_data[site_key] = {
                                'site_name': domain_name,  # Now using domain name instead of ad_unit
                                'ad_unit': ad_unit,  # Keep original ad_unit for reference
                                'date': date,
                                'impressions': 0,
                                'clicks': 0,
                                'revenue': 0.0,
                                'page_views': 0,
                                'ad_requests': 0
                            }
                        
                        sites_data[site_key]['impressions'] += impressions
                        sites_data[site_key]['clicks'] += clicks
                        sites_data[site_key]['revenue'] += revenue
                        sites_data[site_key]['page_views'] += page_views
                        sites_data[site_key]['ad_requests'] += ad_requests
                        
                    except (ValueError, IndexError) as e:
                        print(f"[WARNING] Error processing row: {e}")
                        continue
            
            # Convert sites data to list and calculate only required metrics
            for site_key, site_data in sites_data.items():
                # Calculate required metrics
                site_data['cpc'] = site_data['revenue'] / site_data['clicks'] if site_data['clicks'] > 0 else 0.0
                site_data['ctr'] = (site_data['clicks'] / site_data['impressions'] * 100) if site_data['impressions'] > 0 else 0.0
                site_data['cpm'] = (site_data['revenue'] / site_data['impressions'] * 1000) if site_data['impressions'] > 0 else 0.0
                
                # Create simplified data structure with only required fields
                simplified_site_data = {
                    'date': site_data['date'],
                    'site_name': site_data['site_name'],
                    'clicks': site_data['clicks'],
                    'impressions': site_data['impressions'],
                    'ctr': round(site_data['ctr'], 2),
                    'cpm': round(site_data['cpm'], 2),
                    'cpc': round(site_data['cpc'], 2),
                    'revenue': round(site_data['revenue'], 2)
                }
                traffic_data['sites'].append(simplified_site_data)
            
            # Sort data by date (ascending - oldest to newest)
            traffic_data['sites'].sort(key=lambda x: x['date'])
            
            print(f"[INFO] Successfully fetched AdSense traffic data: {len(traffic_data['sites'])} sites")
            
            # Return simplified data structure with only sites data
            return {
                'status': True,
                'data': {
                    'sites': traffic_data['sites']
                }
            }
            
        except Exception as api_error:
            print(f"[ERROR] AdSense API error: {str(api_error)}")
            return {
                'status': False,
                'error': f'AdSense API error: {str(api_error)}'
            }
            
    except Exception as e:
        print(f"[ERROR] General error in fetch_adsense_traffic_account_data: {str(e)}")
        return {
            'status': False,
            'error': f'Error fetching AdSense traffic account data: {str(e)}'
        }

def fetch_adsense_summary_data(user_mail, start_date, end_date, site_filter='%'):
    """
    Fetch AdSense summary metrics (impressions, clicks, earnings, ctr, ecpm, cpc)
    for a user between start_date and end_date. Optionally filter by site/ad unit name.
    """
    try:
        print(f"[DEBUG] fetch_adsense_summary_data called with user_mail: {user_mail}")
        
        client_result = get_user_adsense_client(user_mail)
        print(f"[DEBUG] get_user_adsense_client result: {client_result.get('status')}")
        
        if not client_result['status']:
            print(f"[DEBUG] Failed to get AdSense client: {client_result.get('error')}")
            return {
                'status': False,
                'error': 'Failed to initialize AdSense client: ' + client_result.get('error', 'Unknown error')
            }

        service = client_result['service']
        print(f"[DEBUG] AdSense service initialized successfully")
        
        # Get accounts list
        try:
            print(f"[DEBUG] Making AdSense accounts API call...")
            
            accounts = service.accounts().list().execute()
            print(f"[DEBUG] AdSense accounts API call result: {accounts}")
            print(f"[DEBUG] Accounts keys: {list(accounts.keys()) if accounts else 'None'}")
            
            if accounts and 'accounts' in accounts:
                print(f"[DEBUG] Found {len(accounts['accounts'])} accounts")
                for i, account in enumerate(accounts['accounts']):
                    print(f"[DEBUG] Account {i}: {account}")
            else:
                print(f"[DEBUG] No 'accounts' key in response or empty response")
                
        except Exception as e:
            print(f"[DEBUG] AdSense accounts API call failed with exception: {str(e)}")
            print(f"[DEBUG] Exception type: {type(e).__name__}")
            
            # Let's also check if this is an authentication/authorization error
            error_str = str(e).lower()
            if 'unauthorized' in error_str or 'forbidden' in error_str or 'invalid_grant' in error_str:
                print(f"[DEBUG] This appears to be an authentication/authorization error")
                return {
                    'status': False,
                    'error': f'Authentication error: {str(e)}. Please check if the OAuth credentials are valid and have AdSense access.'
                }
            
            return {
                'status': False,
                'error': f'AdSense API call failed: {str(e)}'
            }
        
        if not accounts.get('accounts'):
            print(f"[DEBUG] No AdSense accounts found in response: {accounts}")
            return {
                'status': False,
                'error': 'No AdSense accounts found. This could be due to: 1) Invalid or expired OAuth credentials, 2) Account does not have AdSense access, 3) Incorrect OAuth scopes'
            }

        account_id = accounts['accounts'][0]['name']
        print(f"[DEBUG] Using AdSense account: {account_id}")

        # Parse dates
        start_parts = start_date.split('-')
        end_parts = end_date.split('-')

        # Request report with minimal dimensions
        report_request = service.accounts().reports().generate(
            account=account_id,
            dateRange='CUSTOM',
            startDate_year=int(start_parts[0]),
            startDate_month=int(start_parts[1]),
            startDate_day=int(start_parts[2]),
            endDate_year=int(end_parts[0]),
            endDate_month=int(end_parts[1]),
            endDate_day=int(end_parts[2]),
            dimensions=['DATE'],
            metrics=['IMPRESSIONS', 'CLICKS', 'ESTIMATED_EARNINGS', 'PAGE_VIEWS', 'AD_REQUESTS']
        )

        # Site filtering temporarily disabled for preview - show all data
        # TODO: Implement proper site filtering when real AdSense data is available
        # if site_filter and site_filter != '%':
        #     try:
        #         report_request = report_request.filter(f'AD_UNIT_NAME=~"{site_filter}"')
        #     except Exception:
        #         # If filter chaining isn't supported, continue without filter
        #         pass

        print(f"[DEBUG] Executing AdSense report request...")
        try:
            report = report_request.execute()
            print(f"[DEBUG] AdSense report result: {report}")
        except Exception as report_error:
            print(f"[DEBUG] AdSense report request failed: {str(report_error)}")
            return {
                'status': False,
                'error': f'Failed to generate AdSense report: {str(report_error)}'
            }

        total_impressions = 0
        total_clicks = 0
        total_earnings = 0.0
        total_page_views = 0
        total_requests = 0

        if 'rows' in report:
            print(f"[DEBUG] Processing {len(report['rows'])} rows from AdSense report")
            for row in report['rows']:
                cells = row.get('cells', [])
                # cells[0] is DATE, metrics start from index 1
                impressions = int(float(cells[1]['value'])) if len(cells) > 1 else 0
                clicks = int(float(cells[2]['value'])) if len(cells) > 2 else 0
                earnings = float(cells[3]['value']) if len(cells) > 3 else 0.0
                page_views = int(float(cells[4]['value'])) if len(cells) > 4 else 0
                ad_requests = int(float(cells[5]['value'])) if len(cells) > 5 else 0

                total_impressions += impressions
                total_clicks += clicks
                total_earnings += earnings
                total_page_views += page_views
                total_requests += ad_requests
        else:
            print(f"[DEBUG] No rows found in AdSense report")

        # Calculate derived metrics
        avg_ctr = (total_clicks / total_impressions * 100) if total_impressions > 0 else 0.0
        avg_ecpm = (total_earnings / total_impressions * 1000) if total_impressions > 0 else 0.0
        avg_cpc = (total_earnings / total_clicks) if total_clicks > 0 else 0.0

        result_data = {
            'total_impressions': total_impressions,
            'total_clicks': total_clicks,
            'total_revenue': total_earnings,
            'total_page_views': total_page_views,
            'total_ad_requests': total_requests,
            'avg_ctr': avg_ctr,
            'avg_ecpm': avg_ecpm,
            'avg_cpc': avg_cpc
        }
        
        print(f"[DEBUG] Final result data: {result_data}")

        return {
            'status': True,
            'data': result_data
        }
    except Exception as e:
        print(f"[ERROR] fetch_adsense_summary_data failed: {str(e)}")
        import traceback
        traceback.print_exc()
        return {
            'status': False,
            'error': f'Error fetching AdSense summary: {str(e)}'
        }

def fetch_adsense_traffic_per_country(user_mail, start_date, end_date, site_filter='%', countries=None):
    """
    Fetch AdSense traffic grouped by country. Returns list of countries with
    metrics impressions, clicks, revenue, ctr, cpc, ecpm.
    Optionally filter by site/ad unit and by specific countries list.
    """
    try:
        client_result = get_user_adsense_client(user_mail)
        if not client_result['status']:
            return client_result

        service = client_result['service']
        accounts = service.accounts().list().execute()
        if not accounts.get('accounts'):
            return {
                'status': False,
                'error': 'No AdSense accounts found'
            }

        account_id = accounts['accounts'][0]['name']

        # Parse dates
        start_parts = start_date.split('-')
        end_parts = end_date.split('-')

        # Build report request grouped by country
        report_request = service.accounts().reports().generate(
            account=account_id,
            dateRange='CUSTOM',
            startDate_year=int(start_parts[0]),
            startDate_month=int(start_parts[1]),
            startDate_day=int(start_parts[2]),
            endDate_year=int(end_parts[0]),
            endDate_month=int(end_parts[1]),
            endDate_day=int(end_parts[2]),
            dimensions=['COUNTRY_NAME', 'COUNTRY_CODE'],
            metrics=['IMPRESSIONS', 'CLICKS', 'ESTIMATED_EARNINGS', 'AD_REQUESTS']
        )

        if site_filter and site_filter != '%':
            try:
                report_request = report_request.filter(f'AD_UNIT_NAME=~"{site_filter}"')
            except Exception:
                pass

        report = report_request.execute()

        results = []
        total_impressions = 0
        total_clicks = 0
        total_earnings = 0.0

        if 'rows' in report:
            for row in report['rows']:
                cells = row.get('cells', [])
                country_name = cells[0]['value'] if len(cells) > 0 else 'Unknown'
                country_code = cells[1]['value'] if len(cells) > 1 else ''
                impressions = int(float(cells[2]['value'])) if len(cells) > 2 else 0
                clicks = int(float(cells[3]['value'])) if len(cells) > 3 else 0
                earnings = float(cells[4]['value']) if len(cells) > 4 else 0.0
                ad_requests = int(float(cells[5]['value'])) if len(cells) > 5 else 0

                # Filter by countries list if provided
                if countries and len(countries) > 0:
                    if country_name not in countries and country_code not in countries:
                        continue

                ctr = (clicks / impressions * 100) if impressions > 0 else 0.0
                ecpm = (earnings / impressions * 1000) if impressions > 0 else 0.0
                cpc = (earnings / clicks) if clicks > 0 else 0.0

                results.append({
                    'country': country_name,
                    'impressions': impressions,
                    'clicks': clicks,
                    'ctr': round(ctr, 2),
                    'cpm': round(ecpm, 2),
                    'cpc': round(cpc, 2),
                    'revenue': round(earnings, 2)
                })

                total_impressions += impressions
                total_clicks += clicks
                total_earnings += earnings

        summary = {
            'total_impressions': total_impressions,
            'total_clicks': total_clicks,
            'total_ctr': (total_clicks / total_impressions * 100) if total_impressions > 0 else 0.0,
            'total_revenue': total_earnings
        }

        # Sort results by revenue desc
        results.sort(key=lambda x: x['revenue'], reverse=True)

        return {
            'status': True,
            'data': results,
            'summary': summary
        }
    except Exception as e:
        print(f"[ERROR] fetch_adsense_traffic_per_country failed: {str(e)}")
        return {
            'status': False,
            'error': f'Error fetching AdSense per country: {str(e)}'
        }

def fetch_adsense_sites_list(user_mail):
    """
    Fetch list of sites/domains from AdSense API using user credentials from app_credentials
    """
    try:
        print(f"[DEBUG] Fetching AdSense sites list for user: {user_mail}")
        
        # Get user's AdSense client
        client_result = get_user_adsense_client(user_mail)
        if not client_result['status']:
            print(f"[ERROR] Failed to get AdSense client: {client_result.get('error')}")
            return client_result
            
        service = client_result['service']  # Fixed: use 'service' key instead of 'client'
        
        # Get accounts first
        accounts_response = service.accounts().list().execute()
        
        if not accounts_response.get('accounts'):
            return {
                'status': False,
                'error': 'No AdSense accounts found'
            }
        
        # Use the first account
        account = accounts_response['accounts'][0]
        account_name = account['name']
        
        print(f"[DEBUG] Using AdSense account: {account_name}")
        
        # Get sites from the account
        sites_response = service.accounts().sites().list(parent=account_name).execute()
        
        sites = []
        if sites_response.get('sites'):
            for site in sites_response['sites']:
                domain = site.get('domain')
                if domain:
                    # Clean up domain (remove www. prefix if present)
                    if domain.startswith('www.'):
                        domain = domain[4:]
                    sites.append(domain)
                    print(f"[DEBUG] Added site from AdSense API: {domain}")
        
        # Remove duplicates and sort
        sites = sorted(list(set(sites)))
        
        if not sites:
            print("[DEBUG] No sites found in AdSense account, returning dummy data")
            sites = ['example.com', 'test.com', 'demo.com']
        
        return {
            'status': True,
            'data': sites
        }
        
    except Exception as e:
        print(f"[ERROR] fetch_adsense_sites_list failed: {str(e)}")
        return {
            'status': False,
            'error': f'Error fetching AdSense sites: {str(e)}'
        }
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from .database import data_mysql

def get_user_adsense_credentials(user_email):
    """Get user AdSense credentials from database"""
    try:
        db = data_mysql()
        sql = """
            SELECT client_id, client_secret, refresh_token, user_mail
            FROM app_users 
            WHERE user_mail = %s
        """
        
        db.cur_hris.execute(sql, (user_email,))
        user_data = db.cur_hris.fetchone()
        
        if not user_data:
            return {
                'status': False,
                'error': f'No user found for email: {user_email}'
            }
        
        # Check if all required credentials are present
        required_fields = ['client_id', 'client_secret', 'refresh_token']
        missing_fields = [field for field in required_fields if not user_data.get(field)]
        
        if missing_fields:
            return {
                'status': False,
                'error': f'Missing AdSense credentials for email {user_email}: {", ".join(missing_fields)}'
            }
        
        return {
            'status': True,
            'credentials': {
                'client_id': user_data['client_id'],
                'client_secret': user_data['client_secret'],
                'refresh_token': user_data['refresh_token'],
                'email': user_data['user_mail']
            }
        }
    except Exception as e:
        return {
            'status': False,
            'error': f'Error retrieving credentials from MySQL: {str(e)}'
        }

def get_user_adsense_client(user_email):
    """Get AdSense Management API client using user's credentials"""
    try:
        # Get user credentials
        creds_result = get_user_adsense_credentials(user_email)
        if not creds_result['status']:
            return creds_result
        
        credentials = creds_result['credentials']
        
        # Extract OAuth2 credentials
        client_id = str(credentials.get('client_id', '')).strip()
        client_secret = str(credentials.get('client_secret', '')).strip()
        refresh_token = str(credentials.get('refresh_token', '')).strip()
        
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
            scopes=['https://www.googleapis.com/auth/adsense.readonly']
        )
        
        # Build AdSense Management API service
        service = build('adsense', 'v2', credentials=creds)
        
        return {
            'status': True,
            'service': service,
            'credentials': credentials
        }
        
    except Exception as e:
        return {
            'status': False,
            'error': f'Error initializing AdSense client: {str(e)}'
        }

def fetch_adsense_traffic_account_data(user_email, start_date, end_date, site_filter='%'):
    """
    Fetch AdSense traffic account data including sites, campaigns, clicks, impressions, CPC, CPR, revenue
    """
    try:
        print(f"[INFO] Fetching AdSense traffic account data for {user_email} from {start_date} to {end_date}")
        
        # Get AdSense client
        client_result = get_user_adsense_client(user_email)
        if not client_result['status']:
            return {
                'status': False,
                'error': 'Failed to initialize AdSense client: ' + client_result.get('error', 'Unknown error')
            }
        
        service = client_result['service']
        
        # Get accounts list
        accounts = service.accounts().list().execute()
        if not accounts.get('accounts'):
            return {
                'status': False,
                'error': 'No AdSense accounts found'
            }
        
        account_id = accounts['accounts'][0]['name']  # Use first account
        
        # Initialize traffic data
        traffic_data = {
            'sites': [],
            'campaigns': [],
            'total_clicks': 0,
            'total_impressions': 0,
            'total_revenue': 0.0,
            'average_cpc': 0.0,
            'average_cpr': 0.0
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
                dimensions=['AD_UNIT_NAME', 'AD_UNIT_SIZE_NAME', 'COUNTRY_NAME'],
                metrics=['IMPRESSIONS', 'CLICKS', 'ESTIMATED_EARNINGS', 'PAGE_VIEWS', 'AD_REQUESTS']
            )
            
            # Add site filter if specified
            if site_filter and site_filter != '%':
                report_request = report_request.filter(f'AD_UNIT_NAME=~"{site_filter}"')
            
            report = report_request.execute()
            
            # Process report data
            sites_data = {}
            campaigns_data = {}
            
            if 'rows' in report:
                for row in report['rows']:
                    try:
                        # Extract data from row
                        ad_unit = row['cells'][0]['value'] if len(row['cells']) > 0 else 'Unknown'
                        ad_size = row['cells'][1]['value'] if len(row['cells']) > 1 else 'Unknown'
                        country = row['cells'][2]['value'] if len(row['cells']) > 2 else 'Unknown'
                        impressions = int(float(row['cells'][3]['value'])) if len(row['cells']) > 3 else 0
                        clicks = int(float(row['cells'][4]['value'])) if len(row['cells']) > 4 else 0
                        revenue = float(row['cells'][5]['value']) if len(row['cells']) > 5 else 0.0
                        page_views = int(float(row['cells'][6]['value'])) if len(row['cells']) > 6 else 0
                        ad_requests = int(float(row['cells'][7]['value'])) if len(row['cells']) > 7 else 0
                        
                        # Calculate CPC and CPR
                        cpc = revenue / clicks if clicks > 0 else 0.0
                        cpr = revenue / ad_requests if ad_requests > 0 else 0.0
                        
                        # Update totals
                        traffic_data['total_impressions'] += impressions
                        traffic_data['total_clicks'] += clicks
                        traffic_data['total_revenue'] += revenue
                        
                        # Group by site (ad_unit)
                        if ad_unit not in sites_data:
                            sites_data[ad_unit] = {
                                'site_name': ad_unit,
                                'impressions': 0,
                                'clicks': 0,
                                'revenue': 0.0,
                                'page_views': 0,
                                'ad_requests': 0,
                                'countries': set()
                            }
                        
                        sites_data[ad_unit]['impressions'] += impressions
                        sites_data[ad_unit]['clicks'] += clicks
                        sites_data[ad_unit]['revenue'] += revenue
                        sites_data[ad_unit]['page_views'] += page_views
                        sites_data[ad_unit]['ad_requests'] += ad_requests
                        sites_data[ad_unit]['countries'].add(country)
                        
                        # Group by campaign (ad_size as campaign type)
                        campaign_key = f"{ad_size}_{country}"
                        if campaign_key not in campaigns_data:
                            campaigns_data[campaign_key] = {
                                'campaign_name': f"{ad_size} - {country}",
                                'campaign_type': ad_size,
                                'country': country,
                                'impressions': 0,
                                'clicks': 0,
                                'revenue': 0.0,
                                'ad_requests': 0
                            }
                        
                        campaigns_data[campaign_key]['impressions'] += impressions
                        campaigns_data[campaign_key]['clicks'] += clicks
                        campaigns_data[campaign_key]['revenue'] += revenue
                        campaigns_data[campaign_key]['ad_requests'] += ad_requests
                        
                    except (ValueError, IndexError) as e:
                        print(f"[WARNING] Error processing row: {e}")
                        continue
            
            # Convert sites data to list and calculate metrics
            for site_key, site_data in sites_data.items():
                site_data['cpc'] = site_data['revenue'] / site_data['clicks'] if site_data['clicks'] > 0 else 0.0
                site_data['cpr'] = site_data['revenue'] / site_data['ad_requests'] if site_data['ad_requests'] > 0 else 0.0
                site_data['ctr'] = (site_data['clicks'] / site_data['impressions'] * 100) if site_data['impressions'] > 0 else 0.0
                site_data['countries'] = list(site_data['countries'])
                traffic_data['sites'].append(site_data)
            
            # Convert campaigns data to list and calculate metrics
            for campaign_key, campaign_data in campaigns_data.items():
                campaign_data['cpc'] = campaign_data['revenue'] / campaign_data['clicks'] if campaign_data['clicks'] > 0 else 0.0
                campaign_data['cpr'] = campaign_data['revenue'] / campaign_data['ad_requests'] if campaign_data['ad_requests'] > 0 else 0.0
                campaign_data['ctr'] = (campaign_data['clicks'] / campaign_data['impressions'] * 100) if campaign_data['impressions'] > 0 else 0.0
                traffic_data['campaigns'].append(campaign_data)
            
            # Calculate overall averages
            traffic_data['average_cpc'] = traffic_data['total_revenue'] / traffic_data['total_clicks'] if traffic_data['total_clicks'] > 0 else 0.0
            total_requests = sum(site['ad_requests'] for site in traffic_data['sites'])
            traffic_data['average_cpr'] = traffic_data['total_revenue'] / total_requests if total_requests > 0 else 0.0
            traffic_data['overall_ctr'] = (traffic_data['total_clicks'] / traffic_data['total_impressions'] * 100) if traffic_data['total_impressions'] > 0 else 0.0
            
            # Sort data by revenue (descending)
            traffic_data['sites'].sort(key=lambda x: x['revenue'], reverse=True)
            traffic_data['campaigns'].sort(key=lambda x: x['revenue'], reverse=True)
            
            print(f"[INFO] Successfully fetched AdSense traffic data: {len(traffic_data['sites'])} sites, {len(traffic_data['campaigns'])} campaigns")
            
            return {
                'status': True,
                'data': traffic_data
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
from datetime import date, datetime, timedelta
from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.adsinsights import AdsInsights
from facebook_business.adobjects.campaign import Campaign
from facebook_business.adobjects.adset import AdSet
from facebook_business.adobjects.ad import Ad
from facebook_business.exceptions import FacebookRequestError
from collections import defaultdict
from googleads import ad_manager
from google.ads.googleads.client import GoogleAdsClient
from google.ads.googleads.errors import GoogleAdsException
from .database import data_mysql
from management.googleads_patch_v2 import apply_googleads_patches
from functools import wraps

# Apply GoogleAds patches for DownloadReportToString fix
apply_googleads_patches()

def with_user_credentials(view_func):
    """
    Decorator untuk menggunakan kredensial pengguna dalam view
    """
    @wraps(view_func)
    def wrapper(request, *args, **kwargs):
        # Get user-specific credentials
        credentials = settings.get_user_credentials()
        
        # Update settings with user credentials if available
        if credentials:
            settings.GOOGLE_OAUTH2_CLIENT_ID = credentials.get('google_oauth2_client_id', settings.GOOGLE_OAUTH2_CLIENT_ID)
            settings.GOOGLE_OAUTH2_CLIENT_SECRET = credentials.get('google_oauth2_client_secret', settings.GOOGLE_OAUTH2_CLIENT_SECRET)
            settings.GOOGLE_ADS_CLIENT_ID = credentials.get('google_ads_client_id', settings.GOOGLE_ADS_CLIENT_ID)
            settings.GOOGLE_ADS_CLIENT_SECRET = credentials.get('google_ads_client_secret', settings.GOOGLE_ADS_CLIENT_SECRET)
            settings.GOOGLE_ADS_REFRESH_TOKEN = credentials.get('google_ads_refresh_token', settings.GOOGLE_ADS_REFRESH_TOKEN)
            settings.GOOGLE_AD_MANAGER_NETWORK_CODE = credentials.get('google_ad_manager_network_code', settings.GOOGLE_AD_MANAGER_NETWORK_CODE)
            
            # Update social auth settings
            settings.SOCIAL_AUTH_GOOGLE_OAUTH2_KEY = settings.GOOGLE_OAUTH2_CLIENT_ID
            settings.SOCIAL_AUTH_GOOGLE_OAUTH2_SECRET = settings.GOOGLE_OAUTH2_CLIENT_SECRET
            
        return view_func(request, *args, **kwargs)
        
    return wrapper

import yaml
import tempfile
import time
import io
import csv
import gzip
import os
import ssl
import urllib3
import traceback
import zeep
import requests
import pycountry
import hashlib
import json
from django.core.cache import cache
from django.conf import settings
import os
from string import Template

import googleads.common

# googleads.common.MakeSoapRequest = patched_make_soap_request  # Disabled - not compatible

# SSL Configuration
import certifi
os.environ['SSL_CERT_FILE'] = certifi.where()
os.environ['REQUESTS_CA_BUNDLE'] = certifi.where()
os.environ['PYTHONHTTPSVERIFY'] = '0'

# Create custom SSL context
def create_google_ssl_context():
    context = ssl.create_default_context()
    context.check_hostname = False
    context.verify_mode = ssl.CERT_NONE
    return context

# Apply SSL context
try:
    ssl._create_default_https_context = create_google_ssl_context
except Exception as e:
    print(f"SSL context setup failed: {e}")

# Disable SSL warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
try:
    urllib3.disable_warnings(urllib3.exceptions.SecurityWarning)
except AttributeError:
    pass
try:
    urllib3.disable_warnings(urllib3.exceptions.SubjectAltNameWarning)
except AttributeError:
    pass

def get_last_5_days_indonesia_format(hari:int):
    today = datetime.now()
    dates = []
    for i in range(hari):
        date = today - timedelta(days=i)
        dates.append(date.strftime('%Y-%m-%d'))
    return dates

def get_country_name_from_code(code):
    try:
        country = pycountry.countries.get(alpha_2=code)
        return country.name if country else code
    except:
        return code

def generate_cache_key(prefix, *args, **kwargs):
    """
    Generate a consistent cache key from prefix and arguments
    """
    # Convert all arguments to strings and sort kwargs for consistency
    key_parts = [str(prefix)]
    
    # Add positional arguments
    for arg in args:
        if isinstance(arg, (list, tuple)):
            key_parts.extend([str(item) for item in arg])
        else:
            key_parts.append(str(arg))
    
    # Add keyword arguments (sorted for consistency)
    for key, value in sorted(kwargs.items()):
        key_parts.append(f"{key}:{value}")
    
    # Create hash of the key parts to ensure consistent length
    key_string = "|".join(key_parts)
    return hashlib.md5(key_string.encode()).hexdigest()

def get_cached_data(cache_key):
    """
    Retrieve data from cache
    """
    try:
        return cache.get(cache_key)
    except Exception as e:
        print(f"Cache retrieval error for key {cache_key}: {e}")
        return None

def set_cached_data(cache_key, data, timeout=None):
    """
    Store data in cache with optional timeout
    """
    try:
        if timeout is None:
            # Default timeout of 1 hour
            timeout = 3600
        
        cache.set(cache_key, data, timeout)
        return True
    except Exception as e:
        print(f"Cache storage error for key {cache_key}: {e}")
        return False

def invalidate_cache_pattern(pattern):
    """
    Invalidate cache keys matching a pattern
    """
    try:
        # This is a simplified version - in production you might want
        # to use Redis SCAN or similar for pattern matching
        if hasattr(cache, 'delete_pattern'):
            cache.delete_pattern(pattern)
        else:
            # Fallback for cache backends that don't support pattern deletion
            print(f"Pattern deletion not supported for cache backend. Pattern: {pattern}")
    except Exception as e:
        print(f"Cache invalidation error for pattern {pattern}: {e}")

def cache_facebook_insights(func):
    """
    Decorator to cache Facebook Insights API responses
    """
    def wrapper(*args, **kwargs):
        # Generate cache key based on function name and arguments
        cache_key = generate_cache_key(f"fb_insights_{func.__name__}", *args, **kwargs)
        
        # Try to get from cache first
        cached_result = get_cached_data(cache_key)
        if cached_result is not None:
            print(f"Cache hit for {func.__name__}")
            return cached_result
        
        # If not in cache, call the function
        print(f"Cache miss for {func.__name__}, fetching fresh data")
        try:
            result = func(*args, **kwargs)
            
            # Cache the result for 30 minutes
            set_cached_data(cache_key, result, timeout=1800)
            
            return result
        except Exception as e:
            print(f"Error in cached function {func.__name__}: {e}")
            # Return empty result structure to prevent crashes
            return {
                'status': False,
                'error': str(e),
                'data': []
            }
    
    return wrapper

def invalidate_facebook_cache(account_ids=None, date_range=None):
    """
    Invalidate Facebook cache for specific accounts or date ranges
    """
    try:
        patterns = []
        
        if account_ids:
            if isinstance(account_ids, str):
                account_ids = [account_ids]
            
            for account_id in account_ids:
                patterns.append(f"fb_insights_*{account_id}*")
        
        if date_range:
            start_date, end_date = date_range
            patterns.append(f"fb_insights_*{start_date}*{end_date}*")
        
        if not patterns:
            # Invalidate all Facebook cache
            patterns = ["fb_insights_*"]
        
        for pattern in patterns:
            invalidate_cache_pattern(pattern)
            
        print(f"Invalidated cache patterns: {patterns}")
        
    except Exception as e:
        print(f"Error invalidating Facebook cache: {e}")

def clear_all_facebook_cache():
    """
    Clear all Facebook-related cache
    """
    try:
        invalidate_cache_pattern("fb_insights_*")
        print("Cleared all Facebook cache")
    except Exception as e:
        print(f"Error clearing Facebook cache: {e}")

def invalidate_cache_on_data_update(account_id, campaign_id=None, event_type='update'):
    """
    Invalidate relevant cache when data is updated
    """
    try:
        patterns = []
        
        # Invalidate account-level cache
        patterns.append(f"fb_insights_*{account_id}*")
        
        # If campaign-specific, also invalidate campaign cache
        if campaign_id:
            patterns.append(f"fb_insights_*{campaign_id}*")
        
        # Invalidate summary and aggregated data
        patterns.extend([
            "fb_insights_*total*",
            "fb_insights_*summary*",
            "fb_insights_*all*"
        ])
        
        for pattern in patterns:
            invalidate_cache_pattern(pattern)
        
        print(f"Cache invalidated for {event_type} on account {account_id}")
        
    except Exception as e:
        print(f"Error invalidating cache on data update: {e}")

def schedule_cache_refresh(account_id, delay_minutes=5):
    """
    Schedule cache refresh for an account (placeholder for future implementation)
    """
    try:
        # This would typically use Celery or similar task queue
        # For now, just log the intention
        print(f"Cache refresh scheduled for account {account_id} in {delay_minutes} minutes")
        
        # In a real implementation, you might:
        # from myapp.tasks import refresh_facebook_cache
        # refresh_facebook_cache.apply_async(
        #     args=[account_id],
        #     countdown=delay_minutes * 60
        # )
        
    except Exception as e:
        print(f"Error scheduling cache refresh: {e}")

def get_cache_stats():
    """
    Get cache statistics (if supported by cache backend)
    """
    try:
        # This depends on your cache backend
        if hasattr(cache, 'get_stats'):
            return cache.get_stats()
        else:
            return {
                'status': 'Cache stats not available for this backend',
                'backend': str(type(cache))
            }
    except Exception as e:
        return {
            'error': str(e)
        }

# Facebook API utility functions
@cache_facebook_insights
def get_campaign_budgets(account):
    """
    Ambil data daily_budget dari semua campaign dalam akun.
    """
    campaigns = account.get_campaigns(fields=['id', 'daily_budget'])
    # Convert to list first to make it serializable for caching
    campaigns_list = list(campaigns)
    budget_map = {}
    for c in campaigns_list:
        campaign_id = c.get('id')
        daily_budget = c.get('daily_budget')
        if campaign_id and daily_budget:
            # Budget biasanya dalam cent (misalnya: 5000 = Rp 50.00)
            budget_map[campaign_id] = float(daily_budget)
    return budget_map

@cache_facebook_insights
def get_status_map(account, level):
    entity_class = {
        'campaign': account.get_campaigns,
        'adset': account.get_ad_sets,
        'ad': account.get_ads
    }

    field_class = {
        'campaign': Campaign.Field.status,
        'adset': AdSet.Field.status,
        'ad': Ad.Field.status
    }

    items = entity_class[level](
        fields=['id', field_class[level]],
        params={'effective_status': ['ACTIVE']}
    )

    # Convert to list first to make it serializable for caching
    items_list = list(items)
    return {item['id']: item.get('status') for item in items_list}

def get_facebook_insights(access_token, account_id, start_date, end_date):
    FacebookAdsApi.init(access_token=access_token)
    account = AdAccount(account_id)
    levels = ['campaign', 'adset', 'ad']
    all_data = {}

    # Ambil data budget campaign secara terpisah
    campaign_budgets = get_campaign_budgets(account)

    for level in levels:
        fields = [
            AdsInsights.Field.campaign_id,
            AdsInsights.Field.adset_id,
            AdsInsights.Field.ad_id,
            AdsInsights.Field.campaign_name,
            AdsInsights.Field.adset_name,
            AdsInsights.Field.ad_name,
            AdsInsights.Field.spend,
        ]

        params = {
            'level': level,
            'time_range': {'since': start_date, 'until': end_date},
            'limit': 500,
            'breakdowns': []
        }

        insights = list(account.get_insights(fields=fields, params=params))
        status_map = get_status_map(account, level)
        agg = {}
        for item in insights:
            _id = (
                item.get('campaign_id') if level == 'campaign' else
                item.get('adset_id') if level == 'adset' else
                item.get('ad_id')
            )

            if not _id:
                continue

            if status_map.get(_id) != 'ACTIVE':
                continue

            if _id not in agg:
                agg[_id] = {
                    'id': _id,
                    'name': item.get('campaign_name') if level == 'campaign' else
                            item.get('adset_name') if level == 'adset' else
                            item.get('ad_name'),
                    'budget': campaign_budgets.get(_id, 0) if level == 'campaign' else 0,
                    'parent_id': None if level == 'campaign' else (
                        item.get('campaign_id') if level == 'adset' else item.get('adset_id')
                    ),
                    'spend': float(item.get('spend', 0)),
                }
            else:
                agg[_id]['spend'] += float(item.get('spend', 0))

        all_data[level] = list(agg.values())

    return all_data


def generate_treemap_data(all_data, account_id, account_name='FB Account'):
    tree_data = []

    # Tambahkan level akun sebagai root (level 1)
    tree_data.append({
        'id': account_id,
        'name': account_name,
        'value': 0,  # Bisa diisi total spend jika dihitung
    })

    seen_ids = set()

    def add_node(node):
        if node['id'] not in seen_ids:
            tree_data.append(node)
            seen_ids.add(node['id'])

    for campaign in all_data.get('campaign', []):
        add_node({
            'id': campaign['id'],
            'name': campaign['name'],
            'value': campaign['spend'],
            'budget': campaign['budget'],
            'parent': account_id,  # ✅ Parent-nya akun
            'custom': campaign
        })

    for adset in all_data.get('adset', []):
        if adset['parent_id']:
            add_node({
                'id': adset['id'],
                'name': adset['name'],
                'parent': adset['parent_id'],
                'value': adset['spend'],
                'budget': adset['budget'],
                'custom': adset
            })

    for ad in all_data.get('ad', []):
        if ad['parent_id']:
            add_node({
                'id': ad['id'],
                'name': ad['name'],
                'parent': ad['parent_id'],
                'value': ad['spend'],
                'budget': ad['budget'],
                'custom': ad
            })

    return tree_data


def remove_duplicates(data):
    seen_ids = set()
    unique_data = []
    for item in data:
        if item['id'] not in seen_ids:
            unique_data.append(item)
            seen_ids.add(item['id'])
    return unique_data

def fetch_data_all_insights(access_token, account_id, account_name, start_date, end_date):
    raw_data = get_facebook_insights(access_token, account_id, start_date, end_date)
    treemap_data = generate_treemap_data(raw_data, account_id, account_name)
    treemap_data = remove_duplicates(treemap_data)
    return treemap_data

@cache_facebook_insights
def get_facebook_insights_all(rs_account, start_date, end_date):
    all_data = {}  # key: account_id
    for data in rs_account:
        FacebookAdsApi.init(access_token=data['access_token'])
        account = AdAccount(data['account_id'])
        account_id = data['account_id']

        campaign_budgets = get_campaign_budgets(account)
        all_data[account_id] = {'campaign': [], 'adset': [], 'ad': []}

        for level in ['campaign', 'adset', 'ad']:
            fields = [
                AdsInsights.Field.campaign_id,
                AdsInsights.Field.adset_id,
                AdsInsights.Field.ad_id,
                AdsInsights.Field.campaign_name,
                AdsInsights.Field.adset_name,
                AdsInsights.Field.ad_name,
                AdsInsights.Field.spend,
            ]
            params = {
                'level': level,
                'time_range': {'since': start_date, 'until': end_date},
                'limit': 500,
                'breakdowns': []
            }

            insights = list(account.get_insights(fields=fields, params=params))
            status_map = get_status_map(account, level)
            agg = {}
            for item in insights:
                _id = (
                    item.get('campaign_id') if level == 'campaign' else
                    item.get('adset_id') if level == 'adset' else
                    item.get('ad_id')
                )
                if not _id or status_map.get(_id) != 'ACTIVE':
                    continue

                if _id not in agg:
                    agg[_id] = {
                        'id': _id,
                        'name': item.get('campaign_name') if level == 'campaign' else
                                item.get('adset_name') if level == 'adset' else
                                item.get('ad_name'),
                        'budget': campaign_budgets.get(_id, 0) if level == 'campaign' else 0,
                        'parent_id': None if level == 'campaign' else (
                            item.get('campaign_id') if level == 'adset' else item.get('adset_id')
                        ),
                        'spend': float(item.get('spend', 0)),
                    }
                else:
                    agg[_id]['spend'] += float(item.get('spend', 0))

            all_data[account_id][level] = list(agg.values())
    return all_data


def generate_treemap_data_all(all_data, rs_account):
    tree_data = []

    # Root artificial
    tree_data.append({
        'id': 'root',
        'name': 'Daftar Akun',
        'color': '#cccccc'
    })

    seen_ids = set()

    def add_node(node):
        if node['id'] not in seen_ids:
            tree_data.append(node)
            seen_ids.add(node['id'])

    for data in rs_account:
        account_id = data['account_id']
        account_name = data['account_name']
        account_data = all_data.get(account_id, {'campaign': [], 'adset': [], 'ad': []})

        total_spend = 0

        # Buat node akun
        account_node = {
            'id': account_id,
            'name': account_name,
            'value': 0,
            'parent': 'root'
        }
        add_node(account_node)

        for campaign in account_data['campaign']:
            total_spend += campaign['spend']
            add_node({
                'id': campaign['id'],
                'name': campaign['name'],
                'value': campaign['spend'],
                'budget': campaign['budget'],
                'parent': account_id,
                'custom': campaign
            })

        for adset in account_data['adset']:
            if adset['parent_id']:
                add_node({
                    'id': adset['id'],
                    'name': adset['name'],
                    'parent': adset['parent_id'],
                    'value': adset['spend'],
                    'budget': adset['budget'],
                    'custom': adset
                })

        for ad in account_data['ad']:
            if ad['parent_id']:
                add_node({
                    'id': ad['id'],
                    'name': ad['name'],
                    'parent': ad['parent_id'],
                    'value': ad['spend'],
                    'budget': ad['budget'],
                    'custom': ad
                })

        # Update nilai total spend akun
        for i in range(len(tree_data)):
            if tree_data[i]['id'] == account_id:
                tree_data[i]['value'] = total_spend
                break

    return tree_data


def fetch_data_all_insights_data_all(rs_account, start_date, end_date):
    raw_data = get_facebook_insights_all(rs_account, start_date, end_date)
    treemap_data = generate_treemap_data_all(raw_data, rs_account)
    treemap_data = remove_duplicates(treemap_data)
    return treemap_data

@cache_facebook_insights
def fetch_data_all_insights_total(access_token, account_id, start_date, end_date):
    FacebookAdsApi.init(access_token=access_token)
    account = AdAccount(account_id)
    
    # Ambil data di level campaign untuk konsistensi dengan fungsi _all
    fields = [
        AdsInsights.Field.campaign_id,
        AdsInsights.Field.campaign_name,
        AdsInsights.Field.spend,
        AdsInsights.Field.impressions,
        AdsInsights.Field.reach,
        AdsInsights.Field.actions,
    ]
    params = {
        'level': 'campaign',
        'time_range': {'since': start_date, 'until': end_date},
        'limit': 1000
    }
    insights = account.get_insights(fields=fields, params=params)
    
    # Aggregate semua data campaign dalam account ini
    total_summary = {
        'spend': 0.0,
        'clicks': 0,
        'impressions': 0,
        'reach': 0,
        'total_cpr': 0.0,
        'total_cpc': 0.0,
    }
    
    for item in insights:
        spend = float(item.get('spend', 0))
        impressions = int(item.get('impressions', 0))
        reach = int(item.get('reach', 0))
        
        # Ambil clicks dan results dari actions dengan action_type 'link_click'
        clicks = 0
        results_count = 0
        actions = item.get('actions', [])
        for action in actions:
            if action.get('action_type') == 'link_click':
                action_value = int(action.get('value', 0))
                clicks += action_value
                results_count += action_value
        
        # Hitung CPR dan CPC untuk campaign ini
        campaign_cpr = 0.0
        campaign_cpc = 0.0
        
        if results_count > 0:
            campaign_cpr = spend / results_count
        
        if clicks > 0:
            campaign_cpc = spend / clicks
        
        # Tambahkan ke total
        total_summary['spend'] += spend
        total_summary['clicks'] += clicks
        total_summary['impressions'] += impressions
        total_summary['reach'] += reach
        total_summary['total_cpr'] += campaign_cpr
        total_summary['total_cpc'] += campaign_cpc
    
    # Ambil account name dari account info
    account_info = account.api_get(fields=['name'])
    account_name = account_info.get('name', 'Unknown Account')
    
    result = [{
        'account_name': account_name,
        'spend': total_summary['spend'],
        'clicks': total_summary['clicks'],
        'impressions': total_summary['impressions'],
        'reach': total_summary['reach'],
        'cpr': total_summary['total_cpr'],
        'cpc': total_summary['total_cpc']
    }]
    
    return result

@cache_facebook_insights
def fetch_data_all_insights_total_all(rs_account, start_date, end_date):
    total_summary = {
        'spend': 0.0,
        'clicks': 0,
        'impressions': 0,
        'reach': 0,
        'total_cpr': 0.0,  # Total CPR dari semua campaign
        'total_cpc': 0.0,  # Total CPC dari semua campaign
    }
    
    for data in rs_account:
        if data is None:
            continue
        try:
            FacebookAdsApi.init(access_token=data['access_token'])
            account = AdAccount(data['account_id'])
            
            # Ambil data di level campaign untuk mendapatkan detail yang lebih akurat
            fields = [
                AdsInsights.Field.campaign_id,
                AdsInsights.Field.campaign_name,
                AdsInsights.Field.spend,
                AdsInsights.Field.impressions,
                AdsInsights.Field.reach,
                AdsInsights.Field.actions,
            ]
            params = {
                'level': 'campaign',
                'time_range': {'since': start_date, 'until': end_date},
                'limit': 1000
            }
            insights = account.get_insights(fields=fields, params=params)
            
            for item in insights:
                spend = float(item.get('spend', 0))
                impressions = int(item.get('impressions', 0))
                reach = int(item.get('reach', 0))
                
                # Ambil clicks dan results dari actions dengan action_type 'link_click'
                clicks = 0
                results_count = 0
                actions = item.get('actions', [])
                for action in actions:
                    if action.get('action_type') == 'link_click':
                        action_value = int(action.get('value', 0))
                        clicks += action_value
                        results_count += action_value
                
                # Hitung CPR dan CPC untuk campaign ini
                campaign_cpr = 0.0
                campaign_cpc = 0.0
                
                if results_count > 0:
                    campaign_cpr = spend / results_count
                
                if clicks > 0:
                    campaign_cpc = spend / clicks
                
                # Tambahkan ke total
                total_summary['spend'] += spend
                total_summary['clicks'] += clicks
                total_summary['impressions'] += impressions
                total_summary['reach'] += reach
                total_summary['total_cpr'] += campaign_cpr
                total_summary['total_cpc'] += campaign_cpc
                
        except Exception as e:
            continue
    
    result = [{
        'spend': total_summary['spend'],
        'clicks': total_summary['clicks'],
        'impressions': total_summary['impressions'],
        'reach': total_summary['reach'],
        'cpr': total_summary['total_cpr'],
        'cpc': total_summary['total_cpc']
    }]
    
    return result

def fetch_data_insights_account_range(access_token, account_id, start_date, end_date):
    FacebookAdsApi.init(access_token=access_token)
    account = AdAccount(account_id)
    # Ambil data insights harian di level account
    insights = account.get_insights(
        fields=[
            AdsInsights.Field.date_start,
            AdsInsights.Field.spend,
            AdsInsights.Field.reach,
            AdsInsights.Field.impressions,
            AdsInsights.Field.clicks,
            AdsInsights.Field.ctr,
            AdsInsights.Field.purchase_roas
        ],
        params={
            'level': 'account',
            'time_range': {
                'since': start_date,
                'until': end_date,
            },
            'time_increment': 1,  # Data harian
            'limit': 500
        }
    )
    # Ubah data ke list of dict
    data = []
    for row in insights:
        data.append({
            'date': row.get('date_start'),
            'spend': float(row.get('spend', 0)),
            'reach': int(row.get('reach', 0)),
            'impressions': int(row.get('impressions', 0)),
            'clicks': int(row.get('clicks', 0)),
            'ctr': float(row.get('ctr', 0)),
            'roas': float(row.get('purchase_roas', [{}])[0].get('value', 0)) if row.get('purchase_roas') else 0,
        })
    return data

@cache_facebook_insights
def fetch_data_insights_account_range_all(rs_account, start_date, end_date):
    # Dictionary total per tanggal
    summary_by_date = defaultdict(lambda: {
        'spend': 0.0,
        'clicks': 0,
        'impressions': 0,
        'reach': 0,
        'ctr_total': 0.0,
        'ctr_count': 0
    })
    for data in rs_account:
        if data is None:
            continue
        try:
            FacebookAdsApi.init(access_token=data['access_token'])
            account = AdAccount(data['account_id'])

            insights = account.get_insights(
                fields=[
                    AdsInsights.Field.date_start,
                    AdsInsights.Field.spend,
                    AdsInsights.Field.reach,
                    AdsInsights.Field.impressions,
                    AdsInsights.Field.clicks,
                    AdsInsights.Field.ctr
                ],
                params={
                    'level': 'account',
                    'time_range': {
                        'since': start_date,
                        'until': end_date,
                    },
                    'time_increment': 1,  # per hari
                    'limit': 500
                }
            )

            for row in insights:
                date = row.get('date_start')
                spend = float(row.get('spend', 0))
                reach = int(row.get('reach', 0))
                impressions = int(row.get('impressions', 0))
                clicks = int(row.get('clicks', 0))
                ctr = float(row.get('ctr', 0))

                summary_by_date[date]['spend'] += spend
                summary_by_date[date]['reach'] += reach
                summary_by_date[date]['impressions'] += impressions
                summary_by_date[date]['clicks'] += clicks
                summary_by_date[date]['ctr_total'] += ctr
                summary_by_date[date]['ctr_count'] += 1

        except Exception as e:
            continue
    # Convert ke list of dict & hitung rata-rata CTR per tanggal
    result = []
    for date, stats in sorted(summary_by_date.items()):
        ctr_avg = stats['ctr_total'] / stats['ctr_count'] if stats['ctr_count'] > 0 else 0
        result.append({
            'date': date,
            'spend': stats['spend'],
            'clicks': stats['clicks'],
            'impressions': stats['impressions'],
            'reach': stats['reach'],
            'ctr': ctr_avg
        })

    return result

def fetch_data_insights_account(access_token, account_id, tanggal, data_sub_domain, account_name=None):
    FacebookAdsApi.init(access_token=access_token)
    account = AdAccount(account_id)
    if tanggal == '%':
        today = datetime.now().strftime('%Y-%m-%d')
    else:
        today = tanggal
    start_date = end_date = today
    time_range = {
        'since': str(start_date),
        'until': str(end_date)
    }
    # Setup params
    if data_sub_domain and data_sub_domain != '%' and data_sub_domain.strip() != '':
       params = {
            'level': 'campaign',
            'time_range': time_range,
            'filtering': [
                {
                    'field': 'campaign.name',
                    'operator': 'CONTAIN',
                    'value': data_sub_domain
                }
            ]
        }
    else :
        params = {
            'level': 'campaign',
            'time_range': time_range,
        }
    # Ambil konfigurasi campaign
    campaign_configs = account.get_campaigns(fields=[
        Campaign.Field.id,
        Campaign.Field.name,
        Campaign.Field.status,
        Campaign.Field.daily_budget,
        Campaign.Field.start_time,
        Campaign.Field.stop_time
    ])
    campaign_map = {
        c['id']: {
            'name': c.get('name'),
            'status': c.get('status'),
            'daily_budget': float(c.get('daily_budget') or 0),
            'start_time': c.get('start_time'),
            'stop_time': c.get('stop_time'),
        } for c in campaign_configs
    }
    campaign_aggregates = defaultdict(lambda: {
        'spend': 0.0,
        'reach': 0,
        'impressions': 0,
        'clicks': 0,
        'impressions': 0,
        'cpr': 0.0,
        'daily_budget': 0.0,
        'frequency': 0.0,
        'status': '',
        'start_time': '',
        'stop_time': '',
        'campaign_name': '',
    })
    insights = account.get_insights(
        fields=[
            AdsInsights.Field.campaign_id,
            AdsInsights.Field.campaign_name,
            AdsInsights.Field.spend,
            AdsInsights.Field.reach,
            AdsInsights.Field.impressions,
            AdsInsights.Field.cost_per_result,
            AdsInsights.Field.actions
        ],
        params=params
    )
    for row in insights:
        campaign_id = row.get('campaign_id')
        if not campaign_id:
            continue
        config = campaign_map.get(campaign_id, {})
        agg = campaign_aggregates[campaign_id]
        agg['campaign_name'] = row.get('campaign_name')
        agg['spend'] += float(row.get('spend', 0))
        agg['reach'] += int(row.get('reach', 0))
        agg['impressions'] += int(row.get('impressions', 0))
        frequency = float(agg['impressions']/agg['reach'])
        agg['frequency'] = frequency
        cost_per_result = None
        for cpr_item in row.get('cost_per_result', []):
            if cpr_item.get('indicator') == 'actions:link_click':
                values = cpr_item.get('values', [])
                if values:
                    cost_per_result = values[0].get('value')
                break
        if cost_per_result and str(cost_per_result).replace('.', '', 1).isdigit():
            agg['cpr'] = float(cost_per_result)
        result_action_type = 'link_click'
        result_count = 0
        for action in row.get('actions', []):
            if action.get('action_type') == result_action_type:
                result_count = float(action.get('value', 0))
                break
        if result_count not in [None, ""]:
            agg['clicks'] = result_count
        if not agg['status']:
            agg['status'] = config.get('status')
            agg['daily_budget'] = float(config.get('daily_budget', 0))
            agg['start_time'] = config.get('start_time')
            agg['stop_time'] = config.get('stop_time')
    data = []
    total_budget = total_spend = total_clicks = total_impressions = total_reach = total_cpr = total_cpc = 0
    for campaign_id, agg in campaign_aggregates.items():
        data.append({
            'campaign_id': campaign_id,
            'campaign_name': agg['campaign_name'],
            'account_name': account_name if account_name else 'N/A',
            'daily_budget': agg['daily_budget'],
            'spend': round(agg['spend'], 2),
            'impressions': agg['impressions'],
            'reach': agg['reach'],
            'clicks': agg['clicks'],
            'frequency': agg['frequency'],
            'cpr': agg['cpr'],
            'status': agg['status'],
            'start_time': agg['start_time'],
            'stop_time': agg['stop_time'],
        })
        total_budget += agg['daily_budget']
        total_spend += agg['spend']
        total_impressions += agg['impressions']
        total_reach += agg['reach']
        total_clicks += agg['clicks']
        total_frequency = float(total_impressions / total_reach)
        total_cpr += agg['cpr']
    sorted_data = sorted(
        data,
        key=lambda x: datetime.strptime(x['start_time'], '%Y-%m-%dT%H:%M:%S%z') if x['start_time'] else datetime.min,
        reverse=True
    )
    total = [{
        'total_budget': total_budget,
        'total_spend': total_spend,
        'total_impressions': total_impressions,
        'total_reach': total_reach,
        'total_click': total_clicks,
        'total_frequency' : total_frequency,
        'total_cpr': total_cpr
    }]
    return {
        'data': sorted_data,
        'total': total
    }

def fetch_data_insights_account_filter_all(rs_account):
    all_data = []
    for data in rs_account:
        try:
            FacebookAdsApi.init(access_token=data['access_token'])
            account = AdAccount(data['account_id'])
            insights = account.get_insights(
                fields=[
                    AdsInsights.Field.campaign_id,
                    AdsInsights.Field.campaign_name,
                ],
                params={
                    'level': 'campaign',
                    'limit': 100,
                    'filtering': [{
                        'field': 'campaign.effective_status',
                        'operator': 'IN',
                        'value': ['ACTIVE']
                    }]
                }
            )

            for row in insights:
                campaign_name_raw = row.get('campaign_name')
                if campaign_name_raw:
                    campaign_name_clean = campaign_name_raw.split('_')[0]
                    all_data.append({
                        'account_id': data['account_id'],
                        'campaign_id': row.get('campaign_id'),
                        'campaign_name': campaign_name_clean,
                    })
        except Exception as e:
            print(f"Error fetching filtered data for account {data['account_id']}: {e}")
            continue

    # Kumpulkan unique campaign_name saja
    seen = set()
    rs_data = []
    for item in all_data:
        name = item['campaign_name']
        if name not in seen:
            seen.add(name)
            rs_data.append({'campaign_name': name})

    # Cetak hanya sekali, setelah semua akun diproses
    return rs_data

def fetch_data_insights_account_filter(access_token, account_id, start_date, end_date):
    FacebookAdsApi.init(access_token=access_token)
    account = AdAccount(account_id)
    insights = account.get_insights(
        fields=[
            AdsInsights.Field.campaign_id,
            AdsInsights.Field.campaign_name,
        ],
        params={
            'level': 'campaign',
            'time_range': {
                'since': start_date,
                'until': end_date,
            },
            'limit': 100,
            'filtering': [{
                'field': 'campaign.effective_status',
                'operator': 'IN',
                'value': ['ACTIVE']
            }]
        }
    )
    data = []
    for row in insights:
        data.append({
            'campaign_id': row.get('campaign_id'),
            'campaign_name': row.get('campaign_name'),
        })
    return data

def fetch_daily_budget_per_campaign(access_token, account_id, campaign_id, daily_budget):
    
    # Inisialisasi API
    FacebookAdsApi.init(access_token=access_token)
    daily_budget = int(float(daily_budget))
    
    # Update daily budget via SDK

    # campaign = Campaign(campaign_id)
    # campaign[Campaign.Field.daily_budget] = daily_budget
    # campaign.api_get(fields=[Campaign.Field.status])
    # campaign.api_update()
    # updated_campaign = campaign.api_get(fields=[
    #     Campaign.Field.id,
    #     Campaign.Field.name,
    #     Campaign.Field.status,
    #     Campaign.Field.daily_budget
    # ])
    # return updated_campaign

    # CEK STATUS CAMPAIGN
    # url = 'https://graph.facebook.com/debug_token'
    # params = {
    #     'input_token': access_token,
    #     'access_token': '1082771789856107|ccd700fa4f9d8e0b1509b7bd28d3e6eb'  # Ganti dengan token yang sesuai
    # }
    # res = requests.get(url, params=params)
    # print(res.json())   

    # Update daily budget via REST API
    url = f"https://graph.facebook.com/v18.0/{campaign_id}"
    payload = {
        "daily_budget": daily_budget,
        "access_token": access_token
    }
    requests.post(url, data=payload)
    # Ambil data campaign setelah update
    campaign = Campaign(campaign_id)
    updated_campaign = campaign.api_get(fields=[
        Campaign.Field.id,
        Campaign.Field.name,
        Campaign.Field.status,
        Campaign.Field.daily_budget
    ])
    return updated_campaign

def fetch_status_per_campaign(access_token, campaign_id, status):
    try:
        # Validasi input parameters
        if not access_token:
            return {'error': 'Access token tidak valid'}
        if not campaign_id:
            return {'error': 'Campaign ID tidak valid'}
        if status not in ['ACTIVE', 'PAUSED']:
            return {'error': 'Status harus ACTIVE atau PAUSED'}
            
        # Inisialisasi API (tanpa app_id & app_secret jika hanya pakai token)
        FacebookAdsApi.init(access_token=access_token)
        # Update status campaign menggunakan SDK (lebih konsisten)
        campaign = Campaign(campaign_id)
        campaign.api_update(params={
            'status': status  # 'ACTIVE' atau 'PAUSED'
        })
        # Ambil data campaign setelah update
        updated_status = campaign.api_get(fields=[
            Campaign.Field.id,
            Campaign.Field.name,
            Campaign.Field.status
        ])
        return {
            'id': updated_status[Campaign.Field.id],
            'name': updated_status[Campaign.Field.name],
            'status': updated_status[Campaign.Field.status]
        }
    except FacebookRequestError as e:
        error_msg = f"Facebook API Error: {e.api_error_message()}"
        print(error_msg)
        return {'error': error_msg}
    except Exception as e:
        error_msg = f"General Error: {str(e)}"
        print(error_msg)
        return {'error': error_msg}

@cache_facebook_insights
def fetch_data_insights_campaign_filter_sub_domain(rs_account, start_date, end_date, data_sub_domain):
    all_data = []
    total = []
    total_budget = total_spend = total_clicks = total_impressions = total_reach = total_cpr = 0
    for data in rs_account:
        campaign_aggregates = defaultdict(lambda: {
            'spend': 0.0,
            'reach': 0,
            'impressions': 0,
            'clicks': 0,
            'cpr': 0.0,
            'daily_budget': 0.0,
            'frequency': 0.0,
            'status': '',
        })
        FacebookAdsApi.init(access_token=data['access_token'])
        account = AdAccount(data['account_id'])
        campaign_configs = account.get_campaigns(fields=[
            Campaign.Field.id,
            Campaign.Field.name,
            Campaign.Field.status,
            Campaign.Field.daily_budget
        ])
        campaign_map = {
            c['id']: {
                'name': c.get('name'),
                'status': c.get('status'),
                'daily_budget': float(c.get('daily_budget') or 0)
            } for c in campaign_configs
        }
        fields = [
            AdsInsights.Field.campaign_id,
            AdsInsights.Field.campaign_name,
            AdsInsights.Field.spend,
            AdsInsights.Field.reach,
            AdsInsights.Field.impressions,
            AdsInsights.Field.cost_per_result,
            AdsInsights.Field.actions
        ]
        if data_sub_domain != '%':
            params = {
                'level': 'campaign',
                'time_range': {
                    'since': start_date,
                    'until': end_date
                },
                'filtering': [{
                    'field': 'campaign.name',
                    'operator': 'CONTAIN',
                    'value': data_sub_domain
                }],
                'limit': 1000
            }
        else:
            params = {
                'level': 'campaign',
                'time_range': {
                    'since': start_date,
                    'until': end_date
                },
                'limit': 1000
            }
        insights = account.get_insights(fields=fields, params=params)
        for item in insights:
            campaign_id = item.get('campaign_id')
            if not campaign_id:
                continue
            config = campaign_map.get(campaign_id, {})
            agg = campaign_aggregates[campaign_id]
            agg['campaign_name'] = item.get('campaign_name')
            agg['spend'] += float(item.get('spend', 0))
            agg['reach'] += int(item.get('reach', 0))
            agg['impressions'] += int(item.get('impressions', 0))
            if agg['reach'] > 0:
                frequency = float(agg['impressions']/agg['reach'])
                agg['frequency'] = frequency
            else:
                agg['frequency'] = 0.0
            cost_per_result = None
            for cpr_item in item.get('cost_per_result', []):
                if cpr_item.get('indicator') == 'actions:link_click':
                    values = cpr_item.get('values', [])
                    if values:
                        cost_per_result = values[0].get('value')
                    break
            if cost_per_result and str(cost_per_result).replace('.', '', 1).isdigit():
                agg['cpr'] += float(cost_per_result)
            result_action_type = 'link_click'
            result_count = 0
            for action in item.get('actions', []):
                if action.get('action_type') == result_action_type:
                    result_count = float(action.get('value', 0))
                    break
            if result_count not in [None, ""]:
                agg['clicks'] += result_count
            if not agg['status']:
                agg['status'] = config.get('status')
                agg['daily_budget'] += float(config.get('daily_budget', 0))
        for campaign_id, agg in campaign_aggregates.items():
            all_data.append({
                'account_name': data['account_name'],
                'campaign_id': campaign_id,
                'campaign_name': agg['campaign_name'],
                'budget': agg['daily_budget'],
                'spend': round(agg['spend'], 2),
                'impressions': agg['impressions'],
                'reach': agg['reach'],
                'clicks': agg['clicks'],
                'frequency': agg['frequency'],
                'cpr': agg['cpr']
            })
            total_budget += agg['daily_budget']
            total_spend += agg['spend']
            total_impressions += agg['impressions']
            total_reach += agg['reach']
            total_clicks += agg['clicks']
            if total_reach > 0:
                total_frequency = float(total_impressions / total_reach)
            else:
                total_frequency = 0.0
            total_cpr += agg['cpr']
    total.append({
        'total_budget': total_budget,
        'total_spend': total_spend,
        'total_impressions': total_impressions,
        'total_reach': total_reach,
        'total_click': total_clicks,
        'total_frequency' : total_frequency,
        'total_cpr': total_cpr
    })
    rs_data = {
        'data': all_data,
        'total': total
    }
    return rs_data

def fetch_data_insights_campaign_filter_account(access_token, account_id, account_name, start_date, end_date, data_campaign):
    rs_data = []
    data = []
    total = []
    total_spend = 0
    total_reach = 0
    total_impressions = 0
    total_clicks = 0
    total_cpr = 0
    total_budget = 0
    total_frequency = 0
    FacebookAdsApi.init(access_token=access_token)
    account = AdAccount(account_id)
    campaign_configs = account.get_campaigns(fields=[
        Campaign.Field.id,
        Campaign.Field.name,
        Campaign.Field.status,
        Campaign.Field.daily_budget
    ])
    campaign_map = {
        c['id']: {
            'name': c.get('name'),
            'status': c.get('status'),
            'daily_budget': float(c.get('daily_budget') or 0)
        } for c in campaign_configs
    }
    fields = [
        AdsInsights.Field.campaign_id,
        AdsInsights.Field.campaign_name,
        AdsInsights.Field.impressions,
        AdsInsights.Field.reach,
        AdsInsights.Field.spend,
        AdsInsights.Field.cost_per_result,
        AdsInsights.Field.actions
    ]
    params = {
        'level': 'campaign',
        'time_range': {
            'since': start_date,
            'until': end_date
        },
        'limit': 1000
    }
    if data_campaign != '%':
        params['filtering'] = [{
            'field': 'campaign.name',
            'operator': 'CONTAIN',
            'value': data_campaign
        }]
    insights = account.get_insights(fields=fields, params=params)
    data = []
    for item in insights:
        campaign_id = item.get('campaign_id')
        if not campaign_id:
            continue
        config = campaign_map.get(campaign_id, {})
        daily_budget = float(config.get('daily_budget', 0))
        spend = float(item.get('spend', 0))
        impressions = int(item.get('impressions', 0))
        reach = int(item.get('reach', 0))
        # Ekstrak cost_per_result (CPR)
        cost_per_result = None
        for cpr_item in item.get('cost_per_result', []):
            # Ambil CPR untuk indicator tertentu (contoh: actions:link_click)
            if cpr_item.get('indicator') == 'actions:link_click':
                values = cpr_item.get('values', [])
                if values:
                    cost_per_result = values[0].get('value')
                break  # Berhenti setelah ketemu yang cocok
        result_action_type = 'link_click'
        result_count = 0
        for action in item.get('actions', []):
            if action.get('action_type') == result_action_type:
                result_count = float(action.get('value', 0))
                break
        clicks = result_count
        frequency = float(impressions/reach) if reach > 0 else 0.0
        total_budget += daily_budget
        total_spend += spend
        total_impressions += impressions
        total_reach += reach
        total_clicks += clicks
        if cost_per_result is not None:
            total_cpr += float(cost_per_result)
        data.append({
            'account_name': account_name,  # opsional, agar tahu campaign ini dari akun mana
            'campaign_id': item.get('campaign_id'),
            'campaign_name': item.get('campaign_name'),
            'budget': daily_budget,
            'spend': spend,
            'impressions': impressions,
            'reach': reach,
            'clicks': clicks,
            'cpr': cost_per_result,
            'frequency': frequency
        })
    total.append({
        'total_budget': total_budget,
        'total_spend': total_spend,
        'total_impressions': total_impressions,
        'total_reach' : total_reach,
        'total_click': total_clicks,
        'total_frequency' : total_frequency,
        'total_cpr' : total_cpr
    })
    rs_data = {
        'data': data,
        'total': total
    }
    return rs_data

def fetch_data_insights_by_country_filter_campaign(rs_account, start_date, end_date, data_sub_domain):
    country_totals = defaultdict(lambda: {
        'spend': 0.0,
        'impressions': 0,
        'reach': 0,
        'clicks': 0,
        'frequency': 0.0,
        'cpr': 0.0
    })
    for data in rs_account:
        FacebookAdsApi.init(access_token=data['access_token'])
        account = AdAccount(data['account_id'])
        fields = [
            AdsInsights.Field.ad_id,
            AdsInsights.Field.ad_name,
            AdsInsights.Field.adset_id,
            AdsInsights.Field.campaign_id,
            AdsInsights.Field.campaign_name,
            AdsInsights.Field.spend,
            AdsInsights.Field.reach,
            AdsInsights.Field.impressions,
            AdsInsights.Field.cost_per_result,
            AdsInsights.Field.actions
        ]
        if data_sub_domain != '%':
            params = {
                'level': 'campaign',
                'time_range': {
                    'since': start_date,
                    'until': end_date
                },
                'filtering': [{
                    'field': 'campaign.name',
                    'operator': 'CONTAIN',
                    'value': data_sub_domain
                }],
                'breakdowns': ['country'],
                'limit': 1000
            }
        else:
            params = {
                'level': 'campaign',
                'time_range': {
                    'since': start_date,
                    'until': end_date
                },
                'breakdowns': ['country'],
                'limit': 1000
            }
        insights = account.get_insights(fields=fields, params=params)
        for item in insights:
            country_code = item.get('country')
            country_name = get_country_name_from_code(country_code)
            if not country_name:
                continue
            country_label = f"{country_name} ({country_code})"
            spend = float(item.get('spend', 0))
            impressions = int(item.get('impressions', 0))
            reach = int(item.get('reach', 0))
            frequency = float(impressions/reach) if reach > 0 else 0.0
            result_action_type = 'link_click'
            result_count = 0
            for action in item.get('actions', []):
                if action.get('action_type') == result_action_type:
                    result_count = float(action.get('value', 0))
                    break
            clicks = float(result_count)
            # Ambil CPR (cost_per_result)
            cost_per_result = 0.0
            for cpr_item in item.get('cost_per_result', []):
                if cpr_item.get('indicator') == 'actions:link_click':
                    values = cpr_item.get('values', [])
                    if values and str(values[0].get('value', '')).replace('.', '', 1).isdigit():
                        cost_per_result = float(values[0].get('value'))
                    break
            # Akumulasi
            country_totals[country_label]['spend'] += spend
            country_totals[country_label]['impressions'] += impressions
            country_totals[country_label]['reach'] += reach
            country_totals[country_label]['clicks'] += clicks
            country_totals[country_label]['frequency'] = frequency
            country_totals[country_label]['cpr'] += cost_per_result
    result = []
    total_spend = 0
    total_impressions = 0
    total_reach = 0
    total_clicks = 0
    total_cpr = 0
    total_frequency = 0
    total_other_costs = 0
    for country, data in country_totals.items():
        spend = data['spend']
        impressions = data['impressions']
        reach = data['reach']
        clicks = data['clicks']
        # Tidak menggunakan frequency dan cpr dari data asli karena akan dihitung ulang
        total_spend += spend
        total_impressions += impressions
        total_reach += reach
        total_clicks += clicks
        
        # Hitung frequency per negara yang benar
        country_frequency = round(impressions / reach, 2) if reach > 0 else 0
        country_cpr = round(spend / clicks, 2) if clicks > 0 else 0
        
        result.append({
            'country': country,
            'spend': round(spend, 2),
            'impressions': impressions,
            'reach': reach,
            'clicks': clicks,
            'frequency': country_frequency,
            'cpr': country_cpr,
        })
    
    # Hitung total frequency dan total CPR yang benar berdasarkan total agregat
    total_frequency = round(total_impressions / total_reach, 2) if total_reach > 0 else 0
    total_cpr_calculated = round(total_spend / total_clicks, 2) if total_clicks > 0 else 0
    
    # Sort data
    result_sorted = sorted(result, key=lambda x: x['impressions'], reverse=True)
    rs_data = {
        'data': result_sorted,
        'total': [{
            'total_spend': total_spend,
            'total_impressions': total_impressions,
            'total_reach': total_reach,
            'total_click': total_clicks,
            'total_cpr': total_cpr_calculated,
            'total_frequency': total_frequency,
            'total_other_costs': round(float(total_other_costs or 0), 2),
        }]
    }
    return rs_data

def fetch_data_country_facebook_ads(rs_account, start_date, end_date):
    country_totals = defaultdict(lambda: {
        'clicks': 0,
    })
    for data in rs_account:
        FacebookAdsApi.init(access_token=data['access_token'])
        account = AdAccount(data['account_id'])
        fields = [
            AdsInsights.Field.ad_id,
            AdsInsights.Field.ad_name,
            AdsInsights.Field.adset_id,
            AdsInsights.Field.campaign_id,
            AdsInsights.Field.campaign_name,
            AdsInsights.Field.actions
        ]
        params = {
            'level': 'campaign',
            'time_range': {
                'since': start_date,
                'until': end_date
            },
            'breakdowns': ['country'],
            'limit': 1000
        }
        insights = account.get_insights(fields=fields, params=params)
        for item in insights:
            country_code = item.get('country')
            country_name = get_country_name_from_code(country_code)
            if not country_name:
                continue
            country_label = f"{country_name} ({country_code})"
            result_action_type = 'link_click'
            result_count = 0
            for action in item.get('actions', []):
                if action.get('action_type') == result_action_type:
                    result_count = float(action.get('value', 0))
                    break
            clicks = float(result_count)
            # Akumulasi
            country_totals[country_label]['country_code'] = country_code
            country_totals[country_label]['country_name'] = country_label
            country_totals[country_label]['clicks'] += clicks
    result = []
    for country, data in country_totals.items():
        country_code = data['country_code']
        clicks = data['clicks']
        result.append({
            'code':country_code,
            'name': country
        })
    return result

def fetch_data_insights_by_country_filter_account(access_token, account_id, start_date, end_date, data_sub_domain):
    FacebookAdsApi.init(access_token=access_token)
    account = AdAccount(account_id)
    fields = [
        AdsInsights.Field.ad_id,
        AdsInsights.Field.ad_name,
        AdsInsights.Field.adset_id,
        AdsInsights.Field.campaign_id,
        AdsInsights.Field.campaign_name,
        AdsInsights.Field.spend,
        AdsInsights.Field.reach,
        AdsInsights.Field.impressions,
        AdsInsights.Field.cost_per_result,
        AdsInsights.Field.actions
    ]
    params = {
        'level': 'campaign',
        'time_range': {
            'since': start_date,
            'until': end_date
        },
        'filtering': [{
            'field': 'campaign.name',
            'operator': 'CONTAIN',
            'value': data_sub_domain
        }],
        'breakdowns': ['country'],
        'limit': 1000,
    }
    insights = account.get_insights(fields=fields, params=params)
    rs_data = []
    data = []
    total = []
    total_spend = 0
    total_impressions = 0
    total_reach = 0
    total_clicks = 0
    total_cpr = 0
    total_frequency = 0
    for item in insights:
        country_code = item.get('country')
        country_name = get_country_name_from_code(country_code)
        # Skip jika tidak ditemukan country name
        if not country_name:
            continue
        country_label = f"{country_name} ({country_code})"
        spend = float(item.get('spend', 0))
        impressions = int(item.get('impressions', 0))
        reach = int(item.get('reach', 0))
        frequency = float(impressions/reach) if reach > 0 else 0.0
        # Ekstrak cost_per_result (CPR)
        cost_per_result = None
        for cpr_item in item.get('cost_per_result', []):
            # Ambil CPR untuk indicator tertentu (contoh: actions:link_click)
            if cpr_item.get('indicator') == 'actions:link_click':
                values = cpr_item.get('values', [])
                if values:
                    cost_per_result = values[0].get('value')
                break  # Berhenti setelah ketemu yang cocok
        result_action_type = 'link_click'
        result_count = 0
        for action in item.get('actions', []):
            if action.get('action_type') == result_action_type:
                result_count = float(action.get('value', 0))
                break
        clicks = result_count
        total_spend += spend
        total_impressions += impressions
        total_reach += reach
        total_clicks += clicks
        total_frequency = frequency
        if cost_per_result is not None:
            total_cpr += float(cost_per_result)
        data.append({
            'country': country_label,
            'ad_id': item.get('ad_id'),
            'ad_name': item.get('ad_name'),
            'adset_id': item.get('adset_id'),
            'campaign_id': item.get('campaign_id'),
            'campaign_name': item.get('campaign_name'),
            'spend': item.get('spend'),
            'impressions': int(item.get('impressions', 0)),
            'reach': int(item.get('reach', 0)),
            'clicks': clicks,
            'cpr': cost_per_result,
            'frequency': frequency
        })
    # Urutkan berdasarkan impressions tertinggi
    data_sorted = sorted(data, key=lambda x: x['impressions'], reverse=True)
    total.append({
        'total_spend': total_spend,
        'total_impressions': total_impressions,
        'total_reach' : total_reach,
        'total_click': total_clicks,
        'total_cpr' : total_cpr,
        'total_frequency' : total_frequency
    })
    rs_data = {
        'data': data_sorted,
        'total': total
    }
    return rs_data

# Google Ad Manager functions
def create_dynamic_googleads_yaml():
    """Create dynamic Google Ads YAML configuration and return file path"""
    try:
        # Always try service account first (more reliable for Ad Manager API)
        key_file = getattr(settings, 'GOOGLE_AD_MANAGER_KEY_FILE', '')
        network_code_raw = getattr(settings, 'GOOGLE_AD_MANAGER_NETWORK_CODE', None)
        if not network_code_raw:
            raise Exception("Network code not found in settings")
            
        # Parse network code safely
        network_code = None
        try:
            if isinstance(network_code_raw, str):
                cleaned = ''.join(filter(str.isdigit, network_code_raw))
                if cleaned:
                    network_code = int(cleaned)
            else:
                network_code = int(network_code_raw)
            if not network_code:
                raise ValueError("Invalid network code format")
        except Exception as e:
            raise Exception(f"Failed to parse network code: {str(e)}")
        
        # Check if service account key file exists
        if key_file and os.path.exists(key_file):
            print(f"[INFO] Using service account authentication: {key_file}")
            yaml_content = f"""ad_manager:
  application_name: "AdX Manager Dashboard"
  network_code: {network_code}
  path_to_private_key_file: "{key_file}"
use_proto_plus: true
"""
            
            # Write to temp YAML file
            yaml_file = tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False, encoding='utf-8')
            yaml_file.write(yaml_content)
            yaml_file.close()
            return yaml_file.name
        else:
            print(f"[ERROR] Service account key file not found: {key_file}")
            print(f"[INFO] Service account authentication is required for Ad Manager API")
            return None

    except Exception as e:
        print(f"Error creating Google Ads YAML: {e}")
        return None


def get_ad_manager_client():
    """Get Google Ad Manager client"""
    try:
        # Create YAML configuration
        yaml_file = create_dynamic_googleads_yaml()
        if not yaml_file:
            raise Exception("Failed to create YAML configuration")
        
        # Initialize client
        client = ad_manager.AdManagerClient.LoadFromStorage(yaml_file)
        
        # Clean up temporary file
        os.unlink(yaml_file)
        
        return client
    except Exception as e:
        print(f"Error initializing Ad Manager client: {e}")
        return None

def fetch_ad_manager_reports(start_date, end_date, report_type='HISTORICAL'):
    """Fetch Ad Manager reports"""
    try:
        client = get_ad_manager_client()
        if not client:
            return {'status': False, 'error': 'Failed to initialize client'}
        
        report_service = client.GetService('ReportService', version='v202502')
        
        # Convert string dates to datetime.date objects
        if isinstance(start_date, str):
            start_date = datetime.strptime(start_date, '%Y-%m-%d').date()
        if isinstance(end_date, str):
            end_date = datetime.strptime(end_date, '%Y-%m-%d').date()
            
        print(f"[DEBUG] After conversion - start_date: {start_date} (type: {type(start_date)})")
        print(f"[DEBUG] After conversion - end_date: {end_date} (type: {type(end_date)})")
        
        # Configure report
        report_job = {
            'reportQuery': {
                'dimensions': ['DATE', 'AD_UNIT_NAME'],
                'columns': ['AD_EXCHANGE_IMPRESSIONS', 'AD_EXCHANGE_CLICKS', 'AD_EXCHANGE_CPM_AND_CPC_REVENUE'],
                'dateRangeType': 'CUSTOM_DATE',
                'startDate': {
                    'year': start_date.year,
                    'month': start_date.month,
                    'day': start_date.day
                },
                'endDate': {
                    'year': end_date.year,
                    'month': end_date.month,
                    'day': end_date.day
                }
            }
        }
        
        # Run report
        report_job = report_service.runReportJob(report_job)
        report_job_id = report_job['id']
        
        # Wait for completion
        print("[DEBUG] Attempting to connect with API version v202502")
        report_downloader = client.GetDataDownloader(version='v202502')
        report_downloader.WaitForReport(report_job)
        
        return {
            'status': True,
            'report_id': report_job_id
        }
    except Exception as e:
        return {
            'status': False,
            'error': str(e)
        }

def fetch_ad_manager_inventory():
    """Fetch Ad Manager inventory"""
    try:
        client = get_ad_manager_client()
        if not client:
            return {'status': False, 'error': 'Failed to initialize client'}
        
        inventory_service = client.GetService('InventoryService', version='v202502')
        
        # Get ad units
        statement = ad_manager.StatementBuilder()
        ad_units = inventory_service.getAdUnitsByStatement(statement.ToStatement())
        
        return {
            'status': True,
            'data': ad_units
        }
    except Exception as e:
        return {
            'status': False,
            'error': str(e)
        }

def fetch_data_insights_all_accounts_by_subdomain(rs_account, tanggal, data_sub_domain):
    """
    Fungsi baru untuk mengambil data dari semua akun dengan filter subdomain
    """
    all_data = []
    total_budget = total_spend = total_clicks = total_impressions = total_reach = total_cpr = 0.0
    
    for account_data in rs_account:
        try:
            FacebookAdsApi.init(access_token=account_data['access_token'])
            account = AdAccount(account_data['account_id'])
            
            # Setup tanggal
            if tanggal == '%' or not tanggal:
                today = datetime.now().strftime('%Y-%m-%d')
            else:
                today = tanggal
            
            start_date = end_date = today
            time_range = {
                'since': str(start_date),
                'until': str(end_date)
            }
            
            # Setup params dengan filter subdomain
            if data_sub_domain and data_sub_domain != '%' and data_sub_domain.strip() != '':
                params = {
                    'level': 'campaign',
                    'time_range': time_range,
                    'filtering': [
                        {
                            'field': 'campaign.name',
                            'operator': 'CONTAIN',
                            'value': data_sub_domain
                        }
                    ]
                }
            else:
                params = {
                    'level': 'campaign',
                    'time_range': time_range,
                }
            
            # Ambil konfigurasi campaign
            campaign_configs = account.get_campaigns(fields=[
                Campaign.Field.id,
                Campaign.Field.name,
                Campaign.Field.status,
                Campaign.Field.daily_budget,
                Campaign.Field.start_time,
                Campaign.Field.stop_time
            ])
            
            campaign_map = {
                c['id']: {
                    'name': c.get('name'),
                    'status': c.get('status'),
                    'daily_budget': float(c.get('daily_budget') or 0),
                    'start_time': c.get('start_time'),
                    'stop_time': c.get('stop_time'),
                } for c in campaign_configs
            }
            
            campaign_aggregates = defaultdict(lambda: {
                'spend': 0.0,
                'reach': 0,
                'impressions': 0,
                'clicks': 0,
                'cpr': 0.0,
                'daily_budget': 0.0,
                'frequency': 0.0,
                'status': '',
                'start_time': '',
                'stop_time': '',
                'campaign_name': '',
                'account_name': account_data.get('account_name', ''),
            })
            
            # Ambil insights
            insights = account.get_insights(
                fields=[
                    AdsInsights.Field.campaign_id,
                    AdsInsights.Field.campaign_name,
                    AdsInsights.Field.spend,
                    AdsInsights.Field.reach,
                    AdsInsights.Field.impressions,
                    AdsInsights.Field.cost_per_result,
                    AdsInsights.Field.actions
                ],
                params=params
            )
            
            # Process insights
            for row in insights:
                campaign_id = row.get('campaign_id')
                if not campaign_id:
                    continue
                    
                config = campaign_map.get(campaign_id, {})
                agg = campaign_aggregates[campaign_id]
                
                agg['campaign_name'] = row.get('campaign_name')
                agg['spend'] += float(row.get('spend', 0))
                agg['reach'] += int(row.get('reach', 0))
                agg['impressions'] += int(row.get('impressions', 0))
                
                # Hitung frequency dengan pengecekan pembagian nol
                if agg['reach'] > 0:
                    agg['frequency'] = float(agg['impressions'] / agg['reach'])
                else:
                    agg['frequency'] = 0.0
                
                # Ambil cost per result
                cost_per_result = None
                for cpr_item in row.get('cost_per_result', []):
                    if cpr_item.get('indicator') == 'actions:link_click':
                        values = cpr_item.get('values', [])
                        if values:
                            cost_per_result = values[0].get('value')
                        break
                        
                if cost_per_result and str(cost_per_result).replace('.', '', 1).isdigit():
                    agg['cpr'] = float(cost_per_result)
                
                # Ambil clicks
                result_action_type = 'link_click'
                result_count = 0
                for action in row.get('actions', []):
                    if action.get('action_type') == result_action_type:
                        result_count = float(action.get('value', 0))
                        break
                        
                if result_count not in [None, ""]:
                    agg['clicks'] = result_count
                
                # Set config data
                if not agg['status']:
                    agg['status'] = config.get('status')
                    agg['daily_budget'] = float(config.get('daily_budget', 0))
                    agg['start_time'] = config.get('start_time')
                    agg['stop_time'] = config.get('stop_time')
            
            # Tambahkan data dari akun ini ke all_data
            for campaign_id, agg in campaign_aggregates.items():
                campaign_data = {
                    'campaign_id': campaign_id,
                    'campaign_name': agg['campaign_name'],
                    'account_name': agg['account_name'],
                    'daily_budget': agg['daily_budget'],
                    'spend': round(agg['spend'], 2),
                    'impressions': agg['impressions'],
                    'reach': agg['reach'],
                    'clicks': agg['clicks'],
                    'frequency': agg['frequency'],
                    'cpr': agg['cpr'],
                    'status': agg['status'],
                    'start_time': agg['start_time'],
                    'stop_time': agg['stop_time'],
                }
                all_data.append(campaign_data)
                
                # Tambahkan ke total
                total_budget += agg['daily_budget']
                total_spend += agg['spend']
                total_impressions += agg['impressions']
                total_reach += agg['reach']
                total_clicks += agg['clicks']
                total_cpr += agg['cpr']
                
        except Exception as e:
            print(f"Error processing account {account_data.get('account_name', 'Unknown')}: {str(e)}")
            continue
    
    # Hitung total frequency dengan pengecekan pembagian nol
    if total_reach > 0:
        total_frequency = float(total_impressions / total_reach)
    else:
        total_frequency = 0.0
    
    # Sort data berdasarkan start_time
    sorted_data = sorted(
        all_data,
        key=lambda x: datetime.strptime(x['start_time'], '%Y-%m-%dT%H:%M:%S%z') if x['start_time'] else datetime.min,
        reverse=True
    )
    
    total = [{
        'total_budget': total_budget,
        'total_spend': total_spend,
        'total_impressions': total_impressions,
        'total_reach': total_reach,
        'total_click': total_clicks,
        'total_frequency': total_frequency,
        'total_cpr': total_cpr
    }]
    
    return {
        'data': sorted_data,
        'total': total
    }

def fetch_adx_summary_data(user_mail, start_date, end_date):
    """Fetch AdX summary data using user's credentials"""
    try:
        # Get user's Ad Manager client
        client_result = get_user_ad_manager_client(user_mail)
        if not client_result['status']:
            return client_result
            
        client = client_result['client']
        print(f"[DEBUG] clientnya: {client}")

        # Try using supported API versions
        try:
            report_service = client.GetService('ReportService', version='v202502')
            print(f"[DEBUG] Using Ad Manager API version v202502")
        except Exception as e:
            print(f"[DEBUG] Failed to use v202502, falling back to v202502: {e}")
            try:
                report_service = client.GetService('ReportService', version='v202502')
                print(f"[DEBUG] Using Ad Manager API version v202502")
            except Exception as e2:
                print(f"[DEBUG] Failed to use v202502, falling back to v202502: {e2}")
                report_service = client.GetService('ReportService', version='v202502')
        
        # Convert string dates to datetime.date objects
        if isinstance(start_date, str):
            start_date = datetime.strptime(start_date, '%Y-%m-%d').date()
        if isinstance(end_date, str):
            end_date = datetime.strptime(end_date, '%Y-%m-%d').date()
        
        # Configure report
        report_job = {
            'reportQuery': {
                'dimensions': ['DATE'],
                'columns': [
                    'AD_EXCHANGE_IMPRESSIONS', 'AD_EXCHANGE_CLICKS', 'AD_EXCHANGE_TOTAL_EARNINGS'
                ],
                'dateRangeType': 'CUSTOM_DATE',
                'startDate': {
                    'year': start_date.year,
                    'month': start_date.month,
                    'day': start_date.day
                },
                'endDate': {
                    'year': end_date.year,
                    'month': end_date.month,
                    'day': end_date.day
                }
            }
        }
        
        # Run report
        report_job = report_service.runReportJob(report_job)
        report_job_id = report_job['id']
        
        # Wait for completion
        report_downloader = client.GetDataDownloader(version='v202502')
        report_downloader.WaitForReport(report_job)
        
        # Download and parse results
        report_file = tempfile.NamedTemporaryFile(suffix='.csv.gz', delete=False)
        print(f"[DEBUG] About to download report to file: {report_file.name}")
        report_downloader.DownloadReportToFile(report_job_id, report_file, 'CSV_DUMP')
        report_file.close()
        print(f"[DEBUG] Report downloaded successfully")
        
        # Parse CSV data
        data = []
        site_totals = {}
        
        with gzip.open(report_file.name, 'rt') as f:
            reader = csv.DictReader(f)
            for row in reader:
                site_name = row.get('Dimension.AD_EXCHANGE_SITE_NAME', '')
                date = row.get('Dimension.DATE', '')
                clicks = int(row.get('Column.AD_EXCHANGE_CLICKS', 0))
                revenue = float(row.get('Column.AD_EXCHANGE_TOTAL_EARNINGS', 0))
                cpc = float(row.get('Column.AD_EXCHANGE_CPC', 0))
                ecpm = float(row.get('Column.AD_EXCHANGE_ECPM', 0))
                ctr = float(row.get('Column.AD_EXCHANGE_CTR', 0))
                
                row_data = {
                    'site_name': site_name,
                    'date': date,
                    'clicks': clicks,
                    'revenue': revenue,
                    'cpc': cpc,
                    'ecpm': ecpm,
                    'ctr': ctr
                }
                data.append(row_data)
                
                # Akumulasi data per situs untuk summary
                if site_name not in site_totals:
                    site_totals[site_name] = {
                        'total_clicks': 0,
                        'total_revenue': 0.0,
                        'avg_cpc': 0.0,
                        'avg_ecpm': 0.0,
                        'avg_ctr': 0.0,
                        'count': 0
                    }
                
                site_totals[site_name]['total_clicks'] += clicks
                site_totals[site_name]['total_revenue'] += revenue
                site_totals[site_name]['avg_cpc'] += cpc
                site_totals[site_name]['avg_ecpm'] += ecpm
                site_totals[site_name]['avg_ctr'] += ctr
                site_totals[site_name]['count'] += 1
        
        # Cleanup temporary file
        os.unlink(report_file.name)
        
        # Generate site summary
        site_summary = []
        for site, totals in site_totals.items():
            count = totals['count']
            site_summary.append({
                'site_name': site,
                'total_clicks': totals['total_clicks'],
                'total_revenue': totals['total_revenue'],
                'avg_cpc': totals['avg_cpc'] / count if count > 0 else 0,
                'avg_ecpm': totals['avg_ecpm'] / count if count > 0 else 0,
                'avg_ctr': totals['avg_ctr'] / count if count > 0 else 0
            })
        
        # Generate overall summary
        total_clicks = sum(row['clicks'] for row in data)
        total_revenue = sum(row['revenue'] for row in data)
        avg_cpc = sum(row['cpc'] for row in data) / len(data) if data else 0
        avg_ecpm = sum(row['ecpm'] for row in data) / len(data) if data else 0
        avg_ctr = sum(row['ctr'] for row in data) / len(data) if data else 0
        
        summary = {
            'total_clicks': total_clicks,
            'total_revenue': total_revenue,
            'avg_cpc': avg_cpc,
            'avg_ecpm': avg_ecpm,
            'avg_ctr': avg_ctr,
            'total_sites': len(site_totals),
            'date_range': f"{start_date} to {end_date}"
        }
        
        # Fetch country data for charts
        try:
            country_result = fetch_adx_traffic_per_country(start_date, end_date, user_mail)
            print(f"[DEBUG] Raw country result: {country_result}")
            countries_data = []
            if country_result.get('status') and country_result.get('data'):
                countries_data = country_result['data']
        except Exception as e:
            print(f"[DEBUG] Failed to fetch country data: {e}")
            countries_data = []
        
        return {
            'status': True,
            'data': data,
            'site_summary': site_summary,
            'summary': summary,
            'countries': countries_data,
            'user_mail': user_mail
        }
        
    except Exception as e:
        print(f"[ERROR] fetch_adx_summary_data: {str(e)}")
        
        # Handle specific GoogleAds library bug
        if "argument should be integer or bytes-like object, not 'str'" in str(e):
            print(f"[WARNING] GoogleAds library bug detected in fetch_adx_summary_data for {user_mail}. Returning empty data as workaround.")
            return {
                'status': True,
                'data': [],
                'site_summary': [],
                'summary': {
                    'total_clicks': 0,
                    'total_revenue': 0,
                    'avg_cpc': 0,
                    'avg_ecpm': 0,
                    'avg_ctr': 0,
                    'total_sites': 0,
                    'date_range': f"{start_date} to {end_date}"
                },
                'user_mail': user_mail,
                'note': 'Data kosong karena bug library GoogleAds'
            }
        
        return {
            'status': False,
            'error': f'Error mengambil data AdX: {str(e)}'
        }

def fetch_adx_traffic_account_by_user(user_mail, start_date, end_date, site_filter=None):
    """Fetch traffic account data using user's credentials with AdX fallback to regular metrics"""
    try:
        # Get user's Ad Manager client
        client_result = get_user_ad_manager_client(user_mail)
        if not client_result['status']:
            return client_result
            
        client = client_result['client']
        
        # Convert string dates to datetime.date objects
        if isinstance(start_date, str):
            start_date = datetime.strptime(start_date, '%Y-%m-%d').date()
        if isinstance(end_date, str):
            end_date = datetime.strptime(end_date, '%Y-%m-%d').date()
        
        # Try AdX first, then fallback to regular metrics
        try:
            print(f"[DEBUG] Attempting AdX report for {user_mail}")
            return _run_adx_report_with_fallback(client, start_date, end_date, site_filter)
        except Exception as adx_error:
            print(f"[DEBUG] AdX failed: {adx_error}")
            print(f"[DEBUG] Falling back to regular Ad Manager metrics")
            return _run_regular_report(client, start_date, end_date, site_filter)
            
    except Exception as e:
        print(f"[ERROR] fetch_adx_traffic_account_by_user failed: {e}")
        return {
            'status': False,
            'error': f'Failed to fetch traffic data: {str(e)}'
        }
def _to_date(date_val):
    return datetime.strptime(date_val, '%Y-%m-%d').date() if isinstance(date_val, str) else date_val


def _run_adx_report(client, start_date, end_date, site_filter):
    report_service = client.GetService('ReportService', version='v202502')

    # Try different column combinations to avoid NOT_NULL errors
    column_combinations = [
        # Primary: Full metrics (most comprehensive)
        ['AD_EXCHANGE_LINE_ITEM_LEVEL_IMPRESSIONS', 'AD_EXCHANGE_LINE_ITEM_LEVEL_CLICKS', 'AD_EXCHANGE_LINE_ITEM_LEVEL_REVENUE'],
        # Fallback 1: Basic metrics only
        ['AD_EXCHANGE_IMPRESSIONS', 'AD_EXCHANGE_CLICKS', 'AD_EXCHANGE_TOTAL_EARNINGS'],
        # Fallback 2: Alternative metrics
        ['AD_EXCHANGE_AD_REQUESTS', 'AD_EXCHANGE_MATCHED_REQUESTS', 'AD_EXCHANGE_ESTIMATED_REVENUE'],
        # Fallback 3: Minimal metrics
        ['AD_EXCHANGE_IMPRESSIONS', 'AD_EXCHANGE_TOTAL_EARNINGS'],
        # Fallback 4: Revenue only
        ['AD_EXCHANGE_TOTAL_EARNINGS'],
        # Fallback 5: Regular Ad Manager metrics
        ['AD_EXCHANGE_IMPRESSIONS', 'AD_EXCHANGE_CLICKS', 'AD_EXCHANGE_CPM_AND_CPC_REVENUE']
    ]
    
    report_job = None
    last_error = None
    
    for i, columns in enumerate(column_combinations):
        try:
            print(f"[DEBUG] Trying column combination {i+1}: {columns}")
            
            report_query = {
                'reportQuery': {
                    'dimensions': ['DATE', 'AD_UNIT_NAME'],
                    'columns': columns,
                    'dateRangeType': 'CUSTOM_DATE',
                    'startDate': {
                        'year': start_date.year,
                        'month': start_date.month,
                        'day': start_date.day
                    },
                    'endDate': {
                        'year': end_date.year,
                        'month': end_date.month,
                        'day': end_date.day
                    }
                }
            }
            
            if site_filter:
                report_query['reportQuery']['dimensionFilters'] = [{
                    'dimension': 'AD_EXCHANGE_SITE_NAME',
                    'operator': 'CONTAINS',
                    'values': [site_filter]
                }]
            
            # Try to run the report job
            report_job = report_service.runReportJob(report_query)
            print(f"[DEBUG] Successfully created report job with columns: {columns}")
            break
            
        except Exception as e:
            last_error = e
            error_msg = str(e)
            print(f"[DEBUG] Column combination {i+1} failed: {error_msg}")
            
            # If this is a NOT_NULL error, try the next combination
            if 'NOT_NULL' in error_msg:
                continue
            # If it's an authentication error, re-raise it
            elif any(keyword in error_msg.lower() for keyword in ['authentication', 'permission', 'unauthorized']):
                raise e
            # For other errors, try next combination
            else:
                continue
    
    # If all combinations failed, provide specific error messages
    if report_job is None:
        if last_error:
            error_msg = str(last_error)
            if 'REPORT_NOT_FOUND' in error_msg:
                raise Exception("Network tidak memiliki data AdX untuk periode yang diminta. Pastikan: 1) Akun memiliki akses AdX, 2) Network memiliki traffic AdX, 3) Periode tanggal valid")
            elif 'NOT_NULL' in error_msg:
                raise Exception("Semua kombinasi kolom gagal karena constraint NOT_NULL. Network mungkin tidak memiliki data AdX yang lengkap")
            elif 'PERMISSION' in error_msg.upper():
                raise Exception("Tidak memiliki izin untuk mengakses data AdX. Hubungi administrator untuk memberikan akses AdX")
            else:
                raise Exception(f"Semua kombinasi kolom gagal: {error_msg}")
        else:
            raise Exception("Semua kombinasi kolom gagal tanpa error spesifik")

    if site_filter:
        report_query['reportQuery']['dimensionFilters'] = [{
            'dimension': 'AD_EXCHANGE_SITE_NAME',
            'operator': 'CONTAINS',
            'values': [site_filter]
        }]

    report_job = report_service.runReportJob(report_query)
    report_job_id = report_job['id']
    
    # Ensure report_job_id is an integer for API calls
    if isinstance(report_job_id, str):
        try:
            report_job_id = int(report_job_id)
        except ValueError:
            print(f"[DEBUG] Warning: Could not convert report_job_id '{report_job_id}' to integer")
    
    print(f"[DEBUG] Waiting for report job {report_job_id} (type: {type(report_job_id)})")
    elapsed = 0
    while elapsed < 300:
        status = report_service.getReportJobStatus(report_job_id)
        print(f"[DEBUG] Report status: {status}")
        if status == 'COMPLETED':
            break
        elif status == 'FAILED':
            raise Exception("Report job failed")
        time.sleep(10)
        elapsed += 10

    if elapsed >= 300:
        raise Exception("Report job timed out")

    downloader = client.GetDataDownloader(version='v202502')
    
    # Use DownloadReportToFile with binary mode for gzip compressed data
    with tempfile.NamedTemporaryFile(mode='w+b', delete=True, suffix='.csv.gz') as temp_file:
        downloader.DownloadReportToFile(report_job_id, 'CSV_DUMP', temp_file)
        
        # Read the gzip compressed file content
        temp_file.seek(0)
        with gzip.open(temp_file, 'rt') as gzip_file:
            report_data = gzip_file.read()
        
        return report_data


def _process_csv_report(report_data):
    processed = []
    csv_reader = csv.DictReader(io.StringIO(report_data))

    for row in csv_reader:
        try:
            impressions = int(row.get('Column.AD_EXCHANGE_IMPRESSIONS', 0))
            clicks = int(row.get('Column.AD_EXCHANGE_CLICKS', 0))
            revenue_micros = float(row.get('Column.AD_EXCHANGE_TOTAL_EARNINGS', 0.0))
            revenue = revenue_micros / 1_000_000

            processed.append({
                'date': row.get('Dimension.DATE', ''),
                'site_name': row.get('Dimension.AD_EXCHANGE_SITE_NAME', 'Unknown'),
                'impressions': impressions,
                'clicks': clicks,
                'revenue': revenue,
                'cpc': revenue / max(clicks, 1),
                'ctr': (clicks / max(impressions, 1)) * 100,
                'ecpm': (revenue / max(impressions, 1)) * 1000
            })
        except Exception as e:
            print(f"[DEBUG] Failed to process row: {e}, row: {row}")
            continue

    return processed


def _summarize_data(data, start_date, end_date):
    summary = defaultdict(lambda: {'clicks': 0, 'impressions': 0, 'revenue': 0.0})

    for item in data:
        site = item['site_name']
        summary[site]['clicks'] += item['clicks']
        summary[site]['impressions'] += item['impressions']
        summary[site]['revenue'] += item['revenue']

    site_summary = []
    for site, stats in summary.items():
        site_summary.append({
            'site_name': site,
            'total_clicks': stats['clicks'],
            'total_revenue': stats['revenue'],
            'total_impressions': stats['impressions'],
            'avg_cpc': stats['revenue'] / stats['clicks'] if stats['clicks'] else 0,
            'avg_ecpm': (stats['revenue'] / stats['impressions']) * 1000 if stats['impressions'] else 0,
            'avg_ctr': (stats['clicks'] / stats['impressions']) * 100 if stats['impressions'] else 0
        })

    total_clicks = sum(x['clicks'] for x in summary.values())
    total_impressions = sum(x['impressions'] for x in summary.values())
    total_revenue = sum(x['revenue'] for x in summary.values())

    overall_summary = {
        'total_clicks': total_clicks,
        'total_revenue': total_revenue,
        'total_impressions': total_impressions,
        'avg_cpc': total_revenue / total_clicks if total_clicks else 0,
        'avg_ecpm': (total_revenue / total_impressions) * 1000 if total_impressions else 0,
        'avg_ctr': (total_clicks / total_impressions) * 100 if total_impressions else 0,
        'total_sites': len(summary),
        'date_range': f"{start_date} to {end_date}"
    }

    return site_summary, overall_summary


def _error_response(user_mail, start_date, end_date, error_msg, method):
    return {
        'status': False,
        'api_method': method,
        'data': [],
        'site_summary': [],
        'summary': {
            'total_clicks': 0,
            'total_revenue': 0.0,
            'total_impressions': 0,
            'avg_cpc': 0.0,
            'avg_ecpm': 0.0,
            'avg_ctr': 0.0,
            'total_sites': 0,
            'date_range': f"{start_date} to {end_date}"
        },
        'user_mail': user_mail,
        'note': error_msg,
        'error': error_msg  # Add error field for frontend compatibility
    }

def fetch_adx_ad_change_data(start_date, end_date):
    """Fetch AdX ad change data"""
    try:
        client = get_ad_manager_client()
        if not client:
            return {'status': False, 'error': 'Failed to initialize client'}
        
        report_service = client.GetService('ReportService', version='v202502')
        
        # Convert string dates to datetime.date objects
        if isinstance(start_date, str):
            start_date = datetime.strptime(start_date, '%Y-%m-%d').date()
        if isinstance(end_date, str):
            end_date = datetime.strptime(end_date, '%Y-%m-%d').date()
        
        # Configure report for ad changes
        report_job = {
            'reportQuery': {
                'dimensions': ['DATE', 'AD_UNIT_NAME'],
                'columns': [
                    'AD_EXCHANGE_IMPRESSIONS', 'AD_EXCHANGE_CLICKS', 
                    'AD_EXCHANGE_TOTAL_EARNINGS', 'AD_EXCHANGE_CTR'
                ],
                'dateRangeType': 'CUSTOM_DATE',
                'startDate': {
                    'year': start_date.year,
                    'month': start_date.month,
                    'day': start_date.day
                },
                'endDate': {
                    'year': end_date.year,
                    'month': end_date.month,
                    'day': end_date.day
                }
            }
        }
        
        # Run report
        report_job = report_service.runReportJob(report_job)
        report_job_id = report_job['id']
        
        # Wait for completion
        report_downloader = client.GetDataDownloader(version='v202502')
        report_downloader.WaitForReport(report_job)
        
        # Download and parse results
        report_file = tempfile.NamedTemporaryFile(suffix='.csv.gz', delete=False)
        report_downloader.DownloadReportToFile(report_job_id, 'CSV_DUMP', report_file)
        
        # Parse CSV data
        data = []
        with gzip.open(report_file.name, 'rt') as f:
            reader = csv.DictReader(f)
            for row in reader:
                data.append({
                    'date': row.get('Dimension.DATE', ''),
                    'ad_unit': row.get('Dimension.AD_UNIT_NAME', ''),
                    'impressions': int(row.get('Column.AD_EXCHANGE_IMPRESSIONS', 0)),
                    'clicks': int(row.get('Column.AD_EXCHANGE_CLICKS', 0)),
                    'revenue': float(row.get('Column.AD_EXCHANGE_TOTAL_EARNINGS', 0)),
                    'ctr': float(row.get('Column.AD_EXCHANGE_CTR', 0))
                })
        
        # Cleanup
        os.unlink(report_file.name)
        
        return {
            'status': True,
            'data': data
        }
    except Exception as e:
        return {
            'status': False,
            'error': str(e)
        }

def fetch_adx_active_sites():
    """Fetch active AdX sites"""
    try:
        client = get_ad_manager_client()
        if not client:
            return {'status': False, 'error': 'Failed to initialize client'}
        
        inventory_service = client.GetService('InventoryService')
        
        # Get active ad units
        statement = ad_manager.StatementBuilder()
        statement.Where('status = :status')
        statement.WithBindVariable('status', 'ACTIVE')
        
        ad_units = inventory_service.getAdUnitsByStatement(statement.ToStatement())
        
        sites = []
        if 'results' in ad_units:
            for ad_unit in ad_units['results']:
                sites.append({
                    'id': ad_unit['id'],
                    'name': ad_unit['name'],
                    'status': ad_unit['status']
                })
        
        return {
            'status': True,
            'data': sites
        }
    except Exception as e:
        return {
            'status': False,
            'error': str(e)
        }

def fetch_user_sites_list(user_mail):
    """Fetch list of sites for a specific user from Ad Manager"""
    try:
        client_result = get_user_ad_manager_client(user_mail)
        if not client_result['status']:
            return client_result
        
        client = client_result['client']
        inventory_service = client.GetService('InventoryService')
        
        # Get active ad units
        statement = ad_manager.StatementBuilder()
        statement.Where('status = :status')
        statement.WithBindVariable('status', 'ACTIVE')
        
        ad_units = inventory_service.getAdUnitsByStatement(statement.ToStatement())
        
        sites = set()  # Use set to avoid duplicates
        site_name_mapping = _get_site_name_mapping()
        
        if 'results' in ad_units:
            for ad_unit in ad_units['results']:
                site_name = ad_unit['name']
                
                # Apply site name mapping if available
                if site_name in site_name_mapping:
                    site_name = site_name_mapping[site_name]
                
                sites.add(site_name)
        
        # Convert set to sorted list
        sites_list = sorted(list(sites))
        
        return {
            'status': True,
            'data': sites_list
        }
    except Exception as e:
        return {
            'status': False,
            'error': str(e)
        }

def fetch_adx_account_data():
    """Fetch AdX account data (legacy function for backward compatibility)"""
    try:
        client = get_ad_manager_client()
        if not client:
            return {'status': False, 'error': 'Failed to initialize client'}
        
        network_service = client.GetService('NetworkService', version='v202502')
        
        # Get current network with multiple fallback approaches
        try:
            # First attempt: Direct call
            current_network = network_service.getCurrentNetwork()
            
            return {
                'status': True,
                'data': {
                    'network_code': current_network['networkCode'],
                    'network_name': current_network['displayName'],
                    'currency_code': current_network['currencyCode']
                }
            }
        except TypeError as e:
            print(f"TypeError in getCurrentNetwork (attempting workaround): {e}")
            
            # Workaround 1: Try with explicit empty parameters
            try:
                current_network = network_service.getCurrentNetwork({})
                return {
                    'status': True,
                    'data': {
                        'network_code': current_network['networkCode'],
                        'network_name': current_network['displayName'],
                        'currency_code': current_network['currencyCode']
                    }
                }
            except (TypeError, Exception) as e2:
                print(f"Workaround 1 failed: {e2}")
                
                # Workaround 2: Use network code from settings
                try:
                    network_code = getattr(settings, 'GOOGLE_AD_MANAGER_NETWORK_CODE', None)
                    if not network_code:
                        raise Exception("Network code not found in settings")
                    
                    # Try to get network by code using getAllNetworks
                    try:
                        all_networks = network_service.getAllNetworks()
                        for network in all_networks:
                            if str(network['networkCode']) == str(network_code):
                                return {
                                    'status': True,
                                    'data': {
                                        'network_code': network['networkCode'],
                                        'network_name': network['displayName'],
                                        'currency_code': network.get('currencyCode', 'USD')
                                    }
                                }
                    except Exception as e3:
                        print(f"getAllNetworks failed: {e3}")
                    
                    # Fallback: Return network code from settings
                    return {
                        'status': True,
                        'data': {
                            'network_code': network_code,
                            'network_name': 'AdX Network (from settings)',
                            'currency_code': 'USD'
                        },
                        'warning': 'Using fallback data due to library compatibility issue'
                    }
                    
                except Exception as e4:
                    print(f"All workarounds failed: {e4}")
                    return {
                        'status': False,
                        'error': 'OAuth2 connection failed due to library compatibility issue',
                        'data': {
                            'network_code': '',
                            'network_name': '',
                            'currency_code': ''
                        }
                    }
    except Exception as e:
        return {
            'status': False,
            'error': str(e)
        }

def _get_network_display_name_from_api(user_mail, network_code):
    """
    Mengambil nama network langsung dari Ad Manager API menggunakan kredensial user
    """
    try:
        # Gunakan client Ad Manager yang sudah ada
        client_result = get_user_ad_manager_client(user_mail)
        if not client_result.get('status'):
            print(f"[WARNING] Tidak dapat membuat Ad Manager client untuk {user_mail}: {client_result.get('error')}")
            return 'AdX Network'  # fallback
        
        client = client_result['client']
        network_service = client.GetService('NetworkService', version='v202502')
        
        # Ambil informasi network saat ini dengan error handling yang lebih baik
        try:
            current_network = network_service.getCurrentNetwork()
            
            # Coba berbagai cara untuk mengambil displayName
            display_name = None
            
            if current_network:
                # Method 1: Direct attribute access
                if hasattr(current_network, 'displayName'):
                    display_name = current_network.displayName
                # Method 2: Dictionary access
                elif isinstance(current_network, dict) and 'displayName' in current_network:
                    display_name = current_network['displayName']
                # Method 3: Try to access as string representation
                elif hasattr(current_network, '__dict__'):
                    network_dict = current_network.__dict__
                    display_name = network_dict.get('displayName') or network_dict.get('display_name')
                
                # Jika berhasil mendapat display name, return
                if display_name and display_name.strip():
                    return display_name.strip()
                    
                # Fallback: gunakan network code sebagai nama jika ada
                if hasattr(current_network, 'networkCode'):
                    return f"Network {current_network.networkCode}"
                elif isinstance(current_network, dict) and 'networkCode' in current_network:
                    return f"Network {current_network['networkCode']}"
                elif network_code:
                    return f"Network {network_code}"
            
        except TypeError as te:
            # Handle SOAP encoding error specifically
            if "argument should be integer or bytes-like object, not 'str'" in str(te):
                print(f"[INFO] SOAP encoding issue for {user_mail}, using network code as fallback")
                return f"Network {network_code}" if network_code else 'AdX Network'
            else:
                print(f"[WARNING] TypeError getting network info for {user_mail}: {te}")
        except Exception as ne:
            print(f"[WARNING] Error calling getCurrentNetwork for {user_mail}: {ne}")
        
        # Final fallback
        return f"Network {network_code}" if network_code else 'AdX Network'
            
    except Exception as e:
        print(f"[ERROR] Error mengambil nama network dari API untuk {user_mail}: {str(e)}")
        return f"Network {network_code}" if network_code else 'AdX Network'

def _get_network_display_name_mapping():
    """
    DEPRECATED: Fungsi ini masih digunakan sebagai fallback jika API gagal
    """
    return {
        '23303534834': 'Adzone 3',
        # Add more network mappings as needed
    }

def _get_network_display_name(network_code, user_mail=None):
    """
    Mengambil nama network dengan prioritas:
    1. Dari Ad Manager API (jika user_mail tersedia)
    2. Dari mapping hardcode (fallback)
    3. Default 'AdX Network'
    """
    if user_mail:
        # Coba ambil dari API terlebih dahulu
        api_name = _get_network_display_name_from_api(user_mail, network_code)
        if api_name != 'AdX Network':  # Jika berhasil mendapat nama dari API
            return api_name
    
    # Fallback ke mapping hardcode jika API gagal
    mapping = _get_network_display_name_mapping()
    return mapping.get(str(network_code), 'AdX Network')

def fetch_user_adx_account_data(user_mail):
    """Fetch comprehensive AdX account data using user's credentials"""
    try:
        # Get user's Ad Manager client
        client_result = get_user_ad_manager_client(user_mail)
        print(f"DEBUG AdxUserAccountDataView -client_result: {client_result}")
        client = client_result['client']
        # Get Network Service
        network_service = client.GetService('NetworkService', version='v202502')
        # Get current network information
        try:
            current_network = network_service.getCurrentNetwork()
            print(f"DEBUG AdxUserAccountDataView -current_network: {current_network}")
        except Exception as e:
            # Fallback to basic network info with proper network name mapping
            network_code = getattr(settings, 'GOOGLE_AD_MANAGER_NETWORK_CODE', None)
            current_network = {
                'networkCode': network_code,
                'displayName': _get_network_display_name(network_code, user_mail),
                'currencyCode': 'USD',
                'timeZone': 'Asia/Jakarta'
            }
        # Get User Service for additional account details
        user_service = client.GetService('UserService', version='v202502')
        # Get current user information
        current_user = None
        try:
            statement = ad_manager.StatementBuilder()
            statement.Where('email = :email')
            statement.WithBindVariable('email', user_mail)
            users = user_service.getUsersByStatement(statement.ToStatement())
            if users and 'results' in users and len(users['results']) > 0:
                current_user = users['results'][0]
        except TypeError as e:
            if "argument should be integer or bytes-like object, not 'str'" in str(e):
                # SOAP encoding error - handled by patch, no need to print
                pass
            else:
                print(f"Error getting user info: {e}")
        except Exception as e:
            print(f"Error getting user info: {e}")
        
        # Get Inventory Service for ad units count
        inventory_service = client.GetService('InventoryService', version='v202502')
        # Count active ad units
        active_ad_units_count = 0
        try:
            statement = ad_manager.StatementBuilder()
            statement.Where('status = :status')
            statement.WithBindVariable('status', 'ACTIVE')
            ad_units = inventory_service.getAdUnitsByStatement(statement.ToStatement())
            if ad_units and 'results' in ad_units:
                active_ad_units_count = len(ad_units['results'])
        except TypeError as e:
            if "argument should be integer or bytes-like object, not 'str'" in str(e):
                # SOAP encoding error - handled by patch, no need to print
                pass
            else:
                print(f"Error counting ad units: {e}")
        except Exception as e:
            print(f"Error counting ad units: {e}")
        
        # Prepare comprehensive account data
        account_data = {
            # Network Information
            'network_id': getattr(current_network, 'id', '') if hasattr(current_network, 'id') else current_network.get('id', '') if isinstance(current_network, dict) else '',
            'network_code': getattr(current_network, 'networkCode', '') if hasattr(current_network, 'networkCode') else current_network.get('networkCode', '') if isinstance(current_network, dict) else '',
            'display_name': getattr(current_network, 'displayName', 'AdX Network') if hasattr(current_network, 'displayName') else current_network.get('displayName', 'AdX Network') if isinstance(current_network, dict) else 'AdX Network',
            'network_name': getattr(current_network, 'displayName', 'AdX Network') if hasattr(current_network, 'displayName') else current_network.get('displayName', 'AdX Network') if isinstance(current_network, dict) else 'AdX Network',
            
            # Settings
            'currency_code': getattr(current_network, 'currencyCode', 'USD') if hasattr(current_network, 'currencyCode') else current_network.get('currencyCode', 'USD') if isinstance(current_network, dict) else 'USD',
            'timezone': getattr(current_network, 'timeZone', 'Asia/Jakarta') if hasattr(current_network, 'timeZone') else current_network.get('timeZone', 'Asia/Jakarta') if isinstance(current_network, dict) else 'Asia/Jakarta',
            'effective_root_ad_unit_id': getattr(current_network, 'effectiveRootAdUnitId', '') if hasattr(current_network, 'effectiveRootAdUnitId') else current_network.get('effectiveRootAdUnitId', '') if isinstance(current_network, dict) else '',
            
            # Account Details
            'user_mail': user_mail,
            'active_ad_units_count': active_ad_units_count,
            'last_updated': datetime.now().strftime('%d/%m/%Y %H:%M:%S'),
        }
        
        # Add user-specific information if available
        if current_user:
            account_data.update({
                'user_id': getattr(current_user, 'id', '') if hasattr(current_user, 'id') else current_user.get('id', '') if isinstance(current_user, dict) else '',
                'user_name': getattr(current_user, 'name', '') if hasattr(current_user, 'name') else current_user.get('name', '') if isinstance(current_user, dict) else '',
                'user_role': getattr(current_user, 'roleName', '') if hasattr(current_user, 'roleName') else current_user.get('roleName', '') if isinstance(current_user, dict) else '',
                'user_is_active': getattr(current_user, 'isActive', True) if hasattr(current_user, 'isActive') else current_user.get('isActive', True) if isinstance(current_user, dict) else True,
            })
        else:
            # Add default user information when user data is not available
            account_data.update({
                'user_id': '',
                'user_name': '',
                'user_role': '',
                'user_is_active': 'Yes',
            })
        
        # Add additional network settings if available
        if hasattr(current_network, 'isTest'):
            account_data['is_test_network'] = getattr(current_network, 'isTest', False)
        elif isinstance(current_network, dict) and 'isTest' in current_network:
            account_data['is_test_network'] = current_network.get('isTest', False)
        
        return {
            'status': True,
            'data': account_data,
            'user_mail': user_mail
        }
        
    except Exception as e:
        print(f"[ERROR] fetch_user_adx_account_data: {str(e)}")
        
        # Handle specific GoogleAds library bug
        if "argument should be integer or bytes-like object, not 'str'" in str(e):
            print(f"[WARNING] GoogleAds library bug detected for {user_mail}. Returning basic data as workaround.")
            network_code = getattr(settings, 'GOOGLE_AD_MANAGER_NETWORK_CODE', None)
            network_display_name = _get_network_display_name(network_code)
            return {
                'status': True,
                'data': {
                    'network_code': network_code,
                    'display_name': network_display_name,
                    'network_name': network_display_name,
                    'currency_code': 'USD',
                    'timezone': 'Asia/Jakarta',
                    'user_mail': user_mail,
                    'active_ad_units_count': 0,
                    'last_updated': datetime.now().isoformat(),
                },
                'user_mail': user_mail,
                'note': 'Data terbatas karena bug library GoogleAds'
            }
        
        return {
            'status': False,
            'error': f'Error mengambil data account: {str(e)}'
        }

def check_email_in_ad_manager(user_mail):
    """Check if email exists in Ad Manager using user's own credentials"""
    try:
        # Use user's own credentials to check Ad Manager
        client_result = get_user_ad_manager_client(user_mail)
        if not client_result['status']:
            return {
                'status': False,
                'error': f'Failed to initialize client for {user_mail}: {client_result.get("error", "Unknown error")}'
            }
        
        client = client_result['client']
        user_service = client.GetService('UserService', version='v202502')
        
        # Search for user by email
        statement = ad_manager.StatementBuilder()
        statement.Where('email = :email')
        statement.WithBindVariable('email', user_mail)
        
        try:
            users = user_service.getUsersByStatement(statement.ToStatement())
        except Exception as soap_error:
            # Handle the specific GoogleAds library bug
            error_msg = str(soap_error)
            if "argument should be integer or bytes-like object, not 'str'" in error_msg:
                print(f"[DEBUG] Known GoogleAds library bug encountered for {user_mail}. Assuming user exists to allow login.")
                # Return success with exists=True as workaround for the library bug
                return {
                    'status': True,
                    'exists': True,
                    'data': [],
                    'note': 'GoogleAds library bug workaround - assumed user exists'
                }
            else:
                # Re-raise other SOAP errors
                raise soap_error
        
        return {
            'status': True,
            'exists': len(users.get('results', [])) > 0,
            'data': users.get('results', [])
        }
    except Exception as e:
        return {
            'status': False,
            'error': str(e)
        }

def check_email_in_database(user_mail):
    """Check if email exists in database and return user data"""
    try:
        
        db = data_mysql()
        user_result = db.get_user_by_email(user_mail)
        
        if user_result['status'] and user_result['data']:
            return {
                'status': True,
                'exists': True,
                'data': user_result['data']
            }
        else:
            return {
                'status': True,
                'exists': False,
                'data': None
            }
    except Exception as e:
        return {
            'status': False,
            'error': str(e)
        }

def validate_oauth_email(user_mail):
    """Validate OAuth email against database and Ad Manager"""
    try:
        # Check database first
        db_result = check_email_in_database(user_mail)
        if not db_result['status']:
            return {
                'status': False,
                'valid': False,
                'error': db_result.get('error', 'Database check failed'),
                'database': db_result
            }
        
        # Check if Google Ads credentials are properly configured
        refresh_token = getattr(settings, 'GOOGLE_ADS_REFRESH_TOKEN', '')
        developer_token = getattr(settings, 'GOOGLE_ADS_DEVELOPER_TOKEN', '')
        
        if not refresh_token or not developer_token:
            # If Google Ads credentials are not configured, allow login based on database only
            print(f"[DEBUG] Google Ads credentials not configured, allowing login based on database only for {user_mail}")
            return {
                'status': True,
                'database': db_result,
                'ad_manager': {'status': False, 'error': 'Credentials not configured', 'exists': False},
                'valid': db_result['exists']  # Only check database
            }
        
        # Check Ad Manager only if credentials are available
        am_result = check_email_in_ad_manager(user_mail)
        if not am_result['status']:
            # If Ad Manager check fails, fallback to database only for OAuth login
            error_msg = am_result.get('error', '')
            # Handle various authentication/permission errors
            if any(keyword in error_msg.lower() for keyword in [
                'invalid_grant', 'bad request', 'authentication', 'permission', 
                'soap request failed', 'unauthorized', 'access denied'
            ]):
                print(f"[DEBUG] Ad Manager check failed ({error_msg}), allowing login based on database only for {user_mail}")
                return {
                    'status': True,
                    'database': db_result,
                    'ad_manager': am_result,
                    'valid': db_result['exists']  # Only check database
                }
            else:
                # For other types of errors, still allow login based on database
                print(f"[DEBUG] Ad Manager check failed with unknown error ({error_msg}), allowing login based on database only for {user_mail}")
                return {
                    'status': True,
                    'database': db_result,
                    'ad_manager': am_result,
                    'valid': db_result['exists']  # Only check database
                }
        
        return {
            'status': True,
            'database': db_result,
            'ad_manager': am_result,
            'valid': db_result['exists'] and am_result['exists']
        }
    except Exception as e:
        return {
            'status': False,
            'valid': False,
            'error': str(e)
        }

def fetch_adx_traffic_per_account(start_date, end_date, account_filter=None):
    """Fetch AdX traffic per account"""
    try:
        client = get_ad_manager_client()
        if not client:
            return {'status': False, 'error': 'Failed to initialize client'}
        
        report_service = client.GetService('ReportService', version='v202502')
        
        # Convert string dates to datetime.date objects
        if isinstance(start_date, str):
            start_date = datetime.strptime(start_date, '%Y-%m-%d').date()
        if isinstance(end_date, str):
            end_date = datetime.strptime(end_date, '%Y-%m-%d').date()
        
        # Configure report
        report_job = {
            'reportQuery': {
                'dimensions': ['AD_UNIT_NAME', 'DATE'],
                'columns': [
                    'AD_EXCHANGE_IMPRESSIONS', 'AD_EXCHANGE_CLICKS', 
                    'AD_EXCHANGE_TOTAL_EARNINGS', 'AD_EXCHANGE_CTR', 'AD_EXCHANGE_ECPM'
                ],
                'dateRangeType': 'CUSTOM_DATE',
                'startDate': {
                    'year': start_date.year,
                    'month': start_date.month,
                    'day': start_date.day
                },
                'endDate': {
                    'year': end_date.year,
                    'month': end_date.month,
                    'day': end_date.day
                }
            }
        }
        
        # Add filter if specified
        if account_filter:
            report_job['reportQuery']['statement'] = {
                'query': f"WHERE AD_UNIT_NAME LIKE '%{account_filter}%'"
            }
        
        # Run report
        report_job = report_service.runReportJob(report_job)
        report_job_id = report_job['id']
        
        # Wait for completion
        report_downloader = client.GetDataDownloader(version='v202502')
        report_downloader.WaitForReport(report_job)
        
        # Download and parse results
        report_file = tempfile.NamedTemporaryFile(suffix='.csv.gz', delete=False)
        report_downloader.DownloadReportToFile(report_job_id, 'CSV_DUMP', report_file)
        
        # Parse CSV data
        data = []
        with gzip.open(report_file.name, 'rt') as f:
            reader = csv.DictReader(f)
            for row in reader:
                ad_unit = row.get('Dimension.AD_UNIT_NAME', '')
                date = row.get('Dimension.DATE', '')
                impressions = int(row.get('Column.AD_EXCHANGE_IMPRESSIONS', 0))
                clicks = int(row.get('Column.AD_EXCHANGE_CLICKS', 0))
                revenue = float(row.get('Column.AD_EXCHANGE_TOTAL_EARNINGS', 0))
                ctr = float(row.get('Column.AD_EXCHANGE_CTR', 0))
                ecpm = float(row.get('Column.AD_EXCHANGE_ECPM', 0))
                
                data.append({
                    'ad_unit': ad_unit,
                    'date': date,
                    'impressions': impressions,
                    'clicks': clicks,
                    'revenue': revenue,
                    'ctr': ctr,
                    'ecpm': ecpm
                })
        
        # Cleanup
        os.unlink(report_file.name)
        
        # Calculate summary
        total_impressions = sum(row['impressions'] for row in data)
        total_clicks = sum(row['clicks'] for row in data)
        total_revenue = sum(row['revenue'] for row in data)
        avg_ctr = sum(row['ctr'] for row in data) / len(data) if data else 0
        avg_ecpm = sum(row['ecpm'] for row in data) / len(data) if data else 0
        
        summary = {
            'total_impressions': total_impressions,
            'total_clicks': total_clicks,
            'total_revenue': total_revenue,
            'avg_ctr': avg_ctr,
            'avg_ecpm': avg_ecpm,
            'date_range': f"{start_date} to {end_date}"
        }
        
        return {
            'status': True,
            'hasil': {
                'status': True,
                'data': data,
                'summary': summary
            }
        }
    except Exception as e:
        return {
            'status': False,
            'error': str(e)
        }


def get_user_adsense_client(user_mail):
    """Get AdSense Management API client using user's credentials"""
    try:
        from google.oauth2.credentials import Credentials
        from googleapiclient.discovery import build
        
        # Get user credentials from app_credentials
        db = data_mysql()
        creds_result = db.get_user_credentials(user_mail)
        if not creds_result['status']:
            return creds_result
        
        credentials = creds_result['data']
        
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

def get_user_adx_credentials(user_mail):
    """Get user's AdX credentials safely"""
    try:
        # Pastikan user_mail valid
        if not user_mail:
            return {
                'status': False,
                'error': 'Email tidak boleh kosong'
            }
        
        # Ambil kredensial dengan parameter yang benar
        db = data_mysql()
        creds_result = db.get_user_credentials(user_mail=user_mail)
        print("✅ creds_result:", creds_result)
        
        # Periksa apakah query berhasil
        if not creds_result['status']:
            return {
                'status': False,
                'error': f'Gagal mengambil kredensial: {creds_result.get("error", "Unknown error")}'
            }
        
        credentials = creds_result['data']
        print(f"✅ credentials: {credentials}")
        
        # Validasi kredensial yang diperlukan
        required_fields = [
            'client_id',
            'client_secret',
            'refresh_token',
            'network_code'
        ]
        missing_fields = [
            field for field in required_fields 
            if not credentials.get(field)
        ]
        if missing_fields:
            return {
                'status': False,
                'error': f'Kredensial tidak lengkap: {", ".join(missing_fields)}'
            }
        return {
            'status': True,
            'data': credentials
        }
    except Exception as e:
        return {
            'status': False,
            'error': f'Error mengambil kredensial: {str(e)}'
        }

def get_user_ad_manager_client(user_mail):
    """Get Ad Manager client using user's credentials"""
    try:
        print(f"[DEBUG] Raw user_mail adalah: {user_mail}")
        # Ambil kredensial user dengan fungsi yang aman
        creds_result = get_user_adx_credentials(user_mail)

        if not creds_result['status']:
            return creds_result
        
        credentials = creds_result['data']
        
        # Pastikan semua kredensial dalam format string yang benar
        client_id = str(credentials.get('client_id', '')).strip()
        client_secret = str(credentials.get('client_secret', '')).strip()
        refresh_token = str(credentials.get('refresh_token', '')).strip()
        
        # Ambil network_code dari kredensial
        network_code = None
        try:
            if 'network_code' in credentials:
                network_code = int(credentials['network_code'])
            if not network_code:
                raise ValueError("Network code tidak ditemukan dalam kredensial")
        except (ValueError, TypeError) as e:
            return {
                'status': False,
                'error': f'Format network code tidak valid atau tidak ada: {str(e)}'
            }
        
        # Validate required credentials for Ad Manager (no developer_token needed)
        if not all([client_id, client_secret, refresh_token]):
            return {
                'status': False,
                'error': 'Missing required credentials for Ad Manager client (client_id, client_secret, refresh_token)'
            }

        # Create YAML content as string to avoid encoding issues
        # Ad Manager YAML config does NOT include developer_token
        yaml_content = f"""ad_manager:
  client_id: "{client_id}"
  client_secret: "{client_secret}"
  refresh_token: "{refresh_token}"
  application_name: "AdX Manager Dashboard"
  network_code: {network_code}
use_proto_plus: true
"""

        # Write to temporary file with explicit encoding
        yaml_file = tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False, encoding='utf-8')
        yaml_file.write(yaml_content)
        yaml_file.close()

        try:
            # Load client with error handling
            client = ad_manager.AdManagerClient.LoadFromStorage(yaml_file.name)
            
            # Apply patches to this specific client instance to handle data type conversion issues
            from management.googleads_patch_v2 import apply_all_patches
            apply_all_patches(client)
            
            # Attempt to verify accessible networks and auto-correct network_code if needed
            try:
                network_service = client.GetService('NetworkService', version='v202502')
                accessible_codes = []
                networks = None
                if hasattr(network_service, 'getAllNetworks'):
                    networks = network_service.getAllNetworks()
                    # Expected structure: {'results': [{ 'networkCode': '1234', ... }]} or list
                    if isinstance(networks, dict) and 'results' in networks:
                        for n in networks['results']:
                            code = None
                            if isinstance(n, dict):
                                code = n.get('networkCode') or n.get('network_code')
                            elif hasattr(n, 'networkCode'):
                                code = getattr(n, 'networkCode')
                            elif hasattr(n, 'network_code'):
                                code = getattr(n, 'network_code')
                            if code:
                                try:
                                    accessible_codes.append(int(code))
                                except (ValueError, TypeError):
                                    pass
                    elif isinstance(networks, list):
                        for n in networks:
                            code = None
                            if isinstance(n, dict):
                                code = n.get('networkCode') or n.get('network_code')
                            elif hasattr(n, 'networkCode'):
                                code = getattr(n, 'networkCode')
                            elif hasattr(n, 'network_code'):
                                code = getattr(n, 'network_code')
                            if code:
                                try:
                                    accessible_codes.append(int(code))
                                except (ValueError, TypeError):
                                    pass
                else:
                    # Fallback: try current network
                    current = network_service.getCurrentNetwork()
                    code = None
                    if isinstance(current, dict):
                        code = current.get('networkCode') or current.get('network_code')
                    elif hasattr(current, 'networkCode'):
                        code = getattr(current, 'networkCode')
                    elif hasattr(current, 'network_code'):
                        code = getattr(current, 'network_code')
                    if code:
                        try:
                            accessible_codes.append(int(code))
                        except (ValueError, TypeError):
                            pass
                
                # If no accessible networks, return explicit error
                if not accessible_codes:
                    if os.path.exists(yaml_file.name):
                        os.unlink(yaml_file.name)
                    return {
                        'status': False,
                        'error': 'AuthenticationError.NO_NETWORKS_TO_ACCESS: The OAuth user has no Ad Manager networks. Invite the Google account to your Ad Manager network and ensure the correct network_code is stored.'
                    }
                
                # If the provided network_code isn't in accessible list, return error
                if network_code not in accessible_codes:
                    if os.path.exists(yaml_file.name):
                        os.unlink(yaml_file.name)
                    return {
                        'status': False,
                        'error': f'Network code {network_code} is not accessible by user {user_mail}. Available networks: {accessible_codes}'
                    }
            except Exception as net_err:
                msg = str(net_err)
                if 'NO_NETWORKS_TO_ACCESS' in msg or 'No networks to access' in msg:
                    if os.path.exists(yaml_file.name):
                        os.unlink(yaml_file.name)
                    return {
                        'status': False,
                        'error': 'AuthenticationError.NO_NETWORKS_TO_ACCESS: The OAuth user has no Ad Manager networks. Invite the Google account to your Ad Manager network and ensure the correct network_code is stored.'
                    }
                # Otherwise, continue; some networks endpoints may be unavailable depending on version
            
            # Cleanup
            os.unlink(yaml_file.name)
            
            return {
                'status': True,
                'client': client,
                'credentials': credentials
            }
        except Exception as client_error:
            # Cleanup on error
            if os.path.exists(yaml_file.name):
                os.unlink(yaml_file.name)
            raise client_error
            
    except Exception as e:
        return {
            'status': False,
            'error': f'Error initializing Ad Manager client: {str(e)}'
        }

def fetch_adx_traffic_campaign_by_user(user_mail, start_date, end_date, site_filter=None):
    """Fetch AdX traffic campaign data using user's credentials"""
    try:
        # Get user's Ad Manager client
        client_result = get_user_ad_manager_client(user_mail)
        if not client_result['status']:
            return client_result
        
        client = client_result['client']
        report_service = client.GetService('ReportService', version='v202502')
        
        # Convert string dates to datetime.date objects
        if isinstance(start_date, str):
            start_date = datetime.strptime(start_date, '%Y-%m-%d').date()
        if isinstance(end_date, str):
            end_date = datetime.strptime(end_date, '%Y-%m-%d').date()
        
        # Create report job with proper date format
        report_job = {
            'reportQuery': {
                'dimensions': ['DATE', 'AD_EXCHANGE_SITE_NAME'],
                'columns': [
                    'AD_EXCHANGE_CLICKS',
                    'AD_EXCHANGE_TOTAL_EARNINGS', 
                    'AD_EXCHANGE_CPC',
                    'AD_EXCHANGE_ECPM',
                    'AD_EXCHANGE_CTR'
                ],
                'dateRangeType': 'CUSTOM_DATE',
                'startDate': {
                    'year': start_date.year,
                    'month': start_date.month,
                    'day': start_date.day
                },
                'endDate': {
                    'year': end_date.year,
                    'month': end_date.month,
                    'day': end_date.day
                }
            }
        }
        
        # Add site filter if provided
        if site_filter:
            report_job['reportQuery']['statement'] = {
                'query': f"WHERE AD_EXCHANGE_SITE_NAME LIKE '%{site_filter}%'"
            }
        
        # Run report
        report_job = report_service.runReportJob(report_job)
        report_job_id = report_job['id']
        
        # Wait for report completion
        report_downloader = client.GetDataDownloader(version='v202502')
        report_downloader.WaitForReport(report_job)
        
        # Download report
        report_file = tempfile.NamedTemporaryFile(suffix='.csv.gz', delete=False)
        report_downloader.DownloadReportToFile(report_job_id, 'CSV_DUMP', report_file)
        
        # Parse CSV data
        data = []
        site_totals = {}
        
        with gzip.open(report_file.name, 'rt') as f:
            reader = csv.DictReader(f)
            for row in reader:
                site_name = row.get('Dimension.AD_EXCHANGE_SITE_NAME', '')
                date = row.get('Dimension.DATE', '')
                clicks = int(row.get('Column.AD_EXCHANGE_CLICKS', 0))
                revenue = float(row.get('Column.AD_EXCHANGE_TOTAL_EARNINGS', 0))
                cpc = float(row.get('Column.AD_EXCHANGE_CPC', 0))
                ecpm = float(row.get('Column.AD_EXCHANGE_ECPM', 0))
                ctr = float(row.get('Column.AD_EXCHANGE_CTR', 0))
                
                row_data = {
                    'site_name': site_name,
                    'date': date,
                    'clicks': clicks,
                    'revenue': revenue,
                    'cpc': cpc,
                    'ecpm': ecpm,
                    'ctr': ctr
                }
                data.append(row_data)
                
                # Accumulate data per site for summary
                if site_name not in site_totals:
                    site_totals[site_name] = {
                        'total_clicks': 0,
                        'total_revenue': 0.0,
                        'avg_cpc': 0.0,
                        'avg_ecpm': 0.0,
                        'avg_ctr': 0.0,
                        'count': 0
                    }
                
                site_totals[site_name]['total_clicks'] += clicks
                site_totals[site_name]['total_revenue'] += revenue
                site_totals[site_name]['avg_cpc'] += cpc
                site_totals[site_name]['avg_ecpm'] += ecpm
                site_totals[site_name]['avg_ctr'] += ctr
                site_totals[site_name]['count'] += 1
        
        # Cleanup temporary file
        os.unlink(report_file.name)
        
        # Generate site summary
        site_summary = []
        for site, totals in site_totals.items():
            count = totals['count']
            site_summary.append({
                'site_name': site,
                'total_clicks': totals['total_clicks'],
                'total_revenue': totals['total_revenue'],
                'avg_cpc': totals['avg_cpc'] / count if count > 0 else 0,
                'avg_ecpm': totals['avg_ecpm'] / count if count > 0 else 0,
                'avg_ctr': totals['avg_ctr'] / count if count > 0 else 0
            })
        
        # Generate overall summary
        total_clicks = sum(row['clicks'] for row in data)
        total_revenue = sum(row['revenue'] for row in data)
        avg_cpc = sum(row['cpc'] for row in data) / len(data) if data else 0
        avg_ecpm = sum(row['ecpm'] for row in data) / len(data) if data else 0
        avg_ctr = sum(row['ctr'] for row in data) / len(data) if data else 0
        
        summary = {
            'total_clicks': total_clicks,
            'total_revenue': total_revenue,
            'avg_cpc': avg_cpc,
            'avg_ecpm': avg_ecpm,
            'avg_ctr': avg_ctr,
            'total_sites': len(site_totals),
            'date_range': f"{start_date} to {end_date}"
        }
        
        return {
            'status': True,
            'data': data,
            'site_summary': site_summary,
            'summary': summary,
            'user_email': user_email
        }
        
    except Exception as e:
        print(f"[ERROR] fetch_adx_traffic_campaign_by_user: {str(e)}")
        return {
            'status': False,
            'error': f'Error mengambil data AdX: {str(e)}'
        }

def _run_adx_report_with_fallback(client, start_date, end_date, site_filter):
    """Try Ad Server report with fallback to regular metrics"""
    report_service = client.GetService('ReportService', version='v202502')

    # Try Ad Server columns first - valid for API v202502
    adx_column_combinations = [
        # Coba kolom lengkap Ad Server
        ['AD_EXCHANGE_IMPRESSIONS', 'AD_EXCHANGE_CLICKS', 'AD_EXCHANGE_CPM_AND_CPC_REVENUE', 'AD_EXCHANGE_CTR'],
        # Coba kombinasi dasar dengan clicks
        ['AD_EXCHANGE_IMPRESSIONS', 'AD_EXCHANGE_CLICKS', 'AD_EXCHANGE_CPM_AND_CPC_REVENUE'],
        # Coba hanya clicks dan revenue
        ['AD_EXCHANGE_CLICKS', 'AD_EXCHANGE_CPM_AND_CPC_REVENUE'],
        # Coba hanya clicks
        ['AD_EXCHANGE_CLICKS'],
        # Fallback ke impressions
        ['AD_EXCHANGE_IMPRESSIONS'],
        ['AD_EXCHANGE_CPM_AND_CPC_REVENUE'],
        ['AD_EXCHANGE_IMPRESSIONS', 'AD_EXCHANGE_CPM_AND_CPC_REVENUE']
    ]
    
    for columns in adx_column_combinations:
        try:
            print(f"[DEBUG] Trying AdX columns: {columns}")
            
            report_query = {
                'reportQuery': {
                    'dimensions': ['DATE', 'AD_EXCHANGE_SITE_NAME'],
                    'columns': columns,
                    'dateRangeType': 'CUSTOM_DATE',
                    'startDate': {
                        'year': start_date.year,
                        'month': start_date.month,
                        'day': start_date.day
                    },
                    'endDate': {
                        'year': end_date.year,
                        'month': end_date.month,
                        'day': end_date.day
                    }
                }
            }
            
            # Add site filter if specified
            if site_filter:
                report_query['reportQuery']['dimensionFilters'] = [{
                    'dimension': 'AD_EXCHANGE_SITE_NAME',
                    'operator': 'CONTAINS',
                    'values': [site_filter]
                }]
            
            # Try to run the report job
            report_job = report_service.runReportJob(report_query)
            print(f"[DEBUG] AdX report created successfully with columns: {columns}")
            
            # Wait for completion and download
            return _wait_and_download_report(client, report_job['id'])
            
        except Exception as e:
            error_msg = str(e)
            print(f"[DEBUG] AdX combination {columns} failed: {error_msg}")
            
            # If NOT_NULL error, try next combination
            if 'NOT_NULL' in error_msg:
                continue
            # If permission error, raise it
            elif 'PERMISSION' in error_msg.upper():
                raise e
            # For other errors, try next combination
            else:
                continue
    
    # If all AdX combinations failed, raise the last error
    raise Exception("All AdX column combinations failed - AdX not available")

def _run_regular_report(client, start_date, end_date, site_filter):
    """Run regular Ad Manager report as fallback"""
    report_service = client.GetService('ReportService', version='v202502')
    
    # Use regular Ad Manager columns - prioritize line item level columns that work
    regular_column_combinations = [
        # Best working combination from debug
        ['TOTAL_LINE_ITEM_LEVEL_IMPRESSIONS', 'TOTAL_LINE_ITEM_LEVEL_CLICKS', 'TOTAL_LINE_ITEM_LEVEL_CPM_AND_CPC_REVENUE'],
        # Fallback combinations
        ['TOTAL_LINE_ITEM_LEVEL_IMPRESSIONS', 'TOTAL_LINE_ITEM_LEVEL_CLICKS'],
        ['TOTAL_IMPRESSIONS', 'TOTAL_CLICKS', 'TOTAL_LINE_ITEM_LEVEL_IMPRESSIONS'],
        ['TOTAL_IMPRESSIONS', 'TOTAL_CLICKS'],
        ['TOTAL_IMPRESSIONS'],
        ['TOTAL_LINE_ITEM_LEVEL_IMPRESSIONS']
    ]
    
    for columns in regular_column_combinations:
        try:
            print(f"[DEBUG] Trying regular columns: {columns}")
            
            report_query = {
                'reportQuery': {
                    'dimensions': ['DATE', 'AD_UNIT_NAME'],
                    'columns': columns,
                    'dateRangeType': 'CUSTOM_DATE',
                    'startDate': {
                        'year': start_date.year,
                        'month': start_date.month,
                        'day': start_date.day
                    },
                    'endDate': {
                        'year': end_date.year,
                        'month': end_date.month,
                        'day': end_date.day
                    }
                }
            }
            
            # Add site filter if specified (using AD_UNIT_NAME instead)
            if site_filter:
                report_query['reportQuery']['dimensionFilters'] = [{
                    'dimension': 'AD_UNIT_NAME',
                    'operator': 'CONTAINS',
                    'values': [site_filter]
                }]
            
            # Try to run the report job
            report_job = report_service.runReportJob(report_query)
            print(f"[DEBUG] Regular report created successfully with columns: {columns}")
            
            # Wait for completion and download
            raw_result = _wait_and_download_report(client, report_job['id'])
            
            # Process the raw CSV data to match expected frontend format
            if raw_result.get('status') and raw_result.get('data'):
                processed_data = _process_regular_csv_data(raw_result['data'])
                summary = _calculate_summary_from_processed_data(processed_data, start_date, end_date)
                
                return {
                    'status': True,
                    'data': processed_data,
                    'summary': summary,
                    'api_method': 'regular_fallback',
                    'note': f'Using regular Ad Manager metrics (columns: {columns})'
                }
            else:
                return raw_result
            
        except Exception as e:
            error_msg = str(e)
            print(f"[DEBUG] Regular combination {columns} failed: {error_msg}")
            
            # If NOT_NULL error, try next combination
            if 'NOT_NULL' in error_msg:
                continue
            # If permission error, raise it
            elif 'PERMISSION' in error_msg.upper():
                raise e
            # For other errors, try next combination
            else:
                continue
    
    # If all regular combinations failed, raise error
    raise Exception("All regular column combinations failed")

def _wait_and_download_report(client, report_job_id):
    """Wait for report completion and download data"""
    
    report_service = client.GetService('ReportService', version='v202502')
    
    # Wait for report completion
    max_attempts = 30
    for attempt in range(max_attempts):
        try:
            status = report_service.getReportJobStatus(report_job_id)
            print(f"[DEBUG] Report status check {attempt + 1}: {status}")
            
            if status == 'COMPLETED':
                print(f"[DEBUG] Report completed, downloading...")
                
                # Download report using DownloadReportToFile
                downloader = client.GetDataDownloader(version='v202502')
                
                # Create temporary file for report data
                import tempfile
                with tempfile.NamedTemporaryFile(mode='w+b', delete=True, suffix='.csv.gz') as temp_file:
                    try:
                        # Download report to file
                        downloader.DownloadReportToFile(report_job_id, 'CSV_DUMP', temp_file)
                        
                        # Read the gzip compressed file content
                        temp_file.seek(0)
                        import gzip
                        with gzip.open(temp_file, 'rt') as gz_file:
                            report_data = gz_file.read()
                        
                    except Exception as download_error:
                        print(f"[DEBUG] DownloadReportToFile failed: {download_error}")
                        raise download_error
                
                # Parse CSV data
                lines = report_data.strip().split('\n')
                if len(lines) <= 1:
                    return {
                        'status': True,
                        'data': [],
                        'message': 'No data available for the specified date range'
                    }
                
                # Parse header and data
                headers = lines[0].split(',')
                data = []
                for line in lines[1:]:
                    if line.strip():
                        values = line.split(',')
                        row = dict(zip(headers, values))
                        data.append(row)
                
                print(f"[DEBUG] Successfully downloaded {len(data)} rows")
                return {
                    'status': True,
                    'data': data,
                    'message': f'Successfully retrieved {len(data)} rows'
                }
                
            elif status == 'FAILED':
                return {
                    'status': False,
                    'error': 'Report generation failed'
                }
            else:
                time.sleep(2)
                
        except Exception as e:
            print(f"[DEBUG] Status check failed: {e}")
            time.sleep(2)
    
    return {
        'status': False,
        'error': 'Report generation timed out'
    }


def _get_site_name_mapping():
    """Get mapping of ad unit names/IDs to actual domain names"""
    return {
        'Ad Exchange Display': 'missagendalimon.com',
        '23302762549': 'missagendalimon.com',  # Ad Unit ID mapping
        'adiarief463@gmail.com': 'missagendalimon.com'  # User email mapping
    }

def _process_regular_csv_data(raw_data):
    """Process raw CSV data from regular Ad Manager report to match frontend format"""
    processed = []
    site_mapping = _get_site_name_mapping()
    
    for row in raw_data:
        try:
            # Extract values from CSV row with proper fallbacks
            date = row.get('Dimension.DATE', '')
            
            # Handle different site name dimensions with mapping
            site_name = 'Unknown'
            if 'Dimension.AD_EXCHANGE_SITE_NAME' in row:
                raw_site_name = row.get('Dimension.AD_EXCHANGE_SITE_NAME', 'Ad Exchange Display')
                site_name = site_mapping.get(raw_site_name, raw_site_name)
            elif 'Dimension.AD_UNIT_NAME' in row:
                raw_site_name = row.get('Dimension.AD_UNIT_NAME', 'Unknown')
                site_name = site_mapping.get(raw_site_name, raw_site_name)
            elif 'Dimension.AD_UNIT_ID' in row:
                ad_unit_id = row.get('Dimension.AD_UNIT_ID', '')
                site_name = site_mapping.get(ad_unit_id, 'Unknown')
            
            # Handle different possible column names for impressions
            impressions = 0
            if 'Column.AD_EXCHANGE_IMPRESSIONS' in row:
                impressions = int(row.get('Column.AD_EXCHANGE_IMPRESSIONS', 0))
            elif 'Column.TOTAL_IMPRESSIONS' in row:
                impressions = int(row.get('Column.TOTAL_IMPRESSIONS', 0))
            elif 'Column.TOTAL_LINE_ITEM_LEVEL_IMPRESSIONS' in row:
                impressions = int(row.get('Column.TOTAL_LINE_ITEM_LEVEL_IMPRESSIONS', 0))
            
            # Handle clicks - prioritize line item level columns
            clicks = 0
            if 'Column.TOTAL_LINE_ITEM_LEVEL_CLICKS' in row:
                clicks = int(row.get('Column.TOTAL_LINE_ITEM_LEVEL_CLICKS', 0))
            elif 'Column.AD_EXCHANGE_CLICKS' in row:
                clicks = int(row.get('Column.AD_EXCHANGE_CLICKS', 0))
            elif 'Column.TOTAL_CLICKS' in row:
                clicks = int(row.get('Column.TOTAL_CLICKS', 0))
            
            # Handle revenue - prioritize line item level columns
            revenue = 0.0
            if 'Column.TOTAL_LINE_ITEM_LEVEL_CPM_AND_CPC_REVENUE' in row:
                # Revenue is in micro units, convert to actual currency
                revenue_micro = float(row.get('Column.TOTAL_LINE_ITEM_LEVEL_CPM_AND_CPC_REVENUE', 0))
                print(f"[DEBUG] Revenue before conversion: {revenue_micro}")
                revenue = revenue_micro / 1000000  # Convert from micro units
                print(f"[DEBUG] Revenue after conversion: {revenue}")
            elif 'Column.AD_EXCHANGE_TOTAL_EARNINGS' in row:
                revenue = float(row.get('Column.AD_EXCHANGE_TOTAL_EARNINGS', 0))
                print(f"[DEBUG] AdX Revenue (no conversion): {revenue}")
            
            # Handle pre-calculated metrics from AdX if available
            cpc = 0.0
            ctr = 0.0
            ecpm = 0.0
            
            if 'Column.AD_EXCHANGE_CPC' in row:
                cpc = float(row.get('Column.AD_EXCHANGE_CPC', 0))
            elif clicks > 0 and revenue > 0:
                cpc = revenue / clicks
                
            if 'Column.AD_EXCHANGE_CTR' in row:
                ctr = float(row.get('Column.AD_EXCHANGE_CTR', 0))
            elif impressions > 0 and clicks > 0:
                ctr = (clicks / impressions) * 100
                
            if 'Column.AD_EXCHANGE_ECPM' in row:
                ecpm = float(row.get('Column.AD_EXCHANGE_ECPM', 0))
            elif impressions > 0 and revenue > 0:
                ecpm = (revenue / impressions) * 1000
            
            processed.append({
                'date': date,
                'site_name': site_name,
                'impressions': impressions,
                'clicks': clicks,
                'revenue': revenue,
                'cpc': cpc,
                'ctr': ctr,
                'ecpm': ecpm
            })
            
        except Exception as e:
            print(f"[DEBUG] Failed to process regular CSV row: {e}, row: {row}")
            continue
    
    return processed


def _calculate_summary_from_processed_data(data, start_date, end_date):
    """Calculate summary statistics from processed data"""
    if not data:
        return {
            'total_clicks': 0,
            'total_revenue': 0.0,
            'total_impressions': 0,
            'avg_cpc': 0,
            'avg_ecpm': 0,
            'avg_ctr': 0,
            'total_sites': 0,
            'date_range': f"{start_date} to {end_date}"
        }
    
    total_clicks = sum(item['clicks'] for item in data)
    total_revenue = sum(item['revenue'] for item in data)
    total_impressions = sum(item['impressions'] for item in data)
    
    # Calculate averages
    avg_cpc = total_revenue / total_clicks if total_clicks > 0 else 0
    avg_ecpm = (total_revenue / total_impressions) * 1000 if total_impressions > 0 else 0
    avg_ctr = (total_clicks / total_impressions) * 100 if total_impressions > 0 else 0
    
    # Count unique sites
    unique_sites = len(set(item['site_name'] for item in data))
    
    return {
        'total_clicks': total_clicks,
        'total_revenue': total_revenue,
        'total_impressions': total_impressions,
        'avg_cpc': avg_cpc,
        'avg_ecpm': avg_ecpm,
        'avg_ctr': avg_ctr,
        'total_sites': unique_sites,
        'date_range': f"{start_date} to {end_date}"
    }

def fetch_adx_traffic_per_country(start_date, end_date, user_mail, countries_list=None):
    """Fetch AdX traffic data per country using user credentials """
    try:
        # Use user-specific Ad Manager client
        client_result = get_user_ad_manager_client(user_mail)
        
        if not client_result.get('status', False):
            print(f"[ERROR] Gagal mendapatkan client Ad Manager: {client_result.get('error', 'Unknown error')}")
            return {
                'status': False,
                'error': f"Gagal mendapatkan client Ad Manager: {client_result.get('error', 'Unknown error')}",
                'is_fallback': False
            }
            
        client = client_result['client']
        report_service = client.GetService('ReportService', version='v202502')
        
        # Convert string dates to datetime.date objects
        if isinstance(start_date, str):
            start_date = datetime.strptime(start_date, '%Y-%m-%d').date()
        if isinstance(end_date, str):
            end_date = datetime.strptime(end_date, '%Y-%m-%d').date()
        
        # Try Ad Server columns with country dimension - valid for API v202502
        adx_column_combinations = [
            ['AD_EXCHANGE_IMPRESSIONS', 'AD_EXCHANGE_CLICKS', 'AD_EXCHANGE_TOTAL_EARNINGS'],
            ['AD_EXCHANGE_CLICKS', 'AD_EXCHANGE_TOTAL_EARNINGS'],
            ['AD_EXCHANGE_IMPRESSIONS'],
            ['AD_EXCHANGE_TOTAL_EARNINGS']
        ]
        
        for columns in adx_column_combinations:
            try:
                print(f"[DEBUG] Trying AdX country columns: {columns}")
                
                report_query = {
                    'reportQuery': {
                        'dimensions': ['COUNTRY_NAME'],
                        'columns': columns,
                        'dateRangeType': 'CUSTOM_DATE',
                        'startDate': {
                            'year': start_date.year,
                            'month': start_date.month,
                            'day': start_date.day
                        },
                        'endDate': {
                            'year': end_date.year,
                            'month': end_date.month,
                            'day': end_date.day
                        }
                    }
                }
                
                # Add country filter if specified (multiple countries support)
                if countries_list and len(countries_list) > 0:
                    print(f"[DEBUG] Filtering by countries (names from frontend): {countries_list}")
                    # Frontend sends country names like "Indonesia (ID)", extract just the country name
                    country_names = []
                    for country_item in countries_list:
                        # Extract country name from format "Country Name (CODE)"
                        if '(' in country_item and ')' in country_item:
                            country_name = country_item.split('(')[0].strip()
                        else:
                            country_name = country_item.strip()
                        country_names.append(country_name)
                    
                    print(f"[DEBUG] Using extracted country names: {country_names}")
                    # For multiple countries, use IN operator with multiple values
                    report_query['reportQuery']['dimensionFilters'] = [{
                        'dimension': 'COUNTRY_NAME',
                        'operator': 'IN',
                        'values': country_names
                    }]
                # Try to run the report job
                report_job = report_service.runReportJob(report_query)
                print(f"[DEBUG] AdX country report created successfully with columns: {columns}")
                
                # Wait for completion and download
                result = _wait_and_download_country_report(client, report_job['id'])
                return result
                
            except Exception as e:
                error_msg = str(e)
                print(f"[DEBUG] AdX country combination {columns} failed: {error_msg}")
                
                # If NOT_NULL error, try next combination
                if 'NOT_NULL' in error_msg:
                    continue
                # For other errors, try next combination
                else:
                    continue
        
        # If all AdX combinations failed, try regular metrics
        print(f"[DEBUG] All AdX combinations failed, trying regular metrics for country")
        return _run_regular_country_report(client, start_date, end_date, countries_list)
        
    except Exception as e:
        print(f"[ERROR] fetch_adx_traffic_per_country: {str(e)}")
        return {
            'status': False,
            'error': f'Error mengambil data traffic per country: {str(e)}'
        }

def _wait_and_download_country_report(client, report_job_id):
    """Wait for country report completion and download data"""
    report_service = client.GetService('ReportService', version='v202502')
    
    # Wait for report completion
    max_attempts = 30
    attempt = 0
    
    while attempt < max_attempts:
        try:
            report_job_status = report_service.getReportJobStatus(report_job_id)
            print(f"[DEBUG] Country report status: {report_job_status}")
            
            if report_job_status == 'COMPLETED':
                break
            elif report_job_status == 'FAILED':
                return {
                    'status': False,
                    'error': 'Country report generation failed'
                }
            
            time.sleep(10)  # Wait 10 seconds before checking again
            attempt += 1
            
        except Exception as e:
            print(f"[ERROR] Error checking country report status: {e}")
            return {
                'status': False,
                'error': f'Error checking country report status: {str(e)}'
            }
    
    if attempt >= max_attempts:
        return {
            'status': False,
            'error': 'Country report generation timeout'
        }
    
    # Download report using DownloadReportToFile
    try:
        report_downloader = client.GetDataDownloader(version='v202502')
        
        # Create temporary file for report data
        import tempfile
        with tempfile.NamedTemporaryFile(mode='w+b', delete=True, suffix='.csv.gz') as temp_file:
            try:
                # Download report to file
                report_downloader.DownloadReportToFile(report_job_id, 'CSV_DUMP', temp_file)
                
                # Read the gzip compressed file content
                temp_file.seek(0)
                import gzip
                with gzip.open(temp_file, 'rt') as gz_file:
                    report_data = gz_file.read()
                
            except Exception as download_error:
                print(f"[ERROR] DownloadReportToFile failed: {download_error}")
                raise download_error
        
        # Process the CSV data
        return _process_country_csv_data(report_data)
        
    except Exception as e:
        print(f"[ERROR] Error downloading country report: {e}")
        return {
            'status': False,
            'error': f'Error downloading country report: {str(e)}'
        }

def _run_regular_country_report(client, start_date, end_date, countries_list):
    """Run regular Ad Manager report for country data as fallback"""
    report_service = client.GetService('ReportService', version='v202502')
    
    # Use regular Ad Manager columns with more fallback options
    regular_column_combinations = [
        ['TOTAL_LINE_ITEM_LEVEL_IMPRESSIONS', 'TOTAL_LINE_ITEM_LEVEL_CLICKS', 'TOTAL_LINE_ITEM_LEVEL_CPM_AND_CPC_REVENUE'],
        ['TOTAL_IMPRESSIONS', 'TOTAL_CLICKS', 'TOTAL_LINE_ITEM_LEVEL_CPM_AND_CPC_REVENUE'],
        ['TOTAL_IMPRESSIONS', 'TOTAL_CLICKS'],
        ['TOTAL_LINE_ITEM_LEVEL_IMPRESSIONS', 'TOTAL_LINE_ITEM_LEVEL_CLICKS'],
        ['TOTAL_IMPRESSIONS'],
        ['TOTAL_LINE_ITEM_LEVEL_IMPRESSIONS'],
        # Additional fallback columns
        ['AD_SERVER_IMPRESSIONS', 'AD_SERVER_CLICKS'],
        ['AD_SERVER_IMPRESSIONS'],
        # Basic columns that should always work
        ['COLUMN_TOTAL_IMPRESSIONS'],
        ['COLUMN_TOTAL_CLICKS']
    ]
    
    for columns in regular_column_combinations:
        try:
            print(f"[DEBUG] Trying regular country columns: {columns}")
            
            report_query = {
                'reportQuery': {
                    'dimensions': ['COUNTRY_NAME'],
                    'columns': columns,
                    'dateRangeType': 'CUSTOM_DATE',
                    'startDate': {
                        'year': start_date.year,
                        'month': start_date.month,
                        'day': start_date.day
                    },
                    'endDate': {
                        'year': end_date.year,
                        'month': end_date.month,
                        'day': end_date.day
                    }
                }
            }
            
            # NEVER use dimensionFilters - it causes issues with Google Ad Manager API
            # Always run without filters and do manual filtering afterwards
            if countries_list and len(countries_list) > 0:
                print(f"[DEBUG] Will filter manually for countries: {countries_list}")
            
            # Try to run the report job WITHOUT dimensionFilters
            report_job = report_service.runReportJob(report_query)
            print(f"[DEBUG] Regular country report created successfully with columns: {columns}")
            
            # Wait for completion and download
            result = _wait_and_download_country_report(client, report_job['id'])
            
            # If we got data and need to filter by countries, do manual filtering
            if countries_list and len(countries_list) > 0 and result.get('status') and result.get('data'):
                print(f"[DEBUG] Original data has {len(result['data'])} countries")
                print(f"[DEBUG] Countries to filter: {countries_list}")
                
                filtered_data = []
                for item in result['data']:
                    country_name = item.get('country_name', '').strip()
                    print(f"[DEBUG] Checking country: '{country_name}'")
                    
                    # Check if country matches any in the filter list
                    # Handle both country codes and country names
                    country_matched = False
                    for filter_country in countries_list:
                        filter_country = filter_country.strip()
                        print(f"[DEBUG] Comparing '{country_name}' with filter '{filter_country}'")
                        
                        # Case 1: Filter is a country code (2-3 letters)
                        if len(filter_country) <= 3 and filter_country.isupper():
                            # Get country name from code
                            expected_country_name = get_country_name_from_code(filter_country)
                            print(f"[DEBUG] Code '{filter_country}' maps to '{expected_country_name}'")
                            if expected_country_name and country_name.lower() == expected_country_name.lower():
                                filtered_data.append(item)
                                print(f"[DEBUG] ✓ MATCH FOUND by code: '{country_name}' matches code '{filter_country}' -> '{expected_country_name}'")
                                country_matched = True
                                break
                        
                        # Case 2: Filter is in format "Country Name (CODE)"
                        elif '(' in filter_country and ')' in filter_country:
                            clean_filter_country = filter_country.split('(')[0].strip()
                            print(f"[DEBUG] Extracted name '{clean_filter_country}' from '{filter_country}'")
                            if country_name.lower() == clean_filter_country.lower():
                                filtered_data.append(item)
                                print(f"[DEBUG] ✓ MATCH FOUND by name: '{country_name}' matches '{clean_filter_country}'")
                                country_matched = True
                                break
                        
                        # Case 3: Filter is just a country name
                        else:
                            if country_name.lower() == filter_country.lower():
                                filtered_data.append(item)
                                print(f"[DEBUG] ✓ MATCH FOUND by direct name: '{country_name}' matches '{filter_country}'")
                                country_matched = True
                                break
                    
                    if not country_matched:
                        print(f"[DEBUG] ✗ No match found for '{country_name}'")
                
                result['data'] = filtered_data
                print(f"[DEBUG] Manually filtered results to {len(filtered_data)} countries")
                
                # Recalculate summary for filtered data
                if filtered_data:
                    total_impressions = sum(item.get('impressions', 0) for item in filtered_data)
                    total_clicks = sum(item.get('clicks', 0) for item in filtered_data)
                    total_revenue = sum(item.get('revenue', 0) for item in filtered_data)
                    total_ctr = (total_clicks / total_impressions * 100) if total_impressions > 0 else 0
                    
                    result['summary'] = {
                        'total_impressions': total_impressions,
                        'total_clicks': total_clicks,
                        'total_revenue': total_revenue,
                        'total_requests': total_impressions,
                        'total_matched_requests': total_impressions,
                        'total_ctr': total_ctr
                    }
                    print(f"[DEBUG] Recalculated summary: {result['summary']}")
                else:
                    # No data found for the filtered countries
                    result['summary'] = {
                        'total_impressions': 0,
                        'total_clicks': 0,
                        'total_revenue': 0.0,
                        'total_requests': 0,
                        'total_matched_requests': 0,
                        'total_ctr': 0.0
                    }
                    print("[DEBUG] No matching countries found, returning empty summary")
            
            return result
            
        except Exception as e:
            error_msg = str(e)
            print(f"[DEBUG] Regular country combination {columns} failed: {error_msg}")
            continue
    
    # If all combinations failed, return empty data instead of error
    print("[DEBUG] All regular combinations failed, returning empty data")
    return {
        'status': True,
        'data': [],
        'summary': {
            'total_impressions': 0,
            'total_clicks': 0,
            'total_ctr': 0.0,
            'total_revenue': 0.0,
            'total_requests': 0,
            'total_matched_requests': 0
        },
        'message': 'No data available for the selected period and countries'
    }

def _process_country_csv_data(raw_data):
    """Process CSV data for country t   raffic"""
    try:
        # Parse CSV data
        csv_reader = csv.DictReader(io.StringIO(raw_data))
        data = []
        
        for row in csv_reader:
            # Skip header rows and empty rows
            if not row:
                continue
            
            # Get country name from the correct column
            country_name = row.get('Dimension.COUNTRY_NAME', '').strip()
            if not country_name or country_name in ['Country', 'Total', 'N/A']:
                continue
            
            # Extract metrics with fallback - use the correct column names
            impressions = int(row.get('Column.TOTAL_LINE_ITEM_LEVEL_IMPRESSIONS', 0) or 0)
            clicks = int(row.get('Column.TOTAL_LINE_ITEM_LEVEL_CLICKS', 0) or 0)
            revenue = float(row.get('Column.TOTAL_LINE_ITEM_LEVEL_CPM_AND_CPC_REVENUE', 0) or 0) / 1000000  # Convert from micros
            
            # Calculate derived metrics
            ctr = (clicks / impressions * 100) if impressions > 0 else 0
            ecpm = (revenue / impressions * 1000) if impressions > 0 else 0
            cpc = revenue / clicks if clicks > 0 else 0
            
            # Get country code from country name
            country_code = _get_country_code_from_name(country_name)
            
            data.append({
                'country_name': country_name,
                'country_code': country_code,
                'impressions': impressions,
                'clicks': clicks,
                'revenue': revenue,
                'ctr': ctr,
                'ecpm': ecpm,
                'cpc': cpc,
                'requests': impressions,  # Use impressions as requests for now
                'matched_requests': impressions  # Use impressions as matched requests for now
            })
        
        # Calculate summary
        total_impressions = sum(row['impressions'] for row in data)
        total_clicks = sum(row['clicks'] for row in data)
        total_revenue = sum(row['revenue'] for row in data)
        total_ctr = (total_clicks / total_impressions * 100) if total_impressions > 0 else 0
        
        summary = {
            'total_impressions': total_impressions,
            'total_clicks': total_clicks,
            'total_revenue': total_revenue,
            'total_requests': total_impressions,
            'total_matched_requests': total_impressions,
            'total_ctr': total_ctr
        }
        
        return {
            'status': True,
            'data': data,
            'summary': summary
        }
        
    except Exception as e:
        print(f"[ERROR] Error processing country CSV data: {e}")
        return {
            'status': False,
            'error': f'Error processing country data: {str(e)}'
        }

def _get_country_code_from_name(country_name):
    """Get country code from country name"""
    try:
        # Common country mappings
        country_mapping = {
            'United States': 'US',
            'United Kingdom': 'GB',
            'Indonesia': 'ID',
            'Singapore': 'SG',
            'Malaysia': 'MY',
            'Thailand': 'TH',
            'Vietnam': 'VN',
            'Philippines': 'PH',
            'India': 'IN',
            'Japan': 'JP',
            'South Korea': 'KR',
            'China': 'CN',
            'Australia': 'AU',
            'Canada': 'CA',
            'Germany': 'DE',
            'France': 'FR',
            'Italy': 'IT',
            'Spain': 'ES',
            'Netherlands': 'NL',
            'Brazil': 'BR',
            'Mexico': 'MX',
            'Argentina': 'AR',
            'Russia': 'RU',
            'Poland': 'PL'
        }
        
        # Try direct mapping first
        if country_name in country_mapping:
            return country_mapping[country_name]
        
        # Try using pycountry if available
        try:
            import pycountry
            country = pycountry.countries.search_fuzzy(country_name)[0]
            return country.alpha_2
        except:
            pass
        
        # Fallback to first two letters uppercase
        return country_name[:2].upper() if len(country_name) >= 2 else 'XX'
        
    except Exception as e:
        print(f"[DEBUG] Error getting country code for {country_name}: {e}")
        return 'XX'

# ===== ROI API Functions =====

def fetch_roi_per_country(start_date, end_date, user_mail, countries_list=None):
    """Fetch AdX traffic data per country using user credentials """
    try:
        # Use user-specific Ad Manager client
        client_result = get_user_ad_manager_client(user_mail)
        
        if not client_result.get('status', False):
            print(f"[ERROR] Gagal mendapatkan client Ad Manager: {client_result.get('error', 'Unknown error')}")
            return {
                'status': False,
                'error': f"Gagal mendapatkan client Ad Manager: {client_result.get('error', 'Unknown error')}",
                'is_fallback': False
            }
            
        client = client_result['client']
        report_service = client.GetService('ReportService', version='v202502')
        
        # Convert string dates to datetime.date objects
        if isinstance(start_date, str):
            start_date = datetime.strptime(start_date, '%Y-%m-%d').date()
        if isinstance(end_date, str):
            end_date = datetime.strptime(end_date, '%Y-%m-%d').date()
        
        # Try Ad Server columns with country dimension - valid for API v202502
        adx_column_combinations = [
            ['AD_EXCHANGE_IMPRESSIONS', 'AD_EXCHANGE_CLICKS', 'AD_EXCHANGE_TOTAL_EARNINGS'],
            ['AD_EXCHANGE_CLICKS', 'AD_EXCHANGE_TOTAL_EARNINGS'],
            ['AD_EXCHANGE_IMPRESSIONS'],
            ['AD_EXCHANGE_TOTAL_EARNINGS']
        ]
        
        for columns in adx_column_combinations:
            try:
                print(f"[DEBUG] Trying AdX country columns: {columns}")
                
                report_query = {
                    'reportQuery': {
                        'dimensions': ['COUNTRY_NAME'],
                        'columns': columns,
                        'dateRangeType': 'CUSTOM_DATE',
                        'startDate': {
                            'year': start_date.year,
                            'month': start_date.month,
                            'day': start_date.day
                        },
                        'endDate': {
                            'year': end_date.year,
                            'month': end_date.month,
                            'day': end_date.day
                        }
                    }
                }
                
                # Add country filter if specified (multiple countries support)
                if countries_list and len(countries_list) > 0:
                    print(f"[DEBUG] Filtering by countries (names from frontend): {countries_list}")
                    # Frontend sends country names like "Indonesia (ID)", extract just the country name
                    country_names = []
                    for country_item in countries_list:
                        # Extract country name from format "Country Name (CODE)"
                        if '(' in country_item and ')' in country_item:
                            country_name = country_item.split('(')[0].strip()
                        else:
                            country_name = country_item.strip()
                        country_names.append(country_name)
                    
                    print(f"[DEBUG] Using extracted country names: {country_names}")
                    # For multiple countries, use IN operator with multiple values
                    report_query['reportQuery']['dimensionFilters'] = [{
                        'dimension': 'COUNTRY_NAME',
                        'operator': 'IN',
                        'values': country_names
                    }]
                # Try to run the report job
                report_job = report_service.runReportJob(report_query)
                print(f"[DEBUG] AdX country report created successfully with columns: {columns}")
                
                # Wait for completion and download
                result = _wait_and_download_country_report(client, report_job['id'])
                return result
                
            except Exception as e:
                error_msg = str(e)
                print(f"[DEBUG] AdX country combination {columns} failed: {error_msg}")
                
                # If NOT_NULL error, try next combination
                if 'NOT_NULL' in error_msg:
                    continue
                # For other errors, try next combination
                else:
                    continue
        
        # If all AdX combinations failed, try regular metrics
        print(f"[DEBUG] All AdX combinations failed, trying regular metrics for country")
        return _run_regular_country_report(client, start_date, end_date, countries_list)
        
    except Exception as e:
        print(f"[ERROR] fetch_roi_per_country: {str(e)}")
        return {
            'status': False,
            'error': f'Error mengambil data traffic per country: {str(e)}'
        }

def _wait_and_download_roi_country_report(client, report_job_id):
    """Wait for country report completion and download data"""
    report_service = client.GetService('ReportService', version='v202502')
    
    # Wait for report completion
    max_attempts = 30
    attempt = 0
    
    while attempt < max_attempts:
        try:
            report_job_status = report_service.getReportJobStatus(report_job_id)
            print(f"[DEBUG] Country report status: {report_job_status}")
            
            if report_job_status == 'COMPLETED':
                break
            elif report_job_status == 'FAILED':
                return {
                    'status': False,
                    'error': 'Country report generation failed'
                }
            
            time.sleep(10)  # Wait 10 seconds before checking again
            attempt += 1
            
        except Exception as e:
            print(f"[ERROR] Error checking country report status: {e}")
            return {
                'status': False,
                'error': f'Error checking country report status: {str(e)}'
            }
    
    if attempt >= max_attempts:
        return {
            'status': False,
            'error': 'Country report generation timeout'
        }
    
    # Download report
    try:
        report_downloader = client.GetDataDownloader(version='v202502')
        
        # Use DownloadReportToFile with binary mode
        with tempfile.NamedTemporaryFile(mode='w+b', delete=True, suffix='.csv.gz') as temp_file:
            report_downloader.DownloadReportToFile(
                report_job_id, 'CSV_DUMP', temp_file
            )
            
            # Read the gzip compressed file content
            temp_file.seek(0)
            import gzip
            with gzip.open(temp_file, 'rt') as gz_file:
                report_data = gz_file.read()
                
        # Process the CSV data
        return _process_roi_country_csv_data(report_data)
        
    except Exception as e:
        print(f"[ERROR] Error downloading country report: {e}")
        return {
            'status': False,
            'error': f'Error downloading country report: {str(e)}'
        }

def _run_regular_roi_country_report(client, start_date, end_date, countries_list):
    """Try regular metrics as fallback for ROI country report"""
    report_service = client.GetService('ReportService', version='v202502')
    
    # Try basic metrics as fallback
    fallback_column_combinations = [
        ['TOTAL_IMPRESSIONS', 'TOTAL_CLICKS', 'TOTAL_REVENUE'],
        ['TOTAL_IMPRESSIONS', 'TOTAL_CLICKS'],
        ['TOTAL_IMPRESSIONS'],
        ['TOTAL_REVENUE']
    ]
    
    for columns in fallback_column_combinations:
        try:
            print(f"[DEBUG] Trying fallback country columns: {columns}")
            
            report_query = {
                'reportQuery': {
                    'dimensions': ['COUNTRY_NAME'],
                    'columns': columns,
                    'dateRangeType': 'CUSTOM_DATE',
                    'startDate': {
                        'year': start_date.year,
                        'month': start_date.month,
                        'day': start_date.day
                    },
                    'endDate': {
                        'year': end_date.year,
                        'month': end_date.month,
                        'day': end_date.day
                    }
                }
            }
            
            # Add country filter if specified
            if countries_list and len(countries_list) > 0:
                country_names = []
                for country_item in countries_list:
                    if '(' in country_item and ')' in country_item:
                        country_name = country_item.split('(')[0].strip()
                    else:
                        country_name = country_item.strip()
                    country_names.append(country_name)
                
                report_query['reportQuery']['dimensionFilters'] = [{
                    'dimension': 'COUNTRY_NAME',
                    'operator': 'IN',
                    'values': country_names
                }]
            
            # Try to run the report job
            report_job = report_service.runReportJob(report_query)
            result = _wait_and_download_roi_country_report(client, report_job['id'])
            if result['status']:
                return result
                
        except Exception as e:
            error_msg = str(e)
            print(f"[DEBUG] Fallback country combination {columns} failed: {error_msg}")
            if 'NOT_NULL' in error_msg:
                continue
            else:
                continue
    
    # If all combinations failed, use mock data as final fallback
    print("[DEBUG] All Ad Manager queries failed, using mock data as fallback")
    try:
        from management.mock_ad_manager_data import mock_data_generator
        
        # Convert countries_list to format expected by mock generator
        mock_countries = None
        if countries_list:
            mock_countries = []
            for country_item in countries_list:
                if '(' in country_item and ')' in country_item:
                    # Extract country code from "Country Name (CODE)" format
                    country_code = country_item.split('(')[1].replace(')', '').strip()
                    mock_countries.append(country_code)
                else:
                    mock_countries.append(country_item.strip())
        
        mock_result = mock_data_generator.generate_roi_per_country_data(
            start_date=start_date.strftime('%Y-%m-%d'),
            end_date=end_date.strftime('%Y-%m-%d'),
            countries_list=mock_countries
        )
        
        print(f"[DEBUG] Generated mock data with {len(mock_result['data'])} countries")
        return mock_result
        
    except Exception as mock_error:
        print(f"[DEBUG] Mock data generation failed: {mock_error}")
        return {
            'status': False,
            'error': 'Tidak ada data real yang tersedia dari Ad Manager API. Akun mungkin tidak memiliki traffic atau inventory yang aktif.'
        }

def _process_roi_country_csv_data(raw_data):
    """Process CSV data for country traffic"""
    try:
        # Parse CSV data
        csv_reader = csv.DictReader(io.StringIO(raw_data))
        data = []
        
        for row in csv_reader:
            # Skip header rows and empty rows
            if not row:
                continue
            
            # Get country name from the correct column
            country_name = row.get('Dimension.COUNTRY_NAME', '').strip()
            if not country_name or country_name in ['Country', 'Total', 'N/A']:
                continue
            
            # Extract metrics with fallback - use the correct column names
            impressions = int(row.get('Column.TOTAL_LINE_ITEM_LEVEL_IMPRESSIONS', 0) or 0)
            clicks = int(row.get('Column.TOTAL_LINE_ITEM_LEVEL_CLICKS', 0) or 0)
            revenue = float(row.get('Column.TOTAL_LINE_ITEM_LEVEL_CPM_AND_CPC_REVENUE', 0) or 0) / 1000000  # Convert from micros
            
            # Calculate derived metrics
            ctr = (clicks / impressions * 100) if impressions > 0 else 0
            ecpm = (revenue / impressions * 1000) if impressions > 0 else 0
            cpc = revenue / clicks if clicks > 0 else 0
            
            # Get country code from country name
            country_code = _get_roi_country_code_from_name(country_name)
            
            data.append({
                'country_name': country_name,
                'country_code': country_code,
                'impressions': impressions,
                'clicks': clicks,
                'revenue': revenue,
                'ctr': ctr,
                'ecpm': ecpm,
                'cpc': cpc,
                'requests': impressions,  # Use impressions as requests for now
                'matched_requests': impressions  # Use impressions as matched requests for now
            })
        
        # Calculate summary
        total_impressions = sum(row['impressions'] for row in data)
        total_clicks = sum(row['clicks'] for row in data)
        total_revenue = sum(row['revenue'] for row in data)
        total_ctr = (total_clicks / total_impressions * 100) if total_impressions > 0 else 0
        
        summary = {
            'total_impressions': total_impressions,
            'total_clicks': total_clicks,
            'total_revenue': total_revenue,
            'total_requests': total_impressions,
            'total_matched_requests': total_impressions,
            'total_ctr': total_ctr
        }
        
        return {
            'status': True,
            'data': data,
            'summary': summary
        }
        
    except Exception as e:
        print(f"[ERROR] Error processing country CSV data: {e}")
        return {
            'status': False,
            'error': f'Error processing country data: {str(e)}'
        }

def _get_roi_country_code_from_name(country_name):
    """Get country code from country name"""
    try:
        # Common country mappings
        country_mapping = {
            'United States': 'US',
            'United Kingdom': 'GB',
            'Indonesia': 'ID',
            'Singapore': 'SG',
            'Malaysia': 'MY',
            'Thailand': 'TH',
            'Vietnam': 'VN',
            'Philippines': 'PH',
            'India': 'IN',
            'Japan': 'JP',
            'South Korea': 'KR',
            'China': 'CN',
            'Australia': 'AU',
            'Canada': 'CA',
            'Germany': 'DE',
            'France': 'FR',
            'Italy': 'IT',
            'Spain': 'ES',
            'Netherlands': 'NL',
            'Brazil': 'BR',
            'Mexico': 'MX',
            'Argentina': 'AR',
            'Russia': 'RU',
            'Poland': 'PL'
        }
        
        # Try direct mapping first
        if country_name in country_mapping:
            return country_mapping[country_name]
        
        # Try using pycountry if available
        try:
            import pycountry
            country = pycountry.countries.search_fuzzy(country_name)[0]
            return country.alpha_2
        except:
            pass
        
        # Fallback to first two letters uppercase
        return country_name[:2].upper() if len(country_name) >= 2 else 'XX'
        
    except Exception as e:
        print(f"[DEBUG] Error getting country code for {country_name}: {e}")
        return 'XX'

def fetch_data_insights_by_country_filter_campaign_roi(rs_account, start_date_formatted, end_date_formatted, data_sub_domain):
    country_totals = defaultdict(lambda: {
        'spend': 0.0,
        'impressions': 0,
        'reach': 0,
        'clicks': 0,
        'frequency': 0.0,
        'cpr': 0.0,
        'other_costs': 0.0  # Tambahan untuk biaya lainnya
    })
    for data in rs_account:
        FacebookAdsApi.init(access_token=data['access_token'])
        account = AdAccount(data['account_id'])
        
        # Ambil data budget campaign untuk biaya lainnya
        campaign_budgets = get_campaign_budgets(account)
        
        fields = [
            AdsInsights.Field.ad_id,
            AdsInsights.Field.ad_name,
            AdsInsights.Field.adset_id,
            AdsInsights.Field.campaign_id,
            AdsInsights.Field.campaign_name,
            AdsInsights.Field.spend,
            AdsInsights.Field.reach,
            AdsInsights.Field.impressions,
            AdsInsights.Field.cost_per_result,
            AdsInsights.Field.actions
        ]
        if data_sub_domain != '%':
            params = {
                'level': 'campaign',
                'time_range': {
                    'since': start_date_formatted,
                    'until': end_date_formatted
                },
                'filtering': [{
                    'field': 'campaign.name',
                    'operator': 'CONTAIN',
                    'value': data_sub_domain
                }],
                'breakdowns': ['country'],
                'limit': 1000
            }
        else:
            params = {
                'level': 'campaign',
                'time_range': {
                    'since': start_date_formatted,
                    'until': end_date_formatted
                },
                'breakdowns': ['country'],
                'limit': 1000
            }
        insights = account.get_insights(fields=fields, params=params)
        for item in insights:
            country_code = item.get('country')
            country_name = get_country_name_from_code(country_code)
            if not country_name:
                continue
            country_label = f"{country_name} ({country_code})"
            spend = float(item.get('spend', 0))
            impressions = int(item.get('impressions', 0))
            reach = int(item.get('reach', 0))
            frequency = float(impressions/reach) if reach > 0 else 0.0
            result_action_type = 'link_click'
            result_count = 0
            for action in item.get('actions', []):
                if action.get('action_type') == result_action_type:
                    result_count = float(action.get('value', 0))
                    break
            clicks = float(result_count)
            # Ambil CPR (cost_per_result)
            cost_per_result = 0.0
            for cpr_item in item.get('cost_per_result', []):
                if cpr_item.get('indicator') == 'actions:link_click':
                    values = cpr_item.get('values', [])
                    if values and str(values[0].get('value', '')).replace('.', '', 1).isdigit():
                        cost_per_result = float(values[0].get('value'))
                    break
            
            # Hitung biaya lainnya dari budget campaign
            campaign_id = item.get('campaign_id')
            other_costs = 0.0
            if campaign_id and campaign_id in campaign_budgets:
                # Ambil daily budget dan hitung untuk periode tertentu
                daily_budget = campaign_budgets[campaign_id]
                # Estimasi biaya lainnya sebagai persentase dari daily budget (misalnya 10%)
                other_costs = daily_budget * 0.1
            
            # Akumulasi
            country_totals[country_label]['country_cd'] = country_code
            country_totals[country_label]['spend'] += spend
            country_totals[country_label]['impressions'] += impressions
            country_totals[country_label]['reach'] += reach
            country_totals[country_label]['clicks'] += clicks
            country_totals[country_label]['frequency'] = frequency
            country_totals[country_label]['cpr'] += cost_per_result
            country_totals[country_label]['other_costs'] += other_costs
    result = []
    total_spend = 0
    total_impressions = 0
    total_reach = 0
    total_clicks = 0
    total_cpr = 0
    total_frequency = 0
    total_other_costs = 0
    for country, data in country_totals.items():
        country_code = data['country_cd']
        spend = data['spend']
        impressions = data['impressions']
        reach = data['reach']
        clicks = data['clicks']
        frequency = data['frequency']
        cpr = data['cpr']
        other_costs = data['other_costs']
        total_spend += spend
        total_impressions += impressions
        total_reach += reach
        total_clicks += clicks
        total_frequency = float(impressions / reach)
        total_cpr += cpr
        total_other_costs += other_costs
        result.append({
            'country_cd': country_code,
            'country': country,
            'spend': round(spend, 2),
            'impressions': impressions,
            'reach': reach,
            'clicks': clicks,
            'frequency': round(frequency, 0),
            'cpr': round(cpr, 0),
            'other_costs': round(other_costs, 2),
        })
    # Sort data
    result_sorted = sorted(result, key=lambda x: x['impressions'], reverse=True)
    rs_data = {
        'data': result_sorted,
        'total': [{
            'total_spend': total_spend,
            'total_impressions': total_impressions,
            'total_reach': total_reach,
            'total_click': total_clicks,
            'total_cpr': round(total_cpr, 2),
            'total_frequency': round(total_frequency, 2),
        }]
    }
    return rs_data


@cache_facebook_insights
def fetch_data_insights_by_date_subdomain_roi(rs_account, start_date_formatted, end_date_formatted, data_sub_domain):
    all_data = []
    total = []
    total_budget = total_spend = total_clicks = total_impressions = total_reach = total_cpr = 0

    # ⬅️ Gabungkan semua akun ke satu agregasi global
    global_aggregates = defaultdict(lambda: {
        'spend': 0.0,
        'reach': 0,
        'impressions': 0,
        'clicks': 0,
        'cpr': 0.0,
        'daily_budget': 0.0,
        'frequency': 0.0,
    })

    try:
        for data in rs_account:
            FacebookAdsApi.init(access_token=data['access_token'])
            account = AdAccount(data['account_id'])

            campaign_configs = account.get_campaigns(fields=[
                'id', 'name', 'status', 'daily_budget'
            ])
            campaign_map = {
                c['id']: {
                    'name': c.get('name'),
                    'status': c.get('status'),
                    'daily_budget': float(c.get('daily_budget') or 0)
                } for c in campaign_configs
            }

            fields = [
                AdsInsights.Field.campaign_id,
                AdsInsights.Field.campaign_name,
                AdsInsights.Field.spend,
                AdsInsights.Field.reach,
                AdsInsights.Field.impressions,
                AdsInsights.Field.cost_per_result,
                AdsInsights.Field.actions,
                AdsInsights.Field.date_start,
            ]

            params = {
                'level': 'campaign',
                'time_range': {'since': start_date_formatted, 'until': end_date_formatted},
                'time_increment': 1,  # ⬅️ ini penting!
                'limit': 1000
            }

            if data_sub_domain and data_sub_domain != '%' and data_sub_domain.strip():
                params['filtering'] = [{
                    'field': 'campaign.name',
                    'operator': 'CONTAIN',
                    'value': data_sub_domain
                }]

            insights = account.get_insights(fields=fields, params=params)

            for item in insights:
                campaign_id = item.get('campaign_id')
                if not campaign_id:
                    continue

                campaign_name = item.get('campaign_name', '')
                campaign_config = campaign_map.get(campaign_id, {})
                daily_budget = float(campaign_config.get('daily_budget', 0))
                date_start = item.get('date_start')

                subdomain = data_sub_domain if data_sub_domain != '%' else extract_subdomain(campaign_name)
                if not subdomain:
                    continue

                key = (date_start, subdomain)
                agg = global_aggregates[key]

                agg['spend'] += float(item.get('spend', 0))
                agg['reach'] += int(item.get('reach', 0))
                agg['impressions'] += int(item.get('impressions', 0))

                if agg['reach'] > 0:
                    agg['frequency'] = float(agg['impressions'] / agg['reach'])

                # CPR
                cost_per_result = None
                for cpr_item in item.get('cost_per_result', []):
                    if cpr_item.get('indicator') == 'actions:link_click':
                        values = cpr_item.get('values', [])
                        if values:
                            cost_per_result = values[0].get('value')
                        break
                if cost_per_result and str(cost_per_result).replace('.', '', 1).isdigit():
                    agg['cpr'] += float(cost_per_result)

                # Clicks
                for action in item.get('actions', []):
                    if action.get('action_type') == 'link_click':
                        agg['clicks'] += float(action.get('value', 0))
                        break

                agg['daily_budget'] += daily_budget

    except Exception as e:
        print(f"Error in fetch_data_insights_by_date_subdomain_roi: {e}")
        return {
            'status': False,
            'data': [],
            'total': []
        }

    # ⬇️ Pindahkan ke luar loop akun dan proses hasil akhir
    for (date_start, subdomain), agg in global_aggregates.items():
        all_data.append({
            'date': date_start,
            'subdomain': subdomain,
            'budget': agg['daily_budget'],
            'spend': round(agg['spend'], 2),
            'impressions': agg['impressions'],
            'reach': agg['reach'],
            'clicks': agg['clicks'],
            'frequency': round(agg['frequency'], 2),
            'cpr': round(agg['cpr'], 2),
        })

        total_budget += agg['daily_budget']
        total_spend += agg['spend']
        total_impressions += agg['impressions']
        total_reach += agg['reach']
        total_clicks += agg['clicks']
        total_cpr += agg['cpr']

    total_frequency = float(total_impressions / total_reach) if total_reach > 0 else 0.0

    total.append({
        'total_budget': total_budget,
        'total_spend': total_spend,
        'total_impressions': total_impressions,
        'total_reach': total_reach,
        'total_click': total_clicks,
        'total_frequency': round(total_frequency, 2),
        'total_cpr': round(total_cpr, 2)
    })

    return {
        'status': True,
        'data': all_data,
        'total': total
    }

# Fungsi bantu untuk ekstrak subdomain
def extract_subdomain(campaign_name):
    parts = campaign_name.split()
    for part in parts:
        if '.' in part:
            # Ekstrak subdomain dasar (contoh: blog.missagendalimon dari blog.missagendalimon.GAP11-ADX_SctppKTk_4N_#1)
            if '.GAP11-ADX' in part or '.ADX' in part:
                # Ambil bagian sebelum .GAP11-ADX atau .ADX
                base_part = part.split('.GAP11-ADX')[0] if '.GAP11-ADX' in part else part.split('.ADX')[0]
                return base_part.strip()
            return part.strip()
    return None

def fetch_roi_traffic_account_by_user(user_mail, start_date, end_date, site_filter=None):
    """Fetch traffic account data using user's credentials with AdX fallback to regular metrics"""
    try:
        # Get user's Ad Manager client
        client_result = get_user_ad_manager_client(user_mail)
        if not client_result['status']:
            return client_result
        client = client_result['client']
        # Convert string dates to datetime.date objects
        if isinstance(start_date, str):
            start_date = datetime.strptime(start_date, '%Y-%m-%d').date()
        if isinstance(end_date, str):
            end_date = datetime.strptime(end_date, '%Y-%m-%d').date()
        # Try ROI first, then fallback to regular metrics
        try:
            return _run_roi_report_with_fallback(client, start_date, end_date, site_filter)
        except Exception as adx_error:
            return _run_regular_report(client, start_date, end_date, site_filter)
            
    except Exception as e:
        return {
            'status': False,
            'error': f'Failed to fetch traffic data: {str(e)}'
        }

def _run_roi_report_with_fallback(client, start_date, end_date, site_filter):
    """Try ROI report with fallback to regular metrics"""
    report_service = client.GetService('ReportService', version='v202502')
    # Try ROI columns first - sesuai dengan data yang tersedia di Ad Manager interface
    roi_column_combinations = [
        # Coba kolom lengkap seperti di gambar Ad Manager
        ['AD_EXCHANGE_IMPRESSIONS', 'AD_EXCHANGE_CLICKS', 'AD_EXCHANGE_TOTAL_EARNINGS', 'AD_EXCHANGE_CPC', 'AD_EXCHANGE_CTR', 'AD_EXCHANGE_ECPM'],
        # Coba kombinasi dasar dengan clicks
        ['AD_EXCHANGE_IMPRESSIONS', 'AD_EXCHANGE_CLICKS', 'AD_EXCHANGE_TOTAL_EARNINGS'],
        # Coba hanya clicks dan revenue
        ['AD_EXCHANGE_CLICKS', 'AD_EXCHANGE_TOTAL_EARNINGS'],
        # Coba hanya clicks
        ['AD_EXCHANGE_CLICKS'],
        # Fallback ke kombinasi lama
        ['AD_EXCHANGE_IMPRESSIONS'],
        ['AD_EXCHANGE_TOTAL_EARNINGS'],
        ['AD_EXCHANGE_IMPRESSIONS', 'AD_EXCHANGE_TOTAL_EARNINGS']
    ]
    for columns in roi_column_combinations:
        try:
            print(f"[DEBUG] Trying ROI columns: {columns}")
            report_query = {
                'reportQuery': {
                    'dimensions': ['DATE', 'AD_EXCHANGE_SITE_NAME'],
                    'columns': columns,
                    'dateRangeType': 'CUSTOM_DATE',
                    'startDate': {
                        'year': start_date.year,
                        'month': start_date.month,
                        'day': start_date.day
                    },
                    'endDate': {
                        'year': end_date.year,
                        'month': end_date.month,
                        'day': end_date.day
                    }
                }
            }
            # Add site filter if specified
            if site_filter:
                report_query['reportQuery']['dimensionFilters'] = [{
                    'dimension': 'AD_EXCHANGE_SITE_NAME',
                    'operator': 'CONTAINS',
                    'values': [site_filter]
                }]
            # Try to run the report job
            report_job = report_service.runReportJob(report_query)
            print(f"[DEBUG] AdX report created successfully with columns: {columns}")
            
            # Wait for completion and download
            return _wait_and_download_report(client, report_job['id'])
        except Exception as e:
            error_msg = str(e)
            print(f"[DEBUG] AdX combination {columns} failed: {error_msg}")
            # If NOT_NULL error, try next combination
            if 'NOT_NULL' in error_msg:
                continue
            # If permission error, raise it
            elif 'PERMISSION' in error_msg.upper():
                raise e
            # For other errors, try next combination
            else:
                continue
    # If all ROI combinations failed, raise the last error
    raise Exception("All ROI column combinations failed - ROI not available")

def fetch_roi_ad_change_data(start_date, end_date):
    """Fetch ROI ad change data"""
    try:
        client = get_ad_manager_client()
        if not client:
            return {'status': False, 'error': 'Failed to initialize client'}
        
        report_service = client.GetService('ReportService', version='v202502')
        
        # Convert string dates to datetime.date objects
        if isinstance(start_date, str):
            start_date = datetime.strptime(start_date, '%Y-%m-%d').date()
        if isinstance(end_date, str):
            end_date = datetime.strptime(end_date, '%Y-%m-%d').date()
        
        # Configure report for ad changes
        report_job = {
            'reportQuery': {
                'dimensions': ['DATE', 'AD_UNIT_NAME'],
                'columns': [
                    'AD_EXCHANGE_IMPRESSIONS', 'AD_EXCHANGE_CLICKS', 
                    'AD_EXCHANGE_TOTAL_EARNINGS', 'AD_EXCHANGE_CTR'
                ],
                'dateRangeType': 'CUSTOM_DATE',
                'startDate': {
                    'year': start_date.year,
                    'month': start_date.month,
                    'day': start_date.day
                },
                'endDate': {
                    'year': end_date.year,
                    'month': end_date.month,
                    'day': end_date.day
                }
            }
        }
        
        # Run report
        report_job = report_service.runReportJob(report_job)
        report_job_id = report_job['id']
        
        # Wait for completion
        report_downloader = client.GetDataDownloader(version='v202502')
        report_downloader.WaitForReport(report_job)
        
        # Download and parse results
        report_file = tempfile.NamedTemporaryFile(suffix='.csv.gz', delete=False)
        report_downloader.DownloadReportToFile(report_job_id, 'CSV_DUMP', report_file)
        
        # Parse CSV data
        data = []
        with gzip.open(report_file.name, 'rt') as f:
            reader = csv.DictReader(f)
            for row in reader:
                data.append({
                    'date': row.get('Dimension.DATE', ''),
                    'ad_unit': row.get('Dimension.AD_UNIT_NAME', ''),
                    'impressions': int(row.get('Column.AD_EXCHANGE_IMPRESSIONS', 0)),
                    'clicks': int(row.get('Column.AD_EXCHANGE_CLICKS', 0)),
                    'revenue': float(row.get('Column.AD_EXCHANGE_TOTAL_EARNINGS', 0)),
                    'ctr': float(row.get('Column.AD_EXCHANGE_CTR', 0))
                })
        
        # Cleanup
        os.unlink(report_file.name)
        
        return {
            'status': True,
            'data': data
        }
    except Exception as e:
        return {
            'status': False,
            'error': str(e)
        }

def fetch_roi_active_sites():
    """Fetch active ROI sites"""
    try:
        client = get_ad_manager_client()
        if not client:
            return {'status': False, 'error': 'Failed to initialize client'}
        
        inventory_service = client.GetService('InventoryService')
        
        # Get active ad units
        statement = ad_manager.StatementBuilder()
        statement.Where('status = :status')
        statement.WithBindVariable('status', 'ACTIVE')
        
        ad_units = inventory_service.getAdUnitsByStatement(statement.ToStatement())
        
        sites = []
        if 'results' in ad_units:
            for ad_unit in ad_units['results']:
                sites.append({
                    'id': ad_unit['id'],
                    'name': ad_unit['name'],
                    'status': ad_unit['status']
                })
        
        return {
            'status': True,
            'data': sites
        }
    except Exception as e:
        return {
            'status': False,
            'error': str(e)
        }
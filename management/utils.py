from datetime import datetime, timedelta
from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.adsinsights import AdsInsights
from facebook_business.adobjects.campaign import Campaign
from facebook_business.adobjects.adset import AdSet
from facebook_business.adobjects.ad import Ad
from facebook_business.exceptions import FacebookRequestError
from collections import defaultdict
from datetime import datetime, timedelta
from googleads import ad_manager
import yaml
import tempfile
import csv
import gzip
import os

import requests
import pycountry
import hashlib
import json
from django.core.cache import cache
from django.conf import settings


def get_last_5_days_indonesia_format(hari:int):
    hari_ini = datetime.now()
    tanggal_list = [
        (hari_ini - timedelta(days=i)).strftime('%d-%m-%Y')
        for i in reversed(range(hari))
    ]
    return tanggal_list

def get_country_name_from_code(code):
    try:
        country = pycountry.countries.get(alpha_2=code.upper())
        return country.name if country else None
    except Exception:
        return None


# Redis Cache Utility Functions
def generate_cache_key(prefix, *args, **kwargs):
    """
    Generate a unique cache key based on function parameters
    """
    # Create a string from all arguments
    key_parts = [str(prefix)]
    for arg in args:
        if isinstance(arg, (list, dict)):
            key_parts.append(json.dumps(arg, sort_keys=True))
        else:
            key_parts.append(str(arg))
    
    for k, v in sorted(kwargs.items()):
        if isinstance(v, (list, dict)):
            key_parts.append(f"{k}:{json.dumps(v, sort_keys=True)}")
        else:
            key_parts.append(f"{k}:{v}")
    
    # Create hash of the key to ensure consistent length
    key_string = "|".join(key_parts)
    return hashlib.md5(key_string.encode()).hexdigest()


def get_cached_data(cache_key):
    """
    Get data from Redis cache
    """
    try:
        return cache.get(cache_key)
    except Exception as e:
        print(f"Cache get error: {e}")
        return None


def set_cached_data(cache_key, data, timeout=None):
    """
    Set data to Redis cache with optional timeout and fallback cache
    """
    try:
        if timeout is None:
            timeout = getattr(settings, 'CACHE_TIMEOUTS', {}).get('facebook_insights', 300)
        # Set normal cache with timeout
        cache.set(cache_key, data, timeout)
        # Set fallback cache with longer timeout (24 hours) for rate limit scenarios
        fallback_key = f"fallback_{cache_key}"
        cache.set(fallback_key, data, 86400)  # 24 hours
        return True
    except Exception as e:
        print(f"Cache set error: {e}")
        return False


def invalidate_cache_pattern(pattern):
    """
    Invalidate cache keys matching a pattern
    """
    try:
        # For django-redis, we can use delete_pattern
        if hasattr(cache, 'delete_pattern'):
            cache.delete_pattern(f"*{pattern}*")
        return True
    except Exception as e:
        print(f"Cache invalidation error: {e}")
        return False


def cache_facebook_insights(func):
    """
    Decorator to cache Facebook insights functions with error handling
    """
    def wrapper(*args, **kwargs):
        # Generate cache key based on function name and parameters
        cache_key = generate_cache_key(func.__name__, *args, **kwargs)
        
        # Try to get from cache first
        cached_result = get_cached_data(cache_key)
        if cached_result is not None:
            print(f"Cache hit for {func.__name__}")
            return cached_result
        
        # If not in cache, execute function with error handling
        print(f"Cache miss for {func.__name__}, fetching from API")
        try:
            result = func(*args, **kwargs)
            
            # Cache the result if successful
            timeout = getattr(settings, 'CACHE_TIMEOUTS', {}).get('facebook_insights', 300)
            set_cached_data(cache_key, result, timeout)
            
            return result
        except FacebookRequestError as e:
            # If rate limit error, return cached data if available (even expired)
            if 'User request limit reached' in str(e):
                print(f"Rate limit reached for {func.__name__}, checking for any cached data")
                # Try to get cached data without expiration check
                cache_key_fallback = f"fallback_{cache_key}"
                fallback_result = cache.get(cache_key_fallback)
                if fallback_result is not None:
                    print(f"Using fallback cache for {func.__name__}")
                    return fallback_result
            # Re-raise the error if no fallback available
            raise e
    return wrapper


def invalidate_facebook_cache(account_ids=None, date_range=None):
    """
    Invalidate Facebook insights cache for specific accounts or date ranges
    """
    try:
        patterns = []
        
        if account_ids:
            if isinstance(account_ids, str):
                account_ids = [account_ids]
            for account_id in account_ids:
                patterns.append(f"*{account_id}*")
        
        if date_range:
            start_date, end_date = date_range
            patterns.extend([f"*{start_date}*", f"*{end_date}*"])
        
        # If no specific patterns, invalidate all Facebook insights cache
        if not patterns:
            patterns = ['fetch_data_all_insights', 'fetch_data_insights']
        
        for pattern in patterns:
            invalidate_cache_pattern(pattern)
        
        print(f"Cache invalidated for patterns: {patterns}")
        return True
    except Exception as e:
        print(f"Cache invalidation error: {e}")
        return False


def clear_all_facebook_cache():
    """
    Clear all Facebook insights related cache
    """
    try:
        patterns = [
            'fetch_data_all_insights',
            'fetch_data_insights',
            'get_facebook_insights'
        ]
        
        for pattern in patterns:
            invalidate_cache_pattern(pattern)
        
        print("All Facebook insights cache cleared")
        return True
    except Exception as e:
        print(f"Error clearing Facebook cache: {e}")
        return False


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
    if data_sub_domain != '%':
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
    for country, data in country_totals.items():
        spend = data['spend']
        impressions = data['impressions']
        reach = data['reach']
        clicks = data['clicks']
        frequency = data['frequency']
        cpr = data['cpr']
        total_spend += spend
        total_impressions += impressions
        total_reach += reach
        total_clicks += clicks
        total_frequency = float(impressions / reach)
        total_cpr += cpr
        result.append({
            'country': country,
            'spend': round(spend, 2),
            'impressions': impressions,
            'reach': reach,
            'clicks': clicks,
            'frequency': round(frequency, 0),
            'cpr': round(cpr, 0),
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

def get_ad_manager_client():
    """Inisialisasi client Google Ad Manager"""
    try:
        # Path ke file konfigurasi
        yaml_file = os.path.join(os.path.dirname(__file__), '..', 'googleads.yaml')
        
        # Inisialisasi client
        client = ad_manager.AdManagerClient.LoadFromStorage(yaml_file)
        return client
    except Exception as e:
        raise Exception(f"Error initializing Ad Manager client: {str(e)}")

def fetch_ad_manager_reports(start_date, end_date, report_type='HISTORICAL'):
    """Mengambil laporan dari Google Ad Manager"""
    try:
        client = get_ad_manager_client()
        report_service = client.GetService('ReportService')
        
        # Konfigurasi laporan
        report_job = {
            'reportQuery': {
                'dimensions': ['DATE', 'AD_UNIT_NAME', 'ADVERTISER_NAME'],
                'columns': ['TOTAL_IMPRESSIONS', 'TOTAL_CLICKS', 'TOTAL_REVENUE'],
                'dateRangeType': 'CUSTOM_DATE',
                'startDate': start_date,
                'endDate': end_date
            }
        }
        
        # Jalankan laporan
        report_job = report_service.runReportJob(report_job)
        report_job_id = report_job['id']
        
        # Tunggu hingga selesai
        report_downloader = client.GetDataDownloader()
        report_downloader.WaitForReport(report_job)
        
        # Download hasil
        report_file = tempfile.NamedTemporaryFile(
            suffix='.csv.gz', delete=False
        )
        
        report_downloader.DownloadReportToFile(
            report_job_id, 'CSV_DUMP', report_file
        )
        
        return report_file.name
        
    except Exception as e:
        return {'error': str(e)}

def fetch_ad_manager_inventory():
    """Mengambil data inventory dari Ad Manager"""
    try:
        client = get_ad_manager_client()
        inventory_service = client.GetService('InventoryService')
        
        # Query untuk mengambil ad units
        statement = ad_manager.StatementBuilder(version='v202308')
        statement.Where('status = :status')
        statement.WithBindVariable('status', 'ACTIVE')
        
        response = inventory_service.getAdUnitsByStatement(
            statement.ToStatement()
        )
        
        ad_units = []
        if 'results' in response:
            for ad_unit in response['results']:
                ad_units.append({
                    'id': ad_unit['id'],
                    'name': ad_unit['name'],
                    'status': ad_unit['status'],
                    'adUnitCode': ad_unit.get('adUnitCode', ''),
                    'parentId': ad_unit.get('parentId', '')
                })
        
        return {'status': True, 'data': ad_units}
        
    except Exception as e:
        return {'status': False, 'error': str(e)}

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
            if data_sub_domain != '%' and data_sub_domain:
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

# ===== AdX Manager Utility Functions =====

def fetch_adx_summary_data(start_date, end_date):
    """Mengambil data summary AdX untuk dashboard"""
    try:
        client = get_ad_manager_client()
        report_service = client.GetService('ReportService')
        
        # Konfigurasi laporan summary
        report_job = {
            'reportQuery': {
                'dimensions': ['DATE'],
                'columns': [
                    'TOTAL_IMPRESSIONS', 'TOTAL_CLICKS', 'TOTAL_REVENUE',
                    'TOTAL_AD_REQUESTS', 'TOTAL_MATCHED_REQUESTS'
                ],
                'dateRangeType': 'CUSTOM_DATE',
                'startDate': start_date,
                'endDate': end_date
            }
        }
        
        # Jalankan laporan
        report_job = report_service.runReportJob(report_job)
        report_job_id = report_job['id']
        
        # Tunggu hingga selesai
        report_downloader = client.GetDataDownloader()
        report_downloader.WaitForReport(report_job)
        
        # Download dan parse hasil
        report_file = tempfile.NamedTemporaryFile(suffix='.csv.gz', delete=False)
        report_downloader.DownloadReportToFile(report_job_id, 'CSV_DUMP', report_file)
        
        # Parse CSV data
        data = []
        total_impressions = 0
        total_clicks = 0
        total_revenue = 0.0
        total_requests = 0
        total_matched = 0
        
        with gzip.open(report_file.name, 'rt') as f:
            reader = csv.DictReader(f)
            for row in reader:
                impressions = int(row.get('Dimension.TOTAL_IMPRESSIONS', 0))
                clicks = int(row.get('Dimension.TOTAL_CLICKS', 0))
                revenue = float(row.get('Dimension.TOTAL_REVENUE', 0))
                requests = int(row.get('Dimension.TOTAL_AD_REQUESTS', 0))
                matched = int(row.get('Dimension.TOTAL_MATCHED_REQUESTS', 0))
                
                data.append({
                    'date': row.get('Dimension.DATE', ''),
                    'impressions': impressions,
                    'clicks': clicks,
                    'revenue': revenue,
                    'requests': requests,
                    'matched': matched,
                    'ctr': (clicks / impressions * 100) if impressions > 0 else 0,
                    'fill_rate': (matched / requests * 100) if requests > 0 else 0
                })
                
                total_impressions += impressions
                total_clicks += clicks
                total_revenue += revenue
                total_requests += requests
                total_matched += matched
        
        # Cleanup
        os.unlink(report_file.name)
        
        return {
            'status': True,
            'data': data,
            'summary': {
                'total_impressions': total_impressions,
                'total_clicks': total_clicks,
                'total_revenue': total_revenue,
                'total_requests': total_requests,
                'total_matched': total_matched,
                'overall_ctr': (total_clicks / total_impressions * 100) if total_impressions > 0 else 0,
                'overall_fill_rate': (total_matched / total_requests * 100) if total_requests > 0 else 0
            }
        }
        
    except Exception as e:
        return {'status': False, 'error': str(e)}

def fetch_adx_account_data():
    """Mengambil data account AdX"""
    try:
        client = get_ad_manager_client()
        network_service = client.GetService('NetworkService')
        
        # Ambil informasi network/account
        current_network = network_service.getCurrentNetwork()
        
        account_data = {
            'id': current_network['id'],
            'displayName': current_network['displayName'],
            'networkCode': current_network['networkCode'],
            'timeZone': current_network['timeZone'],
            'currencyCode': current_network['currencyCode'],
            'effectiveRootAdUnitId': current_network.get('effectiveRootAdUnitId', ''),
            'isTest': current_network.get('isTest', False)
        }
        
        return {'status': True, 'data': account_data}
        
    except Exception as e:
        return {'status': False, 'error': str(e)}

def fetch_adx_traffic_per_account(start_date, end_date, account_filter=None):
    """Mengambil data traffic per account AdX"""
    try:
        client = get_ad_manager_client()
        report_service = client.GetService('ReportService')
        
        # Konfigurasi laporan per account
        report_job = {
            'reportQuery': {
                'dimensions': ['AD_UNIT_NAME', 'DATE'],
                'columns': [
                    'TOTAL_IMPRESSIONS', 'TOTAL_CLICKS', 'TOTAL_REVENUE',
                    'TOTAL_AD_REQUESTS', 'TOTAL_MATCHED_REQUESTS'
                ],
                'dateRangeType': 'CUSTOM_DATE',
                'startDate': start_date,
                'endDate': end_date
            }
        }
        
        # Tambahkan filter jika ada
        if account_filter:
            report_job['reportQuery']['statement'] = {
                'query': f"WHERE AD_UNIT_NAME LIKE '%{account_filter}%'"
            }
        
        # Jalankan laporan
        report_job = report_service.runReportJob(report_job)
        report_job_id = report_job['id']
        
        # Tunggu hingga selesai
        report_downloader = client.GetDataDownloader()
        report_downloader.WaitForReport(report_job)
        
        # Download dan parse hasil
        report_file = tempfile.NamedTemporaryFile(suffix='.csv.gz', delete=False)
        report_downloader.DownloadReportToFile(report_job_id, 'CSV_DUMP', report_file)
        
        # Parse CSV data
        data = []
        account_totals = {}
        
        with gzip.open(report_file.name, 'rt') as f:
            reader = csv.DictReader(f)
            for row in reader:
                ad_unit = row.get('Dimension.AD_UNIT_NAME', '')
                date = row.get('Dimension.DATE', '')
                impressions = int(row.get('Column.TOTAL_IMPRESSIONS', 0))
                clicks = int(row.get('Column.TOTAL_CLICKS', 0))
                revenue = float(row.get('Column.TOTAL_REVENUE', 0))
                requests = int(row.get('Column.TOTAL_AD_REQUESTS', 0))
                matched = int(row.get('Column.TOTAL_MATCHED_REQUESTS', 0))
                
                row_data = {
                    'ad_unit': ad_unit,
                    'date': date,
                    'impressions': impressions,
                    'clicks': clicks,
                    'revenue': revenue,
                    'requests': requests,
                    'matched': matched,
                    'ctr': (clicks / impressions * 100) if impressions > 0 else 0,
                    'fill_rate': (matched / requests * 100) if requests > 0 else 0
                }
                
                data.append(row_data)
                
                # Aggregate per account
                if ad_unit not in account_totals:
                    account_totals[ad_unit] = {
                        'impressions': 0, 'clicks': 0, 'revenue': 0.0,
                        'requests': 0, 'matched': 0
                    }
                
                account_totals[ad_unit]['impressions'] += impressions
                account_totals[ad_unit]['clicks'] += clicks
                account_totals[ad_unit]['revenue'] += revenue
                account_totals[ad_unit]['requests'] += requests
                account_totals[ad_unit]['matched'] += matched
        
        # Cleanup
        os.unlink(report_file.name)
        
        # Format account totals
        account_summary = []
        for ad_unit, totals in account_totals.items():
            account_summary.append({
                'ad_unit': ad_unit,
                'total_impressions': totals['impressions'],
                'total_clicks': totals['clicks'],
                'total_revenue': totals['revenue'],
                'total_requests': totals['requests'],
                'total_matched': totals['matched'],
                'ctr': (totals['clicks'] / totals['impressions'] * 100) if totals['impressions'] > 0 else 0,
                'fill_rate': (totals['matched'] / totals['requests'] * 100) if totals['requests'] > 0 else 0
            })
        
        return {
            'status': True,
            'data': data,
            'account_summary': account_summary
        }
        
    except Exception as e:
        return {'status': False, 'error': str(e)}

def fetch_adx_traffic_per_campaign(start_date, end_date, campaign_filter=None):
    """Mengambil data traffic per campaign AdX"""
    try:
        client = get_ad_manager_client()
        report_service = client.GetService('ReportService')
        
        # Konfigurasi laporan per campaign
        report_job = {
            'reportQuery': {
                'dimensions': ['ORDER_NAME', 'LINE_ITEM_NAME', 'DATE'],
                'columns': [
                    'TOTAL_IMPRESSIONS', 'TOTAL_CLICKS', 'TOTAL_REVENUE',
                    'TOTAL_AD_REQUESTS', 'TOTAL_MATCHED_REQUESTS'
                ],
                'dateRangeType': 'CUSTOM_DATE',
                'startDate': start_date,
                'endDate': end_date
            }
        }
        
        # Tambahkan filter jika ada
        if campaign_filter:
            report_job['reportQuery']['statement'] = {
                'query': f"WHERE ORDER_NAME LIKE '%{campaign_filter}%' OR LINE_ITEM_NAME LIKE '%{campaign_filter}%'"
            }
        
        # Jalankan laporan
        report_job = report_service.runReportJob(report_job)
        report_job_id = report_job['id']
        
        # Tunggu hingga selesai
        report_downloader = client.GetDataDownloader()
        report_downloader.WaitForReport(report_job)
        
        # Download dan parse hasil
        report_file = tempfile.NamedTemporaryFile(suffix='.csv.gz', delete=False)
        report_downloader.DownloadReportToFile(report_job_id, 'CSV_DUMP', report_file)
        
        # Parse CSV data
        data = []
        campaign_totals = {}
        
        with gzip.open(report_file.name, 'rt') as f:
            reader = csv.DictReader(f)
            for row in reader:
                order_name = row.get('Dimension.ORDER_NAME', '')
                line_item = row.get('Dimension.LINE_ITEM_NAME', '')
                date = row.get('Dimension.DATE', '')
                impressions = int(row.get('Column.TOTAL_IMPRESSIONS', 0))
                clicks = int(row.get('Column.TOTAL_CLICKS', 0))
                revenue = float(row.get('Column.TOTAL_REVENUE', 0))
                requests = int(row.get('Column.TOTAL_AD_REQUESTS', 0))
                matched = int(row.get('Column.TOTAL_MATCHED_REQUESTS', 0))
                
                campaign_key = f"{order_name} - {line_item}"
                
                row_data = {
                    'order_name': order_name,
                    'line_item': line_item,
                    'campaign': campaign_key,
                    'date': date,
                    'impressions': impressions,
                    'clicks': clicks,
                    'revenue': revenue,
                    'requests': requests,
                    'matched': matched,
                    'ctr': (clicks / impressions * 100) if impressions > 0 else 0,
                    'fill_rate': (matched / requests * 100) if requests > 0 else 0
                }
                
                data.append(row_data)
                
                # Aggregate per campaign
                if campaign_key not in campaign_totals:
                    campaign_totals[campaign_key] = {
                        'impressions': 0, 'clicks': 0, 'revenue': 0.0,
                        'requests': 0, 'matched': 0, 'order_name': order_name,
                        'line_item': line_item
                    }
                
                campaign_totals[campaign_key]['impressions'] += impressions
                campaign_totals[campaign_key]['clicks'] += clicks
                campaign_totals[campaign_key]['revenue'] += revenue
                campaign_totals[campaign_key]['requests'] += requests
                campaign_totals[campaign_key]['matched'] += matched
        
        # Cleanup
        os.unlink(report_file.name)
        
        # Format campaign totals
        campaign_summary = []
        for campaign, totals in campaign_totals.items():
            campaign_summary.append({
                'campaign': campaign,
                'order_name': totals['order_name'],
                'line_item': totals['line_item'],
                'total_impressions': totals['impressions'],
                'total_clicks': totals['clicks'],
                'total_revenue': totals['revenue'],
                'total_requests': totals['requests'],
                'total_matched': totals['matched'],
                'ctr': (totals['clicks'] / totals['impressions'] * 100) if totals['impressions'] > 0 else 0,
                'fill_rate': (totals['matched'] / totals['requests'] * 100) if totals['requests'] > 0 else 0
            })
        
        return {
            'status': True,
            'data': data,
            'campaign_summary': campaign_summary
        }
        
    except Exception as e:
        return {'status': False, 'error': str(e)}

def fetch_adx_traffic_per_country(start_date, end_date, country_filter=None):
    """Mengambil data traffic per country AdX"""
    try:
        client = get_ad_manager_client()
        report_service = client.GetService('ReportService')
        
        # Konfigurasi laporan per country
        report_job = {
            'reportQuery': {
                'dimensions': ['COUNTRY_NAME', 'DATE'],
                'columns': [
                    'TOTAL_IMPRESSIONS', 'TOTAL_CLICKS', 'TOTAL_REVENUE',
                    'TOTAL_AD_REQUESTS', 'TOTAL_MATCHED_REQUESTS'
                ],
                'dateRangeType': 'CUSTOM_DATE',
                'startDate': start_date,
                'endDate': end_date
            }
        }
        
        # Tambahkan filter jika ada
        if country_filter:
            report_job['reportQuery']['statement'] = {
                'query': f"WHERE COUNTRY_NAME LIKE '%{country_filter}%'"
            }
        
        # Jalankan laporan
        report_job = report_service.runReportJob(report_job)
        report_job_id = report_job['id']
        
        # Tunggu hingga selesai
        report_downloader = client.GetDataDownloader()
        report_downloader.WaitForReport(report_job)
        
        # Download dan parse hasil
        report_file = tempfile.NamedTemporaryFile(suffix='.csv.gz', delete=False)
        report_downloader.DownloadReportToFile(report_job_id, 'CSV_DUMP', report_file)
        
        # Parse CSV data
        data = []
        country_totals = {}
        
        with gzip.open(report_file.name, 'rt') as f:
            reader = csv.DictReader(f)
            for row in reader:
                country = row.get('Dimension.COUNTRY_NAME', '')
                date = row.get('Dimension.DATE', '')
                impressions = int(row.get('Column.TOTAL_IMPRESSIONS', 0))
                clicks = int(row.get('Column.TOTAL_CLICKS', 0))
                revenue = float(row.get('Column.TOTAL_REVENUE', 0))
                requests = int(row.get('Column.TOTAL_AD_REQUESTS', 0))
                matched = int(row.get('Column.TOTAL_MATCHED_REQUESTS', 0))
                
                row_data = {
                    'country': country,
                    'date': date,
                    'impressions': impressions,
                    'clicks': clicks,
                    'revenue': revenue,
                    'requests': requests,
                    'matched': matched,
                    'ctr': (clicks / impressions * 100) if impressions > 0 else 0,
                    'fill_rate': (matched / requests * 100) if requests > 0 else 0
                }
                
                data.append(row_data)
                
                # Aggregate per country
                if country not in country_totals:
                    country_totals[country] = {
                        'impressions': 0, 'clicks': 0, 'revenue': 0.0,
                        'requests': 0, 'matched': 0
                    }
                
                country_totals[country]['impressions'] += impressions
                country_totals[country]['clicks'] += clicks
                country_totals[country]['revenue'] += revenue
                country_totals[country]['requests'] += requests
                country_totals[country]['matched'] += matched
        
        # Cleanup
        os.unlink(report_file.name)
        
        # Format country totals
        country_summary = []
        for country, totals in country_totals.items():
            country_summary.append({
                'country': country,
                'total_impressions': totals['impressions'],
                'total_clicks': totals['clicks'],
                'total_revenue': totals['revenue'],
                'total_requests': totals['requests'],
                'total_matched': totals['matched'],
                'ctr': (totals['clicks'] / totals['impressions'] * 100) if totals['impressions'] > 0 else 0,
                'fill_rate': (totals['matched'] / totals['requests'] * 100) if totals['requests'] > 0 else 0
            })
        
        return {
            'status': True,
            'data': data,
            'country_summary': country_summary
        }
        
    except Exception as e:
        return {'status': False, 'error': str(e)}
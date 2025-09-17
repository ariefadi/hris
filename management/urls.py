from unicodedata import name
from django.urls import path, include
from . import views
from . import views_adsense

urlpatterns = [
    # LOGIN / LOGOUT
    path('admin/login', views.LoginAdmin.as_view(), name='admin_login'),
    path('admin/login_process', views.LoginProcess.as_view(), name='admin_login_process'),
    path('admin/logout', views.LogoutAdmin.as_view(), name='admin_logout'),
    path('admin/oauth_redirect', views.redirect_login_user, name='oauth_redirect'),
    # DASHBOARD
    path('admin/dashboard', views.DashboardAdmin.as_view(), name='dashboard_admin'),
    path('admin/dashboard_data', views.DashboardData.as_view(), name='dashboard_data'),
    # USER MANAGEMENT
    # Menu Data User   
    path('admin/data_user', views.DataUser.as_view(), name='data_user'),
    path('admin/page_user', views.page_user.as_view()),
    path('admin/post_tambah_user', views.post_tambah_user.as_view()),
    path('admin/get_user_by_id/<str:user_id>', views.get_user_by_id.as_view()),
    path('admin/post_edit_user', views.post_edit_user.as_view()),
    # Menu Login User
    path('admin/data_login_user', views.DataLoginUser.as_view(), name='data_login_user'),
    path('admin/page_login_user', views.page_login_user.as_view()),
    # Menu Master Plan
    path('admin/master_plan', views.MasterPlan.as_view(), name='master_plan'),
    path('admin/page_master_plan', views.page_master_plan.as_view()),
    # path('admin/post_tambah_master_plan', views.post_tambah_master_plan.as_view()),
    # MENU FACEBOOK ADS 
    # Menu Summary Facebook Ads
    path('admin/summary_facebook', views.SummaryFacebookAds.as_view(), name='summary_facebook'),
    path('admin/page_summary_facebook', views.page_summary_facebook.as_view()),
    # Menu Account Facebook Ads
    path('admin/account_facebook', views.AccountFacebookAds.as_view(), name='account_facebook'),
    path('admin/page_account_facebook', views.page_account_facebook.as_view()),
    path('admin/post_account_ads', views.post_account_ads.as_view()),
    path('admin/edit_account_facebook/<str:account_ads_id>', views.EditAccountFacebookAds.as_view(), name='edit_account_facebook'),
    path('admin/update_account_facebook', views.UpdateAccountFacebookAds.as_view(), name='update_account_facebook'),
    # Menu Per Account Facebook Ads
    path('admin/per_account_facebook', views.PerAccountFacebookAds.as_view(), name='per_account_facebook'),
    path('admin/page_per_account_facebook', views.page_per_account_facebook.as_view()),
    path('admin/update_daily_budget_per_campaign', views.update_daily_budget_per_campaign.as_view()),
    path('admin/update_switch_campaign', views.update_switch_campaign.as_view()),
    path('admin/facebook_ads/bulk_update_campaign_status/', views.bulk_update_campaign_status.as_view()),
    # Menu Per Campaign Facebook Ads
    path('admin/per_campaign_facebook', views.PerCampaignFacebookAds.as_view(), name='per_campaign_facebook'),
    # path('admin/get_campaign_per_account', views.get_campaign_per_account.as_view()),
    path('admin/page_per_campaign_facebook', views.page_per_campaign_facebook.as_view()),
    # Menu Per Country Facebook Ads
    path('admin/per_country_facebook', views.PerCountryFacebookAds.as_view(), name='per_country_facebook'),
    path('admin/page_per_country_facebook', views.page_per_country_facebook.as_view()),
    # Cache Management
    path('admin/cache_stats', views.CacheStatsView.as_view(), name='cache_stats'),
    
    # MENU ADX MANAGER
    # Menu AdX Summary
    path('admin/adx_summary', views.AdxSummaryView.as_view(), name='adx_summary'),
    path('admin/page_adx_summary', views.AdxSummaryDataView.as_view(), name='adx_summary_data'),
    path('admin/page_adx_ad_change_data', views.AdxSummaryAdChangeDataView.as_view(), name='adx_ad_change_data'),
    path('admin/page_adx_active_sites', views.AdxActiveSitesView.as_view(), name='adx_active_sites'),
    # Menu AdX Account Data
    path('admin/adx_account', views.AdxAccountView.as_view(), name='adx_account'),
    path('admin/page_adx_account', views.AdxAccountDataView.as_view()),
    path('admin/page_adx_user_account', views.AdxUserAccountDataView.as_view()),
    path('admin/generate_refresh_token', views.GenerateRefreshTokenView.as_view(), name='generate_refresh_token'),
    path('admin/save_oauth_credentials', views.SaveOAuthCredentialsView.as_view(), name='save_oauth_credentials'),
    # AdX Traffic Account
    path('admin/adx_traffic_account', views.AdxTrafficPerAccountView.as_view(), name='adx_traffic_account'),
    path('adx-traffic-account/', views.AdxTrafficPerAccountView.as_view(), name='adx_traffic_account_alias'),
    path('admin/page_adx_traffic_account', views.AdxTrafficPerAccountDataView.as_view()),
    path('admin/adx_sites_list', views.AdxSitesListView.as_view(), name='adx_sites_list'),
    # Menu AdX Traffic Per Campaign
    path('admin/adx_traffic_campaign', views.AdxTrafficPerCampaignView.as_view(), name='adx_traffic_campaign'),
    path('admin/adx_traffic_campaign_data', views.AdxTrafficPerCampaignDataView.as_view(), name='adx_traffic_campaign_data'),
    # Menu AdX Traffic Per Country
    path('admin/adx_traffic_country', views.AdxTrafficPerCountryView.as_view(), name='adx_traffic_country'),
    path('admin/page_adx_traffic_country', views.AdxTrafficPerCountryDataView.as_view()),

    # MENU ADSENSE MANAGER
    # Menu Adsense Summary
    # path('admin/adsense_summary', views.AdsenseSummaryView.as_view(), name='adsense_summary'),
    # path('admin/adsense_summary_data/', views.AdsenseSummaryDataView.as_view()),
    # # Menu Adsense Account Data
    # path('admin/adsense_account', views.AdsenseAccountView.as_view(), name='adsense_account'), 
    # path('admin/page_adsense_account', views.AdsenseAccountDataView.as_view()),
    # Menu Adsense Traffic Account
    path('admin/adsense_traffic_account', views_adsense.AdsenseTrafficAccountView.as_view(), name='adsense_traffic_account'),
    path('admin/adsense_traffic_account_data', views_adsense.AdsenseTrafficAccountDataView.as_view(), name='adsense_traffic_account_data'),
    path('admin/adsense_sites_list', views_adsense.AdsenseSitesListView.as_view(), name='adsense_sites_list'),
    # # Menu Adsense Traffic Country
    # path('admin/adsense_traffic_country', views.AdsenseTrafficPerCountryView.as_view(), name='adsense_traffic_country'),
    # path('admin/page_adsense_traffic_country', views.AdsenseTrafficPerCountryDataView.as_view()),
    # # Menu Adsense Traffic Campaign
    # path('admin/adsense_traffic_campaign', views.AdsenseTrafficPerCampaignView.as_view(), name='adsense_traffic_campaign'),
    # path('admin/page_adsense_traffic_campaign', views.AdsenseTrafficPerCampaignDataView.as_view()),

    # REFRESH TOKEN MANAGEMENT
    path('admin/refresh_token', views.RefreshTokenManagement.as_view(), name='refresh_token_management'),
    path('admin/api/check_refresh_token', views.CheckRefreshTokenAPI.as_view(), name='check_refresh_token_api'),
    path('admin/api/generate_refresh_token', views.GenerateRefreshTokenAPI.as_view(), name='generate_refresh_token_api'),
    path('admin/api/get_all_users_refresh_token', views.GetAllUsersRefreshTokenAPI.as_view(), name='get_all_users_refresh_token_api'),

    # MENU REPORT
    # Menu Report ROI
    path('admin/roi_summary', views.RoiSummaryView.as_view(), name='roi_summary'),
    path('admin/page_roi_summary', views.RoiSummaryDataView.as_view()),
    path('admin/page_roi_ad_change_data', views.RoiSummaryAdChangeDataView.as_view(), name='roi_ad_change_data'),
    path('admin/page_roi_active_sites', views.RoiActiveSitesView.as_view(), name='roi_active_sites'),
    # Menu ROI Per Domain
    path('admin/roi_traffic_domain', views.RoiTrafficPerDomainView.as_view(), name='roi_traffic_domain'),
    path('admin/page_roi_traffic_domain', views.RoiTrafficPerDomainDataView.as_view()),
    path('admin/roi_sites_list', views.RoiSitesListView.as_view(), name='roi_sites_list'),
    # Menu ROI Per Country
    path('admin/roi_traffic_country', views.RoiTrafficPerCountryView.as_view(), name='roi_traffic_country'),
    path('admin/page_roi_traffic_country', views.RoiTrafficPerCountryDataView.as_view()),

    # test
    path('admin/fetch_report', views.fetch_report, name='fetch_report'),

]
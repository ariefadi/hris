from unicodedata import name
from django.urls import path, include
from . import views

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
    
    # MENU ADX MANAGER
    # Menu AdX Summary
    path('admin/adx_summary', views.AdxSummaryView.as_view(), name='adx_summary'),
    path('admin/page_adx_summary', views.AdxSummaryDataView.as_view()),
    # Menu AdX Account Data
    path('admin/adx_account', views.AdxAccountView.as_view(), name='adx_account'),
    path('admin/page_adx_account', views.AdxAccountDataView.as_view()),
    # Menu AdX Traffic Per Account
    path('admin/adx_traffic_account', views.AdxTrafficPerAccountView.as_view(), name='adx_traffic_account'),
    path('admin/page_adx_traffic_account', views.AdxTrafficPerAccountDataView.as_view()),
    # Menu AdX Traffic Per Campaign
    path('admin/adx_traffic_campaign', views.AdxTrafficPerCampaignView.as_view(), name='adx_traffic_campaign'),
    path('admin/page_adx_traffic_campaign', views.AdxTrafficPerCampaignDataView.as_view()),
    # Menu AdX Traffic Per Country
    path('admin/adx_traffic_country', views.AdxTrafficPerCountryView.as_view(), name='adx_traffic_country'),
    path('admin/page_adx_traffic_country', views.AdxTrafficPerCountryDataView.as_view()),
]
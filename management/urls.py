from unicodedata import name
from django.urls import path
from django.views.generic.base import RedirectView
from . import views
from . import views_adsense
from .oauth_views_package.oauth_views import (
    oauth_management_dashboard,
    oauth_status_api,
    generate_oauth_url_api,
    oauth_callback_api
)

urlpatterns = [
    # LOGIN / LOGOUT
    path('admin/login', views.LoginAdmin.as_view(), name='admin_login'),
    path('admin/login_process', views.LoginProcess.as_view(), name='admin_login_process'),
    path('admin/register_account', views.RegisterAccountAdmin.as_view(), name='admin_register_account'),
    path('admin/forgot_password', views.ForgotPasswordView.as_view(), name='forgot_password'),
    path('admin/logout', views.LogoutAdmin.as_view(), name='admin_logout'),
    path('admin/oauth_redirect', views.OAuthRedirectView.as_view(), name='oauth_redirect'),
    # DASHBOARD
    path('admin/dashboard', views.DashboardAdmin.as_view(), name='dashboard_admin'),
    path('admin/dashboard_data', views.DashboardData.as_view(), name='dashboard_data'),
    path('admin/dashboard_scoring_data', views.DashboardScoringDataView.as_view(), name='dashboard_scoring_data'),
    path('admin/dashboard_traffic_metrics', views.DashboardTrafficMetricsView.as_view(), name='dashboard_traffic_metrics'),
    path('admin/dashboard_country_traffic_metrics', views.DashboardCountryTrafficMetricsView.as_view(), name='dashboard_country_traffic_metrics'),
    path('admin/dashboard_create_scoring', views.DashboardCreateScoringView.as_view(), name='dashboard_create_scoring'),
    path('admin/dashboard_hourly_heatmap', views.DashboardDomainHourlyHeatmapView.as_view()),
    path('admin/dashboard_portfolio_pulse', views.DashboardPortfolioPulseView.as_view()),
    path('admin/dashboard_scoring_compare', views.DashboardScoringCompareView.as_view(), name='dashboard_scoring_compare'),
    path('admin/dashboard_scoring_rekap_compare', views.DashboardScoringRekapCompareView.as_view(), name='dashboard_scoring_rekap_compare'),
    path('admin/dashboard_sync', views.DashboardSyncView.as_view()),
    path('admin/dashboard/account_detail', views.DashboardAccountDetailPageView.as_view(), name='dashboard_account_detail'),
    path('admin/page_dashboard_account_detail', views.DashboardAccountDetailDataView.as_view(), name='dashboard_account_detail_data'),
    path('admin/dashboard_account_domain_suggest', views.DashboardAccountDomainSuggestView.as_view(), name='dashboard_account_domain_suggest'),
    # Portal switching
    path('admin/switch_portal/<str:portal_id>', views.SwitchPortal.as_view(), name='switch_portal'),
 
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
    path('admin/delete_account_facebook', views.DeleteAccountFacebookAds.as_view(), name='delete_account_facebook'),
    path('admin/facebook_account_token_check', views.FacebookAccountTokenCheckView.as_view(), name='facebook_account_token_check'),
    path('admin/facebook_account_token_check_all', views.FacebookAccountTokenCheckAllView.as_view(), name='facebook_account_token_check_all'),
    path('admin/facebook_account_token_extend', views.FacebookAccountTokenExtendView.as_view(), name='facebook_account_token_extend'),
    path('admin/facebook_account_oauth_start', views.FacebookAccountOAuthStartView.as_view(), name='facebook_account_oauth_start'),
    path('admin/facebook_account_oauth_callback', views.FacebookAccountOAuthCallbackView.as_view(), name='facebook_account_oauth_callback'),
    # Menu Per Account Facebook Ads
    path('admin/per_account_facebook', views.PerAccountFacebookAds.as_view(), name='per_account_facebook'),
    path('admin/create_campaign', views.CreateCampaignFacebookAds.as_view(), name='create_campaign_facebook'),
    path('admin/page_create_campaign_meta', views.CreateCampaignMetaListView.as_view(), name='page_create_campaign_meta'),
    path('admin/get_campaign_meta_detail', views.GetCampaignMetaDetailView.as_view(), name='get_campaign_meta_detail'),
    path('admin/update_campaign_meta', views.UpdateCampaignMetaView.as_view(), name='update_campaign_meta'),
    path('admin/page_per_account_facebook', views.page_per_account_facebook.as_view()),
    path('admin/update_daily_budget_per_campaign', views.update_daily_budget_per_campaign.as_view()),
    path('admin/update_switch_campaign', views.update_switch_campaign.as_view()),
    path('admin/create_campaign_per_account', views.create_campaign_per_account.as_view()),
    path('admin/create_campaign_fullstack_per_account', views.create_campaign_fullstack_per_account.as_view()),
    path('admin/facebook_adset_reach_estimate', views.FacebookAdsetReachEstimateView.as_view(), name='facebook_adset_reach_estimate'),
    path('admin/create_adset_ad_per_account', views.create_adset_ad_per_account.as_view()),
    path('admin/facebook_ads/bulk_update_campaign_status/', views.bulk_update_campaign_status.as_view()),
    # Menu Per Campaign Facebook Ads
    path('admin/per_campaign_facebook', views.PerCampaignFacebookAds.as_view(), name='per_campaign_facebook'),
    # path('admin/get_campaign_per_account', views.get_campaign_per_account.as_view()),
    path('admin/page_per_campaign_facebook', views.page_per_campaign_facebook.as_view()),
    path('admin/page_per_campaign_facebook_detail', views.page_per_campaign_facebook_detail.as_view()),
    # Menu Per Country Facebook Ads
    path('admin/per_country_facebook', views.PerCountryFacebookAds.as_view(), name='per_country_facebook'),
    path('admin/page_per_country_facebook', views.page_per_country_facebook.as_view()),
    path('admin/get_countries_facebook_ads', views.get_countries_facebook_ads, name='get_countries_facebook_ads'),
    path('admin/facebook_domain_suggest', views.FacebookDomainSuggestView.as_view(), name='facebook_domain_suggest'),
    path('admin/ads_invalid', views.InvalidReportAdsView.as_view(), name='invalid_report_ads'),
    path('admin/page_ads_invalid', views.InvalidReportAdsDataView.as_view(), name='invalid_report_ads_data'),
    path('admin/page_ads_invalid_detail', views.InvalidReportAdsDetailView.as_view(), name='invalid_report_ads_detail'),
    path('admin/facebook_language_suggest', views.FacebookLanguageSuggestView.as_view(), name='facebook_language_suggest'),
    path('admin/facebook_location_suggest', views.FacebookLocationSuggestView.as_view(), name='facebook_location_suggest'),
    path('admin/facebook_identity_suggest', views.FacebookIdentitySuggestView.as_view(), name='facebook_identity_suggest'),
    path('admin/facebook_page_messaging_assets', views.FacebookPageMessagingAssetsView.as_view(), name='facebook_page_messaging_assets'),
    path('admin/facebook_creative_media_library', views.FacebookCreativeMediaLibraryView.as_view(), name='facebook_creative_media_library'),
    path('admin/facebook_creative_media_thumb', views.FacebookCreativeMediaThumbView.as_view(), name='facebook_creative_media_thumb'),
    path('admin/facebook_creative_video_source', views.FacebookCreativeVideoSourceView.as_view(), name='facebook_creative_video_source'),
    path('admin/facebook_creative_discovery_links', views.FacebookCreativeDiscoveryLinksView.as_view(), name='facebook_creative_discovery_links'),
    path('admin/facebook_existing_post_library', views.FacebookExistingPostLibraryView.as_view(), name='facebook_existing_post_library'),
    path('admin/facebook_create_page_post', views.FacebookCreatePagePostView.as_view(), name='facebook_create_page_post'),
    path('admin/facebook_detailed_targeting_suggest', views.FacebookDetailedTargetingSuggestView.as_view(), name='facebook_detailed_targeting_suggest'),
    path('admin/facebook_detailed_targeting_browse', views.FacebookDetailedTargetingBrowseView.as_view(), name='facebook_detailed_targeting_browse'),
    path('admin/facebook_custom_audience_suggest', views.FacebookCustomAudienceSuggestView.as_view(), name='facebook_custom_audience_suggest'),
    # Cache Management
    path('admin/cache_stats', views.CacheStatsView.as_view(), name='cache_stats'),
    path('admin/ads_sites_list', views.AdsSitesListView.as_view(), name='ads_sites_list'),
    path('admin/ads_account_list', views.AdsAccountListView.as_view(), name='ads_account_list'),
    path('admin/ads_campaign_list', views.AdsCampaignListView.as_view(), name='ads_campaign_list'),

    # MENU ADX MANAGER
    # Menu AdX Summary
    path('admin/adx_summary', views.AdxSummaryView.as_view(), name='adx_summary'),
    path('admin/page_adx_summary', views.AdxSummaryDataView.as_view(), name='adx_summary_data'),
    path('admin/invalid_report_adx', views.InvalidReportAdxView.as_view(), name='invalid_report_adx'),
    path('admin/adx_invalid', views.InvalidReportAdxView.as_view(), name='adx_invalid'),
    path('admin/page_invalid_report_adx', views.InvalidReportAdxDataView.as_view(), name='invalid_report_adx_data'),
    path('admin/page_adx_invalid', views.InvalidReportAdxDataView.as_view(), name='adx_invalid_data'),
    path('admin/page_invalid_report_adx_detail', views.InvalidReportAdxDetailView.as_view(), name='invalid_report_adx_detail'),
    path('admin/page_adx_invalid_detail', views.InvalidReportAdxDetailView.as_view(), name='adx_invalid_detail'),
    path('admin/adx_domain_suggest', views.AdxDomainSuggestView.as_view(), name='adx_domain_suggest'),
    path('admin/page_adx_ad_change_data', views.AdxSummaryAdChangeDataView.as_view(), name='adx_ad_change_data'),
    path('admin/page_adx_active_sites', views.AdxActiveSitesView.as_view(), name='adx_active_sites'),
    # Menu AdX Account Data
    path('admin/adx_account', views.AdxAccountView.as_view(), name='adx_account'),
    path('admin/adx_account/oauth_start', views.AdxAccountOAuthStartView.as_view(), name='adx_account_oauth_start'),
    path('admin/adx_account/oauth_callback', views.AdxAccountOAuthCallbackView.as_view(), name='adx_account_oauth_callback'),
    path('admin/page_adx_account', views.AdxAccountDataView.as_view()),
    path('admin/page_adx_user_account', views.AdxUserAccountDataView.as_view()),
    path('admin/assign_account_user', views.AssignAccountUserView.as_view(), name='assign_account_user'),
    path('admin/update_account_name', views.UpdateAccountNameView.as_view(), name='update_account_name'),
    path('admin/delete_adx_account_credentials', views.DeleteAdxAccountCredentialsView.as_view(), name='delete_adx_account_credentials'),
    path('admin/generate_refresh_token', views.GenerateRefreshTokenView.as_view(), name='generate_refresh_token'),
    path('admin/save_oauth_credentials', views.SaveOAuthCredentialsView.as_view(), name='save_oauth_credentials'),
    # AdX Traffic Account
    path('admin/adx_traffic_account', views.AdxTrafficPerAccountView.as_view(), name='adx_traffic_account'),
    path('adx-traffic-account/', views.AdxTrafficPerAccountView.as_view(), name='adx_traffic_account_alias'),
    path('admin/page_adx_traffic_account', views.AdxTrafficPerAccountDataView.as_view()),
    path('admin/adx_sites_list', views.AdxSitesListView.as_view(), name='adx_sites_list'),
    path('admin/adx_accounts_list', views.AdxAccountListView.as_view(), name='adx_accounts_list'),
    # Menu AdX Traffic Per Campaign
    path('admin/adx_traffic_campaign', views.AdxTrafficPerCampaignView.as_view(), name='adx_traffic_campaign'),
    path('admin/adx_traffic_campaign_data', views.AdxTrafficPerCampaignDataView.as_view(), name='adx_traffic_campaign_data'),
    # Menu AdX Traffic Per Country
    path('admin/adx_traffic_country', views.AdxTrafficPerCountryView.as_view(), name='adx_traffic_country'),
    path('admin/page_adx_traffic_country', views.AdxTrafficPerCountryDataView.as_view()),
    path('admin/get_countries_adx', views.get_countries_adx, name='get_countries_adx'),
    # MENU ADSENSE MANAGER
    # Menu Adsense Summary
    path('admin/adsense_summary', views_adsense.AdsenseSummaryView.as_view(), name='adsense_summary'),
    path('admin/adsense_summary_data/', views_adsense.AdsenseSummaryDataView.as_view()),
    path('admin/invalid_report_adsense', views_adsense.InvalidReportAdsenseView.as_view(), name='invalid_report_adsense'),
    path('admin/adsense_invalid', views_adsense.InvalidReportAdsenseView.as_view(), name='adsense_invalid'),
    path('admin/page_invalid_report_adsense', views_adsense.InvalidReportAdsenseDataView.as_view(), name='invalid_report_adsense_data'),
    path('admin/page_adsense_invalid', views_adsense.InvalidReportAdsenseDataView.as_view(), name='adsense_invalid_data'),
    path('admin/page_invalid_report_adsense_detail', views_adsense.InvalidReportAdsenseDetailView.as_view(), name='invalid_report_adsense_detail'),
    path('admin/page_adsense_invalid_detail', views_adsense.InvalidReportAdsenseDetailView.as_view(), name='adsense_invalid_detail'),
    # Menu Adsense Account Data
    path('admin/adsense_account', views_adsense.AdsenseAccountView.as_view(), name='adsense_account'), 
    path('admin/page_adsense_account', views_adsense.AdsenseAccountDataView.as_view()),
    path('admin/adsense_credentials_list', views_adsense.AdsenseCredentialsListView.as_view(), name='adsense_credentials_list'),
    # Menu Adsense Traffic Account
    path('admin/adsense_traffic_account', views_adsense.AdsenseTrafficAccountView.as_view(), name='adsense_traffic_account'),
    path('admin/adsense_traffic_account_data', views_adsense.AdsenseTrafficAccountDataView.as_view(), name='adsense_traffic_account_data'),
    path('admin/adsense_sites_list', views_adsense.AdsenseSitesListView.as_view(), name='adsense_sites_list'),
    path('admin/adsense_accounts_list', views_adsense.AdsenseAccountListView.as_view(), name='adsense_accounts_list'),
    # # Menu Adsense Traffic Country
    path('admin/adsense_traffic_country', views_adsense.AdsenseTrafficPerCountryView.as_view(), name='adsense_traffic_country'),
    path('admin/adsense_traffic_country_data', views_adsense.AdsenseTrafficPerCountryDataView.as_view(), name='adsense_traffic_country_data'),
    path('admin/get_countries_adsense', views_adsense.get_countries_adsense, name='get_countries_adsense'),
    path('admin/adsense_domain_suggest', views_adsense.AdsenseDomainSuggestView.as_view(), name='adsense_domain_suggest'),
    # Menu AdSense Policy Events (Gmail)
    path('admin/adsense_policy_events', views_adsense.AdsensePolicyEventsView.as_view(), name='adsense_policy_events'),
    path('admin/adsense_policy_events_sync', views_adsense.AdsensePolicyEventsSyncView.as_view(), name='adsense_policy_events_sync'),

    # OAuth Management - menggunakan oauth_views_package untuk konsistensi
    path('admin/oauth/management/', oauth_management_dashboard, name='oauth_management_dashboard'),
    path('admin/oauth/status/', oauth_status_api, name='oauth_status_api'),
    path('admin/oauth/generate-url/', generate_oauth_url_api, name='generate_oauth_url_api'),
    path('admin/oauth/callback/', oauth_callback_api, name='oauth_callback_api'),
    
    # MENU REPORT
    # Menu Report Account
    path('admin/report_account', views.ReportAccountView.as_view(), name='report_account'),
    path('admin/report_account/detail', views.ReportAccountDetailPageView.as_view(), name='report_account_detail_page'),
    path('admin/page_report_account', views.ReportAccountDataView.as_view(), name='report_account_data'),
    path('admin/page_report_account_detail', views.ReportAccountDetailView.as_view(), name='report_account_detail'),
    path('admin/report_account_suggest', views.ReportAccountSuggestView.as_view(), name='report_account_suggest'),
    path('admin/report_account_domain_suggest', views.ReportAccountDomainSuggestView.as_view(), name='report_account_domain_suggest'),
    path('admin/report_account_lookup', views.ReportAccountLookupView.as_view(), name='report_account_lookup'),
    # Menu Report ROI
    path('admin/roi_summary', views.RoiSummaryView.as_view(), name='roi_summary'),
    # Menu ROI Per Domain
    path('admin/roi_traffic_domain', views.RoiTrafficPerDomainView.as_view(), name='roi_traffic_domain'),
    path('admin/page_roi_traffic_domain', views.RoiTrafficPerDomainDataView.as_view()),
    # Menu Traffic Per Domain
    path('admin/traffic_per_domain', views.TrafficPerDomainReportView.as_view(), name='report_traffic_domain'),
    path('admin/komparasi_traffic', views.KomparasiTrafficReportView.as_view(), name='komparasi_traffic'),
    path('admin/traffic_overlap', views.TrafficOverlapProxyView.as_view(), name='traffic_overlap_proxy'),
    path('admin/traffic_per_domain_ad_spend', views.TrafficPerDomainAdSpendView.as_view(), name='traffic_per_domain_ad_spend'),
    path('admin/traffic_per_campaign', views.TrafficPerCampaignReportView.as_view(), name='report_traffic_campaign'),
    path('admin/traffic_campaign_list', views.TrafficCampaignListView.as_view(), name='traffic_campaign_list'),
    # Menu ROI Per Country
    path('admin/roi_traffic_country', views.RoiTrafficPerCountryView.as_view(), name='roi_traffic_country'),
    path('admin/page_roi_traffic_country', views.RoiTrafficPerCountryDataView.as_view()),
    path('admin/page_roi_country_hourly', views.RoiCountryHourlyDataView.as_view()),
    # path('admin/roi_hourly_adx_filter', views.RoiHourlyAdxFilterView.as_view(), name='roi_hourly_adx_filter'),
    path('admin/roi_hourly_domain_filter', views.RoiHourlyDomainFilterView.as_view(), name='roi_hourly_domain_filter'),
    # Menu ROI Monitoring Domain
    path('admin/monitoring_domain', views.RoiMonitoringDomainView.as_view(), name='roi_monitoring_domain'),
    path('admin/page_monitoring_domain', views.RoiMonitoringDomainDataView.as_view()),
    path('admin/page_monitoring_domain_campaigns', views.RoiMonitoringDomainCampaignsView.as_view()),
    path('admin/page_monitoring_domain_campaign_breakdown', views.RoiMonitoringDomainCampaignBreakdownView.as_view()),
    path('admin/update_daily_budget_monitoring_domain_campaign', views.RoiMonitoringDomainUpdateDailyBudgetCampaignView.as_view()),
    path('admin/update_campaign_status_monitoring_domain_campaign', views.RoiMonitoringDomainUpdateCampaignStatusCampaignView.as_view()),
    # Menu ROI Monitoring Country
    path('admin/monitoring_country', views.RoiMonitoringCountryView.as_view(), name='roi_monitoring_country'),
    path('admin/page_monitoring_country', views.RoiMonitoringCountryDataView.as_view()),
    path('admin/page_monitoring_country_breakdown', views.RoiMonitoringCountryBreakdownView.as_view()),
    path('admin/page_monitoring_country_breakdown/', views.RoiMonitoringCountryBreakdownView.as_view()),
    path('page_monitoring_country_breakdown', views.RoiMonitoringCountryBreakdownView.as_view()),
    path('page_monitoring_country_breakdown/', views.RoiMonitoringCountryBreakdownView.as_view()),
    path('admin/page_monitoring_country_hourly_heatmap', views.RoiMonitoringCountryHourlyHeatmapView.as_view()),
    path('admin/page_monitoring_scoring_baseline_hourly', views.MonitoringScoringBaselineHourlyView.as_view()),
    # Menu ROI Rekapitulasi
    path('admin/rekapitulasi_earning', views.RoiRekapitulasiView.as_view(), name='roi_rekapitulasi'),
    path('admin/page_rekapitulasi_earning', views.RoiRekapitulasiDataView.as_view()),

    # Menu Report Adsense
    # Menu Report Summary Adsense
    path('admin/rekap_summary_adsense', views_adsense.RekapAdsenseSummaryView.as_view(), name='rekap_summary_adsense'),
    # Menu Rekap Adsense Per Domain
    path('admin/rekap_traffic_domain_adsense', views_adsense.RekapTrafficPerDomainView.as_view(), name='rekap_traffic_domain_adsense'),
    path('admin/page_rekap_traffic_domain_adsense', views_adsense.RekapTrafficPerDomainDataView.as_view()),
    # Menu Rekap Adsense Per Country
    path('admin/rekap_traffic_country_adsense', views_adsense.RekapTrafficPerCountryAdsenseView.as_view(), name='rekap_traffic_country_adsense'),
    path('admin/page_traffic_country_adsense', views_adsense.RekapTrafficPerCountryAdsenseDataView.as_view()),
    # Menu Rekap Monitoring Domain Adsense
    path('admin/monitor_domain_adsense', views_adsense.RoiMonitoringDomainAdsenseView.as_view(), name='monitor_domain_adsense'),
    path('admin/page_monitoring_domain_adsense', views_adsense.RoiMonitoringDomainAdsenseDataView.as_view()),
    path('admin/page_monitoring_domain_adsense_campaign_breakdown', views_adsense.RoiMonitoringDomainAdsenseCampaignBreakdownView.as_view()),
    # Menu Rekap Monitoring Country Adsense
    path('admin/monitor_country_adsense', views_adsense.RoiMonitoringCountryAdsenseView.as_view(), name='monitor_country_adsense'),
    path('admin/page_monitoring_country_adsense', views_adsense.RoiMonitoringCountryAdsenseDataView.as_view()),
    path('admin/page_monitoring_country_adsense_breakdown', views_adsense.RoiMonitoringCountryAdsenseBreakdownView.as_view()),
    # Menu Report Adsense Rekapitulasi
    path('admin/earning_adsense', views_adsense.AdsenseRekapitulasiView.as_view(), name='rekapitulasi_adsense'),
    path('admin/page_earning_adsense', views_adsense.AdsenseRekapitulasiDataView.as_view()),

    # test
    path('admin/fetch_report', views.fetch_report, name='fetch_report'),
    # Utility: Import OAuth client dari environment ke app_credentials
    path('admin/app_credentials/import_env', views.ImportEnvAppCredentialsView.as_view(), name='app_credentials_import_env'),

    # SETTINGS
    path('settings/overview', views.SettingsOverview.as_view(), name='settings_overview'),

]

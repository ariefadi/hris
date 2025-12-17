from django.urls import path
from . import task
from . import master

urlpatterns = [
    path('task/draft', task.DraftIndexView.as_view(), name='projects_task_draft'),
    path('task/draft/create', task.DraftCreateView.as_view(), name='projects_task_draft_create'),
    path('task/draft/<str:partner_id>/edit', task.DraftEditView.as_view(), name='projects_task_draft_edit'),
    path('task/draft/<str:partner_id>/delete', task.DraftDeleteView.as_view(), name='projects_task_draft_delete'),
    path('task/monitoring', task.MonitoringIndexView.as_view(), name='projects_task_monitoring'),
    path('task/technical', task.TechnicalIndexView.as_view(), name='projects_task_technical'),
    path('task/technical/server-lookup', task.TechnicalServerLookupView.as_view(), name='projects_task_technical_server_lookup'),
    path('task/technical/server-save', task.TechnicalServerSaveView.as_view(), name='projects_task_technical_server_save'),
    path('task/technical/partner-domains', task.TechnicalPartnerDomainsView.as_view(), name='projects_task_technical_partner_domains'),
    path('task/technical/link-domain', task.TechnicalLinkDomainView.as_view(), name='projects_task_technical_link_domain'),
    path('task/technical/subrow-lookup', task.TechnicalSubrowLookupView.as_view(), name='projects_task_technical_subrow_lookup'),
    path('task/technical/subrow-update', task.TechnicalSubrowUpdateView.as_view(), name='projects_task_technical_subrow_update'),
    path('task/technical/subrow-delete', task.TechnicalSubrowDeleteView.as_view(), name='projects_task_technical_subrow_delete'),
    path('task/technical/send', task.TechnicalSendView.as_view(), name='projects_task_technical_send'),
    path('task/publisher', task.PublisherIndexView.as_view(), name='projects_task_publisher'),
    path('task/publisher/keywords-save', task.PublisherKeywordsSaveView.as_view(), name='projects_task_publisher_keywords_save'),
    path('task/publisher/keywords-load', task.PublisherKeywordsLoadView.as_view(), name='projects_task_publisher_keywords_load'),
    path('task/publisher/keywords-delete', task.PublisherKeywordsDeleteView.as_view(), name='projects_task_publisher_keywords_delete'),
    path('task/publisher/keywords-by-niece', task.PublisherKeywordsByNieceView.as_view(), name='projects_task_publisher_keywords_by_niece'),
    # path('task/publisher/keywords-check', task.PublisherKeywordsCheckView.as_view(), name='projects_task_publisher_keywords_check'),
    path('task/publisher/send', task.PublisherSendView.as_view(), name='projects_task_publisher_send'),
    path('task/tracker', task.TrackerIndexView.as_view(), name='projects_task_tracker'),
    path('task/tracker/update', task.TrackerUpdateView.as_view(), name='projects_task_tracker_update'),
    path('task/tracker/send', task.TrackerSendView.as_view(), name='projects_task_tracker_send'),
    path('task/selesai', task.SelesaiIndexView.as_view(), name='projects_task_selesai'),
    path('task/plugin', task.PluginIndexView.as_view(), name='projects_task_plugin'),
    path('task/plugin/update', task.PluginUpdateView.as_view(), name='projects_task_plugin_update'),
    path('task/plugin/send', task.PluginSendView.as_view(), name='projects_task_plugin_send'),
    path('task/ads', task.AdsIndexView.as_view(), name='projects_task_ads'),
    path('task/ads/pages', task.AdsPagesView.as_view(), name='projects_task_ads_pages'),
    path('task/ads/update', task.AdsUpdateView.as_view(), name='projects_task_ads_update'),
    path('task/ads/send', task.AdsSendView.as_view(), name='projects_task_ads_send'),

    # MASTER
    
    # DOMAIN
    path('master/domain', master.DomainIndexView.as_view(), name='projects_master_domain'),
    path('master/domain/create', master.DomainCreateView.as_view(), name='projects_master_domain_create'),
    path('master/domain/<int:domain_id>/edit', master.DomainEditView.as_view(), name='projects_master_domain_edit'),
    path('master/domain/<int:domain_id>/delete', master.DomainDeleteView.as_view(), name='projects_master_domain_delete'),

    # SERVER
    path('master/server', master.ServerIndexView.as_view(), name='projects_master_server'),
    path('master/server/create', master.ServerCreateView.as_view(), name='projects_master_server_create'),
    path('master/server/<int:server_id>/edit', master.ServerEditView.as_view(), name='projects_master_server_edit'),
    path('master/server/<int:server_id>/delete', master.ServerDeleteView.as_view(), name='projects_master_server_delete'),

    path('master/website', master.WebsiteIndexView.as_view(), name='projects_master_website'),
    path('master/website/<int:website_id>/update', master.WebsiteUpdateView.as_view(), name='projects_master_website_update'),
    path('master/website/keywords', master.WebsiteKeywordsView.as_view(), name='projects_master_website_keywords'),

    # NIECE
    path('master/niece', master.NieceIndexView.as_view(), name='projects_master_niece'),
    path('master/niece/create', master.NieceCreateView.as_view(), name='projects_master_niece_create'),
    path('master/niece/<int:niece_id>/edit', master.NieceEditView.as_view(), name='projects_master_niece_edit'),
    path('master/niece/<int:niece_id>/update', master.NieceUpdateView.as_view(), name='projects_master_niece_update'),
    path('master/niece/<int:niece_id>/delete', master.NieceDeleteView.as_view(), name='projects_master_niece_delete'),
    path('master/niece/keyword', master.KeywordIndexView.as_view(), name='projects_master_keyword'),
    path('master/niece/keyword/create', master.KeywordCreateView.as_view(), name='projects_master_keyword_create'),
    path('master/niece/keyword/<int:keyword_id>/update', master.KeywordUpdateView.as_view(), name='projects_master_keyword_update'),
    path('master/niece/keyword/<int:keyword_id>/delete', master.KeywordDeleteView.as_view(), name='projects_master_keyword_delete'),

    # COUNTRY
    path('master/country', master.CountryIndexView.as_view(), name='projects_master_country'),
    path('master/country/create', master.CountryCreateView.as_view(), name='projects_master_country_create'),
    path('master/country/<str:negara_kd>/edit', master.CountryEditView.as_view(), name='projects_master_country_edit'),
    path('master/country/<str:negara_kd>/update', master.CountryUpdateView.as_view(), name='projects_master_country_update'),
    path('master/country/<str:negara_kd>/delete', master.CountryDeleteView.as_view(), name='projects_master_country_delete'),
    path('master/country/get/<str:negara_kd>', master.CountryGetView.as_view(), name='projects_master_country_get'),
]

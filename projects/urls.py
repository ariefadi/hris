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
]

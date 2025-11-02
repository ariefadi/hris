from django.urls import path
from . import views

app_name = 'settings'

urlpatterns = [
    path('overview', views.Overview.as_view(), name='overview'),
    # Sistem / Portal CRUD
    path('sistem/portal', views.PortalIndexView.as_view(), name='sistem_portal_index'),
    path('sistem/portal/create', views.PortalCreateView.as_view(), name='sistem_portal_create'),
    path('sistem/portal/<str:portal_id>/edit', views.PortalEditView.as_view(), name='sistem_portal_edit'),
    path('sistem/portal/<str:portal_id>/delete', views.PortalDeleteView.as_view(), name='sistem_portal_delete'),
    # Sistem / Groups CRUD
    path('sistem/groups', views.GroupsIndexView.as_view(), name='sistem_groups_index'),
    path('sistem/groups/create', views.GroupsCreateView.as_view(), name='sistem_groups_create'),
    path('sistem/groups/<str:group_id>/edit', views.GroupsEditView.as_view(), name='sistem_groups_edit'),
    path('sistem/groups/<str:group_id>/delete', views.GroupsDeleteView.as_view(), name='sistem_groups_delete'),
    # Sistem / Roles CRUD
    path('sistem/roles', views.RolesIndexView.as_view(), name='sistem_roles_index'),
    path('sistem/roles/create', views.RolesCreateView.as_view(), name='sistem_roles_create'),
    path('sistem/roles/<str:role_id>/edit', views.RolesEditView.as_view(), name='sistem_roles_edit'),
    path('sistem/roles/<str:role_id>/delete', views.RolesDeleteView.as_view(), name='sistem_roles_delete'),
    # Sistem / Permissions Index (CRUD to follow)
    path('sistem/permissions', views.PermissionsIndexView.as_view(), name='sistem_permissions_index'),
    path('sistem/permissions/access_update/<str:role_id>', views.PermissionsAccessUpdateView.as_view(), name='sistem_permissions_access_update'),
    path('sistem/permissions/filter_portal_process', views.PermissionsFilterPortalProcessView.as_view(), name='sistem_permissions_filter'),
    path('sistem/permissions/process', views.PermissionsProcessView.as_view(), name='sistem_permissions_process'),
    # Sistem / Menu (Index only for now)
    path('sistem/menu', views.MenuIndexView.as_view(), name='sistem_menu_index'),
    path('sistem/menu/<str:portal_id>/edit', views.MenuEditView.as_view(), name='sistem_menu_edit'),
    # Sistem / Menu Items (Add/Edit)
    path('sistem/menu/<str:portal_id>/add', views.MenuItemCreateView.as_view(), name='sistem_menu_add'),
    path('sistem/menu/<str:portal_id>/nav/<str:nav_id>/edit', views.MenuItemEditView.as_view(), name='sistem_menu_item_edit'),
]
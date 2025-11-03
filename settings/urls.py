from django.urls import path
from . import sistem
from . import users

urlpatterns = [
    path('overview', sistem.Overview.as_view(), name='overview'),
    # Sistem / Portal CRUD
    path('sistem/portal', sistem.PortalIndexView.as_view(), name='portal'),
    path('sistem/portal/create', sistem.PortalCreateView.as_view(), name='portal_create'),
    path('sistem/portal/<str:portal_id>/edit', sistem.PortalEditView.as_view(), name='portal_edit'),
    path('sistem/portal/<str:portal_id>/delete', sistem.PortalDeleteView.as_view(), name='portal_delete'),
    # Sistem / Groups CRUD
    path('sistem/groups', sistem.GroupsIndexView.as_view(), name='groups'),
    path('sistem/groups/create', sistem.GroupsCreateView.as_view(), name='groups_create'),
    path('sistem/groups/<str:group_id>/edit', sistem.GroupsEditView.as_view(), name='groups_edit'),
    path('sistem/groups/<str:group_id>/delete', sistem.GroupsDeleteView.as_view(), name='groups_delete'),
    # Sistem / Roles CRUD
    path('sistem/roles', sistem.RolesIndexView.as_view(), name='roles'),
    path('sistem/roles/create', sistem.RolesCreateView.as_view(), name='roles_create'),
    path('sistem/roles/<str:role_id>/edit', sistem.RolesEditView.as_view(), name='roles_edit'),
    path('sistem/roles/<str:role_id>/delete', sistem.RolesDeleteView.as_view(), name='roles_delete'),
    # Sistem / Permissions Index (CRUD to follow)
    path('sistem/permissions', sistem.PermissionsIndexView.as_view(), name='permissions'),
    path('sistem/permissions/access_update/<str:role_id>', sistem.PermissionsAccessUpdateView.as_view(), name='permissions_access_update'),
    path('sistem/permissions/filter_portal_process', sistem.PermissionsFilterPortalProcessView.as_view(), name='permissions_filter'),
    path('sistem/permissions/process', sistem.PermissionsProcessView.as_view(), name='permissions_process'),
    # Sistem / Menu (Index only for now)
    path('sistem/menu', sistem.MenuIndexView.as_view(), name='menu'),
    path('sistem/menu/<str:portal_id>/edit', sistem.MenuEditView.as_view(), name='menu_edit'),
    # Sistem / Menu Items (Add/Edit)
    path('sistem/menu/<str:portal_id>/add', sistem.MenuItemCreateView.as_view(), name='menu_add'),
    path('sistem/menu/<str:portal_id>/nav/<str:nav_id>/edit', sistem.MenuItemEditView.as_view(), name='menu_item_edit'),
    path('sistem/menu/<str:portal_id>/nav/<str:nav_id>/delete', sistem.MenuItemDeleteView.as_view(), name='menu_item_delete'),
    # Sistem / Mail Utility
    path('sistem/mail', sistem.MailIndexView.as_view(), name='sistem_mail'),
    # Users / Data
    path('users/data', users.UsersDataView.as_view(), name='users_data'),
    path('users/data/page', users.UsersDataListView.as_view(), name='users_data_page'),
    path('users/data/get/<str:user_id>', users.UsersDataGetByIdView.as_view(), name='users_data_get_by_id'),
    path('users/data/create', users.UsersDataCreateView.as_view(), name='users_data_create'),
    path('users/data/update', users.UsersDataUpdateView.as_view(), name='users_data_update'),
    path('users/data/delete', users.UsersDataDeleteView.as_view(), name='users_data_delete'),
    path('users/data/add', users.UsersDataAddPageView.as_view(), name='users_data_add'),
    path('users/data/edit/<str:user_id>', users.UsersDataEditPageView.as_view(), name='users_data_edit'),
    path('users/data/roles/update', users.UsersRolesUpdateView.as_view(), name='users_roles_update'),
    # Users / Login Activity
    path('users/login_activity', users.DataLoginUser.as_view(), name='users_login_activity'),
    path('users/page_login_user', users.page_login_user.as_view()),
    # Menu Master Plan
    path('users/master_plan', users.MasterPlan.as_view(), name='master_plan'),
    path('users/page_master_plan', users.page_master_plan.as_view()),
    path('users/page_detail_master_plan/<str:master_plan_id>', users.page_detail_master_plan.as_view(), name='page_detail_master_plan'),
    path('users/add_master_plan', users.add_master_plan.as_view(), name='add_master_plan'),
    path('users/post_tambah_master_plan', users.post_tambah_master_plan.as_view(), name='post_tambah_master_plan'),
    # path('users/post_tambah_master_plan', users.post_tambah_master_plan.as_view()),
]
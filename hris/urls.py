from django.contrib import admin
from django.urls import path, include, re_path
from django.shortcuts import redirect
from django.conf import settings
from django.conf.urls.static import static
from management import views
from hris import views as project_views

# Mendaftarkan handler404 untuk halaman error kustom (project-level)
handler404 = 'hris.views.custom_404'

def root_redirect(request):
    # Redirect root ke halaman login untuk menghindari loop
    return redirect('/management/admin/login')

urlpatterns = [
    path('admin/', admin.site.urls),
    path('', root_redirect),
    path('management/', include('management.urls')),
    path('settings/', include('settings.urls')),
    path('accounts/', include('social_django.urls', namespace='social')),
    # Direct access to AdX Traffic Account
    path('adx-traffic-account/', views.AdxTrafficPerAccountView.as_view(), name='adx_traffic_account_direct'),
]

# Serve static files during development
if settings.DEBUG:
    doc_root = settings.STATICFILES_DIRS[0] if getattr(settings, 'STATICFILES_DIRS', None) else None
    if doc_root:
        urlpatterns += static(settings.STATIC_URL, document_root=doc_root)
    # Catch-all route to render custom 404 page during development (DEBUG=True)
    urlpatterns += [
        re_path(r'^.*$', project_views.dev_404, name='dev_404'),
    ]

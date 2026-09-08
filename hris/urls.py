from django.contrib import admin
from django.urls import path, include, re_path
from django.shortcuts import redirect
from django.conf import settings
from django.conf.urls.static import static
from django.contrib.staticfiles.urls import staticfiles_urlpatterns
from management import views
from hris import views as project_views
from hris import pwa as pwa_views

# Mendaftarkan handler404 untuk halaman error kustom (project-level)
handler404 = 'hris.views.custom_404'

def root_redirect(request):
    # Redirect root ke halaman login untuk menghindari loop
    return redirect('/management/admin/login')

urlpatterns = [
    path('admin/', admin.site.urls),
    path('manifest.webmanifest', pwa_views.web_manifest, name='pwa_manifest'),
    path('sw.js', pwa_views.service_worker, name='pwa_service_worker'),
    path('favicon.ico', pwa_views.favicon, name='favicon'),
    path('', root_redirect),
    path('management/', include('management.urls')),
    path('settings/', include('settings.urls')),
    path('projects/', include('projects.urls')),
    path('accounts/', include('social_django.urls', namespace='social')),
    # Direct access to AdX Traffic Account
    path('adx-traffic-account/', views.AdxTrafficPerAccountView.as_view(), name='adx_traffic_account_direct'),
]

# Serve static files during development
if settings.DEBUG:
    urlpatterns += static(settings.STATIC_URL, document_root=settings.STATICFILES_DIRS[0])
    urlpatterns += static(settings.MEDIA_URL, document_root=settings.MEDIA_ROOT)
    # Catch-all route to render custom 404 page during development (DEBUG=True)
    # This should be LAST to avoid catching static files
    urlpatterns += [
        re_path(r'^.*$', project_views.dev_404, name='dev_404'),
    ]

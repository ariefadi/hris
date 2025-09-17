from django.contrib import admin
from django.urls import path, include
from django.shortcuts import redirect
from management import views

def root_redirect(request):
    # Redirect root ke halaman login untuk menghindari loop
    return redirect('/management/admin/login')

urlpatterns = [
    path('admin/', admin.site.urls),
    path('', root_redirect),
    path('management/', include('management.urls')),
    path('accounts/', include('social_django.urls', namespace='social')),
    # Direct access to AdX Traffic Account
    path('adx-traffic-account/', views.AdxTrafficPerAccountView.as_view(), name='adx_traffic_account_direct'),
]

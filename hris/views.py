from django.shortcuts import render


def custom_404(request, exception):
    """Project-level 404 handler rendering the global template.
    Registered via handler404 in hris/urls.py.
    """
    return render(request, '404.html', status=404)


def dev_404(request):
    """Catch-all for DEBUG=True to preview the 404 template consistently."""
    return render(request, '404.html', status=404)
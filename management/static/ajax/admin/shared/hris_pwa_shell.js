/**
 * PWA standalone shell: dashboard-only chrome, mobile-responsive layout (no page zoom).
 */
(function (window, document) {
    'use strict';

    function isStandalone() {
        try {
            return window.matchMedia('(display-mode: standalone)').matches
                || window.matchMedia('(display-mode: fullscreen)').matches
                || window.navigator.standalone === true
                || /[?&]pwa=1(?:&|$)/.test(window.location.search || '');
        } catch (err) {
            return false;
        }
    }

    function setPwaCookie() {
        var secure = window.location.protocol === 'https:' ? '; Secure' : '';
        document.cookie = 'hris_pwa_app=1; Path=/; Max-Age=31536000; SameSite=Lax' + secure;
    }

    function clearPwaCookie() {
        var secure = window.location.protocol === 'https:' ? '; Secure' : '';
        document.cookie = 'hris_pwa_app=; Path=/; Max-Age=0; SameSite=Lax' + secure;
    }

    function clearZoom() {
        var root = document.documentElement;
        var body = document.body;
        root.classList.remove('hris-pwa-zoomed', 'hris-pwa-no-zoom');
        root.style.zoom = '';
        root.style.removeProperty('--pwa-zoom');
        if (body) {
            body.style.width = '';
            body.style.minWidth = '';
            body.style.maxWidth = '';
            body.style.transform = '';
        }
    }

    function markShell() {
        document.documentElement.classList.add('hris-pwa-app');
        if (document.body) {
            document.body.classList.add('hris-pwa-app', 'sidebar-collapse');
            document.body.classList.remove('sidebar-open');
        }
        setPwaCookie();
        clearZoom();
    }

    function allowedPath(path) {
        var clean = String(path || '').replace(/\/+$/, '') || '/';
        var allow = {
            '/management/admin/login': true,
            '/management/admin/logout': true,
            '/management/admin/dashboard': true,
            '/management/admin/forgot_password': true,
            '/management/admin/register_account': true,
            '/management/admin/oauth_redirect': true
        };
        if (allow[clean]) return true;
        if (clean.indexOf('/management/admin/login') === 0) return true;
        return false;
    }

    function enforceDashboard() {
        if (allowedPath(window.location.pathname)) return;
        window.location.replace('/management/admin/dashboard');
    }

    clearZoom();
    if (isStandalone()) {
        markShell();
        if (document.readyState === 'loading') {
            document.addEventListener('DOMContentLoaded', markShell);
        }
        enforceDashboard();
    } else {
        clearPwaCookie();
        if (document.readyState === 'loading') {
            document.addEventListener('DOMContentLoaded', clearZoom);
        }
    }
})(window, document);

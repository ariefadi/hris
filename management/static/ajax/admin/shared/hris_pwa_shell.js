/**
 * PWA standalone shell + compact phone layout.
 * Page zoom stays off; users can zoom dashboard/modal content via the floating control.
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

    function isCompact() {
        try {
            if (isStandalone()) return true;
            return window.matchMedia('(max-width: 991.98px)').matches;
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
        root.style.zoom = '1';
        root.style.removeProperty('--pwa-zoom');
        if (body) {
            body.style.zoom = '';
            body.style.transform = '';
            body.style.width = '';
        }
        document.querySelectorAll('.ds-dt-scroll, .detail-scoring-table-wrap, .table-responsive').forEach(function (el) {
            el.style.zoom = '';
            el.style.transform = '';
            el.style.width = '';
        });
        document.querySelectorAll('.hris-m-cards, .pwa-table-hint').forEach(function (el) { el.remove(); });
    }

    function markCompact() {
        document.documentElement.classList.toggle('hris-compact-ui', isCompact());
        if (document.body && isCompact()) {
            document.body.classList.add('sidebar-collapse');
            document.body.classList.remove('sidebar-open');
        }
        clearZoom();
        syncZoomControl();
    }

    function markShell() {
        document.documentElement.classList.add('hris-pwa-app');
        if (document.body) {
            document.body.classList.add('hris-pwa-app', 'sidebar-collapse');
            document.body.classList.remove('sidebar-open');
        }
        setPwaCookie();
        markCompact();
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

    var ZOOM_KEY = 'hris_ui_zoom';
    var ZOOM_MIN = 0.5;
    var ZOOM_MAX = 1.5;
    var ZOOM_STEP = 0.1;

    function isDashboardPath() {
        return String(window.location.pathname || '').indexOf('/management/admin/dashboard') === 0;
    }

    function useTransformZoom() {
        try {
            return /iPhone|iPad|iPod/i.test(window.navigator.userAgent || '')
                || (window.navigator.platform === 'MacIntel' && (window.navigator.maxTouchPoints || 0) > 1);
        } catch (err) {
            return false;
        }
    }

    function clampZoom(value) {
        var n = Number(value);
        if (!Number.isFinite(n)) n = 1;
        n = Math.round(n * 10) / 10;
        if (n < ZOOM_MIN) n = ZOOM_MIN;
        if (n > ZOOM_MAX) n = ZOOM_MAX;
        return n;
    }

    function readZoom() {
        try {
            return clampZoom(window.localStorage.getItem(ZOOM_KEY) || '1');
        } catch (err) {
            return 1;
        }
    }

    function writeZoom(value) {
        try {
            window.localStorage.setItem(ZOOM_KEY, String(value));
        } catch (err) {}
    }

    function applyZoom(value) {
        var zoom = clampZoom(value);
        var root = document.documentElement;
        root.style.setProperty('--hris-user-zoom', String(zoom));
        root.classList.toggle('is-user-zoomed-in', zoom > 1);
        root.classList.toggle('hris-zoom-transform', useTransformZoom());
        writeZoom(zoom);
        var label = document.getElementById('hrisZoomValue');
        if (label) label.textContent = String(Math.round(zoom * 100)) + '%';
        var outBtn = document.getElementById('hrisZoomOut');
        var inBtn = document.getElementById('hrisZoomIn');
        if (outBtn) outBtn.disabled = zoom <= ZOOM_MIN;
        if (inBtn) inBtn.disabled = zoom >= ZOOM_MAX;
        return zoom;
    }

    function syncZoomControl() {
        var el = document.getElementById('hrisZoomControl');
        if (!el) return;
        el.classList.toggle('is-on', isCompact() && isDashboardPath());
    }

    function mountZoomControl() {
        if (!isDashboardPath() || document.getElementById('hrisZoomControl')) return;
        var wrap = document.createElement('div');
        wrap.id = 'hrisZoomControl';
        wrap.className = 'hris-zoom-control';
        wrap.setAttribute('role', 'group');
        wrap.setAttribute('aria-label', 'Zoom tampilan');
        wrap.innerHTML = ''
            + '<button type="button" id="hrisZoomOut" data-zoom-out aria-label="Perkecil tampilan">−</button>'
            + '<button type="button" id="hrisZoomValue" data-zoom-reset title="Kembalikan 100%">100%</button>'
            + '<button type="button" id="hrisZoomIn" data-zoom-in aria-label="Perbesar tampilan">+</button>';
        wrap.addEventListener('click', function (e) {
            var btn = e.target && e.target.closest ? e.target.closest('button') : null;
            if (!btn) return;
            var current = readZoom();
            if (btn.hasAttribute('data-zoom-out')) applyZoom(current - ZOOM_STEP);
            else if (btn.hasAttribute('data-zoom-in')) applyZoom(current + ZOOM_STEP);
            else if (btn.hasAttribute('data-zoom-reset')) applyZoom(1);
        });
        document.body.appendChild(wrap);
        applyZoom(readZoom());
        syncZoomControl();
    }

    function initUserZoom() {
        if (!isDashboardPath()) return;
        applyZoom(readZoom());
        if (document.body) mountZoomControl();
        else document.addEventListener('DOMContentLoaded', mountZoomControl);
    }

    markCompact();
    try {
        var mq = window.matchMedia('(max-width: 991.98px)');
        if (mq.addEventListener) mq.addEventListener('change', function () {
            markCompact();
            syncZoomControl();
        });
        else if (mq.addListener) mq.addListener(function () {
            markCompact();
            syncZoomControl();
        });
    } catch (err) {}
    if (isStandalone()) {
        markShell();
        if (document.readyState === 'loading') {
            document.addEventListener('DOMContentLoaded', markShell);
        }
        if (!allowedPath(window.location.pathname)) {
            window.location.replace('/management/admin/dashboard');
        }
    } else {
        clearPwaCookie();
        if (document.readyState === 'loading') {
            document.addEventListener('DOMContentLoaded', markCompact);
        }
    }
    initUserZoom();
})(window, document);

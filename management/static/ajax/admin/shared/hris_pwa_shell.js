/**
 * PWA standalone shell + compact phone layout.
 * Page zoom stays off; users can zoom dashboard/modal content via the floating control.
 */
(function (window, document) {
    'use strict';

    function isNarrow() {
        try {
            return window.matchMedia('(max-width: 991.98px)').matches;
        } catch (err) {
            return false;
        }
    }

    function isMobileUa() {
        try {
            return /Android|webOS|iPhone|iPad|iPod|BlackBerry|IEMobile|Opera Mini|Mobile/i.test(window.navigator.userAgent || '');
        } catch (err) {
            return false;
        }
    }

    function isStandalone() {
        try {
            if (window.matchMedia('(display-mode: standalone)').matches) return true;
            if (window.navigator.standalone === true) return true;
            // ?pwa=1 is a mobile preview flag, never lock a wide desktop browser.
            if (/[?&]pwa=1(?:&|$)/.test(window.location.search || '') && (isMobileUa() || isNarrow())) {
                return true;
            }
            return false;
        } catch (err) {
            return false;
        }
    }

    function isCompact() {
        return isStandalone() || isNarrow();
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
        applyZoom(readZoom());
    }

    function unmarkShell() {
        document.documentElement.classList.remove('hris-pwa-app', 'hris-pwa-zoomed', 'hris-pwa-no-zoom');
        if (document.body) document.body.classList.remove('hris-pwa-app');
        clearPwaCookie();
        markCompact();
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

    function syncShell() {
        if (isStandalone()) markShell();
        else unmarkShell();
    }

    function allowedPath(path) {
        var clean = String(path || '').replace(/\/+$/, '') || '/';
        if (clean.indexOf('/management/') === 0) return true;
        if (clean.indexOf('/settings/') === 0) return true;
        return false;
    }

    var ZOOM_KEY = 'hris_ui_zoom';
    var ZOOM_MIN = 0.5;
    var ZOOM_MAX = 1.5;
    var ZOOM_STEP = 0.1;

    function shouldShowZoom() {
        return isCompact();
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

    function zoomTargets() {
        return document.querySelectorAll('.content, .login-shell, .score-detail-modal__body');
    }

    function chatZoomTargets() {
        return document.querySelectorAll('.hris-live-chat-panel');
    }

    function clearZoomStyles(el) {
        el.style.removeProperty('zoom');
        el.style.removeProperty('transform');
        el.style.removeProperty('transform-origin');
        el.style.removeProperty('width');
        el.style.removeProperty('max-height');
        el.style.removeProperty('left');
        el.style.removeProperty('right');
    }

    function applyZoomToTargets(zoom) {
        var useTf = useTransformZoom();
        zoomTargets().forEach(function (el) {
            if (Math.abs(zoom - 1) < 0.001) {
                el.style.removeProperty('zoom');
                el.style.removeProperty('transform');
                el.style.removeProperty('transform-origin');
                if (el.classList.contains('login-shell') || el.classList.contains('content') || el.classList.contains('score-detail-modal__body')) {
                    el.style.removeProperty('width');
                }
                return;
            }
            if (useTf) {
                el.style.setProperty('zoom', '1', 'important');
                el.style.setProperty('transform', 'scale(' + zoom + ')', 'important');
                el.style.setProperty('transform-origin', 'top left', 'important');
                el.style.setProperty('width', (100 / zoom) + '%', 'important');
                return;
            }
            el.style.setProperty('transform', 'none', 'important');
            el.style.setProperty('zoom', String(zoom), 'important');
        });
        applyZoomToChat(zoom);
    }

    function resetChatFabZoom() {
        var fab = document.getElementById('hrisLiveChatFab');
        if (!fab) return;
        fab.style.setProperty('zoom', '1', 'important');
        fab.style.setProperty('transform', 'none', 'important');
        fab.style.removeProperty('transform-origin');
        fab.style.removeProperty('width');
        fab.style.removeProperty('max-height');
        fab.style.removeProperty('left');
        fab.style.removeProperty('right');
    }

    function applyZoomToChat(zoom) {
        resetChatFabZoom();
        var useTf = useTransformZoom();
        chatZoomTargets().forEach(function (el) {
            if (Math.abs(zoom - 1) < 0.001) {
                clearZoomStyles(el);
                return;
            }
            if (useTf) {
                el.style.setProperty('zoom', '1', 'important');
                el.style.setProperty('transform', 'scale(' + zoom + ')', 'important');
                el.style.setProperty('transform-origin', 'bottom right', 'important');
            } else {
                el.style.setProperty('transform', 'none', 'important');
                el.style.setProperty('zoom', String(zoom), 'important');
            }
            el.style.setProperty('left', 'calc(8px / ' + zoom + ')', 'important');
            el.style.setProperty('right', 'auto', 'important');
            el.style.setProperty('width', 'calc((100vw - 16px) / ' + zoom + ')', 'important');
            el.style.setProperty('max-height', 'calc((100dvh - 140px) / ' + zoom + ')', 'important');
        });
    }

    function applyZoom(value) {
        var zoom = shouldShowZoom() ? clampZoom(value) : 1;
        var root = document.documentElement;
        root.style.setProperty('--hris-user-zoom', String(zoom));
        root.classList.toggle('is-user-zoomed-in', zoom > 1);
        root.classList.toggle('hris-zoom-transform', shouldShowZoom() && useTransformZoom());
        if (shouldShowZoom()) writeZoom(zoom);
        applyZoomToTargets(zoom);
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
        el.classList.toggle('is-on', shouldShowZoom());
    }

    function mountZoomControl() {
        if (document.getElementById('hrisZoomControl')) return;
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
        applyZoom(readZoom());
        if (document.body) mountZoomControl();
        else document.addEventListener('DOMContentLoaded', mountZoomControl);
        if (window.MutationObserver && document.documentElement) {
            var zoomObsTimer = null;
            var obs = new MutationObserver(function () {
                if (zoomObsTimer) return;
                zoomObsTimer = setTimeout(function () {
                    zoomObsTimer = null;
                    applyZoom(readZoom());
                }, 80);
            });
            obs.observe(document.body || document.documentElement, { childList: true, subtree: true });
        }
    }

    syncShell();
    try {
        var mq = window.matchMedia('(max-width: 991.98px)');
        var onViewport = function () { syncShell(); };
        if (mq.addEventListener) mq.addEventListener('change', onViewport);
        else if (mq.addListener) mq.addListener(onViewport);
    } catch (err) {}
    try {
        var dm = window.matchMedia('(display-mode: standalone)');
        var onDisplay = function () { syncShell(); };
        if (dm.addEventListener) dm.addEventListener('change', onDisplay);
        else if (dm.addListener) dm.addListener(onDisplay);
    } catch (err) {}
    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', syncShell);
    }
    if (isStandalone() && !allowedPath(window.location.pathname)) {
        window.location.replace('/management/admin/dashboard');
    }
    initUserZoom();
})(window, document);

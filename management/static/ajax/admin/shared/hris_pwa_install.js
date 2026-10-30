(function (window, document) {
    'use strict';

    var deferredPrompt = null;
    var cfg = window.HRIS_PWA || {};
    var swUrl = cfg.swUrl || '/sw.js';

    function isStandalone() {
        return window.matchMedia('(display-mode: standalone)').matches
            || window.navigator.standalone === true;
    }

    function isIos() {
        var ua = window.navigator.userAgent || '';
        var iOS = /iphone|ipad|ipod/i.test(ua);
        var iPadOs = window.navigator.platform === 'MacIntel' && window.navigator.maxTouchPoints > 1;
        return iOS || iPadOs;
    }

    function qs(id) {
        return document.getElementById(id);
    }

    function setHidden(el, hidden) {
        if (!el) return;
        el.style.display = hidden ? 'none' : '';
    }

    function registerWorker() {
        if (!('serviceWorker' in navigator)) return;
        navigator.serviceWorker.register(swUrl, { scope: '/' }).catch(function () {});
    }

    function showIosHelp() {
        var html = '<div style="text-align:left;line-height:1.55">'
            + '<p class="mb-2">Di iPhone / iPad, pasang lewat Safari:</p>'
            + '<ol class="mb-0 pl-3">'
            + '<li>Ketuk tombol <strong>Bagikan</strong> (kotak dengan panah ke atas) di bawah atau atas Safari.</li>'
            + '<li>Pilih <strong>Add to Home Screen</strong> / <strong>Tambah ke Layar Utama</strong>.</li>'
            + '<li>Ketuk <strong>Add</strong> untuk memasang Kiwipixel Analytics.</li>'
            + '</ol></div>';
        if (window.hrisSwalFire) {
            window.hrisSwalFire({
                icon: 'info',
                title: 'Install di iPhone',
                html: html,
                confirmButtonText: 'Mengerti'
            });
            return;
        }
        if (window.Swal && typeof window.Swal.fire === 'function') {
            window.Swal.fire({
                icon: 'info',
                title: 'Install di iPhone',
                html: html,
                confirmButtonText: 'Mengerti'
            });
            return;
        }
        window.alert('Di iPhone: ketuk Bagikan, lalu pilih Add to Home Screen.');
    }

    function showGenericHelp() {
        var html = '<div style="text-align:left;line-height:1.55">'
            + '<p class="mb-2">Pasang aplikasi ke layar HP:</p>'
            + '<ul class="mb-0 pl-3">'
            + '<li><strong>Android Chrome:</strong> menu ⋮ lalu <em>Install app</em> / <em>Add to Home screen</em>.</li>'
            + '<li><strong>iPhone Safari:</strong> tombol Bagikan, lalu <em>Add to Home Screen</em>.</li>'
            + '</ul></div>';
        if (window.hrisSwalFire) {
            window.hrisSwalFire({
                icon: 'info',
                title: 'Install Aplikasi',
                html: html,
                confirmButtonText: 'Mengerti'
            });
            return;
        }
        if (window.Swal && typeof window.Swal.fire === 'function') {
            window.Swal.fire({
                icon: 'info',
                title: 'Install Aplikasi',
                html: html,
                confirmButtonText: 'Mengerti'
            });
            return;
        }
        window.alert('Gunakan menu browser untuk Add to Home Screen / Install app.');
    }

    function promptInstall() {
        if (deferredPrompt) {
            var promptEvent = deferredPrompt;
            deferredPrompt = null;
            updateUI();
            promptEvent.prompt();
            promptEvent.userChoice.finally(function () {
                updateUI();
            });
            return;
        }
        if (isIos()) {
            showIosHelp();
            return;
        }
        showGenericHelp();
    }

    function updateUI() {
        var navItem = qs('hrisPwaInstall');
        var actionBtn = qs('hrisPwaInstallAction');
        var statusEl = qs('hrisPwaInstallStatus');
        var hintEl = qs('hrisPwaInstallHint');
        var loginBtn = qs('hrisPwaLoginInstall');

        if (isStandalone()) {
            setHidden(navItem, true);
            setHidden(loginBtn, true);
            return;
        }
        setHidden(navItem, false);
        setHidden(loginBtn, false);

        var canPrompt = !!deferredPrompt;
        if (actionBtn) {
            actionBtn.textContent = canPrompt ? 'Install Sekarang' : (isIos() ? 'Cara Install di iPhone' : 'Cara Install');
        }
        if (statusEl) {
            statusEl.textContent = canPrompt
                ? 'Siap dipasang di perangkat ini'
                : (isIos() ? 'iPhone memakai Safari + Add to Home Screen' : 'Pasang lewat menu browser');
        }
        if (hintEl) {
            hintEl.textContent = canPrompt
                ? 'Ketuk tombol di bawah untuk menambahkan aplikasi ke layar HP.'
                : (isIos()
                    ? 'Safari tidak menampilkan popup install otomatis. Ikuti langkah Add to Home Screen.'
                    : 'Jika tombol install browser tidak muncul, buka Chrome lalu pilih Install app.');
        }
    }

    window.addEventListener('beforeinstallprompt', function (event) {
        event.preventDefault();
        deferredPrompt = event;
        updateUI();
    });

    window.addEventListener('appinstalled', function () {
        deferredPrompt = null;
        updateUI();
    });

    document.addEventListener('click', function (event) {
        var node = event.target;
        if (node && node.nodeType === 3) node = node.parentElement;
        var trigger = node && node.closest ? node.closest('[data-hris-pwa-install]') : null;
        if (!trigger) return;
        event.preventDefault();
        event.stopPropagation();
        promptInstall();
    }, true);

    document.addEventListener('DOMContentLoaded', function () {
        registerWorker();
        updateUI();
    });
})(window, document);

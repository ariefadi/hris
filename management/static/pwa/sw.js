/* Kiwipixel Analytics PWA service worker */
const CACHE_NAME = 'kiwipixel-pwa-v3';
const PRECACHE = [
    '/static/pwa/icon-192.png',
    '/static/pwa/icon-512.png',
    '/static/pwa/apple-touch-icon.png',
];

self.addEventListener('install', function (event) {
    event.waitUntil(
        caches.open(CACHE_NAME).then(function (cache) {
            return cache.addAll(PRECACHE);
        }).then(function () {
            return self.skipWaiting();
        }).catch(function () {
            return self.skipWaiting();
        })
    );
});

self.addEventListener('activate', function (event) {
    event.waitUntil(
        caches.keys().then(function (keys) {
            return Promise.all(keys.filter(function (key) {
                return key !== CACHE_NAME;
            }).map(function (key) {
                return caches.delete(key);
            }));
        }).then(function () {
            return self.clients.claim();
        })
    );
});

self.addEventListener('fetch', function (event) {
    var request = event.request;
    if (request.method !== 'GET') return;
    var url;
    try {
        url = new URL(request.url);
    } catch (err) {
        return;
    }
    if (url.origin !== self.location.origin) return;
    if (url.pathname.indexOf('/static/pwa/') !== 0) return;
    event.respondWith(
        caches.match(request).then(function (cached) {
            return cached || fetch(request);
        })
    );
});

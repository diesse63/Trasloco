const CACHE_NAME = 'trasloco-smart-v1';
const APP_SHELL = [
    './',
    './index.html',
    './manifest.webmanifest',
    './styles/style.css',
    './scripts/app.js',
    './scripts/supabaseClient.js',
    './pannelli/inserimento.html',
    './pannelli/gestione.html',
    './pannelli/stanze.html',
    './pannelli/scatole.html',
    './assets/icons/icon-192.svg',
    './assets/icons/icon-512.svg',
];

self.addEventListener('install', (event) => {
    event.waitUntil(
        caches.open(CACHE_NAME).then((cache) => cache.addAll(APP_SHELL))
    );
    self.skipWaiting();
});

self.addEventListener('activate', (event) => {
    event.waitUntil(
        caches.keys().then((keys) => Promise.all(
            keys
                .filter((key) => key !== CACHE_NAME)
                .map((key) => caches.delete(key))
        ))
    );
    self.clients.claim();
});

self.addEventListener('fetch', (event) => {
    const request = event.request;
    if (request.method !== 'GET') return;

    const url = new URL(request.url);
    if (url.origin !== self.location.origin) return;

    // Navigazioni: network-first, fallback su index e cache.
    if (request.mode === 'navigate') {
        event.respondWith(
            fetch(request)
                .then((response) => {
                    const copy = response.clone();
                    caches.open(CACHE_NAME).then((cache) => cache.put('./index.html', copy));
                    return response;
                })
                .catch(async () => {
                    const cache = await caches.open(CACHE_NAME);
                    return cache.match('./index.html') || cache.match('./');
                })
        );
        return;
    }

    // Risorse statiche: cache-first con aggiornamento in background.
    event.respondWith(
        caches.match(request).then((cached) => {
            const networkFetch = fetch(request)
                .then((response) => {
                    const copy = response.clone();
                    caches.open(CACHE_NAME).then((cache) => cache.put(request, copy));
                    return response;
                })
                .catch(() => cached);

            return cached || networkFetch;
        })
    );
});

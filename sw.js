const CACHE_NAME = 'taxi-etoile-v3';
const ASSETS_TO_CACHE = [
  './',
  './index.html',
  './admin.html',
  './tarifs.html',
  './services.html',
  './a-propos.html',
  './contact.html',
  './faq.html',
  './galerie.html',
  './chauffeurs.html',
  './politique-confidentialite.html',
  './conditions.html',
  './404.html',
  './style.css',
  './main.js',
  './firebase-config.js',
  './manifest.json',
  './icons/icon-192.png',
  './icons/icon-512.png',
  './icons/apple-touch-icon.png',
  'https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.4.0/css/all.min.css'
];

self.addEventListener('install', event => {
  event.waitUntil(
    caches.open(CACHE_NAME).then(cache => cache.addAll(ASSETS_TO_CACHE))
  );
  self.skipWaiting();
});

self.addEventListener('activate', event => {
  event.waitUntil(
    caches.keys().then(keys => Promise.all(
      keys.map(key => key !== CACHE_NAME ? caches.delete(key) : Promise.resolve())
    ))
  );
  self.clients.claim();
});

self.addEventListener('fetch', event => {
  if (event.request.method !== 'GET') return;

  event.respondWith(
    caches.match(event.request).then(cacheRes => {
      if (cacheRes) return cacheRes;

      return fetch(event.request)
        .then(fetchRes => {
          const copy = fetchRes.clone();
          caches.open(CACHE_NAME).then(cache => {
            if (event.request.url.startsWith(self.location.origin) || event.request.url.includes('cdnjs.cloudflare.com')) {
              cache.put(event.request, copy);
            }
          });
          return fetchRes;
        })
        .catch(() => {
          if (event.request.mode === 'navigate') {
            return caches.match('./index.html');
          }
          return new Response('', { status: 504, statusText: 'Offline' });
        });
    })
  );
});

// KeyTrack Service Worker — PWA offline support + smart caching
const CACHE_NAME = 'keytrack-v1';
const STATIC_ASSETS = [
  '/home',
  '/listings',
  '/search-ai',
  '/static/manifest.json',
];

// Install — pre-cache key pages
self.addEventListener('install', e => {
  e.waitUntil(
    caches.open(CACHE_NAME).then(cache => {
      return Promise.allSettled(STATIC_ASSETS.map(url => cache.add(url)));
    }).then(() => self.skipWaiting())
  );
});

// Activate — clean old caches
self.addEventListener('activate', e => {
  e.waitUntil(
    caches.keys().then(keys =>
      Promise.all(keys.filter(k => k !== CACHE_NAME).map(k => caches.delete(k)))
    ).then(() => self.clients.claim())
  );
});

// Fetch — network-first for API, cache-first for assets
self.addEventListener('fetch', e => {
  const url = new URL(e.request.url);

  // Always network for APIs and POST requests
  if (e.request.method !== 'GET' || url.pathname.startsWith('/api/') ||
      url.pathname.startsWith('/stripe/') || url.pathname.startsWith('/feed/')) {
    return;
  }

  // Cache-first for static files
  if (url.pathname.startsWith('/static/') ||
      url.pathname.match(/\.(css|js|png|jpg|ico|woff2?)$/)) {
    e.respondWith(
      caches.match(e.request).then(cached => {
        if (cached) return cached;
        return fetch(e.request).then(resp => {
          if (resp.ok) {
            const clone = resp.clone();
            caches.open(CACHE_NAME).then(c => c.put(e.request, clone));
          }
          return resp;
        });
      })
    );
    return;
  }

  // Network-first for HTML pages with offline fallback
  e.respondWith(
    fetch(e.request)
      .then(resp => {
        if (resp.ok) {
          const clone = resp.clone();
          caches.open(CACHE_NAME).then(c => c.put(e.request, clone));
        }
        return resp;
      })
      .catch(() => caches.match(e.request).then(cached => {
        if (cached) return cached;
        // Offline fallback for navigation
        if (e.request.mode === 'navigate') {
          return caches.match('/home');
        }
      }))
  );
});

// Background sync — queue actions while offline
self.addEventListener('sync', e => {
  if (e.tag === 'save-vehicle') {
    // Retry queued save requests when back online
    e.waitUntil(retrySavedVehicles());
  }
});

async function retrySavedVehicles() {
  // Placeholder for offline save queue
}

// Push notifications
self.addEventListener('push', e => {
  const data = e.data?.json() || {};
  e.waitUntil(
    self.registration.showNotification(data.title || 'KeyTrack', {
      body: data.body || 'New update',
      icon: '/static/icon-192.png',
      badge: '/static/icon-192.png',
      data: { url: data.url || '/buyer/dashboard' },
      actions: [
        { action: 'view', title: 'View' },
        { action: 'dismiss', title: 'Dismiss' }
      ]
    })
  );
});

self.addEventListener('notificationclick', e => {
  e.notification.close();
  if (e.action !== 'dismiss') {
    const url = e.notification.data?.url || '/buyer/dashboard';
    e.waitUntil(clients.openWindow(url));
  }
});

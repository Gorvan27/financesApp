// Network-first SW. Версию менять при каждом деплое.
const CACHE='finances-v6-20260703';
const ASSETS=['./','./index.html','./manifest.json'];

self.addEventListener('install',e=>{
  e.waitUntil(caches.open(CACHE).then(c=>c.addAll(ASSETS)).catch(()=>{}));
  self.skipWaiting();
});

self.addEventListener('activate',e=>{
  e.waitUntil(
    caches.keys().then(keys=>Promise.all(
      keys.filter(k=>k!==CACHE).map(k=>caches.delete(k))
    )).then(()=>clients.claim())
  );
});

self.addEventListener('fetch',e=>{
  if(e.request.method!=='GET')return; // POST (Sheets) не трогаем
  e.respondWith(
    fetch(e.request).then(res=>{
      if(res.ok&&new URL(e.request.url).origin===location.origin){
        const clone=res.clone();
        caches.open(CACHE).then(c=>c.put(e.request,clone));
      }
      return res;
    }).catch(()=>caches.match(e.request).then(r=>r||caches.match('./')))
  );
});

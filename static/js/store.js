;(function () {
  const API_BASE = ""; // same origin
  const WS_PATH = "/ws/events";
  const PERMITS_PATH = "/permits";

  // ---------------- IndexedDB minimal wrapper ----------------
  const DB_NAME = "permit_tracker";
  const DB_VERSION = 1;
  let dbInstance = null;

  function openDb() {
    return new Promise((resolve, reject) => {
      const req = indexedDB.open(DB_NAME, DB_VERSION);
      req.onupgradeneeded = (e) => {
        const db = e.target.result;
        if (!db.objectStoreNames.contains("permits")) {
          const s = db.createObjectStore("permits", { keyPath: "id" });
          s.createIndex("by_updated_at", "updated_at");
          s.createIndex("by_org_id", "org_id");
        }
        if (!db.objectStoreNames.contains("meta")) {
          db.createObjectStore("meta", { keyPath: "key" });
        }
      };
      req.onsuccess = (e) => { dbInstance = e.target.result; resolve(dbInstance); };
      req.onerror = (e) => reject(e.target.error);
    });
  }

  async function tx(storeName, mode, fn) {
    if (!dbInstance) await openDb();
    return new Promise((resolve, reject) => {
      const t = dbInstance.transaction(storeName, mode);
      const s = t.objectStore(storeName);
      const res = fn(s);
      t.oncomplete = () => resolve(res);
      t.onerror = (e) => reject(e.target.error);
      t.onabort = (e) => reject(e.target.error);
    });
  }

  const DB = {
    upsertPermits: async (rows, orgId) => {
      if (!rows || !rows.length) return;
      await tx("permits", "readwrite", (s) => {
        for (const r of rows) {
          // Ensure org_id is set for tenant isolation
          r.org_id = r.org_id || orgId;
          s.put(r);
        }
      });
    },
    getAllPermits: (orgId) =>
      tx("permits", "readonly", (s) => {
        return new Promise((resolve, reject) => {
          const out = [];
          const index = s.index("by_org_id");
          const req = index.openCursor(IDBKeyRange.only(orgId));
          req.onsuccess = (e) => {
            const cur = e.target.result;
            if (cur) { out.push(cur.value); cur.continue(); } else { resolve(out); }
          };
          req.onerror = (e) => reject(e.target.error);
        });
      }),
    getMeta: (key) => tx("meta", "readonly", (s) => new Promise((resolve, reject) => {
      const req = s.get(key);
      req.onsuccess = () => resolve(req.result ? req.result.value : null);
      req.onerror = (e) => reject(e.target.error);
    })),
    setMeta: (key, value) => tx("meta", "readwrite", (s) => s.put({ key, value })),
  };

  // ---------------- Networking ----------------
  async function jsonFetch(url, opts={}) {
    const r = await fetch(url, { credentials: "same-origin", ...opts });
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    return r.json();
  }

  async function fetchDeltas(sinceEventId, orgId) {
    const u = new URL(PERMITS_PATH, window.location.origin);
    if (sinceEventId != null) u.searchParams.set("since_event_id", String(sinceEventId));
    if (orgId) u.searchParams.set("org_id", orgId);
    return jsonFetch(u.toString());
  }

  // ---------------- Store & Sync ----------------
  const listeners = new Set();
  let lastEventId = 0;
  let hasInit = false;
  let currentOrgId = 'default_org';
  let accessToken = null;
  let ws = null;
  let reconnectAttempts = 0;
  const maxReconnectAttempts = 5;

  function notify() {
    for (const cb of listeners) { try { cb(); } catch {} }
  }

  async function initialLoad() {
    // 1) Paint from cache
    const cached = await DB.getAllPermits(currentOrgId);
    if (cached.length) notify();

    // 2) Pull fresh from API (cold start -> returns top N + last_event_id)
    const data = await fetchDeltas(null, currentOrgId);
    lastEventId = data.last_event_id || 0;
    await DB.upsertPermits(data.permits || [], currentOrgId);
    await DB.setMeta("last_event_id", lastEventId);
    notify();
  }

  function openWS() {
    // Debug: Check what cookies are available
    console.log('🍪 document.cookie contents:', document.cookie);
    
    // For HttpOnly cookies, we can't access them from JavaScript
    // The WebSocket should use the browser's automatic cookie inclusion
    // Let's try connecting without the access_token in the URL first
    
    let wsUrl;
    if (location.protocol === "https:") {
      wsUrl = `wss://${location.host}${WS_PATH}?org_id=${encodeURIComponent(currentOrgId)}`;
    } else {
      wsUrl = `ws://${location.host}${WS_PATH}?org_id=${encodeURIComponent(currentOrgId)}`;
    }
    
    console.log('🔌 Connecting to WebSocket:', wsUrl);
    ws = new WebSocket(wsUrl);

    ws.onopen = () => {
      console.log('🔄 Real-time sync connected');
    };
    ws.onmessage = async (evt) => {
      try {
        const msg = JSON.parse(evt.data);
        if (msg && msg.type === "batch.permit.delta") {
          lastEventId = msg.last_event_id || lastEventId;
          await DB.upsertPermits(msg.permits || [], currentOrgId);
          await DB.setMeta("last_event_id", lastEventId);
          console.log(`📡 Received ${msg.permits?.length || 0} permit updates`);
          notify();
        }
      } catch (e) {
        console.warn('WebSocket message error:', e);
      }
    };
    ws.onclose = (event) => {
      console.log('🔌 Real-time sync disconnected, reconnecting...');
      
      // Handle authentication errors
      if (event.code === 4001) {
        console.warn('WebSocket authentication failed, redirecting to login');
        window.location.href = '/login';
        return;
      }
      
      if (event.code === 4003) {
        console.warn('WebSocket access denied to organization');
        return;
      }
      
      // Exponential backoff for reconnection
      if (reconnectAttempts < maxReconnectAttempts) {
        const delay = Math.min(1000 * Math.pow(2, reconnectAttempts), 30000);
        reconnectAttempts++;
        setTimeout(() => {
          openWS();
        }, delay);
      } else {
        console.error('Max reconnection attempts reached');
      }
    };
    ws.onerror = () => {
      try { ws.close(); } catch {}
    };
  }

  async function backfillLoop() {
    // Periodic safety net (handles missed WS messages or long sleeps on iOS)
    while (true) {
      try {
        const stored = await DB.getMeta("last_event_id");
        const since = stored != null ? stored : lastEventId;
        const data = await fetchDeltas(since, currentOrgId);
        if (data.last_event_id && data.last_event_id !== since) {
          lastEventId = data.last_event_id;
          await DB.upsertPermits(data.permits || [], currentOrgId);
          await DB.setMeta("last_event_id", lastEventId);
          if (data.permits?.length > 0) {
            console.log(`🔄 Backfill sync: ${data.permits.length} updates`);
            notify();
          }
        }
      } catch (e) {
        console.warn('Backfill sync error:', e);
      }
      await new Promise((r) => setTimeout(r, 15000)); // 15s
    }
  }

  // Public API
  const Store = {
    init: async (orgId = 'default_org') => {
      if (hasInit) return;
      
      // Check authentication first
      try {
        const response = await fetch(`${API_BASE}/auth/me`, {
          credentials: 'include'
        });
        
        if (!response.ok) {
          console.warn('Authentication required, redirecting to login');
          window.location.href = '/login';
          return;
        }
        
        const userData = await response.json();
        console.log('Authenticated as:', userData.email);
      } catch (error) {
        console.warn('Auth check failed:', error);
        window.location.href = '/login';
        return;
      }
      
      hasInit = true;
      currentOrgId = orgId;
      
      // Paint quickly from cache while network warms
      await openDb();

      // Load cached + cold pull, then open WS and start backfill
      initialLoad().catch((e) => console.warn('Initial load error:', e));
      openWS();
      backfillLoop(); // no await
    },
    subscribe: (cb) => {
      listeners.add(cb);
      return () => listeners.delete(cb);
    },
    // Convenience: read current list (for your dashboard to render)
    getAllPermits: () => DB.getAllPermits(currentOrgId),
    
    // Get current org ID
    getCurrentOrgId: () => currentOrgId,
    
    // Set org ID (for future auth integration)
    setOrgId: (orgId) => {
      currentOrgId = orgId;
    }
  };

  // Expose globally so your existing dashboard can hook in
  window.PermitStore = Store;

})();

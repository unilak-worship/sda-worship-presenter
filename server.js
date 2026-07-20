/**
 * SDA Worship System — Server
 * - Serves presenter.html, bible_data.json, hymns_data.js
 * - WebSocket hub: slide sync + WebRTC signaling + presenter permission system
 * - File upload endpoint for video/image sharing
 *
 * NOTE: this is the CLOUD server for the unilak-worship/sda-worship-presenter
 * repo. Commit it there as `server.js` (not to be confused with the Electron
 * desktop app's own server.js).
 */

const express = require('express');
const http    = require('http');
const { WebSocketServer } = require('ws');
const path    = require('path');
const fs      = require('fs');
const os      = require('os');

const PORT       = process.env.PORT || 4040;
const UPLOAD_DIR = path.join(__dirname, 'public', 'uploads');

if (!fs.existsSync(UPLOAD_DIR)) fs.mkdirSync(UPLOAD_DIR, { recursive: true });

const app    = express();
const server = http.createServer(app);
const wss    = new WebSocketServer({ server });

// ── CORS ─────────────────────────────────────────────────────────────────────
app.use((req, res, next) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, X-Filename, X-Filetype');
  if (req.method === 'OPTIONS') { res.sendStatus(200); return; }
  next();
});

// ── Static files ──────────────────────────────────────────────────────────────
app.use(express.static(path.join(__dirname, 'public'), {
  setHeaders: (res, fp) => {
    if (fp.endsWith('bible_data.json') || fp.endsWith('hymns_data.js'))
      res.setHeader('Cache-Control', 'no-cache');
    if (fp.includes('/uploads/'))
      res.setHeader('Access-Control-Allow-Origin', '*');
  }
}));
app.get('/', (req, res) => res.sendFile(path.join(__dirname, 'public', 'presenter.html')));

// ── File Upload ───────────────────────────────────────────────────────────────
app.post('/upload', (req, res) => {
  const rawName  = req.headers['x-filename'] || ('file_' + Date.now());
  const mimeType = req.headers['x-filetype'] || 'application/octet-stream';
  const ext      = path.extname(rawName).toLowerCase().replace(/[^a-z0-9.]/g, '');
  const baseName = path.basename(rawName, path.extname(rawName))
    .replace(/[^a-z0-9_\-]/gi, '_').substring(0, 60);
  const safeFile = baseName + '_' + Date.now() + ext;
  const filePath = path.join(UPLOAD_DIR, safeFile);
  const writeStream = fs.createWriteStream(filePath);
  let size = 0;

  req.on('data', chunk => {
    size += chunk.length;
    if (size > 500 * 1024 * 1024) {
      writeStream.destroy(); fs.unlink(filePath, () => {});
      if (!res.headersSent) res.status(413).json({ error: 'File too large (max 500MB)' });
      return;
    }
    writeStream.write(chunk);
  });
  req.on('end', () => {
    writeStream.end();
    const url = '/uploads/' + safeFile;
    console.log(`[Upload] ${safeFile} (${(size/1024/1024).toFixed(1)}MB)`);
    const msg = JSON.stringify({ type:'file-share', fileName:rawName, fileUrl:url, mimeType, fileSize:size });
    wss.clients.forEach(c => { if (c.readyState === 1) try { c.send(msg); } catch(_) {} });
    if (!res.headersSent) res.json({ success:true, url, fileName:rawName, fileSize:size });
  });
  req.on('error', err => {
    writeStream.destroy(); fs.unlink(filePath, () => {});
    if (!res.headersSent) res.status(500).json({ error: err.message });
  });
});

app.get('/uploads-list', (req, res) => {
  try {
    const files = fs.readdirSync(UPLOAD_DIR)
      .filter(f => !f.startsWith('.'))
      .map(f => {
        const stat = fs.statSync(path.join(UPLOAD_DIR, f));
        return { name:f, url:'/uploads/'+f, size:stat.size, mtime:stat.mtime };
      }).sort((a,b) => b.mtime - a.mtime).slice(0, 20);
    res.json(files);
  } catch(e) { res.json([]); }
});

app.get('/status', (req, res) => {
  const presenterList = [...presenterMap.values()].map(p => ({
    id: p.id, name: p.name, role: p.role, granted: p.granted, joinedAt: p.joinedAt
  }));
  res.json({ status:'running', clients:wss.clients.size, uptime:Math.floor(process.uptime())+'s', presenters: presenterList });
});

// ── WebSocket hub ─────────────────────────────────────────────────────────────
let lastSlideState = { type: 'blank' };

// Message types that are transport/control only — never projector state.
// Without this, any unrecognised message (e.g. the operator's 5s heartbeat)
// became the "current slide" and was replayed to every newly-connected client.
const NON_STATE_TYPES = new Set([
  'webrtc-offer', 'webrtc-answer', 'webrtc-ice', 'ping',
  'get-presenter-list', 'operator-join', 'presenter-join',
  'presenter-grant', 'presenter-deny', 'presenter-kick',
  'presenter-status', 'presenter-list', 'program-go', 'file-share',
  'presenter-request', 'presenter-request-result', 'share-stop',
  'choir-sync', 'program-sync', 'library-sync', 'share-ready',
]);

// Presenter registry: wsId → { ws, id, name, role, granted, joinedAt }
const presenterMap = new Map();
let wsIdCounter = 0;

// Find the operator connection (role === 'operator')
function getOperatorWs() {
  for (const p of presenterMap.values()) {
    if (p.role === 'operator' && p.ws.readyState === 1) return p.ws;
  }
  return null;
}

// Broadcast to ALL connected clients
function broadcastAll(msg, excludeWs = null) {
  const str = JSON.stringify(msg);
  wss.clients.forEach(c => {
    if (c !== excludeWs && c.readyState === 1) try { c.send(str); } catch(_) {}
  });
}

// Send updated presenter list to operator
function notifyOperatorPresenterList() {
  const op = getOperatorWs();
  if (!op) return;
  const list = [...presenterMap.values()]
    .filter(p => p.role === 'presenter')
    .map(p => ({ id: p.id, name: p.name, granted: p.granted, joinedAt: p.joinedAt }));
  try { op.send(JSON.stringify({ type: 'presenter-list', presenters: list })); } catch(_) {}
}

// Resolve a target presenter from a message, tolerating string/number ids.
// Browser dataset values are always strings, so `p.id === msg.presenterId`
// ("137" === 137) silently failed. Compare as strings instead.
function findPresenter(msg) {
  const tid = String(msg.presenterId ?? msg.id ?? msg.presId ?? '');
  if (!tid) return null;
  return [...presenterMap.values()].find(p => String(p.id) === tid) || null;
}

// Is this socket a registered operator?
function isOperator(wsId) {
  const sender = presenterMap.get(wsId);
  return !!sender && sender.role === 'operator';
}

wss.on('connection', (ws) => {
  const wsId = ++wsIdCounter;
  console.log(`[WS] Client ${wsId} connected (${wss.clients.size} total)`);

  // Send current slide state immediately
  try { ws.send(JSON.stringify(lastSlideState)); } catch(_) {}

  ws.on('message', raw => {
    let msg;
    try { msg = JSON.parse(raw); } catch(_) { return; }
    if (!msg || !msg.type) return;

    // ── Presenter announces itself ──────────────────────────────────────────
    if (msg.type === 'presenter-join') {
      presenterMap.set(wsId, {
        ws, id: wsId,
        name: msg.name || 'Unknown',
        role: 'presenter',
        granted: false,
        joinedAt: new Date().toISOString(),
      });
      // Notify operator of new join request
      notifyOperatorPresenterList();
      // Tell the presenter they are waiting for approval (include their id so
      // the client can ignore status messages meant for other presenters)
      try { ws.send(JSON.stringify({
        type: 'presenter-status',
        id: wsId, presId: wsId,
        status: 'waiting',
        message: 'Waiting for operator approval…',
      })); } catch(_) {}
      return;
    }

    // ── Operator registers itself ───────────────────────────────────────────
    // Idempotent: the operator re-announces every 5s as a heartbeat, which both
    // re-registers it after a reconnect and refreshes its presenter list.
    if (msg.type === 'operator-join') {
      presenterMap.set(wsId, { ws, id: wsId, name: 'Operator', role: 'operator', granted: true, joinedAt: new Date().toISOString() });
      notifyOperatorPresenterList();
      return;
    }

    // ── Operator requests the current presenter list ────────────────────────
    if (msg.type === 'get-presenter-list') {
      notifyOperatorPresenterList();
      return;
    }

    // ── Operator grants/denies presenter (operator only) ────────────────────
    if (msg.type === 'presenter-grant' || msg.type === 'presenter-deny') {
      if (!isOperator(wsId)) return;
      const entry = findPresenter(msg);
      if (!entry) return;

      const granted = msg.type === 'presenter-grant';
      entry.granted = granted;
      try { entry.ws.send(JSON.stringify({
        type:    'presenter-status',
        id:      entry.id,
        presId:  entry.id,
        status:  granted ? 'granted' : 'denied',
        message: granted
          ? 'Access granted — you can now control the projector'
          : 'Access was denied by the operator',
      })); } catch(_) {}
      notifyOperatorPresenterList();
      return;
    }

    // ── Operator removes/kicks a presenter (operator only) ──────────────────
    if (msg.type === 'presenter-kick') {
      if (!isOperator(wsId)) return;
      const entry = findPresenter(msg);
      if (!entry) return;

      entry.granted = false;
      try { entry.ws.send(JSON.stringify({
        type:    'presenter-status',
        id:      entry.id,
        presId:  entry.id,
        status:  'kicked',
        message: 'You have been removed by the operator.',
      })); } catch(_) {}
      notifyOperatorPresenterList();
      return;
    }

    // ── Operator sets a presenter's status (operator only, targeted) ────────
    // Previously this fell through to the catch-all and was broadcast to every
    // client — and the presenter page applied it without checking the id, so
    // granting one presenter granted them all.
    if (msg.type === 'presenter-status') {
      if (!isOperator(wsId)) return;
      const entry = findPresenter(msg);
      if (!entry) return;

      if (msg.status === 'granted') entry.granted = true;
      if (msg.status === 'revoked' || msg.status === 'kicked') entry.granted = false;

      try { entry.ws.send(JSON.stringify({
        type:   'presenter-status',
        id:     entry.id,
        presId: entry.id,
        status: msg.status,
      })); } catch(_) {}
      notifyOperatorPresenterList();
      return;
    }

    // ── Presenter proposes a program/announcement change ────────────────────
    // Routed ONLY to the operator, who approves or rejects it. Presenters may
    // never write to the operator's data directly.
    if (msg.type === 'presenter-request') {
      const sender = presenterMap.get(wsId);
      if (!sender || sender.role !== 'presenter') return;

      const op = getOperatorWs();
      if (!op) {
        try { ws.send(JSON.stringify({
          type: 'presenter-request-result',
          reqId: msg.reqId,
          approved: false,
          message: 'Operator is not connected — try again shortly.',
        })); } catch (_) {}
        return;
      }

      try { op.send(JSON.stringify({
        ...msg,
        fromId:   sender.id,
        fromName: sender.name,
      })); } catch (_) {}
      return;
    }

    // ── Operator's verdict on a presenter request (operator only) ───────────
    if (msg.type === 'presenter-request-result') {
      if (!isOperator(wsId)) return;
      const entry = [...presenterMap.values()].find(p => String(p.id) === String(msg.toId));
      if (!entry) return;
      try { entry.ws.send(JSON.stringify(msg)); } catch (_) {}
      return;
    }

    // ── Slide/blank from GRANTED presenter or operator ──────────────────────
    if (msg.type === 'slide' || msg.type === 'blank' || msg.type === 'unblank') {
      const sender = presenterMap.get(wsId);
      // Allow if: sender is unregistered (e.g. projector), operator, or granted
      const isAllowed = !sender || sender.role === 'operator' || sender.granted;
      if (!isAllowed) {
        try { ws.send(JSON.stringify({ type: 'presenter-status', status: 'denied', message: 'You do not have permission to control the projector.' })); } catch(_) {}
        return;
      }
      lastSlideState = msg;
      broadcastAll(msg, ws);
      return;
    }

    // ── Ping / keepalive ────────────────────────────────────────────────────
    if (msg.type === 'ping') return;

    // ── All other messages (WebRTC signaling, file-share) — relay freely ────
    if (!NON_STATE_TYPES.has(msg.type)) lastSlideState = msg;
    broadcastAll(msg, ws);
  });

  ws.on('close', () => {
    presenterMap.delete(wsId);
    notifyOperatorPresenterList();
    console.log(`[WS] Client ${wsId} disconnected (${wss.clients.size} total)`);
  });
  ws.on('error', err => console.error('[WS] Error:', err.message));
});

// ── Start ─────────────────────────────────────────────────────────────────────
server.listen(PORT, '0.0.0.0', () => {
  const nets = os.networkInterfaces();
  let ip = 'localhost';
  for (const n of Object.values(nets)) {
    const v4 = n.find(x => x.family === 'IPv4' && !x.internal);
    if (v4) { ip = v4.address; break; }
  }
  console.log(`\n  ✝  SDA Worship System — Server\n`);
  console.log(`  Local:   http://${ip}:${PORT}`);
  console.log(`  Uploads: ${UPLOAD_DIR}\n`);
});

module.exports = { server, wss, PORT };

/**
 * SDA Worship System — Server
 * - Serves presenter.html, bible_data.json, hymns_data.js
 * - WebSocket hub: slide sync + WebRTC signaling + presenter permission system
 * - File upload endpoint for video/image sharing
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
    id: p.id, name: p.name, granted: p.granted, joinedAt: p.joinedAt
  }));
  res.json({ status:'running', clients:wss.clients.size, uptime:Math.floor(process.uptime())+'s', presenters: presenterList });
});

// ── WebSocket hub ─────────────────────────────────────────────────────────────
let lastSlideState = { type: 'blank' };

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

wss.on('connection', (ws) => {
  const wsId = ++wsIdCounter;
  console.log(`[WS] Client ${wsId} connected (${wss.clients.size} total)`);

  // Send current slide state immediately
  try { ws.send(JSON.stringify(lastSlideState)); } catch(_) {}

  ws.on('message', raw => {
    let msg;
    try { msg = JSON.parse(raw); } catch(_) { return; }

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
      // Tell the presenter they are waiting for approval
      try { ws.send(JSON.stringify({ type: 'presenter-status', status: 'waiting', message: 'Waiting for operator approval…' })); } catch(_) {}
      return;
    }

    // ── Operator registers itself ───────────────────────────────────────────
    if (msg.type === 'operator-join') {
      presenterMap.set(wsId, { ws, id: wsId, name: 'Operator', role: 'operator', granted: true, joinedAt: new Date().toISOString() });
      // Send current presenter list to newly connected operator
      notifyOperatorPresenterList();
      return;
    }

    // ── Operator grants/denies presenter ───────────────────────────────────
    if (msg.type === 'presenter-grant' || msg.type === 'presenter-deny') {
      const entry = [...presenterMap.values()].find(p => p.id === msg.presenterId);
      if (!entry) return;
      const granted = msg.type === 'presenter-grant';
      entry.granted = granted;
      // Notify the presenter
      try { entry.ws.send(JSON.stringify({
        type: 'presenter-status',
        status: granted ? 'granted' : 'denied',
        message: granted ? 'Access granted — you can now control the projector' : 'Access was denied by the operator',
      })); } catch(_) {}
      // Update operator list
      notifyOperatorPresenterList();
      return;
    }

    // ── Operator removes/kicks a presenter ─────────────────────────────────
    if (msg.type === 'presenter-kick') {
      const entry = [...presenterMap.values()].find(p => p.id === msg.presenterId);
      if (!entry) return;
      entry.granted = false;
      try { entry.ws.send(JSON.stringify({ type: 'presenter-status', status: 'kicked', message: 'You have been removed by the operator.' })); } catch(_) {}
      notifyOperatorPresenterList();
      return;
    }

    // ── Slide/blank from GRANTED presenter or operator ─────────────────────
    if (msg.type === 'slide' || msg.type === 'blank') {
      const sender = presenterMap.get(wsId);
      // Allow if: sender is operator, or sender is a granted presenter
      const isAllowed = !sender || sender.role === 'operator' || sender.granted;
      if (!isAllowed) {
        try { ws.send(JSON.stringify({ type: 'presenter-status', status: 'denied', message: 'You do not have permission to control the projector.' })); } catch(_) {}
        return;
      }
      if (!['webrtc-offer','webrtc-answer','webrtc-ice','ping'].includes(msg.type))
        lastSlideState = msg;
      broadcastAll(msg, ws);
      return;
    }

    // ── Ping / keepalive ───────────────────────────────────────────────────
    if (msg.type === 'ping') return;

    // ── All other messages (WebRTC signaling, file-share) — relay freely ───
    if (!['webrtc-offer','webrtc-answer','webrtc-ice','ping'].includes(msg.type))
      lastSlideState = msg;
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

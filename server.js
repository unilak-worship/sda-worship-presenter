/**
 * SDA Worship System — Server
 * - Serves presenter.html, bible_data.json, hymns_data.js
 * - WebSocket hub for slide sync + WebRTC signaling
 * - File upload endpoint for PPT/video sharing
 */

const express = require('express');
const http    = require('http');
const { WebSocketServer } = require('ws');
const path    = require('path');
const fs      = require('fs');
const os      = require('os');

const PORT       = process.env.PORT || 4040;
const UPLOAD_DIR = path.join(__dirname, 'public', 'uploads');

// Ensure uploads directory exists
if (!fs.existsSync(UPLOAD_DIR)) fs.mkdirSync(UPLOAD_DIR, { recursive: true });

const app    = express();
const server = http.createServer(app);
const wss    = new WebSocketServer({ server });

// ── CORS ──────────────────────────────────────────────────────────────────
app.use((req, res, next) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, X-Filename, X-Filetype');
  if (req.method === 'OPTIONS') { res.sendStatus(200); return; }
  next();
});

// ── Static files ──────────────────────────────────────────────────────────
app.use(express.static(path.join(__dirname, 'public'), {
  setHeaders: (res, fp) => {
    if (fp.endsWith('bible_data.json') || fp.endsWith('hymns_data.js')) {
      res.setHeader('Cache-Control', 'no-cache');
    }
    // Allow cross-origin for video streaming
    if (fp.includes('/uploads/')) {
      res.setHeader('Access-Control-Allow-Origin', '*');
    }
  }
}));

// ── Root → presenter.html ─────────────────────────────────────────────────
app.get('/', (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'presenter.html'));
});

// ── File Upload endpoint ──────────────────────────────────────────────────
// POST /upload  — raw body stream, filename in X-Filename header
app.post('/upload', (req, res) => {
  const rawName = req.headers['x-filename'] || ('file_' + Date.now());
  const mimeType = req.headers['x-filetype'] || 'application/octet-stream';
  
  // Sanitize filename — keep extension, replace unsafe chars
  const ext      = path.extname(rawName).toLowerCase().replace(/[^a-z0-9.]/g, '');
  const baseName = path.basename(rawName, path.extname(rawName))
    .replace(/[^a-z0-9_\-]/gi, '_').substring(0, 60);
  const safeFile = baseName + '_' + Date.now() + ext;
  const filePath = path.join(UPLOAD_DIR, safeFile);

  const writeStream = fs.createWriteStream(filePath);
  let size = 0;

  req.on('data', chunk => {
    size += chunk.length;
    if (size > 500 * 1024 * 1024) { // 500MB limit
      writeStream.destroy();
      fs.unlink(filePath, () => {});
      res.status(413).json({ error: 'File too large (max 500MB)' });
      return;
    }
    writeStream.write(chunk);
  });

  req.on('end', () => {
    writeStream.end();
    const url = '/uploads/' + safeFile;
    console.log(`[Upload] ${safeFile} (${(size/1024/1024).toFixed(1)}MB)`);

    // Broadcast to all clients that a new file is available
    const msg = JSON.stringify({
      type: 'file-share',
      fileName: rawName,
      fileUrl: url,
      mimeType,
      fileSize: size,
    });
    wss.clients.forEach(c => { if (c.readyState === 1) try { c.send(msg); } catch(_) {} });

    res.json({ success: true, url, fileName: rawName, fileSize: size });
  });

  req.on('error', err => {
    writeStream.destroy();
    fs.unlink(filePath, () => {});
    res.status(500).json({ error: err.message });
  });
});

// ── List uploaded files ────────────────────────────────────────────────────
app.get('/uploads-list', (req, res) => {
  try {
    const files = fs.readdirSync(UPLOAD_DIR).map(f => {
      const stat = fs.statSync(path.join(UPLOAD_DIR, f));
      return { name: f, url: '/uploads/' + f, size: stat.size, mtime: stat.mtime };
    }).sort((a, b) => b.mtime - a.mtime).slice(0, 20);
    res.json(files);
  } catch(e) { res.json([]); }
});

// ── Health check ───────────────────────────────────────────────────────────
app.get('/status', (req, res) => {
  res.json({ status: 'running', clients: wss.clients.size,
    uptime: Math.floor(process.uptime()) + 's' });
});

// ── WebSocket hub (slides + WebRTC signaling + file-share) ────────────────
let lastState = { type: 'blank' };

wss.on('connection', (ws, req) => {
  console.log(`[WS] Client connected (${wss.clients.size} total)`);

  // Send current state to new client
  try { ws.send(JSON.stringify(lastState)); } catch(_) {}

  ws.on('message', raw => {
    let msg;
    try { msg = JSON.parse(raw); } catch(_) { return; }

    // Persist last slide/blank state (not WebRTC signaling)
    if (!['webrtc-offer','webrtc-answer','webrtc-ice','ping'].includes(msg.type)) {
      lastState = msg;
    }

    // Relay to ALL other clients (slide sync + WebRTC signaling)
    wss.clients.forEach(client => {
      if (client !== ws && client.readyState === 1) {
        try { client.send(JSON.stringify(msg)); } catch(_) {}
      }
    });
  });

  ws.on('close', () => console.log(`[WS] Client disconnected (${wss.clients.size} total)`));
  ws.on('error', err => console.error('[WS] Error:', err.message));
});

// ── Start ──────────────────────────────────────────────────────────────────
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

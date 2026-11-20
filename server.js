// LUDO online server — zero dependencies (Node 16+).
//   node server.js            -> http://localhost:8080  (serves Card-and-Dice.html + WebSocket relay)
// Deploy anywhere that runs Node (Render, Railway, Fly.io, a VPS...). It uses the PORT env variable.
const http = require('http'), fs = require('fs'), path = require('path'), crypto = require('crypto');
const PORT = process.env.PORT || 8080;
const GUID = '258EAFA5-E914-47DA-95CA-C5AB0DC85B11';
const MAX_MEMBERS = 8, MAX_FRAME = 8192;
const rooms = new Map();

/* ---------- static page ---------- */
const STATIC = { '/manifest.webmanifest': 'application/manifest+json', '/icon-192.png': 'image/png', '/icon-512.png': 'image/png' };
const server = http.createServer((req, res) => {
  const u = req.url.split('?')[0];
  if (STATIC[u]) {
    return fs.readFile(path.join(__dirname, u), (e, b) => {
      if (e) { res.writeHead(404); return res.end('Not found'); }
      res.writeHead(200, { 'Content-Type': STATIC[u], 'Cache-Control': 'public, max-age=3600' }); res.end(b);
    });
  }
  if (u === '/' || u === '/ludo.html' || u === '/Card-and-Dice.html' || u === '/index.html') {
    const file = ['Card-and-Dice.html', 'card-and-dice.html', 'index.html', 'ludo.html', 'LUDO.html'].map(f => path.join(__dirname, f)).find(f => fs.existsSync(f));
    fs.readFile(file || 'Card-and-Dice.html', (e, b) => {
      if (e) { res.writeHead(500); return res.end('Card-and-Dice.html not found next to server.js'); }
      res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8', 'Cache-Control': 'no-cache' });
      res.end(b);
    });
  } else if (u === '/health') { res.writeHead(200); res.end('ok'); }
  else { res.writeHead(404); res.end('Not found'); }
});

/* ---------- minimal WebSocket (RFC 6455) ---------- */
function frame(op, payload) {
  const p = Buffer.isBuffer(payload) ? payload : Buffer.from(payload || '');
  let h;
  if (p.length < 126) h = Buffer.from([0x80 | op, p.length]);
  else if (p.length < 65536) { h = Buffer.alloc(4); h[0] = 0x80 | op; h[1] = 126; h.writeUInt16BE(p.length, 2); }
  else { h = Buffer.alloc(10); h[0] = 0x80 | op; h[1] = 127; h.writeBigUInt64BE(BigInt(p.length), 2); }
  return Buffer.concat([h, p]);
}

class Client {
  constructor(socket) {
    this.socket = socket; this.buf = Buffer.alloc(0); this.room = null; this.id = -1; this.alive = true;
    this.msgBuf = []; this.msgOp = 0;
    socket.setNoDelay(true);
    socket.on('data', d => this.onData(d));
    socket.on('close', () => this.close());
    socket.on('error', () => this.close());
  }
  send(obj) { if (this.alive) try { this.socket.write(frame(1, JSON.stringify(obj))); } catch (e) {} }
  ping() { if (this.alive) try { this.socket.write(frame(9)); } catch (e) {} }
  onData(d) {
    this.buf = Buffer.concat([this.buf, d]);
    for (;;) {
      const b = this.buf; if (b.length < 2) return;
      const fin = !!(b[0] & 0x80), op = b[0] & 0x0f, masked = !!(b[1] & 0x80);
      let len = b[1] & 0x7f, off = 2;
      if (len === 126) { if (b.length < 4) return; len = b.readUInt16BE(2); off = 4; }
      else if (len === 127) { if (b.length < 10) return; len = Number(b.readBigUInt64BE(2)); off = 10; }
      if (len > MAX_FRAME) return this.kill();
      if (!masked) return this.kill();
      if (b.length < off + 4 + len) return;
      const mask = b.slice(off, off + 4), data = Buffer.from(b.slice(off + 4, off + 4 + len));
      for (let i = 0; i < data.length; i++) data[i] ^= mask[i & 3];
      this.buf = b.slice(off + 4 + len);
      if (op === 8) { try { this.socket.write(frame(8)); } catch (e) {} return this.kill(); }
      if (op === 9) { try { this.socket.write(frame(10, data)); } catch (e) {} continue; }
      if (op === 10) continue;
      if (op === 1 || op === 2) { this.msgOp = op; this.msgBuf = [data]; } else if (op === 0) this.msgBuf.push(data);
      if (fin) {
        const text = Buffer.concat(this.msgBuf).toString('utf8'); this.msgBuf = [];
        let m; try { m = JSON.parse(text); } catch (e) { continue; }
        if (m && typeof m === 'object') handle(this, m);
      }
    }
  }
  kill() { try { this.socket.destroy(); } catch (e) {} this.close(); }
  close() { if (!this.alive) return; this.alive = false; leave(this); }
}

/* ---------- rooms ---------- */
const LET = 'ABCDEFGHJKLMNPQRSTUVWXYZ';
function newCode() { for (;;) { let c = ''; for (let i = 0; i < 4; i++) c += LET[crypto.randomInt(LET.length)]; if (!rooms.has(c)) return c; } }
function members(r) { return [...r.members.keys()].sort((a, b) => a - b); }
function broadcast(r, obj, except) { for (const c of r.members.values()) if (c !== except) c.send(obj); }
function roomInfo(r) { broadcast(r, { t: 'room', members: members(r), host: 0 }); }

/* ---------- random-player matchmaking: one queue per game ---------- */
const queues = { ludo: [], sl: [], cb: [], jt: [], dh: [], tp: [], kt: [], mr: [] };
function unqueue(c) { for (const g in queues) queues[g] = queues[g].filter(e => e.c !== c); }
function qInfo(g) { queues[g].forEach(e => e.c.send({ t: 'queued', n: queues[g].length })); }
function matchGame(g, list) {
  queues[g] = queues[g].filter(e => !list.includes(e));
  const code = newCode(), r = { code, members: new Map(), nextId: list.length, locked: true };
  rooms.set(code, r);
  list.forEach((e, i) => { e.c.room = r; e.c.id = i; r.members.set(i, e.c); e.c.send({ t: 'matched', code, id: i, game: g }); });
  roomInfo(r); qInfo(g);
}
// 4 players start at once; with 2-3 waiting, the game starts after 8 s (empty seats become computer players)
setInterval(() => {
  for (const g in queues) {
    const q = queues[g] = queues[g].filter(e => e.c.alive);
    if (q.length >= 4) matchGame(g, q.slice(0, 4));
    else if (q.length >= 2 && Date.now() - q[0].t >= 8000) matchGame(g, q.slice());
  }
}, 1000);

function leave(c) {
  unqueue(c);
  const r = c.room; if (!r) return;
  c.room = null; r.members.delete(c.id);
  if (c.id === 0) { broadcast(r, { t: 'closed' }); for (const m of r.members.values()) m.room = null; rooms.delete(r.code); }
  else if (r.members.size === 0) rooms.delete(r.code);
  else { broadcast(r, { t: 'left', id: c.id }); roomInfo(r); }
}

function handle(c, m) {
  switch (m.t) {
    case 'create': {
      leave(c);
      const code = newCode(), r = { code, members: new Map(), nextId: 1, locked: false };
      c.room = r; c.id = 0; r.members.set(0, c); rooms.set(code, r);
      c.send({ t: 'created', code, id: 0 }); roomInfo(r); break;
    }
    case 'join': {
      leave(c);
      const code = String(m.code || '').toUpperCase().trim(), r = rooms.get(code);
      if (!r) return c.send({ t: 'err', m: 'Room not found. Check the code.' });
      if (r.locked) return c.send({ t: 'err', m: 'That game has already started.' });
      if (r.members.size >= MAX_MEMBERS) return c.send({ t: 'err', m: 'Room is full.' });
      c.room = r; c.id = r.nextId++; r.members.set(c.id, c);
      c.send({ t: 'joined', code, id: c.id }); roomInfo(r); break;
    }
    case 'quick': {
      leave(c);
      const g = queues[m.game] ? m.game : 'ludo';
      queues[g].push({ c, t: Date.now() }); qInfo(g);
      if (queues[g].length >= 4) matchGame(g, queues[g].slice(0, 4));
      break;
    }
    case 'unqueue': unqueue(c); break;
    case 'lock': if (c.room && c.id === 0) c.room.locked = true; break;
    case 'msg': if (c.room) broadcast(c.room, { t: 'msg', from: c.id, d: m.d }, c); break;
    default: break;
  }
}

server.on('upgrade', (req, socket) => {
  const key = req.headers['sec-websocket-key'];
  if (!key || String(req.headers.upgrade).toLowerCase() !== 'websocket') return socket.destroy();
  const accept = crypto.createHash('sha1').update(key + GUID).digest('base64');
  socket.write('HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: ' + accept + '\r\n\r\n');
  const c = new Client(socket); clients.add(c); socket.on('close', () => clients.delete(c));
});
const clients = new Set();
setInterval(() => clients.forEach(c => c.ping()), 20000); // keeps connections alive behind proxies

server.on('error', e => {
  if (e.code === 'EADDRINUSE') console.log('\nPort ' + PORT + ' is already in use: another copy of this server is still running.\nClose every VS Code terminal (trash icon) or press Ctrl+C in them, then run node server.js again.\n');
  else console.log(e);
  process.exit(1);
});
server.listen(PORT, () => {
  console.log('LUDO server running on port ' + PORT);
  console.log('  On this computer : http://localhost:' + PORT);
  const nets = require('os').networkInterfaces();
  for (const k of Object.keys(nets)) for (const n of nets[k]) if (n.family === 'IPv4' && !n.internal)
    console.log('  On your phone    : http://' + n.address + ':' + PORT + '   (phone must be on the same Wi-Fi)');
});

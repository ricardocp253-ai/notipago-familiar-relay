const http = require('http');
const { WebSocketServer, WebSocket } = require('ws');

const PORT = process.env.PORT || 8787;
const MAX_BUFFER_PER_ROOM = 100;
const TTL_MS = 24 * 60 * 60 * 1000;
const ROOM_ID_REGEX = /^[a-zA-Z0-9_-]{16,64}$/;

function isValidEnvelope(payload) {
  if (!payload || typeof payload !== 'object') return false;
  if (payload.type !== 'encrypted_event') return false;
  if (typeof payload.roomId !== 'string' || !ROOM_ID_REGEX.test(payload.roomId)) return false;
  if (typeof payload.eventId !== 'string' || payload.eventId.trim().length === 0) return false;
  if (typeof payload.timestamp !== 'number' || !Number.isFinite(payload.timestamp)) return false;
  if (typeof payload.iv !== 'string' || payload.iv.trim().length === 0) return false;
  if (typeof payload.ciphertext !== 'string' || payload.ciphertext.trim().length === 0) return false;
  return true;
}

function cleanEnvelope(event) {
  return {
    type: event.type,
    roomId: event.roomId,
    eventId: event.eventId,
    timestamp: event.timestamp,
    iv: event.iv,
    ciphertext: event.ciphertext
  };
}

function createServer() {
  const rooms = new Map();

  function getOrCreateRoom(roomId) {
    let room = rooms.get(roomId);
    if (!room) {
      room = {
        events: [],
        eventMap: new Map(),
        clients: new Set()
      };
      rooms.set(roomId, room);
    }
    return room;
  }

  function pruneExpiredEvents(room) {
    const cutoff = Date.now() - TTL_MS;
    while (room.events.length > 0 && room.events[0]._cachedAt < cutoff) {
      const expired = room.events.shift();
      room.eventMap.delete(expired.eventId);
    }
  }

  function addEventToRoom(roomId, event) {
    const room = getOrCreateRoom(roomId);
    pruneExpiredEvents(room);

    if (room.eventMap.has(event.eventId)) {
      return { deduplicated: true };
    }

    const storedEvent = {
      ...cleanEnvelope(event),
      _cachedAt: Date.now()
    };

    room.events.push(storedEvent);
    room.eventMap.set(event.eventId, storedEvent);

    if (room.events.length > MAX_BUFFER_PER_ROOM) {
      const overflow = room.events.splice(0, room.events.length - MAX_BUFFER_PER_ROOM);
      for (const item of overflow) {
        room.eventMap.delete(item.eventId);
      }
    }

    return { deduplicated: false };
  }

  function getEventsSince(roomId, since) {
    const room = rooms.get(roomId);
    if (!room) return [];
    pruneExpiredEvents(room);
    return room.events
      .filter(event => event.timestamp > since)
      .map(cleanEnvelope);
  }

  function broadcastToRoom(roomId, payload, senderWs = null) {
    const room = rooms.get(roomId);
    if (!room) return 0;
    const message = typeof payload === 'string' ? payload : JSON.stringify(payload);
    let deliveredCount = 0;

    for (const client of room.clients) {
      if (client !== senderWs && client.readyState === WebSocket.OPEN) {
        client.send(message);
        deliveredCount++;
      }
    }
    return deliveredCount;
  }

  function cleanupEmptyRoom(roomId) {
    const room = rooms.get(roomId);
    if (room && room.clients.size === 0 && room.events.length === 0) {
      rooms.delete(roomId);
    }
  }

  const server = http.createServer((req, res) => {
    res.setHeader('Access-Control-Allow-Origin', '*');
    res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
    res.setHeader('Access-Control-Allow-Headers', 'Content-Type');

    if (req.method === 'OPTIONS') {
      res.writeHead(204);
      res.end();
      return;
    }

    const host = req.headers.host || 'localhost';
    const parsedUrl = new URL(req.url, `http://${host}`);

    if (req.method === 'GET' && parsedUrl.pathname === '/health') {
      let activeRooms = 0;
      for (const room of rooms.values()) {
        if (room.clients.size > 0 || room.events.length > 0) {
          activeRooms++;
        }
      }
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({
        ok: true,
        uptime: Math.floor(process.uptime()),
        activeRooms
      }));
      return;
    }

    if (req.method === 'POST' && parsedUrl.pathname === '/api/events') {
      let body = '';
      req.on('data', chunk => {
        body += chunk;
        if (body.length > 1e6) {
          req.socket.destroy();
        }
      });

      req.on('end', () => {
        let envelope;
        try {
          envelope = JSON.parse(body);
        } catch {
          res.writeHead(400, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify({ ok: false, error: 'Invalid JSON payload' }));
          return;
        }

        if (!isValidEnvelope(envelope)) {
          res.writeHead(400, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify({ ok: false, error: 'Invalid encrypted envelope' }));
          return;
        }

        const sanitized = cleanEnvelope(envelope);
        const { deduplicated } = addEventToRoom(sanitized.roomId, sanitized);
        let deliveredTo = 0;

        if (!deduplicated) {
          deliveredTo = broadcastToRoom(sanitized.roomId, sanitized);
        }

        res.writeHead(201, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({
          ok: true,
          deliveredTo,
          deduplicated
        }));
      });
      return;
    }

    if (req.method === 'GET' && parsedUrl.pathname === '/api/events') {
      const roomId = parsedUrl.searchParams.get('roomId');
      if (!roomId || !ROOM_ID_REGEX.test(roomId)) {
        res.writeHead(400, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ ok: false, error: 'Valid roomId is required' }));
        return;
      }

      const sinceParam = parsedUrl.searchParams.get('since');
      const since = sinceParam !== null ? (Number(sinceParam) || 0) : 0;
      const events = getEventsSince(roomId, since);

      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ ok: true, events }));
      return;
    }

    res.writeHead(404, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ ok: false, error: 'Not Found' }));
  });

  const wss = new WebSocketServer({ noServer: true });

  server.on('upgrade', (request, socket, head) => {
    const host = request.headers.host || 'localhost';
    const parsedUrl = new URL(request.url, `http://${host}`);

    if (parsedUrl.pathname !== '/ws') {
      socket.write('HTTP/1.1 404 Not Found\r\n\r\n');
      socket.destroy();
      return;
    }

    const roomId = parsedUrl.searchParams.get('roomId');
    if (!roomId || !ROOM_ID_REGEX.test(roomId)) {
      socket.write('HTTP/1.1 400 Bad Request\r\n\r\n');
      socket.destroy();
      return;
    }

    const role = parsedUrl.searchParams.get('role') || 'RECEPTOR';
    const sinceParam = parsedUrl.searchParams.get('since');
    const since = sinceParam !== null ? (Number(sinceParam) || 0) : null;

    wss.handleUpgrade(request, socket, head, ws => {
      wss.emit('connection', ws, request, { roomId, role, since });
    });
  });

  wss.on('connection', (ws, request, { roomId, role, since }) => {
    ws.roomId = roomId;
    ws.role = role;
    ws.isAlive = true;

    const room = getOrCreateRoom(roomId);
    room.clients.add(ws);

    ws.send(JSON.stringify({
      type: 'connected',
      roomId,
      peersOnline: room.clients.size
    }));

    if (since !== null || role === 'RECEPTOR') {
      const threshold = since !== null ? since : 0;
      const events = getEventsSince(roomId, threshold);
      ws.send(JSON.stringify({
        type: 'sync_batch',
        events
      }));
    }

    broadcastToRoom(roomId, {
      type: 'presence',
      peersOnline: room.clients.size
    }, ws);

    ws.on('pong', () => {
      ws.isAlive = true;
    });

    ws.on('message', data => {
      let message;
      try {
        message = JSON.parse(data.toString());
      } catch {
        return;
      }

      if (message.type === 'ping') {
        ws.send(JSON.stringify({ type: 'pong', ts: Date.now() }));
        return;
      }

      if (message.type === 'encrypted_event') {
        if (!isValidEnvelope(message) || message.roomId !== ws.roomId) {
          ws.send(JSON.stringify({ type: 'error', message: 'Invalid envelope or roomId mismatch' }));
          return;
        }

        const sanitized = cleanEnvelope(message);
        const { deduplicated } = addEventToRoom(ws.roomId, sanitized);
        if (!deduplicated) {
          broadcastToRoom(ws.roomId, sanitized, ws);
        }

        ws.send(JSON.stringify({
          type: 'ack',
          eventId: sanitized.eventId
        }));
      }
    });

    ws.on('close', () => {
      room.clients.delete(ws);
      if (room.clients.size > 0) {
        broadcastToRoom(roomId, {
          type: 'presence',
          peersOnline: room.clients.size
        });
      } else {
        cleanupEmptyRoom(roomId);
      }
    });

    ws.on('error', () => {
      ws.terminate();
    });
  });

  const heartbeat = setInterval(() => {
    for (const ws of wss.clients) {
      if (ws.isAlive === false) {
        ws.terminate();
        continue;
      }
      ws.isAlive = false;
      ws.ping();
    }
  }, 30000);
  heartbeat.unref();

  const originalClose = server.close.bind(server);
  server.close = function (cb) {
    clearInterval(heartbeat);
    for (const client of wss.clients) {
      client.terminate();
    }
    wss.close(() => {
      originalClose(cb);
    });
  };

  server.rooms = rooms;
  server.wss = wss;
  return server;
}

if (require.main === module) {
  const server = createServer();
  server.listen(PORT, () => {
    console.log(`Relay server running on port ${PORT}`);
  });
}

module.exports = { createServer, isValidEnvelope };

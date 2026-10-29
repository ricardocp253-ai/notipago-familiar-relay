const http = require('http');
const assert = require('assert');
const { WebSocket } = require('ws');
const { createServer } = require('./server');

function httpRequest(method, urlStr, payload = null) {
  return new Promise((resolve, reject) => {
    const url = new URL(urlStr);
    const options = {
      hostname: url.hostname,
      port: url.port,
      path: url.pathname + url.search,
      method,
      headers: {
        'Content-Type': 'application/json'
      }
    };

    const req = http.request(options, res => {
      let data = '';
      res.on('data', chunk => {
        data += chunk;
      });
      res.on('end', () => {
        try {
          const body = data ? JSON.parse(data) : null;
          resolve({ status: res.statusCode, body });
        } catch (err) {
          reject(new Error(`Failed to parse response JSON: ${data}`));
        }
      });
    });

    req.on('error', reject);
    if (payload) {
      req.write(JSON.stringify(payload));
    }
    req.end();
  });
}

function createMessageCollector(ws) {
  const queue = [];
  const waiters = [];

  ws.on('message', data => {
    try {
      const msg = JSON.parse(data.toString());
      const waiterIdx = waiters.findIndex(w => w.predicate(msg));
      if (waiterIdx !== -1) {
        const [waiter] = waiters.splice(waiterIdx, 1);
        clearTimeout(waiter.timer);
        waiter.resolve(msg);
      } else {
        queue.push(msg);
      }
    } catch (err) {
      console.error('Error parsing WS message:', err);
    }
  });

  return {
    waitFor(predicate, description = 'message', timeoutMs = 3000) {
      return new Promise((resolve, reject) => {
        const queueIdx = queue.findIndex(predicate);
        if (queueIdx !== -1) {
          const [msg] = queue.splice(queueIdx, 1);
          return resolve(msg);
        }

        const timer = setTimeout(() => {
          const idx = waiters.findIndex(w => w.resolve === resolve);
          if (idx !== -1) waiters.splice(idx, 1);
          reject(new Error(`Timeout waiting for ${description} after ${timeoutMs}ms`));
        }, timeoutMs);

        waiters.push({ predicate, resolve, timer });
      });
    },
    getQueued() {
      return [...queue];
    }
  };
}

async function runTests() {
  console.log('=== Iniciando Suite de Pruebas de Relay Server ===\n');

  const server = createServer();
  await new Promise((resolve, reject) => {
    server.listen(0, '127.0.0.1', resolve);
    server.on('error', reject);
  });

  const port = server.address().port;
  const baseUrl = `http://127.0.0.1:${port}`;
  const wsBaseUrl = `ws://127.0.0.1:${port}/ws`;
  const roomId = 'room_e2ee_test_room_12345678';

  console.log(`[1] Servidor levantado en puerto efímero: ${port}`);

  try {
    // 1. Health check inicial
    const health0 = await httpRequest('GET', `${baseUrl}/health`);
    assert.strictEqual(health0.status, 200);
    assert.strictEqual(health0.body.ok, true);
    assert.strictEqual(health0.body.activeRooms, 0);
    console.log('✓ GET /health responde OK con 0 salas activas');

    // 2. Conectar EMISOR
    const emisorWs = new WebSocket(`${wsBaseUrl}?roomId=${roomId}&role=EMISOR`);
    const emisorCollector = createMessageCollector(emisorWs);
    await new Promise(r => emisorWs.on('open', r));

    const emisorConnected = await emisorCollector.waitFor(
      m => m.type === 'connected',
      'EMISOR connected message'
    );
    assert.strictEqual(emisorConnected.roomId, roomId);
    assert.strictEqual(emisorConnected.peersOnline, 1);
    console.log('✓ EMISOR conectado exitosamente (peersOnline: 1)');

    // 3. Ping / Pong
    emisorWs.send(JSON.stringify({ type: 'ping' }));
    const pong = await emisorCollector.waitFor(m => m.type === 'pong', 'pong response');
    assert.strictEqual(typeof pong.ts, 'number');
    console.log('✓ Ping/Pong WebSocket verificado');

    // 4. Conectar RECEPTOR
    let receptorWs = new WebSocket(`${wsBaseUrl}?roomId=${roomId}&role=RECEPTOR`);
    let receptorCollector = createMessageCollector(receptorWs);
    await new Promise(r => receptorWs.on('open', r));

    const receptorConnected = await receptorCollector.waitFor(
      m => m.type === 'connected',
      'RECEPTOR connected'
    );
    assert.strictEqual(receptorConnected.peersOnline, 2);

    const receptorSyncEmpty = await receptorCollector.waitFor(
      m => m.type === 'sync_batch',
      'RECEPTOR initial sync_batch'
    );
    assert.strictEqual(Array.isArray(receptorSyncEmpty.events), true);
    assert.strictEqual(receptorSyncEmpty.events.length, 0);

    const emisorPresence2 = await emisorCollector.waitFor(
      m => m.type === 'presence' && m.peersOnline === 2,
      'EMISOR presence 2'
    );
    assert.strictEqual(emisorPresence2.peersOnline, 2);
    console.log('✓ RECEPTOR conectado (peersOnline: 2, sync_batch vacío recibido)');

    // 5. Broadcast en tiempo real (Event 1)
    const baseTime = Date.now();
    const event1 = {
      type: 'encrypted_event',
      roomId,
      eventId: 'evt-0001-uuid-notipago',
      timestamp: baseTime,
      iv: Buffer.from('iv1_123456789012').toString('base64'),
      ciphertext: Buffer.from('ciphertext1_encrypted_payment').toString('base64')
    };

    emisorWs.send(JSON.stringify(event1));

    const ack1 = await emisorCollector.waitFor(
      m => m.type === 'ack' && m.eventId === event1.eventId,
      'ack for event 1'
    );
    assert.strictEqual(ack1.eventId, event1.eventId);

    const recv1 = await receptorCollector.waitFor(
      m => m.type === 'encrypted_event' && m.eventId === event1.eventId,
      'real-time event 1 broadcast'
    );
    assert.deepStrictEqual(recv1, event1);
    console.log('✓ Broadcast en tiempo real verificado (ACK recibido en emisor, sobre recibido en receptor)');

    // 6. Deduplicación
    emisorWs.send(JSON.stringify(event1));
    const ackDup = await emisorCollector.waitFor(
      m => m.type === 'ack' && m.eventId === event1.eventId,
      'ack for duplicate event'
    );
    assert.strictEqual(ackDup.eventId, event1.eventId);

    await new Promise(r => setTimeout(r, 150));
    const dupCheck = receptorCollector.getQueued().filter(m => m.eventId === event1.eventId);
    assert.strictEqual(dupCheck.length, 0);
    console.log('✓ Deduplicación por eventId verificada (el receptor no recibió duplicado)');

    // 7. Desconexión de RECEPTOR
    receptorWs.close();
    const emisorPresence1 = await emisorCollector.waitFor(
      m => m.type === 'presence' && m.peersOnline === 1,
      'EMISOR presence after RECEPTOR disconnect'
    );
    assert.strictEqual(emisorPresence1.peersOnline, 1);
    console.log('✓ Desconexión de RECEPTOR detectada (notificación de presencia emitida)');

    // 8. Publicación offline por POST /api/events (Event 2 y Event 3)
    const event2 = {
      type: 'encrypted_event',
      roomId,
      eventId: 'evt-0002-uuid-notipago',
      timestamp: baseTime + 1000,
      iv: Buffer.from('iv2_123456789012').toString('base64'),
      ciphertext: Buffer.from('ciphertext2_encrypted_payment').toString('base64')
    };

    const postRes2 = await httpRequest('POST', `${baseUrl}/api/events`, event2);
    assert.strictEqual(postRes2.status, 201);
    assert.strictEqual(postRes2.body.ok, true);
    assert.strictEqual(postRes2.body.deduplicated, false);
    assert.strictEqual(postRes2.body.deliveredTo, 1); // EMISOR sigue conectado

    const event3 = {
      type: 'encrypted_event',
      roomId,
      eventId: 'evt-0003-uuid-notipago',
      timestamp: baseTime + 2000,
      iv: Buffer.from('iv3_123456789012').toString('base64'),
      ciphertext: Buffer.from('ciphertext3_encrypted_payment').toString('base64')
    };

    const postRes3 = await httpRequest('POST', `${baseUrl}/api/events`, event3);
    assert.strictEqual(postRes3.status, 201);
    assert.strictEqual(postRes3.body.ok, true);
    assert.strictEqual(postRes3.body.deduplicated, false);
    assert.strictEqual(postRes3.body.deliveredTo, 1);
    console.log('✓ POST /api/events almacenó eventos en buffer durante desconexión del receptor');

    // 9. Verificar GET /api/events con since
    const getRes = await httpRequest('GET', `${baseUrl}/api/events?roomId=${roomId}&since=${event1.timestamp}`);
    assert.strictEqual(getRes.status, 200);
    assert.strictEqual(getRes.body.ok, true);
    assert.strictEqual(getRes.body.events.length, 2);
    assert.strictEqual(getRes.body.events[0].eventId, event2.eventId);
    assert.strictEqual(getRes.body.events[1].eventId, event3.eventId);
    console.log('✓ GET /api/events?since=... devolvió correctamente los 2 eventos pendientes');

    // 10. Reconexión del RECEPTOR con since
    receptorWs = new WebSocket(`${wsBaseUrl}?roomId=${roomId}&role=RECEPTOR&since=${event1.timestamp}`);
    receptorCollector = createMessageCollector(receptorWs);
    await new Promise(r => receptorWs.on('open', r));

    const recConnected2 = await receptorCollector.waitFor(
      m => m.type === 'connected',
      'RECEPTOR reconnected'
    );
    assert.strictEqual(recConnected2.peersOnline, 2);

    const syncBatch = await receptorCollector.waitFor(
      m => m.type === 'sync_batch',
      'RECEPTOR sync_batch on reconnect'
    );
    assert.strictEqual(syncBatch.events.length, 2);
    assert.strictEqual(syncBatch.events[0].eventId, event2.eventId);
    assert.strictEqual(syncBatch.events[1].eventId, event3.eventId);
    console.log('✓ Reconexión con "since" entregó sync_batch con todos los sobres pendientes');

    // 11. Health check final
    const healthFinal = await httpRequest('GET', `${baseUrl}/health`);
    assert.strictEqual(healthFinal.status, 200);
    assert.strictEqual(healthFinal.body.ok, true);
    assert.strictEqual(healthFinal.body.activeRooms, 1);
    console.log('✓ Health check final verificado (1 sala activa)');

    // 12. Cierre limpio
    emisorWs.close();
    receptorWs.close();

    await new Promise(resolve => server.close(resolve));
    console.log('\n=== Todas las pruebas pasaron satisfactoriamente (100%) ===');
    process.exit(0);
  } catch (error) {
    console.error('\n❌ ERROR EN PRUEBAS:', error);
    try {
      server.close();
    } catch {}
    process.exit(1);
  }
}

runTests();

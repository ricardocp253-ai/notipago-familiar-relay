# NotiPago Familiar - Relay en Tiempo Real (Zero-Knowledge E2EE)

Microservicio ultraliviano en Node.js y WebSockets (`ws`) para el enrutamiento seguro de notificaciones de pago cifradas de extremo a extremo.

## Principios de Diseño
- **Zero-Knowledge**: El servidor nunca recibe datos en texto plano. Solo enruta sobres cifrados (`EncryptedEnvelope`).
- **Buffer en Memoria**: Conserva hasta 100 sobres por `roomId` durante 24 horas (TTL) con deduplicación por `eventId`.
- **Sin Dependencias Pesadas**: Cero bases de datos externas; ideal para ejecución en LAN o despliegue en capas gratuitas (Render, Railway, Fly.io).

---

## Contrato de Sobre Cifrado (`EncryptedEnvelope`)

```json
{
  "type": "encrypted_event",
  "roomId": "string (16-64 chars hex/alphanumeric)",
  "eventId": "string (UUID)",
  "timestamp": 1730226000000,
  "iv": "string (base64)",
  "ciphertext": "string (base64)"
}
```

---

## Endpoints

### HTTP REST
- `GET /health`: Estado del servicio, uptime y cantidad de salas activas.
- `POST /api/events`: Publica un `EncryptedEnvelope`. Retransmite en tiempo real a clientes WS conectados a la sala.
- `GET /api/events?roomId=<id>&since=<timestampMs>`: Recupera sobres pendientes emitidos con `timestamp > since`.

### WebSocket
- `ws://<host>:<port>/ws?roomId=<id>&role=<EMISOR|RECEPTOR>&since=<timestampMs>`
  - Eventos emitidos por el servidor: `connected`, `presence`, `sync_batch`, `ack`.
  - Mensajes entrantes soportados: `encrypted_event`, `ping`.

---

## Ejecución Local

### 1. Instalación y Ejecución
```bash
npm install
npm start
```
El servidor escucha en el puerto `8787` (o el indicado en `process.env.PORT`).

### 2. Pruebas Automatizadas
```bash
npm test
```

### 3. Docker Local
```bash
docker build -t notipago-relay-server .
docker run -p 8787:8787 notipago-relay-server
```

---

## Despliegue en la Nube

### Render (Web Service Gratuito)
1. Conecta el repositorio de GitHub en Render.
2. Selecciona **Web Service** -> Runtime: **Node**.
3. **Build Command**: `npm ci --omit=dev`
4. **Start Command**: `npm start`
5. Configura la variable de entorno: `PORT=8787` (o usa la asignada por Render).

### Railway
1. `railway init` o "New Project from GitHub Repo".
2. Railway detectará automáticamente el `Dockerfile` o `package.json`.
3. El puerto se mapea automáticamente con `PORT`.

### Fly.io
```bash
fly launch
fly deploy
```

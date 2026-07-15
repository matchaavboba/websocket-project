const express = require('express');
const http = require('http');
const WebSocket = require('ws');
const grpc = require('@grpc/grpc-js');
const protoLoader = require('@grpc/proto-loader');
const path = require('path');
const axios = require('axios');
const { getCacheEntries } = require('./store');

const app = express();
const server = http.createServer(app);
const wss = new WebSocket.Server({ server });

app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

app.get('/', (req, res) => {
  res.sendFile(path.join(__dirname, 'public/index.html'));
});

const PROTO_PATH = path.join(__dirname, '../proto/weather.proto');
const packageDef = protoLoader.loadSync(PROTO_PATH, {
  keepCase: true, longs: String, enums: String, defaults: true, oneofs: true,
});
const proto = grpc.loadPackageDefinition(packageDef).weather;
const GRPC_SERVER = process.env.GRPC_SERVER || 'localhost:50051';

const weatherClient  = new proto.WeatherService(GRPC_SERVER, grpc.credentials.createInsecure());
const alertClient    = new proto.AlertService(GRPC_SERVER, grpc.credentials.createInsecure());
const favoriteClient = new proto.FavoriteService(GRPC_SERVER, grpc.credentials.createInsecure());

// ── Broadcast ke semua WS client ──────────────────────────────────────────────
function broadcast(data) {
  const msg = JSON.stringify(data);
  wss.clients.forEach(client => {
    if (client.readyState === WebSocket.OPEN) {
      client.send(msg);
    }
  });
}

// ── Broadcast clients count ke semua ─────────────────────────────────────────
function broadcastClientCount() {
  broadcast({ type: 'status', clients: wss.clients.size });
}

// ── Map untuk menyimpan active watch calls per WebSocket client ───────────────
// Key: ws (object), Value: Map<city, grpcCall>
const watchSessions = new WeakMap();

// ── WebSocket connection handler ──────────────────────────────────────────────
wss.on('connection', (ws) => {
  console.log('[WS] Client connected. Total:', wss.clients.size);
  broadcastClientCount();

  // Inisialisasi watch session untuk client ini
  watchSessions.set(ws, new Map());

  // Kirim status awal ke client yang baru connect
  ws.send(JSON.stringify({
    type: 'status',
    connected: true,
    clients: wss.clients.size,
    message: 'Connected to SkyWeather WebSocket',
  }));

  ws.on('message', async (raw) => {
    let msg;
    try { msg = JSON.parse(raw); } catch { return; }

    console.log('[WS] Command received:', msg.type);

    switch (msg.type) {

      // ── Unary: Get single city weather ──────────────────────────────────────
      case 'cmd_get_weather': {
        const { city, user_id } = msg;
        ws.send(JSON.stringify({ type: 'stream_log', log: `→ gRPC GetWeather("${city}")`, style: 'send' }));
        weatherClient.GetWeather({ city, user_id: user_id || 'ws_user' }, (err, response) => {
          if (err) {
            ws.send(JSON.stringify({ type: 'stream_log', log: `✗ Error: ${err.message}`, style: 'err' }));
            ws.send(JSON.stringify({ type: 'error', message: err.message }));
            return;
          }
          ws.send(JSON.stringify({ type: 'stream_log', log: `← WeatherResponse: ${response.city} ${response.temperature}°C`, style: 'recv' }));
          ws.send(JSON.stringify({ type: 'weather_result', data: response }));
          broadcast({ type: 'activity_log', message: `🔍 ${response.city} searched — ${response.temperature}°C`, time: new Date().toISOString() });
        });
        break;
      }

      // ── Client Streaming: Multi-city dengan stream_progress realtime ─────────
      case 'cmd_stream_cities': {
        const { cities } = msg;
        if (!cities || !cities.length) break;

        ws.send(JSON.stringify({ type: 'stream_log', log: `// Client-side Streaming started (${cities.length} cities)`, style: 'muted' }));
        // FIX #1: stream_start kini dikirim DAN ada handler di frontend
        ws.send(JSON.stringify({ type: 'stream_start', total: cities.length }));

        // onProgress: dipanggil setiap kota berhasil di-fetch → update chart realtime
        const liveResults = [];
        const onProgress = (weather) => {
          liveResults.push(weather);
          ws.send(JSON.stringify({
            type: 'stream_progress',
            weather,
            received: liveResults.length,
            total: cities.length,
          }));
          // Broadcast chart update realtime ke semua client
          broadcast({ type: 'chart_update', data: [...liveResults] });
        };

        const call = weatherClient.GetMultiCityWeather((err, summary) => {
          if (err) {
            ws.send(JSON.stringify({ type: 'stream_log', log: `✗ Stream error: ${err.message}`, style: 'err' }));
            ws.send(JSON.stringify({ type: 'stream_end' }));
            return;
          }
          ws.send(JSON.stringify({ type: 'stream_log', log: `← MultiCitySummary: avg ${summary.avg_temperature}°C, ${summary.total_cities} cities`, style: 'recv' }));
          ws.send(JSON.stringify({ type: 'stream_summary', data: summary }));
          ws.send(JSON.stringify({ type: 'stream_end' }));
          broadcast({ type: 'chart_update', data: summary.results });
        }, onProgress);  // Teruskan onProgress ke gRPC handler

        let i = 0;
        const interval = setInterval(() => {
          if (i < cities.length) {
            const city = cities[i];
            ws.send(JSON.stringify({ type: 'stream_log', log: `→ write({ city: "${city}" })`, style: 'send' }));
            call.write({ city });
            i++;
          } else {
            clearInterval(interval);
            ws.send(JSON.stringify({ type: 'stream_log', log: `→ call.end()`, style: 'send' }));
            call.end();
          }
        }, 400);
        break;
      }

      // ── Unary: Check alert ───────────────────────────────────────────────────
      case 'cmd_check_alert': {
        const { city } = msg;
        ws.send(JSON.stringify({ type: 'stream_log', log: `→ gRPC CheckAlert("${city}")`, style: 'send' }));
        alertClient.CheckAlert({ city }, (err, response) => {
          if (err) {
            ws.send(JSON.stringify({ type: 'stream_log', log: `✗ Error: ${err.message}`, style: 'err' }));
            return;
          }
          ws.send(JSON.stringify({ type: 'stream_log', log: `← AlertResponse: ${response.alert_type} (${response.severity})`, style: 'recv' }));
          ws.send(JSON.stringify({ type: 'alert_result', data: response }));
        });
        break;
      }

      // ── Server Streaming: Watch 1 kota secara berkala ────────────────────────
      case 'cmd_watch_city': {
        const { city, user_id } = msg;
        const sessions = watchSessions.get(ws);

        // Stop watch sebelumnya untuk kota yang sama jika ada
        if (sessions.has(city)) {
          sessions.get(city).cancel();
          sessions.delete(city);
        }

        ws.send(JSON.stringify({ type: 'stream_log', log: `→ gRPC WatchCityWeather("${city}") started`, style: 'send' }));
        ws.send(JSON.stringify({ type: 'watch_start', city }));

        const watchCall = weatherClient.WatchCityWeather({ city, user_id: user_id || 'ws_user' });

        watchCall.on('data', (response) => {
          ws.send(JSON.stringify({ type: 'stream_log', log: `← Watch: ${response.city} ${response.temperature}°C`, style: 'recv' }));
          ws.send(JSON.stringify({ type: 'watch_data', city, data: response }));
        });

        watchCall.on('error', (err) => {
          ws.send(JSON.stringify({ type: 'stream_log', log: `✗ Watch error: ${err.message}`, style: 'err' }));
          ws.send(JSON.stringify({ type: 'watch_end', city }));
          sessions.delete(city);
        });

        watchCall.on('end', () => {
          ws.send(JSON.stringify({ type: 'watch_end', city }));
          sessions.delete(city);
        });

        sessions.set(city, watchCall);
        break;
      }

      // ── Stop server streaming watch ──────────────────────────────────────────
      case 'cmd_stop_watch': {
        const { city } = msg;
        const sessions = watchSessions.get(ws);
        if (sessions && sessions.has(city)) {
          sessions.get(city).cancel();
          sessions.delete(city);
          ws.send(JSON.stringify({ type: 'stream_log', log: `// Watch stopped for "${city}"`, style: 'muted' }));
          ws.send(JSON.stringify({ type: 'watch_end', city }));
        }
        break;
      }

      // ── Server-Initiated: Force alert check sekarang (tanpa nunggu 30 detik) ─
      case 'cmd_force_alert_check': {
        ws.send(JSON.stringify({ type: 'stream_log', log: `→ Force alert check triggered`, style: 'send' }));
        pushAutoAlerts();
        ws.send(JSON.stringify({ type: 'stream_log', log: `// Auto-alert check executed`, style: 'muted' }));
        break;
      }

      // ── Cache status: kirim isi cache aktif ke client ─────────────────────────
      case 'cmd_get_cache_status': {
        const entries = getCacheEntries();
        ws.send(JSON.stringify({ type: 'cache_status', entries }));
        ws.send(JSON.stringify({ type: 'stream_log', log: `← Cache status: ${entries.length} active entries`, style: 'recv' }));
        break;
      }

      // ── Favorites ────────────────────────────────────────────────────────────
      case 'cmd_add_favorite': {
        const { user_id, city } = msg;
        favoriteClient.AddFavorite({ user_id, city }, (err, response) => {
          if (err) { ws.send(JSON.stringify({ type: 'error', message: err.message })); return; }
          ws.send(JSON.stringify({ type: 'favorite_updated', data: response }));
        });
        break;
      }

      case 'cmd_remove_favorite': {
        const { user_id, city } = msg;
        favoriteClient.RemoveFavorite({ user_id, city }, (err, response) => {
          if (err) { ws.send(JSON.stringify({ type: 'error', message: err.message })); return; }
          ws.send(JSON.stringify({ type: 'favorite_updated', data: response }));
        });
        break;
      }

      case 'cmd_get_favorites': {
        const { user_id } = msg;
        favoriteClient.GetFavorites({ user_id }, (err, response) => {
          if (err) return;
          ws.send(JSON.stringify({ type: 'favorites_list', data: response }));
        });
        break;
      }

      case 'cmd_get_history': {
        const { user_id } = msg;
        favoriteClient.GetHistory({ user_id }, (err, response) => {
          if (err) return;
          ws.send(JSON.stringify({ type: 'history_list', data: response }));
        });
        break;
      }
    }
  });

  ws.on('close', () => {
    // Bersihkan semua watch session saat client disconnect
    const sessions = watchSessions.get(ws);
    if (sessions) {
      for (const [city, call] of sessions) {
        call.cancel();
        console.log(`[WS] Auto-cancelled watch for "${city}" (client disconnected)`);
      }
    }
    console.log('[WS] Client disconnected. Total:', wss.clients.size);
    broadcastClientCount();
  });
});

// ── Server-Initiated: Auto-alert setiap 30 detik ─────────────────────────────
const WATCHED_CITIES = ['Surabaya', 'Jakarta', 'Bali'];

function pushAutoAlerts() {
  for (const city of WATCHED_CITIES) {
    alertClient.CheckAlert({ city }, (err, response) => {
      if (err) return;
      if (response.has_alert) {
        broadcast({
          type: 'server_alert',
          city: response.city,
          alert_type: response.alert_type,
          severity: response.severity,
          message: response.message,
          time: new Date().toISOString(),
        });
        console.log(`[WS] Server pushed alert: ${response.city} — ${response.alert_type}`);
      } else {
        broadcast({
          type: 'server_ping',
          city: response.city,
          message: `✅ ${response.city}: cuaca normal`,
          time: new Date().toISOString(),
        });
      }
    });
  }
}

// Auto-alert interval: hanya broadcast jika ada client yang terhubung
setInterval(() => {
  if (wss.clients.size > 0) {
    console.log('[WS] Server pushing auto-alerts...');
    pushAutoAlerts();
  }
}, 30000);

// Push pertama kali setelah 5 detik server berjalan
setTimeout(pushAutoAlerts, 5000);

// ── HTTP REST API (fallback non-WS) ──────────────────────────────────────────
function mapGrpcStatus(code) {
  const map = { 3: 400, 5: 404, 7: 403, 14: 503, 16: 401 };
  return map[code] || 500;
}

app.get('/api/weather/:city', (req, res) => {
  const { city } = req.params;
  const userId = req.query.user_id || 'web_user';
  weatherClient.GetWeather({ city, user_id: userId }, (err, response) => {
    if (err) return res.status(mapGrpcStatus(err.code)).json({ error: err.message });
    res.json(response);
  });
});

// FIX #4: REST multi-city kini pakai delay agar semantik streaming benar
app.post('/api/weather/multi', (req, res) => {
  const { cities } = req.body;
  if (!cities || !Array.isArray(cities) || cities.length === 0) {
    return res.status(400).json({ error: 'cities array is required' });
  }
  const call = weatherClient.GetMultiCityWeather((err, summary) => {
    if (err) return res.status(mapGrpcStatus(err.code)).json({ error: err.message });
    res.json(summary);
  });
  // Kirim dengan delay kecil agar gRPC stream diproses satu per satu
  cities.forEach((city, i) => {
    setTimeout(() => {
      call.write({ city });
      if (i === cities.length - 1) call.end();
    }, i * 100);
  });
});

app.get('/api/alert/:city', (req, res) => {
  alertClient.CheckAlert({ city: req.params.city }, (err, response) => {
    if (err) return res.status(mapGrpcStatus(err.code)).json({ error: err.message });
    res.json(response);
  });
});

app.get('/api/favorites/:userId', (req, res) => {
  favoriteClient.GetFavorites({ user_id: req.params.userId }, (err, response) => {
    if (err) return res.status(mapGrpcStatus(err.code)).json({ error: err.message });
    res.json(response);
  });
});

app.post('/api/favorites', (req, res) => {
  const { user_id, city } = req.body;
  favoriteClient.AddFavorite({ user_id, city }, (err, response) => {
    if (err) return res.status(mapGrpcStatus(err.code)).json({ error: err.message });
    res.json(response);
  });
});

app.delete('/api/favorites', (req, res) => {
  const { user_id, city } = req.body;
  favoriteClient.RemoveFavorite({ user_id, city }, (err, response) => {
    if (err) return res.status(mapGrpcStatus(err.code)).json({ error: err.message });
    res.json(response);
  });
});

app.get('/api/history/:userId', (req, res) => {
  favoriteClient.GetHistory({ user_id: req.params.userId }, (err, response) => {
    if (err) return res.status(mapGrpcStatus(err.code)).json({ error: err.message });
    res.json(response);
  });
});

// ── Cache status REST endpoint ────────────────────────────────────────────────
app.get('/api/cache', (req, res) => {
  res.json({ entries: getCacheEntries() });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
  console.log(`╔══════════════════════════════════════════╗`);
  console.log(`║   🌐 SkyWeather Web + WebSocket Server   ║`);
  console.log(`║   http://localhost:${PORT}                ║`);
  console.log(`║   ws://localhost:${PORT}                  ║`);
  console.log(`╚══════════════════════════════════════════╝`);
});
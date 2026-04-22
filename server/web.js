const express = require('express');
const grpc = require('@grpc/grpc-js');
const protoLoader = require('@grpc/proto-loader');
const path = require('path');

const app = express();
app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

// Serve index.html for root path
app.get('/', (req, res) => {
  res.sendFile(path.join(__dirname, 'public/index.html'));
});

// Load proto
const PROTO_PATH = path.join(__dirname, '../proto/weather.proto');
const packageDef = protoLoader.loadSync(PROTO_PATH, {
  keepCase: true, longs: String, enums: String, defaults: true, oneofs: true,
});
const proto = grpc.loadPackageDefinition(packageDef).weather;
const SERVER = 'localhost:50051';

const weatherClient  = new proto.WeatherService(SERVER, grpc.credentials.createInsecure());
const alertClient    = new proto.AlertService(SERVER, grpc.credentials.createInsecure());
const favoriteClient = new proto.FavoriteService(SERVER, grpc.credentials.createInsecure());

app.get('/api/weather/:city', (req, res) => {
  const { city } = req.params;
  const userId = req.query.user_id || 'web_user';
  weatherClient.GetWeather({ city, user_id: userId }, (err, response) => {
    if (err) return res.status(mapGrpcStatus(err.code)).json({ error: err.message });
    res.json(response);
  });
});

app.post('/api/weather/multi', (req, res) => {
  const { cities } = req.body;
  if (!cities || !Array.isArray(cities) || cities.length === 0) {
    return res.status(400).json({ error: 'cities array is required' });
  }

  const call = weatherClient.GetMultiCityWeather((err, summary) => {
    if (err) return res.status(mapGrpcStatus(err.code)).json({ error: err.message });
    res.json(summary);
  });

  cities.forEach(city => call.write({ city }));
  call.end();
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

function mapGrpcStatus(code) {
  const map = { 3: 400, 5: 404, 7: 403, 14: 503, 16: 401 };
  return map[code] || 500;
}

const PORT = 3000;
app.listen(PORT, () => {
  console.log(`╔══════════════════════════════════════╗`);
  console.log(`║   🌐 Web Server Running               ║`);
  console.log(`║   http://localhost:${PORT}              ║`);
  console.log(`╚══════════════════════════════════════╝`);
});

# SkyWeather — gRPC + WebSocket Weather Monitoring System

Sistem monitoring cuaca real-time yang menggabungkan **gRPC** (untuk komunikasi backend-to-backend) dengan **WebSocket** (untuk push data ke browser). Backend gRPC mengambil data cuaca asli dari OpenWeatherMap, sementara web server bertindak sebagai jembatan (gRPC client + WebSocket server) yang meneruskan data itu secara live ke dashboard.

Dibuat untuk tugas mata kuliah **Integrasi Sistem**, sebagai implementasi 3 pola komunikasi gRPC (Unary, Client Streaming, Server Streaming) yang dikombinasikan dengan WebSocket untuk push notification real-time.

> ⚠️ **Sebelum menjalankan:** API key OpenWeatherMap sempat ter-*hardcode* di source code. Ganti dengan key baru dan simpan di `.env` — lihat bagian [Keamanan](#keamanan-penting) di bawah.

## Fitur

- **Unary RPC** — `GetWeather`: ambil cuaca 1 kota
- **Client Streaming RPC** — `GetMultiCityWeather`: kirim banyak kota satu per satu ke server, server balas 1 ringkasan (rata-rata suhu, kota terpanas/terdingin, kota ekstrem)
- **Server Streaming RPC** — `WatchCityWeather`: server push data cuaca kota tertentu tiap 15 detik selama client masih "watching"
- **Alert Engine** — deteksi kondisi ekstrem (badai, suhu >38°C atau <5°C, angin kencang, kelembapan >90%) berdasarkan threshold
- **Auto Alert Push** — server broadcast status cuaca 3 kota (Surabaya, Jakarta, Bali) tiap 30 detik ke semua client WebSocket yang terhubung, tanpa diminta
- **Favorites & History** — simpan kota favorit per user dan riwayat 10 pencarian terakhir (disimpan di memory, hilang saat server restart)
- **Caching** — hasil dari OpenWeatherMap di-cache 5 menit supaya hemat API call
- **REST API fallback** — endpoint HTTP biasa buat semua fitur di atas, kalau WebSocket tidak dipakai

## Tech Stack

- **Node.js** + **Express**
- **@grpc/grpc-js** & **@grpc/proto-loader** — server & client gRPC
- **ws** — WebSocket server
- **axios** — fetch data dari OpenWeatherMap API
- **Protocol Buffers** (`proto/weather.proto`) — skema gRPC

## Struktur Folder

```
websocket-project/
├── proto/
│   └── weather.proto          # Skema gRPC: WeatherService, AlertService, FavoriteService
├── server/
│   ├── index.js                # gRPC server (port 50051)
│   ├── web.js                  # Express + WebSocket bridge (port 3000), sekaligus gRPC client
│   ├── store.js                # In-memory store: cache, favorites, history
│   ├── services/
│   │   ├── weatherService.js   # Implementasi GetWeather, GetMultiCityWeather, WatchCityWeather
│   │   ├── alertService.js     # Implementasi CheckAlert + threshold logic
│   │   └── favoriteService.js  # Implementasi Add/Remove/Get Favorite & History
│   └── public/
│       └── index.html          # Dashboard frontend
├── client/
│   ├── client.js                # Demo gRPC client sekuensial (1 user)
│   └── multi-client.js          # Simulasi 3 user gRPC secara paralel
└── package.json
```

## Arsitektur

```
                    ┌─────────────────────┐
  Browser  ◄──WS───►│  server/web.js       │
  (index.html)      │  Express + WS        │───gRPC──►  server/index.js
                    │  (port 3000)          │  (localhost:50051)  gRPC Server
                    └─────────────────────┘                    │
                                                      ┌─────────┴─────────┐
                                                      │ WeatherService     │
  client/client.js ─────────────gRPC────────────────►│ AlertService       │──► OpenWeatherMap API
  client/multi-client.js ───────gRPC────────────────►│ FavoriteService    │
                                                      └────────────────────┘
```

`server/index.js` (gRPC server) dan `server/web.js` (web + WebSocket bridge) adalah **dua proses terpisah** yang harus jalan bersamaan — `web.js` terhubung ke gRPC server sebagai client di `localhost:50051`.

## Keamanan (Penting)

API key OpenWeatherMap saat ini ter-*hardcode* di `server/services/weatherService.js` dan `server/services/alertService.js`. Sebelum lanjut:

1. Generate API key baru di [openweathermap.org](https://openweathermap.org/api) (anggap key lama sudah bocor karena sempat ke-push ke repo publik)
2. Buat file `.env` di root project:
   ```
   OPENWEATHER_API_KEY=key_baru_kamu_disini
   ```
3. Ganti baris `const API_KEY = '...'` di kedua file service jadi:
   ```js
   const API_KEY = process.env.OPENWEATHER_API_KEY;
   ```
4. Install `dotenv` (`npm install dotenv`) dan load di paling atas `server/index.js` & `server/web.js`:
   ```js
   require('dotenv').config();
   ```
5. Tambahkan ke `.gitignore`:
   ```
   .env
   node_modules/
   ```

## Instalasi & Menjalankan

```bash
npm install
```

Buka **2 terminal**, jalankan gRPC server dulu, baru web server:

```bash
# Terminal 1 — gRPC server (wajib jalan duluan)
npm run server

# Terminal 2 — Web + WebSocket bridge
npm run web
```

Buka browser di `http://localhost:3000`.

Opsional, buat lihat demo gRPC murni tanpa browser:

```bash
npm run client   # demo 1 user, sekuensial
npm run multi    # simulasi 3 user paralel
```

## Deploy ke Railway

Karena ada 2 proses backend yang harus saling terhubung, ada 2 penyesuaian kecil sebelum deploy:

1. **Ganti `localhost:50051` jadi Railway private networking hostname.** Di `server/web.js`, baris:
   ```js
   const GRPC_SERVER = 'localhost:50051';
   ```
   ganti jadi env var, misal:
   ```js
   const GRPC_SERVER = process.env.GRPC_SERVER || 'localhost:50051';
   ```
   Nanti di Railway, dua service ini (grpc-server & web-server) ada dalam satu project bisa saling akses lewat internal hostname (`<nama-service>.railway.internal`), bukan `localhost`.

2. **Pakai `PORT` dari environment**, bukan hardcode. Di `server/web.js`:
   ```js
   const PORT = process.env.PORT || 3000;
   ```
   Railway assign port secara dinamis lewat env var `PORT`.

3. Bikin **2 service terpisah** dalam 1 Railway project:
   - Service 1: start command `node server/index.js` (gRPC server)
   - Service 2: start command `node server/web.js` (web + WS bridge), dengan env var `GRPC_SERVER` diarahkan ke hostname internal service 1
   - Tambahkan `OPENWEATHER_API_KEY` sebagai environment variable di service yang butuh (web-server, karena yang fetch cuaca)

Kalau kamu udah bikin perubahan kode di atas dan connect repo ini ke Railway, share aja — aku bantu cek konfigurasi service-nya.

## Catatan

- Favorites & history disimpan di memory (`store.js`), jadi akan hilang tiap kali server di-restart. Kalau butuh persisten, perlu ditambah database (SQLite/PostgreSQL) — di luar scope tugas ini kecuali diminta.
- `GRPC_SERVER` di `client/client.js` dan `client/multi-client.js` masih hardcode `localhost:50051` — cukup dibiarkan untuk demo lokal, tidak perlu ikut di-deploy.

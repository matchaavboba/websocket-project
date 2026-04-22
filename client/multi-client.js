const grpc = require('@grpc/grpc-js');
const protoLoader = require('@grpc/proto-loader');
const path = require('path');

const PROTO_PATH = path.join(__dirname, '../proto/weather.proto');
const packageDef = protoLoader.loadSync(PROTO_PATH, {
  keepCase: true, longs: String, enums: String, defaults: true, oneofs: true,
});
const proto = grpc.loadPackageDefinition(packageDef).weather;
const SERVER = 'localhost:50051';

const CLIENTS = [
  { userId: 'alice',   cities: ['Jakarta', 'Bali'],      favCity: 'Bali' },
  { userId: 'bob',     cities: ['Surabaya', 'Malang'],   favCity: 'Surabaya' },
  { userId: 'charlie', cities: ['Medan', 'Bandung'],     favCity: 'Bandung' },
];

async function simulateClient({ userId, cities, favCity }) {
  const weatherClient = new proto.WeatherService(SERVER, grpc.credentials.createInsecure());
  const alertClient   = new proto.AlertService(SERVER, grpc.credentials.createInsecure());
  const favClient     = new proto.FavoriteService(SERVER, grpc.credentials.createInsecure());

  console.log(`\n👤 [${userId}] Starting session...`);

  await new Promise((resolve, reject) => {
    const call = weatherClient.GetMultiCityWeather((err, summary) => {
      if (err) {
        console.error(`❌ [${userId}] Stream error: ${err.message}`);
        return reject(err);
      }
      console.log(`✅ [${userId}] Multi-city summary: avg ${summary.avg_temperature}°C across ${summary.total_cities} cities`);
      resolve(summary);
    });

    cities.forEach(city => call.write({ city }));
    call.end();
  });

  await new Promise((resolve) => {
    alertClient.CheckAlert({ city: favCity }, (err, res) => {
      if (err) { console.error(`❌ [${userId}] Alert error: ${err.message}`); return resolve(); }
      const status = res.has_alert ? `⚠️  ALERT: ${res.alert_type}` : '✅ Normal';
      console.log(`🔔 [${userId}] ${favCity}: ${status}`);
      resolve();
    });
  });

  await new Promise((resolve) => {
    favClient.AddFavorite({ user_id: userId, city: favCity }, (err, res) => {
      if (err) { console.error(`❌ [${userId}] Fav error: ${err.message}`); return resolve(); }
      console.log(`⭐ [${userId}] ${res.message}`);
      resolve();
    });
  });

  console.log(`🏁 [${userId}] Session complete.`);
}

async function main() {
  console.log('╔══════════════════════════════════════╗');
  console.log('║  👥 Multi-Client Simulation           ║');
  console.log(`║  ${CLIENTS.length} clients running concurrently      ║`);
  console.log('╚══════════════════════════════════════╝');

  await Promise.all(CLIENTS.map(simulateClient));

  console.log('\n\n🎉 All clients finished!');
}

main().catch(console.error);

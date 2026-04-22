const grpc = require('@grpc/grpc-js');
const protoLoader = require('@grpc/proto-loader');
const path = require('path');

const PROTO_PATH = path.join(__dirname, '../proto/weather.proto');
const packageDef = protoLoader.loadSync(PROTO_PATH, {
  keepCase: true,
  longs: String,
  enums: String,
  defaults: true,
  oneofs: true,
});
const proto = grpc.loadPackageDefinition(packageDef).weather;
const SERVER = 'localhost:50051';

const weatherClient = new proto.WeatherService(SERVER, grpc.credentials.createInsecure());
const alertClient = new proto.AlertService(SERVER, grpc.credentials.createInsecure());
const favoriteClient = new proto.FavoriteService(SERVER, grpc.credentials.createInsecure());

function log(label, data) {
  console.log(`\n${'─'.repeat(50)}`);
  console.log(`📌 ${label}`);
  console.log('─'.repeat(50));
  console.log(JSON.stringify(data, null, 2));
}

function delay(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

function getWeather(city, userId) {
  return new Promise((resolve, reject) => {
    weatherClient.GetWeather({ city, user_id: userId }, (err, response) => {
      if (err) {
        console.error(`❌ GetWeather error [${grpc.status[err.code]}]: ${err.message}`);
        return reject(err);
      }
      log(`GetWeather → ${city}`, response);
      resolve(response);
    });
  });
}

function getMultiCityWeather(cities) {
  return new Promise((resolve, reject) => {
    console.log(`\n${'─'.repeat(50)}`);
    console.log(`📌 GetMultiCityWeather (Client-side Streaming)`);
    console.log(`   Streaming ${cities.length} cities: ${cities.join(', ')}`);
    console.log('─'.repeat(50));

    const call = weatherClient.GetMultiCityWeather((err, summary) => {
      if (err) {
        console.error(`❌ GetMultiCityWeather error [${grpc.status[err.code]}]: ${err.message}`);
        return reject(err);
      }
      console.log('\n📊 Summary received from server:');
      console.log(`   Total Cities   : ${summary.total_cities}`);
      console.log(`   Avg Temperature: ${summary.avg_temperature}°C`);
      console.log(`   Hottest City   : ${summary.hottest_city}`);
      console.log(`   Coldest City   : ${summary.coldest_city}`);
      console.log(`   Extreme Cities : ${summary.extreme_cities.length > 0 ? summary.extreme_cities.join(', ') : 'None'}`);
      resolve(summary);
    });

    let i = 0;
    const interval = setInterval(() => {
      if (i < cities.length) {
        const city = cities[i];
        console.log(`   → Sending: "${city}"`);
        call.write({ city });
        i++;
      } else {
        clearInterval(interval);
        call.end(); // Signal end of stream
      }
    }, 300);
  });
}

function checkAlert(city) {
  return new Promise((resolve, reject) => {
    alertClient.CheckAlert({ city }, (err, response) => {
      if (err) {
        console.error(`❌ CheckAlert error [${grpc.status[err.code]}]: ${err.message}`);
        return reject(err);
      }
      log(`CheckAlert → ${city}`, response);
      resolve(response);
    });
  });
}

function addFavorite(userId, city) {
  return new Promise((resolve, reject) => {
    favoriteClient.AddFavorite({ user_id: userId, city }, (err, response) => {
      if (err) {
        console.error(`❌ AddFavorite error: ${err.message}`);
        return reject(err);
      }
      log(`AddFavorite → ${userId}: ${city}`, response);
      resolve(response);
    });
  });
}

function getFavorites(userId) {
  return new Promise((resolve, reject) => {
    favoriteClient.GetFavorites({ user_id: userId }, (err, response) => {
      if (err) {
        console.error(`❌ GetFavorites error: ${err.message}`);
        return reject(err);
      }
      log(`GetFavorites → ${userId}`, response);
      resolve(response);
    });
  });
}

function removeFavorite(userId, city) {
  return new Promise((resolve, reject) => {
    favoriteClient.RemoveFavorite({ user_id: userId, city }, (err, response) => {
      if (err) {
        console.error(`❌ RemoveFavorite error [${grpc.status[err.code]}]: ${err.message}`);
        return reject(err);
      }
      log(`RemoveFavorite → ${userId}: ${city}`, response);
      resolve(response);
    });
  });
}

function getHistory(userId) {
  return new Promise((resolve, reject) => {
    favoriteClient.GetHistory({ user_id: userId }, (err, response) => {
      if (err) {
        console.error(`❌ GetHistory error: ${err.message}`);
        return reject(err);
      }
      console.log(`\n${'─'.repeat(50)}`);
      console.log(`📌 GetHistory → ${userId}`);
      console.log('─'.repeat(50));
      console.log(`   ${response.records.length} record(s) found`);
      response.records.forEach((r, i) => {
        console.log(`   [${i + 1}] ${r.city}, ${r.country} — ${r.temperature}°C — ${r.timestamp}`);
      });
      resolve(response);
    });
  });
}

function testErrorHandling() {
  return new Promise((resolve) => {
    console.log(`\n${'─'.repeat(50)}`);
    console.log('📌 Error Handling Demo');
    console.log('─'.repeat(50));

    weatherClient.GetWeather({ city: 'CityThatDoesNotExist12345', user_id: '' }, (err) => {
      if (err) {
        console.log(`   ✅ NOT_FOUND caught: [${grpc.status[err.code]}] ${err.message}`);
      }
    });

    weatherClient.GetWeather({ city: '', user_id: '' }, (err) => {
      if (err) {
        console.log(`   ✅ INVALID_ARGUMENT caught: [${grpc.status[err.code]}] ${err.message}`);
      }
    });

    setTimeout(resolve, 2000);
  });
}

async function main() {
  const USER = 'student_001';

  console.log('╔══════════════════════════════════════╗');
  console.log('║  🌤️  Weather gRPC Client Demo         ║');
  console.log(`║  👤 User: ${USER}               ║`);
  console.log('╚══════════════════════════════════════╝');

  try {
    await getWeather('Surabaya', USER);
    await delay(500);

    await getMultiCityWeather(['Jakarta', 'Bali', 'Yogyakarta', 'Medan', 'Makassar']);
    await delay(500);

    await checkAlert('Surabaya');
    await delay(500);

    await addFavorite(USER, 'Surabaya');
    await addFavorite(USER, 'Bali');
    await addFavorite(USER, 'Surabaya'); // duplicate test
    await getFavorites(USER);
    await removeFavorite(USER, 'Bali');
    await getFavorites(USER);
    await delay(500);

    await getWeather('Jakarta', USER);
    await getWeather('Malang', USER);
    await getHistory(USER);

    await testErrorHandling();

    console.log('\n✅ Demo complete!');
  } catch (err) {
    console.error('\n💥 Unexpected error:', err.message);
  }
}

main();

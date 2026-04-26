const grpc = require('@grpc/grpc-js');
const axios = require('axios');
const { getCached, setCache, addHistory } = require('../store');

const API_KEY = '3f1706c440a235728ed23c82900803f9';
const BASE_URL = 'https://api.openweathermap.org/data/2.5/weather';

async function fetchWeatherFromAPI(city) {
  const url = `${BASE_URL}?q=${encodeURIComponent(city)}&appid=${API_KEY}&units=metric`;
  const res = await axios.get(url);
  const d = res.data;

  const windSpeed = d.wind?.speed || 0;
  const isExtreme =
    windSpeed > 15 ||
    d.main.temp > 38 ||
    d.main.temp < 0 ||
    ['Thunderstorm', 'Tornado', 'Hurricane'].includes(d.weather[0]?.main);

  return {
    city: d.name,
    country: d.sys.country,
    temperature: d.main.temp,
    feels_like: d.main.feels_like,
    humidity: d.main.humidity,
    wind_speed: windSpeed,
    description: d.weather[0]?.description || '',
    icon: d.weather[0]?.icon || '',
    is_extreme: isExtreme,
    timestamp: new Date().toISOString(),
  };
}

// ── Unary RPC: GetWeather ─────────────────────────────────────────────────────
async function GetWeather(call, callback) {
  const { city, user_id } = call.request;

  if (!city || city.trim() === '') {
    return callback({
      code: grpc.status.INVALID_ARGUMENT,
      message: 'City name cannot be empty',
    });
  }

  const cached = getCached(city);
  if (cached) {
    console.log(`[WeatherService] Cache hit for "${city}"`);
    if (user_id) addHistory(user_id, cached);
    return callback(null, cached);
  }

  try {
    console.log(`[WeatherService] Fetching weather for "${city}"...`);
    const weather = await fetchWeatherFromAPI(city);
    setCache(city, weather);
    if (user_id) addHistory(user_id, weather);
    callback(null, weather);
  } catch (err) {
    if (err.response?.status === 404) {
      return callback({
        code: grpc.status.NOT_FOUND,
        message: `City "${city}" not found. Check the spelling and try again.`,
      });
    }
    if (err.response?.status === 401) {
      return callback({
        code: grpc.status.UNAUTHENTICATED,
        message: 'Invalid OpenWeatherMap API key.',
      });
    }
    console.error('[WeatherService] Error:', err.message);
    return callback({
      code: grpc.status.UNAVAILABLE,
      message: 'Weather API is currently unavailable. Please try again later.',
    });
  }
}

// ── Client Streaming RPC: GetMultiCityWeather ─────────────────────────────────
// onProgress(weather) dipanggil setiap kali satu kota berhasil di-fetch
// sehingga web.js bisa push stream_progress ke WebSocket client secara realtime
function GetMultiCityWeather(call, callback, onProgress) {
  const results = [];
  const errors = [];

  call.on('data', async (req) => {
    const { city } = req;
    if (!city || city.trim() === '') {
      errors.push('Empty city name skipped');
      return;
    }

    try {
      let weather = getCached(city);
      if (!weather) {
        weather = await fetchWeatherFromAPI(city);
        setCache(city, weather);
      }
      results.push(weather);
      console.log(`[WeatherService][Stream] Received "${city}" → ${weather.temperature}°C`);

      // Push progress ke layer WebSocket jika callback disediakan
      if (typeof onProgress === 'function') {
        onProgress(weather);
      }
    } catch (err) {
      if (err.response?.status === 404) {
        errors.push(`"${city}" not found`);
      } else {
        errors.push(`"${city}" fetch failed`);
      }
      console.warn(`[WeatherService][Stream] Skipped "${city}": ${err.message}`);
    }
  });

  call.on('end', () => {
    if (results.length === 0) {
      return callback({
        code: grpc.status.NOT_FOUND,
        message: `No valid cities found. Errors: ${errors.join(', ')}`,
      });
    }

    const temps = results.map(r => r.temperature);
    const avgTemp = temps.reduce((a, b) => a + b, 0) / temps.length;
    const hottestCity = results.reduce((a, b) => (a.temperature > b.temperature ? a : b)).city;
    const coldestCity = results.reduce((a, b) => (a.temperature < b.temperature ? a : b)).city;
    const extremeCities = results.filter(r => r.is_extreme).map(r => r.city);

    const summary = {
      results,
      total_cities: results.length,
      avg_temperature: Math.round(avgTemp * 10) / 10,
      hottest_city: hottestCity,
      coldest_city: coldestCity,
      extreme_cities: extremeCities,
    };

    console.log(`[WeatherService][Stream] Summary ready: ${results.length} cities processed`);
    callback(null, summary);
  });

  call.on('error', (err) => {
    console.error('[WeatherService][Stream] Stream error:', err.message);
  });
}

// ── Server Streaming RPC: WatchCityWeather ────────────────────────────────────
// Server push data cuaca 1 kota setiap WATCH_INTERVAL_MS detik
// Browser kirim cmd_stop_watch → web.js panggil call.cancel()
const WATCH_INTERVAL_MS = 15000; // 15 detik

function WatchCityWeather(call) {
  const { city, user_id } = call.request;
  console.log(`[WeatherService][Watch] Started watching "${city}"`);

  async function pushLatest() {
    try {
      // Selalu fetch fresh untuk watch mode, supaya data selalu terkini
      const weather = await fetchWeatherFromAPI(city);
      setCache(city, weather);
      if (user_id) addHistory(user_id, weather);
      call.write(weather);
      console.log(`[WeatherService][Watch] Pushed "${city}" → ${weather.temperature}°C`);
    } catch (err) {
      console.error(`[WeatherService][Watch] Error for "${city}":`, err.message);
      // Jangan end stream saat error — coba lagi di interval berikutnya
    }
  }

  // Push pertama kali langsung tanpa menunggu interval
  pushLatest();

  const interval = setInterval(pushLatest, WATCH_INTERVAL_MS);

  call.on('cancelled', () => {
    clearInterval(interval);
    console.log(`[WeatherService][Watch] Watch for "${city}" stopped`);
  });

  call.on('error', () => {
    clearInterval(interval);
  });
}

module.exports = { GetWeather, GetMultiCityWeather, WatchCityWeather };

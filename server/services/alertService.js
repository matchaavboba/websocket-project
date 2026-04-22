const grpc = require('@grpc/grpc-js');
const axios = require('axios');
const { getCached, setCache } = require('../store');

const API_KEY = '3f1706c440a235728ed23c82900803f9';
const BASE_URL = 'https://api.openweathermap.org/data/2.5/weather';

const THRESHOLDS = {
  HIGH_TEMP: 38,      // °C
  LOW_TEMP: 5,        // °C
  HIGH_WIND: 15,      // m/s (~54 km/h)
  HIGH_HUMIDITY: 90,  // %
};

function detectAlert(weather) {
  const { temperature, wind_speed, humidity, description } = weather;

  if (['thunderstorm', 'tornado', 'hurricane'].some(k => description.toLowerCase().includes(k))) {
    return { has_alert: true, alert_type: 'STORM', severity: 'CRITICAL', message: `⛈️ Severe storm detected in ${weather.city}! Stay indoors.` };
  }
  if (temperature >= THRESHOLDS.HIGH_TEMP) {
    return { has_alert: true, alert_type: 'HEAT', severity: 'HIGH', message: `🌡️ Extreme heat in ${weather.city} (${temperature}°C). Stay hydrated!` };
  }
  if (temperature <= THRESHOLDS.LOW_TEMP) {
    return { has_alert: true, alert_type: 'COLD', severity: 'HIGH', message: `🥶 Very cold in ${weather.city} (${temperature}°C). Dress warmly!` };
  }
  if (wind_speed >= THRESHOLDS.HIGH_WIND) {
    return { has_alert: true, alert_type: 'WIND', severity: 'MEDIUM', message: `💨 Strong winds in ${weather.city} (${wind_speed} m/s). Secure loose objects.` };
  }
  if (humidity >= THRESHOLDS.HIGH_HUMIDITY) {
    return { has_alert: true, alert_type: 'HUMIDITY', severity: 'LOW', message: `💧 Very high humidity in ${weather.city} (${humidity}%). May feel uncomfortable.` };
  }

  return { has_alert: false, alert_type: 'NONE', severity: 'NONE', message: `✅ Weather conditions in ${weather.city} are normal.` };
}

async function CheckAlert(call, callback) {
  const { city } = call.request;

  if (!city || city.trim() === '') {
    return callback({
      code: grpc.status.INVALID_ARGUMENT,
      message: 'City name cannot be empty',
    });
  }

  try {
    let weather = getCached(city);

    if (!weather) {
      console.log(`[AlertService] Fetching data for "${city}"...`);
      const url = `${BASE_URL}?q=${encodeURIComponent(city)}&appid=${API_KEY}&units=metric`;
      const res = await axios.get(url);
      const d = res.data;

      weather = {
        city: d.name,
        country: d.sys.country,
        temperature: d.main.temp,
        feels_like: d.main.feels_like,
        humidity: d.main.humidity,
        wind_speed: d.wind?.speed || 0,
        description: d.weather[0]?.description || '',
        icon: d.weather[0]?.icon || '',
        is_extreme: false,
        timestamp: new Date().toISOString(),
      };
      setCache(city, weather);
    }

    const alertInfo = detectAlert(weather);
    console.log(`[AlertService] Alert for "${city}": ${alertInfo.alert_type} (${alertInfo.severity})`);

    callback(null, {
      city: weather.city,
      ...alertInfo,
    });
  } catch (err) {
    if (err.response?.status === 404) {
      return callback({
        code: grpc.status.NOT_FOUND,
        message: `City "${city}" not found.`,
      });
    }
    console.error('[AlertService] Error:', err.message);
    return callback({
      code: grpc.status.UNAVAILABLE,
      message: 'Alert service temporarily unavailable.',
    });
  }
}

module.exports = { CheckAlert };

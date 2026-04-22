const store = {
  favorites: {},

  history: {},

  cache: {},

  CACHE_TTL_MS: 5 * 60 * 1000,
};

function getFavorites(userId) {
  return store.favorites[userId] || [];
}

function addFavorite(userId, city) {
  if (!store.favorites[userId]) store.favorites[userId] = [];
  const normalized = city.trim().toLowerCase();
  const already = store.favorites[userId].map(c => c.toLowerCase());
  if (already.includes(normalized)) return false; // already exists
  store.favorites[userId].push(city.trim());
  return true;
}

function removeFavorite(userId, city) {
  if (!store.favorites[userId]) return false;
  const before = store.favorites[userId].length;
  store.favorites[userId] = store.favorites[userId].filter(
    c => c.toLowerCase() !== city.trim().toLowerCase()
  );
  return store.favorites[userId].length < before;
}

function getHistory(userId) {
  return store.history[userId] || [];
}

function addHistory(userId, weatherRecord) {
  if (!store.history[userId]) store.history[userId] = [];
  store.history[userId].unshift(weatherRecord); // newest first
  if (store.history[userId].length > 10) {
    store.history[userId] = store.history[userId].slice(0, 10);
  }
}

function getCached(city) {
  const entry = store.cache[city.toLowerCase()];
  if (!entry) return null;
  if (Date.now() > entry.expiresAt) {
    delete store.cache[city.toLowerCase()];
    return null;
  }
  return entry.data;
}

function setCache(city, data) {
  store.cache[city.toLowerCase()] = {
    data,
    expiresAt: Date.now() + store.CACHE_TTL_MS,
  };
}

module.exports = {
  getFavorites,
  addFavorite,
  removeFavorite,
  getHistory,
  addHistory,
  getCached,
  setCache,
};

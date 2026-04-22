const grpc = require('@grpc/grpc-js');
const {
  getFavorites,
  addFavorite,
  removeFavorite,
  getHistory,
} = require('../store');

function AddFavorite(call, callback) {
  const { user_id, city } = call.request;

  if (!user_id || user_id.trim() === '') {
    return callback({ code: grpc.status.INVALID_ARGUMENT, message: 'user_id is required' });
  }
  if (!city || city.trim() === '') {
    return callback({ code: grpc.status.INVALID_ARGUMENT, message: 'city is required' });
  }

  const added = addFavorite(user_id, city);
  const cities = getFavorites(user_id);

  if (!added) {
    return callback(null, {
      success: false,
      message: `"${city}" is already in your favorites.`,
      cities,
    });
  }

  console.log(`[FavoriteService] User "${user_id}" added "${city}" to favorites`);
  callback(null, {
    success: true,
    message: `"${city}" added to favorites! You now have ${cities.length} favorite(s).`,
    cities,
  });
}

function RemoveFavorite(call, callback) {
  const { user_id, city } = call.request;

  if (!user_id || user_id.trim() === '') {
    return callback({ code: grpc.status.INVALID_ARGUMENT, message: 'user_id is required' });
  }
  if (!city || city.trim() === '') {
    return callback({ code: grpc.status.INVALID_ARGUMENT, message: 'city is required' });
  }

  const removed = removeFavorite(user_id, city);
  const cities = getFavorites(user_id);

  if (!removed) {
    return callback({
      code: grpc.status.NOT_FOUND,
      message: `"${city}" was not found in your favorites.`,
    });
  }

  console.log(`[FavoriteService] User "${user_id}" removed "${city}" from favorites`);
  callback(null, {
    success: true,
    message: `"${city}" removed from favorites.`,
    cities,
  });
}

function GetFavorites(call, callback) {
  const { user_id } = call.request;

  if (!user_id || user_id.trim() === '') {
    return callback({ code: grpc.status.INVALID_ARGUMENT, message: 'user_id is required' });
  }

  const cities = getFavorites(user_id);
  console.log(`[FavoriteService] User "${user_id}" has ${cities.length} favorite(s)`);

  callback(null, { user_id, cities });
}

function GetHistory(call, callback) {
  const { user_id } = call.request;

  if (!user_id || user_id.trim() === '') {
    return callback({ code: grpc.status.INVALID_ARGUMENT, message: 'user_id is required' });
  }

  const records = getHistory(user_id);
  console.log(`[FavoriteService] History for "${user_id}": ${records.length} record(s)`);

  callback(null, { user_id, records });
}

module.exports = { AddFavorite, RemoveFavorite, GetFavorites, GetHistory };

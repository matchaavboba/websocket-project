const grpc = require('@grpc/grpc-js');
const protoLoader = require('@grpc/proto-loader');
const path = require('path');

const weatherHandler = require('./services/weatherService');
const alertHandler = require('./services/alertService');
const favoriteHandler = require('./services/favoriteService');

const PROTO_PATH = path.join(__dirname, '../proto/weather.proto');
const packageDef = protoLoader.loadSync(PROTO_PATH, {
  keepCase: true,
  longs: String,
  enums: String,
  defaults: true,
  oneofs: true,
});
const proto = grpc.loadPackageDefinition(packageDef).weather;

function main() {
  const server = new grpc.Server();

  server.addService(proto.WeatherService.service, {
    GetWeather: weatherHandler.GetWeather,
    GetMultiCityWeather: weatherHandler.GetMultiCityWeather,
    WatchCityWeather: weatherHandler.WatchCityWeather,
  });

  server.addService(proto.AlertService.service, {
    CheckAlert: alertHandler.CheckAlert,
  });

  server.addService(proto.FavoriteService.service, {
    AddFavorite: favoriteHandler.AddFavorite,
    RemoveFavorite: favoriteHandler.RemoveFavorite,
    GetFavorites: favoriteHandler.GetFavorites,
    GetHistory: favoriteHandler.GetHistory,
  });

  const address = '0.0.0.0:50051';
  server.bindAsync(address, grpc.ServerCredentials.createInsecure(), (err, port) => {
    if (err) {
      console.error('❌ Server failed to start:', err.message);
      return;
    }
    console.log('╔══════════════════════════════════════╗');
    console.log('║   🌤️  Weather gRPC Server Running     ║');
    console.log(`║   📡 Listening on port ${port}          ║`);
    console.log('╠══════════════════════════════════════╣');
    console.log('║  Services:                           ║');
    console.log('║  ✅ WeatherService  (Unary + Stream) ║');
    console.log('║  ✅ AlertService   (Unary)           ║');
    console.log('║  ✅ FavoriteService (Unary)          ║');
    console.log('╚══════════════════════════════════════╝');
  });
}

main();

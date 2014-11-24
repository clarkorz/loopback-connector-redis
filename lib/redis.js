/**
 * Module dependencies
 */
var RedisClient = require('./redis-client');
var RedisCRUD   = require('./redis-crud');


/**
 *
 * Initialize the Redis connector against the given data source
 *
 * @param {DataSource} dataSource The loopback-datasource-juggler dataSource
 * @param {function} [callback] The callback function
 */
exports.initialize = function initializeSchema(dataSource, callback) {
  if (!require('redis')) return;

  var settings = dataSource.settings || {}; // The settings is passed in from the dataSource

  if (settings.url) {
    var redisUrl = require('url').parse(settings.url);
    var redisAuth = (redisUrl.auth || '').split(':');
    settings.host = redisUrl.hostname;
    settings.port = redisUrl.port;

    if (redisAuth.length > 1) {
      settings.database = redisAuth[0];
      settings.password = redisAuth.slice(1).join(':');
    }
  }

  if (settings.host || settings.port) {
    settings.host = settings.host || '127.0.0.1';
    settings.port = settings.port || 6379;
  }

  if (settings.password) {
    settings.options.auth_pass = settings.password;
  }

  if (settings.type && !~['client', 'crud'].indexOf(settings.type)) {
    throw new Error('connector type `' + settings.type + '` must be one of [client, curd].');
  }
  var connector = new RedisClient(settings, dataSource); // Construct the connector instance


  switch (settings.type) {
    case 'client':
      break;
    case 'crud':
    default: // defaults to use Redis CRUD connector
      settings.type = 'crud';
      connector = new RedisCRUD(connector); // Construct the connector instance
      break;
  }

  dataSource.connector = connector; // Attach connector to dataSource
  connector.dataSource = dataSource; // Hold a reference to dataSource

  if (callback) {
    dataSource.connector.connect(callback);
  }
};

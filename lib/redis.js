/**
 * Module dependencies
 */
var redis = require('redis');
var util  = require('util');
var Connector = require('loopback-connector').Connector;
var debug = require('debug')('loopback:connector:redis:client');


/**
 * The constructor for Redis connector
 * @param {Object} settings The settings object
 * @param {DataSource} dataSource The data source instance
 * @constructor
 */
function Redis(settings, dataSource) {
  Connector.call(this, 'redis', settings);

  this.client = null;

  this.debug = settings.debug || debug.enabled;

  if (this.debug) {
    debug('Settings: %j', settings);
  }

  this.dataSource = dataSource;
}

// Inherit from loopback-datasource-juggler Base Connector
util.inherits(Redis, Connector);

var commands = Object.keys(redis.Multi.prototype).filter(function (n) {
    return n.match(/^[a-z]/);
});

commands.forEach(function (cmd) {

  Redis.prototype[cmd] = function (args, callback) {

    var client = this.client, log;

    if (typeof args === 'string') {
      args = [args];
    }

    if (typeof args === 'function') {
      callback = args;
      args = [];
    }

    if (!args) args = [];

    log = this.dataSource.connector.logger(cmd.toUpperCase() + ' ' + args.map(function (a) {
      if (typeof a === 'object') return JSON.stringify(a);
      return a;
    }).join(' '));

    args.push(function (err, res) {
      if (err) console.log(err);
      log();
      callback && callback(err, res);
    });

    client[cmd].apply(client, args);
  };
});


/**
 * Connect to Redis
 * @param {Function} [callback] The callback function
 *
 * @callback callback
 * @param {Error} err The error object
 * @param {RedisClient} client The RedisClient object
 */
Redis.prototype.connect = function (callback) {
  var self = this, settings = self.settings;

  if (this.client) {
    if (callback) {
      process.nextTick(function () {
        callback && callback(null, self.client);
      });
    }
    return;
  }

  var args = [];
  if (settings.unix_socket) {
    args.push(settings.unix_socket);
  } else if (settings.host || settings.port) {
    args.push(settings.port);
    args.push(settings.host);
  }
  args.push(settings.options || {});

  var client = redis.createClient.apply(null, args);

  var initialized = false;

  // listen to all connections
  client.on('connect', function () {
    if (self.debug) {
      debug('Redis connection is established: ' + settings.host + ':' + settings.port);
    }

    if (!initialized) {
      // connect duration initializing
      if (settings.database) {
        client.select(settings.database, function (err) {
          if (err) return; // select command will emit error event

          if (self.debug) {
            debug('Redis connection is selected: ' + settings.database);
          }

          initialized = true;
          self.client = client;
          callback && callback(null, self.client);
        });
      } else {
        initialized = true;
        self.client = client;
        callback && callback(null, self.client);
      }
    }
  });

  // listen to all errors
  client.on('error', function (err) {
    if(!initialized) {
      // error duration initializing
      if (self.debug) {
        console.error('Redis connection is failed: ' + settings.host + ':' + settings.port, err);
      }
      initialized = true;
      callback && callback(err);
    } else {
      // errors after initialized
      if (self.debug) {
        console.error('Redis is on error: ', err);
      }
    }
  });
};

Redis.prototype.multi = function (commands, callback) {
  if (commands.length === 0) return callback && callback();
  if (commands.length === 1) {
    return this[commands[0].shift().toLowerCase()].call(
      this,
      commands[0],
      callback && function (err, reply) {
        callback(err, reply ? [reply] : [])
      });
  }

  var log = this.dataSource.connector.logger('MULTI\n  ' + commands.map(function (x) {
    return x.join(' ');
  }).join('\n  ') + '\nEXEC');

  this.client.multi(commands).exec(function (err, replies) {
    if (err) console.log(err);
    log();
    callback && callback(err, replies);
  });
};

Redis.prototype.transaction = function () {
    return new Transaction(this);
};


/**
 *
 * @param client
 * @constructor
 */
function Transaction(client) {
  this._client = client;
  this._handlers = [];
  this._schedule = [];
}

Transaction.prototype.run = function (cb) {
    var t = this;
    var atLeastOneHandler = false;
    switch (this._schedule.length) {
        case 0: return cb();
        case 1: return this._client[this._schedule[0].shift()].call(
        this._client,
        this._schedule[0],
        this._handlers[0] || cb);
        default:
        this._client.multi(this._schedule, function (err, replies) {
            if (err) return cb(err);
            replies.forEach(function (r, i) {
                if (t._handlers[i]) {
                    atLeastOneHandler = true;
                    t._handlers[i](err, r);
                }
            });
            if (!atLeastOneHandler) cb(err);
        });
    }

};

commands.forEach(function (k) {

    Transaction.prototype[k] = function (args, cb) {
        if (typeof args === 'string') {
            args = [args];
        }
        args.unshift(k);
        this._schedule.push(args);
        this._handlers.push(cb || false);
    };

});


module.exports = Redis;

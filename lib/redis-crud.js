/**
 * Module dependencies
 */
var util  = require('util');
var async = require('async');
var Connector = require('loopback-connector').Connector;
var debug = require('debug')('loopback:connector:redis:crud');

/**
 *
 * @param client
 * @constructor
 */
function RedisCRUD(client) {
  this._models = {};
  this.client = client;
  this.indexes = {};
  this.name = 'redis';
}

// Inherit from loopback-datasource-juggler Base Connector
util.inherits(RedisCRUD, Connector);

RedisCRUD.prototype.connect = function (callback) {
  this.client.connect(callback);
};

RedisCRUD.prototype.define = function (descr) {
  var m = descr.model.modelName;
  this._models[m] = descr;
  this.indexes[m] = {};
  Object.keys(descr.properties).forEach(function (prop) {
    if (descr.properties[prop].index) {
      this.indexes[m][prop] = descr.properties[prop].type;
    }
  }.bind(this));
};

RedisCRUD.prototype.defineForeignKey = function (model, key, cb) {
  this.indexes[model][key] = Number;
  cb(null, Number);
};

RedisCRUD.prototype.forDb = function (model, data) {
  var p = this._models[model].properties;
  for (var i in data) {
    if (typeof data[i] === 'undefined') {
      delete data[i];
      continue;
    }
    if (!p[i]){
      data[i] = JSON.stringify(data[i]);
      continue;
    }
    if (p[i].type.name != "Boolean" && !(i in data && data[i] !== null)) {
      data[i] = "";
      continue;
    }
    switch (p[i].type.name) {
      case "Date":
        if (data[i].getTime) {
          // just Date object
          data[i] = data[i].getTime().toString();
        } else if (typeof data[i] === 'number') {
          // number of milliseconds
          data[i] = parseInt(data[i], 10).toString();
        } else if (typeof data[i] === 'string' && !isNaN(parseInt(data[i]))) {
          // numeric string
          data[i] = data[i];
        } else {
          // something odd
          data[i] = '0';
        }
        break;
      case "Number":
        data[i] = data[i] && data[i].toString();
        break;
      case "Boolean":
        data[i] = data[i] ? "true" : "false";
        break;
      case "String":
      case "Text":
        break;
      default:
        data[i] = JSON.stringify(data[i]);
    }
  }
  return data;
};

/*!
 * Decide if a field should be included
 * @param {Object} fields
 * @returns {Boolean}
 * @private
 */
function isIncluded(fields, f) {
  if(!fields) {
    return true;
  }
  if(Array.isArray(fields)) {
    return fields.indexOf(f) >= 0;
  }
  if(fields[f]) {
    // Included
    return true;
  }
  if((f in fields) && !fields[f]) {
    // Excluded
    return false;
  }
  for(var f1 in fields) {
    return !fields[f1]; // If the fields has exclusion
  }
  return true;
}

RedisCRUD.prototype.fromDb = function (model, data, fields) {
  fields = fields || {};
  var p = this._models[model].properties, d;
  for (var i in data) {
    if(!isIncluded(fields, i)) {
      // Exclude
      delete data[i];
      continue;
    }
    if (!p[i]) continue;
    if (!data[i]) {
      data[i] = "";
      continue;
    }
    switch (p[i].type.name) {
      case "Date":
        d = new Date(data[i]);
        d.setTime(data[i]);
        data[i] = d;
        break;
      case "Number":
        data[i] = Number(data[i]);
        break;
      case "Boolean":
        data[i] = data[i] === "true" || data[i] === "1";
        break;
      case "String":
      case "Text":
        break;
      default:
        d = data[i];
        try {
          data[i] = JSON.parse(data[i]);
        }
        catch(e) {
          data[i] = d;
        }
    }
  }
  return data;
};

RedisCRUD.prototype.save = function (model, data, callback) {
  data = this.forDb(model, data);
  deleteNulls(data);
  this.client.hgetall(model + ':' + data.id, function (err, prevData) {
    if (err) return callback(err);
    this.client.hmset([model + ':' + data.id, data], function (err) {
      if (err) return callback(err);
      if (prevData) {
        Object.keys(prevData).forEach(function (k) {
          if (data.hasOwnProperty(k)) return;
          data[k] = prevData[k];
        });
      }
      this.updateIndexes(model, data.id, data, callback, this.forDb(model, prevData));
    }.bind(this));
  }.bind(this));
};

RedisCRUD.prototype.updateIndexes = function (model, id, data, callback, prevData) {
  var p = this._models[model].properties;
  var i = this.indexes[model];
  var schedule = [];
  if (!callback.removed) {
    schedule.push(['SADD', 's:' + model, id]);
  }
  Object.keys(i).forEach(function (key) {
    if (data.hasOwnProperty(key)) {
      var val = data[key];
      schedule.push([
        'SADD',
        'i:' + model + ':' + key + ':' + val,
        id
      ]);
    }
    if (prevData && prevData[key] !== data[key]) {
      schedule.push([
        'SREM',
        'i:' + model + ':' + key + ':' + prevData[key],
        id
      ]);
    }
  });

  if (schedule.length) {
    this.client.multi(schedule, function (err) {
      callback(err, data);
    });
  } else {
    callback(null);
  }
};

RedisCRUD.prototype.create = function (model, data, callback) {
  if (data.id) return create.call(this, data.id, true);

  this.client.incr('id:' + model, function (err, id) {
    create.call(this, id);
  }.bind(this));

  function create(id, upsert) {
    data.id = id.toString();
    this.save(model, data, function (err) {
      if (callback) {
        callback(err, id);
      }
    });

    // push the id to the list of user ids for sorting
    this.client.sadd(['s:' + model, data.id]);
  }
};

RedisCRUD.prototype.updateOrCreate = function (model, data, callback) {
  if (!data.id) {
    return this.create(model, data, callback);
  }
  var client = this.client;
  this.save(model, data, function (error, obj) {
    var key = 'id:' + model;
    client.get(key, function (err, id) {
      if (!id && data.id) {
        client.set([key, data.id], ok);
      } else {
        ok();
      }
    });
    function ok() {
      callback(error, obj);
    }
  });
};

RedisCRUD.prototype.exists = function (model, id, callback) {
  this.client.exists(model + ':' + id, function (err, exists) {
    if (callback) {
      callback(err, exists);
    }
  });
};

RedisCRUD.prototype.find = function find(model, id, callback) {
  this.client.hgetall(model + ':' + id, function (err, data) {
    if (data && Object.keys(data).length > 0) {
      data.id = id;
    } else {
      data = null;
    }
    callback(err, this.fromDb(model, data));
  }.bind(this));
};

RedisCRUD.prototype.destroy = function destroy(model, id, callback) {
  var br = this;
  var tr = br.client.transaction();

  br.client.hgetall(model + ':' + id, function (err, data) {
    if (err) return callback(err);

    tr.srem(['s:' + model, id]);
    tr.del(model + ':' + id);
    tr.run(function (err) {
      if (err) return callback(err);
      callback.removed = true;

      br.updateIndexes(model, id, {}, callback, data);
    });
  });
};

RedisCRUD.prototype.possibleIndexes = function (model, filter) {
  if (!filter || Object.keys(filter.where || {}).length === 0) return false;

  var foundIndex = [];
  var noIndex = [];
  Object.keys(filter.where).forEach(function (key) {
    var i = this.indexes[model][key];
    if (i) {
      var val = filter.where[key];
      if (typeof val === 'object' && val.inq && val.inq.length) {
        val.inq.forEach(function(v) {
          foundIndex.push('i:' + model + ':' + key + ':' + v);
        });
        return;
      }
      if (i.name === 'Date') {
        if (val.getTime) {
          val = val.getTime();
        } else if (typeof val === 'number' || (typeof val === 'string'
          && !isNaN(parseInt(val, 10)))) {
          val = parseInt(val, 10).toString();
        } else if (!val) {
          val = '0';
        }
      }
      foundIndex.push('i:' + model + ':' + key + ':' + val);
    } else {
      noIndex.push(key);
    }
  }.bind(this));

  return [foundIndex, noIndex];
};

RedisCRUD.prototype.all = function all(model, filter, callback) {
  var ts = Date.now();
  var client = this.client;
  var cmd;
  var redis = this;
  var sortCmd = [];
  var props = this._models[model].properties;
  var allNumeric = true;
  var dest = 'temp' + (Date.now() * Math.random());
  var innerSetUsed = false;
  var trans = this.client.transaction();

  if (!filter) {
    filter = {order: 'id'};
  }

  // WHERE
  if (filter.where) {

    // special case: search by id
    if (filter.where.id) {
      if (filter.where.id.inq) {
        return handleKeys(null, filter.where.id.inq.map(function(id) {
          return model + ':' + id;
        }));
      } else {
        return handleKeys(null, [model + ':' + filter.where.id]);
      }
    }

    var pi = this.possibleIndexes(model, filter);
    var indexes = pi[0];
    var noIndexes = pi[1];

    if (noIndexes.length) {
      throw new Error(model + ': no indexes found for ' +
      noIndexes.join(', ') +
      ' impossible to sort and filter using redis connector');
    }

    if (indexes && indexes.length) {
      innerSetUsed = true;
      if (indexes.length > 1) {
        indexes.unshift(dest);
        trans.sinterstore(indexes);
      } else {
        dest = indexes[0];
      }
    } else {
      throw new Error('No indexes for ' + filter.where);
    }
  } else {
    dest = 's:' + model;
    // no filtering, just sort/limit (if any)
  }

  // only counting?
  if (filter.getCount) {
    trans.scard(dest, callback);
    return trans.run();
  }

  // ORDER
  var reverse = false;
  if (!filter.order) {
    filter.order = 'id';
  }
  var orders = filter.order;
  if (typeof filter.order === "string"){
    orders = [filter.order];
  }

  orders.forEach(function (key) {
    var m = key.match(/\s+(A|DE)SC$/i);
    if (m) {
      key = key.replace(/\s+(A|DE)SC/i, '');
      if (m[1] === 'DE') reverse = true;
    }
    if (props[key].type.name !== 'Number' && props[key].type.name !== 'Date') {
      allNumeric = false;
    }
    sortCmd.push("BY", model + ":*->" + key);
  });

  // LIMIT
  if (filter.limit || filter.skip) {
    var offset = (filter.skip || 0), limit = (filter.limit || -1);
    sortCmd.push("LIMIT", offset, limit);
  }

  // we need ALPHA modifier when sorting string values
  // the only case it's not required - we sort numbers
  if (!allNumeric) {
    sortCmd.push('ALPHA');
  }

  if (reverse) {
    sortCmd.push('DESC');
  }

  sortCmd.unshift(dest);
  sortCmd.push("GET", "#");
  cmd = "SORT " + sortCmd.join(" ");
  var ttt = Date.now();
  trans.sort(sortCmd, function (err, ids){
    if (err) {
      return callback(err, []);
    }
    var sortedKeys = ids.map(function (i) {
      return model + ":" + i;
    });
    handleKeys(err, sortedKeys);
  });

  if (dest.match(/^temp/)) {
    trans.del(dest);
  }

  trans.run(callback);

  function handleKeys(err, keys) {
    var t2 = Date.now();
    var query = keys.map(function (key) {
      return ['hgetall', key];
    });
    client.multi(query, function (err, replies) {
      if (err) {
        return callback(err);
      }

      var objs = (replies || []).map(function (r) {
        return redis.fromDb(model, r, filter.fields);
      });

      if (filter && filter.include) {
        redis._models[model].model.include(objs, filter.include, callback);
      } else {
        callback(err, objs);
      }
    });
  }

};

RedisCRUD.prototype.destroyAll = function destroyAll(model, where, callback) {
  var br = this;
  if(!callback && 'function' === typeof where) {
    callback = where;
    where = undefined;
  }
  if(where) {
        this.all(model, {where: where}, function(err, results) {
      if(err || !results) {
        callback && callback(err, results);
        return;
      }
      var tasks = [];
      results.forEach(function(result) {
        tasks.push(function(done) {br.destroy(model, result.id, done)});
      });
      async.parallel(tasks, callback);
    });
  } else {
    this.client.multi([
      ['KEYS', model + ':*'],
      ['KEYS', '*:' + model + ':*']
    ], function (err, k) {
      br.client.del(k[0].concat(k[1]).concat('s:' + model), callback);
    });
  }

};

RedisCRUD.prototype.count = function count(model, callback, where) {
  if (where && Object.keys(where).length) {
    this.all(model, {where: where, getCount: true}, callback);
  } else {
    this.client.scard('s:' + model, callback);
  }
};

RedisCRUD.prototype.updateAttributes = function updateAttrs(model, id, data, cb) {
  data.id = id;
  this.save(model, data, cb);
};

function deleteNulls(data) {
  Object.keys(data).forEach(function (key) {
    if (data[key] === null) delete data[key];
  });
}

RedisCRUD.prototype.disconnect = function disconnect(cb) {
  this.client.quit(cb);
};

RedisCRUD.prototype.transaction = function () {
  throw new Error('not implemented');
};

RedisCRUD.prototype.automigrate = function automigrate(models, cb) {
  var self = this;

  if (self.client.client) {
    this.client.flushdb(cb);
  } else {
    self.dataSource.once('connected', function () {
      self.automigrate(models, cb);
    });
  }
};


RedisCRUD.prototype.ping = function (cb) {
  var self = this;
  if (self.client) {
    this.client.ping(cb);
  } else {
    self.dataSource.once('connected', function () {
      self.ping(cb);
    });
    self.dataSource.once('error', function (err) {
      cb(err);
    });
    self.connect(function() {});
  }
};


module.exports = RedisCRUD;

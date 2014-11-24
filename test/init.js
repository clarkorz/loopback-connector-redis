module.exports = require('should');

var DataSource = require('loopback-datasource-juggler').DataSource;

var localSettings = tryRequire('./settings.local.json');

global.getSchema = function(settings) {
    var db = new DataSource(require('../'), defaults(settings, localSettings));
    // db.log = function (a) { console.log(a); };
    return db;
};


function tryRequire(path) {
  try {
    return require(path);
  } catch (e) {
    return {};
  }
}

function defaults(dest, src) {
  Object.keys(src).forEach(function (key) {
    if (src.hasOwnProperty(key) && !dest.hasOwnProperty(key)) {
      dest[key] = src[key];
    }
  });

  return dest;
}

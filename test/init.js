module.exports = require('should');

var _ = require('lodash');
var DataSource = require('loopback-datasource-juggler').DataSource;

var localSettings = tryRequire('./settings.local.json');

global.getSchema = function(settings) {
    var db = new DataSource(require('../'), _.defaults({}, settings, localSettings));
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

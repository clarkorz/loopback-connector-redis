module.exports = require('should');

var DataSource = require('loopback-datasource-juggler').DataSource;

global.getSchema = function() {
    var db = new DataSource(require('../'), { host: 'clarkorz.local', port: 6379 });
    // db.log = function (a) { console.log(a); };
    return db;
};

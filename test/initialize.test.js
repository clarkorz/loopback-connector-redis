var should = require('./init.js');

var ds;

describe('initialize', function () {

  describe('a client connector', function () {

    before(function() {
      ds = getSchema({ type: 'client' });
    });

    it('should create a client connector', function () {
      ds.settings.should.have.property('type', 'client');
    });

  });

  describe('a curd connector', function () {

    before(function() {
      ds = getSchema({ type: 'crud' });
    });

    it('should create a curd connector', function () {
      ds.settings.should.have.property('type', 'crud');
    });

  });

});

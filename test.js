const Naive = require('.');
const fs = require('fs');
const crypto = require('crypto');

const expect = require("chai").expect;

var db;

describe('init()', function () {
  it('should should create a directory matching the dir in the contstrcutor', function () {
    return new Promise(async function (resolve, reject) {
      var directory = 'testDir';
      db = new Naive(directory);
      await db.init();
      fs.access(directory, fs.constants.F_OK, async function (err) {
        if (err) {
          reject(err);
        }
        resolve(true);
      });
    });
  });
});

describe('put()', function () {
  it('should insert a new value', function () {
    return new Promise(async function (resolve, reject) {
      try {
        db.put('key1', 'value1');
        expect(await db.get('key1')).to.equal('value1');
        resolve(true);
      } catch (error) {
        reject(error);
      }
    });
  });
  it('should update an existing value', function () {
    return new Promise(async function (resolve, reject) {
      try {
        db.put('key1', 'value2');
        expect(await db.get('key1')).to.equal('value2');
        resolve(true);
      } catch (error) {
        reject(error);
      }
    });
  });
});

describe('get()', function () {
  it('should return a previously upserted value', function () {
    return new Promise(async function (resolve, reject) {
      try {
        db.put('key2', 'value2');
        expect(await db.get('key2')).to.equal('value2');
        resolve(true);
      } catch (error) {
        reject(error);
      }
    });
  });
  it('should return false for a non-existent value', function () {
    return new Promise(async function (resolve, reject) {
      try {
        expect(await db.get('nonexistant')).to.be.false;
        resolve(true);
      } catch (error) {
        reject(error);
      }
    });
  });
  it('should return false for a deleted value', function () {
    return new Promise(async function (resolve, reject) {
      try {
        await db.delete('key1');
        expect(await db.get('key1')).to.be.false;
        resolve(true);
      } catch (error) {
        reject(error);
      }
    });
  });
});

describe('delete()', function () {
  it('should mark a record as deleted', function () {
    return new Promise(async function (resolve, reject) {
      try {
        await db.delete('key2');
        expect(await db.get('key2')).to.be.false;
        resolve(true);
      } catch (error) {
        reject(error);
      }
    });
  });
});

describe('bulk', function () {
  it('~8MB Test', function () {
    return new Promise(async function (resolve, reject) {
      try {
        var test_checks = [];
        for (var i = 0; i < 20000; i++) {
          var key = 'key' + i;
          var val = crypto.randomBytes(400).toString('hex');
          await db.put(key, val);
          if (i % 200 == 0) {
            test_checks.push({ key, val });
          }
        }
        for (let i in test_checks) {
          expect(test_checks[i].key + ':' + await db.get(test_checks[i].key)).to.equal(test_checks[i].key + ':' + test_checks[i].val);
        }
        resolve(true);
      } catch (error) {
        reject(error);
      }
    });
  }).timeout(20000);
  it('~16MB Test', function () {
    return new Promise(async function (resolve, reject) {
      try {
        var test_checks = [];
        for (var i = 0; i < 40000; i++) {
          var key = 'key' + i;
          var val = crypto.randomBytes(400).toString('hex');
          await db.put(key, val);
          if (i % 400 == 0) {
            test_checks.push({ key, val });
          }
        }
        for (let i in test_checks) {
          expect(test_checks[i].key + ':' + await db.get(test_checks[i].key)).to.equal(test_checks[i].key + ':' + test_checks[i].val);
        }
        resolve(true);
      } catch (error) {
        reject(error);
      }
    });
  }).timeout(20000);
  it('~32MB Test', function () {
    return new Promise(async function (resolve, reject) {
      try {
        var test_checks = [];
        for (var i = 0; i < 80000; i++) {
          var key = 'key' + i;
          var val = crypto.randomBytes(400).toString('hex');
          await db.put(key, val);
          if (i % 800 == 0) {
            test_checks.push({ key, val });
          }
        }
        for (let i in test_checks) {
          expect(test_checks[i].key + ':' + await db.get(test_checks[i].key)).to.equal(test_checks[i].key + ':' + test_checks[i].val);
        }
        resolve(true);
      } catch (error) {
        reject(error);
      }
    });
  }).timeout(40000);
  it('~64MB Test', function () {
    return new Promise(async function (resolve, reject) {
      try {
        var test_checks = [];
        for (var i = 0; i < 80000; i++) {
          var key = 'key' + i;
          var val = crypto.randomBytes(800).toString('hex');
          await db.put(key, val);
          if (i % 1600 == 0) {
            test_checks.push({ key, val });
          }
        }
        for (let i in test_checks) {
          expect(test_checks[i].key + ':' + await db.get(test_checks[i].key)).to.equal(test_checks[i].key + ':' + test_checks[i].val);
        }
        resolve(true);
      } catch (error) {
        reject(error);
      }
    });
  }).timeout(40000);
});
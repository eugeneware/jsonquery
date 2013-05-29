var expect = require('chai').expect
  , Stream = require('stream')
  , jsonquery = require('../jsonquery');

function generator(n) {
  var s = new Stream;
  s.readable = true;
  var i = 0;
  function next() {
    s.emit('data', {
      name: 'Name ' + i,
      number: i++
    });
    if (i == n) {
      s.emit('end');
    } else {
      setTimeout(next, 0);
    }
  }
  setTimeout(next, 0);
  return s;
}

function mapper(fn, cb) {
  cb = cb || function() {};
  var s = new Stream;
  s.readable = true;
  s.writable = true;
  s.write = function (data) {
    s.emit('data', fn(data));
  };
  s.end = function (data) {
    if (arguments.length) s.write(data);
    s.writeable = false;
    s.emit('end');
    cb();
  };
  s.destroy = function () {
    s.writeable = false;
  };

  return s;
}

function selector(fn, cb) {
  cb = cb || function() {};
  var s = new Stream;
  s.readable = true;
  s.writable = true;
  s.write = function (data) {
    if (fn(data)) {
      s.emit('data', data);
    }
  };
  s.end = function (data) {
    if (arguments.length) s.write(data);
    s.writeable = false;
    s.emit('end');
    cb();
  };

  return s;
}

function reducer(fn, initial, cb) {
  cb = cb || function() {};
  var s = new Stream;
  s.readable = true;
  s.writable = true;
  var acc = initial;
  s.write = function (data) {
    acc = fn(data, acc);
  };
  s.end = function (data) {
    if (arguments.length) s.write(data);
    s.writeable = false;
    s.emit('data', acc);
    s.emit('end');
    cb(acc);
  };
  s.destroy = function () {
    s.writeable = false;
  };

  return s;
}

function pluck(name) {
  return mapper(prop(name));
}

function logger() {
  return mapper(console.log, process.exit);
}

function prop(name) {
  return function (data) {
    return data[name];
  }
}

describe('jsonquery', function () {
  it('should be able to filter', function (done) {
    var count = 0;
    generator(100)
      .pipe(jsonquery({ number: { $mod: [ 5, 0 ] } }))
      .on('data', function (data) {
        expect(data.number % 5).to.equal(0);
        count++;
      })
      .on('end', function () {
        expect(count).to.equal(20);
        done();
      });
  });
});

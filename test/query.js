var expect = require('chai').expect
  , Stream = require('stream')
  , jsonquery = require('../jsonquery');

function generator(n) {
  var s = new Stream;
  s.readable = true;
  var i = 0;
  function next() {
    s.emit('data', {
      name: "Name " + i,
      number: "Number " + i,
      val: i*10,
      favorites: [i*10, (i + 1)*10],
      awesome: true,
      nullify: null,
      tree: {
        a: i,
        b: i + 1,
      }
    });
    i++;
    if (i == n) {
      s.emit('end');
    } else {
      process.nextTick(next);
    }
  }
  process.nextTick(next);
  return s;
}

describe('jsonquery tests', function () {
  var size = 100;

  it('should be able to do basic queries', function (done) {
    var count = 0;
    generator(size)
      .pipe(jsonquery({ number: 'Number 7' }))
      .on('data', function (doc) {
        expect(doc.name).to.equal('Name 7');
        expect(doc.number).to.equal('Number 7');
        expect(doc.val).to.equal(70);
        count++;
      })
      .on('end', function () {
        expect(count).to.equal(1);
        done();
      });
  });

  it('should be able to do basic AND queries', function (done) {
    var count = 0;
    generator(size)
      .pipe(jsonquery({ number: 'Number 7', val: 70 }))
      .on('data', function (doc) {
        expect(doc.name).to.equal('Name 7');
        expect(doc.number).to.equal('Number 7');
        expect(doc.val).to.equal(70);
        count++;
      })
      .on('end', function () {
        expect(count).to.equal(1);
        done();
      });
  });

  it('should be able to do basic OR queries', function (done) {
    var count = 0;
    generator(size)
      .pipe(jsonquery({ $or:  [ { number: 'Number 7' }, { val: 50 } ] }))
      .on('data', function (doc) {
        expect(doc.val == 50 || doc.number == 'Number 7').to.be.true;
        count++;
      })
      .on('end', function () {
        expect(count).to.equal(2);
        done();
      });
  });

  it('should be able to do basic $AND queries', function (done) {
    var count = 0;
    generator(size)
      .pipe(jsonquery({ $and:  [ { number: 'Number 7' }, { val: 70 } ] }))
      .on('data', function (doc) {
        expect(doc.number).to.equal('Number 7');
        expect(doc.val).to.equal(70);
        count++;
      })
      .on('end', function () {
        expect(count).to.equal(1);
        done();
      });
  });

  it('should be able to do nested $or and $and queries', function (done) {
    var count = 0;
    generator(size)
      .pipe(jsonquery({ $or:  [ { $and: [ { number: 'Number 7' }, { val: 70 } ] }, { val: 50 } ] }))
      .on('data', function (doc) {
        expect(doc.val == 50 || doc.number == 'Number 7').to.be.true;
        count++;
      })
      .on('end', function () {
        expect(count).to.equal(2);
        done();
      });
  });

  it('should be able to do basic $in queries', function (done) {
    var count = 0;
    generator(size)
      .pipe(jsonquery({ val: { $in: [ 70, 50 ] } }))
      .on('data', function (doc) {
        expect([50, 70]).to.include(doc.val);
        count++;
      })
      .on('end', function () {
        expect(count).to.equal(2);
        done();
      });
  });

  it('should be able to do $or and $in queries', function (done) {
    var count = 0;
    generator(size)
      .pipe(jsonquery({ val: { $or: [ { $in: [ 70, 50 ] }, { $in: [ 60, 20 ] } ] } }))
      .on('data', function (doc) {
        expect([20, 50, 60, 70]).to.include(doc.val);
        count++;
      })
      .on('end', function () {
        expect(count).to.equal(4);
        done();
      });
  });

  it('should be able to do $gt queries', function (done) {
    var count = 0;
    generator(size)
      .pipe(jsonquery({ val: { $gt: 900 } }))
      .on('data', function (doc) {
        expect(doc.val).to.be.above(900);
        count++;
      })
      .on('end', function () {
        expect(count).to.equal(9);
        done();
      });
  });

  it('should be able to do $lt queries', function (done) {
    var count = 0;
    generator(size)
      .pipe(jsonquery({ val: { $lt: 900 } }))
      .on('data', function (doc) {
        expect(doc.val).to.be.below(900);
        count++;
      })
      .on('end', function () {
        expect(count).to.equal(90);
        done();
      });
  });

  it('should be able to nest $or, $lt, and $gt', function (done) {
    var count = 0;
    generator(size)
      .pipe(jsonquery({ val: { $or: [ { $lt: 20 }, { $gt: 950 } ] } }))
      .on('data', function (doc) {
        count++;
      })
      .on('end', function () {
        expect(count).to.equal(6);
        done();
      });
  });

  it('should be able to nest $and, $lt, and $gt', function (done) {
    var count = 0;
    generator(size)
      .pipe(jsonquery({ val: { $and: [ { $gt: 970 }, { $gt: 950 } ] } }))
      .on('data', function (doc) {
        expect(doc.val).to.be.above(970);
        expect(doc.val).to.be.above(950);
        count++;
      })
      .on('end', function () {
        expect(count).to.equal(2);
        done();
      });
  });

  it('should be able to do $ne queries', function (done) {
    var count = 0;
    generator(size)
      .pipe(jsonquery({ val: { $ne: 900 } }))
      .on('data', function (doc) {
        expect(doc.val).to.not.equal(900);
        count++;
      })
      .on('end', function () {
        expect(count).to.equal(99);
        done();
      });
  });

  it('should be able to do $lte queries', function (done) {
    var count = 0;
    generator(size)
      .pipe(jsonquery({ val: { $lte: 900 } }))
      .on('data', function (doc) {
        expect(doc.val).to.be.lte(900);
        count++;
      })
      .on('end', function () {
        expect(count).to.equal(91);
        done();
      });
  });

  it('should be able to do $gte queries', function (done) {
    var count = 0;
    generator(size)
      .pipe(jsonquery({ val: { $gte: 900 } }))
      .on('data', function (doc) {
        expect(doc.val).to.be.gte(900);
        count++;
      })
      .on('end', function () {
        expect(count).to.equal(10);
        done();
      });
  });

  it('should be able to do $all queries', function (done) {
    var count = 0;
    generator(size)
      .pipe(jsonquery({ favorites: { $all: [50, 60] } }))
      .on('data', function (doc) {
        expect(doc.favorites).to.deep.equal([50, 60]);
        count++;
      })
      .on('end', function () {
        expect(count).to.equal(1);
        done();
      });
  });

  it('should be able to do $elemMatch queries', function (done) {
    var count = 0;
    generator(size)
      .pipe(jsonquery({ tree: { $elemMatch: { a: 1, b: 2 } } }))
      .on('data', function (doc) {
        expect(doc.tree.a).to.equal(1);
        expect(doc.tree.b).to.equal(2);
        count++;
      })
      .on('end', function () {
        expect(count).to.equal(1);
        done();
      });
  });

  it('should be able to do child queries', function (done) {
    var count = 0;
    generator(size)
      .pipe(jsonquery({ 'tree.a': 1 }))
      .on('data', function (doc) {
        expect(doc.tree.a).to.equal(1);
        count++;
      })
      .on('end', function () {
        expect(count).to.equal(1);
        done();
      });
  });

  it('should be able to do nested child queries', function (done) {
    var count = 0;
    generator(size)
      .pipe(jsonquery({ 'tree.a': { $in: [1, 5] } }))
      .on('data', function (doc) {
        expect([1, 5]).to.include(doc.tree.a);
        count++;
      })
      .on('end', function () {
        expect(count).to.equal(2);
        done();
      });
  });

  it('should be able to do regex queries', function (done) {
    var count = 0;
    generator(size)
      .pipe(jsonquery({ number: /er 7$/ }))
      .on('data', function (doc) {
        expect(doc.number).to.equal('Number 7');
        count++;
      })
      .on('end', function () {
        expect(count).to.equal(1);
        done();
      });
  });

  it('should be able to do $in queries with regex', function (done) {
    var count = 0;
    generator(size)
      .pipe(jsonquery({ number: { $in: [ /er 7$/, /er 5$/ ] } }))
      .on('data', function (doc) {
        expect(['Number 5', 'Number 7']).to.include(doc.number);
        count++;
      })
      .on('end', function () {
        expect(count).to.equal(2);
        done();
      });
  });

  it('should be able to do $all queries with regex', function (done) {
    var count = 0;
    generator(size)
      .pipe(jsonquery({ favorites: { $all: [/^50$/, /^60$/] } }))
      .on('data', function (doc) {
        expect(doc.favorites).to.deep.equal([50, 60]);
        count++;
      })
      .on('end', function () {
        expect(count).to.equal(1);
        done();
      });
  });

  it('should be able to do $elemMatch queries with regex', function (done) {
    var count = 0;
    generator(size)
      .pipe(jsonquery({ tree: { $elemMatch: { a: /^1$/, b: 2 } } }))
      .on('data', function (doc) {
        expect(doc.tree.a).to.equal(1);
        expect(doc.tree.b).to.equal(2);
        count++;
      })
      .on('end', function () {
        expect(count).to.equal(1);
        done();
      });
  });

  it('should be able to do basic $nin queries', function (done) {
    var count = 0;
    generator(size)
      .pipe(jsonquery({ val: { $nin: [ 70, 50 ] } }))
      .on('data', function (doc) {
        expect(doc.val).to.not.equal(70);
        expect(doc.val).to.not.equal(50);
        count++;
      })
      .on('end', function () {
        expect(count).to.equal(98);
        done();
      });
  });

  it('should be able to do basic $mod queries', function (done) {
    var count = 0;
    generator(size)
      .pipe(jsonquery({ val: { $mod: [ 7, 1 ] } }))
      .on('data', function (doc) {
        expect(doc.val % 7 === 1).to.be.true;
        count++;
      })
      .on('end', function () {
        expect(count).to.equal(14);
        done();
      });
  });

  it('should be able to do $size queries', function (done) {
    var count = 0;
    generator(size)
      .pipe(jsonquery({ favorites: { $size: 2 } }))
      .on('data', function (doc) {
        expect(doc.favorites.length).to.equal(2);
        count++;
      })
      .on('end', function () {
        expect(count).to.equal(100);
        done();
      });
  });

  it('should be able to do $exists queries', function (done) {
    var count = 0;
    generator(size)
      .pipe(jsonquery({ $and: [ { tree: { $exists: true } }, { missing: { $exists: false } } ] }))
      .on('data', function (doc) {
        expect(doc.tree).to.exist;
        expect(doc.missing).to.not.exist;
        count++;
      })
      .on('end', function () {
        expect(count).to.equal(100);
        done();
      });
  });

  it('should have limited support for $type queries', function (done) {
    var count = 0;
    generator(size)
      .pipe(jsonquery({
        name: { $type: 2 },
        val: { $type: 1 },
        tree: { $type: 3 },
        favorites: { $type: 4 },
        awesome: { $type: 8 },
        nullify: { $type: 10 }
      }))
      .on('data', function (doc) {
        expect(doc.name).to.be.a('string');
        count++;
      })
      .on('end', function () {
        expect(count).to.equal(100);
        done();
      });
  });

  it('should be able to do basic $not queries part 1', function (done) {
    var count = 0;
    generator(size)
      .pipe(jsonquery({ $not: { number: 'Number 7', val: 70 } }))
      .on('data', function (doc) {
        expect(doc.number).to.not.equal('Number 7');
        expect(doc.number).to.not.equal(70);
        count++;
      })
      .on('end', function () {
        expect(count).to.equal(99);
        done();
      });
  });

  it('should be able to do basic $not queries part 2', function (done) {
    var count = 0;
    generator(size)
      .pipe(jsonquery({ number: { $not: { $nin: ['Number 7'] } } }))
      .on('data', function (doc) {
        expect(doc.number).to.equal('Number 7');
        count++;
      })
      .on('end', function () {
        expect(count).to.equal(1);
        done();
      });
  });

  it('should not match a document missing a field', function (done) {
    var count = 0;
    stream = jsonquery({ foo: { $all: ['bar'] } })

    stream
      .on('data', function (doc) {
        expect(doc.foo).to.deep.equal(['bar']);
        count++;
      })
      .on('end', function () {
        expect(count).to.equal(1);
        done();
      });

    stream.write({ bar: ['baz'] })
    stream.write({ foo: ['bar'] })
    stream.end()
  });
});

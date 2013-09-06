# jsonquery

MongoDB query language implemented as a Streaming filter.

This library implements the entire MongoDB query language as a node.js
filtering stream;

[![build status](https://secure.travis-ci.org/eugeneware/jsonquery.png)](http://travis-ci.org/eugeneware/jsonquery)

## Installation

To install, use npm:

```
$ npm install jsonquery
```

## Examples

Here's an example of usage:

``` js
var jsonquery = require('jsonquery');

var count = 0;
generator(100) // a readable stream that outputs JSON documents
  .pipe(jsonquery({ val: { $and: [ { $gt: 970 }, { $gt: 950 } ] } })) // filter
  .on('data', function (doc) {
    expect(doc.val).to.be.above(970);
    expect(doc.val).to.be.above(950);
    count++;
  })
  .on('end', function () {
    expect(count).to.equal(2);
  });
```

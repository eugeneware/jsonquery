Stream = require('stream')

# dereference value being matched against
lookup = (needle, haystack) ->
  keys = needle.split '.'

  val = haystack
  for key in keys
    return if val is undefined
    val = val[key]

  val

# equality that also does comparison against RegExps
eq = (a, b) ->
  if b instanceof RegExp
    b.test a
  else if Array.isArray(a)
    a.indexOf(b) != -1
  else
    b == a

# checking the type against mongoDB BSON types
checkType = (val, typeId) ->
  switch typeId
    when 1 # Double
      typeof val == 'number'

    when 2 # String
      typeof val == 'string'

    when 3 # Object
      typeof val == 'object'

    when 4 # Array
      val instanceof Array

    when 8 # Boolean
      typeof val == 'boolean'

    when 10 # Null
      typeof val == 'object' and not val

    else
      false

# match predicate expression against document
match = (predicate, haystack) ->
  matches = 0
  matchCount = Object.keys(predicate).length
  for n, v of predicate
    # LHS $and, $or, $not
    if n[0] == '$'
      matches += operator n, v, haystack
    # complex RHS predicate (eg. $in, $gt)
    else if v and v.constructor == Object
      matches++ if valOpMatch(lookup(n, haystack), v, haystack)
    # simple lookup
    else
      matches++ if eq(lookup(n, haystack),  v)

  matches == matchCount

# process complex RHS predicates (eg. $in, $gt)
valOpMatch = (val, predicate, haystack) ->
  matchCount = Object.keys(predicate).length
  matches = 0

  for n, v of predicate
    # keys must be an operator
    if n[0] == '$'
      matches++ if valOp(n, val, v, haystack)

  matches == matchCount

# operators on the RHS of queries
valOp = (op, val, args, haystack) ->
  switch op
    when '$in'
      matchCount = 0
      matches = 0
      for part in args
        matches++ if eq(val, part)
      matches > 0

    when '$nin'
      not valOp('$in', val, args, haystack)

    when '$all'
      matchCount = args.length
      matches = 0

      if val
        for part in args
          for v in val
            if eq(v, part)
              matches++
              break

      matches == matchCount

    when '$elemMatch'
      match args, val

    when '$or'
      matches = 0
      for part in args
        matches++ if valOpMatch(val, part, haystack)

      matches > 0

    when '$and'
      matchCount = args.length
      matches = 0
      for part in args
        matches++ if valOpMatch(val, part, haystack)

      matches == matchCount

    when '$not'
      not valOpMatch(val, args, haystack)

    when '$gt'
      val > args

    when '$gte'
      val >= args

    when '$ne'
      val != args

    when '$lt'
      val < args

    when '$lte'
      val <= args

    when '$mod'
      (val % args[0]) == args[1]

    when '$size'
      val instanceof Array and val.length == args

    when '$exists'
      if args
        val isnt undefined
      else
        val is undefined

    when '$type'
      checkType val, args

    else
      false

# operations on the LHS of queries
operator = (op, predicate, haystack) ->
  switch op
    when '$or'
      matches = 0
      for part in predicate
        matches++ if match(part, haystack)

      matches > 0

    when '$and'
      matchCount = predicate.length
      matches = 0
      for part in predicate
        matches++ if match(part, haystack)

      matches == matchCount

    when '$not'
      not match(predicate, haystack)

    else
      false

queryStream = (query) ->
  s = new Stream
  s.writable = true
  s.readable = true

  s.write = (buf) ->
    if match query, buf
      s.emit('data', buf)

  s.end = (buf) ->
    if arguments.length then s.write(buf)
    s.writeable = false

    s.emit('end')

  s.destroy = ->
    s.writeable = false

  s

module.exports = queryStream
module.exports.match = (haystack, predicate) ->
  match predicate, haystack

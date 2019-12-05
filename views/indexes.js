const through = require('through2')

const { opsForRecords } = require('./helpers')

const CHAR_END = '\uffff'
const CHAR_SPLIT = '\u0000'
const CHAR_START = '\u0001'

module.exports = function indexedView (lvl, db) {
  return {
    name: 'indexes',
    map (msgs, next) {
      opsForRecords(db, msgs, mapToIndex, (err, ops) => {
        if (err) return next(err)
        // console.log('OPS', ops)
        lvl.batch(ops, next)
      })
    },

    api: {
      query (kappa, opts, cb) {
        // const { schema, prop, value, gt, lt, gte, lte, reverse, limit } = opts
        const proxy = transform()
        if (!opts.schema || !opts.prop) {
          proxy.destroy(new Error('schema and prop are required.'))
        } else {
          opts.schema = db.schemas.resolveName(opts.schema)
          if (!opts.schema) return proxy.destroy(new Error('Invalid schema name.'))
          const lvlopts = queryOptsToLevelOpts(opts)
          lvl.createReadStream(lvlopts).pipe(proxy)
        }
        return proxy
      }
    }
  }
}

function mapToIndex (msg, db) {
  const schema = db.getSchema(msg)
  const ops = []
  const { id, key: source, seq, schema: schemaName, value } = msg
  if (!schema || !schema.properties) return ops
  // TODO: Recursive?
  for (const [field, def] of Object.entries(schema.properties)) {
    // Only care for fields that want to be indexed and are not undefined.
    if (!def.index) continue
    if (typeof value[field] === 'undefined') continue

    if (def.type === 'array') var values = value[field]
    else values = [value[field]]
    values.forEach(val => {
      ops.push({
        key: [schemaName, field, val, id, source].join(CHAR_SPLIT),
        value: seq
      })
    })
  }
  return ops
}

function queryOptsToLevelOpts (opts) {
  const { schema, prop, reverse, limit, value, gt, gte, lt, lte } = opts
  const lvlopts = { reverse, limit }
  const key = schema + CHAR_SPLIT + prop + CHAR_SPLIT
  lvlopts.gt = key + CHAR_SPLIT
  lvlopts.lt = key + CHAR_END
  if (value) {
    lvlopts.gt = key + value + CHAR_SPLIT
    lvlopts.lt = key + value + CHAR_SPLIT + CHAR_END
  } else if (gt) {
    lvlopts.gt = key + gt + CHAR_SPLIT
    lvlopts.lt = key + gt + CHAR_END
  } else if (gte) {
    lvlopts.gte = key + gte + CHAR_SPLIT
    lvlopts.lt = key + gte + CHAR_END
  }
  if (lt) {
    lvlopts.lt = key + lt + CHAR_START
  } else if (lte) {
    lvlopts.lt = undefined
    lvlopts.lte = key + lte + CHAR_END
  }
  return lvlopts
}

function transform () {
  return through.obj(function (row, enc, next) {
    const decoded = decodeNode(row)
    this.push(decoded)
    next()
  })
}

function decodeNode (node) {
  const { key, value: seq } = node
  const [schema, prop, value, id, source] = key.split(CHAR_SPLIT)
  return { schema, id, key: source, seq, params: { prop, value } }
}

// function bucketsBySchema (msgs) {
//   const buckets = {}
//   // Sort into buckets by schema.
//   for (const msg of msgs) {
//     const { schema } = msg
//     if (!buckets[schema]) buckets[schema] = []
//     buckets[schema].push(msg)
//   }
//   return buckets
// }

// function loadAllSchemas (cstore, schemanames, cb) {
//   const schemas = {}
//   let pending = schemanames.length
//   schemanames.forEach(schemaname => cstore.getSchema(schemaname, onschema))
//   function onschema (err, schema) {
//     if (!err && schema) schemas[schema.name] = schema
//     if (--pending === 0) cb(null, schemas)
//   }
// }
// function msgToOps (schema, msg) {
//   const ops = []
//   const { id, key: source, seq, schema: schemaName, value } = msg
//   if (!schema || !schema.properties) return ops
//   // TODO: Recursive?
//   for (const [prop, def] of Object.entries(schema.properties)) {
//     // Only care for props that want to be indexed and are not undefined.
//     if (!def.index) continue
//     if (typeof value[prop] === 'undefined') continue

//     const ikey = [schemaName, prop, value[prop], id, source].join(CHAR_SPLIT)
//     const ivalue = seq

//     ops.push({
//       type: 'put',
//       key: ikey,
//       value: ivalue
//     })
//   }
//   return ops
// }


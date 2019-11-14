const through = require('through2')

const { CHAR_END, CHAR_SPLIT, CHAR_START } = require('../lib/constants')

module.exports = function indexedView (lvl, cstore) {
  return {
    map (msgs, next) {
      const buckets = bucketsBySchema(msgs)
      loadAllSchemas(cstore, Object.keys(buckets), onschemas)

      function onschemas (err, schemas) {
        if (err) return // TODO
        const ops = []
        Object.entries(buckets).forEach(([schemaname, msgs]) => {
          // Skip for which we don't have a schema.
          if (!schemas[schemaname]) return
          msgs.forEach(msg => {
            ops.push(...msgToOps(schemas[schemaname], msg))
          })
        })
        // Now ops is filled.
        lvl.batch(ops, next)
      }
    },

    api: {
      query (kappa, opts, cb) {
        // const { schema, prop, value, gt, lt, gte, lte, reverse, limit } = opts
        const proxy = transform()

        if (!opts.schema || !opts.prop) {
          proxy.destroy(new Error('schema and prop are required.'))
        } else {
          cstore.expandSchemaName(opts.schema, (err, schemaname) => {
            if (err) return proxy.destroy(new Error('Invalid schema name.'))
            opts.schema = schemaname
            const lvlopts = queryOptsToLevelOpts(opts)
            lvl.createReadStream(lvlopts).pipe(proxy)
          })
        }

        return proxy
      }
    }
  }
}

function bucketsBySchema (msgs) {
  const buckets = {}
  // Sort into buckets by schema.
  for (const msg of msgs) {
    const { schema } = msg
    if (!buckets[schema]) buckets[schema] = []
    buckets[schema].push(msg)
  }
  return buckets
}

function loadAllSchemas (cstore, schemanames, cb) {
  const schemas = {}
  let pending = schemanames.length
  schemanames.forEach(schemaname => cstore.getSchema(schemaname, onschema))
  function onschema (err, schema) {
    if (!err && schema) schemas[schema.name] = schema
    if (--pending === 0) cb(null, schemas)
  }
}

function msgToOps (schema, msg) {
  const ops = []
  const { id, source, seq, schema: schemaName, value } = msg
  // TODO: Recursive?
  for (const [prop, def] of Object.entries(schema.properties)) {
    // Only care for props that want to be indexed and are not undefined.
    if (!def.index) continue
    if (typeof value[prop] === 'undefined') continue

    const ikey = [schemaName, prop, value[prop], id, source].join(CHAR_SPLIT)
    const ivalue = seq

    ops.push({
      type: 'put',
      key: ikey,
      value: ivalue
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
    this.push(decodeNode(row))
    next()
  })
}

function decodeNode (node) {
  const { key, value: seq } = node
  const [schema, prop, value, id, source] = key.split(CHAR_SPLIT)
  return { schema, prop, value, id, source, seq }
}

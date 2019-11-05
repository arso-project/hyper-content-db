const { Transform } = require('stream')
const charwise = require('charwise')

const { CHAR_END, CHAR_SPLIT, CHAR_START } = require('../lib/constants')

module.exports = schemaView

function schemaView (db, cstore) {
  function map (msgs, next) {
    const buckets = {}
    const ops = []

    // Sort into buckets by schema.
    for (const msg of msgs) {
      const { schema } = msg
      if (!buckets[schema]) buckets[schema] = []
      buckets[schema].push(msg)
    }

    // Load all schemas.
    const schemas = {}
    let pending = Object.keys(buckets).length
    Object.keys(buckets).forEach(schema => cstore.getSchema(schema, onschema))

    function onschema (err, schema) {
      if (!err && schema) schemas[schema.name] = schema
      if (--pending === 0) finish()
    }

    function finish () {
      Object.keys(schemas).forEach(schemaname => {
        buckets[schemaname].forEach(msg => {
          mapMessage(schemas[schemaname], msg)
        })
      })

      // Now ops is filled.
      db.batch(ops, () => {
        next()
      })
    }

    function mapMessage (schema, msg) {
      const { id, source, seq, schema: schemaName, value } = msg
      // TODO: Recursive?
      for (const [prop, def] of Object.entries(schema.properties)) {
        if (!def.index) continue
        if (typeof value[prop] === 'undefined') continue

        const ikey = [schemaName, prop, value[prop], id, source].join(CHAR_SPLIT)
        // const ikey = `${schemaName}|${prop}|${value[prop]}` +
        //   CHAR_SPLIT +
        //   `${id}|${source}`

        const ivalue = seq

        ops.push({
          type: 'put',
          key: ikey,
          value: ivalue
        })
      }
    }
  }

  const api = {
    query (kappa, opts, cb) {
      // const example = {
      //   schema: 'arso.xyz/Book',
      //   prop: 'publicatenDate',
      //   value: '2018-11-12--......',
      //   gte: '2018-11-12',
      //   lte: '2019-01-01',
      //   reverse: true,
      //   limit: 10
      // }

      const proxy = new Transform({
        objectMode: true,
        transform (row, enc, next) {
          this.push(decodeNode(row))
          next()
        }
      })

      setImmediate(init)

      return proxy

      function init () {
        if (!opts.schema || !opts.prop) return proxy.destroy(new Error('schema and prop are required.'))
        cstore.expandSchemaName(opts.schema, (err, name) => {
          if (err) return proxy.destroy(new Error('Invalid schema name.'))
          opts.schema = name
          run()
        })
      }

      // const { schema, prop, value, gt, lt, gte, lte, reverse, limit } = opts

      function run () {
        const lvlopts = {
          reverse: opts.reverse,
          limit: opts.limit
        }
        const key = opts.schema + CHAR_SPLIT + opts.prop + CHAR_SPLIT
        lvlopts.gt = key + CHAR_SPLIT
        lvlopts.lt = key + CHAR_END
        if (opts.value) {
          lvlopts.gt = key + opts.value + CHAR_SPLIT
          lvlopts.lt = key + opts.value + CHAR_SPLIT + CHAR_END
        } else if (opts.gt) {
          lvlopts.gt = key + opts.gt + CHAR_SPLIT
          lvlopts.lt = key + opts.gt + CHAR_END
        } else if (opts.gte) {
          lvlopts.gte = key + opts.gte + CHAR_SPLIT
          lvlopts.lt = key + opts.gte + CHAR_END
        }
        if (opts.lt) {
          lvlopts.lt = key + opts.lt + CHAR_START
        } else if (opts.lte) {
          lvlopts.lt = undefined
          lvlopts.lte = key + opts.lte + CHAR_END
        }

        const rs = db.createReadStream(lvlopts)

        rs.pipe(proxy)
      }
    }
  }

  return {
    map,
    api
  }
}

function decodeNode (node) {
  const { key, value: seq } = node
  const [schema, prop, value, id, source] = key.split(CHAR_SPLIT)
  return { schema, prop, value, id, source, seq }
}

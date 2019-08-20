const { Transform } = require('stream')

const { CHAR_END, CHAR_SPLIT, CHAR_START } = require('../lib/constants')

module.exports = schemaView

function schemaView (db, cstore) {
  async function map (msgs, next) {
    let bins = {}
    const ops = []
    const proms = []

    for (let msg of msgs) {
      const { schema: name } = msg
      if (!bins[name]) {
        bins[name] = { msgs: [] }
        proms.push(new Promise(resolve => {
          cstore.getSchema(name, (err, schema) => {
            bins[name].schema = schema
            resolve()
          })
        }))
      }
      bins[name].msgs.push(msg)
    }

    // Wait until all schemas are loaded.
    await Promise.all(proms)

    Object.values(bins).forEach(bin => {
      // Filter out messages without a schema.
      if (!bin.schema) return
      bin.msgs.forEach(msg => mapMessage(bin.schema, msg))
    })

    db.batch(ops, () => {
      next()
    })

    function mapMessage (schema, msg) {
      const { id, source, seq, schema: schemaName, value } = msg
      for (let [name, def] of Object.entries(schema.properties)) {
        if (!def.index) continue
        if (typeof value[name] === 'undefined') continue

        const ikey = `${schemaName}|${name}|${value[name]}` +
          CHAR_SPLIT +
          `${id}|${source}`

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
    query (kcore, opts, cb) {
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

      process.nextTick(init)

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
        const key = `${opts.schema}|${opts.prop}|`
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

      // TODO: continue...
    }
  }

  return {
    map,
    api
  }
}

function decodeNode (node) {
  let { key, value: seq } = node
  let [path, rec] = key.split(CHAR_SPLIT)
  let [schema, prop, value] = path.split('|')
  let [id, source] = rec.split('|')
  return { schema, prop, value, id, source, seq }
}

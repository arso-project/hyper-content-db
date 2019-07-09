const { Transform } = require('stream')

const { CHAR_END, CHAR_SPLIT, CHAR_START } = require('../constants')

module.exports = schemaView

function schemaView (db, cstore) {
  function map (msg, next) {
    const ops = []
    const { id, source, seq, schema, value } = msg
    // TODO: This should not run on each map cycle.
    cstore.getSchema(schema, (err, schemadef) => {
      // console.log('get', err, schema)
      if (err || !schemadef) return
      Object.entries(schemadef.properties).forEach(([name, def]) => {
        if (def.index) {
          if (typeof value[name] === 'undefined') return
          const ikey = `${schema}|${name}|${value[name]}` + CHAR_SPLIT + `${id}|${source}`
          const ivalue = seq
          ops.push({
            type: 'put',
            key: ikey,
            value: ivalue
          })
        }
      })
      console.log('MAP', ops)
      next(ops)
    })
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

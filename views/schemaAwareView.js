const END = '\uffff'
const SPLIT = '\u0000'

function schemaView (db, cstore) {
  function map (msg) {
    const ops = []
    const { id, source, seq, schema, value } = msg
    cstore.getSchema(schema, (err, schema) => {
      if (err || !schema) return
      Object.entries(schema.properties).forEach((name, def) => {
        if (def.index) {
          if (typeof value[name] === 'undefined') return
          const ikey = `${schema}|${name}|${value[name]}` + SPLIT + `${id}|${source}`
          const ivalue = seq
          ops.push({
            type: 'put',
            key: ikey,
            value: ivalue
          })
        }
      })
    })
  }
  const api = {
    query (kcore, opts, cb) {
      const example = {
        schema: 'arso.xyz/Book',
        prop: 'publicatenDate',
        value: '2018-11-12--......',
        gte: '2018-11-12',
        lte: '2019-01-01',
        reverse: true,
        limit: 10
      }
      const { schema, prop, value, gte, lte, reverse, limit } = opts
      const ikey = `${schema}|${prop}|`

      // TODO: continue...
    }
  }
  return {
    map
  }
}

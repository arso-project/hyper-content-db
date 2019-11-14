const through = require('through2')
const keyEncoding = require('charwise')

const INDEXES = {
  is: ['id', 'schema', 'source'],
  si: ['schema', 'id', 'source']
}

module.exports = function recordView (lvl) {
  return {
    map (msgs, next) {
      const ops = []
      msgs.forEach(msg => {
        if (!validate(msg)) return
        ops.push(...msgToOps(msg))
      })
      lvl.batch(ops, next)
    },

    api: {
      all (kappa) {
        const rs = lvl.createReadStream({
          gte: ['is'],
          lte: ['is', undefined],
          keyEncoding
        })
        return rs.pipe(transform(INDEXES.is))
      },

      get (kappa, opts) {
        if (!opts) return this.all()
        const { schema, id, source } = opts
        let rs
        if (schema && !id) rs = this.bySchema(schema)
        else if (!schema && id) rs = this.byId(id)
        else rs = this.byIdAndSchema(id, schema)

        if (source) return rs.pipe(filterSource(source))
        else return rs
      },

      bySchema (kappa, schema) {
        const rs = lvl.createReadStream({
          gte: ['si', schema],
          lte: ['si', schema, undefined],
          keyEncoding
        })
        return rs.pipe(transform())
      },

      byId (kappa, id) {
        const rs = lvl.createReadStream({
          gte: ['is', id],
          lte: ['is', id, undefined],
          keyEncoding
        })
        return rs.pipe(transform())
      },

      byIdAndSchema (kappa, id, schema) {
        const rs = lvl.createReadStream({
          gte: ['is', id, schema],
          lte: ['is', id, schema, undefined],
          keyEncoding
        })
        return rs.pipe(transform())
      }
    }
  }
}

function validate (msg) {
  return msg.id && msg.schema && msg.source && typeof msg.seq !== 'undefined'
}

function msgToOps (msg) {
  const ops = []
  const value = msg.seq || 0
  const shared = { type: 'put', value, keyEncoding }
  Object.entries(INDEXES).forEach(([key, fields]) => {
    fields = fields.map(field => msg[field])
    ops.push({
      key: [key, ...fields],
      ...shared
    })
  })
  return ops
}

function transform () {
  return through.obj(function (row, enc, next) {
    const { key, value: seq } = row
    const idx = key.shift()
    const index = INDEXES[idx]
    const record = { seq }
    for (let i = 0; i < key.length; i++) {
      record[index[i]] = key[i]
    }
    this.push(record)
    next()
  })
}

function filterSource (source) {
  return through.obj(function (row, enc, next) {
    if (row.source === source) this.push(row)
    next()
  })
}

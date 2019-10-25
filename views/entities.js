const through = require('through2')
const charwise = require('charwise')

module.exports = recordView

const keyEncoding = charwise

const indexes = {
  is: ['id', 'schema', 'source'],
  si: ['schema', 'id', 'source']
}

function validate (msg) {
  return msg.id && msg.schema && msg.source && typeof msg.seq !== 'undefined'
}

function recordView (db) {
  const view = {
    map (msgs, next) {
      // console.log('ldb MSGS', msgs)
      const ops = []
      msgs.forEach(msg => {
        if (!validate(msg)) return
        const { id, schema, seq, source } = msg
        const value = seq || 0
        const type = 'put'
        const _opts = { type, value, keyEncoding }
        ops.push({
          key: ['is', id, schema, source],
          ..._opts
        })
        ops.push({
          key: ['si', schema, id, source],
          ..._opts
        })
      })
      // console.log('ldb BATCH', ops)
      db.batch(ops, next)
    },

    api: {
      all (kcore) {
        const rs = db.createReadStream({
          gte: ['is'],
          lte: ['is', undefined],
          keyEncoding
        })
        return rs.pipe(transform(indexes.is))
      },

      get (kcore, opts) {
        const { schema, id, source } = opts
        let rs
        if (schema && !id) rs = this.bySchema(schema)
        else if (!schema && id) rs = this.byId(id)
        else rs = this.byIdAndSchema(id, schema)

        if (source) {
          return rs.pipe(through.obj(function (row, enc, next) {
            if (row.source === source) this.push(row)
            next()
          }))
        } else {
          return rs
        }
      },

      bySchema (kcore, schema) {
        const rs = db.createReadStream({
          gte: ['si', schema],
          lte: ['si', schema, undefined],
          keyEncoding
        })
        return rs.pipe(transform(indexes.si))
      },

      byId (kcore, id) {
        const rs = db.createReadStream({
          gte: ['is', id],
          lte: ['is', id, undefined],
          keyEncoding
        })
        return rs.pipe(transform(indexes.is))
      },

      byIdAndSchema (kcore, id, schema) {
        const rs = db.createReadStream({
          gte: ['is', id, schema],
          lte: ['is', id, schema, undefined],
          keyEncoding
        })
        return rs.pipe(transform(indexes.is))
      }
    }
  }

  return view
}

function transform (index) {
  return through.obj(function (row, enc, next) {
    const { key, value: seq } = row
    const idx = key.shift()
    const index = indexes[idx]
    const record = { seq }
    for (let i = 0; i < key.length; i++) {
      record[index[i]] = key[i]
    }
    this.push(record)
    next()
  })
}

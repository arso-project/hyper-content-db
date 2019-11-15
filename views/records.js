const through = require('through2')
const keyEncoding = require('charwise')
const { opsForRecords } = require('./helpers')

const INDEXES = {
  is: ['id', 'schema', 'key'],
  si: ['schema', 'id', 'key']
}

module.exports = function recordView (lvl, db) {
  return {
    name: 'records',
    map (msgs, next) {
      opsForRecords(db, msgs, putOps, (err, ops) => {
        if (err) return next(err)
        lvl.batch(ops, next)
      })
    },

    api: {
      all (kappa, cb) {
        return lvl.createReadStream({
          gte: ['is'],
          lte: ['is', undefined],
          keyEncoding
        }).pipe(transform(INDEXES.is))
      },

      get (kappa, req) {
        if (!req) return this.all()
        if (typeof req === 'string') req = { id: req }
        let { schema, id, key, seq } = req
        schema = db.schemas.resolveName(schema)
        let rs
        if (schema && !id) rs = this.bySchema(schema)
        else if (!schema && id) rs = this.byId(id)
        else rs = this.byIdAndSchema(id, schema)

        if (key) rs = rs.pipe(filterSource(key, seq))
        return rs
      },

      bySchema (kappa, schema) {
        schema = db.schemas.resolveName(schema)
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
        schema = db.schemas.resolveName(schema)
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
  const result = msg.id && msg.schema && msg.key && typeof msg.seq !== 'undefined'
  return result
}

function putOps (msg, db) {
  const ops = []
  if (!validate(msg)) return ops
  const value = msg.seq || 0
  const shared = { value, keyEncoding }
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

function filterSource (key, seq) {
  return through.obj(function (row, enc, next) {
    if (row.key === key) {
      if (!seq || seq === row.seq) this.push(row)
    }
    next()
  })
}

// function recordOps (db, record, cb) {
//   db.kappa.api.kv.isLinked(record, (err, isOutdated) => {
//     // linked records are outdated/overwritten, nothing to do here.
//     if (err || isOutdated) return cb(err, [])
//     // check if we have to delete other records.
//     delOps(db, record, (err, ops = []) => {
//       if (err) return cb(err)
//       // finally, add the put itself.
//       if (!record.delete) ops.push(...putOps(record))
//       cb(err, ops)
//     })
//   })
// }

// function putOps (msg, op = 'put') {
//   const ops = []
//   const value = msg.seq || 0
//   const shared = { type: op, value, keyEncoding }
//   Object.entries(INDEXES).forEach(([key, fields]) => {
//     fields = fields.map(field => msg[field])
//     ops.push({
//       key: [key, ...fields],
//       ...shared
//     })
//   })
//   return ops
// }

// function delOps (db, record, cb) {
//   const ops = []
//   if (record.delete) {
//     ops.push(...putOps(record, 'del'))
//   }
//   let pending = 1
//   if (record.links) {
//     pending += record.links.length
//     record.links.forEach(link => {
//       db.loadLink(link, (err, record) => {
//         if (!err && record) ops.push(...putOps(record, 'del'))
//         done()
//       })
//     })
//   }
//   done()
//   function done () {
//     if (--pending === 0) cb(null, ops)
//   }
// }

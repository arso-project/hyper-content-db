const through = require('through2')
const keyEncoding = require('charwise')
const { opsForRecords } = require('./helpers')
const Live = require('level-live')
// const collect = require('stream-collector')

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
        return query(lvl, {
          gte: ['is'],
          lte: ['is', undefined],
          keyEncoding
        })
      },

      get (kappa, req) {
        const self = kappa.view.records
        if (!req) return self.all()
        if (typeof req === 'string') req = { id: req }
        let { schema, id, key, seq } = req
        if (schema) schema = db.schemas.resolveName(schema)
        let rs
        if (schema && !id) rs = self.bySchema(schema, req)
        else if (!schema && id) rs = self.byId(id, req)
        else rs = self.byIdAndSchema(id, schema, req)

        if (key) rs = rs.pipe(filterSource(key, seq))
        return rs
      },

      bySchema (kappa, schema, opts) {
        schema = db.schemas.resolveName(schema)
        const rs = query(lvl, {
          ...opts,
          gte: ['si', schema],
          lte: ['si', schema, undefined]
        })
        return rs
      },

      byId (kappa, id, opts) {
        const rs = query(lvl, lvl, {
          ...opts,
          gte: ['is', id],
          lte: ['is', id, undefined]
        })
        return rs
      },

      byIdAndSchema (kappa, id, schema, opts) {
        schema = db.schemas.resolveName(schema)
        return query(lvl, {
          ...opts,
          gte: ['is', id, schema],
          lte: ['is', id, schema, undefined]
        })
      },
    }
  }
}

function query (db, opts) {
  opts.keyEncoding = keyEncoding
  let rs
  if (opts.live) {
    rs = new Live(db, opts)
  } else {
    rs = db.createReadStream(opts)
  }
  return rs.pipe(transform())
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
    const { key, value: seq, type } = row
    const idx = key.shift()
    const index = INDEXES[idx]
    const record = { seq: Number(seq), type }
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

const { CHAR_END } = require('../lib/constants')
const through = require('through2')

module.exports = entityView

function entityView (db) {
  // const idSchema = sub(db, 'is')
  // const schemaId = sub(db, 'si')
  return {
    map (msgs, next) {
      // console.log('ldb MSGS', msgs)
      const ops = []
      msgs.forEach(msg => {
        const { id, schema, seq, source } = msg
        const value = `${source}@${seq}`
        const type = 'put'
        ops.push({
          type,
          key: `is|${id}|${schema}`,
          value
        })
        ops.push({
          type,
          key: `si|${schema}|${id}`,
          value
        })
      })
      // console.log('ldb BATCH', ops)
      db.batch(ops, next)
    },
    api: {
      all (kcore) {
        const rs = db.createReadStream({
          gt: 'is|',
          lt: 'is|' + CHAR_END
        })

        return rs.pipe(through.obj(function (row, enc, next) {
          // console.log('ldb GET', row)
          let [id, schema] = row.key.split('|').slice(1)
          let [source, seq] = row.value.split('@')
          this.push({ id, schema, source, seq })
          next()
        }))
      },
      allWithSchema (kcore, opts) {
        const schema = opts.schema
        let rs = db.createReadStream({
          gt: `si|${schema}|`,
          lt: `si|${schema}|` + CHAR_END
        })
        return rs.pipe(through.obj(function (row, enc, next) {
          let [schema, id] = row.key.split('|').slice(1)
          let [source, seq] = row.value.split('@')
          this.push({ id, schema, source, seq })
          next()
        }))
      },
      allWithId (kcore, opts) {
        const id = opts.id
        let rs = db.createReadStream({
          gt: `is|${id}|`,
          lt: `is|${id}|` + CHAR_END
        })
        return rs.pipe(through.obj(function (row, enc, next) {
          let [id, schema] = row.key.split('|').slice(1)
          let [source, seq] = row.value.split('@')
          this.push({ id, schema, source, seq })
          next()
        }))
      }
    }
  }
}

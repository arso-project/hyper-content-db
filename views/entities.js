const { CHAR_END } = require('../constants')
const through = require('through2')
const pump = require('pump')

module.exports = entityView

function entityView (db) {
  return {
    map (msgs, next) {
      // console.log('ldb MSGS', msgs)
      const ops = []
      msgs.forEach(msg => {
        const { id, schema, seq, source } = msg
        let value = `${source}@${seq}`
        let type = 'put'
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
        let rs = db.createReadStream({
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
      allWithSchema (kcore, schema) {
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
      }
    }
  }
}

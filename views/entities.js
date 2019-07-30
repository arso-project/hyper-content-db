const { CHAR_END } = require('../constants')

module.exports = entityView

function entityView (db) {
  return {
    map (msgs, next) {
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
      db.batch(ops, next)
    },
    api: {
      all (kcore, cb) {
        let ids = {}
        let rs = db.createReadStream({
          gt: 'is|',
          lt: 'is|' + CHAR_END
        })
        rs.on('data', row => {
          let [id, schema] = row.key.split('|').slice(1)
          let [source, seq] = row.value.split('@')
          ids[id] = ids[id] || []
          ids[id].push({ schema, source, seq })
        })
        rs.on('end', () => cb(null, ids))
      },
      allWithSchema (kcore, schema, cb) {
        let ids = {}
        let rs = db.createReadStream({
          gt: `si|${schema}|`,
          lt: `si|${schema}|` + CHAR_END
        })
        rs.on('data', row => {
          let id = row.key.split('|').pop()
          let [source, seq] = row.value.split('@')
          ids[id] = ids[id] || []
          ids[id].push({ schema, source, seq })
        })
        rs.on('end', () => cb(null, ids))
      }
    }
  }
}

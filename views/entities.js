const { CHAR_END } = require('../constants')

module.exports = entityView

function entityView (db) {
  return {
    map (msg, next) {
      const { id, schema, seq, source } = msg
      let value = `${source}@${seq}`
      let rows = [
        [`is|${id}|${schema}`, value],
        [`si|${schema}|${id}`, value]
      ]
      next(rows.map(r => ({ type: 'put', key: r[0], value: r[1] })))
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

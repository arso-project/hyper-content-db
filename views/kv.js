const umkv = require('unordered-materialized-kv')
const { keyseq } = require('../lib/util')

module.exports = function kvView (lvl, db) {
  const kv = umkv(lvl, {
    onupdate, onremove
  })

  return {
    name: 'kv',
    map (msgs, next) {
      const ops = msgs.map(record => ({
        key: record.id,
        id: keyseq(record),
        links: record.links
      }))
      kv.batch(ops, next)
    },
    api: {
      getLinks (kappa, record, cb) {
        kv.get(record.id, cb)
      },
      isLinked (kappa, record, cb) {
        kv.isLinked(keyseq(record), cb)
      }
    }
  }

  function onupdate (msg) {
    // console.log('onupdate', msg)
  }
  function onremove (msg) {
    // console.log('onremove', msg)
  }
}

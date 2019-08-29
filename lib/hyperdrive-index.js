const hypertrieIndex = require('hypertrie-index')
const inspect = require('inspect-custom-symbol')
const StatEncoder = require('hyperdrive/lib/stat')

module.exports = hyperdriveIndex

function hyperdriveIndex (drive, opts) {
  const htiOpts = {
    map,
    batchSize: opts.batchSize,
    prefix: opts.prefix,
    storeState: opts.storeState,
    fetchState: opts.fetchState,
    // This should not really be needed, but the hypertrie
    // logic does not directly comply to the interface expected
    // by the codecs module. TODO: PR to hyperdrive.
    valueEncoding: {
      encode: stat => stat.encode(),
      decode: StatEncoder.decode
    },
    transformNode: true
  }

  const index = hypertrieIndex(drive._db.trie, htiOpts)

  return index

  function map (msgs, done) {
    asyncFilterMap({
      data: msgs,
      filter: msg => msg.value.isFile(),
      map: _map,
      done: _done
    })

    function _map (msg, next) {
      msg.source = drive.key
      overrideInspect(msg)

      if (!opts.readFile) return next(null, msg)

      // const checkout = drive.checkout(msg.seq)
      drive.readFile(msg.key, (err, data) => {
        msg.fileContent = data
        next(err, msg)
      })
    }

    function _done (err, msgs) {
      // todo: handle err better?
      if (err) index.emit('error', err)
      if (msgs.length) opts.map(msgs, done)
      else done()
    }
  }
}

function asyncFilterMap (opts) {
  const { data, filter, map, done } = opts

  let pending = data.length
  let nextMsgs = []
  let errors = []

  if (!pending) return done(null, data)
  data.forEach((msg, i) => {
    if (!filter(msg)) return finish(null, msg)
    map(msg, finish)
  })

  function finish (err, msg) {
    if (err) errors.push(err)
    if (typeof msg !== 'undefined') nextMsgs.push(msg)
    if (--pending === 0) done(errors.length ? errors : null, nextMsgs)
  }
}

function overrideInspect (msg) {
  const keys = ['seq', 'key', 'value', 'source', 'fileContent']
  msg[inspect] = function (depth, opts) {
    return keys.reduce((agg, key) => {
      agg[key] = msg[key]
      return agg
    }, {})
  }
}

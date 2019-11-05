const tape = require('tape')
const cstore = require('..')
const ram = require('random-access-memory')
const collect = require('collect-stream')

tape('replication and sources', async t => {
  const store1 = cstore(ram)
  var store2, id

  const schema = 'arso.xyz/Entity'
  const record1 = { title: 'world', tags: ['foo', 'bar'] }
  const record2 = { title: 'moon', tags: ['bar', 'baz'] }

  await runAll([
    cb => store1.ready(cb),
    cb => {
      store2 = cstore(ram, store1.key)
      store2.ready(cb)
    },
    cb => store1.put({ schema, value: record1 }, (err, _id) => {
      id = _id
      cb(err)
    }),
    cb => store2.put({ schema, id, value: record2 }, cb),

    // cb => {
    //   console.log('store1 world: ', store1.key.toString('hex'))
    //   console.log('store1 local: ', store1.localKey.toString('hex'))
    //   console.log('store2 world: ', store2.key.toString('hex'))
    //   console.log('store2 local: ', store2.localKey.toString('hex'))
    //   cb()
    // },

    // First replication. Note that this will keep running.
    cb => replicate(store1, store2, cb),

    cb => {
      store2.get({ schema, id }, (err, records) => {
        t.error(err, 'no err')
        t.equal(records.length, 2)
        t.deepEqual(records.map(r => r.value.title).sort(), ['moon', 'world'], 'titles ok')
        cb()
      })
    },

    // the primary source has not added store2's local writer
    cb => store1.get({ schema, id }, (err, records) => {
      t.error(err)
      t.equal(records.length, 1, '1 record before merge')
      cb()
    }),

    cb => store1.addSource(store2.localKey, cb),

    cb => store1.get({ schema, id }, (err, records) => {
      console.log(store1.multidrive._sources.size)
      t.error(err)
      t.equal(records.length, 2, 'two records after merge')
      cb()
    }),

    cb => {
      store1.kappa.ready(() => {
        const rs = store1.api.entities.all()
        collect(rs, (err, records) => {
          t.error(err)
          t.equal(records.length, 2)
          cb()
        })
      })
    },

    cb => {
      t.end()
    }
  ])
})

function replicate (a, b, cb) {
  cb = once(cb)
  var stream = a.replicate(true, { live: true })
  stream.pipe(b.replicate(false, { live: true })).pipe(stream)
  setImmediate(cb)
}

function once (fn) {
  let didrun = false
  return (...args) => {
    if (didrun) return
    didrun = true
    return fn(...args)
  }
}

function runAll (ops) {
  return new Promise((resolve, reject) => {
    runNext(ops.shift())
    function runNext (op, previousResult) {
      op((err, result) => {
        if (err) return reject(err)
        let next = ops.shift()
        if (!next) return resolve()
        return runNext(next, result)
      }, previousResult)
    }
  })
}

function key (k) {
  return k.toString('hex').slice(0, 4)
}

function contentFeed (drive) {
  const contentState = drive._contentStates.get(drive._db)
  if (!contentState) return false
  return contentState.feed
}

function logDrive (drive, name) {
  const cf = contentFeed(drive)
  name = name || 'Hyperdrive'
  console.log(`%s:
    key %s
    ckey %s
    writable %s
    version %s
    contentLength %s`, name, key(drive.key), cf && key(cf.key), drive.writable, drive.version, cf && cf.length)
}

const tape = require('tape')
const cstore = require('..')
const ram = require('random-access-memory')

tape('replication and sources', async t => {
  const store1 = cstore(ram)
  var store2, id, store2localWriterKey

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

    cb => store2.writer((err, drive) => {
      store2localWriterKey = drive.key
      cb(err)
    }),

    // First replication. Note that this will keep running.
    cb => replicate(store1, store2, cb),
    cb => {
      store2.get({ schema, id }, (err, records) => {
        t.error(err, 'no err')
        t.equal(records.length, 2)
        t.equal(records[0].id, id)
        t.equal(records[1].id, id)
        t.equal(records[0].value.title, 'world')
        t.equal(records[1].value.title, 'moon')
        cb()
      })
    },

    // the primary source has not added store2's local writer
    cb => store1.get({ schema, id }, (err, records) => {
      t.error(err)
      t.equal(records.length, 1)
      cb()
    }),

    cb => store1.addSource(store2localWriterKey, cb),

    cb => setTimeout(cb, 100),

    cb => store1.get({ schema, id }, (err, records) => {
      t.error(err)
      t.equal(records.length, 2)
      cb()
    }),

    cb => {
      store1.sources(drives => {
        cb()
      })
    }
  ])

  t.end()
})

function replicate (a, b, cb) {
  cb = once(cb)
  var stream = a.replicate({ live: true })
  stream.pipe(b.replicate()).pipe(stream).on('end', cb)
  setTimeout(() => cb(), 100)
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

// function validateCore(t, core, values) {
//   const ops = values.map((v, idx) => cb => {
//     core.get(idx, (err, value) => {
//       t.error(err, 'no error')
//       t.same(value, values[idx])
//       return cb(null)
//     })
//   })
//   return runAll(ops)
// }

function key (k) {
  return k.toString('hex').slice(0, 2)
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

// function repl (s1, s2, cb) {
//   // const opts = { live: false }
//   const opts = {}
//   // const stream = s1.replicate(opts)
//   // stream.pipe(s2.replicate(opts)).pipe(stream)
//   // stream.on('end', cb)
//   // stream.on('error', err => console.error(err))
//   const dr1 = s1.multidrive.primaryDrive
//   const dr2 = s2.multidrive.primaryDrive
//   logDrive(dr1, 'drive1')
//   logDrive(dr2, 'drive2')
//   s2.writer((err, drive) => logDrive(drive, 'drive2.writer'))
//   const str1 = dr1.replicate()
//   const str2 = dr2.replicate()
//   console.log('stream1', key(str1.id))
//   console.log('stream2', key(str2.id))
//   pump(str1, str2, str1)
//   // str1.on('data', d => console.log('d1', d))
//   // str2.on('data', d => console.log('d2', d))
//   str1.on('end', cb)
//   setTimeout(() => {
//     logDrive(dr1, 'drive1')
//     logDrive(dr2, 'drive2')
//     // console.log(str1)
//     // console.log(str2)
//     cb()
//   }, 200)
// }

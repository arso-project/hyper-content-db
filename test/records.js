const tape = require('tape')
const cstore = require('..')
const ram = require('random-access-memory')

tape('prefix', t => {
  const store1 = cstore(ram)
  const schema = 'arso.xyz/Entity'
  const record1 = { title: 'world', tags: ['foo', 'bar'] }
  let results = []
  store1.use('view', {
    prefix: '.data',
    map (msgs, next) {
      results = [...results, ...msgs]
      next()
    }
  })
  store1.kcore.on('indexed', () => {
    t.equal(results.length, 1, 'one result')
    t.end()
  })
  store1.ready(() => {
    store1.writer((err, drive) => {
      t.error(err)
      drive.writeFile('foo', 'bar', (err) => {
        t.error(err)
        store1.putRecord(schema, cstore.id(), record1, (err, id) => {
          t.error(err)
        })
      })
    })
  })
})

tape('records', t => {
  const store1 = cstore(ram)

  const schema = 'arso.xyz/Entity'
  const record1 = { title: 'world', tags: ['foo', 'bar'] }
  const record2 = { title: 'moon', tags: ['bar', 'baz'] }

  // This creates a record in each source with the same id.
  store1.ready(() => {
    const store2 = cstore(ram, store1.key)
    store1.putRecord(schema, cstore.id(), record1, (err, id1) => {
      t.error(err, 'no err')
      store2.putRecord(schema, id1, record2, err => {
        t.error(err, 'no err')
        replicate(store1, store2, () => {
          store2.getRecords(schema, id1, (err, records) => {
            t.error(err, 'no err')
            t.equal(records.length, 2)
            t.equal(records[0].id, id1)
            t.equal(records[1].id, id1)
            t.equal(records[0].value.title, 'world')
            t.equal(records[1].value.title, 'moon')
            store2.listRecords(schema, (err, list) => {
              t.error(err)
              t.equal(list.length, 1)
              t.equal(list[0], id1)
              t.end()
            })
          })
        })
      })
    })
  })
})

function replicate (a, b, cb) {
  var stream = a.replicate()
  stream.pipe(b.replicate()).pipe(stream).on('end', cb)
}

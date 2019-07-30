const tape = require('tape')
const cstore = require('..')
const ram = require('random-access-memory')
const collect = require('collect-stream')
const L = require('lodash')

tape('prefix', t => {
  const store1 = cstore(ram)
  const schema = 'arso.xyz/Entity'
  const record1 = { title: 'world', tags: ['foo', 'bar'] }
  let results = []
  store1.useRecordView('view', () => ({
    map (msgs, next) {
      results = [...results, ...msgs]
      next()
    },
    indexed () {
      t.equal(results.length, 1, 'one result')
      t.equal(results[0].value.title, 'world', 'value matches')
      t.end()
    }
  }))

  store1.ready(() => {
    store1.writer((err, drive) => {
      t.error(err, 'noerr writer')
      drive.writeFile('foo', 'bar', (err) => {
        t.error(err, 'noerr writeFile')
        store1.put(schema, cstore.id(), record1, (err, id) => {
          t.error(err, 'noerr put')
        })
      })
    })
  })
})

// TODO: Fix replication.
tape.skip('records', t => {
  const store1 = cstore(ram)

  const schema = 'arso.xyz/Entity'
  const record1 = { title: 'world', tags: ['foo', 'bar'] }
  const record2 = { title: 'moon', tags: ['bar', 'baz'] }

  // This creates a record in each source with the same id.
  store1.ready(() => {
    const store2 = cstore(ram, store1.key)
    store1.put(schema, cstore.id(), record1, (err, id1) => {
      t.error(err, 'no err')
      store2.put(schema, id1, record2, err => {
        t.error(err, 'no err')
        replicate(store1, store2, () => {
          console.log('hi')
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

tape('batch', t => {
  const store1 = cstore(ram)
  const schema = 'foo/bar'
  const records = [
    { op: 'put', id: cstore.id(), schema, value: { title: 'hello' } },
    { op: 'put', id: cstore.id(), schema, value: { title: 'world' } },
    { op: 'put', id: cstore.id(), schema, value: { title: 'moon' } }
  ]
  store1.batch(records, (err, ids) => {
    t.error(err)
    t.equal(ids.length, 3)
    store1.listRecords(schema, (err, ids) => {
      t.error(err)
      t.equal(ids.length, 3)
      let data = []
      ids.forEach(id => store1.getRecords(schema, id, collect))
      function collect (err, records) {
        if (err) t.error(err)
        data = [...data, ...records]
        if (data.length === ids.length) finish(data)
      }
    })
  })

  function finish (data) {
    const results = data.map(d => d.value.title).sort()
    const sources = records.map(r => r.value.title).sort()
    t.deepEqual(results, sources, 'results match')
    t.end()
  }
})

tape('batch and get stream', t => {
  const store = cstore(ram)

  const records = [
    {
      op: 'put',
      schema: 'event',
      value: {
        date: new Date(2019, 12, 10),
        title: 'Release'
      }
    },
    {
      op: 'put',
      schema: 'event',
      value: {
        date: new Date(2019, 9, 2),
        title: 'Party'
      }
    }
  ]

  const stream = store.createBatchStream()
  stream.write(records)

  stream.on('data', data => console.log('batch result', data))

  store.on('indexed', query)

  stream.on('error', err => t.error(err))

  function query () {
    const queryStream = store.api.entities.all()
    const getTransform = store.createGetStream()
    const resultStream = queryStream.pipe(getTransform)
    collect(resultStream, (err, data) => {
      t.error(err)
      data = L.orderBy(data, r => r.value.title)
      t.equal(data.length, 2)
      t.equal(data[0].value.title, 'Party')
      t.equal(data[1].value.title, 'Release')
      t.end()
    })
  }
})

function replicate (a, b, cb) {
  var stream = a.replicate({ live: false })
  stream.pipe(b.replicate()).pipe(stream).on('end', cb)
}

const tape = require('tape')
const cstore = require('..')
const ram = require('random-access-memory')
const view = require('../views/schema-aware.js')

tape('schema view', t => {
  const store1 = cstore(ram)

  const schema = 'post'

  store1.putSchema(schema, {
    properties: {
      title: {
        type: 'string',
        index: true
      },
      date: {
        type: 'string',
        index: true
      }
    }
  })

  store1.useLevelView('idx', view)

  let rows = [
    { title: 'abcd', date: '2019-11' },
    { title: 'abc', date: '2019-12' },
    { title: 'old', date: '2018-07' },
    { title: 'future', date: '2020-01' }
  ]
  let batch = rows.map(value => ({
    op: 'put',
    id: cstore.id(),
    value,
    schema
  }))

  let _run = false

  let runs = 0
  store1.batch(batch, (err, ids) => {
    t.error(err, 'batch')
    store1.on('indexed', () => {
      console.log('INDEXED')
      if (++runs === 2) query()
    })
  })

  function query () {
    if (_run) return
    _run = true
    console.log('run q')

    const queries = [
      {
        name: 'date all',
        q: { schema, prop: 'date' },
        v: ['2018-07', '2019-11', '2019-12', '2020-01']
      },
      {
        name: 'title all',
        q: { schema, prop: 'title' },
        v: ['abc', 'abcd', 'future', 'old']
      },
      {
        name: 'title gte lt',
        q: { schema, prop: 'title', gt: 'abcd', lt: 'h' },
        v: ['abcd', 'future']
      }
    ]
    testQueries(queries, (err) => {
      t.error(err)
      t.end()
    })
  }

  function testQueries (queries, cb) {
    testQuery(queries.shift())

    function testQuery (query) {
      const { name, q, v } = query
      let rs = store1.api.idx.query(q)
      let rows = []
      rs.on('data', d => rows.push(d))
      rs.on('err', err => cb(err))
      rs.on('end', () => {
        t.deepEqual(
          rows.map(r => r.value),
          v,
          name + ': results match'
        )
        if (queries.length) {
          process.nextTick(testQuery, queries.shift())
        } else cb()
      })
    }
  }
})

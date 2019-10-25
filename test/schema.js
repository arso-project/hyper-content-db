const tape = require('tape')
const cstore = require('..')
const ram = require('random-access-memory')
const collect = require('stream-collector')
const through = require('through2')
const view = require('../views/schema-aware.js')
const { runAll } = require('./lib/util')

tape('schema-aware view', t => {

  const schema = 'post'

  const schemadef = {
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
  }

  const rows = [
    { title: 'abcd', date: '2019-11' },
    { title: 'abc', date: '2019-12' },
    { title: 'old', date: '2018-07' },
    { title: 'future', date: '2020-01' }
  ]

  const batch = rows.map(value => ({
    op: 'put',
    value,
    schema
  }))

  const store1 = cstore(ram)
  store1.useRecordView('idx', view)

  runAll([
    cb => store1.putSchema(schema, schemadef, cb),
    cb => store1.batch(batch, cb),
    cb => store1.kappa.ready('idx', cb),
    cb => {
      const queries = [
        {
          name: 'date all',
          query: { schema, prop: 'date' },
          result: ['2018-07', '2019-11', '2019-12', '2020-01']
        },
        {
          name: 'title all',
          query: { schema, prop: 'title' },
          result: ['abc', 'abcd', 'future', 'old']
        },
        {
          name: 'title gte lt',
          query: { schema, prop: 'title', gt: 'abcd', lt: 'h' },
          result: ['abcd', 'future']
        }
      ]
      runAll(queries.map(info => cb => testQuery(info, cb))).then(cb)
    },
    cb => t.end()
  ])

  function testQuery (info, cb) {
    const { name, query, result } = info
    const rs = store1.api.idx.query(query)
      .pipe(store1.createGetStream())
      .pipe(through.obj(function (record, enc, next) {
        this.push(record.value[query.prop])
        next()
      }))
    collect(rs, (err, res) => {
      t.error(err)
      t.deepEqual(res, result, name + ': results match')
      cb()
    })
  }
})

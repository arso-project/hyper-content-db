const tape = require('tape')
const cstore = require('..')
const ram = require('random-access-memory')
const { runAll } = require('./lib/util')

tape('entities', async t => {
  const store1 = cstore(ram)
  const schema = 'arso.xyz/Entity'
  const schema2 = 'arso.xyz/Resource'

  const ids = [cstore.id(), cstore.id(), cstore.id()]

  const records = [
    { op: 'put', id: ids[0], schema, value: { title: 'hello' } },
    { op: 'put', id: ids[1], schema, value: { title: 'hello' } },
    { op: 'put', id: ids[1], schema: schema2, value: { link: 'foo' } },
    { op: 'put', id: ids[2], schema, value: { title: 'moon' } }
  ]

  const ev = store1.api.entities

  runAll([
    cb => {
      store1.batch(records, (err, ids) => {
        t.error(err, 'batch succeeded')
        cb()
      })
    },

    cb => store1.kcore.ready('entities', cb),

    cb => {
      const rs = ev.all()
      collect(rs, (err, rows) => {
        t.error(err)
        t.equal(Object.keys(rows).length, 3, 'row count matches')
        t.equal(rows[ids[1]].length, 2, 'two records for two schemas')
        t.deepEqual(
          rows[ids[1]].map(r => r.schema).sort(),
          [schema, schema2],
          'schemas match'
        )
        cb()
      })
    },

    cb => {
      const rs = ev.bySchema(schema2)
      collect(rs, (err, rows) => {
        // console.log(rows)
        t.error(err)
        t.equal(Object.keys(rows).length, 1, 'count for schema2 matches')
        t.equal(rows[ids[1]][0].schema, schema2, 'schema matches')
        cb()
      })
    },

    cb => {
      const rs = ev.bySchema(schema)
      collect(rs, (err, rows) => {
        t.error(err)
        t.equal(Object.keys(rows).length, 3, 'count for schema1 matches')
        t.deepEqual(Object.keys(rows).sort(), ids.sort(), 'ids match')
        cb()
      })
    },

    cb => t.end()
  ])
})

function collect (stream, cb) {
  const data = {}
  stream.on('data', row => {
    const { id } = row
    data[id] = data[id] || []
    data[id].push(row)
  })
  stream.on('end', () => cb(null, data))
  stream.on('error', err => cb(err))
}

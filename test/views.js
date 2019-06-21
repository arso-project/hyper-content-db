const tape = require('tape')
const cstore = require('..')
const ram = require('random-access-memory')

tape('entity view', t => {
  const store1 = cstore(ram)
  const schema = 'arso.xyz/Entity'
  const schema2 = 'arso.xyz/Resource'

  let ids = [cstore.id(), cstore.id(), cstore.id()]

  const records = [
    { op: 'put', id: ids[0], schema, record: { title: 'hello' } },
    { op: 'put', id: ids[1], schema, record: { title: 'hello' } },
    { op: 'put', id: ids[1], schema: schema2, record: { link: 'foo' } },
    { op: 'put', id: ids[2], schema, record: { title: 'moon' } }
  ]

  store1.batch(records, (err, ids) => {
    t.error(err)
  })

  let missing = 0

  store1.on('indexed', () => {
    const ev = store1.api.entities
    step((done) => ev.all((err, rows) => {
      t.error(err)
      t.equal(Object.keys(rows).length, 3, 'row count matches')
      t.equal(rows[ids[1]].length, 2, 'two records for two schemas')
      t.deepEqual(
        rows[ids[1]].map(r => r.schema).sort(),
        [schema, schema2],
        'schemas match'
      )
      done()
    }))

    step((done) => ev.allWithSchema(schema2, (err, rows) => {
      t.error(err)
      t.equal(Object.keys(rows).length, 1, 'count for schema2 matches')
      t.equal(rows[ids[1]][0].schema, schema2, 'schema matches')
      done()
    }))

    step((done) => ev.allWithSchema(schema, (err, rows) => {
      t.error(err)
      t.equal(Object.keys(rows).length, 3, 'count for schema1 matches')
      t.deepEqual(Object.keys(rows).sort(), ids.sort(), 'ids match')
      done()
    }))
  })

  function step (fn) {
    missing++
    fn((err) => {
      t.error(err)
      if (--missing === 0) t.end()
    })
  }
})

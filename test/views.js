const tape = require('tape')
const cstore = require('..')
const ram = require('random-access-memory')

// function collect (stream, cb) {
//   let buf = []
//   stream.on('data', d => buf.push(d))
//   stream.on('end', () => cb(null, buf))
//   stream.on('error', err => cb(err))
// }

function collectById (stream, cb) {
  let data = {}
  stream.on('data', row => {
    let { id } = row
    data[id] = data[id] || []
    data[id].push(row)
  })
  stream.on('end', () => cb(null, data))
  stream.on('error', err => cb(err))
}

tape('entities', t => {
  const store1 = cstore(ram)
  const schema = 'arso.xyz/Entity'
  const schema2 = 'arso.xyz/Resource'

  let ids = [cstore.id(), cstore.id(), cstore.id()]

  const records = [
    { op: 'put', id: ids[0], schema, value: { title: 'hello' } },
    { op: 'put', id: ids[1], schema, value: { title: 'hello' } },
    { op: 'put', id: ids[1], schema: schema2, value: { link: 'foo' } },
    { op: 'put', id: ids[2], schema, value: { title: 'moon' } }
  ]

  store1.batch(records, (err, ids) => {
    t.error(err, 'batch succeeded')
  })

  const step = stepper(err => {
    t.error(err)
    t.end()
  })

  store1.on('indexed-all', () => {
    const ev = store1.api.entities
    step((done) => {
      const rs = ev.all()
      collectById(rs, (err, rows) => {
        t.error(err)
        t.equal(Object.keys(rows).length, 3, 'row count matches')
        t.equal(rows[ids[1]].length, 2, 'two records for two schemas')
        t.deepEqual(
          rows[ids[1]].map(r => r.schema).sort(),
          [schema, schema2],
          'schemas match'
        )
        done()
      })
    })

    step((done) => {
      const rs = ev.allWithSchema({ schema: schema2 })
      collectById(rs, (err, rows) => {
        console.log(rows)
        t.error(err)
        t.equal(Object.keys(rows).length, 1, 'count for schema2 matches')
        t.equal(rows[ids[1]][0].schema, schema2, 'schema matches')
        done()
      })
    })

    step((done) => {
      const rs = ev.allWithSchema({ schema })
      collectById(rs, (err, rows) => {
        t.error(err)
        t.equal(Object.keys(rows).length, 3, 'count for schema1 matches')
        t.deepEqual(Object.keys(rows).sort(), ids.sort(), 'ids match')
        done()
      })
    })
  })
})

function stepper (cb) {
  let steps = []
  return function step (fn) {
    steps.push(fn)
    if (steps.length === 1) process.nextTick(run)
  }
  function run () {
    const fn = steps.shift()
    fn(done)
  }
  function done (err) {
    if (err) return cb(err)
    process.nextTick(steps.length ? run : cb)
  }
}

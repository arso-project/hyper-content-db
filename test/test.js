const { runAll } = require('./lib/util')
const tape = require('tape')
const Database = require('../db')

const docSchema = {
  properties: {
    title: { type: 'string' },
    body: { type: 'string' },
    tags: { type: 'array', index: true, items: { type: 'string' } }
  }
}

tape('basics', async t => {
  const db = new Database()
  let id1
  await runAll([
    cb => db.ready(cb),
    cb => db.putSchema('doc', docSchema, cb),
    cb => db.put({ schema: 'doc', value: { title: 'hello', body: 'world', tags: ['red'] } }, (err, id) => {
      t.error(err)
      id1 = id
      process.nextTick(cb)
    }),
    cb => {
      db.put({ schema: 'doc', value: { title: 'hi', body: 'mars', tags: ['green'] } }, cb)
    },
    cb => {
      db.put({ schema: 'doc', value: { title: 'hello', body: 'moon', tags: ['green'] }, id: id1 }, cb)
    },
    cb => {
      db.kappa.ready(() => {
        db.loadStream(db.api.records.get({ schema: 'doc' }), (err, records) => {
          t.error(err)
          t.equal(records.length, 2)
          t.deepEqual(records.map(r => r.value.title).sort(), ['hello', 'hi'])
          cb()
        })
      })
    },
    cb => {
      db.loadStream(db.api.indexes.query({ schema: 'doc', prop: 'tags', value: 'green' }), (err, records) => {
        t.deepEqual(records.map(r => r.value.body).sort(), ['mars', 'moon'])
        t.end()
      })
    }
  ])
  t.end()
})

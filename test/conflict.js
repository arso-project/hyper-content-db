const tape = require('tape')
const cstore = require('..')
const ram = require('random-access-memory')
const collect = require('collect-stream')
const L = require('lodash')

const { stepper, once } = require('./lib/util')

tape('conflict', t => {
  const step = stepper(err => {
    t.error(err)
    t.end()
  })

  const schema = 'event'

  let store1, store2

  let ids

  step(cb => {
    store1 = cstore(ram)
    ids = [store1.id(), store1.id()]
    store1.ready(cb)
  })
  step(cb => {
    store2 = cstore(ram, store1.key)
    store2.ready(cb)
  })
  // step(cb => {
  //   let opts = { live: true }
  //   let stream = store1.replicate(opts)
  //   stream.pipe(store2.replicate(opts)).pipe(store1)
  // })
  step(cb => {
    store1.batch([
      { schema, id: ids[0], value: { title: 'first!', slug: 'first' } },
      { schema, id: ids[1], value: { title: 'second!', slug: 'second' } }
    ], cb)
  })
  step((cb, ids1) => {
    t.equal(ids1.length, 2, 'ids1 len 2')
    store2.batch([
      { schema, id: ids[0], value: { title: 'other first', slug: 'first' } },
      { schema, id: ids[1], value: { title: 'other second', slug: 'second' } },
      { schema, value: { title: 'third', slug: 'third' } }
    ], (err, ids2) => cb(err, [ids1, ids2]))
  })
  step('replicate', (cb, [ids1, ids2]) => {
    t.equal(ids2.length, 3, 'ids2 len 3')
    // console.log({ ids1, ids2 })
    // t.deepEqual(ids2.slice(0, 2), ids1)
    replicate(store1, store2, cb)
  })
  step('add source', cb => {
    store1.addSource(store2.localKey, cb)
  })
  step('replicate', cb => replicate(store1, store2, cb))
  step('list', cb => {
    store1.list(schema, (err, list1) => {
      t.error(err)
      store2.list(schema, (err, list2) => cb(err, [list1, list2]))
    })
  })
  step((cb, [list1, list2]) => {
    // console.log('done!')
    t.deepEqual(list1.sort(), list2.sort())
    t.equal(list1.length, 3)
    let rs = store1.createGetStream({ reduce: true })
    list1.forEach(id => rs.write({ id, schema }))
    rs.end(null)
    collect(rs, (err, data) => {
      t.error(err)
      // console.log('RESULT', data)
      t.equal(data.length, 3)
      t.deepEqual(data.map(d => d.value.title).sort(), ['other first', 'other second', 'third'])
      cb()
    })
  })
})

function replicate (a, b, cb) {
  cb = once(cb)
  var stream = a.replicate()
  stream.pipe(b.replicate()).pipe(stream).on('end', cb)
  setTimeout(() => cb(), 100)
}

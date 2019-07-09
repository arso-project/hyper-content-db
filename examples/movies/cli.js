const cstore = require('../..')
const bench = require('nanobench')
const fs = require('fs')
const ram = require('random-access-memory')

const store = cstore('.data')
// const store = cstore(ram)
run()

function run () {
  const s = stepper()
  s.step('readFile', readFile)
  s.step('parse', parse)
  s.step('prepare', prepare)
  s.step('insert', insert)
  s.final((err, result) => {
    console.log('DONE', err)
  })
  s.run()
}

// TODO: Implement putSchema
function init () {
  store.writer(writer => {
    writer.putSchema('movies', {
      label: 'Movies',
      properties: {
        title: {
          type: 'string',
          index: true
        },
        body: {
          type: 'string'
        }
      }
    })
  })
}

function readFile (_, cb) {
  fs.readFile('./movies.txt', cb)
}

function parse (buf, cb) {
  let lines = buf.toString().split('\n')
  let movies = []
  lines.forEach(line => {
    let parts = line.split('\t')
    movies.push({ title: parts[0], body: parts[1] })
  })
  console.log('rows', movies.length)
  cb(null, movies)
}

function prepare (movies, cb) {
  const batchSize = 1000
  let pos = 0
  const batches = []
  const schema = 'movie'

  while (pos < movies.length) {
    let rows = movies.slice(pos, pos + batchSize)
    pos = pos + batchSize
    let batch = rows.map(value => ({
      op: 'put',
      id: cstore.id(),
      value,
      schema
    }))
    batches.push(batch)
  }
  console.log('batches', batches.length)
  cb(null, batches)
}

function insert (batches, cb) {
  let ids = []
  let i = 0
  workBench()
  function workBench () {
    // bench('batch insert no.' + i, b => {
      // b.start()
      let batch = batches.shift()
      store.batch(batch, (err, newIds) => {
        // b.end()
        i++
        if (err) return cb(err)
        ids = [...ids, ...newIds]
        ids = ids.concat(newIds)
        if (batches.length) process.nextTick(workBench)
        else cb(null, ids)
      })
    // })
  }
}

function stepper () {
  let _steps = []
  let _result
  let _final
  let _step

  return { step, run, final }

  function step (name, cb) {
    _steps.push({ name, cb })
  }

  function final (cb) {
    _final = cb
  }

  function run () {
    _step = _steps.shift()
    bench(_step.name, b => {
      b.start()
      _step.cb(_result, (err, data) => {
        b.end()
        if (err) return finish(err)
        _result = data
        if (_steps.length) process.nextTick(run)
        else finish(err)
      })
    })
  }

  function finish (err) {
    if (err && typeof err === 'object') err._step = _step.name
    if (_final) {
      process.nextTick(_final, err, _result)
    } else if (err) {
      throw err
    }
  }
}


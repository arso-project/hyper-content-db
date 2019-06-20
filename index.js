const thunky = require('thunky')
const p = require('path')
const { EventEmitter } = require('events')
const hyperid = require('hyperid')

const multidrive = require('./multidrive')
const kappa = require('./kappa')

const P_DATA = '.data'

module.exports = (...args) => new Contentcore(...args)
module.exports.id = () => Contentcore.id()

class Contentcore extends EventEmitter {
  constructor (storage, key, opts) {
    super()
    this.multidrive = multidrive(storage, key, opts)
    this.kcore = kappa({ multidrive: this.multidrive })
    this.ready = thunky(this._ready.bind(this))

    // this.kcore.use('entities', {
    //   prefix: '.data',
    //   map (msgs, next) {
    //     // console.log('MAP', msgs.map(m => ({...m})))
    //     console.log('MAP', msgs)
    //   }
    // })
  }

  _ready (cb) {
    this.multidrive.ready(err => {
      if (err) return cb(err)
      this.key = this.multidrive.key
      cb(null)
    })
  }

  use (view, opts) {
    this.kcore.use(view, opts)
  }

  writer (cb) {
    this.multidrive.writer(cb)
  }

  replicate (opts) {
    return this.multidrive.replicate(opts)
  }

  addSource (key, cb) {
    this.multidrive.addSource(key, cb)
  }

  sources (cb) {
    this.multidrive.sources(cb)
  }

  batch (msgs, cb) {
    const results = []
    const errors = []
    let missing = 0

    msgs.forEach(msg => {
      missing++
      if (msg.op === 'put') this.putRecord(msg.schema, msg.id, msg.record, finish)
      // if (msg.op === 'del') this.putRecord(msg.schema, msg.id, msg.record, finish)
      else finish()
    })

    function finish (err, result) {
      if (err) errors.push(err)
      if (result) results.push(result)
      if (--missing === 0) cb(errors.length && errors, results)
    }
  }

  putRecord (schema, id, record, cb) {
    // Schema names have to have exactly one slash.
    if (!validateSchemaName(schema)) return cb(new Error('Invalid schema name: ' + schema))

    this.writer((err, drive) => {
      if (err) return cb(err)
      const path = makePath(schema, id)
      const buf = Buffer.from(JSON.stringify(record))
      drive.writeFile(path, buf, (err) => {
        if (err) return cb(err)
        cb(null, id)
      })
    })
  }

  getRecords (schema, id, cb) {
    if (!validateSchemaName(schema)) return cb(new Error('Invalid schema name: ' + schema))
    const records = []
    const errors = []
    let missing = 0
    this.sources((err, drives) => {
      if (err) return cb(err)
      drives.forEach(drive => {
        missing++
        const path = makePath(schema, id)
        const key = hex(drive.key)
        const msg = { path, source: key, id, schema }
        drive.stat(path, (err, stat) => {
          if (err) return onrecord(null)
          drive.readFile(path, (err, buf) => {
            msg.stat = stat
            if (err) return onrecord({ ...msg, error: err })
            try {
              const value = JSON.parse(buf.toString())
              msg.value = value
              onrecord(msg)
            } catch (err) {
              onrecord({ ...msg, error: err })
            }
          })
        })
      })
    })

    function onrecord (msg) {
      if (msg && msg.error) errors.push(msg)
      else if (msg) records.push(msg)
      if (--missing === 0) {
        let error = errors.length ? errors : null
        cb(error, records)
      }
    }
  }

  listRecords (schema, cb) {
    let ids = []
    let missing = 0
    this.sources((err, drives) => {
      if (err) return cb(err)
      drives.forEach(drive => {
        let path = p.join(P_DATA, schema)
        missing++
        drive.readdir(path, (err, list) => {
          if (err) return finish(err)
          if (!list.length) return finish()
          list = list.map(id => id.replace(/\.json$/, ''))
          finish(null, list)
        })
      })
    })

    function finish (err, list) {
      if (!err && list) {
        ids = [...ids, ...list]
      }
      if (--missing === 0) cb(null, list)
    }
  }
}

Contentcore.id = hyperid({ fixedLength: true, urlSafe: true })

function makePath (schema, id) {
  return p.join(P_DATA, schema, id + '.json')
}

function validateSchemaName (schema) {
  return schema.split('/').length === 2
}

function hex (key) {
  return Buffer.isBuffer(key) ? key.toString('hex') : key
}

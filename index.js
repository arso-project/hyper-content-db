const thunky = require('thunky')
const p = require('path')
const { EventEmitter } = require('events')
const hyperid = require('hyperid')
const memdb = require('memdb')
const sub = require('subleveldown')

const multidrive = require('./multidrive')
const kappa = require('./kappa')

const entitiesView = require('./views/entities')
const contentView = require('./views/content')

const { P_DATA, P_SCHEMA } = require('./constants')

module.exports = (...args) => new Contentcore(...args)
module.exports.id = () => Contentcore.id()

class Contentcore extends EventEmitter {
  constructor (storage, key, opts) {
    super()
    opts = opts || {}
    this.multidrive = multidrive(storage, key, opts)
    this.kcore = kappa({ multidrive: this.multidrive })
    this.ready = thunky(this._ready.bind(this))

    this.level = opts.level || memdb()

    this.api = {}

    this.kcore.on('indexed', (...args) => this.emit('indexed', ...args))

    this.useLevelView('entities', entitiesView)
  }

  useLevelView (name, makeInnerView) {
    const db = sub(this.level, 'view.' + name)
    const innerView = makeInnerView(db, this)
    const view = contentView(db, innerView)
    this.kcore.use(name, view)
    this.api[name] = this.kcore.api[name]
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

  hasSource (key) {
    return this.multidrive.hasSource(key)
  }

  sources (cb) {
    this.multidrive.sources(cb)
  }

  source (key, cb) {
    this.multidrive.source(key, cb)
  }

  batch (msgs, cb) {
    const results = []
    const errors = []
    let missing = 0

    msgs.forEach(msg => {
      missing++
      if (msg.op === 'put') this.putRecord(msg.schema, msg.id, msg.record, finish)
      // if (msg.op === 'del') this.putRecord(msg.schema, msg.id, msg.record, finish)
      // NOTE: Without process.nextTick this would break because missing would not fully
      // increase before finishing.
      else process.nextTick(finish)
    })

    function finish (err, result) {
      if (err) errors.push(err)
      if (result) results.push(result)
      if (--missing === 0) cb(errors.length && errors, results)
    }
  }

  putRecord (schema, id, record, cb) {
    // Schema names have to have exactly one slash.
    if (!validSchemaName(schema)) return cb(new Error('Invalid schema name: ' + schema))

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
    if (!validSchemaName(schema)) return cb(new Error('Invalid schema name: ' + schema))
    const records = []
    const errors = []
    let missing = 0
    this.sources(drives => {
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
    this.sources(drives => {
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

  expandSchemaName (name, cb) {
    if (!validSchemaName(name)) return cb(new InvalidSchemaName(name))
    if (name.startsWith('~/')) {
      this.writer((err, drive) => {
        if (err) return cb(err)
        let expanded = hex(drive.key) + name.substring(1)
        cb(null, expanded)
      })
    }
  }

  putSchema (name, schema, cb) {
    this.expandSchemaName(name, (err, name) => {
      if (err) return cb(err)
      const path = p.join(P_SCHEMA, name + '.json')
      const buf = Buffer.from(JSON.stringify(this._encodeSchema(schema, name)))
      this.writer((err, drive) => {
        if (err) return cb(err)
        drive.writeFile(path, buf, cb)
      })
    })
  }

  _encodeSchema (schema, name) {
    const defaults = {
      '$schema': 'http://json-schema.org/draft-07/schema#',
      '$id': `dat://${name}.json`,
      type: 'object'
    }
    return Object.assign({}, defaults, schema)
  }

  getSchema (name, opts, cb) {
    if (typeof opts === 'function') return this.getSchema(name, {}, opts)
    opts = opts || {}
    const self = this

    let missing = 0
    let candidates = []

    this.expandSchemaName(name, (err, name) => {
      if (err) return cb(err)
      const ns = name.split('/').shift()
      const path = p.join(P_SCHEMA, name + '.json')

      if (ns === '~') getLocal(path, finish)
      else if (this.hasSource(ns)) getFrom(ns, path, finish)
      else getAll(path, finish)
    })

    function get (drive, path, cb) {
      drive.readFile(path, (err, buf) => {
        if (!err && buf.length) {
          try {
            const schema = JSON.parse(buf.toString())
            cb(null, schema)
          } catch (e) { cb(e) }
        } else cb()
      })
    }

    function getLocal (path, cb) {
      missing = 1
      self.writer((err, drive) => {
        if (err) return cb(err)
        get(drive, path, cb)
      })
    }

    function getFrom (source, path, cb) {
      missing = 1
      self.source(source, drive => {
        if (!drive) return cb()
        get(drive, path, cb)
      })
    }

    function getAll (path, cb) {
      self.sources(drives => {
        missing = drives.length
        drives.forEach(drive => get(drive, path, cb))
      })
    }

    function finish (err, schema) {
      // TODO: This should be emitted once only.
      if (err) return cb(err)
      if (schema) candidates.push(schema)
      if (--missing === 0) {
        if (!candidates.length) return cb()
        if (candidates.length === 1) return cb(null, candidates[0])
        else return cb(null, reduce(candidates))
      }
    }

    function reduce (schemas) {
      if (opts.reduce) return opts.reduce(schemas)

      let winner
      for (let schema of schemas) {
        winner = winner || schema
        if (schema.version && schema.version > winner.version) {
          winner = schema
        }
      }
      return winner
    }
  }
}

class InvalidSchemaName extends Error {
  constructor (name) {
    super()
    this.message = `Invalid schema name: ${name}`
  }
}

Contentcore.id = hyperid({ fixedLength: true, urlSafe: true })

function makePath (schema, id) {
  return p.join(P_DATA, schema, id + '.json')
}

function validSchemaName (schema) {
  return schema.split('/').length === 2
}

function hex (key) {
  return Buffer.isBuffer(key) ? key.toString('hex') : key
}

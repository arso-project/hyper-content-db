const thunky = require('thunky')
const p = require('path')
const { EventEmitter } = require('events')
const through = require('through2')
const hyperid = require('hyperid')
const memdb = require('memdb')
const sub = require('subleveldown')
const levelBaseView = require('kappa-view')

const multidrive = require('./multidrive')
const kappa = require('./kappa')

const entitiesView = require('./views/entities')
const contentView = require('./views/content')

const { P_DATA, P_SCHEMA, P_SOURCES } = require('./constants')

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

    this.id = Contentcore.id

    this.useRecordView('entities', entitiesView)
  }

  useRecordView (name, makeView, opts) {
    const db = sub(this.level, 'view.' + name)
    // levelBaseView takes care of the state handling
    // and passes on a subdb, and expects regular
    // kappa view opts (i.e., map).
    const view = levelBaseView(db, (db) => {
      // contentView wraps the inner view, taking care of
      // adding a .data prefix and optionally loading
      // record contents.
      return contentView(makeView(db, this, opts))
    })

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

  _initWriter (cb) {
    this.multidrive.writer((err, writer) => {
      if (err) return cb(err)
      // TODO: Don't do this on every start?
      let dirs = [P_DATA, P_SCHEMA, P_SOURCES]
      let pending = dirs.length
      for (let dir of dirs) {
        writer.mkdir(dir, done)
      }
      function done (err) {
        if (err && err.code !== 'EEXIST') return cb(err)
        if (--pending === 0) {
          cb(null, writer)
        }
      }
    })
  }

  use (view, opts) {
    this.kcore.use(view, opts)
  }

  writer (cb) {
    this.ready(err => {
      if (err) return cb(err)
      if (!this._writerReady) this._initWriter(cb)
      else this.multidrive.writer(cb)
    })
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
    // TODO: Support del.
    const results = []
    const errors = []
    let missing = 0

    msgs.forEach(msg => {
      missing++
      if (msg.op === 'put') this.put(msg.schema, msg.id, msg.value, finish)
      // if (msg.op === 'del') this.put(msg.schema, msg.id, msg.value, finish)
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

  /**
   * Create a batch stream.
   *
   * The returned stream is a transform stream. Write batch ops
   * to it, read results and erros.
   *
   * Wants either array of ops or a single op, where op is
   * {
   *  op: 'put' | 'del',
   *  id,
   *  schema,
   *  value
   * }
   *
   * For details see example in tests.
   */
  createBatchStream () {
    const self = this

    const batchStream = through.obj(function (msg, encoding, next) {
      msg = Array.isArray(msg) ? msg : [msg]
      self.batch(msg, (err, ids) => {
        if (err) this.error(err)
        else this.push(ids)
        next(err)
      })
    })

    return batchStream
  }

  /**
   * Create a get stream.
   *
   * The returned stream is a transform stream. Write get requests
   * to it, read results and erros.
   *
   * Wants messages that look like
   * { id, schema, source }
   *
   * TODO: Support no source.
   * TODO: Support seq.
   *
   * For details see example in tests.
   */
  createGetStream () {
    const self = this
    return through.obj(function (msg, enc, next) {
      self.get(msg, (err, record) => {
        if (err) this.error(err)
        else this.push(record)
        next()
      })
    })
  }

  create (schema, value, cb) {
    this.put(schema, this.id(), value, cb)
  }

  put (schema, id, value, cb) {
    // Schema names have to have exactly one slash.
    this.expandSchemaName(schema, (err, schema) => {
      if (err) return cb(err)
      this.writer((err, drive) => {
        if (err) return cb(err)
        const dir = p.join(P_DATA, schema)

        if (!id) id = this.id()

        drive.mkdir(dir, (err) => {
          if (err && err.code !== 'EEXIST') return cb(err)
          const path = makePath(schema, id)
          const buf = Buffer.from(JSON.stringify(value))
          drive.writeFile(path, buf, (err) => {
            if (err) return cb(err)
            console.log('WRITTEN', path, buf.toString())
            cb(null, id)
          })
        })
      })
    })
  }

  get (opts, cb) {
    const { id, schema, source, seq } = opts

    this.source(source, drive => {
      if (!drive) return cb()

      const path = makePath(schema, id)
      const source = hex(drive.key)
      const record = { path, source, id, schema }
      drive.stat(path, (err, stat) => {
        if (err) return onrecord(null)
        record.stat = stat
        drive.readFile(path, (err, buf) => {
          if (err) return 
          try {
            const value = JSON.parse(buf.toString())
            record.value = value
            cb(null, record)
          } catch (err) {
            cb(err)
          }
        })
      })
    })
  }

  getRecords (schema, id, cb) {
    this.expandSchemaName(schema, (err, schema) => {
      if (err) return cb(err)
      const records = []
      const errors = []
      let missing = 0
      this.sources(drives => {
        drives.forEach(drive => {
          missing++
          const path = makePath(schema, id)
          const source = hex(drive.key)
          const msg = { path, source, id, schema }
          drive.stat(path, (err, stat) => {
            if (err) return onrecord(null)
            msg.stat = stat

            drive.readFile(path, (err, buf) => {
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
    })
  }

  listRecords (schema, cb) {
    this.expandSchemaName(schema, (err, schema) => {
      if (err) return cb(err)
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
    })
  }

  expandSchemaName (name, cb) {
    this.ready(() => {
      if (!validSchemaName(name)) return cb(new InvalidSchemaName(name))
      if (name.indexOf('/') === -1) {
        let expanded = hex(this.key) + '/' + name
        cb(null, expanded)
        // this.writer((err, drive) => {
        //   if (err) return cb(err)
        //   let expanded = hex(drive.key) + '/' + name
        //   cb(null, expanded)
        // })
      } else {
        cb(null, name)
      }
    })
  }

  putSchema (name, schema, cb) {
    this.expandSchemaName(name, (err, name) => {
      if (err && cb) return cb(err)
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

    let missing = 1
    let candidates = []

    this.expandSchemaName(name, (err, name) => {
      if (err) return cb(err)
      const ns = name.split('/').shift()
      const path = p.join(P_SCHEMA, name + '.json')

      if (this.hasSource(ns)) getFrom(ns, path, finish)
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

    function getFrom (source, path, cb) {
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
  return schema.match(/^[a-zA-Z0-9_\-./]*$/)
  // return schema.split('/').length === 2
}

function hex (key) {
  return Buffer.isBuffer(key) ? key.toString('hex') : key
}

function mkdirp (fs, path, cb) {
  const parts = path.split('/')
  let pending = parts.length

  // simple once fn
  let error = err => {
    cb(err)
    error = () => {}
  }

  for (let i = 0; i < parts.length; i++) {
    let path = p.join(parts.slice(0, i))
    fs.mkdir(path, (err) => {
      if (err && err !== 'EEXIST') error(err)
      if (--pending === 0) cb()
    })
  }
}

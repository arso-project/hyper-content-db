const thunky = require('thunky')
const p = require('path')
const { EventEmitter } = require('events')
const through = require('through2')
const LRU = require('lru-cache')
// const hyperid = require('hyperid')
const shortid = require('shortid')
const memdb = require('memdb')
const sub = require('subleveldown')
const levelBaseView = require('kappa-view')

const multidrive = require('./multidrive')
const kappa = require('./kappa')

const entitiesView = require('./views/entities')
const contentView = require('./views/content')
const schemaAwareView = require('./views/schema-aware')

const { P_DATA, P_SCHEMA, P_SOURCES } = require('./constants')

// const JSON_STRING = Symbol('json-buffer')

module.exports = (...args) => new Contentcore(...args)
module.exports.id = () => Contentcore.id()
// module.exports.JSON_STRING = JSON_STRING

class Contentcore extends EventEmitter {
  constructor (storage, key, opts) {
    super()
    opts = opts || {}

    this.multidrive = multidrive(storage, key, opts)

    this.kcore = kappa({
      multidrive: this.multidrive,
      viewContext: this
    })

    this.recordCache = new LRU({
      max: opts.cacheSize || 16777216, // 16M
      length: record => (record.stat && record.stat.size) || 256
    })

    this.level = opts.level || memdb()

    this.api = {}

    this.kcore.on('indexed', (...args) => this.emit('indexed', ...args))
    this.kcore.on('indexed-all', (...args) => this.emit('indexed-all', ...args))

    this.id = Contentcore.id

    if (opts.defaultViews !== false) {
      this.useRecordView('entities', entitiesView)
      this.useRecordView('indexes', schemaAwareView)
    }

    this.ready = thunky(this._ready.bind(this))
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
      this.discoveryKey = this.multidrive.discoveryKey
      // TODO: Always wait for a writer?
      this.multidrive.writer(() => cb())
    })
  }

  _initWriter (cb) {
    this._writerReady = true
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

  get localKey () {
    return this.multidrive.localKey
  }

  replicate (opts) {
    return this.multidrive.replicate(opts)
  }

  addSource (key, cb) {
    cb = cb || noop
    this.multidrive.saveSource(key, cb)
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
    cb = cb || noop
    const results = []
    const errors = []

    let pending = msgs.length

    for (let msg of msgs) {
      const { op = 'put', schema, id, value } = msg

      if (op === 'put') this.put(schema, id, value, finish)
      else if (op === 'del') this.del(schema, id, finish)
      else if (op === 'source') this.addSource(value)
      else if (op === 'schema') this.putSchema(schema, value, finish)
      // NOTE: Without process.nextTick this would break because
      // pending would not fullyincrease before finishing.
      else process.nextTick(finish)
    }

    function finish (err, result) {
      if (err) errors.push(err)
      if (result) results.push(result)
      if (--pending === 0) cb(errors.length && errors, results)
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
   *  op: 'put' | 'del' | 'schema',
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
   * Emits messages that look like
   * { id, schema, source, value, stat }
   *
   * TODO: Support no source.
   * TODO: Support seq.
   *
   * For details see example in tests.
   */
  createGetStream (opts) {
    const self = this
    return through.obj(function (msg, enc, next) {
      self.get(msg, opts, (err, record) => {
        if (err) {
          console.error('ERROR', err)
          this.emit('error', err)
        } else if (record) {
          this.push(record)
        }
        next()
      })
    })
  }

  create (schema, value, cb) {
    this.put(schema, this.id(), value, cb)
  }

  put (schema, id, value, cb) {
    // TODO: Make this the default and only support this form.
    if (typeof schema === 'object') {
      return this.put(schema.schema, schema.id, schema.value, id)
    }
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
            cb(null, id)
          })
        })
      })
    })
  }

  get (req, opts, cb) {
    if (typeof opts === 'function') return this.get(req, null, opts)
    const self = this
    opts = opts || {}

    const { id, schema, source, seq } = req

    if (seq && !source) return cb(new Error('Invalid request: seq without source'))

    if (opts.reduce === true) opts.reduce = defaultReduce

    this.expandSchemaName(schema, (err, schema) => {
      if (err) return cb(err)
      let pending
      let records = []

      if (source) {
        this.source(source, drive => load(drive, cb))
      } else {
        this.sources(drives => {
          pending = drives.length
          drives.forEach(drive => load(drive, onrecord))
        })
      }

      function onrecord (err, record) {
        // Skip not found errors.
        if (err && err.code !== 'ENOENT') return cb(err)
        if (record) records.push(record)
        if (--pending === 0) {
          if (opts.reduce) {
            let result
            let sources = []
            for (let record of records) {
              if (!result) result = record
              else result = opts.reduce(result, record)
              sources.push(record.source)
            }
            result.alternatives = records.filter(r => r !== result)
            records = result
          }
          cb(null, records)
        }
      }

      function load (drive, cb) {
        if (!drive) return cb()

        const path = makePath(schema, id)
        const source = hex(drive.key)

        const cacheSeq = seq || drive.version

        const cacheKey = `${source}@${cacheSeq}/${path}`
        const cachedRecord = self.recordCache.get(cacheKey)
        if (cachedRecord) return cb(null, cachedRecord)

        const record = { source, id, schema }

        // TODO: Find out why seq has to be incremented by one.
        // If doing drive.checkout(seq), the files are not found.
        if (seq) drive = drive.checkout(Math.min(seq + 1, drive.version))

        drive.stat(path, (err, stat, trie) => {
          if (err) return cb(err)

          if (opts.fullStat) record.stat = stat

          record.meta = cleanStat(stat)

          drive.readFile(path, (err, buf) => {
            if (err) return cb(err)
            try {
              const string = buf.toString()
              const value = JSON.parse(string)
              record.value = value
              self.recordCache.set(cacheKey, record)
              cb(null, record)
            } catch (err) {
              cb(err)
            }
          })
        })
      }
    })

    function defaultReduce (a, b) {
      return a.meta.mtime > b.meta.mtime ? a : b
    }
  }

  getRecords (schema, id, cb) {
    return this.get({ schema, id }, cb)
  }

  list (schema, cb) {
    this.expandSchemaName(schema, (err, schema) => {
      if (err) return cb(err)
      let ids = new Set()
      let pending
      this.sources(drives => {
        pending = drives.length
        drives.forEach(drive => {
          let path = p.join(P_DATA, schema)
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
          list.forEach(id => ids.add(id))
        }
        if (--pending === 0) cb(null, Array.from(ids))
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
      const encoded = this._encodeSchema(schema, name)
      const path = p.join(P_SCHEMA, name + '.json')
      const buf = Buffer.from(JSON.stringify(encoded))
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
      type: 'object',
      title: schema.title || name
    }
    return Object.assign({}, defaults, schema)
  }

  getSchema (name, opts, cb) {
    if (typeof opts === 'function') return this.getSchema(name, {}, opts)
    opts = opts || {}
    const self = this

    cb = once(cb)

    let pending = 1
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
        if (err || !buf.length) return cb()
        try {
          const schema = JSON.parse(buf.toString())
          cb(null, schema)
        } catch (err) { cb(err) }
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
        pending = drives.length
        drives.forEach(drive => get(drive, path, cb))
      })
    }

    function finish (err, schema) {
      if (err) return cb(err)
      if (schema) candidates.push(schema)
      if (--pending === 0) {
        if (!candidates.length) return cb()
        if (candidates.length === 1) return cb(null, candidates[0])
        else return cb(null, reduce(candidates))
      }
    }

    function reduce (schemas) {
      if (opts.reduce) return opts.reduce(schemas)
      return schemas.reduce((winner, schema) => {
        if (!winner || !winner.version) return schema
        if (!schema.version) return winner
        return schema.version > winner.version ? schema : winner
      }, null)
    }
  }
}

class InvalidSchemaName extends Error {
  constructor (name) {
    super()
    this.message = `Invalid schema name: ${name}`
  }
}

// Contentcore.id = hyperid({ fixedLength: true, urlSafe: true })
Contentcore.id = () => shortid.generate()

function makePath (schema, id) {
  return p.join(P_DATA, schema, id + '.json')
}

function validSchemaName (schema) {
  if (!schema || typeof schema !== 'string') return false
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

function cleanStat (stat) {
  return {
    ctime: stat.ctime,
    mtime: stat.mtime,
    size: stat.size,
    seq: stat.seq
  }
}

function once (fn) {
  let wrapper = (...args) => {
    fn(...args)
    wrapper = () => {}
  }
  return wrapper
}

function noop () {}

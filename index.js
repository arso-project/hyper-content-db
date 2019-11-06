const thunky = require('thunky')
const p = require('path')
const { EventEmitter } = require('events')
const through = require('through2')
const LRU = require('lru-cache')
const hyperdriveSource = require('kappa-core/sources/hyperdrive')
const { Kappa } = require('kappa-core')
const raf = require('random-access-file')
const shortid = require('shortid')
const memdb = require('memdb')
const sub = require('subleveldown')
const levelBaseView = require('kappa-view')
const Corestore = require('corestore')

const multidrive = require('./lib/multidrive')

const entitiesView = require('./views/entities')
const contentView = require('./views/content')
const schemaAwareView = require('./views/schema-aware')

const { P_DATA, P_SCHEMA, P_SOURCES } = require('./lib/constants')

// const JSON_STRING = Symbol('json-buffer')

module.exports = (...args) => new HyperContentDB(...args)
module.exports.id = () => HyperContentDB.id()
// module.exports.JSON_STRING = JSON_STRING

class HyperContentDB extends EventEmitter {
  constructor (storage, key, opts) {
    super()
    opts = opts || {}

    this.level = opts.level || memdb()

    if (typeof storage === 'function') {
      this.storage = path => storage(path)
    } else if (typeof storage === 'string') {
      this.storage = path => raf(storage + '/' + path)
    }

    this.corestore = opts.corestore || new Corestore(path => this.storage('corestore/' + path))

    this.kappa = new Kappa()

    this.multidrive = multidrive({
      key,
      corestore: this.corestore,
      storage: path => this.storage('multidrive/' + path)
    })

    this.multidrive.on('source', drive => {
      this.kappa.source(drive.key.toString('hex'), hyperdriveSource, {
        drive,
        mount: false
      })
    })

    this.recordCache = new LRU({
      max: opts.cacheSize || 16777216, // 16M
      length: record => (record.stat && record.stat.size) || 256
    })

    this.api = {}

    this.kappa.on('indexed', (...args) => this.emit('indexed', ...args))
    this.kappa.on('indexed-all', (...args) => this.emit('indexed-all', ...args))

    this.id = HyperContentDB.id

    if (opts.defaultViews !== false) {
      this.useRecordView('entities', entitiesView)
      this.useRecordView('indexes', schemaAwareView)
    }

    this.ready = thunky(this._ready.bind(this))
    this.ready()
  }

  useRecordView (name, makeView, opts = {}) {
    const db = sub(this.level, 'view.' + name)
    // levelBaseView takes care of the state handling
    // and passes on a subdb, and expects regular
    // kappa view opts (i.e., map).
    const view = levelBaseView(db, (db) => {
      // contentView wraps the inner view, taking care of
      // adding a .data prefix and optionally loading
      // record contents.
      return contentView(makeView(db, this, opts), this)
    })

    this.kappa.use(name, view)
    this.api[name] = this.kappa.api[name]
  }

  useFileView (name, makeView, opts = {}) {
    const db = sub(this.level, 'view.' + name)
    // levelBaseView takes care of the state handling
    // and passes on a subdb, and expects regular
    // kappa view opts (i.e., map).
    const view = levelBaseView(db, (db) => {
      // contentView wraps the inner view, taking care of
      // adding a .data prefix and optionally loading
      // record contents.
      return {
        transformNodes: true,
        prefix: opts.prefix || undefined,
        ...makeView(db, this, opts)
      }
    })

    this.kappa.use(name, view)
    this.api[name] = this.kappa.api[name]
  }

  _ready (cb) {
    this.multidrive.ready(err => {
      if (err) return cb(err)
      // TODO: Always wait for a writer?
      this.multidrive.writer((err) => cb(err))
    })
  }

  get key () {
    return this.multidrive.key
  }

  get discoveryKey () {
    return this.multidrive.discoveryKey
  }

  close () {
    this.emit('close')
  }

  _initWriter (cb) {
    this._writerReady = true
    this.multidrive.writer((err, drive) => {
      if (err) return cb(err)
      // TODO: Don't do this on every start?
      const dirs = [P_DATA, P_SCHEMA, P_SOURCES]
      let pending = dirs.length
      for (const dir of dirs) {
        drive.mkdir(dir, done)
      }
      function done (err) {
        if (err && err.code !== 'EEXIST') return cb(err)
        if (--pending === 0) cb(null, drive)
      }
    })
  }

  use (view, opts) {
    this.kappa.use(view, opts)
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

  replicate (isInitiator, opts) {
    return this.corestore.replicate(isInitiator, opts)
  }

  addSource (key, cb) {
    cb = cb || noop
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
    cb = cb || noop
    const results = []
    const errors = []

    let pending = msgs.length

    for (let msg of msgs) {
      const { op = 'put', schema, id, value } = msg

      if (op === 'put') this.put({ schema, id, value }, finish)
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
        if (err) this.emit('error', err)
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
          this.emit('error', err)
        } else if (record) {
          if (Array.isArray(record)) {
            record.forEach(record => this.push(record))
          } else {
            this.push(record)
          }
        }
        next()
      })
    })
  }

  put (req, cb) {
    let { schema, id, value } = req
    if (!id) id = this.id()

    this.expandSchemaName(schema, (err, schema) => {
      if (err) return cb(err)
      this.writer((err, drive) => {
        if (err) return cb(err)
        const dir = p.join(P_DATA, schema)
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
    cb = once(cb)
    opts = opts || {}

    let { id, schema, source, seq } = req

    seq = Number(seq)
    if (seq && !source) return cb(new Error('Invalid request: seq without source'))

    if (opts.reduce === true) opts.reduce = defaultReduce

    this.expandSchemaName(schema, (err, schema) => {
      if (err) return cb(err)
      let pending
      let records = []

      if (source) {
        pending = 1
        this.source(source, drive => load(drive, onrecord))
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
        if (--pending === 0) finish()
      }

      function finish () {
        // If reduce is false, return all records.
        if (!opts.reduce) return cb(null, records)

        if (!records.length) return cb(null, null)
        if (records.length === 1) return cb(null, records[0])

        const result = records.reduce((result, record) => {
          if (!result) return record
          else return opts.reduce(result, record)
        }, null)
        if (result) result.alternatives = records.filter(r => r.source !== result.source)
        cb(null, result)
      }

      function load (drive, cb) {
        if (!drive) return cb()

        const path = makePath(schema, id)
        const source = hex(drive.key)
        const cacheKey = `${source}@${seq || drive.version}/${path}`

        const cachedRecord = self.recordCache.get(cacheKey)
        if (cachedRecord) return cb(null, cachedRecord)

        const record = { source, id, schema }

        // TODO: Find out why seq has to be incremented by one.
        // If doing drive.checkout(seq), the files are not found.
        if (seq) drive = drive.checkout(Math.min(seq + 1, drive.version))

        drive.stat(path, (err, stat, trie) => {
          if (err || !stat.isFile()) return cb(err, null)

          if (opts.fullStat) record.stat = stat

          record.meta = cleanStat(stat)

          drive.readFile(path, (err, buf) => {
            if (err) return cb(err)
            try {
              record.value = JSON.parse(buf.toString())
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

  // TODO: This should likely be streaming.
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
            list = list.map(parseId)
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

  putSchema (name, schema, cb = noop) {
    this.expandSchemaName(name, (err, name) => {
      if (err) return cb(err)
      // const id = schemaId(name)
      const id = name
      const value = this._encodeSchema(schema, name, id)
      this.put({ schema: 'core/schema', id, value }, cb)
    })
  }

  getSchema (name, opts, cb) {
    if (typeof opts === 'function') return this.getSchema(name, {}, opts)
    opts = opts || {}
    this.expandSchemaName(name, (err, name) => {
      if (err) return cb(err)
      // const id = schemaId(name)
      const id = name
      this.get({ schema: 'core/schema', id }, { reduce }, (err, record) => {
        if (err) return cb(err)
        if (!record) return cb(null, null)
        return cb(null, record.value)
      })
    })

    function reduce (a, b) {
      if (opts.reduce) return opts.reduce(a, b)
      if (a.version && b.version) return a.version > b.version ? a : b
      if (a.version) return a
      if (b.version) return b
      return a
    }
  }

  _encodeSchema (schema, name, id) {
    const $id = `dat://${hex(this.key)}/${makePath('core/schema', id)}`
    const defaults = {
      '$schema': 'http://json-schema.org/draft-07/schema#',
      '$id': $id,
      type: 'object',
      name,
      title: name,
      properties: schema.properties
    }
    return Object.assign({}, defaults, schema)
  }
}

// function schemaId (name) {
//   return name.replace('/', '__')
// }

class InvalidSchemaName extends Error {
  constructor (name) {
    super()
    this.message = `Invalid schema name: ${name}`
  }
}

// HyperContentDB.id = hyperid({ fixedLength: true, urlSafe: true })
HyperContentDB.id = () => shortid.generate()

function makePath (schema, id) {
  id = id.replace('/', '')
  return p.join(P_DATA, schema, id + '.json')
}

function parsePath (path) {
  const parts = path.split('/')
  parts.shift()
  let { schemaNS, schemaName, filename } = parts
  id = parseId(filename)
  const schema = [schemaNS, schemaName].join('/')
  return { id, schema }
}

function parseId (filename) {
  const id = filename.replace('__', '/').replace('.json', '')
  return id
}

function validSchemaName (schema) {
  if (!schema || typeof schema !== 'string') return false
  return schema.match(/^[a-zA-Z0-9_\-./]*$/)
  // return schema.split('/').length === 2
}

function hex (key) {
  return Buffer.isBuffer(key) ? key.toString('hex') : key
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

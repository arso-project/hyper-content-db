const Corestore = require('corestore')
const ram = require('random-access-memory')
const Ajv = require('ajv')
const sub = require('subleveldown')
const memdb = require('memdb')
const { uuid } = require('./util')
const { Transform } = require('stream')
const corestoreSource = require('kappa-core/sources/corestore')
const { Kappa } = require('kappa-core')
const collect = require('stream-collector')
const crypto = require('crypto')

const { Record } = require('./lib/messages')

const kvView = require('./views/kv')
const recordsView = require('./views/records')
const indexView = require('./views/indexes')

module.exports = class Database {
  constructor (opts = {}) {
    const self = this

    this.key = opts.key
    if (!this.key) this.key = crypto.randomBytes(32)

    this.corestore = opts.corestore || defaultCorestore(opts)
    this.schemas = opts.schemas || new SchemaStore({ key: this.key })

    this.lvl = opts.level || memdb()

    this.kappa = new Kappa()
    this.kappa.source('input', corestoreSource, {
      store: this.corestore,
      transform (msgs, next) {
        next(msgs.map(msg => {
          const { key, seq, value } = msg
          return self.schemas.decodeRecord(value, { key, seq })
        }))
      }
    })

    this.views = this.kappa.useStack('db', [
      kvView(sub(this.lvl, 'kv'), this),
      recordsView(sub(this.lvl, 'rc'), this),
      indexView(sub(this.lvl, 'idx'), this)
    ])

    this.feeds = {}
    this._opened = false
  }

  replicate (isInitiator, opts) {
    return this.corestore.replicate(isInitiator, opts)
  }

  ready (cb) {
    this.corestore.ready(() => {
      this.writer.ready(() => {
        this._initSchemas(() => {
          this._opened = true
          cb()
        })
      })
    })
  }

  _initSchemas (cb) {
    const qs = this.api.records.bySchema('core/schema')
    this.loadStream(qs, (err, schemas) => {
      if (err) return cb(err)
      schemas.forEach(msg => this.schemas.put(msg.id, msg.value))
      cb()
    })
  }

  get api () {
    return this.kappa.api
  }

  get writer () {
    if (!this._localWriter) {
      const feed = this.corestore.default({
        name: 'localwriter',
        valueEncoding: Record,
        parents: [this.key]
      })
      this._localWriter = this.feed(feed.key)
    }
    return this._localWriter
  }

  feed (key) {
    if (Buffer.isBuffer(key)) key = key.toString('hex')
    if (this.feeds[key]) return this.feeds[key]
    const feed = this.corestore.get({
      key,
      valueEncoding: Record,
      parents: [this.key]
    })
    const recordFeed = new RecordFeed(feed, this)
    this.feeds[key] = recordFeed
    return this.feeds[key]
  }

  get sourceId () {
    return this.writer.key.toString('hex')
  }

  put (record, cb) {
    this.writer.put(record, cb)
  }

  putSchema (name, schema, cb) {
    this.ready(() => {
      name = this.schemas.resolveName(name, this.key)
      if (!this.schemas.put(name, schema)) return cb(this.schemas.error)
      const record = {
        schema: 'core/schema',
        value: this.schemas.get(name),
        id: name
      }
      this.put(record, cb)
    })
  }

  putSource (key, cb) {
    if (Buffer.isBuffer(key)) key = key.toString('hex')
    const record = {
      schema: 'core/source',
      id: key,
      value: {
        type: 'kappa-records'
      }
    }
    this.put(record, cb)
  }

  get (id, cb) {
    this.kappa.api.kv.getLinks(id, (err, links) => {
      if (err) cb(err)
      else this.loadAll(links, cb)
    })
  }

  getLinks (record, cb) {
    this.kappa.api.kv.ready(() => {
      this.kappa.api.kv.getLinks(record, cb)
    })
  }

  loadStream (stream, cb) {
    if (typeof stream === 'function') return this.loadStream(null, stream)
    const self = this
    const transform = through(function (req, chunk, next) {
      self.loadLink(req, (err, record) => {
        if (err) this.emit('error', err)
        else this.push(record)
        next()
      })
    })
    if (stream) stream.pipe(transform)
    if (cb) return collect(transform, cb)
    else return transform
  }

  loadAll (links, cb) {
    let pending = links.length
    let res = []
    let errs = []
    links.forEach(link => this.loadLink(link, (err, link) => {
      if (err) errs.push(err)
      else res.push(link)
      if (--pending === 0) cb(errs.length ? errs : null, links)
    }))
  }

  loadLink (link, cb) {
    if (typeof link === 'string') {
      var [key, seq] = link.split('@')
    } else {
      key = link.key
      seq = link.seq
    }
    const feed = this.feed(key)
    feed.get(seq, cb)
  }

  getSchemas () {
    return this.schemas.list()
  }

  getSchema (name) {
    return this.schemas.get(name)
  }
}

class SchemaStore {
  constructor (opts) {
    this.key = opts.key
    this.schemas = {}
    this.ajv = new Ajv()
    this.put('core/schema', {
      type: 'object'
    })
  }

  put (name, schema) {
    name = this.resolveName(name)
    if (this.schemas[name]) return
    schema = this.parseSchema(name, schema)
    this.schemas[name] = schema
    // TODO: Handle error
    this.ajv.addSchema(schema, name)
    return true
  }

  get (name) {
    if (typeof name === 'object') {
      let { schema, source } = name
      name = this.resolveName(schema)
    }
    return this.schemas[name]
  }

  list () {
    return Object.value(this.schemas)
  }

  validate (record) {
    const name = this.resolveName(record.schema)
    const result = this.ajv.validate(name, record.value)
    if (!result) this._lastError = new ValidationError(this.ajv.errorsText(), this.ajv.errors)
    return result
  }

  get error () {
    return this._lastError
  }

  decodeRecord (record, defaults) {
    const schema = this.get(record)
    if (schema.encoding) record.value = schema.encoding.decode(record.value)
    else record.value = JSON.parse(record.value)
    record = { ...defaults, ...record }
    if (Buffer.isBuffer(record.key)) record.key = record.key.toString('hex')
    if (record.seq) record.seq = Number(record.seq)
    return record
  }

  encodeRecord (record, source) {
    record.schema = this.resolveName(record.schema)
    if (record.schema && record.value) {
      const schema = this.get(record.schema)
      if (schema.encoding) record.value = schema.encoding.encode(record.value)
      else record.value = JSON.stringify(record.value)
    }
    return record
  }

  resolveName (name, key) {
    if (!key) key = this.key
    if (Buffer.isBuffer(key)) key = key.toString('hex')
    if (name.indexOf('/') === -1) name = key + '/' + name
    // if (name.indexOf('@') === -1) {
    // TODO: Support versions
    // name = name + '@0'
    // }
    return name
  }

  parseSchema (name, schema) {
    return {
      $id: name,
      type: 'object',
      ...schema
    }
  }
}

class ValidationError extends Error {
  constructor (message, errors) {
    super(message)
    this.errors = errors
  }
}

class RecordFeed {
  constructor (feed, db) {
    this.db = db
    this.feed = feed
    this.ready = this.feed.ready.bind(this.feed)
  }

  get key () {
    return this.feed.key
  }

  put (record, cb) {
    if (!this.db.schemas.validate(record)) return cb(this.db.schemas.error)
    if (!record.id) record.id = uuid()
    this.append(record, err => err ? cb(err) : cb(null, record.id))
  }

  del (id, cb) {
    if (typeof id === 'object') id = id.id
    const record = { id, delete: true }
    this.append(record, cb)
  }

  append (record, cb) {
    if (!this.feed.opened) return this.ready(() => this.append(record, cb))
    if (!this.feed.writable) return cb(new Error('This feed is not writable'))
    record.timestamp = Date.now()
    this.db.getLinks(record, (err, links) => {
      if (err && err.status !== 404) return cb(err)
      record.links = links
      record = this.db.schemas.encodeRecord(record)
      this.feed.append(record, cb)
    })
  }

  get (seq, cb) {
    if (!this.feed.opened) return this.ready(() => this.get(seq, cb))
    this.feed.get(seq, (err, buf) => {
      if (err) return cb(err)
      const decoded = this.db.schemas.decodeRecord(buf, { key: this.key, seq })
      cb(null, decoded)
    })
  }
}

function defaultCorestore (opts) {
  return new Corestore(ram, opts)
}

function through (transform) {
  return new Transform({
    objectMode: true,
    transform
  })
}


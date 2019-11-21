const Corestore = require('corestore')
const ram = require('random-access-memory')
const sub = require('subleveldown')
const memdb = require('memdb')
const corestoreSource = require('kappa-core/sources/corestore')
const { Kappa } = require('kappa-core')
const collect = require('stream-collector')
const crypto = require('crypto')

const { Record: RecordEncoding } = require('./lib/messages')
const SchemaStore = require('./schema')
const { uuid, through } = require('./util')
const kvView = require('./views/kv')
const recordsView = require('./views/records')
const indexView = require('./views/indexes')

module.exports = function CorestoreDatabase (opts = {}) {
  const backend = new CorestoreBackend(opts)
  return new Database({ ...opts, backend })
}

class Record {
  static decode (buf, props = {}) {
    let record = RecordEncoding.decode(buf)
    record = { ...record, ...props }
    if (Buffer.isBuffer(record.key)) record.key = record.key.toString('hex')
    if (record.seq) record.seq = Number(record.seq)
    if (record.value) record.value = JSON.parse(record.value)
    return record
  }

  static encode (record) {
    if (record.value) record.value = JSON.stringify(record.value)
    const buf = RecordEncoding.encode(record)
    return buf
  }
}

class CorestoreBackend {
  constructor (opts) {
    this.corestore = opts.corestore || defaultCorestore(opts)
  }

  open (cb) {
    return this.corestore.ready(cb)
  }

  append (record, cb) {
    const feed = this._localWriter()
    record = Record.encode(record)
    feed.append(record, cb)
  }

  get (key, seq, cb) {
    const feed = this._feed(key)
    feed.get(seq, (err, buf) => {
      if (err) return cb(err)
      const record = Record.decode(buf, { key, seq })
      cb(null, record)
    })
  }

  kappaSource () {
    return {
      create: corestoreSource,
      opts: {
        store: this.corestore,
        transform (msgs, next) {
          next(msgs.map(msg => {
            const { key, seq, value } = msg
            return Record.decode(value, { key, seq })
          }))
        }
      }
    }
  }

  _localWriter () {
    return this._feed({ default: true })
  }

  _feed (opts) {
    if (Buffer.isBuffer(opts)) opts = { key: opts.toString('hex') }
    if (typeof opts === 'string') opts = { key: opts }
    // TODO: Pass a key as parent?
    const feed = this.corestore.get(opts)
    return feed
  }
}

class Database {
  constructor (opts = {}) {
    this.opts = opts
    this.key = opts.key
    if (!this.key) this.key = crypto.randomBytes(32)

    this.backend = opts.backend
    this.backend.key = this.key

    this.encoding = Record
    this.schemas = opts.schemas || new SchemaStore({ key: this.key })
    this.lvl = opts.level || memdb()
    this.kappa = new Kappa()

    this.views = this.kappa.useStack('db', [
      kvView(sub(this.lvl, 'kv'), this),
      recordsView(sub(this.lvl, 'rc'), this),
      indexView(sub(this.lvl, 'idx'), this)
    ])

    const kappaSource = this.backend.kappaSource()
    this.kappa.source('input', kappaSource.create, kappaSource.opts)

    this._opened = false
  }

  // replicate (isInitiator, opts) {
  //   return this.corestore.replicate(isInitiator, opts)
  // }

  ready (cb) {
    this.backend.open(() => {
      this._initSchemas(() => {
        this._opened = true
        cb()
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

  put (record, cb) {
    record.schema = this.schemas.resolveName(record.schema)
    if (!this.schemas.validate(record)) return cb(this.schemas.error)
    if (!record.id) record.id = uuid()
    this._append(record, err => err ? cb(err) : cb(null, record.id))
  }

  del (id, cb) {
    if (typeof id === 'object') id = id.id
    const record = { id, delete: true }
    this._append(record, cb)
  }

  _append (record, cb) {
    record.timestamp = Date.now()
    this.getLinks(record, (err, links) => {
      if (err && err.status !== 404) return cb(err)
      record.links = links
      this.backend.append(record, cb)
    })
  }

  getKeyseq (key, seq, cb) {
    this.backend.get(key, seq, (err, record) => {
      if (err) return cb(err)
      cb(null, record)
    })
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
    this.getKeyseq(key, seq, cb)
  }

  getSchemas () {
    return this.schemas.list()
  }

  getSchema (name) {
    return this.schemas.get(name)
  }
}

function defaultCorestore (opts) {
  return new Corestore(ram, opts)
}

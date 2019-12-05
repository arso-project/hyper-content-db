const ram = require('random-access-memory')
const sub = require('subleveldown')
const memdb = require('level-mem')
const Corestore = require('corestore')
const Kappa = require('kappa-core')
const kappaCorestoreSource = require('kappa-core/sources/corestore')
const collect = require('stream-collector')
const crypto = require('crypto')
const levelBaseView = require('kappa-view')
const State = require('./lib/state')
const { EventEmitter } = require('events')

const { uuid, through } = require('./util')
const { Record: RecordEncoding } = require('./lib/messages')
const SchemaStore = require('./schema')
const createKvView = require('./views/kv')
const createRecordsView = require('./views/records')
const createIndexview = require('./views/indexes')

module.exports = function (opts) {
  return new Database(opts)
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

  static decodeValue (msg) {
    const value = msg.value
    delete msg.value
    return Record.decode(value, msg)
  }

  static encode (record) {
    record = Record.encodeValue(record)
    const buf = RecordEncoding.encode(record)
    return buf
  }

  static encodeValue (record) {
    if (record.value) record.value = JSON.stringify(record.value)
    return record
  }
}

function withDecodedRecords (view) {
  return {
    ...view,
    map (messages, cb) {
      messages = messages.map(msg => {
        try {
          return Record.decodeValue(msg)
        } catch (err) {
          return null
        }
      }).filter(m => m)
      view.map(messages, cb)
    }
  }
}

class Database extends EventEmitter {
  constructor (opts = {}) {
    super()
    const self = this
    this.opts = opts
    this.key = opts.key
    if (!this.key) this.key = crypto.randomBytes(32)

    this.encoding = Record
    this.schemas = opts.schemas || new SchemaStore({ key: this.key })
    this.lvl = opts.db || memdb()

    this.corestore = opts.corestore || new Corestore(opts.storage || ram)
    this.kappa = new Kappa()

    this.state = new State(sub(this.lvl, 'state'))

    this.useRecordView('kv', createKvView)
    this.useRecordView('records', createRecordsView)
    this.useRecordView('indexes', createIndexview)

    this.useRecordView('schema', () => ({
      map (msgs, next) {
        msgs = msgs.filter(msg => msg.schema === 'core/schema')
        msgs.forEach(msg => self.schemas.put(msg.id, msg.value))
        next()
      }
    }))

    this.useRecordView('source', () => ({
      map (msgs, next) {
        msgs = msgs.filter(msg => msg.schema === 'core/source')
        let pending = msgs.length + 1
        msgs.forEach(msg => {
          const key = msg.value.key
          self.corestore.get({ key, parent: self.key })
          // self.multifeed.writer(key, { keyPair }, done)
        })
        done()
        function done () {
          if (--pending === 0) next()
        }
      }
    }))

    this._opened = false
  }

  useRecordView (name, createView, opts) {
    const self = this
    const db = sub(this.lvl, 'view.' + name)
    const view = levelBaseView(db, function (db) {
      return withDecodedRecords(createView(db, self, opts))
    })
    const source = kappaCorestoreSource({
      store: this.corestore,
      state: this.state.prefix(name)
    })
    this.kappa.use(name, source, view)
  }

  replicate (isInitiator, opts) {
    return this.corestore.replicate(isInitiator, null, opts)
  }

  ready (cb) {
    this.corestore.ready(() => {
      this.localWriter().ready(cb)
    })
    // this._initSchemas(() => {
    //   this._opened = true
    //   cb()
    // })
  }

  get localKey () {
    return this.localWriter().key
  }

  localWriter () {
    const feed = this.corestore.get({
      default: true,
      _name: 'localwriter'
    })
    return feed
  }

  _initSchemas (cb) {
    const qs = this.api.records.bySchema('core/schema', {
      live: true
    })
    this.loadStream(qs, (err, schemas) => {
      if (err) return cb(err)
      schemas.forEach(msg => this.schemas.put(msg.id, msg.value))
      cb()
    })
  }

  get api () {
    return this.kappa.view
  }

  put (record, cb) {
    record.op = RecordEncoding.Type.PUT
    record.schema = this.schemas.resolveName(record.schema)
    if (!this.schemas.validate(record)) return cb(this.schemas.error)
    if (!record.id) record.id = uuid()
    this._putRecord(record, err => err ? cb(err) : cb(null, record.id))
  }

  del (id, cb) {
    if (typeof id === 'object') id = id.id
    const record = {
      id,
      op: RecordEncoding.Type.DEL
    }
    this._putRecord(record, cb)
  }

  _putRecord (record, cb) {
    const feed = this.localWriter()
    record.timestamp = Date.now()
    this.getLinks(record, (err, links) => {
      if (err && err.status !== 404) return cb(err)
      record.links = links
      const buf = Record.encode(record)
      feed.append(buf, cb)
    })
  }

  get (req, cb) {
    this.loadStream(this.kappa.api.records.get(req), cb)
  }

  // get (id, cb) {
  //   this.kappa.api.kv.getLinks(id, (err, links) => {
  //     if (err) cb(err)
  //     else this.loadAll(links, cb)
  //   })
  // }

  getLinks (record, cb) {
    this.kappa.view.kv.getLinks(record, cb)
  }

  loadRecord (key, seq, cb) {
    const feed = this.corestore.get({ key })
    feed.get(seq, (err, record) => {
      record = Record.decode(record, { key, seq })
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
        value: schema,
        id: name
      }
      this.put(record, cb)
    })
  }

  getSchema (name) {
    return this.schemas.get(name)
  }

  getSchemas () {
    return this.schemas.list()
  }

  putSource (key, cb) {
    if (Buffer.isBuffer(key)) key = key.toString('hex')
    const record = {
      schema: 'core/source',
      id: key,
      value: {
        type: 'kappa-records',
        key
      }
    }
    console.log('put', record)
    this.put(record, cb)
  }

  loadStream (stream, cb) {
    if (typeof stream === 'function') return this.loadStream(null, stream)
    const self = this
    const transform = through(function (req, enc, next) {
      self.loadRecord(req.key, req.seq, (err, record) => {
        if (err) this.emit('error', err)
        if (record) {
          if (req.meta) record.meta = req.meta
          this.push(record)
        }
        next()
      })
    })
    if (stream) stream.pipe(transform)
    if (cb) return collect(transform, cb)
    else return transform
  }

  // loadAll (links, cb) {
  //   let pending = links.length
  //   let res = []
  //   let errs = []
  //   links.forEach(link => this.loadLink(link, (err, link) => {
  //     if (err) errs.push(err)
  //     else res.push(link)
  //     if (--pending === 0) cb(errs.length ? errs : null, links)
  //   }))
  // }

  loadLink (link, cb) {
    if (typeof link === 'string') {
      var [key, seq] = link.split('@')
    } else {
      key = link.key
      seq = link.seq
    }
    this.loadRecord(key, seq, cb)
  }
}

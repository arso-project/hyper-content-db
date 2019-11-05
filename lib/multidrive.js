const hyperdrive = require('hyperdrive')
const thunky = require('thunky')
const { EventEmitter } = require('events')
const p = require('path')

module.exports = (...args) => new Multidrive(...args)

const { P_SOURCES } = require('./constants')

class Multidrive extends EventEmitter {
  constructor (opts) {
    super()
    this._opts = opts
    this.storage = opts.storage
    this.corestore = opts.corestore
    this.key = opts.key

    this._sources = new Map()
    this.ready = thunky(this._ready.bind(this))
    this._initLocalWriter = thunky(this.__initLocalWriter.bind(this))
  }

  _ready (cb) {
    this._initSource(this.key, (err, drive) => {
      if (err) return cb(err)
      this.primaryDrive = drive
      this.key = this.primaryDrive.key
      cb()
    })
  }

  get discoveryKey () {
    return this.primaryDrive.discoveryKey
  }

  get localKey () {
    if (!this._localWriter || !this._localWriter.key) return undefined
    return this._localWriter.key
  }

  replicate (opts) {
    return this.primaryDrive.replicate(opts)
  }

  _initSource (key, opts, cb) {
    if (typeof opts === 'function') return this._initSource(key, {}, opts)
    opts = { ...this._opts, ...opts || {} }
    if (key && !Buffer.isBuffer(key)) key = Buffer.from(key, 'hex')
    const hkey = hex(key)
    if (this._sources.has(hkey)) return cb(null, this._sources.get(hkey))

    const drive = hyperdrive(this.corestore, key, opts)

    drive.ready(err => {
      if (err) return cb(err)
      this._sources.set(hex(drive.key), drive)
      this.emit('source', drive)
      this._initChildren(drive, cb)
    })
  }

  _initChildren (drive, cb) {
    cb = once(cb)
    drive.readdir(P_SOURCES, (err, list) => {
      if (err) return cb(err, drive)
      let pending = list.length + 1
      list.forEach(key => this._initSource(key, done))
      done()
      function done (err) {
        if (err) return cb(err, drive)
        if (--pending === 0) cb(null, drive)
      }
    })
  }

  hasSource (key) {
    key = hex(key)
    return this._sources.has(key)
  }

  addSource (key, cb) {
    const self = this
    const hkey = hex(key)
    if (this._sources.has(hkey)) return cb(null, this._sources.get(hkey))
    this.writer((err, drive) => {
      if (err) return cb(err)
      drive.mount(p.join(P_SOURCES, hkey), Buffer.from(hkey, 'hex'), onmounted)
    })

    function onmounted (err) {
      if (err && err.code !== 'EEXISTS') return cb(err)
      self._initSource(key, cb)
    }
  }

  sources (fn) {
    this.ready(() => {
      fn([...this._sources.values()])
    })
  }

  source (key, cb) {
    this.ready(() => {
      if (this._sources.has(hex(key))) return cb(this._sources.get(key))
      else cb()
    })
  }

  writer (cb) {
    if (this._localWriter) cb(null, this._localWriter)
    else this._initLocalWriter(err => cb(err, this._localWriter))
  }

  __initLocalWriter (cb) {
    const self = this
    this.ready((err) => {
      if (err) return done(err)
      if (this.primaryDrive.writable) return done(null, this.primaryDrive)
      else openLocalWriter()
    })

    function done (err, drive) {
      if (err || !drive) return cb(err)
      self._localWriter = drive
      cb()
    }

    function openLocalWriter () {
      const keystore = self.storage('localwriter')
      keystore.stat((err, stat) => {
        if (err || !stat || !stat.size) {
          const feed = self.corestore.get({ parents: [self.key] })
          const hkey = feed.key.toString('hex')
          keystore.write(0, Buffer.from(hkey), (err) => {
            if (err) return done(err)
            openWriter(feed.key)
          })
        } else {
          keystore.read(0, 64, (err, hexKey) => {
            if (err) return done(err)
            const key = Buffer.from(hexKey.toString(), 'hex')
            openWriter(key)
          })
        }
      })
    }

    function openWriter (key) {
      self._initSource(key, (err, drive) => {
        done(err, drive)
      })
    }
  }
}

function hex (key) {
  return Buffer.isBuffer(key) ? key.toString('hex') : key
}

function once (fn) {
  let called = false
  return (...args) => {
    if (called) return
    called = true
    fn(...args)
  }
}

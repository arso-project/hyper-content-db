const hyperdrive = require('hyperdrive')
const raf = require('random-access-file')
const crypto = require('hypercore-crypto')
const thunky = require('thunky')
const { EventEmitter } = require('events')
const p = require('path')
const corestore = require('corestore')
const { Corestore } = require('corestore')

module.exports = (...args) => new Multidrive(...args)

const { P_SOURCES } = require('./constants')

class Multidrive extends EventEmitter {
  constructor (storage, key, opts = {}) {
    super()
    this._opts = opts

    // this.storage = typeof storage === 'string' ? () => raf(storage) : storage
    if (typeof storage === 'function') {
      var factory = path => storage(path)
    } else if (typeof storage === 'string') {
      factory = path => raf(storage + '/' + path)
    }
    this.factory = factory

    this.corestore = opts.corestore || corestore(factory)

    this.primaryDrive = hyperdrive(this.corestore, key, {
      sparse: opts.sparse,
      secretKey: opts.secretKey,
      keyPair: opts.keyPair
    })

    this.ready = thunky(this._ready.bind(this))

    this._sources = new Map()
  }

  _ready (cb) {
    this._pushSource(this.primaryDrive, cb)
  }

  get key () {
    return this.primaryDrive.key
  }

  get discoveryKey () {
    return this.primaryDrive.discoveryKey
  }

  get localKey () {
    if (!this._localWriter || !this._localWriter.key) return undefined
    return this._localWriter.key
  }

  _pushSource (drive, cb) {
    cb = cb || noop
    drive.ready(err => {
      if (err) return cb(err)
      // console.log(drive.key.toString('hex').substring(0, 4), 'pushSource', drive.key.toString('hex'))

      this._sources.set(hex(drive.key), drive)
      this.emit('source', drive)

      drive.readdir(P_SOURCES, (err, list) => {
        if (err || !list.length) return cb(err, drive)
        let pending = list.length
        for (let key of list) {
          this._addSource(key, finish)
        }
        function finish (err) {
          if (err) return cb(err, drive)
          if (--pending === 0) cb(null, drive)
        }
      })
    })
  }

  _addSource (key, opts, cb) {
    if (typeof opts === 'function') return this._addSource(key, {}, opts)
    opts = { ...this._opts, ...opts || {} }
    key = hex(key)
    const drive = hyperdrive(this.corestore, Buffer.from(key, 'hex'), opts)
    this._pushSource(drive, cb)
  }

  _writeSource (key, cb) {
    key = hex(key)
    this.writer((err, drive) => {
      if (err) return cb(err)
      // drive.writeFile(p.join(P_SOURCES, hex(key)), Buffer.alloc(0), cb)
      drive.mount(p.join(P_SOURCES, key), Buffer.from(key, 'hex'), cb)
    })
  }

  addSource (key, cb) {
    key = hex(key)
    this.ready(() => {
      // console.log(this.key.toString('hex').substring(0, 4), 'addSource', key.toString('hex'))
      if (this._sources.has(hex(key))) return cb(null, this._sources.get(key))
      this._addSource(key, cb)
    })
  }

  hasSource (key) {
    key = hex(key)
    return this._sources.has(key)
  }

  saveSource (key, cb) {
    if (!key) return cb(new Error('Key is required.'))
    key = hex(key)
    this.addSource(key, err => {
      if (err) return cb(err)
      this._writeSource(key, cb)
    })
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
    else this._initWriter(cb)
  }

  _initWriter (cb) {
    const self = this
    if (!this._loadLocalWriter) this._loadLocalWriter = thunky(loadWriter)
    this._loadLocalWriter(err => cb(err, loadWriter))

    function loadWriter (cb) {
      self.ready(err => {
        if (err) return cb(err)
        if (self.primaryDrive.writable) {
          finish(null, self.primaryDrive)
        } else {
          readKey()
        }
      })

      function readKey () {
        if (self._localWriter) finish(null, self._localWriter)
        let keystore = self.factory('localwriter')
        keystore.stat((err, stat) => {
          if (err || !stat || !stat.size) return createWriter(keystore)
          keystore.read(0, 64, (err, hexKey) => {
            if (err) return finish(err)
            const key = Buffer.from(hexKey.toString(), 'hex')
            openWriter(key)
          })
        })
      }

      function createWriter (keystore) {
        const { publicKey, secretKey } = crypto.keyPair()
        const hexKey = Buffer.from(publicKey.toString('hex'))
        keystore.write(0, hexKey, err => {
          if (err) return cb(err)
          openWriter(publicKey, { secretKey })
        })
      }

      function openWriter (key, opts) {
        self._addSource(key, opts, finish)
      }

      function finish (err, drive) {
        if (err) return cb(err)
        self._localWriter = drive
        cb()
      }
    }
  }

  replicate (opts) {
    return this.primaryDrive.replicate(opts)
  }
}

function hex (key) {
  return Buffer.isBuffer(key) ? key.toString('hex') : key
}

function noop () {}

// function nestStorage (storage, prefix) {
//   prefix = prefix || ''
//   return function (name, opts) {
//     let path = p.join(prefix, name)
//     return storage(path, opts)
//   }
// }

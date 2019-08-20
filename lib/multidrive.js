const hyperdrive = require('hyperdrive')
const mutexify = require('mutexify')
const raf = require('random-access-file')
const crypto = require('hypercore-crypto')
const thunky = require('thunky')
const { EventEmitter } = require('events')
const p = require('path')
const corestore = require('corestore')

module.exports = (...args) => new Multidrive(...args)

const { P_SOURCES } = require('./constants')

class Multidrive extends EventEmitter {
  constructor (storage, key, opts) {
    super()
    opts = opts || {}
    this._opts = opts

    // this.storage = typeof storage === 'string' ? () => raf(storage) : storage
    if (typeof storage === 'function') {
      var factory = path => storage(path)
    } else if (typeof storage === 'string') {
      factory = path => raf(storage + '/' + path)
    }
    this.factory = factory

    this.corestore = corestore(factory)

    this.primaryDrive = hyperdrive(this.corestore, key, {
      sparse: opts.sparse
    })

    this.ready = thunky(this._ready.bind(this))

    this._sources = new Map()
  }

  _ready (cb) {
    this.primaryDrive.ready(err => {
      // console.log('primary drive', this.primaryDrive.key.toString('hex'))
      if (err) return cb(err)
      this.key = this.primaryDrive.key
      this.discoveryKey = this.primaryDrive.discoveryKey
      this._pushSource(this.primaryDrive, cb)
    })
  }

  _pushSource (drive, cb) {
    cb = cb || noop
    drive.ready(err => {
      if (err) return cb(err)

      this._sources.set(hex(drive.key), drive)
      this.emit('source', drive)

      drive.readdir(P_SOURCES, (err, list) => {
        if (err || !list.length) return cb(err, drive)
        let missing = list.length
        for (let source of list) {
          this._addSource(source, finish)
        }
        function finish (err) {
          if (err) return cb(err, drive)
          if (--missing === 0) cb(null, drive)
        }
      })
    })
  }

  _addSource (key, opts, cb) {
    if (typeof opts === 'function') return this._addSource(key, {}, opts)
    opts = opts || {}
    opts.sparse = opts.sparse || this._opts.sparse
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
      if (this._sources.has(hex(key))) return cb(null, this._sources.get(key))
      this._addSource(key, cb)
    })
  }

  hasSource (key) {
    key = hex(key)
    return this._sources.has(key)
  }

  saveSource (key, cb) {
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

  get localKey () {
    if (!this._localWriter || !this._localWriter.key) return undefined
    return this._localWriter.key
  }

  writer (cb) {
    const self = this
    if (this._localWriter) return cb(null, this._localWriter)
    if (!this._loadLocalWriter) this._loadLocalWriter = thunky(loadWriter)
    this._loadLocalWriter(err => cb(err, this._localWriter))

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
    // const self = this
    // if (!opts) opts = {}

    // const stream = this.primaryDrive.replicate(opts)

    // for (let drive of this._sources.values()) {
    //   addDrive(drive)
    // }

    // this.on('source', drive => addDrive(drive))

    // return stream

    // function addDrive (drive) {
    //   if (drive === self.primaryDrive) return
    //   if (stream.destroyed) return
    //   drive.replicate({
    //     live: opts.live,
    //     download: opts.download,
    //     upload: opts.upload,
    //     stream: stream
    //   })
    //   // Each hyperdrive has two feeds, so increase the amount
    //   // of expected feeds.
    //   // stream.expectedFeeds = stream.expectedFeeds + 2
    // }
  }
}

function hex (key) {
  return Buffer.isBuffer(key) ? key.toString('hex') : key
}

function noop () {}

function nestStorage (storage, prefix) {
  prefix = prefix || ''
  return function (name, opts) {
    let path = p.join(prefix, name)
    return storage(path, opts)
  }
}

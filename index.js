const hyperdrive = require('hyperdrive')
const thunky = require('thunky')
const p = require('path')
const mutexify = require('mutexify')
const { EventEmitter } = require('events')
const raf = require('random-access-file')
const crypto = require('hypercore-crypto')

const kappa = require('./kappa')

const P_SOURCES = '.sources'

module.exports = (...args) => new Contentcore(...args)

class Contentcore extends EventEmitter {
  constructor (storage, key, opts) {
    super()
    this.multidrive = new Multidrive(storage, key, opts)
    this.kcore = kappa({ multidrive: this.multidrive })
    this.ready = thunky(this._ready.bind(this))
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
}

class Multidrive extends EventEmitter {
  constructor (storage, key, opts) {
    super()
    this.storage = name => nestStorage(storage, name)

    this.primaryDrive = hyperdrive(this.storage('primary'), key)

    this.ready = thunky(this._ready.bind(this))

    this.writerLock = mutexify()

    this.sources = new Map()
  }

  _ready (cb) {
    this.primaryDrive.ready(err => {
      if (err) return cb(err)
      this.key = this.primaryDrive.key
      this._pushSource(this.primaryDrive, cb)
    })
  }

  _pushSource (drive, cb) {
    cb = cb || noop
    drive.ready(err => {
      if (err) return cb(err)

      this.sources.set(hex(drive.key), drive)
      this.emit('source', drive)
      console.log('EMIT', drive.key.toString('hex'))

      drive.readdir(P_SOURCES, (err, list) => {
        // console.log('DRIVE READDIR', err, list)
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
    // console.log('as', key, opts, cb)
    if (typeof opts === 'function') return this._addSource(key, null, opts)
    const drive = hyperdrive(this.storage(hex(key)), key, opts)
    this._pushSource(drive, cb)
  }

  _writeSource (key, cb) {
    this.writer(drive => {
      drive.writeFile(p.join(P_SOURCES, hex(key)), Buffer.alloc(0), cb)
    })
  }

  addSource (key, cb) {
    this.ready(() => {
      if (this.sources.has(hex(key))) return cb(null, this.sources.get(key))
      this._addSource(key, cb)
    })
  }

  saveSource (key, cb) {
    this.addSource(key, err => {
      if (err) return cb(err)
      this._writeSource(key, cb)
    })
  }

  writer (cb) {
    const self = this
    if (this._localWriter) return cb(null, this._localWriter)
    let release = null
    this.ready(err => {
      if (err) return cb(err)
      if (this.primaryDrive.writable) {
        finish(null, this.primaryDrive)
      } else {
        readKey()
        // self.writerLock(_release => {
        //   release = _release
        //   readKey()
        // })
      }
    })

    function readKey () {
      // console.log('rk')
      if (self._localWriter) finish(null, self._localWriter)
      let keystore = self.storage()('localwriter')
      keystore.read(0, 32, (err, key) => {
        if (err && err.code !== 'ENOENT') return finish(err)
        if (key) makeWriter({ publicKey: key })
        else {
          const keyPair = crypto.keyPair()
          keystore.write(0, keyPair.publicKey, err => {
            if (err) return cb(err)
            makeWriter(keyPair)
          })
        }
      })
    }

    function makeWriter (keyPair) {
      const { publicKey, secretKey } = keyPair
      self._addSource(publicKey, { secretKey }, finish)
    }

    function finish (err, drive) {
      self._localWriter = drive
      if (release) release()
      cb(err, drive)
    }
  }

  replicate (opts) {
    if (!opts) opts = {}

    const stream = this.primaryDrive.replicate(opts)

    for (let [key, drive] of this.sources.entries()) {
      if (drive === this.primaryDrive) continue 
      if (stream.destroyed) continue
      drive.replicate({
        live: opts.live,
        download: opts.download,
        upload: opts.upload,
        stream: stream
      })
    }

    this.on('source', drive => {
      console.log('ADD TO REPL')
      drive.replicate({
        live: opts.live,
        download: opts.download,
        upload: opts.upload,
        stream: stream
      })
    })

    return stream
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
    if (typeof storage === 'string') return raf(p.join(storage, path))
    return storage(path, opts)
  }
}

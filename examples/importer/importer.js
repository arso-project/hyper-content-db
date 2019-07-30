const cstore = require('../..')
const thunky = require('thunky')
const sonarView = require('./views/sonar')
const leveldb = require('level')
const p = require('path')
const mkdirp = require('mkdirp')

module.exports = (...args) => new Importer(...args)

class Importer {
  constructor (opts) {
    this._opts = opts
    this.ready = thunky(this._ready.bind(this))
    this.workers = []
  }

  _ready (cb) {
    const basePath = this._opts.storage
    const paths = {
      level: p.join(basePath, 'level'),
      corestore: p.join(basePath, 'corestore'),
      sonar: p.join(basePath, 'sonar')
    }
    Object.values(paths).forEach(p => mkdirp.sync(p))

    this.level = leveldb(paths.level, 'level')
    this.cstore = cstore(paths.corestore, this._opts.key, { level: this.level })
    this.cstore.useRecordView('sonar', sonarView, { storage: paths.sonar })

    this.cstore.writer((err, drive) => {
      const key = hex(this.cstore.key)
      const localKey = hex(drive.key)
      console.log('Importer ready.')
      console.log(`Primary key: ${key}`)
      console.log(`Local key:   ${localKey}`)

      this.workers.push(
        require('./webpage')(this.cstore)
      )

      cb(err)
    })
  }

  add (url, cb) {
    const self = this
    this.ready(() => {
      let idx = 0
      const handlers = []
      next(idx)

      function next (idx) {
        let worker = self.workers[idx]
        if (!worker) return done()
        worker.input(url, (handle) => {
          if (handle) handlers.push(worker)
          next(++idx)
        })
      }

      function done () {
        if (!handlers.length) return cb(new Error('No handler found for input: ' + url))
        if (handlers.length > 1) return cb(new Error('Conflicting handlers found: ' + handlers.map(h => h.label)))
        handle(handlers[0])
      }

      function handle (handler) {
        const msg = {
          id: cstore.id(),
          url
        }
        handler.handle(msg, (err, statusStream) => {
          if (err) return cb(err)
          statusStream.on('data', msg => console.log('MSG', msg))
          statusStream.on('end', () => cb())
        })
      }
    })
  }
}

function hex (key) {
  return Buffer.isBuffer(key) ? key.toString('hex') : key
}

const cstore = require('../..')
const thunky = require('thunky')
const sonarView = require('./views/sonar')

module.exports = (...args) => new Importer(...args)

class Importer {
  constructor (opts) {
    this._opts = opts
    this.cstore = cstore(opts.storage, opts.key)
    this.ready = thunky(this._ready.bind(this))
    this.workers = []

    this.cstore.useRecordView('sonar', sonarView)
  }

  _ready (cb) {
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

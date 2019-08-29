// NOTE: This is copy-pasted from
// https://github.com/kappa-db/kappa-core/blob/master/index.js
// and adapted to work with multidrive-index instead of
// multifeed-index. Once things become a bit more stable,
// likely we'd want to work on a PR to kappa-core to allow
// swapping out the indexing backend.

var inherits = require('inherits')
var EventEmitter = require('events').EventEmitter
var indexer = require('./multidrive-index')
// const debug = require('debug')('kappa')

module.exports = Kappa

function Kappa (opts) {
  if (!(this instanceof Kappa)) return new Kappa(opts)
  if (!opts) opts = {}

  this._logs = opts.multidrive
  this._indexes = {}
  this._running = new Set()

  this._viewContext = opts.viewContext || this

  this.api = {}
}

inherits(Kappa, EventEmitter)

Kappa.prototype.use = function (name, version, view) {
  if (typeof version !== 'number') {
    view = version
    version = undefined
  }
  var idx = indexer({
    multidrive: this._logs,

    name,

    prefix: view.prefix,
    map: view.map,
    readFile: view.readFile,
    fetchState: view.fetchState,
    storeState: view.storeState,
    batchSize: view.batchSize || 100

    // NOTE: kappa-core also has these.
    // version: version,
    // maxBatch: view.maxBatch || 10,
    // batch: view.map,
  })
  idx.name = name

  // TODO: Rethink event names.
  idx.on('indexed', (driveKey, batch) => {
    this.emit('indexed', name, batch, driveKey)
    if (view.indexed) view.indexed(batch, driveKey)
  })

  idx.on('indexed-all', () => {
    this._running.delete(name)
    if (!this._running.size) this.emit('indexed-all')
  })

  idx.on('start', () => {
    if (!this._running.size) this.emit('start')
    this._running.add(name)
  })

  // idx.on('error', function (err) {
  //   self.emit('error', err)
  // })
  // if (view.indexed) idx.on('indexed', view.indexed)
  const context = this._viewContext
  this._indexes[name] = idx
  this.api[name] = {}
  this.api[name].ready = idx.ready.bind(idx)
  for (var key in view.api) {
    if (typeof view.api[key] === 'function') this.api[name][key] = view.api[key].bind(view.api, context)
    else this.api[name][key] = view.api[key]
  }
}

// Kappa.prototype.feeds = function () {
//   return this._logs.feeds()
// }

Kappa.prototype.ready = function (viewNames, cb) {
  if (typeof viewNames === 'function') {
    cb = viewNames
    viewNames = []
  }

  if (typeof viewNames === 'string') viewNames = [viewNames]
  if (viewNames.length === 0) {
    viewNames = Object.keys(this._indexes)
  }

  var pending = viewNames.length
  var self = this
  this._logs.ready(function () {
    for (var i = 0; i < viewNames.length; i++) {
      self._indexes[viewNames[i]].ready(done)
    }
  })

  function done () {
    if (!--pending) cb()
  }
}

Kappa.prototype.pause = function (viewNames, cb) {
  if (typeof viewNames === 'function') {
    cb = viewNames
    viewNames = []
  }
  cb = cb || noop

  if (!viewNames) viewNames = []
  if (typeof viewNames === 'string') viewNames = [viewNames]
  if (viewNames.length === 0) {
    viewNames = Object.keys(this._indexes)
  }

  var pending = viewNames.length
  var self = this
  this._logs.ready(function () {
    for (var i = 0; i < viewNames.length; i++) {
      self._indexes[viewNames[i]].pause(done)
    }
  })

  function done () {
    if (!--pending) cb()
  }
}

Kappa.prototype.resume = function (viewNames) {
  if (!viewNames) viewNames = []
  if (typeof viewNames === 'string') viewNames = [viewNames]
  if (viewNames.length === 0) {
    viewNames = Object.keys(this._indexes)
  }

  var self = this
  this._logs.ready(function () {
    for (var i = 0; i < viewNames.length; i++) {
      self._indexes[viewNames[i]].resume()
    }
  })
}

Kappa.prototype.writer = function (name, cb) {
  this._logs.writer(name, cb)
}

// Kappa.prototype.feed = function (key) {
//   return this._logs.feed(key)
// }

Kappa.prototype.replicate = function (opts) {
  return this._logs.replicate(opts)
}

function noop () {}

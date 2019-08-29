const thunky = require('thunky')
const { EventEmitter } = require('events')
// const debug = require('debug')('multidrive-index')

const hyperdriveIndex = require('./hyperdrive-index')
const { State } = require('./messages')

module.exports = (...args) => new MultidriveIndex(...args)

let cnt = 0

class MultidriveIndex extends EventEmitter {
  constructor (opts) {
    super()
    this.multidrive = opts.multidrive
    this.name = opts.name || 'index' + cnt++

    ensureStateHandlers(opts)

    this._opts = opts
    this._storeState = opts.storeState
    this._fetchState = opts.fetchState
    this._clearIndex = opts.clearIndex || null

    this._states = {}
    this._indexes = new Map()
    this._running = new Set()

    this.ready = thunky(this._ready.bind(this))
    this.ready()
  }

  _ready (cb) {
    this.multidrive.sources(sources => {
      // this._label = this.multidrive.key.toString('hex').substring(0, 4) + ':' + (this.multidrive.primaryDrive.writable ? 'w' : 'r') + ':' + this.name
      sources.forEach(source => this._onsource(source))
      this.multidrive.on('source', this._onsource.bind(this))
    })
  }

  pause (cb) {
    for (let idx of this._running) {
      idx.pause()
    }
    if (cb) cb()
  }

  resume (cb) {
    for (let idx of this._running) {
      idx.resume()
    }
    if (cb) cb()
  }

  _onsource (drive) {
    const key = drive.key.toString('hex')
    if (this._indexes.has(key)) return

    const opts = {
      map: this._opts.map,
      batchSize: this._opts.batchSize,
      prefix: this._opts.prefix,
      readFile: this._opts.readFile,
      storeState: (state, cb) => this._storeDriveState(key, state, cb),
      fetchState: (cb) => this._fetchDriveState(key, cb)
    }

    const index = hyperdriveIndex(drive, opts)
    this._indexes.set(key, index)

    index.on('start', () => {
      // debug(this._label, 'start', key.substring(0, 4))
      if (!this._running.size) this.emit('start')
      this._running.add(key.toString('hex'))
    })

    index.on('indexed', (nodes, complete) => {
      // debug(this._label, 'indexed', key.substring(0, 4))
      if (!complete) return
      this.emit('indexed', drive.key, nodes)
      this._running.delete(key.toString('hex'))
      if (!this._running.size) this.emit('indexed-all')
    })
  }

  _storeDriveState (key, state, cb) {
    this._states[key] = state
    let buf = encodeStates(this._states)
    this._storeState(buf, cb)
  }

  _fetchDriveState (key, cb) {
    this._fetchState((err, buf) => {
      if (err) return cb(err)
      this._states = decodeStates(buf)
      cb(null, this._states[key])
    })
  }
}

function encodeStates (states) {
  const statesArray = []
  for (let [key, state] of Object.entries(states)) {
    statesArray.push({ key, state })
  }
  return State.encode({ states: statesArray })
}

function decodeStates (buf) {
  if (!buf) return {}
  const value = State.decode(buf)
  const states = {}
  value.states.forEach(({ key, state }) => {
    states[key] = state
  })
  return states
}

function ensureStateHandlers (opts) {
  if (!opts.storeState && !opts.fetchState && !opts.clearIndex) {
    // In-memory storage implementation
    let state
    opts.storeState = function (buf, cb) {
      state = buf
      process.nextTick(cb)
    }
    opts.fetchState = function (cb) {
      process.nextTick(cb, null, state)
    }
    opts.clearIndex = function (cb) {
      state = null
      process.nextTick(cb)
    }
  }
}

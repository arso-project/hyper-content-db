const { State } = require('./lib/messages')
const hypertrieIndex = require('hypertrie-index')
const thunky = require('thunky')
const { Stat } = require('hyperdrive/lib/messages')

module.exports = (...args) => new MultidriveIndex(...args)

class MultidriveIndex {
  constructor (opts) {
    this.multidrive = opts.multidrive
    this._map = opts.map
    this._readFile = opts.readFile

    this._states = {}

    if (!opts.storeState && !opts.fetchState && !opts.clearIndex) {
    // In-memory storage implementation
      let state
      this._storeState = function (buf, cb) {
        state = buf
        process.nextTick(cb)
      }
      this._fetchState = function (cb) {
        process.nextTick(cb, null, state)
      }
      this._clearIndex = function (cb) {
        state = null
        process.nextTick(cb)
      }
    } else {
      this._storeState = opts.storeState
      this._fetchState = opts.fetchState
      this._clearIndex = opts.clearIndex || null
    }

    this.ready = thunky(this._ready.bind(this))
    this.multidrive.on('source', this.source.bind(this))
  }

  _ready (cb) {
    this.multidrive.ready(() => {
      this.multidrive.sources.forEach(source => this.source(source))
    })
  }

  source (drive) {
    const self = this
    const opts = {
      map,
      storeState: (state, cb) => this._storeDriveState(drive.key, state, cb),
      fetchState: (cb) => this._fetchDriveState(drive.key, cb)
    }
    const index = hypertrieIndex(drive._db, opts)
    // const index = hypertrieIndex(drive, opts)
    function map (msg) {
      msg = hypertrieIndex.transformNode(msg, Stat)
      msg.driveKey = drive.key
      if (self._readFile) {
        const checkout = drive.checkout(msg.seq)
        checkout.readFile(msg.key, (err, data) => {
          if (err) finish(err, msg)
          msg.fileContent = data
          finish(null, msg)
        })
      } else {
        finish (null, msg)
      }

      function finish (err, msg) {
        // todo: handle err
        self._map(msg)
      }
    }
  }

  _storeDriveState (key, state, cb) {
    this._states[key] = state
    let buf = this._encodeStates(states)
    this._storeState(buf, cb)
  }

  _fetchDriveState (key, cb) {
    this._fetchState((err, data) => {
      if (err) return cb(err)
      const states = this._decodeStates(data)
      const state = states[key]
      cb(null, state)
    })
  }

  _encodeStates () {
    const states = this._states.entries.map((key, state) => ({ key, state }))
    return State.encode({ states })
  }

  _decodeStates (buf) {
    if (!buf) return {}
    let states = State.decode(buf)
    this._states = states.reduce((agg, row) => {
      agg[row.key] = row.state
      return agg
    }, {})
    return this._states
  }
}

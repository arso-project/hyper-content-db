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

    this._states = new Map()
    this._indexes = new Map()

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
    console.log('new source', drive.key.toString('hex').substring(0, 5))
    const self = this
    const opts = {
      map,
      storeState: (state, cb) => this._storeDriveState(drive.key, state, cb),
      fetchState: (cb) => this._fetchDriveState(drive.key, cb)
    }
    const index = hypertrieIndex(drive._db, opts)
    this._indexes.set(drive.key, index)
    // const index = hypertrieIndex(drive, opts)
    function map (msgs, done) {
      console.log('MAP on', drive.key.toString('hex').substring(0, 5))
      collect(msgs, finish, (msg, next) => {
        msg = hypertrieIndex.transformNode(msg, Stat)
        msg.driveKey = drive.key
        if (self._readFile) {
          const checkout = drive.checkout(msg.seq)
          checkout.readFile(msg.key, (err, data) => {
            if (err) next(err, msg)
            msg.fileContent = data
            next(null, msg)
          })
        } else {
          next(null, msg)
        }
      })

      function finish (err, msgs) {
        // todo: handle err
        self._map(msgs, done)
      }
    }
  }

  _storeDriveState (key, state, cb) {
    this._states.set(key, state)
    let buf = this._encodeStates()
    this._storeState(buf, cb)
  }

  _fetchDriveState (key, cb) {
    this._fetchState((err, data) => {
      if (err) return cb(err)
      this._decodeStates(data)
      const state = this._states.get(key)
      cb(null, state)
    })
  }

  _encodeStates () {
    const states = []
    for (let [key, state] of this._states.entries()) {
      states.push({ key, state })
    }
    return State.encode({ states })
  }

  _decodeStates (buf) {
    if (!buf) return {}
    let value = State.decode(buf)
    value.states.forEach(({ key, state }) => {
      this._states.set(key, state)
    })
    return this._states
  }
}

function collect (msgs, done, fn) {
  let missing = msgs.length
  let nextMsgs = []
  let errors = []
  msgs.forEach((msg, i) => {
    fn(msg, (err, msg) => {
      if (err) errors[i] = err
      nextMsgs[i] = msg
      if (--missing === 0) done(errors.length ? errors : null, nextMsgs)
    })
  })
}

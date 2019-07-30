const { State } = require('./lib/messages')
const hypertrieIndex = require('hypertrie-index')
const thunky = require('thunky')
// const { messages: { Stat: StatEncoder } } = require('hyperdrive-schemas')
const StatEncoder = require('hyperdrive/lib/stat')
const inspect = require('inspect-custom-symbol')
const { EventEmitter } = require('events')

module.exports = (...args) => new MultidriveIndex(...args)

class MultidriveIndex extends EventEmitter {
  constructor (opts) {
    super()
    this.multidrive = opts.multidrive
    this._opts = opts
    this._map = opts.map
    this._readFile = opts.readFile

    this._states = {}
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
    this.multidrive.on('source', this._source.bind(this))
    this.ready()
  }

  _ready (cb) {
    this.multidrive.ready(() => {
      this.multidrive.sources(sources => {
        sources.forEach(source => this._source(source))
      })
    })
  }

  _source (drive) {
    if (this._indexes.has(drive.key)) return
    const self = this
    const opts = {
      map,
      prefix: this._opts.prefix,
      storeState: (state, cb) => this._storeDriveState(drive.key, state, cb),
      fetchState: (cb) => this._fetchDriveState(drive.key, cb)
    }

    const index = hypertrieIndex(drive._db._trie, opts)
    this._indexes.set(drive.key, index)

    index.on('indexed', (nodes) => {
      if (nodes && nodes.length) {
        // console.log('multidrive-index indexed', this.name, nodes)
        this.emit('indexed', drive.key, nodes)
      }
    })

    function map (msgs, done) {
      collect(msgs, finish, (msg, next) => {
        msg = hypertrieIndex.transformNode(msg, StatEncoder)
        msg.source = drive.key
        overrideInspect(msg)
        if (self._readFile) {
          // const checkout = drive.checkout(msg.seq)
          drive.readFile(msg.key, (err, data) => {
            if (err) next(err, msg)
            msg.fileContent = data
            next(null, msg)
          })
        } else {
          next(null, msg)
        }
      })

      function finish (err, msgs) {
        // todo: handle err better?
        if (err) self.emit('error', err)
        self._map(msgs, done)
      }
    }
  }

  _storeDriveState (key, state, cb) {
    this._states[key.toString('hex')] = state
    let buf = this._encodeStates()
    this._storeState(buf, cb)
  }

  _fetchDriveState (key, cb) {
    this._fetchState((err, data) => {
      if (err) return cb(err)
      this._decodeStates(data)
      const state = this._states[key.toString('hex')]
      cb(null, state)
    })
  }

  _encodeStates () {
    const states = []
    for (let [key, state] of Object.entries(this._states)) {
      states.push({ key, state })
    }
    return State.encode({ states })
  }

  _decodeStates (buf) {
    if (!buf) return {}
    let value = State.decode(buf)
    value.states.forEach(({ key, state }) => {
      this._states[key] = state
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

function overrideInspect (msg) {
  const keys = ['seq', 'key', 'value', 'source', 'fileContent']
  msg[inspect] = function (depth, opts) {
    return keys.reduce((agg, key) => {
      agg[key] = msg[key]
      return agg
    }, {})
  }
}

const { State } = require('./lib/messages')
const hypertrieIndex = require('hypertrie-index')
const thunky = require('thunky')
// const { messages: { Stat: StatEncoder } } = require('hyperdrive-schemas')
const StatEncoder = require('hyperdrive/lib/stat')
const inspect = require('inspect-custom-symbol')
const { EventEmitter } = require('events')
const debug = require('debug')('multidrive-index')

module.exports = (...args) => new MultidriveIndex(...args)

let cnt = 0

class MultidriveIndex extends EventEmitter {
  constructor (opts) {
    super()
    this.multidrive = opts.multidrive
    this._opts = opts
    this._map = opts.map
    this._readFile = opts.readFile
    this.name = opts.name || 'index' + cnt++

    this._states = {}
    this._indexes = new Map()
    this._running = new Set()

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
    const key = drive.key.toString('hex')
    if (this._indexes.has(key)) return
    const self = this
    const opts = {
      map,
      batchSize: this._opts.batchSize,
      prefix: this._opts.prefix,
      storeState: (state, cb) => this._storeDriveState(drive.key, state, cb),
      fetchState: (cb) => this._fetchDriveState(drive.key, cb),
      // This should not really be needed, but the hypertrie
      // logic does not directly comply to the interface expected
      // by the codecs module. TODO: PR to hyperdrive.
      valueEncoding: {
        encode: stat => stat.encode(),
        decode: StatEncoder.decode
      },
      transformNode: true
    }
    // console.log('create index', this.name, opts.batchSize)

    const index = hypertrieIndex(drive._db._trie, opts)
    this._indexes.set(key, index)

    index.on('indexed', (nodes, complete) => {
      if (complete && nodes && nodes.length) {
        debug('indexed', this.name, nodes.length, drive.key.toString('hex'))
        this.emit('indexed', drive.key, nodes)
        this._running.delete(key)
        if (!this._running.size) this.emit('indexed-all')
      }
    })

    index.on('start', () => {
      if (!this._running.size) this.emit('start')
      this._running.add(key)
    })

    function map (msgs, done) {
      collect(msgs, finish, (msg, next) => {
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
        if (!msgs.length) return
        self._map(msgs, () => {
          done()
        })
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

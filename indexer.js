const sub = require('subleveldown')
const collect = require('stream-collector')
const { Transform } = require('stream')

class Log {
  constructor (db) {
    this.db = db
    this._queue = []
    this._lock = false
    this._cbs = []
  }

  append (val) {
    this._queue.push(val)
  }

  flush (cb) {
    if (!this._queue.length) return cb()
    this._cbs.push(cb)
    if (this._lock) return
    this._lock = true

    // console.log('flush', this._queue)
    const queue = this._queue
    this._queue = []
    this.head((err, seq) => {
      // console.log('head', seq)
      if (err) return cb(err)
      const ops = []
      for (let value of queue) {
        ops.push({ type: 'put', key: ++seq, value })
      }
      ops.push({ type: 'put', key: 'MAX', value: seq })
      this.db.batch(ops, () => {
        this._cbs.forEach(cb => cb())
        this._lock = false
        this._cbs = []
      })
    })
  }

  createReadStream (opts) {
    const transform = new Transform({
      objectMode: true,
      transform (chunk, enc, next) {
        this.push({ seq: Number(chunk.key), value: chunk.value })
        next()
      }
    })
    return this.db.createReadStream(opts).pipe(transform)
  }

  head (cb) {
    this.db.get('MAX', (err, value) => {
      if (err) return cb(null, 0)
      cb(null, value)
    })
  }
}

class MaterializedFeed {
  constructor (db) {
    this.db = db
    this.log = new Log(sub(db, 'l'))
    // this.keys = new Log(sub(db, 'k'))
    this.max = sub(db, 'm')
    this._keyMap = {}
    this._max = {}
    this._queue = []
  }

  ready (cb) {
    this._readMax(cb)
    // this._readKeys(() => this._readMax(cb))
  }

  head (cb) {
    this.log.head(cb)
  }

  keyHead (key) {
    return this._max[key] || 0
  }

  createReadStream (start, end) {
    const self = this
    const opts = { lte: end, gte: start }
    const transform = new Transform({
      objectMode: true,
      transform (chunk, enc, next) {
        // console.log('T', chunk)
        const [key, seq] = chunk.value.split('@')
        const gseq = chunk.seq
        this.push({ key, seq: Number(seq), gseq })
        next()
      }
    })
    const rs = this.log.createReadStream(opts)
    return rs.pipe(transform)
  }

  getNext (start, count, cb) {
    collect(this.createReadStream(start, start + count), cb)
  }

  _readMax (cb) {
    collect(this.max.createReadStream(), (err, rows) => {
      if (err) {
        this._max = {}
        return
      }
      this._max = rows.reduce((agg, row) => {
        agg[row.key] = row.value
        return agg
      }, {})
    })
  }

  // _readKeys (cb) {
  //   collect(this.keys.createReadStream(), (err, rows) => {
  //     if (err) return cb(err)
  //     this._keyMap = rows.reduce((agg, row) => {
  //       agg[row.value] = agg[row.seq]
  //       return agg
  //     }, {})
  //     cb()
  //   })
  // }

  append (key, seq) {
    this._queue.push({ key, seq })
    if (!this._max[key] || seq > this._max[key]) this._max[key] = seq
  }

  // _putKeys (keys, cb) {
  //   let missing = []
  //   keys.forEach(key => {
  //     if (!this._keyMap[key]) missing.push(key)
  //   })

  //   missing.forEach(key => this.keys.append(key))
  //   this.keys.flush(() => {
  //     this._readKeys(cb)
  //   })
  // }

  _putMax (vals, cb) {
    const ops = Object.entries(vals).map(([key, seq]) => {
      this._max[key] = seq
      return { type: 'put', key, value: seq }
    })
    this.max.batch(ops, cb)
  }

  flush (cb) {
    if (!this._queue.length) return cb()
    // console.log('flush', this._queue)
    const queue = this._queue
    this._queue = []
    queue.forEach(entry => this.log.append(entry.key + '@' + entry.seq))
    this.log.flush(cb)
    // const keys = Array.from(new Set(queue.map(x => x.key)))
    // this._putKeys(keys, () => {
      // const max = {}
      // queue.forEach(entry => {
      //   const { key, seq } = entry
      //   const keyid = this._keyMap[entry.key]
      //   if (seq > this._max[key] && (!max[key] || seq > max[key])) {
      //     max[key] = seq
      //   }
      //   const value = key + '@' + entry.seq
      //   this.log.append(value)
      // })
      // this.log.flush(() => {
      //   this.log.head((err, seq) => {
      //     if (err) return cb(err)
      //     this._head = seq || 0
      //     console.log('flushed!', seq)
      //     this._putMax(max, cb)
      //   })
      // })
    // })
  }
}

module.exports = class Indexer {
  constructor (opts) {
    this.opts = opts
    this.getFeed = opts.getFeed
    this.activeFeeds = {}
    this.db = opts.level
    this.log = new MaterializedFeed(sub(this.db, 'f'))
    this.state = sub(this.db, 's')
    this._watchers = []
    this._lastlength = {}
    this.encoding = opts.encoding
    this.log.ready()
  }

  add (key) {
    if (Buffer.isBuffer(key)) key = key.toString('hex')
    if (this.activeFeeds[key]) return
    const feed = this.getFeed(key)
    this.activeFeeds[key] = feed
    feed.ready(() => {
      this._onappend(feed.key)
      feed.on('append', () => {
        this._onappend(feed.key)
      })
      feed.on('download', (index, data, from) => {
        this._ondownload(feed.key, index)
      })
    })
  }

  _onappend (key) {
    if (Buffer.isBuffer(key)) key = key.toString('hex')
    const seq = this.activeFeeds[key].length - 1
    let last = this.log.keyHead(key)
    if (!last) last = -1
    for (let i = last + 1; i <= seq; i++) {
      this.log.append(key, i)
    }
    this.onupdate()
    // this.log.flush(() => this.onupdate())
  }

  _ondownload (key, seq, data) {
    if (Buffer.isBuffer(key)) key = key.toString('hex')
    this.log.get(key, seq, (err, seq) => {
      if (err || !seq) {
        this.log.append(key, seq)
        this.onupdate()
        // this.log.flush(() => this.onupdate())
      }
    })
  }

  onupdate () {
    this._watchers.forEach(fn => fn())
  }

  read (start, end, cb) {
    const self = this
    this.log.flush(() => {
      const rs = this.log.createReadStream(start, end)
      const transform = new Transform({
        objectMode: true,
        transform (row, enc, next) {
          const { key, seq, gseq } = row
          const feed = self.getFeed(key)
          if (!feed) return next()
          feed.get(seq, (err, value) => {
            if (err) return next()
            let msg
            if (self.encoding) msg = self.encoding.decode(value, { key, seq, gseq })
            else msg = { key, seq, gseq, value }
            this.push(msg)
            next()
          })
        }
      })
      collect(rs.pipe(transform), (err, msgs) => {
        cb(null, msgs)
      })
    })
  }

  createSource () {
    const self = this
    let db
    let i = 0
    let state = 0
    return {
      open (cb, flow) {
        db = sub(self.state, flow.name)
        db.__foo = flow.name
        self._watchers.push(() => flow.update())
        cb()
      },
      pull (next) {
        // console.log('pull a', db.__foo, ++i)
        // db.get('state', (err, state) => {
          // console.log('pull b', err && err.name, state)
          // if (err && !err.notFound) return
          state = state || 0
          state = Number(state)
          self.read(state, state + 50, (err, messages) => {
            // console.log('pull read', state, messages.length)
            // console.log('read', messages.length)
            if (err) return
            next({
              messages,
              pending: !!messages.length,
              onindexed (cb) {
                state = state + messages.length
                cb()
                // db.put('state', '' + state + messages.length, (err) => {
                //   console.log('state put', i, db.__foo, state + messages.length, err)
                //   cb()
                // })
              }
            })
          })
        // })
      }
    }
  }
}

function once (fn) {
  let didrun = false
  return function (...args) {
    if (didrun) return
    didrun = true
    fn(...args)
  }
}

// class Indexer {
//   constructor (opts) {
//     this.db = opts.db
//     this.log = new MaterializedFeed(su('f', this.db))
//     this.state = sub('s', this.db)
//   }

//   map (msgs, next) {
//     for (const msg of msgs) {
//       this.log.append(msg.key, msg.seq)
//     }
//     this.log.flush(next)
//   }

//   get api () {
//     const self = this
//     return {
//       createSource () {
//         let db
//         return {
//           open (cb, { name }) {
//             db = sub(name, self.state)
//           },
//           pull (next) {
//             db.get('state', (err, state) => {
//               if (err) return cb(err)
//               self.log.getNext(state, 50, (err, messages) => {
//                 next({
//                   messages,
//                   pending: messages.length,
//                   onindexed: cb => db.put('state', state + messages.length, cb)
//                 })
//               })
//             })
//           }
//         }
//       }
//     }
//   }
// }

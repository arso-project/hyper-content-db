module.exports = class SimpleState {
  constructor (opts = {}) {
    this.db = opts.db
    this._prefix = opts.prefix || ''
  }

  get _STATE () {
    return this._prefix + '!state!'
  }

  get _VERSION () {
    return this._prefix + '!version!'
  }

  prefix (prefix) {
    return new SimpleState({
      db: this.db,
      prefix: this._prefix + '/' + prefix
    })
  }

  get (name, cb) {
    if (!cb) return this.get('', name)
    const key = this._STATE + name
    getInt(this.db, key, cb)
  }

  put (name, seq, cb) {
    if (!cb) return this.put('', name, seq)
    const key = this._STATE + name
    putInt(this.db, key, seq, cb)
  }

  storeVersion (version, cb) {
    putInt(this.db, this._VERSION, version, cb)
  }

  fetchVersion (cb) {
    getInt(this.db, this._VERSION, cb)
  }
}

function getInt (db, key, cb) {
  db.get(key, (err, value) => {
    if (err) return cb(null, 0)
    if (!value || value === '') return cb(null, 0)
    const int = Number(value)
    cb(null, int)
  })
}

function putInt (db, key, int, cb) {
  db.put(key, int, cb || noop)
}

function noop () {}

// module.exports = class StatefulSource {
//   constructor (opts) {
//     this.state = new SimpleState(opts)
//   }

//   fetchVersion (cb) {
//     this.state.getVersion(cb)
//   }

//   storeVersion (version, cb) {
//     this.state.setVersion(version, cb)
//   }
// }


const thunky = require('thunky')
const p = require('path')
const { EventEmitter } = require('events')
const hyperid = require('hyperid')
const memdb = require('memdb')
const sub = require('subleveldown')
const makeView = require('kappa-view')

const multidrive = require('./multidrive')
const kappa = require('./kappa')

const P_DATA = '.data'
const END = '\uffff'

module.exports = (...args) => new Contentcore(...args)
module.exports.id = () => Contentcore.id()

function contentView (ldb, opts) {
  const view = makeView(ldb, (db) => {
    return {
      prefix: '.data',
      transformNodes: true,
      readFile: true,
      map (msgs, next) {
        let ops = []
        let missing = msgs.length
        msgs = msgs.map(msg => {
          const id = msg.keySplit.pop().replace(/\.json$/, '')
          const schema = msg.keySplit.slice(1).join('/')
          msg = {
            id,
            schema,
            delete: msg.delete,
            stat: msg.value,
            value: msg.fileContent,
            source: msg.source.toString('hex'),
            seq: msg.seq
          }

          opts.map(msg, finish)
          // let res = opts.map(msg)
        })

        function finish (res) {
          if (res && Array.isArray(res)) {
            ops.push.apply(ops, res)
          } else if (typeof res === 'object') {
            ops.push(res)
          }
          if (--missing === 0) {
            console.log('finish', ops)
            ldb.batch(ops, err => {
              // TODO: This error went through silently!!
              console.log('BATCH written', err)
              next(err)
            })
          }
        }
      },
      api: opts.api
    }
  })
  return view
}

function entityView (db) {
  return {
    map (msg, next) {
      const { id, schema, seq, source } = msg
      let value = `${source}@${seq}`
      let rows = [
        [`is|${id}|${schema}`, value],
        [`si|${schema}|${id}`, value]
      ]
      next(rows.map(r => ({ type: 'put', key: r[0], value: r[1] })))
    },
    api: {
      all (kcore, cb) {
        let ids = {}
        let rs = db.createReadStream({
          gt: 'is|',
          lt: 'is|' + END
        })
        rs.on('data', row => {
          let [id, schema] = row.key.split('|').slice(1)
          let [source, seq] = row.value.split('@')
          ids[id] = ids[id] || []
          ids[id].push({ schema, source, seq })
        })
        rs.on('end', () => cb(null, ids))
      },
      allWithSchema (kcore, schema, cb) {
        let ids = {}
        let rs = db.createReadStream({
          gt: `si|${schema}|`,
          lt: `si|${schema}|` + END
        })
        rs.on('data', row => {
          let id = row.key.split('|').pop()
          let [source, seq] = row.value.split('@')
          ids[id] = ids[id] || []
          ids[id].push({ schema, source, seq })
        })
        rs.on('end', () => cb(null, ids))
      }
    }
  }
}

class Contentcore extends EventEmitter {
  constructor (storage, key, opts) {
    super()
    opts = opts || {}
    this.multidrive = multidrive(storage, key, opts)
    this.kcore = kappa({ multidrive: this.multidrive })
    this.ready = thunky(this._ready.bind(this))

    this.level = opts.level || memdb()

    this.api = {}

    this.kcore.on('indexed', (...args) => this.emit('indexed', ...args))

    this.useLevelView('entities', entityView)

    // this.dbs = {}
    // this.dbs.entities = sub(this.level, 'e')
    // const view = contentView(this.dbs.entities, {
    //   map (msg) {
    //     const { id, schema, seq, source } = msg
    //     let value = `${source}@${seq}`
    //     return [
    //       [`is!${id}!${schema}`, value],
    //       [`si!${schema}!${id}`, value]
    //     ]
    //   },
    //   // api: {
    //   //   allIds
    //   // }
    // })
    // this.kcore.use('entities', {
    //   prefix: '.data',
    //   map (msgs, next) {
    //     // console.log('MAP', msgs.map(m => ({...m})))
    //     console.log('MAP', msgs)
    //   }
    // })
  }

  useLevelView (name, makeInnerView) {
    const db = sub(this.level, 'view.' + name)
    const innerView = makeInnerView(db)
    const view = contentView(db, innerView)
    this.kcore.use(name, view)
    this.api[name] = this.kcore.api[name]
  }

  _ready (cb) {
    this.multidrive.ready(err => {
      if (err) return cb(err)
      this.key = this.multidrive.key
      cb(null)
    })
  }

  use (view, opts) {
    this.kcore.use(view, opts)
  }

  writer (cb) {
    this.multidrive.writer(cb)
  }

  replicate (opts) {
    return this.multidrive.replicate(opts)
  }

  addSource (key, cb) {
    this.multidrive.addSource(key, cb)
  }

  sources (cb) {
    this.multidrive.sources(cb)
  }

  batch (msgs, cb) {
    const results = []
    const errors = []
    let missing = 0

    msgs.forEach(msg => {
      missing++
      if (msg.op === 'put') this.putRecord(msg.schema, msg.id, msg.record, finish)
      // if (msg.op === 'del') this.putRecord(msg.schema, msg.id, msg.record, finish)
      else finish()
    })

    function finish (err, result) {
      if (err) errors.push(err)
      if (result) results.push(result)
      if (--missing === 0) cb(errors.length && errors, results)
    }
  }

  putRecord (schema, id, record, cb) {
    // Schema names have to have exactly one slash.
    if (!validateSchemaName(schema)) return cb(new Error('Invalid schema name: ' + schema))

    this.writer((err, drive) => {
      if (err) return cb(err)
      const path = makePath(schema, id)
      const buf = Buffer.from(JSON.stringify(record))
      drive.writeFile(path, buf, (err) => {
        if (err) return cb(err)
        cb(null, id)
      })
    })
  }

  getRecords (schema, id, cb) {
    if (!validateSchemaName(schema)) return cb(new Error('Invalid schema name: ' + schema))
    const records = []
    const errors = []
    let missing = 0
    this.sources((err, drives) => {
      if (err) return cb(err)
      drives.forEach(drive => {
        missing++
        const path = makePath(schema, id)
        const key = hex(drive.key)
        const msg = { path, source: key, id, schema }
        drive.stat(path, (err, stat) => {
          if (err) return onrecord(null)
          drive.readFile(path, (err, buf) => {
            msg.stat = stat
            if (err) return onrecord({ ...msg, error: err })
            try {
              const value = JSON.parse(buf.toString())
              msg.value = value
              onrecord(msg)
            } catch (err) {
              onrecord({ ...msg, error: err })
            }
          })
        })
      })
    })

    function onrecord (msg) {
      if (msg && msg.error) errors.push(msg)
      else if (msg) records.push(msg)
      if (--missing === 0) {
        let error = errors.length ? errors : null
        cb(error, records)
      }
    }
  }

  listRecords (schema, cb) {
    let ids = []
    let missing = 0
    this.sources((err, drives) => {
      if (err) return cb(err)
      drives.forEach(drive => {
        let path = p.join(P_DATA, schema)
        missing++
        drive.readdir(path, (err, list) => {
          if (err) return finish(err)
          if (!list.length) return finish()
          list = list.map(id => id.replace(/\.json$/, ''))
          finish(null, list)
        })
      })
    })

    function finish (err, list) {
      if (!err && list) {
        ids = [...ids, ...list]
      }
      if (--missing === 0) cb(null, list)
    }
  }
}

Contentcore.id = hyperid({ fixedLength: true, urlSafe: true })

function makePath (schema, id) {
  return p.join(P_DATA, schema, id + '.json')
}

function validateSchemaName (schema) {
  return schema.split('/').length === 2
}

function hex (key) {
  return Buffer.isBuffer(key) ? key.toString('hex') : key
}

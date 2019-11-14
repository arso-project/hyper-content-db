const { P_DATA } = require('../lib/constants')

/**
 * This transforms messages as they come from kappa5's
 * hyperdriveSource into record messages.
 */
module.exports = function hyperdriveToRecords (db, opts) {
  return function transform (msgs, next) {
    let pending = msgs.length
    const records = []
    msgs.forEach(msg => {
      if (!validateMessage(msg)) done()
      else transformMessage(db, msg, done)
    })

    function done (err, record) {
      if (!err && record) records.push(record)
      if (--pending === 0) next(records)
    }
  }
}

function validateMessage (msg) {
  const { type, name } = msg
  if (!(type === 'put' || type === 'del')) return false
  if (!validatePath(name)) return false
  const { schema, id } = parsePath(name)
  if (!schema || !id) return false
  return true
}

function transformMessage (db, msg, cb) {
  const { type, name, value: stat, key } = msg
  // TODO: Deletes.
  const { schema, id } = parsePath(name)
  const record = {
    id,
    schema,
    delete: type === 'delete',
    stat,
    source: key,
    key,
    seq: msg.seq || 0 // TODO: We don't have seqs here at the moment..
  }
  readValue(db, key, name, (err, value) => {
    if (err) return cb(err)
    record.value = value
    cb(null, record)
  })
}

function readValue (db, key, path, cb) {
  db.source(key, (drive) => {
    // if (err) return cb(err)
    drive.readFile(path, (err, buf) => {
      if (err) return cb(err)
      let value
      try {
        value = JSON.parse(buf.toString())
      } catch (err) {}
      cb(null, value)
    })
  })
}

function validatePath (path) {
  const parts = path.split('/')
  return parts.length === 4 && parts[0] === P_DATA
}

function parsePath (path) {
  const parts = path.split('/')
  // Remove first element: .data
  parts.shift()
  const [schemaNs, schemaName, filename] = parts
  const schema = [schemaNs, schemaName].join('/')
  const id = filename.replace(/\.json$/, '')
  return { schema, id }
}

module.exports = contentView

function contentView (opts, db) {
  function readValue (msg, cb) {
    const { key, name } = msg
    db.source(key, (drive) => {
      // if (err) return cb(err)
      drive.readFile(name, (err, buf) => {
        if (err) return cb(err)
        let value
        try {
          value = JSON.parse(buf.toString())
        } catch (err) {}
        cb(null, value)
      })
    })
  }
  const view = {
    ...opts,
    prefix: '.data/',
    filter (msgs, next) {
      msgs = msgs.filter(msg => {
        const { type, name, value: stat, key } = msg
        if (!(type === 'put' || type === 'del')) return false
        if (stat && stat.isDirectory()) return false
        return true
      })
      next(msgs)
    },
    map (msgs, next) {
      let pending = 1
      const records = []

      for (const msg of msgs) {
        const { type, name, value: stat, key } = msg
        // TODO: Deletes.

        const { schema, id } = parsePath(name)
        if (!schema || !id) continue

        const record = {
          id,
          schema,
          delete: type === 'delete',
          stat: stat,
          source: key,
          seq: msg.seq || 0 // TODO: We don't have seqs here at the moment..
        }

        pending++
        readValue(msg, (err, value) => {
          if (err) done(err)
          record.value = value
          done(null, record)
        })
      }
      done()

      function done (err, record) {
        if (err) console.error(err) // TODO
        if (record) records.push(record)

        if (--pending !== 0) return

        if (records.length) opts.map(records, next)
        else next()
      }
    }
  }
  return view
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

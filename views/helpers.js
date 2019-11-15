module.exports = { opsForRecord, opsForRecords }

function opsForRecords (db, records, map, cb) {
  let pending = records.length
  const ops = []
  records.forEach(record => opsForRecord(db, record, map, done))
  function done (err, curOps) {
    if (!err) ops.push(...curOps)
    if (--pending === 0) cb(null, ops)
  }
}

function opsForRecord (db, record, map, cb) {
  db.api.kv.isLinked(record, (err, isOutdated) => {
    // linked records are outdated/overwritten, nothing to do here.
    if (err || isOutdated) return cb(err, [])
    // check if we have to delete other records.
    delOps(db, record, map, (err, ops = []) => {
      if (err) return cb(err)
      ops = mapToDel(ops)
      // finally, add the put itself.
      if (!record.delete) ops.push(...mapToPut(map(record, db)))
      cb(err, ops)
    })
  })
}

function mapToPut (ops) {
  return ops.map(op => {
    return { ...op, type: 'put' }
  })
}

function mapToDel (ops) {
  return ops.map(op => {
    return { ...op, type: 'del' }
  })
}

function delOps (db, record, map, cb) {
  let ops = []
  if (record.delete) {
    ops = map(record, db)
  }

  if (!record.links || !record.links.length) {
    return cb(null, ops)
  }

  let pending = record.links.length
  record.links.forEach(link => {
    db.loadLink(link, (err, record) => {
      if (err || !record) return done()
      ops.push(...map(record, db))
      done()
    })
  })
  function done () {
    if (--pending === 0) cb(null, ops)
  }
}

const crypto = require('crypto')
const base32 = require('base32')
const { Transform } = require('stream')

exports.keyseq = function (record) {
  return record.key + '@' + record.seq
}

exports.uuid = function () {
  return base32.encode(crypto.randomBytes(16))
}

exports.through = function (transform) {
  return new Transform({
    objectMode: true,
    transform
  })
}

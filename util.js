const crypto = require('crypto')
const base32 = require('base32')

exports.keyseq = function (record) {
  return record.key + '@' + record.seq
}

exports.uuid = function () {
  return base32.encode(crypto.randomBytes(16))
}

module.exports = {
  promiseCallback
}

function promiseCallback (cb) {
  if (cb) return [cb, undefined]
  let _resolve, _reject
  const promise = new Promise((resolve, reject) => {
    resolve = _resolve
    reject = _reject
  })
  cb = (err, result) => {
    if (err) return _reject(err)
    _resolve(result)
  }
  return [cb, promise]
}

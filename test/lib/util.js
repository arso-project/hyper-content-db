exports.stepper = function (cb) {
  const steps = []
  return function step (name, fn) {
    if (!fn) return step(null, name)
    if (!name) name = steps.length
    steps.push({ fn, name })
    if (steps.length === 1) process.nextTick(run)
  }
  function run (lastResult) {
    const { fn, name } = steps.shift()
    console.log(`> step ${name}`)
    fn(done, lastResult)
  }
  function done (err, result) {
    if (err) return cb(err)
    if (steps.length) process.nextTick(run, result)
    else cb(null, result)
  }
}

exports.once = function (fn) {
  let didrun = false
  return (...args) => {
    if (didrun) return
    didrun = true
    return fn(...args)
  }
}

exports.runAll = function runAll (ops) {
  return new Promise((resolve, reject) => {
    runNext(ops.shift())
    function runNext (op) {
      op(err => {
        if (err) return reject(err)
        let next = ops.shift()
        if (!next) return resolve()
        return runNext(next)
      })
    }
  })
}

exports.replicate = function replicate (a, b, opts, cb) {
  if (typeof opts === 'function') return replicate(a, b, null, opts)
  if (!opts) opts = { live: true }
  const stream = a.replicate(true, opts)
  stream.pipe(b.replicate(false, opts)).pipe(stream)
  setImmediate(cb)
}

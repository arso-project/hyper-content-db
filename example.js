const contentcore = require('./index')
const pump = require('pump')

const log1 = require('debug')('core1')
const log2 = require('debug')('core2')

function contentview (name, opts) {
  const log = (opts && opts.log) || require('debug')('contentview.' + name)
  const contentview = {
    readFile: true,
    map: function (msgs, done) {
      forAll(msgs, done, (msg, next) => {
        log('MSG', msg.driveKey.toString('hex'))
        log(msg)
        if (msg.fileContent) {
          log(' -> CONTENT', msg.fileContent.toString())
        } else {
          log(' -> NO FILE CONTENT')
        }
      })
      // next()
    }
  }
  return contentview
}

firstCore()

function firstCore () {
  const core1 = contentcore('.data1')
  core1.use('contentview', contentview('core1'))
  core1.writer((err, drive) => {
    drive.writeFile('test.txt', 'foobar', err => {
      log1('file written')
      secondCore(core1)
    })
  })
}

function secondCore (core1) {
  core1.ready(() => {
    const core2 = contentcore('.data2', core1.key)
    core2.use('contentview', contentview('core2'))
    core2.writer((err, drive) => {
      let msg = 'hi, my key is ' + drive.key.toString('hex')
      drive.writeFile('second.txt', msg, (err) => {
        log2('file written')
        replicate(core1, core2, () => connect(core1, core2))
      })
    })
  })
}

function connect (core1, core2) {
  console.log('CONNECT')
  log1(core1.key)
  log2(core2.key)
  core2.writer((err, drive2) => {
    core1.addSource(drive2.key, (err, done) => {
      log1('Added source:', drive2.key.toString('hex'))
      setTimeout(() => replicate(core1, core2, () => {
        console.log('SECOND repl finished')
      }, 150))
    })
  })
}

function forAll (msgs, done, fn) {
  let missing = msgs.length
  for (let msg of msgs) {
    fn(msg, () => {
      if (--missing === 0) done()
    })
  }
}

function replicate (a, b, cb) {
  var stream = a.replicate()
  stream.pipe(b.replicate()).pipe(stream).on('end', cb)
}

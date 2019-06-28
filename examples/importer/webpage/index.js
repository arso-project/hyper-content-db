const { Readable } = require('stream')
const freezeDry = require('freeze-dry').default
const ky = require('ky-universal')
const u = require('url')
const p = require('path')
const jsdom = require('jsdom')
const blake2b = require('blake2b')
const mkdirp = require('mkdirp')
const htmlToMd = require('./html-to-markdown.js')
// const got = require('got')
//
const debug = require('debug')('import')

// const { makeId } = require('../common')
//
// fullState (id) {
//   if (!jobs[id]) throw new Error('Invalid job id.')
//   return jobs[id].serialize()
// }

const metascraper = require('metascraper')([
  require('metascraper-author')(),
  require('metascraper-date')(),
  require('metascraper-description')(),
  require('metascraper-image')(),
  require('metascraper-logo')(),
  require('metascraper-clearbit-logo')(),
  require('metascraper-publisher')(),
  require('metascraper-title')(),
  require('metascraper-url')()
])

const readability = require('readability-from-string')

module.exports = importer

function importer (cstore) {
  const jobs = {}
  return {
    label: 'Web page importer',
    input (string, next) {
      if (string.match(/^https?:\/\//)) next(true)
      else next()
    },
    handle (msg, done) {
      const { url, id } = msg
      cstore.writer((err, drive) => {
        if (err) done(err)
        const job = new Importer(cstore, id, url)
        jobs[job.id] = job
        job.setPipeline([
          download,
          metascrape,
          readable,
          freeze,
          saveFiles
        ])
        job.start()
        done(null, job.statusStream)
      })
    }
  }
}

class Importer {
  constructor (cstore, id, url) {
    this.id = id || cstore.id()
    this.url = url
    this.cstore = cstore

    this.state = {}

    this.resources = {}
    this.files = {}
    this.derivedFiles = {}
    this.records = []

    this.pipeline = []

    this.currentStep = -1

    this.statusStream = new Readable({
      objectMode: true,
      read () {}
    })
  }

  start () {
    const self = this
    this.setState({ status: 'work' }, 'start!')

    this.executeNextStep(finish)

    function finish (error) {
      if (error) this.setState({ status: 'error', error }, 'error!')
      self.setState({ status: 'done' }, 'finished!')
      self.statusStream.push(null)
    }
  }

  executeNextStep (done) {
    const self = this
    this.currentStep++
    if (!this.pipeline[this.currentStep]) {
      return done()
    }
    let worker = this.pipeline[this.currentStep]
    this.log('starting step: ' + worker.name)

    process.nextTick(() => {
      try {
        worker(this, (err) => {
          if (err) return error(err)
          this.executeNextStep(done)
        })
      } catch (err) {
        return error(err)
      }
    })

    function error (err) {
      self.error('error in step: ' + worker.name, err)
      self.executeNextStep(done)
    }
  }

  setState (newState, message) {
    if (typeof newState === 'function') this.state = newState(this.state)
    else this.state = { ...this.state, ...newState }
    this.statusStream.push({ state: this.state, message })
  }

  getState (cb) {
    if (cb) cb(this.state)
    return this.state
  }

  log (message) {
    debug(message)
    this.statusStream.push({ message })
  }

  error (message, ...args) {
    debug('error', message, ...args)
    this.statusStream.push({ error: { message, args } })
  }

  setPipeline (steps) {
    this.pipeline = steps
  }

  addResource (id, resource) {
    this.resources[id] = resource
  }

  getResource (id) {
    return this.resources[id]
  }

  addFile (path, body) {
    this.files[path] = body
  }

  addDerivedFile (path, body) {
    this.derivedFiles[path] = body
  }

  addRecord (schema, value) {
    this.records.push({ schema, value })
  }

  serialize () {
    return {
      state: this.state,
      files: this.files,
      resources: this.resources
    }
  }
}

function urlToFilename (url, prefix) {
  let parsed = u.parse(url)
  let PREFIX = '/_import'
  // todo: how to handle GET params?
  prefix = prefix || ''
  let path = p.join(PREFIX, prefix, parsed.hostname, parsed.pathname)
  return path
}

async function download (job, next) {
  let url = job.url
  // let drive = job.api.hyperdrive

  let response = await ky(url)
  let text = await response.text()

  // drive.writeFile(filepath, text)
  job.addResource('html', text)

  // job.addFile(filepath, text)

  const dom = new jsdom.JSDOM(text)
  job.addResource('dom', dom)
  next()
}

async function freeze (job, next) {
  const dom = job.getResource('dom')

  const html = await freezeDry(dom.window.document, {
    docUrl: job.url,
    fetchResource,
    blobToURL,
    getCsp
  })

  // job.addResource('html-clean', html)
  let filepath = urlToFilename(job.url)
  if (filepath.substring(-1).charAt(0) === '/') {
    filepath = filepath + 'index.html'
  }
  job.addFile(filepath, html)
  job.addRecord('file', { path: filepath, mimetype: 'text/html' })

  job.baseFilePath = filepath

  next()

  async function fetchResource (url, opts) {
    // TODO: Fetch locally..
    // const filename = urlToFilename(url, opts)
    const response = await ky(url, opts)
    return response
    // return ky(...args)
    // return got(...args)
  }

  async function blobToURL (blob, link, resource) {
    // const name = hash(blob)
    // console.log('make url: res', resource)
    const filename = urlToFilename(link.resource.url)
    const parent = urlToFilename(resource.url)
    const relative = p.relative(parent, filename)
    job.addFile(filename, blob.toBuffer())
    return relative
    // return '/' + filename
  }

  function getCsp (resource) {
    return null
  }
  // setTimeout(() => next(), 1000)
}

freeze.name = 'freeze-dry'

async function metascrape (job, next) {
  const html = job.getResource('html')
  const url = job.url
  const metadata = await metascraper({ html, url })
  job.addRecord('metascrape', metadata)
  // job.addDerivedFile('meta.json', Buffer.from(JSON.stringify(metadata)))
  next()
}

metascrape.name = 'metascrape'

function readable (job, next) {
  const html = job.getResource('html')
  const readable = readability(html, { href: job.url })
  // job.addResource('readable', readable)
  const md = htmlToMd(readable.content)
  let content = `# ${readable.title}\n\n${md}`
  // job.addDerivedFile('readable.md', content)
  job.addRecord('readable', { ...readable, content })
  next()
}

readability.name = 'readability'

function saveFiles (job, next) {
  job.cstore.writer((err, writer) => {
    if (err) return next(err)
    _saveFiles(job, writer, () => {
      _saveRecords(job, next)
    })
  })
}

function _saveRecords (job, next) {
  const cstore = job.cstore
  const batch = []
  job.records.forEach(record => {
    batch.push({
      op: 'put',
      id: job.id,
      schema: '~/' + record.schema,
      record: record.value
    })
  })
  cstore.batch(batch, next)
}

function _saveFiles (job, drive, next) {
  const basename = job.baseFilePath || urlToFilename(job.url)

  let missing = 0

  for (let [filename, content] of Object.entries(job.files)) {
    if (typeof content === 'string') content = Buffer.from(content, 'utf8')
    if (!content) {
      job.error('No content set for file', filename)
      continue
    }

    if (!filename.startsWith('/')) filename = '/' + filename

    missing++

    mkdirp(p.dirname(filename), { fs: drive }, (err, cb) => {
      if (err && err.code !== 'EEXIST') return cb(err)
      drive.writeFile(filename, content, err => {
        let msg = 'Written file: ' + filename
        done(err, msg)
      })
    })
  }

  for (let [filename, content] of Object.entries(job.derivedFiles)) {
    // if (typeof content === 'string') content = Buffer.from(content, 'utf8')
    if (typeof content === 'string') {
      content = Buffer.from(content)
    }
    if (!content) {
      job.error('No content set for file', filename)
      continue
    }
    let path = p.join('/_import/DERIVED', filename)

    if (!path.startsWith('/')) filename = '/' + filename

    missing++
    mkdirp(p.dirname(path), { fs: drive }, (err, cb) => {
      if (err && err.code !== 'EEXIST') return cb(err)
      drive.writeFile(path, content, (err) => {
        done(err, 'Written derived file: ' + path)
      })
    })
  }

  if (!missing) done()

  function done (err, msg) {
    if (err && msg) job.error(msg, err)
    else if (msg) job.log(msg)
    if (--missing <= 0) next()
  }
}

function hash (blob) {
  let input = Buffer.from(blob)
  let output = Buffer.alloc(128)
  let hash = blake2b(output.length).update(input).digest('hex')
  return hash
}

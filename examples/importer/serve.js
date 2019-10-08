const express = require('express')
const p = require('path')
const Swarm = require('corestore-swarm-networking')

module.exports = serve

function logErrors (err, req, res, next) {
  console.error(err.stack)
  res.status(500)
  next(err)
}

function share (cstore) {
  // const swarm = new Swarm(cstore)
  // swarm.listen()
  // const core = swarm.default()
  // core.ready(() => {
  //   swarm.seed(core.key)
  //   console.log('Seeding ' + core.key.toString('hex'))
  // })
}

function serve (cstore) {
  // share(cstore)
  const port = 8080
  const host = 'localhost'
  const app = express()
  app.use(logErrors)
  app.get('/*', onPath)
  app.get('/', onPath)

  app.listen(port, host, () => console.log(`Server listening on http://${host}:${port}`))

  function onPath (req, res, next) {
    let path = req.params['0'] || ''
    path = p.join('/', path)
    console.log('GET', path)
    cstore.writer((err, drive) => {
      if (err) return next(err)
      drive.stat(path, (err, stat) => {
        console.log(err)
        // TODO
        if (err && err.code === 'ENOENT') return res.status(404).send('File not found.')
        if (err) return next(err)
        console.log('got stat', stat)

        if (stat.isDirectory()) {
          console.log('DIR!')
          drive.readdir(path, (err, list) => {
            if (err) return next(err)
            list = list.sort().filter(a => a)
            let content = `<ul>
              ${list.map(name => `<li><a href="${p.join('/', path, name)}">${name}</a></li>`)}
            </ul>`
            let buf = Buffer.from(content)
            res.setHeader('Content-Type', 'text/html; charset=UTF-8')
            res.setHeader('Content-Length', Buffer.byteLength(buf))
            res.send(buf)
          })
        } else if (stat.isFile()) {
          console.log('FILE!')
          drive.readFile(path, (err, buf) => {
            if (err) return next(err)
            let type
            if (stat.metadata && stat.metadata.headers) {
              console.log('META', stat.metadata)
              const headers = JSON.parse(stat.metadata.headers.toString())
              console.log('HEADERS', headers)
              if (headers['content-type']) {
                console.log('SET TYPE', headers['content-type'])
                type = headers['content-type']
              }
              // Object.entries(headers).forEach((header, value) => {
              //   res.setHeader(header + ': ' + value)
              // })
            }

            if (!type) type = 'text/html; charset=UTF-8'

            res.setHeader('Content-Type', type)
            res.setHeader('Content-Length', Buffer.byteLength(buf))
            res.status(200)
            res.send(buf)
          })
        } else {
          next(new Error('Invalid stat.'))
        }
      })
    })
  }
}

const minimist = require('minimist')
const Importer = require('./importer')
const p = require('path')
const pretty = require('pretty-bytes')
const mirrorFolder = require('mirror-folder')

const argv = minimist(process.argv.slice(2), {
  alias: { key: 'k', storage: 'd' },
  default: { storage: './.data' }
})

try {
  run(argv, close)
} catch (err) {
  close(err)
}

function close (err, msg) {
  if (err) {
    console.error(err)
    process.exit(1)
  }
  if (msg) console.log(msg)
  process.exit(0)
}

function run (argv, cb) {
  const [cmd, ...args] = argv._

  // const cb = err => console.log('done', err)

  switch (cmd) {
    case 'add': return add(args, argv, cb)
    case 'mirror': return mirror(args, argv, cb)
    // case 'work': return work()
    // case 'status': return status()
    case 'show': return show(args, argv, cb)
    case 'search': return search(args, argv, cb)
    case 'serve': return serve(args, argv, cb)
    default: return usage(args, argv, cb)
  }
}

function usage (args, argv, cb) {
  let msg = `archipel-import [-k key] command [arguments]

Commands:

  add URL
  search
  status
  show
  serve

Options:
  -k, --key    Content store key
  -d, --data   Directory to store data at
               Default: ./.data
  `
  cb(null, msg)
}

function open (opts) {
  const importer = Importer({ key: opts.key, storage: opts.storage })
  return importer
}

function add (args, opts, cb) {
  const importer = open(opts)
  const url = args[0]
  if (!url) return cb(new Error('URL is required.'))
  console.log('add', url)
  importer.add(url, cb)
}

function serve (args, opts, cb) {
  const importer = open(opts)
  const cstore = importer.cstore
  require('./serve')(cstore)
}

function mirror (args, opts, cb) {
  console.log(opts)
  const importer = open(opts)
  const store = importer.cstore
  store.writer((err, drive) => {
    // console.log('go', drive)
    let target = p.resolve(args[0])
    console.log('TARGET', target)
    const equals = function (src, dst, cb) {
      cb(null, false)
    }
    let prog = mirrorFolder({ name: '/', fs: drive }, p.resolve(args[0]), { equals }, (err, res) => {
      console.log('done', err, res)
    })
  })
}

function search (args, opts, cb) {
  const importer = open(opts)
  const store = importer.cstore
  store.ready(() => {
    store.on('indexed', (key) => {
      console.log('indexed!')
      store.api.sonar.query(args.join(' '), (err, results) => {
        if (!results) return console.log('no results', err)
        console.log('RESULTS', err, results.results.map(r => {
          return { score: r.score, title: r.doc.title }
        }))
      })
    })
  })
}

function show (args, opts, cb) {
  const importer = open(opts)
  const store = importer.cstore
  store.ready(() => {
    store.on('indexed', (key) => {
      let records = []
      store.api.entities.all((err, list) => {
        let missing = 0
        for (let [id, rows] of Object.entries(list)) {
          for (let row of rows) {
            missing++
            store.getRecords(row.schema, id, (err, record) => {
              records.push(record)
              if (--missing === 0) {
                let flat = records.reduce((agg, rows) => ([...agg, ...rows]), [])
                let mapped = flat.map(simplify)
                console.log(mapped)
                cb()
              }
            })
          }
        }
      })
    })
  })

  function simplify (rec) {
    return {
      id: rec.id,
      schema: rec.schema,
      value: rec.value
    }
  }
}


// function show (args, opts, cb) {
//   const importer = open(opts)
//   const store = importer.cstore
//   store.writer((err, drive) => {
//     if (err) return cb(err)
//     // iterate(drive, args[0] || '/', cb)
//     // statRecursive(drive, args[0] || '/', 100, (err, stat) => {
//     //   console.log(err, stat)
//     //   print([stat])
//     // })
//     console.log('drive', drive.readdir)
//     walk(drive, '/', (err, stats) => {
//       console.log(err, stats)
//     })
//   })

//   function print (stats, indent) {
//     indent = indent || 0
//     stats.forEach(stat => {
//       console.log(stat.path, pretty(stat.size))
//       if (stat.children && stat.children.length) print(stat.children, indent + 2)
//     })
//   }

//   function iterate (drive, cur, cb) {
//     let missing = 0
//     const dirs = []
//     drive.readdir(cur, (err, list) => {
//       if (err) return cb(err)
//       if (!list.length) return done()
//       list.forEach(name => {
//         let path = p.join(cur, name)
//         missing++
//         drive.stat(path, onstat.bind(onstat, path, name))
//       })
//     })
//     function onstat (path, name, err, stat) {
//       if (stat.isDirectory()) {
//         done(null, path)
//       } else if (stat.isFile()) {
//         drive.readFile(path, (err, data) => {
//           console.log(path, pretty(stat.size))
//           console.log()
//           done()
//         })
//       }
//     }

//     function done (err, path) {
//       if (path) dirs.push(path)
//       if (--missing <= 0) handleDirs(err, dirs)
//     }

//     function handleDirs (err, dirs) {
//       let missing = 0
//       if (!dirs.length || err) return cb(err)
//       dirs.forEach(dir => {
//         missing++
//         iterate(drive, dir, (err) => {
//           if (err) return cb(err)
//           if (--missing === 0) return cb()
//         })
//       })
//     }
//   }
// }

// function statRecursive (drive, path, depth, cb) {
//   console.log('GO', path)
//   depth = depth || 0
//   statPath(path, 0, cb)

//   function statPath (path, currentDepth, cb) {
//     console.log('go', path)
//     drive.stat(path, (err, stat) => {
//       // console.log('path', path, depth, currentDepth, stat)
//       // console.log('stat', path, stat.isDirectory())
//       if (err) return cb(err)
//       stat.path = path
//       if (stat.isDirectory() && currentDepth < depth) {
//         // console.log('DIR!', path)
//         statChildren(path, currentDepth + 1, (err, children) => {
//           console.log('CHILDREN', path, err, children)
//           if (err) return cb(err)
//           stat.children = children
//           cb(null, stat)
//         })
//       } else {
//         cb(null, stat)
//       }
//     })
//   }

//   function statChildren (path, currentDepth, cb) {
//     drive.readdir(path, (err, children) => {
//       // console.log('READDIR', path, children)
//       if (err) return cb(err)
//       children = children.filter(c => c)
//       // console.log('CHILDRENa', children)
//       if (!children.length) return cb(null, [])
//       let stats = []
//       let missing = children.length + 1

//       for (let child of children) {
//         statPath(joinPath(path, child), currentDepth, (err, stat) => {
//           // console.log('ret from path', path, child, stat.children)
//           if (err) return cb(err)
//           stats.push(stat)
//           if (--missing === 0) cb(null, stats)
//         })
//       }
//     })
//   }
// }

// function walk (fs, dir, done) {
//   let results = []
//   fs.readdir(dir, function (err, list) {
//     if (err) return done(err)
//     let pending = list.length
//     if (!pending) return done(null, results)
//     list.forEach(function (file) {
//       // file = [dir, file].join('/')
//       file = joinPath(dir, file)
//       fs.stat(file, function (err, stat) {
//         if (err) done(err)
//         console.log(file)
//         if (stat && stat.isDirectory()) {
//           walk(fs, file, function (err, res) {
//             if (err) done(err)
//             results = results.concat(res)
//             if (!--pending) done(null, results)
//           })
//         } else {
//           results.push(file)
//           if (!--pending) done(null, results)
//         }
//       })
//     })
//   })
// };

//   function joinPath (prefix, suffix) {
//     if (prefix.slice(-1) === '/') prefix = prefix.substring(0, prefix.length - 1)
//     if (suffix[0] === '/') suffix = suffix.substring(1)
//     return prefix + '/' + suffix
//   }

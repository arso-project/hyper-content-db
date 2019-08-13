const through = require('through2')
const barco = require('..')
const db = barco('/tmp/testdb')

function dateView (lvl, db) {
  return {
    map (records, next) {
      let ops = []
      console.log('MAP', records.map(r => ({ id: r.id, title: r.value.title})))
      for (let record of records) {
        if (!record.value.date) return
        ops.push({
          type: 'put',
          key: `${record.value.date}!${record.id}!${record.schema}!${record.source}`,
          value: record.seq
        })
      }
      // console.log('ops', ops)
      lvl.batch(ops, next)
    },
    api: {
      range (kcore, from, to) {
        return lvl.createReadStream({
          gte: from.toJSON(),
          lte: to.toJSON()
        }).pipe(through.obj(function (row, enc, next) {
          // console.log('row', row)
          const [date, id, schema, source] = row.key.split('!')
          this.push({ id, schema, source })
        }))
      }
    }
  }
}

db.useRecordView('dates', dateView)

const records = [
  { title: 'Party', date: new Date(2019, 11, 2) },
  { title: 'Demonstration', date: new Date(2020, 1, 10) },
  { title: 'Reading circle', date: new Date(2019, 8, 7) },
  { title: 'Workshop', date: new Date(2019, 12, 5) }
]

const ops = records.map(value => ({ op: 'put', schema: 'event', value }))

db.batch(ops, (err, ids) => {
  if (err) return console.error(err)
  else console.log('put', ids)
})

db.on('indexed-all', () => {
  const queryStream = db.api.dates.range(
    new Date(2019, 9),
    new Date(2019, 12, 31)
  )
  // queryStream.on('data', console.log)

  const getStream = db.createGetStream()
  const resultStream = queryStream.pipe(getStream)
  resultStream.on('data', record => console.log(record.value.title))
})

setTimeout(() => {}, 1000)

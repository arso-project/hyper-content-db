const hypercontent = require('..')
const ram = require('random-access-memory')
const db = hypercontent(ram)

// Let's put a basic schema first.
db.putSchema('event', {
  properties: {
    title: {
      type: 'string',
      index: true
    },
    date: {
      type: 'date',
      index: true
    }
  }
})

// Now add some records.
db.batch([
  { schema: 'event', value: { title: 'Workshop', date: new Date(2019, 10, 10) } },
  { schema: 'event', value: { title: 'Reading', date: new Date(2019, 8, 2) } }
])

// When all indexing is done, query and log results.
db.on('indexed-all', query)

db.ready(() => {
  // Create a second database. Set the first database as primary key.
  // This will make db2 a "fork" or "extension" of the first.
  const db2 = hypercontent(ram, db.key)
  db2.ready(() => {
    // Add the second database as a source for the first.
    db.addSource(db2.localKey)

    // Connect the two databases.
    replicate(db, db2)

    // Add content to the second database.
    db2.batch([
      { schema: 'event', value: { title: 'Dinner', date: new Date(2019, 9, 22) } }
    ])
  })
})

function query () {
  const eventsSortedByDate = db.api.indexes.query({ schema: 'event', prop: 'date' }).pipe(db.createGetStream())
  eventsSortedByDate.on('data', row => console.log(row.value.date, row.value.title))
}

function replicate (a, b) {
  const stream = a.replicate()
  const stream2 = b.replicate()
  stream.pipe(stream2).pipe(stream)
}

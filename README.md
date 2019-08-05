# 🐙 barcobase

A Kappa-style peer-to-peer database, on top of hyperdrives.

```javascript
const barcobase = require('barcobase')
const db = barcobase('./data')
```

## API

#### `const db = barcobase(storage, key, opts)`

`storage` is either a string to a file system path or a [random-access-storage](https://github.com/random-access-storage/) instance.

`key` is a `Buffer` containing a key to the primary drive. If omitted, it will be loaded from storage. If no key exists a new keypair will be generated.

#### `db.ready(cb)`

`cb` is called after the database is fully initialized.

#### `db.replicate(opts)`

Create a hypercore-protocol replication stream. See [hyperdrive](https://github.com/andrewosh/hyperdrive) for details.

#### `db.addSource(key, cb)`

Add an additional source hyperdrive. `key` is the key of a barcobase hyperdrive. Barcobase treats all sources equally. 

> TODO: Document how records from different sources relate to each other.

#### `db.put(record, cb)`

Put a record into the database. 

`record` is a plain js object:
```javascript
{
  id: 'string',
  schema: 'string'
  value: someObject,
}
```

* `schema` is required. All records have a schema name. Schemas are identified by strings. Schemas can either be local or well-defined. Local schemas are identifiers that should be unique in the context of the database, their names may not contain slashes (`/`). Well-defined schemas are identified by a domain, followed by an identifier (e.g. `arso.xyz/event`). They have to include exactly one slash. By this convention, schema names are compatible with the [unwalled.garden](https://unwalled.garden) spec. Usually, you will want to put the schema's declaration into the database (see below), but this is not required.

* `id` identifies a record uniquely within the database. When creating new recors, leave `id` undefined. When updating existing records, `id` is required.

* `value` is the record's value. It has to be a JavaScript object that is serializable to JSON. If the record's schema has its definition stored in the database, the value has to conform to the schema.

	> TODO: Validating records to their schemas is not yet implemented.

* `cb` is a callback that will be called with `(err, id)`.

The records will be saved as files within the database according to a fixed naming schema: `/.data/schema-domain/schema-name/id.json`. For local schemas, the `schema-domain` is the key of the database.

#### `db.get(req, [opts], cb)`

Get a record from the database. `req` should look like this:

```javascript
{
  id: 'string' // required,
  schema: 'string' // required,
  source: 'string' // optional,
  seq: int // optional
}
```
`id` and `schema` are required. If `source` is set to a source key (hex string), it will only lookup in that source. If source is omitted, all sources will be checked. 

`cb` is a callback and will be called with `(err, record)` if source is set and with `(err, records)` if source is omitted.

#### `db.putSchema(name, schema, cb)`

Save a schema into the database. The schema declaration follows the [JSON Schema](https://json-schema.org), with some additional properties.

```javascript
const schema = {
  properties: {
    title: {
      type: 'string',
      index: true
    },
    date: {
      type: 'string',
      index: true
    }
  }
}
db.putSchema('movie', schema, (err) => {
  if (!err) console.log('schema saved.')
})
```

Supported properties in addition to the JSON Schema spec are:

* `index`: Set on a top-level simple field to index values of that field in the database.

The top-level JSON schema declaration can be omitted and will be filled in automatically. 

> TODO: Also support putting full JSON schemas (including the outer section)

#### `db.getSchema(name, [opts], cb)`

Load a schema declaration from the database.

#### `db.batch(ops, cb)`

Execute multiple operations. `ops` looks like this:
```javascript
const ops = [
  {
    op: 'put' | 'del',
    schema,
    id,
    value 
  }
]
```

#### `const batchStream = db.createBatchStream()`

Returns a duplex stream. The writable side expects to be written to with `op` objects as in `db.batch()`. The readable side emits arrays of ids of the putted records and errors in case of errors.

#### `const getStream = db.createGetStream()`

Returns a transform stream that transforms get requests into records.

```javascript
const getStream = db.createGetStream()
getStream.push({ id, schema })
getStream.on('data', (record) => {
  console.log(record.value)
})
```

#### `db.useRecordView(name, makeView, [opts])`

Register a new database view. Views are functions that will be called whenever records are being put or deleted. The database maintains the state of each view so that they catch up on updates automatically. See [kappa-core](https://github.com/kappa-db/kappa-core) for good introduction on how to work with kappa views.

`name` is the name of the view. It has to be unique per database.

`makeView` is a constructor function. It will be called with `(level, db, opts)`:

* `level`: an [LevelUp](https://github.com/Level/levelup)-compatible LevelDB instance for this view
* `db`: the barcobase db
* `opts`: optional opts passed into `useRecordView`

The constructor function should return a view object with the following keys:

* `map: function (records, next) {}`
    This function will be called with a batch of records. Process the entries (e.g. by inserting rows into the leveldb). Call `next()` when done.
* `api`: An object of query functions that this view exposes to the outside world. They should be safe to call (may not modify data) as they may be called from the client side.
* TODO: Document more props.
* TODO: Add support for `filter()` and `reduce()`

##### Example

```javascript
const through = require('through2')
const barco = require('barcobase')
const db = barco('/tmp/testdb')

function dateView (lvl, db) {
  return {
    map (records, next) {
      let ops = []
      for (let record of records) {
        if (!record.value.date) return
        ops.push({
          type: 'put',
          key: `${record.value.date}!${record.id}!${record.schema}!${record.source}`
          value: record.seq
        })
      }
      lvl.batch(ops, next)
    },
    api: {
      range (from, to) {
        from = from.toJSON()
        to = to.toJSON()
        return db.createReadStream({
          gte: from,
          lte: to
        }).pipe(through(function (row, enc, next) {
          const { key: [date, id, schema, source] } = row
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

  const queryStream = db.api.date.range(new Date(2019, 9), new Date(2019, 12, 31))
  const resultStream = queryStream.pipe(db.createGetStream())
  resultStream.on('data', record => console.log(record))
})
```

#### `db.api`

This is where query functions from views are exposed. 

#### `db.on('indexed', cb)`

Emitted whenever a view finished an indexing batch. `cb` is called with `(viewName, sourceKey, batch)` where batch is an array of the processed records.

#### `db.on('indexed-all', cb)`

Emitted whenever all views are finished with processing.

#### `db.on('start', cb)`

Emitted when a new indexing round is started after `indexed-all` has been emitted.


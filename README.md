# content-store (WIP)

A collection of modules for a [kappa-style](https://github.com/kappa-db/kappa-core) document database on top of [hyperdrives](https://github.com/mafintosh/hyperdrive).

*Work in progress*

## multidrive.js

```js
const multidrive = require('./multidrive')

const mdrive = multidrive(storage, key, opts)

mdrive.ready()

mdrive.addSource(key, cb)

mdrive.hasSource(key)

mdrive.saveSource(key, cb)

mdrive.sources(function (sources) {} )

mdrive.source(key, function (source || null) {} )

mdrive.writer(function (err, drive) {})

mdrive.replicate(opts)

```

## multidrive-index.js

This exports a function with the same options as [multifeed-index](https://github.com/kappa-db/multifeed-index/), but under the hood uses [hypertrie-index](https://github.com/Frando/hypertrie-index) to generate the feed of messages. This means that the hypertrie diffing algorithm is at work (TODO: Find out how well this performs on sparse replications). 

Messages emitted:
```js
{
  left: <Node>,
  right: <Node>,
  source: key
}
```
And after using the exported transformNode function:
```js
{
  value: Stat,
  previousValue: Stat | null,
  deleted: false | true,
  fileContent: Buffer // optional, with opts.readFile
  fileContentEncoding: { encode, decode }
}

```

## index.js

A document database abstraction. `putRecord(schema, id, value)`, `getRecord(schema, id)`, `use(view)`, `putSchema(name, schema)`, `getSchema(name)`, `addSource(key)`

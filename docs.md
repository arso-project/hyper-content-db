```javascript

const db = SonarDB(hyperdrive)

db.init(async () => {
  db.putSchema('movie', {
    title: {
      type: 'string',
      index: true
    },
    year: {
      type: 'int',
      index: true
    },
    director: {
      type: 'array',
      item: {
        type: 'reference'
        references: ['director']
      }
    },
  })
})

const id = await db.create('movie', {
  title: 'Foo',
  year: 2002
})

await db.put('movie', id, {
  year: 2003
})

const movies = db.sub('movie')
const directors = db.sub('directors')

const betty = await db.create('director', { name: 'Betty' })

const bar = movieDb.create({
  title: 'Bar',
  year: 2010,
  director: [id]
})

await db.api.rels.get(betty)
// [{ id, source, schema: 'movie', field: 'director', pos: 0 }


```

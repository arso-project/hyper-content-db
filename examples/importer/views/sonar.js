const Sonar = require('sonar')
const sonarSchema = require('sonar/node/fixtures').schema

module.exports = sonarView

function sonarView (db, cstore, opts) {
  const catalog = new Sonar(opts.storage || '.tantivy')

  const getIndex = catalog.openOrCreate('default', sonarSchema)

  return {
    batch: true,
    map (msgs, next) {
      _map(msgs)
        .then(res => next(null, res))
        .catch(err => next(err))
    },
    api: {
      query (kcore, string, cb) {
        _query(string)
          .then(res => cb(null, res))
          .catch(err => cb(err))
      }
    }
  }

  async function _query (string) {
    const index = await getIndex
    const results = await index.query(string)
    return results
  }

  async function _map (msgs) {
    const index = await getIndex
    const docs = []
    console.log('sonar map', msgs.map(msg => ({ id: msg.id, schema: msg.schema, seq: msg.seq })))
    msgs.forEach(msg => {
      let { schema, id, value } = msg
      schema = schema.split('/')[1]
      const doc = { id }
      if (schema === 'metascrape') {
        doc.title = value.title || ''
        doc.body = value.description || ''
      }
      if (schema === 'readable') {
        doc.body = value.content || ''
        doc.title = value.title || ''
      }
      console.log('DOC', doc)

      if (doc.body || doc.title) {
        docs.push(doc)
      }
    })
    console.log('AFTER MAP', docs)
    // console.log('add docs', docs.map(d => ({
    //   title: d.title.length + ' > ' + d.title.substring(0, 10),
    //   body: d.body.length + ' > ' + d.body.substring(0, 10)
    // })))
    try {
      await index.addDocuments(docs)
    } catch (e) {
      console.log('ERROR', e)
    }
    console.log('added')
  }
}

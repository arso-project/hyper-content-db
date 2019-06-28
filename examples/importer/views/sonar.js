const Sonar = require('sonar')
const sonarSchema = require('sonar/node/fixtures').schema

module.exports = sonarView

function sonarView () {
  const catalog = new Sonar('.tantivy')

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
    msgs.forEach(msg => {
      let schema = msg.schema.split('/')[1]
      let id = msg.id
      const doc = { id }
      if (schema === 'metascrape') {
        doc.title = msg.value.title
        doc.body = msg.value.description
      }
      if (schema === 'readable') {
        doc.body = msg.value.content
        doc.title = msg.value.title
      }

      if (doc.body || doc.title) {
        docs.push(doc)
      }
    })
    await index.addDocuments(docs)
  }
}

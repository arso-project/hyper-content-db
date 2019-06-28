const makeView = require('kappa-view')

module.exports = contentView

function contentView (ldb, opts) {
  const view = makeView(ldb, (db) => {
    return {
      prefix: '.data',
      transformNodes: true,
      readFile: true,
      map (msgs, next) {
        let ops = []
        let missing = msgs.length
        msgs = msgs.map(msg => {
          const id = msg.keySplit.pop().replace(/\.json$/, '')
          const schema = msg.keySplit.slice(1).join('/')
          let value
          try {
            value = JSON.parse(msg.fileContent.toString())
          } catch (err) {
            // TODO: What to do with this error?
            value = {}
          }

          msg = {
            id,
            schema,
            delete: msg.delete,
            stat: msg.value,
            value,
            source: msg.source.toString('hex'),
            seq: msg.seq
          }

          if (!opts.batch) opts.map(msg, finish)
          return msg
          // let res = opts.map(msg)
        })

        if (opts.batch) {
          missing = 1
          opts.map(msgs, finish)
        }

        function finish (res) {
          if (res && Array.isArray(res)) {
            ops.push.apply(ops, res)
          } else if (typeof res === 'object') {
            ops.push(res)
          }
          if (--missing === 0) {
            ldb.batch(ops, err => {
              // TODO: This error went through silently!!
              next(err)
            })
          }
        }
      },
      api: opts.api
    }
  })
  return view
}

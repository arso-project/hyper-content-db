module.exports = contentView

function contentView (opts) {
  const view = {
    prefix: '.data',
    transformNodes: true,
    readFile: true,
    map (msgs, next) {
      // console.log('contentView MSGS', msgs)
      // let ops = []
      // let pending = 0
      msgs = msgs.map(msg => {
        if (msg.value.isDirectory()) return
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

        return msg
      }).filter(m => m)

      opts.map(msgs, finish)

      function finish (res) {
        next()
        // if (res && Array.isArray(res)) {
        //   ops.push.apply(ops, res)
        // } else if (typeof res === 'object') {
        //   ops.push(res)
        // }
        // if (--pending <= 0) {
        //   next(null, ops)
        // }
      }
    },
    api: opts.api,
    indexed: opts.indexed
  }
  return view
}

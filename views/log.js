// function logView (kappa) {
//   return {
//     name: 'log',
//     map (msgs, next) {
//       const ops = []
//       let worldseq = this.counter()
//       let pending = msgs.length
//       msgs.forEach(record => {
//         const { delete, key, seq, id, schema, links } = record
//         const value = { key, seq, id, schema, links }
//         kappa.api.kv.isLinked(record, (err, isLinked) => {
//           value.outdated = isLinked
//           ops.push({
//             key: worldseq,
//             value: keyseq(record)
//           })
//         })
//       })
//       ops.push('m!', cnt)
//     },
//     api: {
//       createSource (opts) {
//         const maxBatch = opts.maxBatch || 50
//         return {
//           pull (state, next) {
//             const at = state || 0
//             const to = at + maxBatch
//             const qs = lvl.createReadStream({ gte: at, lt: to })
//               .pipe(through.obj(function (row, enc, next) {
//                 if (row.outdated) return next()
//                 if (!row.delete) {
//                   db.getValue(row, (err, value) => {
//                     row.value = value
//                     this.push(row)
//                     next()
//                   })
//                 } else  {
//                   this.push(row)
//                   next()
//                 }
//               })

//                 collect(qs, (err, res) => {
//                   next(to, res, res.length === maxBatch)
//                 })
//           }
//         }
//       }
//     }
//   }
// }

const Ajv = require('ajv')

const SCHEMAS = {
  'core/schema': { type: 'object' },
  'core/any': { type: 'object' },
  'core/source': { type: 'object' }
}

module.exports = class SchemaStore {
  constructor (opts) {
    this.key = opts.key
    this.schemas = {}
    this.ajv = new Ajv()

    for (let [name, schema] of Object.entries(SCHEMAS)) {
      this.put(name, schema)
    }
  }

  put (name, schema) {
    name = this.resolveName(name)
    if (this.schemas[name]) return
    schema = this.parseSchema(name, schema)
    this.schemas[name] = schema
    // TODO: Handle error
    this.ajv.addSchema(schema, name)
    return true
  }

  get (name) {
    if (typeof name === 'object') {
      let { schema, source } = name
      name = this.resolveName(schema)
    }
    return this.schemas[name]
  }

  list () {
    return { ...this.schemas }
  }

  validate (record) {
    const name = this.resolveName(record.schema)
    let result = false
    try {
      result = this.ajv.validate(name, record.value)
      if (!result) this._lastError = new ValidationError(this.ajv.errorsText(), this.ajv.errors)
    } catch (err) {
      this._lastError = err
    }
    return result
  }

  get error () {
    return this._lastError
  }

  resolveName (name, key) {
    if (!key) key = this.key
    if (Buffer.isBuffer(key)) key = key.toString('hex')
    if (name.indexOf('/') === -1) name = key + '/' + name
    // if (name.indexOf('@') === -1) {
    // TODO: Support versions
    // name = name + '@0'
    // }
    return name
  }

  parseSchema (name, schema) {
    return {
      $id: name,
      type: 'object',
      ...schema
    }
  }
}

class ValidationError extends Error {
  constructor (message, errors) {
    super(message)
    this.errors = errors
  }
}

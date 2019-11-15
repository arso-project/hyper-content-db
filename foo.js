class Foo {
  constructor (n) {
    this.n = n
  }

  get baz () {
    const self = this
    return {
      encode (str) {
        console.log(this, self)
        return self.n + str
      }
    }
  }

  get woot () {
    return function (id) {
      console.log('hi', this.n, id)
    }
  }
}

let f = new Foo('foo')
let enc = f.baz
encode('encme', enc)
function encode (str, enc) {
  console.log(enc.encode(str))
}
f.woot('yeee')

const { Readable } = require('streamx')
const { BLOCK_NOT_AVAILABLE } = require('hypercore-errors')

const DESTROYED = new Error('Destroyed')

module.exports = class BlobReadStream extends Readable {
  constructor (core, id, opts = {}) {
    super(opts)

    const {
      prefetch,
      start = 0,
      end = -1,
      length = end === -1 ? -1 : end - start + 1
    } = opts

    this.core = core
    this.id = id

    this._prefetch = prefetch !== false && core && core.core
    this._range = null
    this._openCallback = null

    this._index = 0
    this._blockEnd = 0
    this._relativeOffset = 0
    this._start = start // bytes position relative to file
    this._length = length // bytes
  }

  static one (core, options) {
    const stream = new this(core, null, options)
    readyAndStart(core, stream)
    return stream
  }

  start (core, id) {
    if (this.destroying) return

    if (this.core === null) this.core = core
    this.id = id

    this._continueOpen(null)
  }

  async _open (cb) {
    if (this.core === null || this.id === null) {
      this._openCallback = cb
      return
    }

    if (!this.core.opened) {
      try {
        await this.core.ready()
      } catch (err) {
        cb(err)
        return
      }
    }

    if (this._length === -1) this._length = this.id.byteLength - this._start
    this._blockEnd = this.id.blockOffset + this.id.blockLength

    if (this._length <= 0) {
      this._length = 0
      this.push(null)

      cb(null)
      return
    }

    let start, end
    try {
      [start, end] = await Promise.all([this._setStart(), this._prefetch ? this._setEnd() : Promise.resolve(0)])
    } catch (err) {
      cb(err)
      return
    }
    if (start < this.id.blockOffset || start >= this._blockEnd) {
      this._length = 0
      this.push(null)

      cb(null)
      return
    }

    if (this._prefetch) this._range = this.core.download({ start, end, linear: true })

    cb(null)
  }

  async _setStart () {
    if (this._start === 0) {
      this._index = this.id.blockOffset
      this._relativeOffset = 0
      return this._index
    }

    const bytes = this.id.byteOffset + this._start

    const result = await this.core.seek(bytes)
    if (!result) throw BLOCK_NOT_AVAILABLE()

    this._index = result[0]
    this._relativeOffset = result[1]

    return this._index
  }

  async _setEnd () {
    if (this._start + this._length >= this.id.byteLength || this._length === -1) {
      return this.id.blockOffset + this.id.blockLength
    }

    const bytes = this._start + this.id.byteOffset + this._length
    const result = await this.core.seek(bytes)
    if (!result) return this.id.blockOffset + this.id.blockLength
    return result[0]
  }

  async _read (cb) {
    let block = null

    try {
      block = await this.core.get(this._index)
    } catch (err) {
      cb(err)
      return
    }

    if (!block) {
      cb(BLOCK_NOT_AVAILABLE())
      return
    }

    if (this._relativeOffset > 0) {
      block = block.subarray(this._relativeOffset)
      this._relativeOffset = 0
    }

    if (this._length < block.byteLength) {
      block = block.subarray(0, this._length)
    }

    this._index++
    this._length -= block.byteLength

    this.push(block)
    if (this._length === 0 || this._index >= this._blockEnd) this.push(null)

    cb(null)
  }

  _continueOpen (err) {
    if (this._openCallback === null) return

    const cb = this._openCallback
    this._openCallback = null

    if (err) {
      cb(err)
      return
    }

    this._open(cb)
  }

  _teardown () {
    if (this._range !== null) {
      this._range.destroy()
      this._range = null
    }

    if (this.core !== null) return this.core.close()
    return Promise.resolve()
  }

  _predestroy () {
    this._teardown().catch(noop)
    this._continueOpen(DESTROYED)
  }

  async _destroy (cb) {
    try {
      await this._teardown()
    } catch (err) {
      cb(err)
      return
    }

    cb(null)
  }
}

function noop () {}

async function readyAndStart (core, stream) {
  try {
    if (core.opened === false) await core.ready()
  } catch (err) {
    stream.destroy(err)
    return
  }

  const id = {
    blockOffset: 0,
    blockLength: core.length,
    byteOffset: 0,
    byteLength: core.byteLength
  }

  stream.start(core, id)
}

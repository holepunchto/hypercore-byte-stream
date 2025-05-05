const { Readable } = require('streamx')
const { BLOCK_NOT_AVAILABLE } = require('hypercore-errors')
const Prefetcher = require('./prefetcher')

const DESTROYED = new Error('Destroyed')

module.exports = class BlobReadStream extends Readable {
  constructor (core, id, opts = {}) {
    super(opts)

    const {
      prefetch = 64,
      start = 0,
      end = -1,
      length = end === -1 ? -1 : end - start + 1
    } = opts

    this.core = core
    this.id = id

    this._prefetch = null
    this._maxPrefetch = prefetch === false ? 0 : prefetch
    this._lastPrefetch = null
    this._openCallback = null

    this._index = 0
    this._blockEnd = 0
    this._relativeOffset = 0
    this._start = start
    this._length = length
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

    this._index = this.id.blockOffset
    this._blockEnd = this.id.blockOffset + this.id.blockLength

    if (this._length === -1) {
      this._length = this.id.byteLength - this._start
    }

    if (this._maxPrefetch > 0 && this.core.core) {
      this._prefetch = new Prefetcher(this.core, {
        max: this._maxPrefetch,
        start: this.id.blockOffset,
        end: this._blockEnd
      })
    }

    if (this._start !== 0) {
      this._seekAndOpen(cb)
      return
    }

    if (this._length <= 0 || this._index >= this._blockEnd) {
      this.push(null)
    }

    cb(null)
  }

  async _seekAndOpen (cb) {
    const bytes = this.id.byteOffset + this._start
    let result = null

    try {
      result = await this.core.seek(bytes, {
        start: this.id.blockOffset,
        end: this.id.blockOffset + this.id.blockLength
      })
    } catch (err) {
      cb(err)
      return
    }

    if (!result) {
      cb(BLOCK_NOT_AVAILABLE())
      return
    }

    this._index = result[0]
    this._relativeOffset = result[1]

    if (this._index < this.id.blockOffset || this._index >= this._blockEnd) {
      this._length = 0
      this.push(null)
    }

    cb(null)
  }

  async _read (cb) {
    if (this._prefetch !== null) this._prefetch.update(this._index)

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
    if (this._prefetch !== null) {
      this._prefetch.destroy()
      this._prefetch = null
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

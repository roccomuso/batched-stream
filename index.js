const {Transform} = require('readable-stream')

class BatchStream extends Transform {
  constructor (options) {
    let {objectMode, size, strictMode, highWaterMark = 16} = options
    if (!(Number.isInteger(size) && size > 0)) throw new Error('size is mandatory (number)')
    super(options)

    this.objectMode = objectMode || false
    this.strictMode = typeof strictMode !== 'undefined' ? strictMode : true // return the rest of the batch when a stream contains a number of items that is not a strict multiply of the batch size.
    this.size = size
    this.currentLength = 0
    this.batch = this.objectMode ? [] : Buffer.alloc(0)

    this.put = this.objectMode ? (chunk) => {
      // object mode
      this.batch.push(chunk)
      if (this.batch.length >= this.size) {
        this.push(this.batch)
        this.batch = []
      }
    } : (chunk) => {
      // classic stream
      this.currentLength += chunk.length
      this.batch = Buffer.concat([this.batch, chunk], this.currentLength)
      // keep batches under size limit
      while (this.currentLength >= this.size) {
        this.push(this.batch.slice(0, this.size))
        this.batch = this.batch.slice(this.size)
        this.currentLength = this.batch.length
      }
    }
  }

  _transform (chunk, encoding, callback) {
    this.put(chunk)
    callback()
  }

  _flush (callback) {
    if (this.batch.length && !this.strictMode) {
      this.push(this.batch)
      this.batch = this.objectMode ? [] : Buffer.alloc(0)
    }
    callback()
  }

}

module.exports = BatchStream

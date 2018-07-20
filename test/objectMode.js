const test = require('tape')
const {Readable} = require('stream')
const BatchStream = require('../')

class FakeStream extends Readable {
  constructor (array, options) {
    super(options)
    this.array = array
  }

  _read () {
    if (this.array.length === 0) return this.push(null) // end stream
    let item = this.array.shift()
    setImmediate(() => {
      this.push(item)
    })
  }
}

// Tests

test('no batches if source length is less than batch size and strictMode is true', t => {
  let fake = new FakeStream(['one', 'two'])
  let batch = new BatchStream({size: 20, objectMode: false, strictMode: true})
  fake.pipe(batch)
  batch.on('data', (data) => {
    t.fail('Should not emit data when size is not reached in strict mode')
  })
  batch.once('end', () => {
    t.ok(true, 'got no data')
    t.end()
  })
})

test('batches if source length is less than batch size and strictMode is false', t => {
  t.plan(4)
  let fake = new FakeStream(['one', 'two'], {objectMode: true})
  let batch = new BatchStream({size: 20, objectMode: true, strictMode: false})
  fake.pipe(batch)
  batch.once('data', (out) => {
    t.equal(out.length, 2, 'got expected data')
    t.equal(out[0], 'one', '1th obj match')
    t.equal(out[1], 'two', '2th obj match')
  })
  batch.on('end', () => {
    t.ok(true, 'batch end')
  })
})

test('batches if source length is greater than batch size and strictMode false', t => {
  t.plan(5)
  let fake = new FakeStream([1, 2, {name: 'Brian'}], {objectMode: true})
  let batch = new BatchStream({size: 2, objectMode: true, strictMode: false})
  fake.pipe(batch)
  batch.once('readable', function () {
    let out = batch.read()
    t.equal(out.length, 2, 'got expected data')
    t.equal(out[0], 1, '1th obj match')
    t.equal(out[1], 2, '2th obj match')
    batch.once('readable', function () {
      let out = batch.read()
      t.equal(out.length, 1, 'got the surplus obj')
      t.equal(out[0].name, 'Brian', 'data match')
    })
  })
})

test('batches if source length is greater than batch size and strictMode true (no odd element)', t => {
  t.plan(4)
  let fake = new FakeStream([1, 2, {name: 'Brian'}], {objectMode: true})
  let batch = new BatchStream({size: 2, objectMode: true, strictMode: true})
  fake.pipe(batch)
  batch.once('readable', function () {
    let out = batch.read()
    t.equal(out.length, 2, 'got expected data')
    t.equal(out[0], 1, '1th obj match')
    t.equal(out[1], 2, '2th obj match')
    batch.once('readable', function () {
      let out = batch.read()
      t.equal(out, null, 'Stream end') // end
    })
  })
})

test('applies back pressure correctly', t => {
  t.plan(2)
  let fake = new FakeStream([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, new Error('Should not get here')], {objectMode: true})
  let batch = new BatchStream({size: 2, highWaterMark: 2, objectMode: true})
  fake.pipe(batch)
  batch.once('readable', function () {
    t.equal(batch.read().length, 2, 'got 1th batch')
    batch.once('readable', function () {
      t.equal(batch.read().length, 2, 'got 2th batch')
    })
  })
})

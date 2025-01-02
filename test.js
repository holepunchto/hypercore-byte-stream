const test = require('brittle')
const Hypercore = require('hypercore')
const tmp = require('test-tmp')
const b4a = require('b4a')
const ByteStream = require('./')

test('basic', async function (t) {
  const { id, core } = await create(t, ['a', 'b', 'c', 'd', 'e'])

  const all = await collect(new ByteStream(core.session(), id))

  t.alike(all, ['a', 'b', 'c', 'd', 'e'])

  const half = await collect(new ByteStream(core.session(), id, { length: 3 }))

  t.alike(half, ['a', 'b', 'c'])

  const lastHalf = await collect(new ByteStream(core.session(), id, { start: 3 }))

  t.alike(lastHalf, ['d', 'e'])
})

test('out or range', async function (t) {
  const { id, core } = await create(t, ['Hello World'])

  const all = await collect(new ByteStream(core.session(), id, { start: 0, length: 21 }))

  t.alike(all, ['Hello World'])
})

test('multiple blobs', async function (t) {
  const { id, core } = await create(t, ['a', 'b', 'c', 'd', 'e'])

  id.blockOffset += 2
  id.blockLength -= 2
  id.byteOffset += 2
  id.byteLength -= 2

  const all = await collect(new ByteStream(core.session(), id))

  t.alike(all, ['c', 'd', 'e'])

  const half = await collect(new ByteStream(core.session(), id, { length: 2 }))

  t.alike(half, ['c', 'd'])

  const lastHalf = await collect(new ByteStream(core.session(), id, { start: 2 }))

  t.alike(lastHalf, ['e'])
})

test('seeks', async function (t) {
  const { id, core } = await create(t, ['aaaa', 'bb', 'ccc', 'd', 'eeeeeeeeee'], 2)

  {
    const result = await collect(new ByteStream(core.session(), id))
    t.alike(result, ['aaaa', 'bb', 'ccc', 'd', 'eeeeeeeeee', 'aaaa', 'bb', 'ccc', 'd', 'eeeeeeeeee'])
  }

  {
    const result = await collect(new ByteStream(core.session(), id, { start: 12 }))
    t.alike(result, ['eeeeeeee', 'aaaa', 'bb', 'ccc', 'd', 'eeeeeeeeee'])
  }

  {
    const result = await collect(new ByteStream(core.session(), id, { start: 12, length: 10 }))
    t.alike(result, ['eeeeeeee', 'aa'])
  }

  {
    const result = await collect(new ByteStream(core.session(), id, { start: 12, length: 5 }))
    t.alike(result, ['eeeee'])
  }

  {
    const result = await collect(new ByteStream(core.session(), id, { start: 12, end: 16 }))
    t.alike(result, ['eeeee'])
  }

  {
    const result = await collect(new ByteStream(core.session(), id, { start: 12, length: 9999 }))
    t.alike(result, ['eeeeeeee', 'aaaa', 'bb', 'ccc', 'd', 'eeeeeeeeee'])
  }
})

test('deferred', async function (t) {
  const { id, core } = await create(t, ['hello', 'world'])

  {
    const b = new ByteStream(null, null)
    b.start(core.session(), id)

    const result = await collect(b)
    t.alike(result, ['hello', 'world'])
  }

  {
    const b = new ByteStream(null, null, { start: 2, length: 4 })
    b.start(core.session(), id)

    const result = await collect(b)
    t.alike(result, ['llo', 'w'])
  }
})

test('one', async function (t) {
  const { core } = await create(t, ['hello', 'world'])

  {
    const b = ByteStream.one(core.session())
    const result = await collect(b)
    t.alike(result, ['hello', 'world'])
  }

  {
    const b = ByteStream.one(core.session(), { start: 2, length: 4 })
    const result = await collect(b)
    t.alike(result, ['llo', 'w'])
  }
})

async function create (t, blocks, repeat = 1) {
  const dir = await tmp(t)
  const core = new Hypercore(dir)

  blocks = blocks.map(b => typeof b === 'string' ? b4a.from(b) : b)

  const batch = []

  for (let i = 0; i < repeat; i++) {
    batch.push(...blocks)
  }

  await core.append(batch)

  t.teardown(() => core.close())

  const id = {
    blockOffset: 0,
    blockLength: core.length,
    byteOffset: 0,
    byteLength: core.byteLength
  }

  return { id, core }
}

async function collect (stream) {
  const chunks = []
  for await (const data of stream) chunks.push(b4a.toString(data))
  return chunks
}

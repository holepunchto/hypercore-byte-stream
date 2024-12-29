# hypercore-byte-stream

A Readable stream around a Hypercore that supports reading byte ranges.

```
npm install hypercore-byte-stream
```

Useful for media streaming

## Usage

``` js
const ByteStream = require('hypercore-byte-stream')

// id is a blob id that sets the outer bounds for the stream
// should contain the following { blockOffset, blockLength, byteOffset, byteLength }

// options can be used be specify a byte range { start, length }
// other options include, { wait, timeout, maxPrefetch }
const stream = new ByteStream(core, id, options)

// if the core only contains a single blob you can use the one helper to init the blob id for you
const unboundedStream = ByteStream.one(core, options)
```

## License

Apache-2.0

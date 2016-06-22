## Every operation is done in a streaming way.

### Uploading a file is implemented as sequence of steps:

1. Extract chunk of N bytes from input stream
2. Calculate checksum of chunk and store it
3. Given M = replication factor, calculate M workers, which will receive this given chunk

Workers organize The Chord Ring which will determine responsibility for given chunk.
### Download a file is implemented as sequence of steps:

1. Determine sequence of chunks and replication factor of file
2. For each chunk, send M requests and wait for the first chunk to arrive. Send arrived chunk to the client


Javascript is used as DSL. [Context](https://github.com/alexsderkach/datastore/blob/master/server/src/main/java/io/datastore/server/execution/scripting/Context.java) provides an API for data manipulation. Supported operations:
- map
- filter
- reduce
- min
- max
- count
- sorted

Every operation is parallelized.
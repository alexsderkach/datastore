## Every operation is done in a streaming way.

### Upload of a file is implemented as sequence of steps:

1. Extract chunk of N bytes from input stream
2. Calculate checksum of chunk
3. Given M = replication factor, calculate M workers, which will receive this given chunk and store it

Workers organize The Chord Ring which will determine responsibility for storing given chunk.
### Download of a file is implemented as sequence of steps:

1. Determine sequence of chunks and replication factor of file
2. For each chunk info, send M requests to workers and wait for the first response (chunk) to arrive. Send arrived chunk to the client


Javascript is used as DSL. [Context](https://github.com/alexsderkach/datastore/blob/master/server/src/main/java/io/datastore/server/execution/scripting/Context.java) and [StreamContext](https://github.com/alexsderkach/datastore/blob/master/server/src/main/java/io/datastore/server/execution/scripting/StreamContext.java) provide an API for data manipulation. Supported operations:
- map
- filter
- reduce
- min
- max
- count
- sorted

Every operation is parallelized.

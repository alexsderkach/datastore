package io.datastore.server.stream.support;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.rxjava.core.buffer.Buffer;
import io.vertx.core.streams.WriteStream;
import io.vertx.rxjava.core.Vertx;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.experimental.Accessors;
import lombok.extern.java.Log;

import java.util.LinkedList;
import java.util.Optional;
import java.util.Queue;

@Log
@Setter
@Accessors(fluent = true, chain = true)
@RequiredArgsConstructor
public class RxChunkingStream implements WriteStream<Buffer> {

  private final Vertx vertx;
  private final BufferHandler bufferHandler;

  private final int chunkSize;
  private final long maxWrites;

  private long writesOutstanding = 0;
  private long written = 0;
  private long bufferLength = 0;
  private Queue<Buffer> bufferQueue = new LinkedList<>();

  private boolean handlerCalledAtLeastOnce = false;
  private boolean closed = false;

  private Optional<Handler<Void>> drainHandler = Optional.empty();
  private Optional<Handler<Throwable>> exceptionHandler = Optional.empty();
  private Optional<Handler<AsyncResult>> endHandler = Optional.empty();

  @Override
  public RxChunkingStream write(Buffer data) {

    bufferQueue.add(data);
    bufferLength += data.length();
    writesOutstanding += data.length();

    processQueue();
    return this;
  }

  private void processQueue() {
    if (bufferLength < chunkSize && !closed) return;

    Buffer mergedBuffer = Buffer.buffer();
    bufferQueue.forEach(mergedBuffer::appendBuffer);

    if (mergedBuffer.length() >= chunkSize) {
      Buffer chunk = mergedBuffer.getBuffer(0, chunkSize);

      bufferQueue.clear();
      bufferQueue.add(mergedBuffer.getBuffer(chunkSize, mergedBuffer.length()));
      bufferLength -= chunk.length();

      handleChunk(chunk);
    } else if (closed && (bufferLength > 0 || !handlerCalledAtLeastOnce)) {
      bufferQueue.clear();
      bufferLength -= mergedBuffer.length();
      handleChunk(mergedBuffer);
    }
  }

  private void handleChunk(Buffer chunk) {
    vertx.executeBlockingObservable(ar -> {
      handlerCalledAtLeastOnce = true;
      bufferHandler.handle(chunk,
          v -> {
            writesOutstanding -= chunk.length();
            written += chunk.length();
            processOutstanding();
          });
    }).subscribe();
  }

  private void processOutstanding() {
    if (closed) {
      end();
    } else if (writesOutstanding < chunkSize) {
      drainHandler.ifPresent(h -> h.handle(null));
    } else {
      processQueue();
    }
  }

  @Override
  public void end() {
    closed = true;
    if (writesOutstanding > 0 || !handlerCalledAtLeastOnce) {
      processQueue();
      return;
    }
    endHandler.ifPresent(h -> h.handle(Future.succeededFuture(written)));
  }

  @Override
  public void end(Buffer buffer) {
    write(buffer);
    end();
  }

  @Override
  public boolean writeQueueFull() {
    return writesOutstanding > maxWrites;
  }

  @Override
  public RxChunkingStream setWriteQueueMaxSize(int maxSize) {
    return this;
  }

  @Override
  public RxChunkingStream exceptionHandler(Handler<Throwable> handler) {
    exceptionHandler = Optional.of(handler);
    return this;
  }

  @Override
  public RxChunkingStream drainHandler(Handler<Void> handler) {
    drainHandler = Optional.of(handler);
    return this;
  }

  public RxChunkingStream endHandler(Handler<AsyncResult> handler) {
    endHandler = Optional.of(handler);
    return this;
  }

  public interface BufferHandler {
    void handle(Buffer buffer, Handler<Void> successHandler);
  }
}

package io.datastore.server.execution.operation.support;

import io.datastore.common.execution.OperationType;
import io.datastore.common.execution.operation.other.Comparator;
import io.datastore.common.execution.stream.DataStream;
import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.streams.ReadStream;
import io.vertx.rxjava.core.Vertx;
import io.vertx.rxjava.core.http.HttpClientResponse;
import lombok.Value;
import lombok.extern.java.Log;

import javax.script.ScriptEngine;
import java.util.Collections;
import java.util.List;
import java.util.PriorityQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.vertx.core.buffer.Buffer.buffer;
import static java.util.stream.Collectors.toCollection;
import static java.util.stream.Collectors.toList;

@Log
public class NWayResponseMergingStream implements ReadStream<Buffer> {

  private final int chunkSize;
  private final Vertx vertx;
  private final DataStream dataStream;
  private final AtomicBoolean streamTerminationResult;


  private Handler<Void> endHandler;
  private Handler<Throwable> exceptionHandler;
  private Handler<Buffer> handler;
  private volatile boolean paused = true;
  private List<HttpClientResponse> responses;
  private Comparator comparator;

  public NWayResponseMergingStream(AtomicBoolean streamTerminationResult,
                                   Vertx vertx,
                                   ScriptEngine engine,
                                   String functionCode,
                                   int chunkSize,
                                   DataStream dataStream,
                                   List<HttpClientResponse> responses) {
    this.streamTerminationResult = streamTerminationResult;
    this.chunkSize = chunkSize >> 2;
    this.dataStream = dataStream;
    this.responses = responses;
    this.vertx = vertx;
    this.comparator = (Comparator) OperationType.COMPARATOR.with(engine, functionCode);
    launchStream();
  }

  @Value
  private class ValueWrapper implements Comparable<ValueWrapper> {
    ResponseToTextDataStream stream;
    String value;

    @Override
    public int compareTo(ValueWrapper other) {
      return comparator.apply(value, other.value).intValue();
    }
  }

  private void launchStream() {
    vertx.executeBlockingObservable(ar -> {
      streamMergedResponses();
      ar.complete(true);
    }, false)
        .subscribe(
            v -> endHandler.handle(null),
            t -> exceptionHandler.handle(t));
  }

  private void streamMergedResponses() {
    log.info("Started merging responses");

    List<ResponseToTextDataStream> streamHolders = responses.stream()
        .map(ResponseToTextDataStream::new)
        .collect(toList());

    Collections.reverse(streamHolders);
    PriorityQueue<ValueWrapper> queue = streamHolders.stream()
        .map(this::getNextValue)
        .collect(toCollection(PriorityQueue::new));

    StringBuilder result = new StringBuilder();

    while (!queue.isEmpty()) {
      ValueWrapper valueWrapper = queue.poll();
      ValueWrapper nextValue = getNextValue(valueWrapper.stream);
      if (nextValue != null) {
        queue.offer(nextValue);
      }
      dataStream.addResult(result, valueWrapper.value);

      if (result.length() >= chunkSize) {
        // flush chunk
        flush(result);
        result = new StringBuilder();
        if (streamTerminationResult.get()) {
          throw new RuntimeException("Stream was terminated in active state");
        }
      }
    }
    // flush what's left
    flush(result);
  }

  private void flush(StringBuilder result) {
    if (result.length() == 0) {
      return;
    }
    while (paused) ;
    String s = result.toString();
    handler.handle(buffer(s));
  }

  private ValueWrapper getNextValue(ResponseToTextDataStream stream) {
    if (!stream.hasNext()) {
      return null;
    }
    return new ValueWrapper(stream, stream.next());
  }

  @Override
  public ReadStream<Buffer> exceptionHandler(Handler<Throwable> exceptionHandler) {
    this.exceptionHandler = exceptionHandler;
    return this;
  }

  @Override
  public ReadStream<Buffer> handler(Handler<Buffer> handler) {
    this.handler = handler;
    return this;
  }

  @Override
  public ReadStream<Buffer> pause() {
    paused = true;
    return this;
  }

  @Override
  public ReadStream<Buffer> resume() {
    paused = false;
    return this;
  }

  @Override
  public ReadStream<Buffer> endHandler(Handler<Void> endHandler) {
    this.endHandler = endHandler;
    return this;
  }
}
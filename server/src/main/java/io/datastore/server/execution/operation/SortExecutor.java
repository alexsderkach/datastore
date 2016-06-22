package io.datastore.server.execution.operation;

import io.datastore.common.HttpUtils;
import io.datastore.common.execution.OperationType;
import io.datastore.common.execution.StreamType;
import io.datastore.common.execution.stream.DataStream;
import io.datastore.server.execution.operation.support.NWayResponseMergingStream;
import io.vertx.core.Handler;
import io.vertx.rxjava.core.buffer.Buffer;
import io.vertx.rxjava.core.http.HttpClientRequest;
import io.vertx.rxjava.core.http.HttpClientResponse;
import io.vertx.rxjava.core.streams.Pump;
import io.vertx.rxjava.core.streams.ReadStream;
import io.vertx.rxjava.core.streams.WriteStream;
import lombok.RequiredArgsConstructor;
import lombok.extern.java.Log;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import rx.subjects.ReplaySubject;

import javax.script.ScriptEngine;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.datastore.common.HttpUtils.SET_URI;
import static io.datastore.common.HttpUtils.UNPROCESSED_PARAM_NAME;
import static io.datastore.common.HttpUtils.putKeyHeader;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.apache.commons.lang3.exception.ExceptionUtils.getStackTrace;

@Log
@Component
@RequiredArgsConstructor(onConstructor = @__(@Autowired))
public class SortExecutor extends OperationExecutor {

  private final ScriptEngine engine;

  @Value("#{${chunk_size_mb}  * 1024 * 1024}")
  private int chunkSize;

  @Override
  public boolean execute(String inputKey,
                         String outputKey,
                         String intermediateResult,
                         boolean passIntermediateToEach,
                         String finalizeFunctionCode,
                         String functionCode,
                         OperationType operationType,
                         StreamType streamType) {
    ResponseHoldingStream responseHoldingStream = new ResponseHoldingStream();
    WriteStream writeStream = WriteStream.newInstance(responseHoldingStream);

    boolean result = execute(
        inputKey,
        outputKey,
        null,
        functionCode,
        operationType,
        streamType,
        writeStream,
        completedFuture(true)
    );

    if (!result) {
      return false;
    }

    log.info("Chunks were sorted, merging");

    CompletableFuture<Object> requestCompletedFuture = new CompletableFuture<>();
    HttpClientRequest request = httpClient.post(myAgentPort, "127.0.0.1", SET_URI);

    putKeyHeader(request, outputKey).setChunked(true);
    request.handler(response -> response.endHandler(v -> requestCompletedFuture.complete(true)));

    DataStream dataStream = streamType.with("", "");
    AtomicBoolean streamTerminationResult = new AtomicBoolean(false);
    ReadStream<Buffer> mergingStream = mergingStream(
        streamTerminationResult,
        responseHoldingStream.responses,
        functionCode,
        dataStream
    );

    Pump pump = Pump.pump(mergingStream, request);

    CompletableFuture<Object> streamCompletionFuture = new CompletableFuture<>();

    request.endHandler(v -> log.info("Request ended"));

    mergingStream.endHandler(v -> {
      log.info("Merge stream has ended");
      request.end();
      streamCompletionFuture.complete(true);
    });
    mergingStream.exceptionHandler(t -> {
      request.end();
      t.printStackTrace();
      streamCompletionFuture.complete(false);
    });

    pump.start();
    mergingStream.resume();

    try {
      CompletableFuture.allOf(streamCompletionFuture, requestCompletedFuture).get(timeout * responseHoldingStream.responses.size(), TimeUnit.MILLISECONDS);
      if (!(boolean) streamCompletionFuture.get()) {
        throw new RuntimeException("Merging streaming failed");
      }
    } catch (Exception e) {
      invalidateKeyData(outputKey);
      log.severe(() -> getStackTrace(e));
      return false;
    }
    return true;
  }

  private ReadStream<Buffer> mergingStream(AtomicBoolean streamTerminationResult,
                                           List<HttpClientResponse> responses,
                                           String functionCode,
                                           DataStream dataStream) {
    NWayResponseMergingStream NWayResponseMergingStream = new NWayResponseMergingStream(
        streamTerminationResult,
        vertx,
        engine,
        functionCode,
        chunkSize,
        dataStream,
        responses
    );
    return ReadStream.newInstance(NWayResponseMergingStream);
  }


  @Override
  protected void handleResponse(HttpClientResponse response,
                                ReplicaOperationRequest replicaOperationRequest,
                                ChunkOperationRequest chunkOperationRequest,
                                OperationRequest operationRequest) {
    log.info(() -> "Response for " + replicaOperationRequest.chunkKey() + " received");

    String unfinishedData = response.getHeader(UNPROCESSED_PARAM_NAME);
    unfinishedData = HttpUtils.decode(unfinishedData);

    ReplaySubject<String> unprocessedDataSubject = chunkOperationRequest.unprocessedDataObservable();
    unprocessedDataSubject.onNext(unfinishedData);
    unprocessedDataSubject.onCompleted();

    response.handler(b -> response.pause());

    chunkOperationRequest.currentWriteStreamObservable()
        .subscribe(writeStream -> {
          if (chunkOperationRequest.isResponseWritten()) {
            return;
          }
          writeStream.write(response);
          chunkOperationRequest.isResponseWritten(true);
          checkTermination(chunkOperationRequest, operationRequest, writeStream);
        });
  }

  private class ResponseHoldingStream implements io.vertx.core.streams.WriteStream<HttpClientResponse> {

    private List<HttpClientResponse> responses = new ArrayList<>();

    @Override
    public io.vertx.core.streams.WriteStream<HttpClientResponse> exceptionHandler(Handler<Throwable> handler) {
      return this;
    }

    @Override
    public io.vertx.core.streams.WriteStream<HttpClientResponse> write(HttpClientResponse response) {
      responses.add(response);
      return null;
    }

    @Override
    public void end() {
    }

    @Override
    public io.vertx.core.streams.WriteStream<HttpClientResponse> setWriteQueueMaxSize(int maxSize) {
      return this;
    }

    @Override
    public boolean writeQueueFull() {
      return false;
    }

    @Override
    public io.vertx.core.streams.WriteStream<HttpClientResponse> drainHandler(Handler<Void> handler) {
      return this;
    }
  }

}

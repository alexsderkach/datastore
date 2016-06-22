package io.datastore.server.execution.operation;

import io.datastore.common.HttpUtils;
import io.datastore.common.execution.OperationType;
import io.datastore.common.execution.StreamType;
import io.datastore.server.metadata.KeyMetadataService;
import io.datastore.server.metadata.Metadata;
import io.datastore.server.stream.Utils;
import io.datastore.server.worker.Worker;
import io.datastore.server.worker.WorkerService;
import io.vertx.rxjava.core.Vertx;
import io.vertx.rxjava.core.buffer.Buffer;
import io.vertx.rxjava.core.http.HttpClient;
import io.vertx.rxjava.core.http.HttpClientRequest;
import io.vertx.rxjava.core.http.HttpClientResponse;
import io.vertx.rxjava.core.streams.WriteStream;
import lombok.Data;
import lombok.experimental.Accessors;
import lombok.extern.java.Log;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import rx.Observable;
import rx.functions.Action1;
import rx.subjects.ReplaySubject;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.datastore.common.Hashing.hash;
import static io.datastore.common.HttpUtils.INTERMEDIATE_RESULT_HEADER_NAME;
import static io.datastore.common.HttpUtils.OPERATION_HEADER_NAME;
import static io.datastore.common.HttpUtils.SCHEDULE_URI;
import static io.datastore.common.HttpUtils.SET_URI;
import static io.datastore.common.HttpUtils.STREAM_TYPE_HEADER_NAME;
import static io.datastore.common.HttpUtils.UNPROCESSED_PARAM_NAME;
import static io.datastore.common.HttpUtils.putHeaders;
import static io.datastore.common.HttpUtils.putKeyHeader;
import static io.vertx.rxjava.core.buffer.Buffer.buffer;
import static org.apache.commons.lang3.exception.ExceptionUtils.getStackTrace;

@Log
public abstract class OperationExecutor {

  @Autowired
  protected HttpClient httpClient;
  @Autowired
  protected WorkerService workerService;
  @Autowired
  protected KeyMetadataService keyMetadataService;
  @Autowired
  protected Vertx vertx;

  @Value("${agent_server_port}")
  protected int myAgentPort;

  @Value("${operation_timeout}")
  protected int timeout;


  public abstract boolean execute(String inputKey,
                                  String outputKey,
                                  String intermediateResult,
                                  boolean passIntermediateToEach,
                                  String finalizeFunctionCode,
                                  String functionCode,
                                  OperationType operationType,
                                  StreamType streamType);

  /**
   * Since we can extract data which will not be processed, even before starting processing,
   * we can receive it and start executing next operation.
   *
   * Create N requests, each will produce 2 observables:
   *  - observable that returns unprocessed data
   *  - observable that returns stream to write to the result
   *
   * Next request is sent to worker, when unprocessed data from previous request arrives
   * Next request is passed WriteStream, when previous request has written to it, and yield it
   */
  boolean execute(String inputKey,
                  String outputKey,
                  String intermediateResult,
                  String functionCode,
                  OperationType operationType,
                  StreamType streamType,
                  WriteStream writeStream,
                  CompletableFuture<Object> streamReceivedFuture) {

    Metadata metadata = keyMetadataService.get(inputKey);
    String originalInputKey = metadata.key();
    int replicationFactor = metadata.replicationFactor();
    String operationName = operationType.name();
    String streamTypeName = streamType.name();

    int chunkCount = metadata.checksums().size();

    CompletableFuture<Boolean> fullOperationCompletionFuture = new CompletableFuture<>();

    AtomicBoolean isFullyTerminated = new AtomicBoolean(false);
    ArrayList<ChunkOperationRequest> chunkOperationRequests = new ArrayList<>(chunkCount);

    OperationRequest operationRequest = new OperationRequest()
        .inputKey(originalInputKey)
        .replicationFactor(replicationFactor)
        .functionCode(functionCode)
        .operationName(operationName)
        .streamType(streamTypeName)
        .isFullyTerminated(isFullyTerminated)
        .chunkCount(chunkCount)
        .outputKey(outputKey)
        .intermediateResult(intermediateResult)
        .fullOperationCompletionFuture(fullOperationCompletionFuture);

    for (int chunkIndex = 1; chunkIndex <= chunkCount; chunkIndex++) {
      ChunkOperationRequest chunkOperationRequest = new ChunkOperationRequest()
          .chunkIndex(chunkIndex)
          .operationRequest(operationRequest);
      chunkOperationRequests.add(chunkOperationRequest);
    }

    ReplaySubject<String> initialUnprocessedDataSubject = ReplaySubject.create();
    ReplaySubject<WriteStream> initialWriteStreamSubject = ReplaySubject.create();

    createOperationChain(
        chunkOperationRequests.iterator(),
        new ResultWrapper(initialUnprocessedDataSubject, initialWriteStreamSubject)
    );

    initialUnprocessedDataSubject.onNext(null);
    initialWriteStreamSubject.onNext(writeStream);

    try {
      CompletableFuture.allOf(streamReceivedFuture, fullOperationCompletionFuture).get();
      return fullOperationCompletionFuture.get();
    } catch (InterruptedException | ExecutionException e) {
      log.severe(e::getMessage);
      return false;
    } finally {
      isFullyTerminated.set(true);
    }
  }

  private void createOperationChain(Iterator<ChunkOperationRequest> operationIterator,
                                    ResultWrapper previousResultWrapper) {
    if (!operationIterator.hasNext()) return;

    ChunkOperationRequest chunkOperationRequest = operationIterator.next();
    OperationRequest operationRequest = chunkOperationRequest.operationRequest();

    log.info("Queueing request for " + operationRequest.inputKey() + "-" + chunkOperationRequest.chunkIndex());

    previousResultWrapper.unprocessedDataObservable()
        .subscribe(previouslyUnprocessedData -> {
          log.info("Received unprocessed data. " +
              "Executing request for " + operationRequest.inputKey() + "-" + chunkOperationRequest.chunkIndex());

          chunkOperationRequest.previouslyUnprocessedData(previouslyUnprocessedData);
          ResultWrapper resultWrapper = executeRequest(chunkOperationRequest, previousResultWrapper.nextWriteStreamObservable());

          createOperationChain(operationIterator, resultWrapper);
        });
  }

  private ResultWrapper executeRequest(ChunkOperationRequest chunkOperationRequest,
                                       Observable<WriteStream> writeStreamObservable) {
    ReplaySubject<String> unprocessedDataObservable = ReplaySubject.create();
    ReplaySubject<WriteStream> nextWriteStreamSubject = ReplaySubject.create();

    chunkOperationRequest
        .unprocessedDataObservable(unprocessedDataObservable)
        .currentWriteStreamObservable(writeStreamObservable)
        .nextWriteStreamObservable(nextWriteStreamSubject);

    vertx.executeBlockingObservable(ar -> executeRequest(chunkOperationRequest))
        .subscribe();

    return new ResultWrapper(unprocessedDataObservable, nextWriteStreamSubject);
  }

  private void executeRequest(ChunkOperationRequest chunkOperationRequest) {

    if (chunkOperationRequest.operationRequest().isFullyTerminated().get()) {
      return;
    }

    OperationRequest operationRequest = chunkOperationRequest.operationRequest();
    CompletableFuture<Boolean> chunkOperationCompletionFuture = new CompletableFuture<>();
    AtomicBoolean isLocallyTerminated = new AtomicBoolean(false);

    log.info(() -> "Executing request: " + chunkOperationRequest.chunkIndex() + "-" + operationRequest.inputKey());

    chunkOperationRequest.isLocallyTerminated(isLocallyTerminated);
    chunkOperationRequest.chunkOperationCompletionFuture(chunkOperationCompletionFuture);
    for (int replica = 1; replica <= operationRequest.replicationFactor(); replica++) {
      String chunkKey = Utils.chunkKey(chunkOperationRequest.chunkIndex(), replica, operationRequest.inputKey());
      ByteBuffer hash = hash(chunkKey);
      ReplicaOperationRequest replicaOperationRequest = new ReplicaOperationRequest()
          .chunkKey(chunkKey)
          .hash(hash)
          .chunkOperationRequest(chunkOperationRequest);
      startOperation(replicaOperationRequest);
    }

    try {
      chunkOperationCompletionFuture.get(timeout, TimeUnit.MILLISECONDS);
    } catch (Exception e) {
      e.printStackTrace();
      chunkOperationRequest.currentWriteStreamObservable()
          .doOnNext(WriteStream::end)
          .subscribe(v -> invalidateKeyData(operationRequest.outputKey()));
      chunkOperationRequest.isLocallyTerminated().set(true);
      chunkOperationRequest.operationRequest().isFullyTerminated().set(true);
      operationRequest.fullOperationCompletionFuture().complete(false);
    }
  }

  protected void tryWorker(ReplicaOperationRequest replicaOperationRequest) {

    ChunkOperationRequest chunkOperationRequest = replicaOperationRequest.chunkOperationRequest();
    OperationRequest operationRequest = chunkOperationRequest.operationRequest();

    log.info(() -> "Processing chunk with key: " + replicaOperationRequest.chunkKey());

    Worker worker = replicaOperationRequest.worker();
    String function = operationRequest.functionCode();
    String previouslyUnprocessedData = HttpUtils.encode(chunkOperationRequest.previouslyUnprocessedData());
    String intermediateResult = HttpUtils.encode(operationRequest.intermediateResult());

    HttpClientRequest request = httpClient.post(worker.getPort(), worker.getHost(), SCHEDULE_URI);
    putHeaders(request, replicaOperationRequest.chunkKey(), function.length());
    request.putHeader(STREAM_TYPE_HEADER_NAME, operationRequest.streamType());
    request.putHeader(OPERATION_HEADER_NAME, operationRequest.operationName());
    if (previouslyUnprocessedData != null) {
      request.putHeader(UNPROCESSED_PARAM_NAME, previouslyUnprocessedData);
    }
    if (intermediateResult != null) {
      request.putHeader(INTERMEDIATE_RESULT_HEADER_NAME, intermediateResult);
    }

    request.toObservable()
        .subscribe(
            response -> handleResponse(response, replicaOperationRequest, chunkOperationRequest, operationRequest),
            e -> {
              log.severe(() -> getStackTrace(e));
              tryNextWorker(replicaOperationRequest);
            }
        );

    request.end(function);
    log.info(() -> "Sent request with key: " + replicaOperationRequest.chunkKey());

  }

  protected void handleResponse(HttpClientResponse response,
                                ReplicaOperationRequest replicaOperationRequest,
                                ChunkOperationRequest chunkOperationRequest,
                                OperationRequest operationRequest) {

    String unfinishedData = response.getHeader(UNPROCESSED_PARAM_NAME);
    unfinishedData = HttpUtils.decode(unfinishedData);

    ReplaySubject<String> unprocessedDataSubject = chunkOperationRequest.unprocessedDataObservable();
    unprocessedDataSubject.onNext(unfinishedData);
    unprocessedDataSubject.onCompleted();

    Buffer operationResult = buffer();
    response.handler(operationResult::appendBuffer);

    response.exceptionHandler(t -> {
      log.severe(() -> getStackTrace(t));
      tryNextWorker(replicaOperationRequest);
    });

    response.endHandler(v -> chunkOperationRequest.currentWriteStreamObservable()
        .subscribe(writeStream -> {
          if (operationResult.length() > 0) {
            writeStream.write(operationResult);
          }
          checkTermination(chunkOperationRequest, operationRequest, writeStream);
        }));
  }

  protected void checkTermination(ChunkOperationRequest chunkOperationRequest,
                                  OperationRequest operationRequest,
                                  WriteStream writeStream) {
    chunkOperationRequest.chunkOperationCompletionFuture().complete(true);
    if (chunkOperationRequest.chunkIndex() == operationRequest.chunkCount()) {
      writeStream.end();
      log.info(() -> "Operation with " + operationRequest.inputKey() + " completed");
      operationRequest.fullOperationCompletionFuture().complete(true);
    } else {
      ReplaySubject<WriteStream> writeStreamSubject = chunkOperationRequest.nextWriteStreamObservable();
      writeStreamSubject.onNext(writeStream);
      writeStreamSubject.onCompleted();
    }
  }

  private void startOperation(ReplicaOperationRequest replicaOperationRequest) {
    vertx.runOnContext(v -> workerService.responsibleWorker(replicaOperationRequest.hash())
        .ifPresent(worker -> tryWorker(replicaOperationRequest.worker(worker))));
  }

  protected void tryNextWorker(ReplicaOperationRequest replicaOperationRequest) {
    ChunkOperationRequest chunkOperationRequest = replicaOperationRequest.chunkOperationRequest();
    if (chunkOperationRequest.isLocallyTerminated().get()
        || chunkOperationRequest.operationRequest().isFullyTerminated().get()) {
      return;
    }

    workerService.getHash(replicaOperationRequest.worker())
        .flatMap(workerService::nextResponsibleWorker)
        .ifPresent(worker -> tryWorker(replicaOperationRequest.worker(worker)));
  }

  protected void invalidateKeyData(String key) {
    log.info(() -> "Invalidating " + key);
    Metadata metadata = keyMetadataService.get(key);
    if (metadata != null) {
      metadata.complete(false);
    }
  }

  @Data
  @Accessors(fluent = true, chain = true)
  public static class OperationRequest {
    private int chunkCount;
    private int replicationFactor;
    private String inputKey;
    private String outputKey;
    private String functionCode;
    private String streamType;
    private String operationName;
    private String intermediateResult;
    private AtomicBoolean isFullyTerminated;
    private CompletableFuture<Boolean> fullOperationCompletionFuture;
  }

  @Data
  @Accessors(fluent = true, chain = true)
  public static class ChunkOperationRequest {
    private OperationRequest operationRequest;
    private boolean isResponseWritten;
    private int chunkIndex;
    private String previouslyUnprocessedData;
    private AtomicBoolean isLocallyTerminated;
    private CompletableFuture<Boolean> chunkOperationCompletionFuture;
    private ReplaySubject<String> unprocessedDataObservable;
    private Observable<WriteStream> currentWriteStreamObservable;
    private ReplaySubject<WriteStream> nextWriteStreamObservable;
    private String intermediateResult;
  }

  @Data
  @Accessors(fluent = true, chain = true)
  public static class ReplicaOperationRequest {
    ChunkOperationRequest chunkOperationRequest;
    String chunkKey;
    Worker worker;
    ByteBuffer hash;
  }

  @lombok.Value
  @Accessors(fluent = true)
  public static class ResultWrapper {
    public ReplaySubject<String> unprocessedDataObservable;
    public ReplaySubject<WriteStream> nextWriteStreamObservable;
  }
}

package io.datastore.server.execution.operation;

import io.datastore.common.ContextConfig;
import io.datastore.common.execution.OperationType;
import io.datastore.common.execution.OperationUtils;
import io.datastore.common.execution.StreamType;
import io.datastore.common.execution.operation.Operation2;
import io.datastore.common.execution.stream.DataStream;
import io.datastore.server.stream.support.RxChunkingStream;
import io.vertx.core.Handler;
import io.vertx.rxjava.core.buffer.Buffer;
import io.vertx.rxjava.core.http.HttpClientRequest;
import io.vertx.rxjava.core.streams.WriteStream;
import lombok.RequiredArgsConstructor;
import lombok.extern.java.Log;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.script.ScriptEngine;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static io.datastore.common.HttpUtils.SET_URI;
import static io.datastore.common.HttpUtils.putKeyHeader;

@Log
@Component
@RequiredArgsConstructor(onConstructor = @__(@Autowired))
public class Operation2Executor extends OperationExecutor {

  private final ScriptEngine engine;

  @Override
  public boolean execute(String inputKey,
                         String outputKey,
                         String intermediateResult,
                         boolean passIntermediateToEach,
                         String finalizeFunctionCode,
                         String functionCode,
                         OperationType operationType,
                         StreamType stream) {
    CompletableFuture<Object> streamReceivedFuture = new CompletableFuture<>();

    Buffer reductionBuffer = Buffer.buffer();
    RxChunkingStream reductionStream = chunkingStream(reductionBuffer::appendBuffer)
        .endHandler(v -> streamReceivedFuture.complete(true));
    WriteStream<Buffer> rxReductionStream = WriteStream.newInstance(reductionStream);

    boolean distributedReductionResult = execute(
        inputKey,
        outputKey,
        passIntermediateToEach ? intermediateResult : null,
        functionCode,
        operationType,
        stream,
        rxReductionStream,
        streamReceivedFuture
    );

    if (!distributedReductionResult) {
      return false;
    }

    if (finalizeFunctionCode == null) {
      finalizeFunctionCode = functionCode;
    }

    DataStream dataStream = stream.with(reductionBuffer.toString(), null);
    Operation2 operation = (Operation2) operationType.with(engine, finalizeFunctionCode);
    String result = OperationUtils.doReduction(dataStream, operation, intermediateResult);

    CompletableFuture<Object> requestCompletedFuture = new CompletableFuture<>();
    HttpClientRequest request = httpClient.post(myAgentPort, "127.0.0.1", SET_URI);

    putKeyHeader(request, outputKey).setChunked(true);
    request.handler(response -> response.endHandler(v -> requestCompletedFuture.complete(true)));
    request.end(result);

    try {
      requestCompletedFuture.get(timeout, TimeUnit.MILLISECONDS);
    } catch (Exception e) {
      invalidateKeyData(outputKey);
      log.severe(e::getMessage);
      return false;
    }
    return true;
  }

  private RxChunkingStream chunkingStream(Handler<Buffer> handler) {
    return new RxChunkingStream(
        vertx,
        (data, successHandler) -> {
          handler.handle(data);
          successHandler.handle(null);
        },
        ContextConfig.HTTP_CHUNK_SIZE,
        ContextConfig.HTTP_CHUNK_SIZE << 1);
  }
}
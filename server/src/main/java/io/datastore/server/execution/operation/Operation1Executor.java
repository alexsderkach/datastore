package io.datastore.server.execution.operation;

import io.datastore.common.execution.OperationType;
import io.datastore.common.execution.StreamType;
import io.vertx.rxjava.core.http.HttpClientRequest;
import lombok.extern.java.Log;
import org.springframework.stereotype.Component;

import java.util.concurrent.CompletableFuture;

import static io.datastore.common.HttpUtils.SET_URI;
import static io.datastore.common.HttpUtils.putKeyHeader;

@Log
@Component
public class Operation1Executor extends OperationExecutor {

  @Override
  public boolean execute(String inputKey,
                         String outputKey,
                         String intermediateResult,
                         boolean passIntermediateToEach,
                         String finalizeFunction,
                         String functionCode,
                         OperationType operationType,
                         StreamType streamType) {
    CompletableFuture<Object> streamReceivedFuture = new CompletableFuture<>();
    HttpClientRequest request = httpClient.post(myAgentPort, "127.0.0.1", SET_URI);

    putKeyHeader(request, outputKey).setChunked(true);
    request.handler(response -> response.endHandler(v -> streamReceivedFuture.complete(true)));

    return execute(inputKey, outputKey, null, functionCode, operationType, streamType, request, streamReceivedFuture);
  }
}
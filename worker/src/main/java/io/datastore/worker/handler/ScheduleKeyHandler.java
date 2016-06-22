package io.datastore.worker.handler;

import io.datastore.common.execution.OperationType;
import io.datastore.common.execution.OperationUtils;
import io.datastore.common.execution.StreamType;
import io.datastore.common.execution.operation.Operation;
import io.datastore.common.execution.operation.Operation1;
import io.datastore.common.execution.operation.Operation2;
import io.datastore.common.execution.operation.other.Comparator;
import io.datastore.common.execution.stream.DataStream;
import io.vertx.rxjava.core.Vertx;
import io.vertx.rxjava.core.buffer.Buffer;
import io.vertx.rxjava.core.http.HttpServerRequest;
import io.vertx.rxjava.core.http.HttpServerResponse;
import lombok.RequiredArgsConstructor;
import lombok.extern.java.Log;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.script.ScriptEngine;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.nio.file.Paths;

import static io.datastore.common.HttpUtils.INTERMEDIATE_RESULT_HEADER_NAME;
import static io.datastore.common.HttpUtils.KEY_HEADER_NAME;
import static io.datastore.common.HttpUtils.OPERATION_HEADER_NAME;
import static io.datastore.common.HttpUtils.STREAM_TYPE_HEADER_NAME;
import static io.datastore.common.HttpUtils.UNPROCESSED_PARAM_NAME;
import static io.datastore.common.HttpUtils.decode;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static io.vertx.rxjava.core.buffer.Buffer.buffer;

@Log
@Component
@RequiredArgsConstructor(onConstructor = @__(@Autowired))
public class ScheduleKeyHandler {

  @Value("${storage_path}")
  private String storagePath;

  private final ScriptEngine engine;
  private final Vertx vertx;

  public void handle(HttpServerRequest request) {
    Buffer body = buffer();
    request.handler(body::appendBuffer);
    request.endHandler(v -> endHandler(request, body));
  }

  private void endHandler(HttpServerRequest request, Buffer body) {
    String streamType = request.getHeader(STREAM_TYPE_HEADER_NAME);
    String key = request.getHeader(KEY_HEADER_NAME);
    String unprocessedData = decode(request.getHeader(UNPROCESSED_PARAM_NAME));
    String operationType = request.getHeader(OPERATION_HEADER_NAME);
    String intermediateResult = decode(request.getHeader(INTERMEDIATE_RESULT_HEADER_NAME));
    String functionCode = body.toString();

    String filePath = Paths.get(storagePath, key).toString();

    log.info(operationType + " - " + key);

    DataStream dataStream;
    try {
      dataStream = StreamType.valueOf(streamType).with(new FileInputStream(filePath), unprocessedData);
    } catch (FileNotFoundException e) {
      e.printStackTrace();
      return;
    }
    Operation operation = OperationType.valueOf(operationType).with(engine, functionCode);

    // respond immediately with data that won't be processed
    HttpServerResponse response = request.response().setChunked(true);
    response.putHeader(UNPROCESSED_PARAM_NAME, dataStream.unprocessed());
    response.setStatusCode(OK.code());


    // prepare executor
    vertx.<String>executeBlockingObservable(ar -> {
      if (operation instanceof Operation1) {
        ar.complete(OperationUtils.doAcceptance(response, key, dataStream, (Operation1) operation));
      } else if (operation instanceof Comparator) {
        response.write(buffer(OK.code()));
        ar.complete(OperationUtils.doSort(response, key, dataStream, (Comparator) operation));
      } else if (operation instanceof Operation2) {
        ar.complete(OperationUtils.doReduction(dataStream, (Operation2) operation, intermediateResult));
      } else {
        ar.fail("Invalid operation");
      }
    }, false)
        .subscribe(response::end);
  }
}
package io.datastore.server.execution;

import io.datastore.common.FileHelper;
import io.datastore.server.metadata.Metadata;
import io.vertx.rxjava.core.Vertx;
import io.vertx.rxjava.core.http.HttpClient;
import io.vertx.rxjava.core.http.HttpClientRequest;
import io.vertx.rxjava.core.http.HttpClientResponse;
import lombok.RequiredArgsConstructor;
import lombok.extern.java.Log;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.nio.file.Paths;

import static io.datastore.common.HttpUtils.CONTENT_LENGTH_HEADER_NAME;
import static io.datastore.common.HttpUtils.GET_URI;
import static io.datastore.common.HttpUtils.putKeyHeader;
import static io.netty.handler.codec.http.HttpResponseStatus.FOUND;
import static java.lang.Long.parseLong;

@Log
@Component
@RequiredArgsConstructor(onConstructor = @__(@Autowired))
public class ScheduleService {

  @Value("${operation_timeout}")
  private int timeout;

  @Value("${agent_server_port}")
  private int myPort;

  @Value("${storage_path}")
  private String storagePath;

  private final HttpClient httpClient;
  private final FileHelper fileHelper;
  private final ExecutionService executionService;
  private final Vertx vertx;

  public void schedule(Metadata metadata) {
    HttpClientRequest request = httpClient.post(myPort, "127.0.0.1", GET_URI);
    String key = metadata.key();
    String outputKey = "script-" + key;
    putKeyHeader(request, key);

    request.toObservable()
        .subscribe(
            response -> handle(response, outputKey),
            e -> log.severe(e::getMessage)
        );

    request.end();
  }

  private void handle(HttpClientResponse response, String outputKey) {
    if (response.statusCode() != FOUND.code()) return;

    long fileSize = parseLong(response.getHeader(CONTENT_LENGTH_HEADER_NAME));

    String outputPath = Paths.get(storagePath, outputKey).toString();

    fileHelper.streamToPath(
        vertx,
        outputPath,
        response,
        v -> handleFullResponse(outputPath, fileSize),
        e -> log.severe(() -> "Couldn't open file: " + e.getMessage())
    );
  }

  private void handleFullResponse(String filePath, long fileSize) {
    log.info(() -> "File received");
    if (!isValid(filePath, fileSize)) {
      log.info(() -> "File is damaged");
    } else {
      log.info(() -> "File is not damaged");
    }

    executionService.execute(filePath);
  }

  private boolean isValid(String filePath, long fileSize) {
    return fileSize == fileHelper.fileSize(filePath);
  }
}

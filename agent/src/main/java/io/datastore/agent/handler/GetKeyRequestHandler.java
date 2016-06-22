package io.datastore.agent.handler;

import io.datastore.common.FileHelper;
import io.vertx.rxjava.core.Vertx;
import io.vertx.rxjava.core.http.HttpClient;
import io.vertx.rxjava.core.http.HttpClientRequest;
import io.vertx.rxjava.core.http.HttpClientResponse;
import lombok.RequiredArgsConstructor;
import lombok.extern.java.Log;

import static io.datastore.agent.Utils.MASTER_PORT;
import static io.datastore.agent.Utils.exit;
import static io.datastore.common.HttpUtils.CONTENT_LENGTH_HEADER_NAME;
import static io.datastore.common.HttpUtils.GET_URI;
import static io.datastore.common.HttpUtils.putKeyHeader;
import static io.netty.handler.codec.http.HttpResponseStatus.FOUND;
import static java.lang.Long.parseLong;

@Log
@RequiredArgsConstructor
public class GetKeyRequestHandler implements RequestHandler {
  private final String master;
  private final String key;
  private final String outputPath;
  private final Vertx vertx;
  private final FileHelper fileHelper;

  @Override
  public void handle() {
    HttpClient httpClient = vertx.createHttpClient();

    HttpClientRequest request = httpClient.post(MASTER_PORT, master, GET_URI);
    putKeyHeader(request, key);

    request.toObservable().subscribe(this::handle, e -> exit(vertx, e::getMessage));

    request.end();
  }

  private void handle(HttpClientResponse response) {
    if (response.statusCode() != FOUND.code()) return;

    long fileSize = parseLong(response.getHeader(CONTENT_LENGTH_HEADER_NAME));

    fileHelper.streamToPath(
        vertx,
        outputPath,
        response,
        v -> handleFullResponse(fileSize),
        e -> exit(vertx, () -> "Couldn't open file: " + e.getMessage())
    );
  }

  private void handleFullResponse(long fileSize) {
    log.info(() -> "File received");
    if (!isValid(fileSize)) {
      log.info(() -> "File is damaged");
    } else {
      log.info(() -> "File is not damaged");
    }
    vertx.close();
  }

  private boolean isValid(long fileSize) {
    return fileSize == fileHelper.fileSize(outputPath);
  }
}

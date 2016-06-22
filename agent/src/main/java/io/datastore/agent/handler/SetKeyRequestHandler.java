package io.datastore.agent.handler;

import io.datastore.common.FileHelper;
import io.vertx.rxjava.core.Vertx;
import io.vertx.rxjava.core.buffer.Buffer;
import io.vertx.rxjava.core.http.HttpClient;
import io.vertx.rxjava.core.http.HttpClientRequest;
import lombok.RequiredArgsConstructor;
import lombok.extern.java.Log;

import static io.datastore.agent.Utils.MASTER_PORT;
import static io.datastore.common.HttpUtils.SET_URI;
import static io.datastore.common.HttpUtils.putHeaders;
import static io.datastore.common.HttpUtils.putKeyHeader;
import static java.lang.String.valueOf;

@Log
@RequiredArgsConstructor
public class SetKeyRequestHandler implements RequestHandler {
  private final String master;
  private final String key;
  private final String inputPath;
  private final Vertx vertx;
  private final FileHelper fileHelper;

  @Override
  public void handle() {
    HttpClient httpClient = vertx.createHttpClient();

    HttpClientRequest request = httpClient.post(MASTER_PORT, master, SET_URI);

    request.toObservable()
        .subscribe(response -> {
          Buffer buffer = Buffer.buffer();
          response.handler(buffer::appendBuffer);
          response.endHandler(v -> {
            log.info(buffer.toString());
            vertx.close();
          });
        });

    putKeyHeader(request, key);
    request.setChunked(true);

    fileHelper.streamFromPath(
        vertx,
        inputPath,
        request,
        v -> log.info(() -> "File sent"),
        e -> {
          log.severe(() -> "Couldn't open file: " + e.getMessage());
          vertx.close();
        }
    );

  }

}

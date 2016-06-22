package io.datastore.worker.handler;

import io.datastore.common.FileHelper;
import io.vertx.rxjava.core.Vertx;
import io.vertx.rxjava.core.http.HttpServerRequest;
import io.vertx.rxjava.core.http.HttpServerResponse;
import lombok.RequiredArgsConstructor;
import lombok.extern.java.Log;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.nio.file.Paths;

import static io.datastore.common.HttpUtils.KEY_HEADER_NAME;
import static io.datastore.common.HttpUtils.putHeaders;
import static io.datastore.common.HttpUtils.setCode;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;

@Log
@Component
@RequiredArgsConstructor(onConstructor = @__(@Autowired))
public class GetKeyHandler {

  @Value("${storage_path}")
  String storagePath;

  private final Vertx vertx;
  private final FileHelper fileHelper;


  public void handle(HttpServerRequest request) {
    String key = request.getHeader(KEY_HEADER_NAME);
    String filePath = Paths.get(storagePath, key).toString();
    long fileSize = fileHelper.fileSize(filePath);

    HttpServerResponse response = request.response();

    putHeaders(response, key, fileSize);

    fileHelper.streamFromPath(
        vertx,
        filePath,
        response,
        v -> {
          log.info(() -> key + " was written to response");
        },
        e -> {
          log.severe(() -> "Couldn't open file: " + e.getMessage());
          setCode(response, NOT_FOUND).end();
        }
    );
  }
}

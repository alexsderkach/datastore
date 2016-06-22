package io.datastore.worker.handler;

import io.datastore.common.FileHelper;
import io.vertx.rxjava.core.Vertx;
import io.vertx.rxjava.core.http.HttpServerRequest;
import lombok.RequiredArgsConstructor;
import lombok.extern.java.Log;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.nio.file.Paths;

import static io.datastore.common.HttpUtils.KEY_HEADER_NAME;
import static io.datastore.common.HttpUtils.finish;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_GATEWAY;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;

@Log
@Component
@RequiredArgsConstructor(onConstructor = @__(@Autowired))
public class SetKeyHandler {

  @Value("${storage_path}")
  String storagePath;

  private final Vertx vertx;
  private final FileHelper fileHelper;


  public void handle(HttpServerRequest request) {
    String key = request.getHeader(KEY_HEADER_NAME);
    String filePath = Paths.get(storagePath, key).toString();

    fileHelper.streamToPath(
        vertx,
        filePath,
        request,
        v -> {
          log.info(() -> key + " was written");
          finish(request.response(), OK, "");
        },
        e -> {
          log.severe(() -> "Couldn't open file: " + e.getMessage());
          finish(request.response(), BAD_GATEWAY, "");
        }
    );
  }
}

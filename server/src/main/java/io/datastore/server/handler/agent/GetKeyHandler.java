package io.datastore.server.handler.agent;

import io.datastore.server.metadata.KeyMetadataRepository;
import io.datastore.server.metadata.KeyMetadataService;
import io.datastore.server.metadata.Metadata;
import io.datastore.server.stream.AggregationService;
import io.vertx.core.Handler;
import io.vertx.rxjava.core.Vertx;
import io.vertx.rxjava.core.http.HttpServerRequest;
import io.vertx.rxjava.core.http.HttpServerResponse;
import lombok.RequiredArgsConstructor;
import lombok.extern.java.Log;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import static io.datastore.common.HttpUtils.KEY_HEADER_NAME;
import static io.datastore.common.HttpUtils.finish;
import static io.datastore.common.HttpUtils.putHeaders;
import static io.datastore.common.HttpUtils.setCode;
import static io.netty.handler.codec.http.HttpResponseStatus.FOUND;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static java.lang.String.valueOf;

@Log
@Component
@RequiredArgsConstructor(onConstructor = @__(@Autowired))
public class GetKeyHandler implements Handler<HttpServerRequest> {

  private final Vertx vertx;
  private final KeyMetadataService keyMetadataService;
  private final AggregationService aggregationService;

  @Override
  public void handle(HttpServerRequest request) {
    String key = request.getHeader(KEY_HEADER_NAME);
    HttpServerResponse response = request.response();

    Metadata metadata = keyMetadataService.get(key);
    if (metadata == null) {
      log.info(() -> key + " not found");
      finish(response, NOT_FOUND, "Key not found");
      return;
    }

    putHeaders(response, key, metadata.fileSize());
    setCode(response, FOUND);

    vertx.executeBlocking(ar -> aggregationService.handle(metadata, response), false, ar -> {});
  }
}

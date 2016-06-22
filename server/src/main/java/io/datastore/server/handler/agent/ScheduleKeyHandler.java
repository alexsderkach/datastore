package io.datastore.server.handler.agent;

import io.datastore.common.HttpUtils;
import io.datastore.server.metadata.KeyMetadataRepository;
import io.datastore.server.metadata.KeyMetadataService;
import io.datastore.server.metadata.Metadata;
import io.datastore.server.execution.ScheduleService;
import io.vertx.core.Handler;
import io.vertx.rxjava.core.Vertx;
import io.vertx.rxjava.core.http.HttpServerRequest;
import io.vertx.rxjava.core.http.HttpServerResponse;
import lombok.RequiredArgsConstructor;
import lombok.extern.java.Log;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import static io.datastore.common.HttpUtils.KEY_HEADER_NAME;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;

@Log
@Component
@RequiredArgsConstructor(onConstructor = @__(@Autowired))
public class ScheduleKeyHandler implements Handler<HttpServerRequest> {

  private final Vertx vertx;
  private final KeyMetadataService keyMetadataService;
  private final ScheduleService scheduleService;

  @Override
  public void handle(HttpServerRequest request) {
    String key = request.getHeader(KEY_HEADER_NAME);
    HttpServerResponse response = request.response();

    Metadata metadata = keyMetadataService.get(key);
    if (metadata == null) {
      log.info(() -> key + " not found");
      HttpUtils.finish(response, NOT_FOUND, "Key not found");
      return;
    }

    scheduleService.schedule(metadata);
    HttpUtils.finish(response, OK, "Scheduled");
  }
}

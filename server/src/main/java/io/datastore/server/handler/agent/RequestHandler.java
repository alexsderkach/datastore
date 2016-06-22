package io.datastore.server.handler.agent;

import io.vertx.core.Handler;
import io.vertx.rxjava.core.http.HttpServerRequest;
import lombok.RequiredArgsConstructor;
import lombok.extern.java.Log;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import static io.datastore.common.HttpUtils.GET_URI;
import static io.datastore.common.HttpUtils.SCHEDULE_URI;
import static io.datastore.common.HttpUtils.SET_URI;

@Log
@Component
@RequiredArgsConstructor(onConstructor = @__(@Autowired))
public class RequestHandler implements Handler<HttpServerRequest> {

  private final SetKeyHandler setKeyHandler;
  private final GetKeyHandler getKeyHandler;
  private final ScheduleKeyHandler scheduleKeyHandler;

  @Override
  public void handle(HttpServerRequest request) {
    String path = request.path();
    switch (path) {
      case GET_URI:
        getKeyHandler.handle(request);
        break;
      case SET_URI:
        setKeyHandler.handle(request);
        break;
      case SCHEDULE_URI:
        scheduleKeyHandler.handle(request);
        break;
    }
  }
}

package io.datastore.agent.handler;

import io.vertx.rxjava.core.Vertx;
import io.vertx.rxjava.core.http.HttpClient;
import io.vertx.rxjava.core.http.HttpClientRequest;
import lombok.RequiredArgsConstructor;
import lombok.extern.java.Log;

import static io.datastore.agent.Utils.MASTER_PORT;
import static io.datastore.common.HttpUtils.SCHEDULE_URI;
import static io.datastore.common.HttpUtils.putKeyHeader;

@Log
@RequiredArgsConstructor
public class ScheduleRequestHandler implements RequestHandler {
  private final String master;
  private final String key;
  private final Vertx vertx;

  @Override
  public void handle() {
    HttpClient httpClient = vertx.createHttpClient();

    HttpClientRequest request = httpClient.post(MASTER_PORT, master, SCHEDULE_URI);
    putKeyHeader(request, key);

    request.handler(r -> {
      r.handler(buffer -> log.info(buffer.toString()));
      r.endHandler(v -> vertx.close());
    });
    request.end();
  }
}

package io.datastore.server;

import io.datastore.common.ContextConfig;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.rxjava.core.Vertx;

public class Main {

  public static void main(String[] args) {
    Vertx vertx = Vertx.vertx();
    ContextConfig.createApplicationContext(vertx);

    vertx.deployVerticle(AgentServerVerticle.class.getName(), verify(vertx));
    vertx.deployVerticle(WorkerServerVerticle.class.getName(), verify(vertx));
  }

  private static Handler<AsyncResult<String>> verify(Vertx vertx) {
    return ar -> {
      if (ar.failed()) {
        vertx.close();
      }
    };
  }
}

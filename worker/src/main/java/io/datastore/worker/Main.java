package io.datastore.worker;

import io.datastore.common.ContextConfig;
import io.vertx.core.AsyncResult;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Handler;
import io.vertx.core.VertxOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.rxjava.core.Vertx;
import lombok.extern.java.Log;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

@Log
public class Main {
  public static void main(String[] args) {
    String master = args[0];
    JsonObject config = new JsonObject().put("master", master).put("httpPort", 50000 + (int)(System.nanoTime() % 10000L));
    DeploymentOptions deploymentOptions = new DeploymentOptions().setConfig(config).setInstances(1);

    Vertx vertx = Vertx.vertx();
    ContextConfig.createApplicationContext(vertx);

    vertx.deployVerticle(WorkerHttpVerticle.class.getName(), deploymentOptions, verify(vertx));
    vertx.deployVerticle(WorkerVerticle.class.getName(), deploymentOptions, verify(vertx));
  }

  private static Handler<AsyncResult<String>> verify(Vertx vertx) {
    return ar -> {
      if (ar.succeeded()) {
        log.info(() -> "Worker start succeeded " + ar.result());
      } else {
        log.severe(() -> "Worker start failed: " + ar.cause().getMessage());
        vertx.close();
      }
    };
  }
}

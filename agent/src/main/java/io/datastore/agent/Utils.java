package io.datastore.agent;

import io.vertx.rxjava.core.Vertx;
import lombok.NoArgsConstructor;
import lombok.extern.java.Log;

import java.util.function.Supplier;

import static lombok.AccessLevel.PRIVATE;

@Log
@NoArgsConstructor(access = PRIVATE)
public final class Utils {

  public static final int MASTER_PORT = 16776;

  public static void exit(Vertx vertx, Supplier<String> message) {
    log.info(message);
    vertx.close();
  }

}

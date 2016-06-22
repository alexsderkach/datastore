package io.datastore.common.command;

import io.vertx.rxjava.core.buffer.Buffer;

public interface CommandWrapper {
  void handle(Buffer buffer, int offset);
}

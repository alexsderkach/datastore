package io.datastore.common.command;

import io.vertx.rxjava.core.net.NetSocket;

public interface CommandHandler {
  CommandWrapper startWith(NetSocket netSocket);
}

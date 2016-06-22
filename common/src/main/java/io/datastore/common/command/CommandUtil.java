package io.datastore.common.command;

import io.vertx.rxjava.core.buffer.Buffer;
import io.vertx.rxjava.core.net.NetSocket;
import lombok.NoArgsConstructor;

import static lombok.AccessLevel.PRIVATE;

@NoArgsConstructor(access = PRIVATE)
public final class CommandUtil {

  public static void handleBuffer(Command command, Buffer buffer, CommandWrapper handler) {
    String commandName = command.name();
    int commandLength = commandName.length();
    if (buffer.length() >= commandLength) {
      String commandBuffer = buffer.getBuffer(0, commandLength).toString();
      if (commandBuffer.equals(commandName)) {
        handler.handle(buffer, commandLength + 1);
      }
    }
  }
}

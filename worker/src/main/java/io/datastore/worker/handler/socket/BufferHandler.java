package io.datastore.worker.handler.socket;

import io.vertx.core.Handler;
import io.vertx.rxjava.core.buffer.Buffer;
import io.vertx.rxjava.core.net.NetSocket;
import lombok.Setter;
import lombok.experimental.Accessors;
import lombok.extern.java.Log;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import static io.datastore.common.command.Command.INIT_SUCCESS;
import static io.datastore.common.command.CommandUtil.handleBuffer;

@Log
@Setter
@Accessors(chain = true, fluent = true)
@Component
@Scope("prototype")
public class BufferHandler implements Handler<Buffer> {

  @Override
  public void handle(Buffer buffer) {
    handleBuffer(INIT_SUCCESS, buffer, this::handleInitSuccess);
  }

  private void handleInitSuccess(Buffer buffer, int offset) {
    log.info(() -> "Worker is serviceable");
  }
}

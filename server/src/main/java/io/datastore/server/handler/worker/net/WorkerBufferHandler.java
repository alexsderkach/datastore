package io.datastore.server.handler.worker.net;

import io.datastore.server.handler.worker.InitWorkerHandler;
import io.vertx.core.Handler;
import io.vertx.rxjava.core.buffer.Buffer;
import io.vertx.rxjava.core.net.NetSocket;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.experimental.Accessors;
import lombok.extern.java.Log;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import static io.datastore.common.command.Command.INIT;
import static io.datastore.common.command.CommandUtil.handleBuffer;

@Log
@Setter
@Accessors(chain = true, fluent = true)
@Component
@Scope("prototype")
@RequiredArgsConstructor(onConstructor = @__(@Autowired))
public class WorkerBufferHandler implements Handler<Buffer> {

  private final InitWorkerHandler initWorkerHandler;

  private NetSocket netSocket;

  @Override
  public void handle(Buffer buffer) {
    handleBuffer(INIT, buffer, initWorkerHandler.startWith(netSocket));
  }
}

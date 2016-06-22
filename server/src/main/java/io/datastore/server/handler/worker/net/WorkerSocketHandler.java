package io.datastore.server.handler.worker.net;

import io.datastore.server.worker.WorkerService;
import io.vertx.core.Handler;
import io.vertx.rxjava.core.net.NetSocket;
import lombok.RequiredArgsConstructor;
import lombok.extern.java.Log;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Lookup;
import org.springframework.stereotype.Component;

@Log
@Component
@RequiredArgsConstructor(onConstructor = @__(@Autowired))
public class WorkerSocketHandler implements Handler<NetSocket> {

  private final WorkerService workerService;

  @Override
  public void handle(NetSocket netSocket) {
    netSocket.handler(getHandler().netSocket(netSocket));
    netSocket.closeHandler(v -> handleClose(netSocket));
  }

  private void handleClose(NetSocket netSocket) {
    workerService.removeWorkerByNetSocket(netSocket);
  }

  @Lookup
  public WorkerBufferHandler getHandler() {
    return null;
  }
}

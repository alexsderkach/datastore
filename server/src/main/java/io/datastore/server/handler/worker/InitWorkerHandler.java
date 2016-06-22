package io.datastore.server.handler.worker;

import io.datastore.common.command.CommandWrapper;
import io.datastore.common.command.CommandHandler;
import io.datastore.server.worker.Worker;
import io.datastore.server.worker.WorkerService;
import io.vertx.rxjava.core.net.NetSocket;
import lombok.RequiredArgsConstructor;
import lombok.extern.java.Log;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import static io.datastore.common.command.Command.INIT_SUCCESS;

@Log
@Component
@RequiredArgsConstructor(onConstructor = @__(@Autowired))
public class InitWorkerHandler implements CommandHandler {

  private final WorkerService workerService;

  @Override
  public CommandWrapper startWith(NetSocket netSocket) {
    return (buffer, offset) -> {
      String info = buffer.getBuffer(offset, buffer.length()).toString();
      String[] split = info.split(" ");
      String mac = split[0];
      String httpPort = split[1];

      Worker worker = workerService.createWorker(netSocket, mac, httpPort);

      netSocket.write(INIT_SUCCESS.toString());

      logNewWorker(worker);
    };
  }

  private void logNewWorker(Worker worker) {
    log.info(() -> "Added new worker: " + worker.getHost() + ":" + worker.getPort());
  }
}

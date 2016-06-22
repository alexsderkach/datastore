package io.datastore.server.worker;

import io.vertx.rxjava.core.net.NetSocket;
import lombok.RequiredArgsConstructor;
import lombok.extern.java.Log;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.nio.ByteBuffer;
import java.util.Optional;

import static io.datastore.common.Hashing.hash;
import static java.lang.Integer.parseInt;

@Log
@Component
@RequiredArgsConstructor(onConstructor = @__(@Autowired))
public class WorkerService {

  private final WorkerRepository workerRepository;

  public Worker createWorker(NetSocket netSocket, String id, String httpPort) {
    ByteBuffer hash = hash(id + httpPort);
    return workerRepository.add(netSocket, hash, httpPort);
  }

  public void removeWorkerByNetSocket(NetSocket netSocket) {
    workerRepository.remove(netSocket);
  }


  public Optional<ByteBuffer> getHash(Worker worker) {
    return workerRepository.getHash(worker);
  }

  public Optional<Worker> responsibleWorker(ByteBuffer hash) {
    return workerRepository.getCeilingWorker(hash);
  }

  public Optional<Worker> nextResponsibleWorker(ByteBuffer hash) {
    return workerRepository.getNextWorker(hash);
  }
}

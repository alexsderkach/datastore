package io.datastore.server.worker;

import io.vertx.rxjava.core.net.NetSocket;
import lombok.extern.java.Log;
import org.springframework.stereotype.Component;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;

@Log
@Component
public class WorkerRepository {

  private Map<Worker, ByteBuffer> workerToHashStore;
  private Map<NetSocket, Worker> socketToWorkerStore;
  private NavigableMap<ByteBuffer, Worker> hashToWorkerStore;

  public WorkerRepository() {
    workerToHashStore = new ConcurrentHashMap<>();
    socketToWorkerStore = new ConcurrentHashMap<>();
    hashToWorkerStore = new ConcurrentSkipListMap<>();
  }

  public Worker add(NetSocket socket, ByteBuffer hash, String httpPort) {
    Worker worker = new Worker(socket, socket.remoteAddress().host(), Integer.parseInt(httpPort), hash);

    workerToHashStore.put(worker, hash);
    socketToWorkerStore.put(socket, worker);
    hashToWorkerStore.put(hash, worker);
    return worker;
  }

  public Optional<ByteBuffer> getHash(Worker worker) {
    return Optional.ofNullable(workerToHashStore.get(worker));
  }

  public Optional<Worker> getCeilingWorker(ByteBuffer hash) {
    Optional<Worker> worker = Optional.ofNullable(hashToWorkerStore.ceilingEntry(hash))
        .map(Map.Entry::getValue);
    if (worker.isPresent()) {
      return worker;
    }
    return getFirst();
  }

  public Optional<Worker> getNextWorker(ByteBuffer hash) {
    Optional<Worker> socket = Optional.ofNullable(hashToWorkerStore.higherEntry(hash))
        .map(Map.Entry::getValue);
    if (socket.isPresent()) {
      return socket;
    }
    return getFirst();
  }

  private Optional<Worker> getFirst() {
    return Optional.ofNullable(hashToWorkerStore.firstEntry())
        .map(Map.Entry::getValue);
  }

  public void remove(NetSocket netSocket) {
    Worker worker = socketToWorkerStore.get(netSocket);
    if (worker != null) {
      workerToHashStore.remove(worker);
      socketToWorkerStore.remove(worker.getNetSocket());
      hashToWorkerStore.remove(worker.getHash());
      log.info(() -> "Removed worker " + worker.getHost() + ":" + worker.getPort());
    }
  }
}

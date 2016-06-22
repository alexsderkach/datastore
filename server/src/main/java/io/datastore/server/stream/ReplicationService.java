package io.datastore.server.stream;

import io.datastore.server.metadata.KeyMetadataService;
import io.datastore.server.worker.Worker;
import io.datastore.server.worker.WorkerService;
import io.vertx.core.Handler;
import io.vertx.core.impl.ConcurrentHashSet;
import io.vertx.rxjava.core.Vertx;
import io.vertx.rxjava.core.buffer.Buffer;
import io.vertx.rxjava.core.http.HttpClient;
import io.vertx.rxjava.core.http.HttpClientRequest;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.experimental.Accessors;
import lombok.extern.java.Log;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.nio.ByteBuffer;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.datastore.common.Hashing.hash;
import static io.datastore.common.HttpUtils.SET_URI;
import static io.datastore.common.HttpUtils.putHeaders;
import static io.datastore.server.stream.Utils.chunkKey;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

@Log
@Component
@RequiredArgsConstructor(onConstructor = @__(@Autowired))
public class ReplicationService {

  @Value("${replication_factor}")
  private int replicationFactor;
  @Value("${operation_timeout}")
  private int timeout;

  private final HttpClient httpClient;
  private final WorkerService workerService;
  private final KeyMetadataService keyMetadataService;
  private final Vertx vertx;

  public void replicate(long chunkIndex,
                        String key,
                        io.vertx.core.buffer.Buffer data,
                        Handler<Throwable> throwableHandler,
                        Handler<Void> successHandler) {
    Buffer rxBuffer = Buffer.newInstance(data);
    this.replicate(replicationFactor, chunkIndex, key, rxBuffer, throwableHandler, successHandler);
  }

  private void replicate(int replicationFactor,
                         long chunkIndex,
                         String key,
                         Buffer data,
                         Handler<Throwable> throwableHandler,
                         Handler<Void> successHandler) {
    CountDownLatch latch = new CountDownLatch(replicationFactor);
    Set<Worker> visited = new ConcurrentHashSet<>();
    AtomicBoolean isTerminated = new AtomicBoolean(false);
    log.info("Replication started");
    for (int i = 1; i <= replicationFactor; i++) {
      String chunkKey = chunkKey(chunkIndex, i, key);
      ByteBuffer hash = hash(chunkKey);
      log.info(() -> "Trying to replicate " + chunkKey);
      ReplicationRequest replicationRequest = new ReplicationRequest()
          .visited(visited)
          .key(chunkKey)
          .data(data)
          .successHandler(v -> latch.countDown())
          .isTerminated(isTerminated);
      vertx.runOnContext(v -> {
        workerService.responsibleWorker(hash)
            .ifPresent(worker -> tryWorker(replicationRequest.worker(worker)));
      });
    }

    try {
      // try to replicate in N milliseconds to prevent long running operations
      if (!latch.await(timeout, MILLISECONDS)) {
        isTerminated.set(true);
        throw new RuntimeException();
      } else {
        isTerminated.set(true);
        keyMetadataService.get(key).checksums().add(hash(data));
        log.info(() -> "Replication for chunk " + chunkIndex + "-" + key + " succeeded");
        vertx.runOnContext(v -> successHandler.handle(null));
      }
    } catch (Exception ignored) {
      long count = latch.getCount();
      if (count > 0) {
        String message = "Couldn't replicate to " + count + " workers in " + timeout + "ms";
        throwableHandler.handle(new RuntimeException(message));
        log.severe(() -> message);
      }
    }
  }

  private void tryWorker(ReplicationRequest replicationRequest) {
    if (replicationRequest.isTerminated().get()) return;

    Worker worker = replicationRequest.worker();
    Set<Worker> visited = replicationRequest.visited();
    if (worker == null) return;
    if (visited.contains(worker)) {
      tryNextWorker(replicationRequest);
      return;
    }

    visited.add(worker);

    HttpClientRequest request = httpClient.post(worker.getPort(), worker.getHost(), SET_URI);
    request.toObservable()
        .subscribe(
            response -> {
              if (response.statusCode() == OK.code()) {
                log.info(() -> "Replica " + replicationRequest.key() + " was created");
                replicationRequest.successHandler().handle(null);
              } else {
                throw new RuntimeException("Couldn't replicate to " + worker.getHost() + ":" + worker.getPort());
              }
            },
            e -> {
              log.severe(e::getMessage);
              tryNextWorker(replicationRequest);
            }
        );
    Buffer data = replicationRequest.data();
    putHeaders(request, replicationRequest.key(), data.length()).end(data);
  }

  private void tryNextWorker(ReplicationRequest replicationRequest) {
    vertx.runOnContext(v -> {
      workerService.getHash(replicationRequest.worker())
          .flatMap(workerService::nextResponsibleWorker)
          .ifPresent(worker -> tryWorker(replicationRequest.worker(worker)));
    });
  }

  @Data
  @Accessors(fluent = true, chain = true)
  private static class ReplicationRequest {
    Set<Worker> visited;
    Worker worker;
    String key;
    Buffer data;
    Handler<Void> successHandler;
    AtomicBoolean isTerminated;
  }
}

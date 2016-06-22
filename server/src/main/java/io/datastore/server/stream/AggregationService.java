package io.datastore.server.stream;

import io.datastore.common.Hashing;
import io.datastore.server.metadata.Metadata;
import io.datastore.server.worker.Worker;
import io.datastore.server.worker.WorkerService;
import io.vertx.core.Handler;
import io.vertx.rxjava.core.buffer.Buffer;
import io.vertx.rxjava.core.http.HttpClient;
import io.vertx.rxjava.core.http.HttpClientRequest;
import io.vertx.rxjava.core.http.HttpServerResponse;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.experimental.Accessors;
import lombok.extern.java.Log;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.nio.ByteBuffer;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.datastore.common.HttpUtils.GET_URI;
import static io.datastore.common.HttpUtils.KEY_HEADER_NAME;
import static io.datastore.common.HttpUtils.finish;
import static io.datastore.server.stream.Utils.chunkKey;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;

@Log
@Component
@RequiredArgsConstructor(onConstructor = @__(@Autowired))
public class AggregationService {

  @Value("${operation_timeout}")
  private int timeout;

  private final HttpClient httpClient;
  private final WorkerService workerService;

  public void handle(Metadata metadata, HttpServerResponse response) {
    int replicationFactor = metadata.replicationFactor();
    long fileSize = metadata.fileSize();
    String key = metadata.key();

    log.info("Aggregating key " + key);

    int chunkIndex = 1;
    for (ByteBuffer checksum : metadata.checksums()) {
      boolean written = false;
      for (int replicaId = 1; replicaId <= replicationFactor && !written; replicaId++) {
        CountDownLatch latch = new CountDownLatch(1);
        String chunkKey = chunkKey(chunkIndex, replicaId, key);
        ByteBuffer hash = Hashing.hash(chunkKey);
        AtomicBoolean isTerminated = new AtomicBoolean(false);
        ChunkRequest chunkRequest = new ChunkRequest()
            .checksum(checksum)
            .hash(hash)
            .key(chunkKey)
            .response(response)
            .successHandler(v -> latch.countDown())
            .isTerminated(isTerminated);
        findAndWriteChunk(chunkRequest);
        try {
          written = latch.await(timeout, TimeUnit.MILLISECONDS);
          isTerminated.set(true);
        } catch (InterruptedException ignored) {
        }
      }
      if (!written) {
        finish(response, NOT_FOUND, "Couldn't find worker for block " + chunkIndex);
        return;
      } else {
        log.info(() -> "Written: " + response.bytesWritten() + "/" + fileSize);
      }
      chunkIndex++;
    }
    response.end();
  }

  private void findAndWriteChunk(ChunkRequest chunkRequest) {
    if (chunkRequest.isTerminated().get()) return;

    workerService.responsibleWorker(chunkRequest.hash()).ifPresent(worker -> tryWorker(chunkRequest.worker(worker)));
  }

  private void tryWorker(ChunkRequest chunkRequest) {
    Worker worker = chunkRequest.worker();
    HttpClientRequest request = httpClient.get(worker.getPort(), worker.getHost(), GET_URI);
    request.toObservable()
        .subscribe(
            workerResponse -> {
              if (workerResponse.statusCode() == OK.code()) {
                Buffer mergedBuffer = Buffer.buffer();
                workerResponse.handler(mergedBuffer::appendBuffer);
                workerResponse.endHandler(v -> {
                  ByteBuffer receivedChecksum = Hashing.hash(mergedBuffer);
                  if (chunkRequest.checksum().equals(receivedChecksum)) {
                    chunkRequest.response().write(mergedBuffer);
                    chunkRequest.successHandler().handle(null);
                  } else {
                    throw new RuntimeException("Checksum validation failed from " + worker.getHost() + ":" + worker.getPort());
                  }
                });
              } else {
                throw new RuntimeException("Couldn't download from " + worker.getHost() + ":" + worker.getPort());
              }
            },
            e -> {
              log.severe(e::getMessage);
              tryNextWorker(chunkRequest);
            }
        );
    request.putHeader(KEY_HEADER_NAME, chunkRequest.key()).end();
  }

  private void tryNextWorker(ChunkRequest chunkRequest) {
    workerService.getHash(chunkRequest.worker())
        .flatMap(workerService::nextResponsibleWorker)
        .ifPresent(worker -> tryWorker(chunkRequest.worker(worker)));
  }

  @Data
  @Accessors(fluent = true, chain = true)
  private static class ChunkRequest {
    ByteBuffer checksum;
    ByteBuffer hash;
    String key;
    HttpServerResponse response;
    Handler<Void> successHandler;
    AtomicBoolean isTerminated;
    Worker worker;
  }
}

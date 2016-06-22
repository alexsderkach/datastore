package io.datastore.server.handler.agent;

import io.datastore.server.metadata.KeyMetadataService;
import io.datastore.server.metadata.Metadata;
import io.datastore.server.stream.ReplicationService;
import io.datastore.server.stream.support.ChunkingStream;
import io.vertx.core.Handler;
import io.vertx.rxjava.core.Vertx;
import io.vertx.rxjava.core.buffer.Buffer;
import io.vertx.rxjava.core.http.HttpServerRequest;
import io.vertx.rxjava.core.http.HttpServerResponse;
import io.vertx.rxjava.core.streams.Pump;
import io.vertx.rxjava.core.streams.WriteStream;
import lombok.RequiredArgsConstructor;
import lombok.extern.java.Log;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import static io.datastore.common.HttpUtils.KEY_HEADER_NAME;
import static io.datastore.common.HttpUtils.finish;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_GATEWAY;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static io.vertx.rxjava.core.streams.Pump.pump;
import static java.lang.String.valueOf;

@Log
@Component
@RequiredArgsConstructor(onConstructor = @__(@Autowired))
public class SetKeyHandler implements Handler<HttpServerRequest> {

  private final Vertx vertx;
  private final ReplicationService replicationService;
  private final KeyMetadataService keyMetadataService;

  @Value("${replication_factor}")
  private int replicationFactor;
  @Value("#{${chunk_size_mb}  * 1024 * 1024}")
  private int chunkSize;

  @Override
  public void handle(HttpServerRequest request) {
    String key = request.getHeader(KEY_HEADER_NAME);

    log.info(() -> "Setting value for key '" + key + "'");

    HttpServerResponse response = request.response();

    request.pause();

    Metadata metadata = new Metadata().replicationFactor(replicationFactor).key(key);
    keyMetadataService.set(key, metadata);

    ChunkingStream chunkingStream = chunkingStream(key)
        .exceptionHandler(e -> finish(response, BAD_GATEWAY, e.getMessage()))
        .endHandler(ar -> {
          if (!response.closed()) {
            log.info(() -> "Setting value for key '" + key + "' is successful");
            long fileSize = (long) ar.result();
            keyMetadataService.get(key).complete(true).fileSize(fileSize);
            finish(response, OK, "Key is set");
          }
        });

    WriteStream<Buffer> rxChunkingStream = WriteStream.<Buffer>newInstance(chunkingStream);
    Pump pump = pump(request, rxChunkingStream);

    request.endHandler(v -> rxChunkingStream.end());

    pump.start();
    request.resume();
  }

  private ChunkingStream chunkingStream(String originalKey) {
    return new ChunkingStream(
        vertx,
        (data, currentChunk, key, throwableHandler, successHandler)
            -> replicationService.replicate(currentChunk, key, data, throwableHandler, successHandler),
        originalKey,
        chunkSize,
        chunkSize << 1);
  }

}

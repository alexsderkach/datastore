package io.datastore.worker;

import io.datastore.worker.handler.RequestHandler;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.rxjava.core.AbstractVerticle;
import lombok.extern.java.Log;
import org.springframework.beans.factory.annotation.Autowired;

import static io.datastore.common.ContextConfig.HTTP_CHUNK_SIZE;
import static io.datastore.common.ContextConfig.getContext;

@Log
public class WorkerHttpVerticle extends AbstractVerticle {

  @Autowired
  private RequestHandler requestHandler;

  @Override
  public void start() throws Exception {
    getContext().getAutowireCapableBeanFactory().autowireBean(this);

    HttpServerOptions options = new HttpServerOptions()
        .setUsePooledBuffers(false)
        .setMaxChunkSize(HTTP_CHUNK_SIZE)
        .setReceiveBufferSize(HTTP_CHUNK_SIZE)
        .setSendBufferSize(HTTP_CHUNK_SIZE);

    vertx.createHttpServer(options)
        .requestHandler(requestHandler)
        .listen(config().getInteger("httpPort"));
  }
}

package io.datastore.server;

import io.datastore.common.ContextConfig;
import io.datastore.server.handler.agent.RequestHandler;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.rxjava.core.AbstractVerticle;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;

import static io.datastore.common.ContextConfig.HTTP_CHUNK_SIZE;

public class AgentServerVerticle extends AbstractVerticle {

  @Value("${agent_server_port}")
  private int port;

  @Autowired
  private RequestHandler requestHandler;

  @Override
  public void start() throws Exception {

    ContextConfig.getContext().getAutowireCapableBeanFactory().autowireBean(this);

    HttpServerOptions options = new HttpServerOptions()
        .setUsePooledBuffers(true)
        .setMaxChunkSize(HTTP_CHUNK_SIZE)
        .setReceiveBufferSize(HTTP_CHUNK_SIZE)
        .setSendBufferSize(HTTP_CHUNK_SIZE);

    vertx.createHttpServer(options)
        .requestHandler(requestHandler)
        .listen(port);
  }
}

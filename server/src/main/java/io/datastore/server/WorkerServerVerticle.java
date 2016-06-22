package io.datastore.server;

import io.datastore.common.ContextConfig;
import io.datastore.server.handler.worker.net.WorkerSocketHandler;
import io.vertx.rxjava.core.AbstractVerticle;
import lombok.extern.java.Log;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;

import java.io.File;

@Log
public class WorkerServerVerticle extends AbstractVerticle {

  @Value("${worker_server_port}")
  private int port;

  @Value("${storage_path}")
  private String storagePath;

  @Autowired
  private WorkerSocketHandler workerSocketHandler;

  @Override
  public void start() throws Exception {

    ContextConfig.getContext().getAutowireCapableBeanFactory().autowireBean(this);

    vertx.createNetServer()
        .connectHandler(workerSocketHandler)
        .listen(port);

    try {
      new File(storagePath).mkdirs();
      log.info(() -> "Successfully created " + storagePath);
    } catch (Exception e) {
      log.severe(() -> "Couldn't create directory: " + e.getMessage());
    }
  }
}

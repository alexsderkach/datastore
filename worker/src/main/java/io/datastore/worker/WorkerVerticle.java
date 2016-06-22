package io.datastore.worker;

import io.datastore.common.ContextConfig;
import io.datastore.worker.handler.socket.NetSocketHandler;
import io.vertx.core.net.NetClientOptions;
import io.vertx.rxjava.core.AbstractVerticle;
import io.vertx.rxjava.core.net.NetClient;
import lombok.extern.java.Log;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;

import java.io.File;

@Log
public class WorkerVerticle extends AbstractVerticle {

  @Value("${master_server_port}")
  private int port;

  @Value("${storage_path}")
  private String storagePath;

  @Autowired
  private NetSocketHandler netSocketHandler;

  @Override
  public void start() throws Exception {
    ContextConfig.getContext().getAutowireCapableBeanFactory().autowireBean(this);

    String master = config().getString("master");

    NetClient netClient = vertx.createNetClient(new NetClientOptions());
    netSocketHandler.setHttpPort(config().getInteger("httpPort"));

    netClient.connectObservable(this.port, master)
        .subscribe(netSocketHandler);

    try {
      new File(storagePath).mkdirs();
      log.info(() -> "Successfully created " + storagePath);
    } catch (Exception e) {
      log.severe(() -> "Couldn't create directory: " + e.getMessage());
    }
  }
}

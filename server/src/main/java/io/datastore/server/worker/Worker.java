package io.datastore.server.worker;

import io.vertx.rxjava.core.net.NetSocket;
import lombok.Value;

import java.nio.ByteBuffer;

@Value
public class Worker {
  private NetSocket netSocket;
  private String host;
  private int port;
  private ByteBuffer hash;
}
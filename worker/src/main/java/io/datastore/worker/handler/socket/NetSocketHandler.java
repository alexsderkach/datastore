package io.datastore.worker.handler.socket;

import io.vertx.rxjava.core.Vertx;
import io.vertx.rxjava.core.net.NetSocket;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.extern.java.Log;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Lookup;
import org.springframework.stereotype.Component;
import rx.Observer;

import java.util.logging.Level;

import static io.datastore.worker.support.NetworkUtil.getId;

@Log
@Component
@RequiredArgsConstructor(onConstructor = @__(@Autowired))
@Setter
public class NetSocketHandler implements Observer<NetSocket> {

  private Integer httpPort;
  private final Vertx vertx;

  @Override
  public void onCompleted() { }

  @Override
  public void onError(Throwable e) {
    log.log(Level.SEVERE, e, () -> "Got error during connection establishment. Exiting");
    vertx.close();
  }

  @Override
  public void onNext(NetSocket netSocket) {
    getId().ifPresent(id -> performInit(netSocket, id));
  }

  private void performInit(NetSocket netSocket, String id) {
    netSocket.endHandler(v -> vertx.close());
    netSocket.handler(getHandler());
    netSocket.write("INIT " + id + " " + httpPort);

  }

  @Lookup
  public BufferHandler getHandler() {
    return null;
  }
}

package io.datastore.server.execution.operation.support;

import io.vertx.rxjava.core.buffer.Buffer;
import io.vertx.rxjava.core.http.HttpClientResponse;
import lombok.extern.java.Log;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;

@Log
public class ResponseToTextDataStream {
  private static final int BUFFER_TIMEOUT = 30000;
  private static final int BUFFER_SIZE = 1 << 13;

  private final HttpClientResponse response;
  private boolean hasEnded = false;
  private Buffer buffer = Buffer.buffer();
  private String itemBuffer = null;
  private int position = 0;
  private CountDownLatch latch;

  public ResponseToTextDataStream(HttpClientResponse response) {
    this.response = response;
    response.endHandler(v -> {
      this.hasEnded = true;
      latch.countDown();
    });
  }

  public boolean hasNext() {
    calculateNext();
    return itemBuffer != null;
  }

  public String next() {
    calculateNext();

    String tmp = itemBuffer;
    itemBuffer = null;
    return tmp;
  }

  private void calculateNext() {
    if (itemBuffer == null) {
      saveNextFromBufferToItemBuffer();
    }
    while (itemBuffer == null && !hasEnded) {
      appendToBufferFromResponse();
      saveNextFromBufferToItemBuffer();
    }
  }

  private void saveNextFromBufferToItemBuffer() {
    StringBuilder builder = new StringBuilder();

    for (int i = position; i < buffer.length(); i++) {
      String c = buffer.getString(i, i + 1);
      if (!"\n".equals(c)) {
        builder.append(c);
      } else {
        position = i + 1;
        itemBuffer = builder.toString();
        break;
      }
    }
  }

  private void appendToBufferFromResponse() {
    latch = new CountDownLatch(1);
    response.handler(tmpBuffer -> {
      response.pause();
      buffer.appendBuffer(tmpBuffer);
      latch.countDown();
    });
    response.resume();
    try {
      if (!latch.await(BUFFER_TIMEOUT, TimeUnit.MILLISECONDS)) {
        throw new RuntimeException("Couldn't fill buffer in " + BUFFER_TIMEOUT);
      }
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }
}

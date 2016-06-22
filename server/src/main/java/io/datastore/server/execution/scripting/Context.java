package io.datastore.server.execution.scripting;

import lombok.RequiredArgsConstructor;
import lombok.extern.java.Log;

@Log
@RequiredArgsConstructor
public class Context {

  public StreamContext text(String key) {
    log.info(() -> "Streaming " + key + " as text file");
    return new TextStreamContext(key);
  }
}

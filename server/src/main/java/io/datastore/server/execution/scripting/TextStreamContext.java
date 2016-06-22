package io.datastore.server.execution.scripting;

import io.datastore.common.execution.StreamType;
import lombok.extern.java.Log;

@Log
public class TextStreamContext extends StreamContext {

  public TextStreamContext(String key) {
    super(null, key, StreamType.TEXT);
  }

  public TextStreamContext(String prefix, String key) {
    super(prefix, key, StreamType.TEXT);
  }

  @Override
  protected StreamContext generateNextContext(String newPrefix, String key) {
    return new TextStreamContext(newPrefix, key);
  }
}

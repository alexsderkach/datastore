package io.datastore.server.metadata;

import lombok.extern.java.Log;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

@Log
@Component
public class KeyMetadataRepository {
  private Map<String, Metadata> store = new ConcurrentHashMap<>();

  public Metadata get(String key) {
    return store.get(key);
  }

  public Metadata set(String key, Metadata metadata) {
    store.put(key, metadata);
    return metadata;
  }

  public void remove(String key) {
    store.remove(key);
  }
}

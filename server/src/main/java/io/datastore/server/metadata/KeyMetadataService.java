package io.datastore.server.metadata;

import lombok.RequiredArgsConstructor;
import lombok.extern.java.Log;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

@Log
@Component
@RequiredArgsConstructor(onConstructor = @__(@Autowired))
public class KeyMetadataService {
  private final KeyMetadataRepository repository;
  private Map<String, String> aliases = new HashMap<>();

  public Metadata get(String key) {
    String mainKey = getMainKey(key);
    return repository.get(mainKey);
  }

  public Metadata set(String key, Metadata metadata) {
    String mainKey = getMainKey(key);
    return repository.set(mainKey, metadata);
  }

  public void linkTo(String newKey, String key) {
    String mainKey = getMainKey(key);
    aliases.put(newKey, mainKey);
    log.info("New Alias: " + newKey + " -> " + key);
  }

  public void remove(String key) {
    String mainKey = getMainKey(key);
    repository.remove(mainKey);
  }

  public String getMainKey(String key) {
    String s;
    while ((s = aliases.get(key)) != null) {
      key = s;
    }
    return key;
  }

}

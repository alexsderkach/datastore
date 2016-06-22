package io.datastore.server.metadata;

import lombok.Data;
import lombok.experimental.Accessors;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

@Data
@Accessors(fluent = true, chain = true)
public class Metadata {
  private List<ByteBuffer> checksums = new ArrayList<>();
  private String key;
  private int replicationFactor;
  private volatile long fileSize;
  private volatile boolean complete;
}

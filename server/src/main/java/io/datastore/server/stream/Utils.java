package io.datastore.server.stream;

public final class Utils {
  public static String chunkKey(long chunkIndex, int replica, String key) {
    return chunkIndex + "-" + replica + "-" + key;
  }
}

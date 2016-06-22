package io.datastore.common;

import io.vertx.rxjava.core.buffer.Buffer;
import lombok.NoArgsConstructor;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import static lombok.AccessLevel.PRIVATE;

@NoArgsConstructor(access = PRIVATE)
public final class Hashing {

  private static MessageDigest DIGEST;
  private static final String ALGORITHM = "SHA-256";

  static {
    try {
      DIGEST = MessageDigest.getInstance(ALGORITHM);
    } catch (Exception ignored) {}
  }

  public static ByteBuffer hash(byte[] bytes) {
    return ByteBuffer.wrap(DIGEST.digest(bytes));
  }


  public static ByteBuffer hash(String value) {
    return hash(value.getBytes(StandardCharsets.UTF_8));
  }

  public static ByteBuffer hash(Buffer buffer) {
    io.vertx.core.buffer.Buffer delegate = (io.vertx.core.buffer.Buffer) buffer.getDelegate();
    return hash(delegate.getBytes());
  }
}

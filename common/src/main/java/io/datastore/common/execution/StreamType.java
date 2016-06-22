package io.datastore.common.execution;

import io.datastore.common.execution.stream.DataStream;
import io.datastore.common.execution.stream.TextDataStream;

import java.io.FileInputStream;

public enum StreamType {
  TEXT {
    public DataStream with(FileInputStream fileInputStream, String unprocessedData) {
      return new TextDataStream(fileInputStream, unprocessedData);
    }

    public DataStream with(String data, String unprocessedData) {
      return new TextDataStream(data, unprocessedData);
    }
  };

  public abstract DataStream with(FileInputStream fileInputStream, String unprocessedData);

  public abstract DataStream with(String data, String unprocessedData);

}

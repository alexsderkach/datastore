package io.datastore.common.execution.stream;

import lombok.extern.java.Log;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.ArrayUtils;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Iterator;

@Log
public class TextDataStream implements DataStream, Iterable<String> {
  private final char[] data;

  public TextDataStream(FileInputStream fileInputStream, String prefix) {
    try {
      if (null == prefix) {
        prefix = "";
      }
      String lines = prefix + IOUtils.toString(fileInputStream);
      this.data = lines.toCharArray();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public TextDataStream(String data, String prefix) {
    if (null == prefix) {
      prefix = "";
    }
    this.data = (prefix + data).toCharArray();
  }

  @Override
  public String unprocessed() {
    int lastIndexNewLine = ArrayUtils.lastIndexOf(data, '\n');
    if (lastIndexNewLine == -1) {
      return new String(data);
    } else if (lastIndexNewLine == data.length - 1) {
      return "";
    } else {
      int nextIndex = lastIndexNewLine + 1;
      return new String(data, nextIndex, data.length - nextIndex);
    }
  }

  @Override
  public String decodeResult(String result) {
    // cut until last '\n'
    int lastIndexNewLine = result.lastIndexOf('\n');
    if (lastIndexNewLine == -1) {
      return result;
    } else {
      return result.substring(0, lastIndexNewLine);
    }
  }

  @Override
  public String encodeResult(String result) {
    return result + '\n';
  }

  @Override
  public StringBuilder addResult(StringBuilder builder, String data) {
    if (data != null) {
      builder.append(encodeResult(data));
    }
    return builder;
  }

  @Override
  public TextIterator iterator() {
    return new TextIterator();
  }

  private class TextIterator implements Iterator<String> {

    private int position = 0;
    private String buffer = null;

    @Override
    public boolean hasNext() {
      saveNextToBuffer();
      return buffer != null;
    }

    @Override
    public String next() {
      saveNextToBuffer();
      String tmp = buffer;
      buffer = null;
      return tmp;
    }

    private void saveNextToBuffer() {
      if (buffer != null) return;

      StringBuilder builder = new StringBuilder();

      for (int i = position; i < data.length; i++) {
        char c = data[i];
        if (c != '\n') {
          builder.append(c);
        } else {
          position = i + 1;
          buffer = builder.toString();
          break;
        }
      }
    }
  }
}

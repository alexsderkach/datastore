package io.datastore.common.execution.stream;

public interface DataStream extends Iterable<String> {
  String unprocessed();
  String decodeResult(String result);
  String encodeResult(String result);
  StringBuilder addResult(StringBuilder builder, String data);
}

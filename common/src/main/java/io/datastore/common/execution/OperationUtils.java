package io.datastore.common.execution;

import io.datastore.common.execution.operation.Operation1;
import io.datastore.common.execution.operation.Operation2;
import io.datastore.common.execution.operation.other.Comparator;
import io.datastore.common.execution.stream.DataStream;
import io.vertx.rxjava.core.http.HttpServerResponse;
import lombok.extern.java.Log;
import org.apache.commons.lang3.time.StopWatch;

import java.util.ArrayList;
import java.util.Collections;

@Log
public class OperationUtils {

  public static String doAcceptance(HttpServerResponse response, String key, DataStream dataStream, Operation1 operation) {

    log.info("Started acceptance");

    StopWatch stopWatch = new StopWatch();
    stopWatch.start();

    int i = 1;
    StringBuilder buffer = new StringBuilder();
    for (String data : dataStream) {
      String result = operation.apply(data);
      dataStream.addResult(buffer, result);
      if (i % 100000 == 0) {
        flushBuffer(response, buffer);
        buffer = new StringBuilder();
        log.info("Sent back " + key + " " + i + " entries");
      }
      i++;
    }
    flushBuffer(response, buffer);
    stopWatch.stop();

    log.info("Accept calculated in " + stopWatch.toString() + " - " + i + " items processed");

    return "";
  }

  public static String doReduction(DataStream dataStream, Operation2 operation, String intermediateResult) {

    log.info("Started reduction");

    // do calculation
    String result = intermediateResult;
    if (result != null) {
      result = dataStream.decodeResult(result);
    }

    int i = 1;
    StopWatch stopWatch = new StopWatch();
    stopWatch.start();
    for (String data : dataStream) {
      if (result == null) {
        result = data;
        continue;
      }
      result = (String) operation.apply(result, data);
      i++;
    }
    stopWatch.stop();

    log.info("Reduce calculated in " + stopWatch.toString() + " - " + i + " items processed");

    return result == null ? "" : dataStream.encodeResult(result);
  }

  public static String doSort(HttpServerResponse response, String key, DataStream dataStream, Comparator comparator) {
    log.info("Started sorting");

    StopWatch stopWatch = new StopWatch();
    stopWatch.start();

    ArrayList<String> values = new ArrayList<>();
    for (String s : dataStream) {
      values.add(s);
    }
    log.info("Ready to sort in " + stopWatch);
    // sorting in Java with comparator is faster 3 times than in JS, because we need to create large array in context
    Collections.sort(values, (a, b) -> comparator.apply(a, b).intValue());
    log.info("Sorted " + values.size() + " entries in " + stopWatch);

    int i = 1;
    StringBuilder buffer = new StringBuilder();
    for (String value : values) {
      dataStream.addResult(buffer, value);
      if (i % 100000 == 0) {
        flushBuffer(response, buffer);
        buffer = new StringBuilder();
        log.info("Sent back " + key + " " + i + " entries");
      }
      i++;
    }
    flushBuffer(response, buffer);
    stopWatch.stop();

    log.info("Result calculated in " + stopWatch.toString() + " - " + i + " items processed");

    return "";
  }

  private static void flushBuffer(HttpServerResponse response, StringBuilder buffer) {
    if (buffer.length() > 0) {
      response.write(buffer.toString());
    }
  }

}

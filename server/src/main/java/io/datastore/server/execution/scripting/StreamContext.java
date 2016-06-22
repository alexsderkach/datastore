package io.datastore.server.execution.scripting;

import io.datastore.common.execution.OperationType;
import io.datastore.common.execution.StreamType;
import io.datastore.server.execution.operation.Operation1Executor;
import io.datastore.server.execution.operation.Operation2Executor;
import io.datastore.server.execution.operation.OperationExecutor;
import io.datastore.server.execution.operation.SortExecutor;
import io.datastore.server.metadata.KeyMetadataService;
import jdk.nashorn.api.scripting.JSObject;
import lombok.extern.java.Log;
import org.apache.commons.lang3.time.StopWatch;

import java.util.UUID;

import static io.datastore.common.ContextConfig.getContext;
import static io.datastore.common.execution.OperationType.COMPARATOR;
import static io.datastore.common.execution.OperationType.FILTER;
import static io.datastore.common.execution.OperationType.MAP;
import static io.datastore.common.execution.OperationType.REDUCE;

@Log
public abstract class StreamContext {
  private final String prefix;
  private final String key;
  private final StreamType streamType;
  private final KeyMetadataService keyMetadataService = getContext().getBean(KeyMetadataService.class);
  private final Operation1Executor operation1Executor = getContext().getBean(Operation1Executor.class);
  private final Operation2Executor operation2Executor = getContext().getBean(Operation2Executor.class);
  private final SortExecutor sortExecutor = getContext().getBean(SortExecutor.class);

  StreamContext(String prefix, String key, StreamType streamType) {
    this.prefix = prefix;
    this.key = key;
    this.streamType = streamType;
  }

  protected abstract StreamContext generateNextContext(String newPrefix, String key);

  public StreamContext map(JSObject function) {
    return doAccept(function.toString(), MAP);
  }

  public StreamContext filter(JSObject function) {
    return doAccept(function.toString(), FILTER);
  }

  public StreamContext reduce(JSObject function) {
    return doReduce(function.toString(), null);
  }

  public StreamContext min() {
    return doMin("function (v) {return v.length;}");
  }

  public StreamContext max() {
    return doMax("function (v) {return v.length;}");
  }

  public StreamContext count() {
    return doReduce(
        "function (a, b) { return '' + (parseFloat(a) + 1); }",
        "0",
        true,
        "function (a, b) { return '' + (parseFloat(a) + parseFloat(b)); }"
    );
  }

  public StreamContext min(JSObject extractor) {
    return doMin(extractor.toString());
  }

  public StreamContext max(JSObject extractor) {
    return doMax(extractor.toString());
  }

  public StreamContext sorted() {
    return doSort("function (a, b) {" +
        "return a.length - b.length;" +
        "}");
  }

  public StreamContext sorted(JSObject function) {
    return doSort(function.toString());
  }

  public void save(String resultKey) {
    String currentKey = getFullKey();

    log.info(() -> "Saving from " + currentKey + " to " + resultKey);

    keyMetadataService.linkTo(resultKey, currentKey);

    log.info(() -> "Saving from " + currentKey + " to " + resultKey);
  }

  private StreamContext doReduce(String function, String intermediateResult) {
    return doReduce(function, intermediateResult, false, null);
  }

  private StreamContext doReduce(String function, String intermediateResult, boolean passIntermediateToEach, String finalizeFunction) {
    return execute(
        function,
        intermediateResult,
        passIntermediateToEach,
        REDUCE,
        operation2Executor,
        finalizeFunction);
  }

  private StreamContext doMin(String extractor) {
    String function = "function (a, b) {" +
        "var na = " + extractor + ".apply(null, [a]);" +
        "var nb = " + extractor + ".apply(null, [b]);" +
        "return na < nb ? a : b;" +
        "}";
    return doReduce(function, null);
  }

  private StreamContext doMax(String extractor) {
    String function = "function (a, b) {" +
        "var na = " + extractor + ".apply(null, [a]);" +
        "var nb = " + extractor + ".apply(null, [b]);" +
        "return na > nb ? a : b;" +
        "}";
    return doReduce(function, null);
  }

  private StreamContext doAccept(String function, OperationType operationType) {
    return execute(function, null, false, operationType, operation1Executor, null);
  }

  private StreamContext execute(String function,
                                String intermediateResult,
                                boolean passIntermediateToEach,
                                OperationType operationType,
                                OperationExecutor operationExecutor,
                                String finalizeFunctionCode) {
    String operationName = operationType.name();
    StreamContext nextContext = generateNextContext(UUID.randomUUID().toString(), key);
    String inputKey = getFullKey();
    String outputKey = nextContext.getFullKey();

    log.info(() -> operationName + " from " + inputKey + " to " + outputKey);

    StopWatch stopWatch = new StopWatch();
    stopWatch.start();

    boolean result = operationExecutor.execute(
        inputKey,
        outputKey,
        intermediateResult,
        passIntermediateToEach,
        finalizeFunctionCode,
        function,
        operationType,
        streamType
    );

    stopWatch.stop();
    log.info(stopWatch.toString());

    if (result) {
      log.info(() -> operationName + " from " + inputKey + " to " + outputKey + " succeeded");
      return nextContext;
    }

    log.info(() -> "Failed to " + operationName + " from " + inputKey + " to " + outputKey);
    return null;
  }


  private StreamContext doSort(String function) {
    return execute(function, null, false, COMPARATOR, sortExecutor, null);
  }

  private String getFullKey() {
    return prefix == null ? key : prefix + "-" + key;
  }
}

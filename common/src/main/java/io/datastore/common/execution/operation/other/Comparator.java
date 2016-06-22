package io.datastore.common.execution.operation.other;

import io.datastore.common.execution.operation.Operation2;

public class Comparator extends Operation2 {
  @Override
  public Double apply(String a, String b) {
    return (Double) function.call(null, a, b);
  }
}

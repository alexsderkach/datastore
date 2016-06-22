package io.datastore.common.execution.operation.one;

import io.datastore.common.execution.operation.Operation1;

public class Map extends Operation1 {

  @Override
  public String apply(String data) {
    return (String) function.call(null, data);
  }
}

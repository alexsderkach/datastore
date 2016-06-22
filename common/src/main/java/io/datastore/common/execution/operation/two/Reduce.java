package io.datastore.common.execution.operation.two;

import io.datastore.common.execution.operation.Operation2;
import lombok.extern.java.Log;

@Log
public class Reduce extends Operation2 {

  @Override
  public String apply(String a, String b) {
    return (String) function.call(null, a, b);
  }
}

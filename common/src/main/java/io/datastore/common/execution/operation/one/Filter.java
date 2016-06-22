package io.datastore.common.execution.operation.one;

import io.datastore.common.execution.operation.Operation1;
import lombok.extern.java.Log;

@Log
public class Filter extends Operation1 {

  @Override
  public String apply(String data) {
    Boolean accepted = (Boolean) function.call(null, data);
    return accepted ? data : null;
  }
}

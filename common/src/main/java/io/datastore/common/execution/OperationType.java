package io.datastore.common.execution;

import io.datastore.common.execution.operation.Operation;
import io.datastore.common.execution.operation.one.Filter;
import io.datastore.common.execution.operation.one.Map;
import io.datastore.common.execution.operation.other.Comparator;
import io.datastore.common.execution.operation.two.Reduce;

import javax.script.ScriptEngine;

public enum OperationType {
  MAP {
    @Override
    public Operation with(ScriptEngine engine, String functionCode) {
      return new Map().with(engine, functionCode);
    }
  },
  FILTER {
    @Override
    public Operation with(ScriptEngine engine, String functionCode) {
      return new Filter().with(engine, functionCode);
    }
  },
  REDUCE {
    @Override
    public Operation with(ScriptEngine engine, String functionCode) {
      return new Reduce().with(engine, functionCode);
    }
  },
  COMPARATOR {
    @Override
    public Operation with(ScriptEngine engine, String functionCode) {
      return new Comparator().with(engine, functionCode);
    }
  };

  public abstract Operation with(ScriptEngine engine, String functionCode);
}

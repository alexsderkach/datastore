package io.datastore.common.execution.operation;

import jdk.nashorn.api.scripting.JSObject;

import javax.script.ScriptEngine;
import javax.script.ScriptException;
import javax.script.SimpleScriptContext;

import static javax.script.ScriptContext.ENGINE_SCOPE;

public abstract class Operation {
  protected static final String FUNCTION_NAME = "$f";
  protected JSObject function;
  protected SimpleScriptContext context;
  protected ScriptEngine engine;

  public Operation with(ScriptEngine engine, String functionCode) {
    this.engine = engine;
    this.context = new SimpleScriptContext();
    this.context.setBindings(engine.createBindings(), ENGINE_SCOPE);
    try {
      engine.eval("var " + FUNCTION_NAME + " = " + functionCode, this.context);
    } catch (ScriptException e) {
      throw new RuntimeException(e);
    }
    this.function = (JSObject) this.context.getAttribute("$f", ENGINE_SCOPE);
    return this;
  }
}

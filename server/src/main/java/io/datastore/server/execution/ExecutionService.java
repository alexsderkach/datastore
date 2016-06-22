package io.datastore.server.execution;

import io.datastore.server.execution.scripting.Context;
import io.vertx.rxjava.core.Vertx;
import lombok.RequiredArgsConstructor;
import lombok.extern.java.Log;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.script.Bindings;
import javax.script.ScriptEngine;
import javax.script.ScriptException;
import javax.script.SimpleScriptContext;
import java.io.FileNotFoundException;
import java.io.FileReader;

import static javax.script.ScriptContext.ENGINE_SCOPE;
import static org.apache.commons.lang3.exception.ExceptionUtils.getStackTrace;

@Log
@Component
@RequiredArgsConstructor(onConstructor = @__(@Autowired))
public class ExecutionService {

  private final Vertx vertx;
  private final ScriptEngine engine;

  public void execute(String filePath) {
    vertx.executeBlockingObservable(
        ar -> {
          try {
            log.info(() -> "Executing " + filePath);

            SimpleScriptContext context = new SimpleScriptContext();
            context.setBindings(engine.createBindings(), ENGINE_SCOPE);
            Bindings bindings = context.getBindings(ENGINE_SCOPE);

            bindings.put("Context", new Context());

            engine.eval(new FileReader(filePath), context);
            ar.complete(filePath);

          } catch (ScriptException | FileNotFoundException e) {
            ar.fail(e);
          }
        },
        false
    ).subscribe(ar -> log.info(() -> "Job " + ar + " succeeded"), e -> log.severe(e::getMessage));
  }
}

package io.datastore.agent.handler;

import lombok.RequiredArgsConstructor;
import lombok.extern.java.Log;
import org.apache.commons.lang3.exception.ExceptionUtils;

import java.io.IOException;
import java.io.InputStream;
import java.util.StringJoiner;
import java.util.logging.Level;

import static java.util.stream.Collectors.joining;
import static org.apache.commons.io.IOUtils.readLines;
import static org.apache.commons.lang3.exception.ExceptionUtils.getStackTrace;

@Log
@RequiredArgsConstructor
public class WorkerRequestHandler implements RequestHandler {

  private final String master;

  @Override
  public void handle() {
    try {
      StringJoiner command = new StringJoiner(" ");
      command.add("java").add("-jar").add("worker.jar").add(master);

      log.info(() -> "Executing command: " + command.toString());
      Runtime.getRuntime().exec(command.toString());
    } catch (IOException e) {
      // Handle error.
      log.severe(() -> getStackTrace(e));
    }

  }
}

package io.datastore.agent;

import io.datastore.agent.handler.GetKeyRequestHandler;
import io.datastore.agent.handler.RequestHandler;
import io.datastore.agent.handler.ScheduleRequestHandler;
import io.datastore.agent.handler.SetKeyRequestHandler;
import io.datastore.agent.handler.WorkerRequestHandler;
import io.datastore.common.FileHelper;
import io.vertx.rxjava.core.Vertx;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import java.util.Optional;

import static java.util.Optional.of;

public class Main {
  public static void main(String[] args) throws ParseException {

    Options options = new Options();
    options.addOption("set", true, "Key to set");
    options.addOption("input", true, "File to use as input");
    options.addOption("get", true, "Key to get");
    options.addOption("output", true, "File to use as output");
    options.addOption("schedule", true, "Key to schedule task from");
    options.addOption("worker", false, "Enable worker mode");

    DefaultParser parser = new DefaultParser();

    CommandLine cl = parser.parse(options, args);
    String[] otherArguments = cl.getArgs();
    if (otherArguments.length != 1 || cl.getOptions().length == 0) {
      printHelp(options);
      return;
    }

    FileHelper fileHelper = new FileHelper();
    Vertx vertx = Vertx.vertx();
    String master = otherArguments[0];
    Optional<RequestHandler> processorOptional = Optional.empty();
    if (cl.hasOption("set") && cl.hasOption("input")) {
      processorOptional = of(new SetKeyRequestHandler(master, cl.getOptionValue("set"), cl.getOptionValue("input"), vertx, fileHelper));
    } else if (cl.hasOption("get") && cl.hasOption("output")) {
      processorOptional = of(new GetKeyRequestHandler(master, cl.getOptionValue("get"), cl.getOptionValue("output"), vertx, fileHelper));
    } else if (cl.hasOption("schedule")) {
      processorOptional = of(new ScheduleRequestHandler(master, cl.getOptionValue("schedule"), vertx));
    } else if (cl.hasOption("worker")) {
      processorOptional = of(new WorkerRequestHandler(master));
    } else {
      printHelp(options);
    }
    processorOptional.ifPresent(RequestHandler::handle);
  }

  private static void printHelp(Options options) {
    HelpFormatter helpFormatter = new HelpFormatter();
    helpFormatter.printHelp("Options", options);
  }
}

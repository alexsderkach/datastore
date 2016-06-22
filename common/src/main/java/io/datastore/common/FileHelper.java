package io.datastore.common;

import io.vertx.core.Handler;
import io.vertx.core.file.OpenOptions;
import io.vertx.rxjava.core.Vertx;
import io.vertx.rxjava.core.buffer.Buffer;
import io.vertx.rxjava.core.http.HttpClientRequest;
import io.vertx.rxjava.core.streams.Pump;
import io.vertx.rxjava.core.streams.ReadStream;
import io.vertx.rxjava.core.streams.WriteStream;
import lombok.extern.java.Log;
import org.apache.commons.lang3.time.StopWatch;
import org.springframework.stereotype.Component;
import rx.functions.Action1;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

@Log
@Component
public class FileHelper {

  public long fileSize(String path) {
    try {
      return Files.size(Paths.get(path));
    } catch (IOException e) {
      log.severe(() -> "Could not calculate file size: " + e.getMessage());
      throw new RuntimeException(e);
    }
  }

  public void streamToPath(Vertx vertx,
                           String outputPath,
                           ReadStream<Buffer> readStream,
                           Handler<Void> endHandler,
                           Action1<Throwable> exceptionHandler) {
    readStream.pause();

    try {
      Files.deleteIfExists(Paths.get(outputPath));
    } catch (IOException e) {
      e.printStackTrace();
      return;
    }

    vertx.fileSystem().openObservable(outputPath, new OpenOptions())
        .subscribe(file -> {
              Pump pump = Pump.pump(readStream, file);

              readStream.endHandler(v -> {
                file.closeObservable().subscribe(v1 -> {}, exceptionHandler);
                endHandler.handle(v);
              });

              pump.start();
              readStream.resume();
            },
            exceptionHandler
        );
  }

  public void streamFromPath(Vertx vertx,
                             String inputPath,
                             WriteStream<Buffer> writeBuffer,
                             Handler<Void> endHandler,
                             Action1<Throwable> exceptionHandler) {
    vertx.fileSystem().openObservable(inputPath, new OpenOptions().setCreate(false))
        .subscribe(
            file -> {
              StopWatch stopWatch = new StopWatch();
              Pump pump = Pump.pump(file, writeBuffer);

              file.endHandler(v -> file.closeObservable()
                  .subscribe(
                      v1 -> {
                        writeBuffer.end();
                        endHandler.handle(v);
                        stopWatch.stop();
                        log.info(() -> "File uploaded in " + stopWatch);
                      },
                      exceptionHandler
                  )
              );

              pump.start();
              stopWatch.start();
            },
            exceptionHandler
        );
  }
}

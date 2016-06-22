package io.datastore.common;

import io.vertx.core.http.HttpClientOptions;
import io.vertx.rxjava.core.Vertx;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.support.PropertySourcesPlaceholderConfigurer;
import org.springframework.core.io.ClassPathResource;

import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;

public class ContextConfig {
  public static final int HTTP_CHUNK_SIZE = 1 << 16;
  private static final int MAX_CONCURRENT_SOCKET_CONNECTIONS = 5000;
  private static AnnotationConfigApplicationContext CONTEXT;
  private static boolean INITIALIZED;

  public static void createApplicationContext(Vertx vertx) {
    if (INITIALIZED) return;

    AnnotationConfigApplicationContext applicationContext = new AnnotationConfigApplicationContext();
    applicationContext.getBeanFactory().registerSingleton("eventBus", vertx.eventBus());
    applicationContext.getBeanFactory().registerSingleton("vertx", vertx);

    HttpClientOptions httpClientOptions = new HttpClientOptions()
        .setUsePooledBuffers(false)
        .setMaxChunkSize(HTTP_CHUNK_SIZE)
        .setReceiveBufferSize(HTTP_CHUNK_SIZE)
        .setMaxPoolSize(MAX_CONCURRENT_SOCKET_CONNECTIONS)
        .setSendBufferSize(HTTP_CHUNK_SIZE);
    applicationContext.getBeanFactory().registerSingleton("httpClient", vertx.createHttpClient(httpClientOptions));

    ScriptEngine scriptEngine = new ScriptEngineManager().getEngineByName("nashorn");
    applicationContext.getBeanFactory().registerSingleton("scriptEngine", scriptEngine);

    applicationContext.scan("io.datastore");
    PropertySourcesPlaceholderConfigurer pph = new PropertySourcesPlaceholderConfigurer();
    pph.setLocation(new ClassPathResource("/application.properties"));
    applicationContext.addBeanFactoryPostProcessor(pph);
    applicationContext.refresh();

    synchronized (ContextConfig.class) {
      CONTEXT = applicationContext;
      INITIALIZED = true;
    }
  }

  public static AnnotationConfigApplicationContext getContext() {
    return CONTEXT;
  }
}

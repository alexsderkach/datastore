package io.datastore.worker.handler;

import io.datastore.common.FileHelper;
import io.vertx.rxjava.core.Vertx;
import io.vertx.rxjava.core.http.HttpServerRequest;
import io.vertx.rxjava.core.http.HttpServerResponse;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import javax.xml.bind.ValidationEvent;
import java.nio.file.Paths;

import static io.datastore.common.HttpUtils.KEY_HEADER_NAME;
import static org.junit.Assert.*;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class SetKeyHandlerTest {
  public static final String STORAGE_PATH = "/tmp/datastore";
  public static final String KEY = "key1";
  public String FILE_PATH = Paths.get(STORAGE_PATH, KEY).toString();

  @Mock
  private Vertx vertx;
  @Mock
  private FileHelper fileHelper;
  @Mock
  private HttpServerRequest httpServerRequest;

  private SetKeyHandler setKeyHandler;

  @Before
  public void setUp() {
    setKeyHandler = new SetKeyHandler(vertx, fileHelper);
    setKeyHandler.storagePath = STORAGE_PATH;
  }

  @Test
  public void shouldStreamToPathWhenHandleIsCalled() {
    when(httpServerRequest.getHeader(eq(KEY_HEADER_NAME))).thenReturn(KEY);

    setKeyHandler.handle(httpServerRequest);

    verify(fileHelper).streamToPath(eq(vertx), eq(FILE_PATH), eq(httpServerRequest), any(), any());
  }
}
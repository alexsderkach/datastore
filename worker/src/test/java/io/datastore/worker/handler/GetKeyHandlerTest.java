package io.datastore.worker.handler;

import io.datastore.common.FileHelper;
import io.vertx.rxjava.core.Vertx;
import io.vertx.rxjava.core.http.HttpClient;
import io.vertx.rxjava.core.http.HttpClientRequest;
import io.vertx.rxjava.core.http.HttpClientResponse;
import io.vertx.rxjava.core.http.HttpServerRequest;
import io.vertx.rxjava.core.http.HttpServerResponse;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.nio.file.Paths;

import static io.datastore.common.HttpUtils.CONTENT_LENGTH_HEADER_NAME;
import static io.datastore.common.HttpUtils.KEY_HEADER_NAME;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class GetKeyHandlerTest {
  public static final String STORAGE_PATH = "/tmp/datastore";
  public static final String KEY = "key1";
  public static final long FILE_SIZE = 100L;
  public String FILE_PATH = Paths.get(STORAGE_PATH, KEY).toString();

  @Mock
  private Vertx vertx;
  @Mock
  private FileHelper fileHelper;
  @Mock
  private HttpServerResponse httpServerResponse;
  @Mock
  private HttpServerRequest httpServerRequest;

  private GetKeyHandler getKeyHandler;

  @Before
  public void setUp() {
    getKeyHandler = new GetKeyHandler(vertx, fileHelper);
    getKeyHandler.storagePath = STORAGE_PATH;
  }

  @Test
  public void shouldStreamFileFromPathWhenHandleIsCalled() {
    when(httpServerRequest.getHeader(eq(KEY_HEADER_NAME))).thenReturn(KEY);
    when(fileHelper.fileSize(eq(FILE_PATH))).thenReturn(FILE_SIZE);
    when(httpServerRequest.response()).thenReturn(httpServerResponse);
    when(httpServerResponse.putHeader(anyString(), anyString())).thenReturn(httpServerResponse);

    getKeyHandler.handle(httpServerRequest);

    verify(httpServerResponse).putHeader(eq(KEY_HEADER_NAME), eq(KEY));
    verify(httpServerResponse).putHeader(eq(CONTENT_LENGTH_HEADER_NAME), eq("" + FILE_SIZE));

    verify(fileHelper).streamFromPath(eq(vertx), eq(FILE_PATH), eq(httpServerResponse), any(), any());
  }
}
package io.datastore.agent.handler;

import io.datastore.common.FileHelper;
import io.vertx.rxjava.core.Vertx;
import io.vertx.rxjava.core.http.HttpClient;
import io.vertx.rxjava.core.http.HttpClientRequest;
import io.vertx.rxjava.core.http.HttpClientResponse;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import rx.subjects.PublishSubject;

import static io.datastore.agent.Utils.MASTER_PORT;
import static io.datastore.common.HttpUtils.CONTENT_LENGTH_HEADER_NAME;
import static io.datastore.common.HttpUtils.GET_URI;
import static io.datastore.common.HttpUtils.KEY_HEADER_NAME;
import static io.netty.handler.codec.http.HttpResponseStatus.FOUND;
import static org.junit.Assert.*;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class GetKeyRequestHandlerTest {

  public static final String MASTER = "127.0.0.1";
  public static final String KEY = "key1";
  public static final String OUTPUT_PATH = "/tmp/key1";
  public static final String FILE_SIZE = "100";

  @Mock
  private Vertx vertx;
  @Mock
  private FileHelper fileHelper;
  @Mock
  private HttpClient httpClient;
  @Mock
  private HttpClientRequest httpClientRequest;
  @Mock
  private HttpClientResponse httpClientResponse;

  private GetKeyRequestHandler getKeyRequestHandler;

  @Before
  public void setUp() {
    getKeyRequestHandler = new GetKeyRequestHandler(MASTER, KEY, OUTPUT_PATH, vertx, fileHelper);
  }

  @Test
  public void shouldStreamToPathWhenResponseIsReceivedAndKeyIsFound() {
    // given
    PublishSubject<HttpClientResponse> requestObservable = PublishSubject.create();

    when(vertx.createHttpClient()).thenReturn(httpClient);
    when(httpClient.post(MASTER_PORT, MASTER, GET_URI)).thenReturn(httpClientRequest);
    when(httpClientRequest.toObservable()).thenReturn(requestObservable);
    when(httpClientResponse.getHeader(CONTENT_LENGTH_HEADER_NAME)).thenReturn(FILE_SIZE);
    when(httpClientResponse.statusCode()).thenReturn(FOUND.code());

    // when
    getKeyRequestHandler.handle();
    requestObservable.onNext(httpClientResponse);
    requestObservable.onCompleted();

    // then
    verify(httpClientRequest).putHeader(eq(KEY_HEADER_NAME), eq(KEY));
    verify(httpClientRequest).end();
    verify(fileHelper).streamToPath(eq(vertx), eq(OUTPUT_PATH), eq(httpClientResponse), any(), any());
  }
}
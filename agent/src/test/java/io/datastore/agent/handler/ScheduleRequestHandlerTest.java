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
import static io.datastore.common.HttpUtils.KEY_HEADER_NAME;
import static io.datastore.common.HttpUtils.SCHEDULE_URI;
import static io.datastore.common.HttpUtils.SET_URI;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class ScheduleRequestHandlerTest {
  public static final String MASTER = "127.0.0.1";
  public static final String KEY = "key1";

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

  private ScheduleRequestHandler scheduleRequestHandler;

  @Before
  public void setUp() {
    scheduleRequestHandler = new ScheduleRequestHandler(MASTER, KEY, vertx);
  }

  @Test
  public void shouldSendPostRequestWhenHandleIsCalled() {
    // given
    when(vertx.createHttpClient()).thenReturn(httpClient);
    when(httpClient.post(eq(MASTER_PORT), eq(MASTER), eq(SCHEDULE_URI))).thenReturn(httpClientRequest);

    // when
    scheduleRequestHandler.handle();

    // then
    verify(httpClientRequest).putHeader(eq(KEY_HEADER_NAME), eq(KEY));
    verify(httpClientRequest).end();

  }
}
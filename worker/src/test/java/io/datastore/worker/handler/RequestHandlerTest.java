package io.datastore.worker.handler;

import io.vertx.rxjava.core.http.HttpServerRequest;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import static io.datastore.common.HttpUtils.GET_URI;
import static io.datastore.common.HttpUtils.SCHEDULE_URI;
import static io.datastore.common.HttpUtils.SET_URI;
import static org.junit.Assert.*;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class RequestHandlerTest {

  @Mock
  private SetKeyHandler setKeyHandler;
  @Mock
  private GetKeyHandler getKeyHandler;
  @Mock
  private ScheduleKeyHandler scheduleKeyHandler;
  @Mock
  private HttpServerRequest httpServerRequest;

  private RequestHandler requestHandler;

  @Before
  public void setUp() {
    requestHandler = new RequestHandler(setKeyHandler, getKeyHandler, scheduleKeyHandler);
  }

  @Test
  public void shouldCallGetKeyHandlerWhenGetRequestIsPassed() {
    when(httpServerRequest.path()).thenReturn(GET_URI);

    requestHandler.handle(httpServerRequest);

    verify(getKeyHandler).handle(eq(httpServerRequest));
  }

  @Test
  public void shouldCallSetKeyHandlerWhenSetRequestIsPassed() {
    when(httpServerRequest.path()).thenReturn(SET_URI);

    requestHandler.handle(httpServerRequest);

    verify(setKeyHandler).handle(eq(httpServerRequest));
  }

  @Test
  public void shouldCallScheduleKeyHandlerWhenScheduleRequestIsPassed() {
    when(httpServerRequest.path()).thenReturn(SCHEDULE_URI);

    requestHandler.handle(httpServerRequest);

    verify(scheduleKeyHandler).handle(eq(httpServerRequest));
  }
}
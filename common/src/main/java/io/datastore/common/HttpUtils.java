package io.datastore.common;

import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.rxjava.core.http.HttpClientRequest;
import io.vertx.rxjava.core.http.HttpServerResponse;
import lombok.NoArgsConstructor;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.net.URLEncoder;

import static lombok.AccessLevel.PRIVATE;

@NoArgsConstructor(access = PRIVATE)
public final class HttpUtils {

  public static final String GET_URI = "/get";
  public static final String SET_URI = "/set";
  public static final String SCHEDULE_URI = "/schedule";

  public static final String STREAM_TYPE_HEADER_NAME = "Stream-Type";
  public static final String INTERMEDIATE_RESULT_HEADER_NAME = "Intermediate";
  public static final String OPERATION_HEADER_NAME = "Operation-Type";
  public static final String UNPROCESSED_PARAM_NAME = "Unfinished";

  public static final String KEY_HEADER_NAME = "Key";
  public static final String CONTENT_LENGTH_HEADER_NAME = "Content-Length";


  public static void finish(HttpServerResponse response, HttpResponseStatus status, String message) {
    setCode(response, status);
    response.putHeader(CONTENT_LENGTH_HEADER_NAME, "" + message.length());
    response.end(message);
    response.close();
  }

  public static HttpServerResponse setCode(HttpServerResponse response, HttpResponseStatus status) {
    return response.setStatusCode(status.code());
  }

  public static HttpServerResponse putHeaders(HttpServerResponse response, String key, long size) {
    return response.putHeader(KEY_HEADER_NAME, key)
        .putHeader(CONTENT_LENGTH_HEADER_NAME, "" + size);
  }

  public static HttpClientRequest putHeaders(HttpClientRequest request, String key, long size) {
    return putKeyHeader(request, key)
        .putHeader(CONTENT_LENGTH_HEADER_NAME, "" + size);
  }

  public static HttpClientRequest putKeyHeader(HttpClientRequest request, String key) {
    request.putHeader(KEY_HEADER_NAME, key);
    return request;
  }


  public static String encode(String text) {
    try {
      if (text != null) {
        return URLEncoder.encode(text, "UTF-8");
      }
    } catch (UnsupportedEncodingException ignored) {
    }
    return null;
  }

  public static String decode(String text) {
    try {
      if (text != null) {
        return URLDecoder.decode(text, "UTF-8");
      }
    } catch (UnsupportedEncodingException ignored) {
    }
    return null;
  }
}

/*
 * Copyright (c) Microsoft Corporation.
 * Licensed under the MIT License.
 */

package io.dapr.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.dapr.exceptions.DaprError;
import io.grpc.Context;
import org.jetbrains.annotations.NotNull;
import reactor.core.publisher.Mono;

import java.io.Closeable;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Map;

public abstract class DaprHttp implements Closeable {

  /**
   * Dapr API used in this client.
   */
  public static final String API_VERSION = "v1.0";

  /**
   * Header used for request id in Dapr.
   */
  protected static final String HEADER_DAPR_REQUEST_ID = "X-DaprRequestId";

  /**
   * Dapr's http default scheme.
   */
  protected static final String DEFAULT_HTTP_SCHEME = "http";

  /**
   * HTTP Methods supported.
   */
  public enum HttpMethod {
    NONE,
    GET,
    PUT,
    POST,
    DELETE,
    HEAD,
    CONNECT,
    OPTIONS,
    TRACE
  }

  public static class Response {
    private byte[] body;
    private Map<String, String> headers;
    private int statusCode;

    /**
     * Represents an http response.
     *
     * @param body       The body of the http response.
     * @param headers    The headers of the http response.
     * @param statusCode The status code of the http response.
     */
    public Response(byte[] body, Map<String, String> headers, int statusCode) {
      this.body = body == null ? EMPTY_BYTES : Arrays.copyOf(body, body.length);
      this.headers = headers;
      this.statusCode = statusCode;
    }

    public byte[] getBody() {
      return Arrays.copyOf(this.body, this.body.length);
    }

    public Map<String, String> getHeaders() {
      return headers;
    }

    public int getStatusCode() {
      return statusCode;
    }
  }

  /**
   * Empty input or output.
   */
  private static final byte[] EMPTY_BYTES = new byte[0];

  /**
   * JSON Object Mapper.
   */
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  /**
   * Invokes an API asynchronously without payload that returns a text payload.
   *
   * @param method        HTTP method.
   * @param urlString     url as String.
   * @param urlParameters URL parameters
   * @param headers       HTTP headers.
   * @param context       OpenTelemetry's Context.
   * @return Asynchronous text
   */
  public Mono<Response> invokeApi(
      String method,
      String urlString,
      Map<String, String> urlParameters,
      Map<String, String> headers,
      Context context) {
    return this.invokeApi(method, urlString, urlParameters, (byte[]) null, headers, context);
  }

  /**
   * Invokes an API asynchronously that returns a text payload.
   *
   * @param method        HTTP method.
   * @param urlString     url as String.
   * @param urlParameters Parameters in the URL
   * @param content       payload to be posted.
   * @param headers       HTTP headers.
   * @param context       OpenTelemetry's Context.
   * @return Asynchronous response
   */
  public Mono<Response> invokeApi(
      String method,
      String urlString,
      Map<String, String> urlParameters,
      String content,
      Map<String, String> headers,
      Context context) {

    return this.invokeApi(
        method, urlString, urlParameters, content == null
            ? EMPTY_BYTES
            : content.getBytes(StandardCharsets.UTF_8), headers, context);
  }

  /**
   * Invokes an API asynchronously that returns a text payload.
   *
   * @param method        HTTP method.
   * @param urlString     url as String.
   * @param urlParameters Parameters in the URL
   * @param content       payload to be posted.
   * @param headers       HTTP headers.
   * @param context       OpenTelemetry's Context.
   * @return Asynchronous response
   */
  public Mono<Response> invokeApi(
          String method,
          String urlString,
          Map<String, String> urlParameters,
          byte[] content,
          Map<String, String> headers,
          Context context) {
    return invokeApi(HttpMethod.valueOf(method), urlString, urlParameters, content, headers, context);
  }

  /**
   * Invokes an API that returns a text payload.
   *
   * @param method        HTTP method.
   * @param urlString     url as String.
   * @param urlParameters Parameters in the URL
   * @param content       payload to be posted.
   * @param headers       HTTP headers.
   * @param context       OpenTelemetry's Context.
   * @return Response
   */
  @NotNull
  protected abstract Mono<Response> invokeApi(HttpMethod method,
                                              String urlString,
                                              Map<String, String> urlParameters,
                                              byte[] content, Map<String, String> headers,
                                              Context context);

  /**
   * Tries to parse an error from Dapr response body.
   *
   * @param json Response body from Dapr.
   * @return DaprError or null if could not parse.
   */
  protected static DaprError parseDaprError(byte[] json) throws IOException {
    if ((json == null) || (json.length == 0)) {
      return null;
    }
    return OBJECT_MAPPER.readValue(json, DaprError.class);
  }

}
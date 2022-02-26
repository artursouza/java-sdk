/*
 * Copyright 2021 The Dapr Authors
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
limitations under the License.
*/

package io.dapr.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.dapr.client.domain.Metadata;
import io.dapr.config.Properties;
import io.dapr.exceptions.DaprError;
import io.dapr.exceptions.DaprException;
import okhttp3.HttpUrl;
import reactor.core.publisher.Mono;
import reactor.util.context.Context;

import java.io.IOException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

public class DaprHttp implements AutoCloseable {

  /**
   * Dapr API used in this client.
   */
  public static final String API_VERSION = "v1.0";

  /**
   * Header used for request id in Dapr.
   */
  private static final String HEADER_DAPR_REQUEST_ID = "X-DaprRequestId";

  /**
   * Header for Content-Type.
   */
  private static final String HEADER_CONTENT_TYPE = "Content-Type";

  /**
   * Dapr's http default scheme.
   */
  private static final String DEFAULT_HTTP_SCHEME = "http";

  /**
   * Context entries allowed to be in HTTP Headers.
   */
  private static final Set<String> ALLOWED_CONTEXT_IN_HEADERS =
      Collections.unmodifiableSet(new HashSet<>(Arrays.asList("grpc-trace-bin", "traceparent", "tracestate")));

  /**
   * HTTP Methods supported.
   */
  public enum HttpMethods {
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
   * Defines the standard application/json type for HTTP calls in Dapr.
   */
  private static final String CONTENT_TYPE_APPLICATION_JSON = "application/json; charset=utf-8";

  /**
   * Empty input or output.
   */
  private static final byte[] EMPTY_BYTES = new byte[0];

  /**
   * JSON Object Mapper.
   */
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  /**
   * Hostname used to communicate to Dapr's HTTP endpoint.
   */
  private final String hostname;

  /**
   * Port used to communicate to Dapr's HTTP endpoint.
   */
  private final int port;

  /**
   * Http client used for all API calls.
   */
  private final HttpClient httpClient;

  /**
   *  Timeout duration per HTTP request.
   */
  private final Duration timeout;

  /**
   * Creates a new instance of {@link DaprHttp}.
   *
   * @param hostname   Hostname for calling Dapr. (e.g. "127.0.0.1")
   * @param port       Port for calling Dapr. (e.g. 3500)
   * @param httpClient HTTPClient used for all API calls in this new instance.
   * @param timeout    Timeout duration per HTTP request.
   */
  DaprHttp(String hostname, int port, HttpClient httpClient, Duration timeout) {
    this.hostname = hostname;
    this.port = port;
    this.httpClient = httpClient;
    this.timeout = timeout;
  }

  /**
   * Invokes an API asynchronously without payload that returns a text payload.
   *
   * @param method        HTTP method.
   * @param pathSegments  Array of path segments ("/a/b/c" maps to ["a", "b", "c"]).
   * @param urlParameters URL parameters
   * @param headers       HTTP headers.
   * @param context       OpenTelemetry's Context.
   * @return Asynchronous text
   */
  public Mono<Response> invokeApi(
      String method,
      String[] pathSegments,
      Map<String, List<String>> urlParameters,
      Map<String, String> headers,
      Context context) {
    return this.invokeApi(method, pathSegments, urlParameters, (byte[]) null, headers, context);
  }

  /**
   * Invokes an API asynchronously that returns a text payload.
   *
   * @param method        HTTP method.
   * @param pathSegments  Array of path segments ("/a/b/c" maps to ["a", "b", "c"]).
   * @param urlParameters Parameters in the URL
   * @param content       payload to be posted.
   * @param headers       HTTP headers.
   * @param context       OpenTelemetry's Context.
   * @return Asynchronous response
   */
  public Mono<Response> invokeApi(
      String method,
      String[] pathSegments,
      Map<String, List<String>> urlParameters,
      String content,
      Map<String, String> headers,
      Context context) {

    return this.invokeApi(
        method, pathSegments, urlParameters, content == null
            ? EMPTY_BYTES
            : content.getBytes(StandardCharsets.UTF_8), headers, context);
  }

  /**
   * Invokes an API asynchronously that returns a text payload.
   *
   * @param method        HTTP method.
   * @param pathSegments  Array of path segments ("/a/b/c" maps to ["a", "b", "c"]).
   * @param urlParameters Parameters in the URL
   * @param content       payload to be posted.
   * @param headers       HTTP headers.
   * @param context       OpenTelemetry's Context.
   * @return Asynchronous response
   */
  public Mono<Response> invokeApi(
          String method,
          String[] pathSegments,
          Map<String, List<String>> urlParameters,
          byte[] content,
          Map<String, String> headers,
          Context context) {
    // fromCallable() is needed so the invocation does not happen early, causing a hot mono.
    return Mono.fromCallable(() -> doInvokeApi(method, pathSegments, urlParameters, content, headers, context))
        .flatMap(f -> Mono.fromFuture(f));
  }

  /**
   * Shutdown call is not necessary for HttpClient.
   * @see HttpClient
   */
  @Override
  public void close() {
    // No code needed
  }

  /**
   * Invokes an API that returns a text payload.
   *
   * @param method        HTTP method.
   * @param pathSegments  Array of path segments (/a/b/c -> ["a", "b", "c"]).
   * @param urlParameters Parameters in the URL
   * @param content       payload to be posted.
   * @param headers       HTTP headers.
   * @param context       OpenTelemetry's Context.
   * @return CompletableFuture for Response.
   */
  private CompletableFuture<Response> doInvokeApi(String method,
                               String[] pathSegments,
                               Map<String, List<String>> urlParameters,
                               byte[] content, Map<String, String> headers,
                               Context context) {
    final String requestId = UUID.randomUUID().toString();
    HttpRequest.BodyPublisher bodyPublisher;

    String contentType = headers != null ? headers.get(Metadata.CONTENT_TYPE) : null;
    if ((contentType == null) || contentType.isBlank()) {
      contentType = CONTENT_TYPE_APPLICATION_JSON;
    }
    if (content == null) {
      bodyPublisher = HttpRequest.BodyPublishers.noBody();
    } else {
      bodyPublisher = HttpRequest.BodyPublishers.ofByteArray(content);
    }
    HttpUrl.Builder urlBuilder = new HttpUrl.Builder();
    urlBuilder.scheme(DEFAULT_HTTP_SCHEME)
        .host(this.hostname)
        .port(this.port);
    for (String pathSegment : pathSegments) {
      urlBuilder.addPathSegment(pathSegment);
    }
    Optional.ofNullable(urlParameters).orElse(Collections.emptyMap()).entrySet().stream()
        .forEach(urlParameter ->
            Optional.ofNullable(urlParameter.getValue()).orElse(Collections.emptyList()).stream()
              .forEach(urlParameterValue ->
                  urlBuilder.addQueryParameter(urlParameter.getKey(), urlParameterValue)));

    HttpRequest.Builder requestBuilder = HttpRequest.newBuilder()
        .uri(urlBuilder.build().uri())
        .header(HEADER_DAPR_REQUEST_ID, requestId)
        .header(HEADER_CONTENT_TYPE, contentType);
    if (context != null) {
      context.stream()
          .filter(entry -> ALLOWED_CONTEXT_IN_HEADERS.contains(entry.getKey().toString().toLowerCase()))
          .forEach(entry -> requestBuilder.header(entry.getKey().toString(), entry.getValue().toString()));
    }
    if (HttpMethods.GET.name().equals(method)) {
      requestBuilder.GET();
    } else if (HttpMethods.DELETE.name().equals(method)) {
      requestBuilder.DELETE();
    } else {
      requestBuilder.method(method, bodyPublisher);
    }

    String daprApiToken = Properties.API_TOKEN.get();
    if (daprApiToken != null) {
      requestBuilder.header(Headers.DAPR_API_TOKEN, daprApiToken);
    }

    if (headers != null) {
      Optional.ofNullable(headers.entrySet()).orElse(Collections.emptySet()).stream()
          .forEach(header -> {
            requestBuilder.setHeader(header.getKey(), header.getValue());
          });
    }

    requestBuilder.timeout(this.timeout);
    HttpRequest request = requestBuilder.build();


    return this.httpClient.sendAsync(request, HttpResponse.BodyHandlers.ofByteArray())
        .thenApply(bytesHttpResponse -> {
          int statusCode = bytesHttpResponse.statusCode();
          if ((statusCode < 200) || (statusCode > 299)) {
            DaprError error = parseDaprError(getBodyBytesOrEmptyArray(bytesHttpResponse));
            if ((error != null) && (error.getErrorCode() != null)) {
              if (error.getMessage() != null) {
                throw new DaprException(error);
              } else {
                throw new DaprException(error.getErrorCode(), "HTTP status code: " + statusCode);
              }
            }

            throw new DaprException("UNKNOWN", "HTTP status code: " + statusCode);
          }

          Map<String, String> mapHeaders = new HashMap<>();
          byte[] result = getBodyBytesOrEmptyArray(bytesHttpResponse);
          bytesHttpResponse.headers().map().entrySet().forEach(pair -> {
            mapHeaders.put(pair.getKey(), String.join(",", pair.getValue()));
          });

          return new Response(result, mapHeaders, statusCode);
        });
  }

  /**
   * Tries to parse an error from Dapr response body.
   *
   * @param json Response body from Dapr.
   * @return DaprError or null if could not parse.
   */
  private static DaprError parseDaprError(byte[] json) {
    if ((json == null) || (json.length == 0)) {
      return null;
    }

    try {
      return OBJECT_MAPPER.readValue(json, DaprError.class);
    } catch (IOException e) {
      throw new DaprException("UNKNOWN", new String(json, StandardCharsets.UTF_8));
    }
  }

  private static byte[] getBodyBytesOrEmptyArray(HttpResponse<byte[]> response) {
    byte[] body = response.body();
    if (body != null) {
      return body;
    }

    return EMPTY_BYTES;
  }

}

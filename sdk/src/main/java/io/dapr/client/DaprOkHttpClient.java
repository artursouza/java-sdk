/*
 * Copyright (c) Microsoft Corporation.
 * Licensed under the MIT License.
 */

package io.dapr.client;

import io.dapr.config.Properties;
import io.dapr.exceptions.DaprError;
import io.dapr.exceptions.DaprException;
import io.grpc.Context;
import io.opentelemetry.OpenTelemetry;
import io.opentelemetry.context.propagation.HttpTextFormat;
import okhttp3.HttpUrl;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.ResponseBody;
import org.jetbrains.annotations.NotNull;
import reactor.core.publisher.Mono;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

public class DaprOkHttpClient extends DaprHttp {

  /**
   * Empty input or output.
   */
  private static final byte[] EMPTY_BYTES = new byte[0];

  /**
   * Sets the headers for OpenTelemetry SDK.
   */
  private static final HttpTextFormat.Setter<Request.Builder> OPENTELEMETRY_SETTER =
      new HttpTextFormat.Setter<Request.Builder>() {
        @Override
        public void set(Request.Builder requestBuilder, String key, String value) {
          requestBuilder.addHeader(key, value);
        }
      };

  /**
   * Defines the standard application/json type for HTTP calls in Dapr.
   */
  private static final MediaType MEDIA_TYPE_APPLICATION_JSON =
      MediaType.get("application/json; charset=utf-8");

  /**
   * Shared object representing an empty request body in JSON.
   */
  private static final RequestBody REQUEST_BODY_EMPTY_JSON =
      RequestBody.Companion.create("", MEDIA_TYPE_APPLICATION_JSON);

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
  private final OkHttpClient httpClient;

  /**
   * Creates a new instance of {@link DaprOkHttpClient}.
   *
   * @param hostname   Hostname for calling Dapr. (e.g. "127.0.0.1")
   * @param port       Port for calling Dapr. (e.g. 3500)
   * @param httpClient RestClient used for all API calls in this new instance.
   */
  DaprOkHttpClient(String hostname, int port, OkHttpClient httpClient) {
    this.hostname = hostname;
    this.port = port;
    this.httpClient = httpClient;
  }


  /**
   * Shutdown call is not necessary for OkHttpClient.
   * @see OkHttpClient
   */
  @Override
  public void close() throws IOException {
    // No code needed
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected Mono<Response> invokeApi(HttpMethod method,
                                     String urlString,
                                     Map<String, String> urlParameters,
                                     byte[] content,
                                     Map<String, String> headers,
                                     Context context) {
    return Mono.fromCallable(() -> this.doInvokeApi(method, urlString, urlParameters, content, headers, context));
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
  protected Response doInvokeApi(HttpMethod method,
                                 String urlString,
                                 Map<String, String> urlParameters,
                                 byte[] content, Map<String, String> headers,
                                 Context context) throws IOException {
    final String requestId = UUID.randomUUID().toString();
    RequestBody body;

    String contentType = headers != null ? headers.get("content-type") : null;
    MediaType mediaType = contentType == null ? MEDIA_TYPE_APPLICATION_JSON : MediaType.get(contentType);
    if (content == null) {
      body = mediaType.equals(MEDIA_TYPE_APPLICATION_JSON)
          ? REQUEST_BODY_EMPTY_JSON
          : RequestBody.Companion.create(new byte[0], mediaType);
    } else {
      body = RequestBody.Companion.create(content, mediaType);
    }
    HttpUrl.Builder urlBuilder = new HttpUrl.Builder();
    urlBuilder.scheme(DEFAULT_HTTP_SCHEME)
        .host(this.hostname)
        .port(this.port)
        .addPathSegments(urlString);
    Optional.ofNullable(urlParameters).orElse(Collections.emptyMap()).entrySet().stream()
        .forEach(urlParameter -> urlBuilder.addQueryParameter(urlParameter.getKey(), urlParameter.getValue()));

    Request.Builder requestBuilder = new Request.Builder()
        .url(urlBuilder.build())
        .addHeader(HEADER_DAPR_REQUEST_ID, requestId);
    if (context != null) {
      OpenTelemetry.getPropagators().getHttpTextFormat().inject(context, requestBuilder, OPENTELEMETRY_SETTER);
    }
    if (HttpMethod.GET.equals(method)) {
      requestBuilder.get();
    } else if (HttpMethod.DELETE.equals(method)) {
      requestBuilder.delete();
    } else {
      requestBuilder.method(method.toString(), body);
    }

    String daprApiToken = Properties.API_TOKEN.get();
    if (daprApiToken != null) {
      requestBuilder.addHeader(Headers.DAPR_API_TOKEN, daprApiToken);
    }

    if (headers != null) {
      Optional.ofNullable(headers.entrySet()).orElse(Collections.emptySet()).stream()
          .forEach(header -> {
            requestBuilder.addHeader(header.getKey(), header.getValue());
          });
    }

    Request request = requestBuilder.build();

    try (okhttp3.Response response = this.httpClient.newCall(request).execute()) {
      if (!response.isSuccessful()) {
        DaprError error = parseDaprError(getBodyBytesOrEmptyArray(response));
        if ((error != null) && (error.getErrorCode() != null) && (error.getMessage() != null)) {
          throw new DaprException(error);
        }

        throw new IllegalStateException("Unknown Dapr error. HTTP status code: " + response.code());
      }

      Map<String, String> mapHeaders = new HashMap<>();
      byte[] result = getBodyBytesOrEmptyArray(response);
      response.headers().forEach(pair -> {
        mapHeaders.put(pair.getFirst(), pair.getSecond());
      });
      return new Response(result, mapHeaders, response.code());
    }
  }

  private static byte[] getBodyBytesOrEmptyArray(okhttp3.Response response) throws IOException {
    ResponseBody body = response.body();
    if (body != null) {
      return body.bytes();
    }

    return EMPTY_BYTES;
  }

}
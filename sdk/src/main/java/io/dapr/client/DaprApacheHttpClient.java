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
import org.apache.http.HttpEntityEnclosingRequest;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpHead;
import org.apache.http.client.methods.HttpOptions;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpTrace;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.nio.client.HttpAsyncClient;
import org.apache.http.util.EntityUtils;
import org.jetbrains.annotations.NotNull;
import reactor.core.publisher.Mono;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class DaprApacheHttpClient extends DaprHttp {

  /**
   * Empty input or output.
   */
  private static final byte[] EMPTY_BYTES = new byte[0];

  /**
   * Sets the headers for OpenTelemetry SDK.
   */
  private static final HttpTextFormat.Setter<HttpUriRequest> OPENTELEMETRY_SETTER =
      (request, key, value) -> request.addHeader(key, value);

  /**
   * URI used to communicate to Dapr's HTTP endpoint.
   */
  private final String baseUri;

  /**
   * Http client used for all API calls.
   */
  private final HttpAsyncClient httpClient;

  /**
   * Creates a new instance of {@link DaprApacheHttpClient}.
   *
   * @param hostname   Hostname for calling Dapr. (e.g. "127.0.0.1")
   * @param port       Port for calling Dapr. (e.g. 3500)
   * @param httpClient RestClient used for all API calls in this new instance.
   */
  DaprApacheHttpClient(String hostname, int port, HttpAsyncClient httpClient) {
    this.baseUri = DEFAULT_HTTP_SCHEME + "://" + hostname + ":" + port;
    this.httpClient = httpClient;
  }


  /**
   * Shutdown call is not necessary for OkHttpClient.
   *
   * @see HttpClient
   */
  @Override
  public void close() {
    // No-op.
  }

  /**
   * {@inheritDoc}
   */
  protected Mono<Response> invokeApi(HttpMethod method,
                                     String urlString,
                                     Map<String, String> urlParameters,
                                     byte[] content, Map<String, String> headers,
                                     Context context) {
    return Mono.fromCallable(() -> {
      final String requestId = UUID.randomUUID().toString();

      final HttpUriRequest request = buildHttpUriRequest(method, urlString, urlParameters);

      String contentTypeString = headers != null ? headers.get("content-type") : null;
      ContentType contentType = contentTypeString == null ? ContentType.APPLICATION_JSON :
          ContentType.create(contentTypeString, Properties.STRING_CHARSET.get());
      if ((content != null) && (content.length > 0) && (request instanceof HttpEntityEnclosingRequest)) {
        ((HttpEntityEnclosingRequest) request).setEntity(new ByteArrayEntity(content, contentType));
      }

      request.addHeader(HEADER_DAPR_REQUEST_ID, requestId);
      if (context != null) {
        OpenTelemetry.getPropagators().getHttpTextFormat().inject(context, request, OPENTELEMETRY_SETTER);
      }

      String daprApiToken = Properties.API_TOKEN.get();
      if (daprApiToken != null) {
        request.addHeader(Headers.DAPR_API_TOKEN, daprApiToken);
      }

      if (headers != null) {
        Optional.ofNullable(headers.entrySet()).orElse(Collections.emptySet()).stream()
            .forEach(header -> {
              request.addHeader(header.getKey(), header.getValue());
            });
      }

      return this.httpClient.execute(request, null);
    }).map(future -> {
      try {
        return future.get();
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      } catch (ExecutionException e) {
        throw new RuntimeException(e);
      }
    }).map(response -> {
      try {
        int statusCode = response.getStatusLine().getStatusCode();
        byte[] responseBody = EntityUtils.toByteArray(response.getEntity());
        if ((statusCode < 200) || (statusCode > 299)) {
          DaprError error = parseDaprError(responseBody);
          if ((error != null) && (error.getErrorCode() != null) && (error.getMessage() != null)) {
            throw new DaprException(error);
          }

          throw new IllegalStateException("Unknown Dapr error. HTTP status code: " + statusCode);
        }

        Map<String, String> mapHeaders = new HashMap<>();
        Arrays.stream(response.getAllHeaders()).forEach(pair -> {
          mapHeaders.put(pair.getName(), pair.getValue());
        });
        return new Response(responseBody, mapHeaders, statusCode);
      } catch (RuntimeException e) {
        throw e;
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    });
  }

  @NotNull
  private HttpUriRequest buildHttpUriRequest(
      HttpMethod method,
      String urlString,
      Map<String, String> urlParameters) throws IOException {
    final URI uri = buildUri(urlString, urlParameters);

    HttpUriRequest request = null;
    switch (method) {
      case GET:
        request = new HttpGet(uri);
        break;
      case PUT:
        request = new HttpPut(uri);
        break;
      case POST:
        request = new HttpPost(uri);
        break;
      case DELETE:
        request = new HttpDelete(uri);
        break;
      case TRACE:
        request = new HttpTrace(uri);
        break;
      case HEAD:
        request = new HttpHead(uri);
        break;
      case OPTIONS:
        request = new HttpOptions(uri);
        break;
      default:
        throw new IllegalArgumentException("Http method is not supported: " + method);
    }
    return request;
  }

  private URI buildUri(String urlString, Map<String, String> urlParameters) throws IOException {
    try {
      URIBuilder builder = new URIBuilder(this.baseUri);
      builder.setPath(urlString);
      Optional.ofNullable(urlParameters).orElse(Collections.emptyMap()).entrySet().stream()
          .forEach(urlParameter -> builder.addParameter(urlParameter.getKey(), urlParameter.getValue()));
      return builder.build();
    } catch (URISyntaxException e) {
      throw new IOException(e);
    }
  }

}
/*
 * Copyright (c) Microsoft Corporation.
 * Licensed under the MIT License.
 */

package io.dapr.client;

import io.dapr.config.Properties;
import okhttp3.OkHttpClient;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.http.nio.client.HttpAsyncClient;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A builder for the DaprHttp.
 */
public class DaprHttpBuilder {

  /**
   * Singleton OkHttpClient.
   */
  private static final AtomicReference<OkHttpClient> OK_HTTP_CLIENT = new AtomicReference<>();

  /**
   * Singleton OkHttpClient.
   */
  private static final AtomicReference<HttpAsyncClient> APACHE_HTTP_CLIENT = new AtomicReference<>();

  /**
   * Static lock object.
   */
  private static final Object LOCK = new Object();

  /**
   * Determines which HTTP client to use.
   */
  private final boolean async = "async".equalsIgnoreCase(Properties.HTTP_CLIENT.get());

  /**
   * Read timeout used to build object.
   */
  private Duration readTimeout = Duration.ofSeconds(Properties.HTTP_CLIENT_READTIMEOUTSECONDS.get());

  /**
   * Sets the read timeout duration for the instance to be built.
   *
   * <p>Instead, set environment variable "DAPR_HTTP_CLIENT_READTIMEOUTSECONDS",
   *   or system property "dapr.http.client.readtimeoutseconds".
   *
   * @param duration Read timeout duration.
   * @return Same builder instance.
   */
  @Deprecated
  public DaprHttpBuilder withReadTimeout(Duration duration) {
    this.readTimeout = duration;
    return this;
  }

  /**
   * Build an instance of the Http client based on the provided setup.
   *
   * @return an instance of {@link DaprHttp}
   * @throws IllegalStateException if any required field is missing
   */
  public DaprHttp build() {
    if (this.async) {
      return buildApacheHttp();
    }

    return buildOkHttp();
  }

  /**
   * Creates and instance of the HTTP Client.
   *
   * @return Instance of {@link DaprOkHttpClient}
   */
  private DaprOkHttpClient buildOkHttp() {
    if (OK_HTTP_CLIENT.get() == null) {
      synchronized (LOCK) {
        if (OK_HTTP_CLIENT.get() == null) {
          OkHttpClient.Builder builder = new OkHttpClient.Builder();
          builder.readTimeout(this.readTimeout);
          OkHttpClient okHttpClient = builder.build();
          OK_HTTP_CLIENT.set(okHttpClient);
        }
      }
    }

    return new DaprOkHttpClient(Properties.SIDECAR_IP.get(), Properties.HTTP_PORT.get(), OK_HTTP_CLIENT.get());
  }

  /**
   * Creates and instance of the HTTP Client.
   *
   * @return Instance of {@link DaprApacheHttpClient}
   */
  private DaprApacheHttpClient buildApacheHttp() {
    if (APACHE_HTTP_CLIENT.get() == null) {
      synchronized (LOCK) {
        if (APACHE_HTTP_CLIENT.get() == null) {
          RequestConfig config = RequestConfig.custom()
              .setSocketTimeout((int)this.readTimeout.toMillis()).build();
          CloseableHttpAsyncClient client = HttpAsyncClientBuilder.create()
              .setDefaultRequestConfig(config).build();
          APACHE_HTTP_CLIENT.set(client);
        }
      }
    }

    return new DaprApacheHttpClient(Properties.SIDECAR_IP.get(), Properties.HTTP_PORT.get(), APACHE_HTTP_CLIENT.get());
  }
}

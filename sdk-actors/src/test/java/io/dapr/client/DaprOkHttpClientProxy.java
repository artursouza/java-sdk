/*
 * Copyright (c) Microsoft Corporation.
 * Licensed under the MIT License.
 */

package io.dapr.client;

import okhttp3.OkHttpClient;

public class DaprOkHttpClientProxy extends DaprOkHttpClient {

  public DaprOkHttpClientProxy(String hostname, int port, OkHttpClient httpClient) {
    super(hostname, port, httpClient);
  }

}

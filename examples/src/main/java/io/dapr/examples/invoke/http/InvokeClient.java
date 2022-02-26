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

package io.dapr.examples.invoke.http;

import io.dapr.client.DaprClient;
import io.dapr.client.DaprClientBuilder;
import io.dapr.client.domain.HttpExtension;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Instant;
import java.util.concurrent.Executors;

/**
 * 1. Build and install jars:
 * mvn clean install
 * 2. cd [repo root]/examples
 * 3. Send messages to the server:
 * dapr run -- java -jar target/dapr-java-sdk-examples-exec.jar \
 *   io.dapr.examples.invoke.http.InvokeClient 'message one' 'message two'
 */
public class InvokeClient {

  /**
   * Identifier in Dapr for the service this client will invoke.
   */
  private static final String SERVICE_APP_ID = "invokedemo";

  /**
   * Starts the invoke client.
   *
   * @param args Messages to be sent as request for the invoke API.
   */
  public static void main(String[] args) throws Exception {
    var executor = Executors.newFixedThreadPool(50);
    try (DaprClient client = (new DaprClientBuilder()).build()) {
      HttpClient httpClient = HttpClient.newHttpClient();
      for (var i = 0; i < 50; i++) {
        final String msg = "" + i;
        final Integer key = i;
        executor.execute(() -> {
          var timestamp = Instant.now().toEpochMilli();

          // use invokeMethod
          if (args.length > 0) {
            System.out.println(String.format("%d sdk client invoke started", timestamp));
            client.invokeMethod(SERVICE_APP_ID, "say", "" + msg, HttpExtension.POST, null,
                byte[].class).block();
            timestamp = Instant.now().toEpochMilli();
            System.out.println(String.format("%d sdk client invoke done", timestamp));
          } else {
            System.out.println(String.format("%d http client request started", timestamp));
            HttpRequest request = HttpRequest.newBuilder()
                .POST(HttpRequest.BodyPublishers.ofString("message"))
                .uri(URI.create("http://localhost:3000/say"))
                .build();
            httpClient.sendAsync(request, HttpResponse.BodyHandlers.ofString())
                .thenApply(HttpResponse::body)
                .thenAccept(System.out::println)
                .join();
            timestamp = Instant.now().toEpochMilli();
            System.out.println(String.format("%d http client request done", timestamp));
          }
        });

      }
    }
  }
}

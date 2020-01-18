/*
 * Copyright (c) Microsoft Corporation.
 * Licensed under the MIT License.
 */

package io.dapr.examples.pubsub.http;

import io.dapr.serializer.DefaultObjectSerializer;
import io.dapr.client.domain.CloudEvent;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Mono;

import java.util.Map;

/**
 * SpringBoot Controller to handle input binding.
 */
@RestController
public class SubscriberController {

  /**
   * Dapr's default serializer/deserializer.
   */
  private static final DefaultObjectSerializer SERIALIZER = new DefaultObjectSerializer();

  @GetMapping("/dapr/subscribe")
  public byte[] daprConfig() throws Exception {
    return SERIALIZER.serialize(new String[] { "message" });
  }

  @PostMapping(path = "/message")
  public Mono<Void> handleMessage(@RequestBody(required = false) byte[] body,
                                   @RequestHeader Map<String, String> headers) {
    return Mono.fromRunnable(() -> {
      try {
        // Dapr's event is compliant to CloudEvent.
        CloudEvent event = CloudEvent.deserialize(body);

        String message = event.getData() == null ? "" : new String(event.getData());
        System.out.println("Subscriber got message: " + message);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    });
  }

}

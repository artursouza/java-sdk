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

package io.dapr.it.pubsub.grpc;

import com.fasterxml.jackson.annotation.JsonMerge;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.Striped;
import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.util.JsonFormat;
import io.dapr.client.domain.CloudEvent;
import io.dapr.grpc.GrpcHealthCheckService;
import io.dapr.utils.TypeRef;
import io.dapr.v1.AppCallbackGrpc;
import io.dapr.v1.CommonProtos;
import io.dapr.v1.DaprAppCallbackProtos;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;

import static io.dapr.it.MethodInvokeServiceProtos.DeleteMessageRequest;
import static io.dapr.it.MethodInvokeServiceProtos.DeleteMessageResponse;
import static io.dapr.it.MethodInvokeServiceProtos.GetMessagesRequest;
import static io.dapr.it.MethodInvokeServiceProtos.GetMessagesResponse;
import static io.dapr.it.MethodInvokeServiceProtos.PostMessageRequest;
import static io.dapr.it.MethodInvokeServiceProtos.PostMessageResponse;
import static io.dapr.it.MethodInvokeServiceProtos.SleepRequest;
import static io.dapr.it.MethodInvokeServiceProtos.SleepResponse;

public class SubscriberService {

  public static final String SUCCESS_MESSAGE = "application discovered on port ";

  /**
   * Server mode: class that encapsulates all server-side logic for Grpc.
   */
  private static class MyDaprService extends AppCallbackGrpc.AppCallbackImplBase {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private static final String SUBSCRIPTIONS = "" +
        "{" +
        "  \"Subscriptions\": [" +
        "    {" +
        "      \"PubsubName\": \"messagebus\"" +
        "      \"Topic\": \"testingtopic\"" +
        "      \"Routes\": \"{" +
        "        \"Default\": \"/testingtopic\"" +
        "        \"Rules\": [" +
        "          { \"Match\": \"event.type == 'myevent.v2'\", \"Path\": \"/testingtopicV2\"}," +
        "          { \"Match\": \"event.type == 'myevent.v3'\", \"Path\": \"/testingtopicV3\"}" +
        "        ]" +
        "      }\"" +
        "    }," +
        "    {" +
        "      \"PubsubName\": \"messagebus\"" +
        "      \"Topic\": \"typedtestingtopic\"" +
        "      \"Routes\": \"{" +
        "        \"Default\": \"/route1b\"" +
        "        \"Rules\": []" +
        "      }\"" +
        "    }," +
        "    {" +
        "      \"PubsubName\": \"messagebus\"" +
        "      \"Topic\": \"binarytopic\"" +
        "      \"Routes\": \"{" +
        "        \"Default\": \"/route2\"" +
        "        \"Rules\": []" +
        "      }\"" +
        "    }," +
        "    {" +
        "      \"PubsubName\": \"messagebus\"" +
        "      \"Topic\": \"anothertopic\"" +
        "      \"Routes\": \"{" +
        "        \"Default\": \"/route3\"" +
        "        \"Rules\": []" +
        "      }\"" +
        "    }," +
        "    {" +
        "      \"PubsubName\": \"messagebus\"" +
        "      \"Topic\": \"ttltopic\"" +
        "      \"Routes\": \"{" +
        "        \"Default\": \"/route4\"" +
        "        \"Rules\": []" +
        "      }\"" +
        "    }," +
        "    {" +
        "      \"PubsubName\": \"messagebus\"" +
        "      \"Topic\": \"testinglongvalues\"" +
        "      \"Routes\": \"{" +
        "        \"Default\": \"/testinglongvalues\"" +
        "        \"Rules\": []" +
        "      }\"" +
        "    }" +
        "]" +
        "}";

    private final Map<String, List<DaprAppCallbackProtos.TopicEventRequest>> messages =
        Collections.synchronizedMap(new HashMap<>());

    private final Striped<Lock> lockByTopic = Striped.lock(13 /* a prime number */);

    /**
     * Server mode: Grpc server.
     */
    private Server server;

    /**
     * Server mode: starts listening on given port.
     *
     * @param port Port to listen on.
     * @throws IOException Errors while trying to start service.
     */
    private void start(int port) throws IOException {
      this.server = ServerBuilder
          .forPort(port)
          .addService(this)
          .addService(new GrpcHealthCheckService())
          .build()
          .start();
      System.out.printf("Server: started listening on port %d\n", port);

      // Now we handle ctrl+c (or any other JVM shutdown)
      Runtime.getRuntime().addShutdownHook(new Thread(() -> {
        System.out.println("Server: shutting down gracefully ...");
        MyDaprService.this.server.shutdown();
        System.out.println("Server: Bye.");
      }));
    }

    /**
     * Server mode: waits for shutdown trigger.
     *
     * @throws InterruptedException Propagated interrupted exception.
     */
    private void awaitTermination() throws InterruptedException {
      if (this.server != null) {
        this.server.awaitTermination();
      }
    }

    /**
     * Server mode: this is the Dapr method to receive Invoke operations via Grpc.
     *
     * @param request          Dapr envelope request,
     * @param responseObserver Dapr envelope response.
     */
    @Override
    public void onInvoke(CommonProtos.InvokeRequest request,
                         StreamObserver<CommonProtos.InvokeResponse> responseObserver) {
      try {
        if (request.getMethod().startsWith("/messages/")) {
          String topic = request.getMethod().substring("/messages/".length());
          List<DaprAppCallbackProtos.TopicEventRequest> list = messages.getOrDefault(topic, Collections.emptyList());
          // TODO(artursouza): find a way to transfer the list of cloud events natively with gRPC without using JSON.
          List<JsonNode> listOfJsonNodes = new ArrayList<>();
          for (var req : list) {
            listOfJsonNodes.add(OBJECT_MAPPER.readTree(JsonFormat.printer().print(req)));
          }
          ByteString data = ByteString.copyFrom(OBJECT_MAPPER.writeValueAsBytes(list));
          CommonProtos.InvokeResponse.Builder responseBuilder = CommonProtos.InvokeResponse.newBuilder();
          responseBuilder.setData(Any.newBuilder().setValue(data));
          responseObserver.onNext(responseBuilder.build());
        }
      } catch (Exception e) {
        responseObserver.onError(e);
      } finally {
        responseObserver.onCompleted();
      }
    }

    public void listTopicSubscriptions(
        com.google.protobuf.Empty request,
        io.grpc.stub.StreamObserver<io.dapr.v1.DaprAppCallbackProtos.ListTopicSubscriptionsResponse> responseObserver) {
      DaprAppCallbackProtos.ListTopicSubscriptionsResponse.Builder responseBuilder =
          DaprAppCallbackProtos.ListTopicSubscriptionsResponse.newBuilder();
      try {
        JsonFormat.parser().merge(SUBSCRIPTIONS, responseBuilder);
        responseObserver.onNext(responseBuilder.build());
      } catch (InvalidProtocolBufferException e) {
        responseObserver.onError(e);
      } finally {
        responseObserver.onCompleted();
      }
    }

    /**
     * <pre>
     * Subscribes events from Pubsub
     * </pre>
     */
    public void onTopicEvent(io.dapr.v1.DaprAppCallbackProtos.TopicEventRequest request,
                             io.grpc.stub.StreamObserver<io.dapr.v1.DaprAppCallbackProtos.TopicEventResponse> responseObserver) {
      String topic = request.getTopic();
      String route = request.getPath();
      String message = request.getData().toString();
      String contentType = request.getDataContentType();
      System.out.println(
          "Topic " + topic + " at route " + route + " got message: " + message + "; Content-type: " + contentType);
      Lock lock = lockByTopic.get(topic);
      lock.lock();
      try {
        if (route.startsWith("/testingtopic")) {
          addMessageByTopic(route.substring(1), request);
        } else {
          addMessageByTopic(topic, request);
        }
      } catch (Exception e) {
        responseObserver.onError(e);
      } finally {
        lock.unlock();
        responseObserver.onCompleted();
      }
    }

    private void addMessageByTopic(String topic, DaprAppCallbackProtos.TopicEventRequest cloudEvent) {
      List<DaprAppCallbackProtos.TopicEventRequest> list = messages.getOrDefault(topic, new ArrayList<>());
      list.add(cloudEvent);
    }
  }

  /**
   * This is the main method of this app.
   * @param args The port to listen on.
   * @throws Exception An Exception.
   */
  public static void main(String[] args) throws Exception {
    int port = Integer.parseInt(args[0]);

    System.out.printf("Service starting on port %d ...\n", port);

    final MyDaprService service = new MyDaprService();
    service.start(port);
    service.awaitTermination();
  }
}

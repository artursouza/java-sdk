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

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import io.dapr.client.domain.HttpExtension;
import io.dapr.serialization.DaprObjectSerializer;
import io.dapr.serialization.DefaultObjectSerializer;
import io.dapr.utils.TypeRef;
import org.junit.Assert;
import org.junit.Test;
import reactor.core.publisher.Mono;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DaprClientHttpWithCustomDeprecatedSerializerTest {

  protected static final JsonFactory JSON_FACTORY = new JsonFactory();

  private static final String TYPE_PLAIN_TEXT = "plain/text";

  private static final String PUBSUB_NAME = "mypubsubname";

  private static final String TOPIC_NAME = "mytopic";

  private static final String BINDING_NAME = "mybinding";

  private static final String STATE_STORE_NAME = "mystatestore";

  private static final String APP_ID = "myappid";

  private static final String METHOD_NAME = "mymethod";

  private static final String INVOKE_PATH = DaprHttp.API_VERSION + "/invoke";

  private static final String PUBLISH_PATH = DaprHttp.API_VERSION + "/publish";

  private static final String BINDING_PATH = DaprHttp.API_VERSION + "/bindings";

  private static final String STATE_PATH = DaprHttp.API_VERSION + "/state";

  /**
   * Useful to certify that the legacy behavior reported in dapr/java-sdk#503 is kept for backwards compatibility.
   */
  private static final io.dapr.serializer.DaprObjectSerializer CUSTOM_SERIALIZER =
      new io.dapr.serializer.DaprObjectSerializer() {

        @Override
        public byte[] serialize(Object o) throws IOException {
          return new DefaultObjectSerializer().serialize(o);
        }

        @Override
        public <T> T deserialize(byte[] data, TypeRef<T> type) throws IOException {
          return new DefaultObjectSerializer().deserialize(data, type);
        }

        @Override
        public String getContentType() {
          return "application/json";
        }
      };

  /**
   * Useful to certify that the legacy behavior reported in dapr/java-sdk#503 is kept for backwards compatibility.
   */
  private static final io.dapr.serializer.DaprObjectSerializer CUSTOM_SERIALIZER_XML =
      new io.dapr.serializer.DaprObjectSerializer() {
        private final XmlMapper XML_MAPPER = new XmlMapper();

        @Override
        public byte[] serialize(Object o) throws IOException {
          return XML_MAPPER.writeValueAsBytes(o);
        }

        @Override
        public <T> T deserialize(byte[] data, TypeRef<T> type) throws IOException {
          if (type.getType() == byte[].class) {
            return (T) data;
          }

          return XML_MAPPER.readValue(data, new TypeReference<T>() {
          });
        }

        @Override
        public String getContentType() {
          return "application/xml";
        }
      };

  /**
   * Useful to certify that the legacy behavior reported in dapr/java-sdk#503 is kept for backwards compatibility.
   */
  private static final io.dapr.serializer.DaprObjectSerializer CUSTOM_SERIALIZER_STRING_BINARY =
      new io.dapr.serializer.DaprObjectSerializer() {

        @Override
        public byte[] serialize(Object o) {
          return o.toString().getBytes(StandardCharsets.UTF_8);
        }

        @Override
        public <T> T deserialize(byte[] data, TypeRef<T> type) {
          return (T) new String(data);
        }

        @Override
        public String getContentType() {
          return "application/octect-stream";
        }
      };

  @Test
  public void pubsubWithCustomSerializer() throws Exception {
    Message[] messages = new Message[]{
        new Message(
            generateMessageId(),
            TYPE_PLAIN_TEXT,
            generatePayload(),
            generateSingleMetadata()),
        new Message(
            generateMessageId(),
            TYPE_PLAIN_TEXT,
            "",
            generateSingleMetadata()),
        new Message(
            generateMessageId(),
            TYPE_PLAIN_TEXT,
            null,
            generateSingleMetadata()),
        new Message(
            generateMessageId(),
            TYPE_PLAIN_TEXT,
            generatePayload(),
            null),
        new Message(
            "",
            TYPE_PLAIN_TEXT,
            generatePayload(),
            generateSingleMetadata()),
        new Message(
            null,
            TYPE_PLAIN_TEXT,
            generatePayload(),
            generateSingleMetadata()),
        new Message(
            generateMessageId(),
            "",
            generatePayload(),
            generateSingleMetadata()),
        new Message(
            generateMessageId(),
            null,
            generatePayload(),
            generateSingleMetadata())
    };

    DaprHttpStub daprHttp = mock(DaprHttpStub.class);
    DaprClient client = DaprClientTestBuilder.buildHttpClient(daprHttp, CUSTOM_SERIALIZER);
    DaprObjectSerializer serializer = new DefaultObjectSerializer();

    for (Message message : messages) {
      when(daprHttp.invokeApi(
          eq("POST"),
          eq((PUBLISH_PATH + "/" + PUBSUB_NAME + "/" + TOPIC_NAME).split("/")),
          any(),
          eq(serializer.serialize(message.data)),
          any(),
          any()))
          .thenAnswer(x -> Mono.just(new DaprHttpStub.ResponseStub(new byte[0], null, 200)));

      client.publishEvent(PUBSUB_NAME, TOPIC_NAME, message.data).block();
    }
  }

  @Test
  public void invokeServiceWithCustomSerializer() throws Exception {
    Message[] messages = new Message[]{
        new Message(
            generateMessageId(),
            TYPE_PLAIN_TEXT,
            generatePayload(),
            generateSingleMetadata()),
        new Message(
            generateMessageId(),
            TYPE_PLAIN_TEXT,
            "",
            generateSingleMetadata()),
        new Message(
            generateMessageId(),
            TYPE_PLAIN_TEXT,
            null,
            generateSingleMetadata()),
        new Message(
            generateMessageId(),
            TYPE_PLAIN_TEXT,
            generatePayload(),
            null),
        new Message(
            "",
            TYPE_PLAIN_TEXT,
            generatePayload(),
            generateSingleMetadata()),
        new Message(
            null,
            TYPE_PLAIN_TEXT,
            generatePayload(),
            generateSingleMetadata()),
        new Message(
            generateMessageId(),
            "",
            generatePayload(),
            generateSingleMetadata()),
        new Message(
            generateMessageId(),
            null,
            generatePayload(),
            generateSingleMetadata())
    };

    DaprHttpStub daprHttp = mock(DaprHttpStub.class);
    DaprClient client = DaprClientTestBuilder.buildHttpClient(daprHttp, CUSTOM_SERIALIZER);

    DaprObjectSerializer serializer = new DefaultObjectSerializer();
    for (Message message : messages) {
      byte[] expectedResponse = message.id == null ? new byte[0] : serializer.serialize(message.id);

      when(daprHttp.invokeApi(
          eq("POST"),
          eq((INVOKE_PATH + "/" + APP_ID + "/method/" + METHOD_NAME).split("/")),
          any(),
          eq(serializer.serialize(message.data)),
          any(),
          any()))
          .thenAnswer(x -> Mono.just(new DaprHttpStub.ResponseStub(expectedResponse, null, 200)));
      Mono<byte[]> response = client.invokeMethod(APP_ID, METHOD_NAME, message.data, HttpExtension.POST,
          message.metadata, byte[].class);
      Assert.assertArrayEquals(expectedResponse, response.block());
    }
  }

  @Test
  public void invokeBindingWithCustomJSONSerializer() throws Exception {
    String data = "Hello World!";
    Map<String, String> expectedBindingRequest = new HashMap<>();
    expectedBindingRequest.put("operation", "POST");
    // The behavior below represents the bug described in dapr/java-sdk#503
    expectedBindingRequest.put("data", "IkhlbGxvIFdvcmxkISI=");  // => "Hello World!" base64 encoded

    DaprHttpStub daprHttp = mock(DaprHttpStub.class);
    DaprClient client = DaprClientTestBuilder.buildHttpClient(daprHttp, CUSTOM_SERIALIZER);

    DaprObjectSerializer serializer = new DefaultObjectSerializer();

    when(daprHttp.invokeApi(
        eq("POST"),
        eq((BINDING_PATH + "/" + BINDING_NAME).split("/")),
        any(),
        eq(serializer.serialize(expectedBindingRequest)),
        any(),
        any()))
        .thenAnswer(r -> Mono.just(new DaprHttpStub.ResponseStub(new byte[0], null, 200)));

    // Now invoke binding
    Mono<byte[]> response = client.invokeBinding(BINDING_NAME, "POST", data, byte[].class);
    Assert.assertArrayEquals(new byte[0], response.block());
  }

  @Test
  public void invokeStateWithCustomJSONSerializer() throws Exception {
    String data = "Hello World!";

    Map<String, String> expectedSaveStateRequest = new HashMap<>();
    expectedSaveStateRequest.put("key", "mykey");
    // The behavior below represents the bug described in dapr/java-sdk#503
    expectedSaveStateRequest.put("value", "IkhlbGxvIFdvcmxkISI=");  // => "Hello World!" base64 encoded

    DaprHttpStub daprHttp = mock(DaprHttpStub.class);
    DaprClient client = DaprClientTestBuilder.buildHttpClient(daprHttp, CUSTOM_SERIALIZER);

    DaprObjectSerializer serializer = new DefaultObjectSerializer();

    when(daprHttp.invokeApi(
        eq("POST"),
        eq((STATE_PATH + "/" + STATE_STORE_NAME).split("/")),
        any(),
        eq(serializer.serialize(Arrays.asList(expectedSaveStateRequest))),
        any(),
        any()))
        .thenAnswer(r -> Mono.just(new DaprHttpStub.ResponseStub("OK".getBytes(), null, 200)));

    // Now invoke binding
    Mono<Void> response = client.saveState(STATE_STORE_NAME, "mykey", data);
    response.block();
  }

  @Test
  public void invokeBindingWithCustomXMLSerializer() throws Exception {
    String data = "Hello World!";

    Map<String, String> expectedBindingRequest = new HashMap<>();
    expectedBindingRequest.put("operation", "POST");
    // The base64 value below represents the bug described in dapr/java-sdk#503
    expectedBindingRequest.put("data", "PFN0cmluZz5IZWxsbyBXb3JsZCE8L1N0cmluZz4=");

    DaprHttpStub daprHttp = mock(DaprHttpStub.class);
    DaprClient client = DaprClientTestBuilder.buildHttpClient(daprHttp, CUSTOM_SERIALIZER_XML);

    DaprObjectSerializer serializer = new DefaultObjectSerializer();

    when(daprHttp.invokeApi(
        eq("POST"),
        eq((BINDING_PATH + "/" + BINDING_NAME).split("/")),
        any(),
        eq(serializer.serialize(expectedBindingRequest)),
        any(),
        any()))
        .thenAnswer(r -> Mono.just(new DaprHttpStub.ResponseStub("</response>".getBytes(), null, 200)));

    // Now invoke binding
    Mono<byte[]> response = client.invokeBinding(BINDING_NAME, "POST", data, byte[].class);
    Assert.assertArrayEquals("</response>".getBytes(), response.block());
  }

  @Test
  public void invokeStateWithCustomXMLSerializer() throws Exception {
    String data = "Hello World!";

    Map<String, String> expectedSaveStateRequest = new HashMap<>();
    expectedSaveStateRequest.put("key", "mykey");
    // The base64 value below represents the bug described in dapr/java-sdk#503
    expectedSaveStateRequest.put("value", "PFN0cmluZz5IZWxsbyBXb3JsZCE8L1N0cmluZz4=");

    DaprHttpStub daprHttp = mock(DaprHttpStub.class);
    DaprClient client = DaprClientTestBuilder.buildHttpClient(daprHttp, CUSTOM_SERIALIZER_XML);

    DaprObjectSerializer serializer = new DefaultObjectSerializer();

    when(daprHttp.invokeApi(
        eq("POST"),
        eq((STATE_PATH + "/" + STATE_STORE_NAME).split("/")),
        any(),
        eq(serializer.serialize(Arrays.asList(expectedSaveStateRequest))),
        any(),
        any()))
        .thenAnswer(r -> Mono.just(new DaprHttpStub.ResponseStub("OK".getBytes(), null, 200)));

    // Now invoke binding
    Mono<Void> response = client.saveState(STATE_STORE_NAME, "mykey", data);
    response.block();
  }

  @Test
  public void invokeBindingWithCustomBytesSerializer() throws Exception {
    String data = "Hello World!";

    Map<String, String> expectedBindingRequest = new HashMap<>();
    expectedBindingRequest.put("operation", "POST");
    expectedBindingRequest.put("data", "SGVsbG8gV29ybGQh");  // => "Hello World!" base64 encoded.

    DaprHttpStub daprHttp = mock(DaprHttpStub.class);
    DaprClient client = DaprClientTestBuilder.buildHttpClient(daprHttp, CUSTOM_SERIALIZER_STRING_BINARY);

    DaprObjectSerializer serializer = new DefaultObjectSerializer();

    when(daprHttp.invokeApi(
        eq("POST"),
        eq((BINDING_PATH + "/" + BINDING_NAME).split("/")),
        any(),
        eq(serializer.serialize(expectedBindingRequest)),
        any(),
        any()))
        .thenAnswer(r -> Mono.just(new DaprHttpStub.ResponseStub("OK".getBytes(), null, 200)));

    // Now invoke binding
    Mono<String> response = client.invokeBinding(BINDING_NAME, "POST", data, String.class);
    Assert.assertEquals("OK", response.block());
  }

  @Test
  public void invokeStateWithCustomBytesSerializer() throws Exception {
    String data = "Hello World!";

    Map<String, String> expectedSaveStateRequest = new HashMap<>();
    expectedSaveStateRequest.put("key", "mykey");
    expectedSaveStateRequest.put("value", "SGVsbG8gV29ybGQh");  // => "Hello World!" base64 encoded.

    DaprHttpStub daprHttp = mock(DaprHttpStub.class);
    DaprClient client = DaprClientTestBuilder.buildHttpClient(daprHttp, CUSTOM_SERIALIZER_STRING_BINARY);

    DaprObjectSerializer serializer = new DefaultObjectSerializer();

    when(daprHttp.invokeApi(
        eq("POST"),
        eq((STATE_PATH + "/" + STATE_STORE_NAME).split("/")),
        any(),
        eq(serializer.serialize(Arrays.asList(expectedSaveStateRequest))),
        any(),
        any()))
        .thenAnswer(r -> Mono.just(new DaprHttpStub.ResponseStub("OK".getBytes(), null, 200)));

    // Now invoke binding
    Mono<Void> response = client.saveState(STATE_STORE_NAME, "mykey", data);
    response.block();
  }

  private static String generateMessageId() {
    return UUID.randomUUID().toString();
  }

  private static String generatePayload() {
    return UUID.randomUUID().toString();
  }

  private static Map<String, String> generateSingleMetadata() {
    return Collections.singletonMap(UUID.randomUUID().toString(), UUID.randomUUID().toString());
  }

  private static final class Message {

    private final String id;

    private final String datacontenttype;

    private final String data;

    private final Map<String, String> metadata;

    private Message(String id, String datacontenttype, String data, Map<String, String> metadata) {
      this.id = id;
      this.datacontenttype = datacontenttype;
      this.data = data;
      this.metadata = metadata;
    }
  }

  private byte[] serialize(Message message) throws IOException {
    if (message == null) {
      return null;
    }

    try (ByteArrayOutputStream bos = new ByteArrayOutputStream()) {
      JsonGenerator generator = JSON_FACTORY.createGenerator(bos);
      generator.writeStartObject();
      if (message.id != null) {
        generator.writeStringField("id", message.id);
      }
      if (message.datacontenttype != null) {
        generator.writeStringField("datacontenttype", message.datacontenttype);
      }
      if (message.data != null) {
        generator.writeStringField("data", message.data);
      }
      generator.writeEndObject();
      generator.close();
      bos.flush();
      return bos.toByteArray();
    }
  }
}

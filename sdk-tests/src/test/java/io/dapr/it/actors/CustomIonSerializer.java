/*
 * Copyright 2022 The Dapr Authors
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

package io.dapr.it.actors;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.ion.IonFactory;
import io.dapr.serialization.DaprObjectSerializer;
import io.dapr.serialization.MimeType;
import io.dapr.utils.TypeRef;

import java.io.IOException;

/**
 * Emulates a custom ION serializer to validate the SDK behaves the way as with the default serializer.
 */
class CustomIonSerializer implements DaprObjectSerializer {

  private static final ObjectMapper MAPPER = new ObjectMapper(new IonFactory())
      .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

  /**
   * {@inheritDoc}
   */
  @Override
  public byte[] serialize(Object o) throws IOException {
    if (o == null) {
      return new byte[0];
    }
    return MAPPER.writeValueAsBytes(o);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public <T> T deserialize(byte[] data, TypeRef<T> type) throws IOException {
    if ((data == null) || (data.length == 0)) {
      return null;
    }
    return MAPPER.readValue(data, MAPPER.constructType(type.getType()));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public MimeType getContentType() {
    return new MimeType("application/ion", MimeType.MimeTypeAttribute.TEXT);
  }

}

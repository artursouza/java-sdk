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

package io.dapr.it.state;

import io.dapr.serialization.DaprObjectSerializer;
import io.dapr.serialization.MimeType;
import io.dapr.utils.TypeRef;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

/**
 * Emulates a custom binary serializer to validate the SDK behaves the way as with the default serializer.
 */
class CustomBinarySerializer implements DaprObjectSerializer {

  /**
   * {@inheritDoc}
   */
  @Override
  public byte[] serialize(Object o) throws IOException {
    try (ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream()) {
      try (ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteArrayOutputStream)) {
        objectOutputStream.writeObject(o);
        objectOutputStream.flush();
      }
      return byteArrayOutputStream.toByteArray();
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public <T> T deserialize(byte[] data, TypeRef<T> type) throws IOException {
    if ((data == null) || (data.length == 0)) {
      return null;
    }

    try(ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(data))) {
      try {
        return (T) in.readObject();
      } catch (ClassNotFoundException e) {
        throw new IOException(e);
      }
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public MimeType getContentType() {
    return MimeType.APPLICATION_OCTETSTREAM;
  }
}

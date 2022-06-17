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

package io.dapr.serializer;

import io.dapr.serialization.DaprObjectSerializer;
import io.dapr.serialization.MimeType;
import io.dapr.utils.TypeRef;

import java.io.IOException;


/**
 * This class is required to keep the fix in custom serializer to be backwards compatible.
 * Due to a bug in previous versions, we must use a binary serializer for a deprecated custom JSON serializer.
 * This class should be removed when the deprecated serialization interface is removed.
 * See https://github.com/dapr/java-sdk/issues/503
 */
@Deprecated
public class AdaptedDaprObjectSerializer implements DaprObjectSerializer {

  private final MimeType contentType;

  private final io.dapr.serializer.DaprObjectSerializer deprecatedSerializer;

  /**
   *  Constructor for a AdaptedDaprObjectSerializer.
   *
   * @param deprecatedSerializer Deprecated serializer to be adapted.
   */
  public AdaptedDaprObjectSerializer(io.dapr.serializer.DaprObjectSerializer deprecatedSerializer) {
    if (deprecatedSerializer == null) {
      throw new IllegalArgumentException("Serializer is required");
    }

    if (deprecatedSerializer.getContentType() == null || deprecatedSerializer.getContentType().isEmpty()) {
      throw new IllegalArgumentException("Content Type should not be null or empty");
    }

    this.deprecatedSerializer = deprecatedSerializer;
    this.contentType = adaptContentType(deprecatedSerializer);
  }

  private static MimeType adaptContentType(io.dapr.serializer.DaprObjectSerializer deprecatedSerializer) {
    if (deprecatedSerializer == null) {
      return null;
    }

    if (deprecatedSerializer.getClass() == io.dapr.serializer.DefaultObjectSerializer.class) {
      // This content type works correctly because it has the TEXT attribute,
      // telling Dapr's client that it should not convert to Base64 and send payload as string.
      return MimeType.APPLICATION_JSON;
    }

    // To keep this backwards compatible, we keep the bug behavior until users can migrate their data.
    // Do not add any attribute, so it can keep the old behavior described in dapr/java-sdk#503
    return new MimeType(deprecatedSerializer.getContentType());
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public byte[] serialize(Object o) throws IOException {
    return this.deprecatedSerializer.serialize(o);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public <T> T deserialize(byte[] data, TypeRef<T> type) throws IOException {
    return this.deprecatedSerializer.deserialize(data, type);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public MimeType getContentType() {
    return this.contentType;
  }
}

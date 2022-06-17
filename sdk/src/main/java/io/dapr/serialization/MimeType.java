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

package io.dapr.serialization;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * A MimeType as per IETF's RFC 6838: https://datatracker.ietf.org/doc/html/rfc6838.
 */
public class MimeType {

  public static final MimeType TEXT_PLAIN =
      new MimeType("text/plain", MimeTypeAttribute.TEXT);

  public static final MimeType APPLICATION_JSON =
      new MimeType("application/json", MimeTypeAttribute.TEXT, MimeTypeAttribute.JSON);

  public static final MimeType APPLICATION_XML =
      new MimeType("application/xml", MimeTypeAttribute.TEXT, MimeTypeAttribute.XML);

  public static final MimeType APPLICATION_OCTETSTREAM =
      new MimeType("application/octet-stream");

  /**
   * Attributes to qualify a MimeType.
   */
  public enum MimeTypeAttribute {
    /**
     * MimeType is text based.
     */
    TEXT,
    JSON,
    XML
  }

  private final String mimeType;

  private final Set<MimeTypeAttribute> attributes;

  /**
   * Constructor for a MimeType.
   *
   * @param mimeType       MimeType as per RFC 6838.
   * @param attributes Attributes to qualify the MimeType.
   */
  public MimeType(String mimeType, MimeTypeAttribute... attributes) {
    this.mimeType = mimeType;
    this.attributes = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(attributes)));
  }

  /**
   * Determines if the given mimetype contains a given attribute.
   *
   * @param attribute Attribute to be checked.
   * @return True if mimetype contains attribute.
   */
  public boolean hasAttribute(MimeTypeAttribute attribute) {
    if (attribute == null) {
      return false;
    }

    return this.attributes.contains(attribute);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String toString() {
    return this.mimeType;
  }

  /**
   * Determines if mimetype is not set.
   * @return True if mimetype is null or empty.
   */
  public boolean isEmpty() {
    return (this.mimeType == null) || (this.mimeType.isEmpty());
  }
}

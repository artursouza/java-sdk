/*
 * Copyright (c) Microsoft Corporation.
 * Licensed under the MIT License.
 */

package io.dapr.client.domain;

import io.dapr.client.DaprHttp;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * HTTP Extension class.
 * This class is only needed if the app you are calling is listening on HTTP.
 * It contains properties that represent data that may be populated for an HTTP receiver.
 */

public final class HttpExtension {
  /**
   * Convenience HttpExtension object for {@link DaprHttp.HttpMethod#NONE} with empty queryString.
   */
  public static final HttpExtension NONE = new HttpExtension(DaprHttp.HttpMethod.NONE, new HashMap<>());
  /**
   * Convenience HttpExtension object for the {@link DaprHttp.HttpMethod#GET} Verb with empty queryString.
   */
  public static final HttpExtension GET = new HttpExtension(DaprHttp.HttpMethod.GET, new HashMap<>());
  /**
   * Convenience HttpExtension object for the {@link DaprHttp.HttpMethod#PUT} Verb with empty queryString.
   */
  public static final HttpExtension PUT = new HttpExtension(DaprHttp.HttpMethod.PUT, new HashMap<>());
  /**
   * Convenience HttpExtension object for the {@link DaprHttp.HttpMethod#POST} Verb with empty queryString.
   */
  public static final HttpExtension POST = new HttpExtension(DaprHttp.HttpMethod.POST, new HashMap<>());
  /**
   * Convenience HttpExtension object for the {@link DaprHttp.HttpMethod#DELETE} Verb with empty queryString.
   */
  public static final HttpExtension DELETE = new HttpExtension(DaprHttp.HttpMethod.DELETE, new HashMap<>());
  /**
   * Convenience HttpExtension object for the {@link DaprHttp.HttpMethod#HEAD} Verb with empty queryString.
   */
  public static final HttpExtension HEAD = new HttpExtension(DaprHttp.HttpMethod.HEAD, new HashMap<>());
  /**
   * Convenience HttpExtension object for the {@link DaprHttp.HttpMethod#CONNECT} Verb with empty queryString.
   */
  public static final HttpExtension CONNECT = new HttpExtension(DaprHttp.HttpMethod.CONNECT, new HashMap<>());
  /**
   * Convenience HttpExtension object for the {@link DaprHttp.HttpMethod#OPTIONS} Verb with empty queryString.
   */
  public static final HttpExtension OPTIONS = new HttpExtension(DaprHttp.HttpMethod.OPTIONS, new HashMap<>());
  /**
   * Convenience HttpExtension object for the {@link DaprHttp.HttpMethod#TRACE} Verb with empty queryString.
   */
  public static final HttpExtension TRACE = new HttpExtension(DaprHttp.HttpMethod.TRACE, new HashMap<>());

  /**
   * HTTP verb.
   */
  private DaprHttp.HttpMethod method;

  /**
   * HTTP querystring.
   */
  private Map<String, String> queryString;

  /**
   * Construct a HttpExtension object.
   * @param method      Required value denoting the HttpMethod.
   * @param queryString Non-null map value for the queryString for a HTTP listener.
   * @see DaprHttp.HttpMethod for supported methods.
   * @throws IllegalArgumentException on null method or queryString.
   */
  public HttpExtension(DaprHttp.HttpMethod method, Map<String, String> queryString) {
    if (method == null) {
      throw new IllegalArgumentException("HttpExtension method cannot be null");
    } else if (queryString == null) {
      throw new IllegalArgumentException("HttpExtension queryString map cannot be null");
    }
    this.method = method;
    this.queryString = Collections.unmodifiableMap(queryString);
  }

  public DaprHttp.HttpMethod getMethod() {
    return method;
  }

  public Map<String, String> getQueryString() {
    return queryString;
  }
}

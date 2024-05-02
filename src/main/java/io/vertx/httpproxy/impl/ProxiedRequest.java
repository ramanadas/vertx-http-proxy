/*
 * Copyright (c) 2011-2020 Contributors to the Eclipse Foundation
 *
 * This program and the accompanying materials are made available under the
 * terms of the Eclipse Public License 2.0 which is available at
 * http://www.eclipse.org/legal/epl-2.0, or the Apache License, Version 2.0
 * which is available at https://www.apache.org/licenses/LICENSE-2.0.
 *
 * SPDX-License-Identifier: EPL-2.0 OR Apache-2.0
 */
package io.vertx.httpproxy.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.MultiMap;
import io.vertx.core.Promise;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpClientRequest;
import io.vertx.core.http.HttpHeaders;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpVersion;
import io.vertx.core.http.impl.HttpServerRequestInternal;
import io.vertx.core.impl.ContextInternal;
import io.vertx.core.net.HostAndPort;
import io.vertx.core.net.impl.HostAndPortImpl;
import io.vertx.core.json.JsonObject;
import io.vertx.core.streams.Pipe;
import io.vertx.httpproxy.Body;
import io.vertx.httpproxy.ProxyRequest;
import io.vertx.httpproxy.ProxyResponse;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class ProxiedRequest implements ProxyRequest {

  private static final CharSequence X_FORWARDED_HOST = HttpHeaders.createOptimized("x-forwarded-host");

  private static final MultiMap HOP_BY_HOP_HEADERS = MultiMap.caseInsensitiveMultiMap()
    .add(HttpHeaders.CONNECTION, "whatever")
    .add(HttpHeaders.KEEP_ALIVE, "whatever")
    .add(HttpHeaders.PROXY_AUTHENTICATE, "whatever")
    .add(HttpHeaders.PROXY_AUTHORIZATION, "whatever")
    .add("te", "whatever")
    .add("trailer", "whatever")
    .add(HttpHeaders.TRANSFER_ENCODING, "whatever")
    .add(HttpHeaders.UPGRADE, "whatever");

  final ContextInternal context;
  private HttpMethod method;
  private final HttpVersion version;
  private String uri;
  private final String absoluteURI;
  private Body body;
  private HostAndPort authority;
  private final MultiMap headers;
  HttpClientRequest request;
  private final HttpServerRequest proxiedRequest;
  private final Map<String, Object> contextData = new HashMap<>();

  public ProxiedRequest(HttpServerRequest proxiedRequest) {

    // Determine content length
    long contentLength = -1L;
    String contentLengthHeader = proxiedRequest.getHeader(HttpHeaders.CONTENT_LENGTH);
    if (contentLengthHeader != null) {
      try {
        contentLength = Long.parseLong(contentLengthHeader);
      } catch (NumberFormatException e) {
        // Ignore ???
      }
    }

    this.method = proxiedRequest.method();
    this.version = proxiedRequest.version();
    this.body = Body.body(proxiedRequest, contentLength);
    this.uri = proxiedRequest.uri();
    this.headers = MultiMap.caseInsensitiveMultiMap().addAll(proxiedRequest.headers());
    this.absoluteURI = proxiedRequest.absoluteURI();
    this.proxiedRequest = proxiedRequest;
    this.context = (ContextInternal) ((HttpServerRequestInternal) proxiedRequest).context();
    this.authority = proxiedRequest.authority();
  }

  @Override
  public HttpVersion version() {
    return version;
  }

  @Override
  public String getURI() {
    return uri;
  }

  @Override
  public ProxyRequest setURI(String uri) {
    this.uri = uri;
    return this;
  }

  @Override
  public Body getBody() {
    return body;
  }

  @Override
  public ProxyRequest setBody(Body body) {
    this.body = body;
    return this;
  }

  @Override
  public ProxyRequest setAuthority(HostAndPort authority) {
    Objects.requireNonNull(authority);
    this.authority= authority;
    return this;
  }

  @Override
  public HostAndPort getAuthority() {
    return authority;
  }

  @Override
  public String absoluteURI() {
    return absoluteURI;
  }

  @Override
  public HttpMethod getMethod() {
    return method;
  }

  @Override
  public ProxyRequest setMethod(HttpMethod method) {
    this.method = method;
    return this;
  }

  @Override
  public HttpServerRequest proxiedRequest() {
    return proxiedRequest;
  }

  @Override
  public ProxyRequest release() {
    body.stream().resume();
    headers.clear();
    body = null;
    return this;
  }

  @Override
  public ProxyResponse response() {
    return new ProxiedResponse(this, proxiedRequest.response());
  }

  void sendRequest(Handler<AsyncResult<ProxyResponse>> responseHandler) {

    request.response().<ProxyResponse>map(r -> {
      r.pause(); // Pause it
      return new ProxiedResponse(this, proxiedRequest.response(), r);
    }).onComplete(responseHandler);


    request.setMethod(method);
    request.setURI(uri);

    // Add all headers
    for (Map.Entry<String, String> header : headers) {
      String name = header.getKey();
      String value = header.getValue();
      if (!HOP_BY_HOP_HEADERS.contains(name) && !name.equals("host")) {
        request.headers().add(name, value);
      }
    }

    //
    if (authority != null) {
      request.authority(authority);
      HostAndPort proxiedAuthority = proxiedRequest.authority();
      if (!equals(authority, proxiedAuthority)) {
        // Should cope with existing forwarded host headers
        request.putHeader(X_FORWARDED_HOST, proxiedAuthority.toString());
      }
    }

    long len = body.length();
    if (len >= 0) {
      request.putHeader(HttpHeaders.CONTENT_LENGTH, Long.toString(len));
    } else {
      Boolean isChunked = HttpUtils.isChunked(proxiedRequest.headers());
      request.setChunked(len == -1 && Boolean.TRUE == isChunked);
    }

    Pipe<Buffer> pipe = body.stream().pipe();
    pipe.endOnComplete(true);
    pipe.endOnFailure(false);
    pipe.to(request).onComplete(ar -> {
      if (ar.failed()) {
        request.reset();
      }
    });
  }

  private static boolean equals(HostAndPort hp1, HostAndPort hp2) {
    if (hp1 == null || hp2 == null) {
      return false;
    }
    return hp1.host().equals(hp2.host()) && hp1.port() == hp2.port();
  }

  @Override
  public ProxyRequest putHeader(CharSequence name, CharSequence value) {
    headers.set(name, value);
    return this;
  }

  @Override
  public MultiMap headers() {
    return headers;
  }

  @Override
  public Future<ProxyResponse> send(HttpClientRequest request) {
    Promise<ProxyResponse> promise = context.promise();
    this.request = request;
    sendRequest(promise);
    return promise.future();
  }

  public ProxiedRequest addPathPrefix(String prefix) {
    this.uri = prefix + this.uri;
    return this;
  }

  public ProxiedRequest removePathPrefix(String prefix) {
    if (this.uri.startsWith(prefix)) {
      this.uri = this.uri.substring(prefix.length());
    }
    return this;
  }

  public ProxiedRequest transformQueryToPathParams() {
    URI originalUri = URI.create(this.uri);
    String originalPath = originalUri.getPath();
    String query = originalUri.getQuery();
    if (query != null && !query.isEmpty()) {
      Map<String, String> queryParams = new HashMap<>();
      for (String param : query.split("&")) {
        String[] pair = param.split("=");
        queryParams.put(pair[0], pair[1]);
      }
      StringBuilder newPath = new StringBuilder(originalPath);
      for (Map.Entry<String, String> entry : queryParams.entrySet()) {
        newPath.append("/").append(entry.getValue());
      }
      this.uri = newPath.toString();
    }
    return this;
  }

  public ProxiedRequest addHeader(String name, String value) {
    this.headers.add(name, value);
    return this;
  }

  public ProxiedRequest removeHeader(String name) {
    this.headers.remove(name);
    return this;
  }

  public ProxiedRequest modifyHeader(String name, String newVal) {
    this.headers.set(name, newVal);
    return this;
  }

  public ProxiedRequest setBody(JsonObject jsonObject) {
    body = Body.body(jsonObject.toBuffer());
    return this;
  }

  public void getBodyAsJson(Handler<JsonObject> resultHandler) {
    long len = body.length();
    if (body != null && len >=0) {
      BufferingWriteStream buffer = new BufferingWriteStream();
      body.stream().pipeTo(buffer).onComplete( ar->{
        if(ar.succeeded()){
          resultHandler.handle(buffer.content().toJsonObject());
        } else {
          System.out.println("not implemented");
        }
      });
    }
  }

  public Future<JsonObject> getBodyAsJson() {
    Promise<JsonObject> promise = Promise.promise();
    long len = body.length();
    if (body != null && len >= 0) {
      BufferingWriteStream buffer = new BufferingWriteStream();

      body.stream().pipeTo(buffer).onComplete( ar -> {
        if (ar.succeeded()) {
          JsonObject jsonObject = buffer.content().toJsonObject();
          promise.complete(jsonObject);
        } else {
          promise.fail(ar.cause());
        }
      });
    } else {
      promise.fail("Body is null");
    }
    return promise.future();
  }
  
  public void setContextData(String key, Object value) {
      contextData.put(key, value);
  }
  
  public <T> T getContextData(String key, Class<T> type) {
      Object o = contextData.get(key);
      return type.isInstance(o) ? type.cast(o) : null;
  }
}

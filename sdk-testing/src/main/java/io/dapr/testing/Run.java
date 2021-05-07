/*
 * Copyright (c) Microsoft Corporation and Dapr Contributors.
 * Licensed under the MIT License.
 */

package io.dapr.testing;


import io.dapr.client.DaprApiProtocol;
import io.dapr.config.Properties;

import java.io.IOException;

public class Run<T extends AppRun, R extends SidecarRun>  {

  private final T appRun;

  private final R sidecarRun;

  public Run(T apprun, R sidecarRun) {
    this.appRun = apprun;
    this.sidecarRun = sidecarRun;
  }

  public void stop() throws IOException, InterruptedException {
    this.appRun.stop();
    this.sidecarRun.stop();
  }

  /**
   * Changes in-memory Java properties to use this sidecar run.
   */
  public void use() {
    System.getProperties().setProperty(Properties.HTTP_PORT.getName(), String.valueOf(this.sidecarRun.getHttpPort()));
    System.getProperties().setProperty(Properties.GRPC_PORT.getName(), String.valueOf(this.sidecarRun.getGrpcPort()));
    System.getProperties().setProperty(Properties.API_PROTOCOL.getName(), DaprApiProtocol.GRPC.name());
    System.getProperties().setProperty(
        Properties.API_METHOD_INVOCATION_PROTOCOL.getName(),
        DaprApiProtocol.GRPC.name());
  }

  /**
   * Changes in-memory Java properties to use this sidecar run over gRPC.
   */
  public void switchToGrpc() {
    System.getProperties().setProperty(Properties.API_PROTOCOL.getName(), DaprApiProtocol.GRPC.name());
    System.getProperties().setProperty(
        Properties.API_METHOD_INVOCATION_PROTOCOL.getName(),
        DaprApiProtocol.GRPC.name());
  }

  /**
   * Changes in-memory Java properties to use this sidecar run over HTTP.
   */
  public void switchToHttp() {
    System.getProperties().setProperty(Properties.API_PROTOCOL.getName(), DaprApiProtocol.HTTP.name());
    System.getProperties().setProperty(
        Properties.API_METHOD_INVOCATION_PROTOCOL.getName(),
        DaprApiProtocol.HTTP.name());
  }
}

/*
 * Copyright (c) Microsoft Corporation and Dapr Contributors.
 * Licensed under the MIT License.
 */

package io.dapr.testing.standalone;

import io.dapr.config.Properties;
import io.dapr.testing.AppRun;
import io.dapr.testing.Stoppable;

import java.io.IOException;
import java.util.HashMap;

import static io.dapr.testing.standalone.Retry.callWithRetry;


/**
 * This class runs an app outside Dapr but adds Dapr env variables.
 */
class CommandAppRun implements AppRun, Stoppable {

  private static final int MAX_WAIT_MILLISECONDS = 30;

  private final Integer appPort;

  private final Command command;

  CommandAppRun(String command,
      Integer appPort,
      DaprPorts daprPorts) {
    this.command = new Command(
        "",
        command,
        new HashMap() {
          {
            put("DAPR_HTTP_PORT", daprPorts.getHttpPort().toString());
            put("DAPR_GRPC_PORT", daprPorts.getGrpcPort().toString());
          }
        });
    this.appPort = appPort;
  }

  public void start() throws InterruptedException, IOException {
    long start = System.currentTimeMillis();
    // First, try to stop previous run (if left running).
    this.stop();
    // Wait for the previous run to kill the prior process.
    System.out.println("Starting application ...");
    this.command.run();
    if (this.appPort != null) {
      long timeLeft = this.MAX_WAIT_MILLISECONDS - (System.currentTimeMillis() - start);
      callWithRetry(() -> {
        System.out.println("Checking if app is listening on port ...");
        assertListeningOnPort(this.appPort);
      }, timeLeft);
    }
    System.out.println("Application started.");
  }

  @Override
  public void stop() throws InterruptedException {
    System.out.println("Stopping application ...");
    try {
      this.command.stop();

      System.out.println("Application stopped.");
    } catch (RuntimeException e) {
      System.out.println("Could not stop command: " + this.command.toString());
    }
  }

  private static void assertListeningOnPort(int port) {
    java.net.SocketAddress socketAddress = new java.net.InetSocketAddress(Properties.SIDECAR_IP.get(), port);
    try (java.net.Socket socket = new java.net.Socket()) {
      socket.connect(socketAddress, 1000);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

}

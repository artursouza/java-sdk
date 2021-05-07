/*
 * Copyright (c) Microsoft Corporation and Dapr Contributors.
 * Licensed under the MIT License.
 */

package io.dapr.testing.standalone;

import io.dapr.client.DaprApiProtocol;
import io.dapr.config.Properties;
import io.dapr.testing.Stoppable;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.dapr.testing.standalone.Retry.callWithRetry;


class CommandSidecarRun implements io.dapr.testing.SidecarRun, Stoppable {

  private static final int MAX_WAIT_MILLISECONDS = 30;

  private static final String DAPR_SUCCESS_MESSAGE = "You're up and running!";

  private static final String DAPR_RUN = "dapr run --app-id %s --app-protocol %s ";

  private final DaprPorts ports;

  private final String appId;

  private final AtomicBoolean started;

  private final Command startCommand;

  private final Command listCommand;

  private final Command stopCommand;

  CommandSidecarRun(String appId,
      Integer appPort,
      DaprPorts daprPorts,
      DaprApiProtocol appProtocol,
      DaprApiProtocol daprProtocol) {
    this.appId = appId;
    this.startCommand =
        new Command(DAPR_SUCCESS_MESSAGE, buildDaprCommand(this.appId, appPort, daprPorts, appProtocol, daprProtocol));
    this.listCommand = new Command(this.appId, "dapr list");
    this.stopCommand = new Command(
        "app stopped successfully",
        "dapr stop --app-id " + this.appId);
    this.ports = daprPorts;
    this.started = new AtomicBoolean(false);
  }

  public void start() throws InterruptedException, IOException {
    long start = System.currentTimeMillis();
    // First, try to stop previous run (if left running).
    this.stop();
    // Wait for the previous run to kill the prior process.
    long timeLeft = this.MAX_WAIT_MILLISECONDS - (System.currentTimeMillis() - start);
    System.out.println("Checking if previous run for Dapr application has stopped ...");
    checkRunState(timeLeft, false);

    System.out.println("Starting dapr application ...");
    this.startCommand.run();
    this.started.set(true);

    timeLeft = this.MAX_WAIT_MILLISECONDS - (System.currentTimeMillis() - start);
    System.out.println("Checking if Dapr application has started ...");
    checkRunState(timeLeft, true);

    if (this.ports.getHttpPort() != null) {
      timeLeft = this.MAX_WAIT_MILLISECONDS - (System.currentTimeMillis() - start);
      callWithRetry(() -> {
        System.out.println("Checking if Dapr is listening on HTTP port ...");
        assertListeningOnPort(this.ports.getHttpPort());
      }, timeLeft);
    }

    if (this.ports.getGrpcPort() != null) {
      timeLeft = this.MAX_WAIT_MILLISECONDS - (System.currentTimeMillis() - start);
      callWithRetry(() -> {
        System.out.println("Checking if Dapr is listening on GRPC port ...");
        assertListeningOnPort(this.ports.getGrpcPort());
      }, timeLeft);
    }
    System.out.println("Dapr application started.");
  }

  @Override
  public void stop() throws InterruptedException, IOException {
    System.out.println("Stopping dapr application ...");
    try {
      this.stopCommand.run();

      System.out.println("Dapr application stopped.");
    } catch (RuntimeException e) {
      System.out.println("Could not stop app " + this.appId + ": " + e.getMessage());
    }
  }

  private void checkRunState(long timeout, boolean shouldBeRunning) throws InterruptedException {
    callWithRetry(() -> {
      try {
        this.listCommand.run();

        if (!shouldBeRunning) {
          throw new RuntimeException("Previous run for app has not stopped yet!");
        }
      } catch (IllegalStateException e) {
        // Bad case if the app is supposed to be running.
        if (shouldBeRunning) {
          throw e;
        }
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }, timeout);
  }

  private static String buildDaprCommand(
      String appId, Integer appPort, DaprPorts ports, DaprApiProtocol appProtocol, DaprApiProtocol daprProtocol) {
    StringBuilder stringBuilder =
        new StringBuilder(String.format(DAPR_RUN, appId, appProtocol.toString().toLowerCase()))
            .append(appPort != null ? " --app-port " + appPort : "")
            .append(ports.getHttpPort() != null ? " --dapr-http-port " + ports.getHttpPort() : "")
            .append(ports.getGrpcPort() != null ? " --dapr-grpc-port " + ports.getGrpcPort() : "")
            .append(" --");
    return stringBuilder.toString();
  }

  private static void assertListeningOnPort(int port) {
    java.net.SocketAddress socketAddress = new java.net.InetSocketAddress(Properties.SIDECAR_IP.get(), port);
    try (java.net.Socket socket = new java.net.Socket()) {
      socket.connect(socketAddress, 1000);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public int getHttpPort() {
    return this.ports.getHttpPort();
  }

  @Override
  public int getGrpcPort() {
    return this.ports.getGrpcPort();
  }
}

/*
 * Copyright (c) Microsoft Corporation and Dapr Contributors.
 * Licensed under the MIT License.
 */

package io.dapr.testing.standalone;

import io.dapr.client.DaprApiProtocol;
import io.dapr.testing.DaprRunner;
import io.dapr.testing.Run;

import java.io.IOException;

public class CommandRunner implements DaprRunner {

  private final String appId;

  private final String command;

  private final Integer port;

  /**
   * Creates a new instance of CommandRunner.
   * @param appId Application Id
   * @param command Command for the app
   * @param port Port for the app (optional)
   */
  public CommandRunner(
      String appId,
      String command,
      Integer port) {
    this.appId = appId;
    this.command = command;
    this.port = port;
  }

  /**
   * Runs this app.
   *
   * @return Dapr run
   */
  public Run<CommandAppRun, CommandSidecarRun> run() throws InterruptedException, IOException {

    DaprPorts ports = DaprPorts.build();
    CommandAppRun appRun = new CommandAppRun(
        this.command,
        this.port,
        ports);

    CommandSidecarRun sidecarRun = new CommandSidecarRun(
        this.appId,
        this.port,
        ports,
        DaprApiProtocol.HTTP,
        DaprApiProtocol.GRPC);
    appRun.start();
    sidecarRun.start();

    return new Run(appRun, sidecarRun);
  }
}

/*
 * Copyright (c) Microsoft Corporation and Dapr Contributors.
 * Licensed under the MIT License.
 */

package io.dapr.testing.standalone;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

class DaprPorts {

  private final Integer grpcPort;

  private final Integer httpPort;

  private DaprPorts(Integer httpPort, Integer grpcPort) {
    this.grpcPort = grpcPort;
    this.httpPort = httpPort;
  }

  public static DaprPorts build() {
    try {
      List<Integer> freePorts = new ArrayList<>(findFreePorts(2));
      return new DaprPorts(freePorts.get(0), freePorts.get(1));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public Integer getGrpcPort() {
    return grpcPort;
  }

  public Integer getHttpPort() {
    return httpPort;
  }

  private static Set<Integer> findFreePorts(int n) throws IOException {
    Set<Integer> output = new HashSet<>();
    for (int i = 0; i < n;) {
      try (ServerSocket socket = new ServerSocket(0)) {
        socket.setReuseAddress(true);
        int port = socket.getLocalPort();
        if (!output.contains(port)) {
          output.add(port);
          i++;
        }
      }
    }
    return output;
  }
}

/*
 * Copyright (c) Microsoft Corporation and Dapr Contributors.
 * Licensed under the MIT License.
 */

package io.dapr.examples.integrationtesting;

import io.dapr.client.DaprClient;
import io.dapr.client.DaprClientBuilder;
import io.dapr.testing.Run;
import io.dapr.testing.standalone.CommandRunner;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.UUID;

/**
 * 1. Build and install jars:
 * mvn clean install
 * 2. cd [repo root]/examples
 * 3. send a message to be saved as state:
 * java -jar target/dapr-java-sdk-examples-exec.jar \
 *     org.junit.platform.console.ConsoleLauncher --select-class=io.dapr.examples.integrationtesting.DaprExampleTest
 */
public class DaprExampleTest {

  private static final String STATE_STORE_NAME = "statestore";

  private static Run run;

  /**
   * Setup.
   *
   * @throws Exception If fails.
   */
  @BeforeClass
  public static void init() throws Exception {
    CommandRunner daprRunner = new CommandRunner("myappid", "echo hi", null);
    run = daprRunner.run();
    run.use();
  }

  /**
   * Teardown.
   *
   * @throws Exception If fails.
   */
  @AfterClass
  public static void teardown() throws Exception {
    if (run != null) {
      run.stop();
    }
  }

  @Test
  public void testSetAndGetState() throws Exception {
    try (DaprClient client = new DaprClientBuilder().build()) {
      System.out.println("Waiting for Dapr sidecar ...");
      client.waitForSidecar(10000).block();
      System.out.println("Dapr sidecar is ready.");

      String key = UUID.randomUUID().toString();
      String value = UUID.randomUUID().toString();

      client.saveState(STATE_STORE_NAME, key, value).block();

      String retrievedValue = client.getState(STATE_STORE_NAME, key, String.class).block().getValue();

      Assert.assertEquals(value, retrievedValue);
    }
  }

}

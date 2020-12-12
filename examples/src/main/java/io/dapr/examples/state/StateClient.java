/*
 * Copyright (c) Microsoft Corporation.
 * Licensed under the MIT License.
 */

package io.dapr.examples.state;

import io.dapr.client.DaprClient;
import io.dapr.client.DaprClientBuilder;
import io.dapr.client.domain.State;
import io.dapr.client.domain.TransactionalStateOperation;
import reactor.core.publisher.Mono;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * 1. Build and install jars:
 * mvn clean install
 * 2. Go to examples directory:
 * cd examples
 * 3. send a message to be saved as state:
 * dapr run --components-path ./components2 -- \
 * java -jar target/dapr-java-sdk-examples-exec.jar io.dapr.examples.state.StateClient 'my message'
 */
public class StateClient {

  public static class MyClass {
    public String message;

    @Override
    public String toString() {
      return message;
    }
  }

  private static final String STATE_STORE_NAME = "statestore";

  private static final String FIRST_KEY_NAME = "myKey";

  private static final String SECOND_KEY_NAME = "myKey2";

  /**
   * Executes the sate actions.
   * @param args messages to be sent as state value.
   */
  public static void main(String[] args) throws Exception {
    try (DaprClient client = new DaprClientBuilder().build()) {
      String message = args.length == 0 ? " " : args[0];

      MyClass firstState = new MyClass();
      firstState.message = message;
      MyClass secondState = new MyClass();
      secondState.message = "test message";

      client.saveState(STATE_STORE_NAME, FIRST_KEY_NAME, firstState).block();
      System.out.println("Saving class with message: " + firstState.message);

      client.saveState(STATE_STORE_NAME, SECOND_KEY_NAME, secondState).block();
      System.out.println("Saving class with message: " + secondState.message);

      Mono<State<MyClass>> retrievedMessageMono = client.getState(STATE_STORE_NAME, FIRST_KEY_NAME, MyClass.class);
      System.out.println("Retrieved class message from state: " + (retrievedMessageMono.block().getValue()).message);

      System.out.println("Updating previous state and adding another state 'test state'... ");
      firstState.message = message + " updated";
      System.out.println("Saving updated class with message: " + firstState.message);

      // get multiple states
      Mono<List<State<MyClass>>> retrievedMessagesMono = client.getStates(STATE_STORE_NAME,
          Arrays.asList(FIRST_KEY_NAME, SECOND_KEY_NAME), MyClass.class);
      System.out.println("Retrieved messages using bulk get:");
      retrievedMessagesMono.block().forEach(System.out::println);

      // This is an example, so for simplicity we are just exiting here.
      // Normally a dapr app would be a web service and not exit main.
      System.out.println("Done");
    }
  }
}

/*
 * Copyright (c) Microsoft Corporation and Dapr Contributors.
 * Licensed under the MIT License.
 */

package io.dapr.testing.standalone;

final class Retry {

  private Retry() {
  }

  static void callWithRetry(Runnable function, long retryTimeoutMilliseconds) throws InterruptedException {
    long started = System.currentTimeMillis();
    while (true) {
      Throwable exception;
      try {
        function.run();
        return;
      } catch (Exception e) {
        exception = e;
      } catch (AssertionError e) {
        exception = e;
      }

      if (System.currentTimeMillis() - started >= retryTimeoutMilliseconds) {
        throw new RuntimeException(exception);
      }
      Thread.sleep(1000);
    }
  }
}

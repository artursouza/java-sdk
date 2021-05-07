/*
 * Copyright (c) Microsoft Corporation and Dapr Contributors.
 * Licensed under the MIT License.
 */

package io.dapr.testing;

import java.io.IOException;

public interface DaprRunner<T extends AppRun, R extends SidecarRun> {

  Run<T, R> run() throws InterruptedException, IOException;
}

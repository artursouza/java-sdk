/*
 * Copyright (c) Microsoft Corporation and Dapr Contributors.
 * Licensed under the MIT License.
 */

package io.dapr.testing;

import java.io.IOException;

public interface SidecarRun extends Stoppable {

  int getHttpPort();

  int getGrpcPort();
}

/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.launcher

import io.airbyte.workers.models.CheckConnectionInput
import io.airbyte.workers.models.DiscoverCatalogInput
import io.airbyte.workers.models.SpecInput
import io.airbyte.workload.launcher.pipeline.consumer.LauncherInput
import io.airbyte.workload.launcher.pipeline.stages.model.SyncPayload
import java.util.UUID

/**
 * Interface for launching and managing workloads.
 * Allows for different implementations of workload execution (e.g. Kubernetes, Docker).
 * The active implementation is determined by AIRBYTE_WORKLOAD_LAUNCHER_TYPE.
 */
interface Launcher {
  fun launchReplication(
    payload: SyncPayload,
    launcherInput: LauncherInput,
  )

  fun launchReset(
    payload: SyncPayload,
    launcherInput: LauncherInput,
  )

  fun launchCheck(
    checkInput: CheckConnectionInput,
    launcherInput: LauncherInput,
  )

  fun launchDiscover(
    discoverCatalogInput: DiscoverCatalogInput,
    launcherInput: LauncherInput,
  )

  fun launchSpec(
    specInput: SpecInput,
    launcherInput: LauncherInput,
  )

  fun workloadRunning(autoId: UUID): Boolean

  fun deleteMutexWorkload(mutexKey: String): Boolean
}

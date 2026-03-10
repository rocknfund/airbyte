/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.launcher.pipeline.stages

import fixtures.RecordFixtures
import io.airbyte.metrics.MetricClient
import io.airbyte.persistence.job.models.ReplicationInput
import io.airbyte.workload.launcher.pipeline.stages.model.LaunchStageIO
import io.airbyte.workload.launcher.pipeline.stages.model.SyncPayload
import io.airbyte.workload.launcher.pods.WorkloadLauncher
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Test

class EnforceMutexStageTest {
  @Test
  fun `deletes existing pods for mutex key`() {
    val payload = SyncPayload(ReplicationInput())
    val mutexKey = "a unique key"

    val launcher: WorkloadLauncher = mockk()
    val metricClient: MetricClient = mockk()
    every { launcher.deleteMutexWorkload(any()) } returns false

    val stage = EnforceMutexStage(launcher, metricClient)
    val io =
      LaunchStageIO(msg = RecordFixtures.launcherInput(mutexKey = mutexKey), payload = payload)

    val result = stage.applyStage(io)

    verify {
      launcher.deleteMutexWorkload(mutexKey)
    }

    assert(result.payload == payload)
  }

  @Test
  fun `noops if mutex key not present`() {
    val payload = SyncPayload(ReplicationInput())

    val launcher: WorkloadLauncher = mockk()
    val metricClient: MetricClient = mockk()
    every { launcher.deleteMutexWorkload(any()) } returns false

    val stage = EnforceMutexStage(launcher, metricClient)
    val io =
      LaunchStageIO(msg = RecordFixtures.launcherInput(mutexKey = null), payload = payload)

    val result = stage.applyStage(io)

    verify(exactly = 0) {
      launcher.deleteMutexWorkload(any())
    }

    assert(result.payload == payload)
  }
}

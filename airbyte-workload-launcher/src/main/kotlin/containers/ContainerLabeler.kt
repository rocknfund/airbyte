/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.launcher.containers

import io.airbyte.workers.pod.Metadata
import io.airbyte.workers.pod.Metadata.CHECK_JOB
import io.airbyte.workers.pod.Metadata.DISCOVER_JOB
import io.airbyte.workers.pod.Metadata.JOB_TYPE_KEY
import io.airbyte.workers.pod.Metadata.SPEC_JOB
import io.airbyte.workers.pod.Metadata.SYNC_JOB
import io.airbyte.workers.pod.Metadata.SYNC_STEP_KEY
import io.airbyte.workers.pod.Metadata.REPLICATION_STEP
import io.micronaut.context.annotation.Requires
import jakarta.inject.Singleton
import java.util.UUID

internal const val AUTO_ID = "auto_id"
internal const val MUTEX_KEY = "mutex_key"
internal const val WORKLOAD_ID = "workload_id"

/**
 * Generates Docker container labels for workload tracking and lifecycle management.
 * Mirrors PodLabeler for Kubernetes, using Docker container labels instead of K8s labels.
 */
@Singleton
@Requires(property = "worker.environment", value = "docker")
class ContainerLabeler {
  fun getAutoIdLabels(autoId: UUID): Map<String, String> =
    mapOf(
      AUTO_ID to autoId.toString(),
    )

  fun getWorkloadLabels(workloadId: String?): Map<String, String> =
    workloadId?.let {
      mapOf(
        WORKLOAD_ID to it,
      )
    } ?: emptyMap()

  fun getMutexLabels(key: String?): Map<String, String> =
    key?.let {
      mapOf(
        MUTEX_KEY to it,
      )
    } ?: emptyMap()

  fun getCheckLabels(): Map<String, String> =
    mapOf(
      JOB_TYPE_KEY to CHECK_JOB,
    )

  fun getDiscoverLabels(): Map<String, String> =
    mapOf(
      JOB_TYPE_KEY to DISCOVER_JOB,
    )

  fun getSpecLabels(): Map<String, String> =
    mapOf(
      JOB_TYPE_KEY to SPEC_JOB,
    )

  fun getSweeperLabels(): Map<String, String> =
    mapOf(
      SWEEPER_LABEL_KEY to SWEEPER_LABEL_VALUE,
    )

  fun getSharedLabels(
    workloadId: String?,
    mutexKey: String?,
    passThroughLabels: Map<String, String>,
    autoId: UUID,
    workspaceId: UUID?,
    networkSecurityTokens: List<String>,
  ): Map<String, String> =
    passThroughLabels +
      getMutexLabels(mutexKey) +
      getWorkloadLabels(workloadId) +
      getAutoIdLabels(autoId) +
      getSweeperLabels()

  companion object {
    const val SWEEPER_LABEL_KEY = "airbyte"
    const val SWEEPER_LABEL_VALUE = "job-container"
  }
}

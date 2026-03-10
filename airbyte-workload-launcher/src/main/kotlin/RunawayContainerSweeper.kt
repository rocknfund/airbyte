/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.launcher

import com.github.dockerjava.api.DockerClient
import com.github.dockerjava.api.model.Container
import io.airbyte.commons.annotation.InternalForTesting
import io.airbyte.featureflag.FeatureFlagClient
import io.airbyte.metrics.MetricClient
import io.airbyte.metrics.OssMetricsRegistry
import io.airbyte.metrics.annotations.Instrument
import io.airbyte.metrics.annotations.Tag
import io.airbyte.metrics.lib.MetricTags
import io.airbyte.workload.api.client.WorkloadApiClient
import io.airbyte.workload.api.domain.WorkloadListActiveRequest
import io.airbyte.workload.api.domain.WorkloadSummary
import io.airbyte.workload.launcher.containers.DockerContainerLauncher
import io.airbyte.workload.launcher.model.DataplaneConfig
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.context.annotation.Requires
import io.micronaut.context.event.ApplicationEventListener
import io.micronaut.scheduling.annotation.Scheduled
import jakarta.inject.Singleton
import java.time.Clock
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import kotlin.time.Duration.Companion.days
import kotlin.time.toJavaDuration

private val logger = KotlinLogging.logger {}

/**
 * Docker equivalent of RunawayPodSweeper.
 * Since Docker container labels are immutable, this class maintains an in-memory marker state
 * to track runaway containers before sweeping them.
 */
@Singleton
@Requires(property = "worker.environment", value = "docker")
open class RunawayContainerSweeper(
  private val workloadApi: WorkloadApiClient,
  private val dockerClient: DockerClient,
  private val containerLauncher: DockerContainerLauncher,
  private val metricClient: MetricClient,
  private val clock: Clock,
  private val featureFlagClient: FeatureFlagClient,
) : ApplicationEventListener<DataplaneConfig> {
  private var dataplaneId: UUID? = null

  // In-memory state tracking to replace the immutable 'delete-by' Docker label.
  // Maps autoId -> epochSeconds
  private val runawayMarkers = ConcurrentHashMap<String, Long>()

  override fun onApplicationEvent(event: DataplaneConfig?) {
    dataplaneId = event?.dataplaneId
  }

  @Instrument(
    start = "WORKLOAD_LAUNCHER_CRON",
    duration = "WORKLOAD_LAUNCHER_CRON_DURATION",
    tags = [Tag(key = MetricTags.CRON_TYPE, value = "runaway_container_mark")],
  )
  @Scheduled(cron = "35 * * * *")
  open fun mark() {
    dataplaneId?.let { mark(it) }
  }

  @InternalForTesting
  internal fun mark(dataplaneId: UUID) {
    logger.info { "Marking runaway Docker containers" }

    val activeWorkloadsByAutoId: Map<String, WorkloadSummary> = try {
      workloadApi
        .workloadListActive(WorkloadListActiveRequest(listOf(dataplaneId.toString())))
        .workloads
        .associateBy { it.autoId }
    } catch (e: Exception) {
      logger.error(e) { "Failed to list active workloads from API" }
      emptyMap()
    }

    val activeContainersByAutoId = mutableMapOf<String, Container>()
    
    val containers = dockerClient.listContainersCmd()
      .withShowAll(false) // Only running
      .withLabelFilter(mapOf("airbyte" to "job-pod"))
      .exec()
      
    for (container in containers) {
      val autoId = container.labels?.get("auto_id")
      if (autoId != null) {
        activeContainersByAutoId[autoId] = container
      }
    }

    val autoIdsToMark = activeContainersByAutoId.keys - activeWorkloadsByAutoId.keys
    metricClient.count(OssMetricsRegistry.WORKLOAD_RUNAWAY_POD, autoIdsToMark.size.toLong())

    val deleteBy = clock.instant().plus(DELETION_GRACE_PERIOD).epochSecond

    var newlyMarked = 0
    // Cleanup runawayMarkers of containers that are no longer running
    runawayMarkers.keys.retainAll(activeContainersByAutoId.keys)

    for (autoId in autoIdsToMark) {
      if (!runawayMarkers.containsKey(autoId)) {
        val container = activeContainersByAutoId[autoId]
        val name = container?.names?.firstOrNull() ?: container?.id
        logger.info { "Marking Docker container $name (autoId: $autoId) for deletion by $deleteBy" }
        runawayMarkers[autoId] = deleteBy
        newlyMarked++
      }
    }

    logger.info {
      "Mark summary (Docker): active_workloads:${activeWorkloadsByAutoId.size}, running_containers:${activeContainersByAutoId.size}, " +
        "runaway:${autoIdsToMark.size} newly_marked:$newlyMarked"
    }
  }

  @Instrument(
    start = "WORKLOAD_LAUNCHER_CRON",
    duration = "WORKLOAD_LAUNCHER_CRON_DURATION",
    tags = [Tag(key = MetricTags.CRON_TYPE, value = "runaway_container_sweep")],
  )
  @Scheduled(cron = "40 * * * *")
  open fun sweep() {
    dataplaneId?.let { sweep(it) }
  }

  @InternalForTesting
  internal fun sweep(dataplaneId: UUID) {
    logger.info { "Sweeping for runaway Docker containers" }

    val currentEpochSeconds = clock.instant().epochSecond

    val containers = dockerClient.listContainersCmd()
      .withShowAll(true)
      .withLabelFilter(mapOf("airbyte" to "job-pod"))
      .exec()

    for (container in containers) {
      val autoId = container.labels?.get("auto_id") ?: continue
      val workloadId = container.labels?.get("workload_id")
      val deleteBy = runawayMarkers[autoId]

      if (deleteBy != null && deleteBy <= currentEpochSeconds) {
        if (containerLauncher.deleteContainerAndResources(container.id, workloadId, autoId)) {
          metricClient.count(metric = OssMetricsRegistry.WORKLOAD_RUNAWAY_POD_DELETED, value = 1)
          runawayMarkers.remove(autoId)
          logger.info { "Swept runaway container ${container.id} (autoId=$autoId)" }
        }
      }
    }
  }

  companion object {
    val DELETION_GRACE_PERIOD = 1.days.toJavaDuration()
  }
}

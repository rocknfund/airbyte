/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.launcher

import com.github.dockerjava.api.DockerClient
import com.github.dockerjava.api.model.Container
import io.airbyte.metrics.MetricAttribute
import io.airbyte.metrics.MetricClient
import io.airbyte.metrics.OssMetricsRegistry
import io.airbyte.metrics.annotations.Instrument
import io.airbyte.metrics.annotations.Tag
import io.airbyte.metrics.lib.MetricTags
import io.airbyte.micronaut.runtime.AirbytePodSweeperConfig
import io.airbyte.micronaut.runtime.POD_SWEEPER_PREFIX
import io.airbyte.workload.launcher.containers.DockerContainerLauncher
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.context.annotation.Requires
import io.micronaut.scheduling.annotation.Scheduled
import jakarta.inject.Singleton
import java.time.Clock
import java.time.Instant
import kotlin.time.Duration.Companion.minutes
import kotlin.time.toJavaDuration

private val logger = KotlinLogging.logger {}

/**
 * Docker equivalent of PodSweeper.
 * Sweeps Docker containers (labeled with airbyte=job-pod) older than certain TTLs,
 * along with their associated networks and volumes.
 */
@Singleton
@Requires(property = "worker.environment", value = "docker")
open class ContainerSweeper(
  private val dockerClient: DockerClient,
  private val containerLauncher: DockerContainerLauncher,
  private val metricClient: MetricClient,
  private val clock: Clock,
  private val airbytePodSweeperConfig: AirbytePodSweeperConfig,
) {
  @Instrument(
    start = "WORKLOAD_LAUNCHER_CRON",
    duration = "WORKLOAD_LAUNCHER_CRON_DURATION",
    tags = [Tag(key = MetricTags.CRON_TYPE, value = "container_sweeper")],
  )
  @Scheduled(fixedRate = "\${$POD_SWEEPER_PREFIX.rate}")
  open fun sweepContainers() {
    logger.info { "Starting Docker container sweeper cycle..." }

    val now = clock.instant()
    val runningCutoff = if (airbytePodSweeperConfig.runningTtl > 0) now.minus(airbytePodSweeperConfig.runningTtl.minutes.toJavaDuration()) else null
    val succeededCutoff = if (airbytePodSweeperConfig.succeededTtl > 0) now.minus(airbytePodSweeperConfig.succeededTtl.minutes.toJavaDuration()) else null
    val unsuccessfulCutoff = if (airbytePodSweeperConfig.unsuccessfulTtl > 0) now.minus(airbytePodSweeperConfig.unsuccessfulTtl.minutes.toJavaDuration()) else null

    runningCutoff?.let { logger.info { "Will sweep running containers older than $it (UTC)." } }
    succeededCutoff?.let { logger.info { "Will sweep succeeded (exited 0) containers older than $it (UTC)." } }
    unsuccessfulCutoff?.let { logger.info { "Will sweep unsuccessful (exited non-0) containers older than $it (UTC)." } }

    val containers = dockerClient.listContainersCmd()
      .withShowAll(true)
      .withLabelFilter(mapOf("airbyte" to "job-pod"))
      .exec()

    for (container in containers) {
      val state = container.state ?: "unknown"
      
      // We inspect to get Start/Finish times
      val containerInfo = try {
        dockerClient.inspectContainerCmd(container.id).exec()
      } catch (e: Exception) {
        logger.warn { "Failed to inspect container ${container.id}, skipping..." }
        continue
      }
      
      val startTimeStr = containerInfo.state?.startedAt
      val finishTimeStr = containerInfo.state?.finishedAt
      
      val containerInstant = when {
        !finishTimeStr.isNullOrBlank() && finishTimeStr != "0001-01-01T00:00:00Z" -> parseDockerDate(finishTimeStr)
        !startTimeStr.isNullOrBlank() && startTimeStr != "0001-01-01T00:00:00Z" -> parseDockerDate(startTimeStr)
        else -> null
      }

      if (containerInstant == null) {
        continue
      }

      val isRunning = state.lowercase() == "running"
      val isExited = state.lowercase() == "exited"
      val exitCode = containerInfo.state?.exitCodeLong ?: -1L

      val labels = container.labels ?: emptyMap()
      val workloadId = labels["workload_id"]
      val autoId = labels["auto_id"]

      if (isRunning) {
        if (runningCutoff != null && containerInstant.isBefore(runningCutoff)) {
          deleteContainer(container, "running", "Running since $containerInstant", workloadId, autoId)
        }
      } else if (isExited && exitCode == 0L) {
        if (succeededCutoff != null && containerInstant.isBefore(succeededCutoff)) {
          deleteContainer(container, "succeeded", "Succeeded since $containerInstant", workloadId, autoId)
        }
      } else {
        if (unsuccessfulCutoff != null && containerInstant.isBefore(unsuccessfulCutoff)) {
          deleteContainer(container, "unsuccessful", "Unsuccessful (exit=$exitCode) since $containerInstant", workloadId, autoId)
        }
      }
    }
    
    logger.info { "Completed Docker container sweeper cycle." }
  }

  private fun parseDockerDate(dateStr: String): Instant? =
    try {
        Instant.parse(dateStr)
    } catch (e: Exception) {
        logger.error(e) { "Error parsing Docker date [$dateStr]" }
        null
    }

  private fun deleteContainer(
    container: Container,
    phase: String,
    reason: String,
    workloadId: String?,
    autoId: String?
  ) {
    if (containerLauncher.deleteContainerAndResources(container.id, workloadId, autoId)) {
        metricClient.count(metric = OssMetricsRegistry.WORKLOAD_LAUNCHER_POD_SWEEPER_COUNT, attributes = arrayOf(MetricAttribute("phase", phase)))
    }
  }
}

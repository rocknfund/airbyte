/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.launcher.containers.stages

import io.airbyte.metrics.MetricAttribute
import io.airbyte.metrics.MetricClient
import io.airbyte.metrics.OssMetricsRegistry
import io.airbyte.metrics.annotations.Instrument
import io.airbyte.metrics.annotations.Tag
import io.airbyte.metrics.lib.MetricTags
import io.airbyte.workload.launcher.Launcher
import io.airbyte.workload.launcher.metrics.MeterFilterFactory
import io.airbyte.workload.launcher.pipeline.stages.StageName
import io.airbyte.workload.launcher.pipeline.stages.model.LaunchStage
import io.airbyte.workload.launcher.pipeline.stages.model.LaunchStageIO
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.context.annotation.Requires
import io.opentelemetry.api.trace.Span
import io.opentelemetry.instrumentation.annotations.WithSpan
import jakarta.inject.Named
import jakarta.inject.Singleton
import reactor.core.publisher.Mono

private val logger = KotlinLogging.logger {}

/**
 * Docker-specific mutex enforcement stage.
 * Kills any containers running for the supplied mutex key.
 * Mirrors pipeline/stages/EnforceMutexStage for Kubernetes.
 */
@Singleton
@Named("mutex")
@Requires(env = ["docker"])
open class EnforceMutexStage(
  private val launcher: Launcher,
  metricClient: MetricClient,
) : LaunchStage(metricClient) {
  @WithSpan(MeterFilterFactory.LAUNCH_PIPELINE_STAGE_OPERATION_NAME)
  @Instrument(
    start = "WORKLOAD_STAGE_START",
    end = "WORKLOAD_STAGE_DONE",
    tags = [Tag(key = MetricTags.STAGE_NAME_TAG, value = "container_mutex")],
  )
  override fun apply(input: LaunchStageIO): Mono<LaunchStageIO> {
    Span.current().setAttribute("resource.name", "EnforceContainerMutexStage")
    return super.apply(input)
  }

  override fun applyStage(input: LaunchStageIO): LaunchStageIO {
    val workloadId = input.msg.workloadId
    val key = input.msg.mutexKey

    if (key == null) {
      logger.info { "No mutex key specified for workload: $workloadId. Continuing..." }
      return input
    }

    logger.info { "Mutex key: $key specified for workload: $workloadId. Checking for existing containers..." }

    val deleted = launcher.deleteMutexWorkload(key)
    if (deleted) {
      logger.info { "Existing containers for mutex key: $key removed." }
      metricClient.count(
        metric = OssMetricsRegistry.PODS_DELETED_FOR_MUTEX_KEY,
        attributes =
          arrayOf(
            MetricAttribute(MetricTags.WORKLOAD_TYPE_TAG, input.msg.workloadType.toString()),
            MetricAttribute(MetricTags.MUTEX_KEY_TAG, key),
          ),
      )
    } else {
      logger.info { "No existing containers found for mutex key: $key. Continuing..." }
    }

    return input
  }

  override fun getStageName(): StageName = StageName.MUTEX
}

/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.launcher.containers.stages

import io.airbyte.metrics.MetricClient
import io.airbyte.metrics.annotations.Instrument
import io.airbyte.metrics.annotations.Tag
import io.airbyte.metrics.lib.MetricTags
import io.airbyte.workload.launcher.Launcher
import io.airbyte.workload.launcher.metrics.MeterFilterFactory
import io.airbyte.workload.launcher.pipeline.stages.StageName
import io.airbyte.workload.launcher.pipeline.stages.model.CheckPayload
import io.airbyte.workload.launcher.pipeline.stages.model.DiscoverCatalogPayload
import io.airbyte.workload.launcher.pipeline.stages.model.LaunchStage
import io.airbyte.workload.launcher.pipeline.stages.model.LaunchStageIO
import io.airbyte.workload.launcher.pipeline.stages.model.SpecPayload
import io.airbyte.workload.launcher.pipeline.stages.model.SyncPayload
import io.micronaut.context.annotation.Requires
import io.opentelemetry.api.trace.Span
import io.opentelemetry.instrumentation.annotations.WithSpan
import jakarta.inject.Named
import jakarta.inject.Singleton
import reactor.core.publisher.Mono

/**
 * Docker-specific launch stage.
 * Launches containers for workloads using DockerContainerClient via WorkloadLauncher.
 * Mirrors pipeline/stages/LaunchPodStage for Kubernetes.
 */
@Singleton
@Named("launch")
@Requires(env = ["docker"])
open class LaunchContainerStage(
  private val launcher: Launcher,
  metricClient: MetricClient,
) : LaunchStage(metricClient) {
  @WithSpan(MeterFilterFactory.LAUNCH_PIPELINE_STAGE_OPERATION_NAME)
  @Instrument(
    start = "WORKLOAD_STAGE_START",
    end = "WORKLOAD_STAGE_DONE",
    tags = [Tag(key = MetricTags.STAGE_NAME_TAG, value = "launch_container")],
  )
  override fun apply(input: LaunchStageIO): Mono<LaunchStageIO> {
    Span.current().setAttribute("resource.name", "LaunchContainerStage")
    return super.apply(input)
  }

  override fun applyStage(input: LaunchStageIO): LaunchStageIO {
    when (val payload = input.payload!!) {
      is SyncPayload ->
        if (payload.input.isReset) {
          launcher.launchReset(payload, input.msg)
        } else {
          launcher.launchReplication(payload, input.msg)
        }
      is CheckPayload -> launcher.launchCheck(payload.input, input.msg)
      is DiscoverCatalogPayload -> launcher.launchDiscover(payload.input, input.msg)
      is SpecPayload -> launcher.launchSpec(payload.input, input.msg)
    }

    return input
  }

  override fun getStageName(): StageName = StageName.LAUNCH
}

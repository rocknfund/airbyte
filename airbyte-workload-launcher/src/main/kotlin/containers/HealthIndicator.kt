/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.launcher.containers

import com.github.dockerjava.api.DockerClient
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.context.annotation.Requires
import io.micronaut.health.HealthStatus
import io.micronaut.management.health.indicator.HealthIndicator
import io.micronaut.management.health.indicator.HealthResult
import jakarta.inject.Singleton
import org.reactivestreams.Publisher
import reactor.core.publisher.Mono

private val logger = KotlinLogging.logger {}

/**
 * Health indicator for the Docker daemon.
 * Reports UP if dockerClient.pingCmd() succeeds, DOWN otherwise.
 * Registered with Micronaut's /health endpoint.
 */
@Singleton
@Requires(property = "worker.environment", value = "docker")
class HealthIndicator(
  private val dockerClient: DockerClient,
) : HealthIndicator {
  override fun getResult(): Publisher<HealthResult> =
    Mono.fromCallable {
      try {
        dockerClient.pingCmd().exec()
        HealthResult
          .builder("docker")
          .status(HealthStatus.UP)
          .build()
      } catch (e: Exception) {
        logger.error(e) { "Docker health check failed" }
        HealthResult
          .builder("docker")
          .status(HealthStatus.DOWN)
          .details(mapOf("error" to (e.message ?: "Unknown error")))
          .build()
      }
    }
}

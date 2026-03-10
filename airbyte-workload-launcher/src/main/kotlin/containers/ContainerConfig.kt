/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.launcher.containers

import io.micronaut.context.annotation.ConfigurationProperties
import io.micronaut.context.annotation.Requires
import jakarta.inject.Singleton

/**
 * Configuration properties for Docker container launcher.
 * Read from application.yml under airbyte.workload.launcher.docker.
 */
@Singleton
@Requires(property = "worker.environment", value = "docker")
@ConfigurationProperties("airbyte.workload.launcher.docker")
class ContainerConfig {
  var host: String = "unix:///var/run/docker.sock"
  var network: String = "airbyte_default"
  var maxConcurrentSyncs: Int = 4
  var workspaceMount: String = "airbyte_workspace"
  var localMount: String = "airbyte_data"
}

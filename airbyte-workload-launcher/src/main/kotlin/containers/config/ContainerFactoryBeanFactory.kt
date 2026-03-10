/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.launcher.containers.config

import io.micronaut.context.annotation.Factory
import io.micronaut.context.annotation.Requires

/**
 * Micronaut bean factory for Docker container factories.
 * Mirrors config/PodFactoryBeanFactory for Kubernetes.
 *
 * In Docker mode, containers are created directly by DockerContainerLauncher
 * rather than through pod factories. The container creation logic lives in
 * DockerContainerClient (for orchestration) and DockerContainerLauncher
 * (for low-level Docker API calls).
 *
 * Docker does not need separate factory beans for check/discover/spec
 * because all container configuration (env vars, mounts, images) is
 * handled inline in the DockerContainerClient methods.
 */
@Factory
@Requires(env = ["docker"])
class ContainerFactoryBeanFactory {
  // Docker containers are created directly via DockerContainerLauncher.createAndStartContainer()
  // rather than through factory beans.
  //
  // K8s uses separate PodFactory beans because K8s pods have complex configuration
  // (tolerations, node selectors, init containers, sidecars, resource limits).
  //
  // Docker containers are simpler: image + env vars + volume mounts.
  // This configuration is handled in DockerContainerClient and DockerContainerLauncher.
}

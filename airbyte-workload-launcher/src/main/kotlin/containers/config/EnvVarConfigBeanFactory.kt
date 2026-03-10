/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.launcher.containers.config

import io.micronaut.context.annotation.Factory
import io.micronaut.context.annotation.Requires

/**
 * Micronaut bean factory for Docker environment variable configuration.
 * Mirrors config/EnvVarConfigBeanFactory for Kubernetes.
 *
 * In Docker mode, environment variables are passed directly as key=value strings
 * (not K8s EnvVar objects). The DockerContainerLauncher handles merging env vars
 * with defaults in createAndStartContainer().
 *
 * The existing @Named env var maps (apiClientEnvMap, featureFlagEnvMap, etc.)
 * from the shared config are technology-neutral Map<String, String> and are
 * reused directly without modification.
 */
@Factory
@Requires(property = "worker.environment", value = "docker")
class EnvVarConfigBeanFactory {
  // Docker containers receive env vars directly via DockerContainerLauncher.createAndStartContainer().
  // The env var maps created by the shared EnvVarConfigBeanFactory are already
  // Map<String, String> (technology-neutral) and are used as-is.
  //
  // Docker-specific env vars (STORAGE_TYPE, WORKSPACE_ROOT, etc.) are configured
  // as defaults in DockerContainerLauncher.createAndStartContainer().
  //
  // Additional Docker-specific env var configuration can be added here as needed.
}

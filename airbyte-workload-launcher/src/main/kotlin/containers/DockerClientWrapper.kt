/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.launcher.containers

import com.github.dockerjava.api.DockerClient
import com.github.dockerjava.core.DefaultDockerClientConfig
import com.github.dockerjava.core.DockerClientImpl
import com.github.dockerjava.zerodep.ZerodepDockerHttpClient
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.context.annotation.Factory
import io.micronaut.context.annotation.Requires
import jakarta.inject.Singleton

private val logger = KotlinLogging.logger {}

/**
 * Factory for creating the Docker client.
 * Mirrors KubernetesClientWrapper for Kubernetes.
 */
@Factory
@Requires(property = "worker.environment", value = "docker")
class DockerClientWrapper {
  @Singleton
  fun dockerClient(config: ContainerConfig): DockerClient {
    val dockerConfigBuilder = DefaultDockerClientConfig.createDefaultConfigBuilder()

    if (config.host != "unix:///var/run/docker.sock" || System.getenv("DOCKER_HOST") == null) {
      dockerConfigBuilder.withDockerHost(config.host)
    }

    val dockerConfig = dockerConfigBuilder.build()

    logger.info { "Creating DockerClient with Zerodep transport. Host: ${dockerConfig.dockerHost}" }

    val httpClient =
      ZerodepDockerHttpClient
        .Builder()
        .dockerHost(dockerConfig.dockerHost)
        .sslConfig(dockerConfig.sslConfig)
        .build()

    return DockerClientImpl.getInstance(dockerConfig, httpClient)
  }
}

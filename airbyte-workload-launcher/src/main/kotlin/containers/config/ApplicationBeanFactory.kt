/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.launcher.containers.config

import io.airbyte.commons.storage.StorageClient
import io.airbyte.commons.storage.DocumentType
import io.airbyte.workers.internal.AirbyteStreamFactory
import io.airbyte.micronaut.runtime.StorageType
import io.micronaut.context.annotation.Factory
import io.micronaut.context.annotation.Requires
import jakarta.inject.Named
import jakarta.inject.Singleton
import kotlin.time.Duration.Companion.seconds
import kotlin.time.toJavaDuration

/**
 * Micronaut bean factory for general Docker application beans.
 * Mirrors config/ApplicationBeanFactory for Kubernetes.
 *
 * Docker does not need K8s-specific retry policies or HTTP error predicates.
 * Only provides generic application-level beans needed by the launcher pipeline.
 */
@Factory
@Requires(env = ["docker"])
class ApplicationBeanFactory {
  @Singleton
  @Named("outputDocumentStore")
  fun dummyOutputDocumentStore(): StorageClient {
    return object : StorageClient {
      override val documentType: DocumentType = DocumentType.WORKLOAD_OUTPUT
      override val storageType: StorageType = StorageType.LOCAL
      override val bucketName: String = "dummy-bucket"
      override fun write(id: String, content: String) {}
      override fun read(id: String): String? = null
      override fun delete(id: String): Boolean = true
      override fun list(id: String): List<String> = emptyList()
    }
  }

  @Singleton
  fun dummyAirbyteStreamFactory(): AirbyteStreamFactory = object : AirbyteStreamFactory {
    override fun create(bufferedReader: java.io.BufferedReader, origin: io.airbyte.workers.internal.MessageOrigin): java.util.stream.Stream<io.airbyte.protocol.models.v0.AirbyteMessage> {
      return java.util.stream.Stream.empty()
    }
  }

  @Singleton
  @Named("claimedProcessorBackoffDuration")
  fun claimedProcessorBackoffDuration() = 5.seconds.toJavaDuration()

  @Singleton
  @Named("claimedProcessorBackoffMaxDelay")
  fun claimedProcessorBackoffMaxDelay() = 60.seconds.toJavaDuration()
}

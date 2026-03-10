/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.launcher.config

import dev.failsafe.RetryPolicy
import io.airbyte.metrics.MetricAttribute
import io.airbyte.metrics.MetricClient
import io.airbyte.metrics.OssMetricsRegistry
import io.airbyte.micronaut.runtime.AirbyteKubernetesConfig
import io.fabric8.kubernetes.client.KubernetesClientTimeoutException
import io.micronaut.context.annotation.Factory
import io.micronaut.context.annotation.Requires
import jakarta.inject.Named
import jakarta.inject.Singleton
import okhttp3.internal.http2.StreamResetException
import java.io.IOException
import java.net.SocketTimeoutException
import java.time.Duration
import kotlin.time.Duration.Companion.seconds
import kotlin.time.toJavaDuration

import io.airbyte.commons.storage.StorageClient
import io.airbyte.commons.storage.DocumentType
import java.util.stream.Stream

/**
 * Micronaut bean factory for general application beans.
 */
@Factory
@Requires(notEnv = ["docker"])
class ApplicationBeanFactory {

  @Singleton
  @Named("outputDocumentStore")
  fun dummyOutputDocumentStore(): StorageClient {
    return object : StorageClient {
      override val documentType: DocumentType = DocumentType.WORKLOAD_OUTPUT
      override val storageType: io.airbyte.micronaut.runtime.StorageType = io.airbyte.micronaut.runtime.StorageType.LOCAL
      override val bucketName: String = "dummy-bucket"
      override fun write(id: String, content: String) {}
      override fun read(id: String): String? = null
      override fun delete(id: String): Boolean = true
      override fun list(id: String): List<String> = emptyList()
    }
  }
  @Singleton
  @Named("kubeHttpErrorRetryPredicate")
  fun kubeHttpErrorRetryPredicate(): (Throwable) -> Boolean =
    { e: Throwable ->
      e is KubernetesClientTimeoutException ||
        e.cause is SocketTimeoutException ||
        e.cause?.cause is StreamResetException ||
        (e.cause is IOException && e.cause?.message == "timeout")
    }

  @Singleton
  @Named("kubernetesClientRetryPolicy")
  fun kubernetesClientRetryPolicy(
    airbyteKubernetesConfig: AirbyteKubernetesConfig,
    @Named("kubeHttpErrorRetryPredicate") predicate: (Throwable) -> Boolean,
    metricClient: MetricClient,
  ): RetryPolicy<Any> =
    RetryPolicy
      .builder<Any>()
      .handleIf(predicate)
      .onRetry { l ->
        metricClient.count(
          metric = OssMetricsRegistry.WORKLOAD_LAUNCHER_KUBE_API_CLIENT_RETRY,
          attributes =
            arrayOf(
              MetricAttribute(
                "max_retries",
                airbyteKubernetesConfig.client.retries.max
                  .toString(),
              ),
              MetricAttribute("retry_attempt", l.attemptCount.toString()),
              l.lastException.message?.let { m ->
                MetricAttribute("exception_message", m)
              },
              MetricAttribute("exception_type", l.lastException.javaClass.name),
            ),
        )
      }.onAbort { l ->
        metricClient.count(
          metric = OssMetricsRegistry.WORKLOAD_LAUNCHER_KUBE_API_CLIENT_ABORT,
          attributes =
            arrayOf(
              MetricAttribute(
                "max_retries",
                airbyteKubernetesConfig.client.retries.max
                  .toString(),
              ),
              MetricAttribute("retry_attempt", l.attemptCount.toString()),
            ),
        )
      }.onFailedAttempt { l ->
        metricClient.count(
          metric = OssMetricsRegistry.WORKLOAD_LAUNCHER_KUBE_API_CLIENT_FAILED,
          attributes =
            arrayOf(
              MetricAttribute(
                "max_retries",
                airbyteKubernetesConfig.client.retries.max
                  .toString(),
              ),
              MetricAttribute("retry_attempt", l.attemptCount.toString()),
            ),
        )
      }.onSuccess { l ->
        metricClient.count(
          metric = OssMetricsRegistry.WORKLOAD_LAUNCHER_KUBE_API_CLIENT_SUCCESS,
          attributes =
            arrayOf(
              MetricAttribute(
                "max_retries",
                airbyteKubernetesConfig.client.retries.max
                  .toString(),
              ),
              MetricAttribute("retry_attempt", l.attemptCount.toString()),
            ),
        )
      }.withDelay(Duration.ofSeconds(airbyteKubernetesConfig.client.retries.delaySeconds))
      .withMaxRetries(airbyteKubernetesConfig.client.retries.max)
      .build()

  @Singleton
  @Named("claimedProcessorBackoffDuration")
  fun claimedProcessorBackoffDuration() = 5.seconds.toJavaDuration()

  @Singleton
  @Named("claimedProcessorBackoffMaxDelay")
  fun claimedProcessorBackoffMaxDelay() = 60.seconds.toJavaDuration()
}

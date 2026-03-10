/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

@file:Suppress("ktlint:standard:max-line-length")

package io.airbyte.workload.launcher.containers

import io.airbyte.api.client.AirbyteApiClient
import io.airbyte.api.client.model.generated.ConnectionIdRequestBody
import io.airbyte.commons.converters.CatalogClientConverters
import io.airbyte.commons.json.Jsons
import io.airbyte.config.Configs
import io.airbyte.config.ConfiguredAirbyteCatalog
import io.airbyte.config.ConnectorJobOutput
import io.airbyte.config.DestinationSyncMode
import io.airbyte.config.WorkloadType
import io.airbyte.config.secrets.SecretsRepositoryReader
import io.airbyte.micronaut.runtime.AirbyteConfig
import io.airbyte.micronaut.runtime.AirbyteContainerOrchestratorConfig
import io.airbyte.persistence.job.models.ReplicationInput
import io.airbyte.protocol.models.v0.AirbyteMessage

import io.airbyte.workers.internal.MessageOrigin
import io.airbyte.workers.models.CheckConnectionInput
import io.airbyte.workers.models.DiscoverCatalogInput
import io.airbyte.workers.models.SpecInput
import io.airbyte.workers.pod.FileConstants
import io.airbyte.workers.serde.PayloadDeserializer

import io.airbyte.workload.launcher.Launcher
import io.airbyte.workload.launcher.client.WorkloadApiClient
import io.airbyte.workload.launcher.containers.factories.ContainerCommandFactory
import io.airbyte.workload.launcher.pipeline.consumer.LauncherInput
import io.airbyte.workload.launcher.pipeline.stages.model.SyncPayload
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.context.annotation.Requires
import jakarta.inject.Named
import jakarta.inject.Singleton
import java.io.File
import java.util.UUID
import java.util.concurrent.Semaphore
import java.util.concurrent.TimeUnit

private val logger = KotlinLogging.logger {}

/**
 * High-level Docker container orchestration client.
 * Mirrors KubePodClient for Kubernetes — composes low-level DockerContainerLauncher
 * operations into business-level workload operations.
 *
 * Implements WorkloadLauncher interface for the hybrid architecture.
 */
@Singleton
@Requires(env = ["docker"])
class DockerContainerClient(
  private val containerLauncher: DockerContainerLauncher,
  private val labeler: ContainerLabeler,
  private val config: ContainerConfig,
  private val apiClient: WorkloadApiClient,
  private val deserializer: PayloadDeserializer,
  private val secretsRepositoryReader: SecretsRepositoryReader,


  private val catalogClientConverters: CatalogClientConverters,
  @param:Named("containerOrchestratorImage") private val orchestratorImage: String,
  private val airbyteApiClient: AirbyteApiClient,
  private val airbyteConfig: AirbyteConfig,
  private val airbyteContainerOrchestratorConfig: AirbyteContainerOrchestratorConfig,
) : Launcher {
  private val syncSemaphore = Semaphore(config.maxConcurrentSyncs)

  override fun workloadRunning(autoId: UUID): Boolean {
    val labels = labeler.getAutoIdLabels(autoId)
    return containerLauncher.containersRunning(labels)
  }

  override fun deleteMutexWorkload(mutexKey: String): Boolean {
    val labels = labeler.getMutexLabels(mutexKey)
    return containerLauncher.removeContainersByLabels(labels)
  }

  override fun launchReplication(
    payload: SyncPayload,
    launcherInput: LauncherInput,
  ) {
    val replicationInput = payload.input
    val workloadId = launcherInput.workloadId
    val autoId = launcherInput.autoId

    logger.info { "Requesting replication launch for workload $workloadId (autoId: $autoId). Available permits: ${syncSemaphore.availablePermits()}" }

    if (!syncSemaphore.tryAcquire(5, TimeUnit.MINUTES)) {
      throw IllegalStateException(
        "Failed to acquire permit to launch workload $workloadId within timeout. " +
          "Currently running syncs: ${config.maxConcurrentSyncs}",
      )
    }
    logger.info { "Acquired permit for $workloadId. Commencing launch sequence." }
    var networkId: String? = null
    val createdContainerIds = mutableListOf<String>()
    val workDir = File("/tmp/workloads/$workloadId")

    try {
      logger.info { "Launching replication for workload $workloadId (autoId: $autoId) in Docker" }

      val labels =
        labeler.getSharedLabels(
          workloadId = workloadId,
          mutexKey = launcherInput.mutexKey,
          passThroughLabels = launcherInput.labels,
          autoId = autoId,
          workspaceId = replicationInput.workspaceId,
          networkSecurityTokens = replicationInput.networkSecurityTokens,
        )

      val networkName = "airbyte-workload-$workloadId"
      val sourceDir = File(workDir, "source")
      val destDir = File(workDir, "dest")
      sourceDir.mkdirs()
      destDir.mkdirs()

      // Create named pipes for source and destination
      listOf(sourceDir, destDir).forEach { dir ->
        listOf(FileConstants.STDIN_PIPE_FILE, FileConstants.STDOUT_PIPE_FILE, FileConstants.STDERR_PIPE_FILE).forEach { pipe ->
          val pipeFile = File(dir, pipe)
          if (!pipeFile.exists()) {
            logger.info { "Creating named pipe: ${pipeFile.absolutePath}" }
            ProcessBuilder("mkfifo", pipeFile.absolutePath).start().waitFor()
          }
        }
      }

      val catalog = getAndPatchCatalog(replicationInput)

      // Hydrate secrets for source and destination configs
      val sourceConfig = secretsRepositoryReader.hydrateConfigFromDefaultSecretPersistence(replicationInput.sourceConfiguration)
      val destConfig = secretsRepositoryReader.hydrateConfigFromDefaultSecretPersistence(replicationInput.destinationConfiguration)

      replicationInput.catalog = catalog

      // 1. Write ORCHESTRATOR input
      File(workDir, "input.json").writeText(Jsons.serialize(replicationInput as Any?))

      // 2. Write SOURCE files
      File(sourceDir, "connectorConfig.json").writeText(Jsons.serialize(sourceConfig as Any?))
      File(sourceDir, "catalog.json").writeText(Jsons.serialize(catalog as Any?))
      replicationInput.state?.let { state ->
        File(sourceDir, "inputState.json").writeText(Jsons.serialize(state as Any?))
      }

      // 3. Write DESTINATION files
      File(destDir, "connectorConfig.json").writeText(Jsons.serialize(destConfig as Any?))
      // Clone and patch catalog for destination to use null namespaces
      // This allows the destination to match records from source which often emits null namespace in Docker
      val destCatalog = Jsons.deserialize(Jsons.serialize(catalog), ConfiguredAirbyteCatalog::class.java)
      destCatalog.streams.forEach { it.stream?.namespace = null }
      File(destDir, "catalog.json").writeText(Jsons.serialize(destCatalog as Any?))

      logger.info { "Catalog written: ${catalog.streams.size} streams, workDir=$workDir" }

      // Create isolated network
      networkId = containerLauncher.createNetwork(networkName)

      val javaOpts = airbyteContainerOrchestratorConfig.javaOpts.ifBlank { "-XX:+ExitOnOutOfMemoryError" }
      val deploymentMode = if (airbyteConfig.edition == Configs.AirbyteEdition.CLOUD) "CLOUD" else "OSS"
      val useFileTransfer = replicationInput.useFileTransfer == true

      // Start Orchestrator
      val orchEnvVars =
        mutableListOf(
          "OPERATION_TYPE=SYNC",
          "WORKLOAD_ID=$workloadId",
          "CONNECTION_ID=${replicationInput.connectionId}",
          "WORKSPACE_ID=${replicationInput.workspaceId}",
          "JOB_ID=${replicationInput.jobRunConfig?.jobId ?: ""}",
          "ATTEMPT_ID=${replicationInput.jobRunConfig?.attemptId ?: ""}",
          "USE_FILE_TRANSFER=$useFileTransfer",
          "DEPLOYMENT_MODE=$deploymentMode",
          "JAVA_OPTS=$javaOpts",
        )

      val orchContainerId =
        containerLauncher.createAndStartContainer(
          image = orchestratorImage,
          entrypoint = "sh",
          args = listOf("-c", ContainerCommandFactory.orchestrator()),
          name = "orchestrator",
          networkName = networkName,
          workDir = workDir,
          labels = labels,
          extraEnv = orchEnvVars,
        )
      createdContainerIds.add(orchContainerId)

      // Start Source
      val sourceEnvVars = listOf("DEPLOYMENT_MODE=$deploymentMode", "JAVA_OPTS=$javaOpts")
      val sourceContainerId =
        containerLauncher.createAndStartContainer(
          image = replicationInput.sourceLauncherConfig.dockerImage,
          entrypoint = "sh",
          args = listOf("-c", ContainerCommandFactory.source()),
          name = "source",
          networkName = networkName,
          workDir = workDir,
          labels = labels,
          extraEnv = sourceEnvVars,
        )
      createdContainerIds.add(sourceContainerId)

      // Start Destination
      val destEnvVars = listOf("DEPLOYMENT_MODE=$deploymentMode", "JAVA_OPTS=$javaOpts")
      val destContainerId =
        containerLauncher.createAndStartContainer(
          image = replicationInput.destinationLauncherConfig.dockerImage,
          entrypoint = "sh",
          args = listOf("-c", ContainerCommandFactory.destination()),
          name = "destination",
          networkName = networkName,
          workDir = workDir,
          labels = labels,
          extraEnv = destEnvVars,
        )
      createdContainerIds.add(destContainerId)

      // Start log aggregation
      val jobId = replicationInput.jobRunConfig?.jobId ?: "0"
      val attemptId = replicationInput.jobRunConfig?.attemptId ?: "0"
      val legacyLogPath = "/tmp/airbyte_local/airbyte-bucket/$jobId/$attemptId/logs.log"

      containerLauncher.aggregateLogs(
        workloadId,
        legacyLogPath,
        listOf(
          orchContainerId to "orchestrator",
          sourceContainerId to "source",
          destContainerId to "destination",
        ),
      )

      // Wait for orchestrator in background (managed async)
      java.util.concurrent.CompletableFuture.runAsync {
        try {
          val exitCode = containerLauncher.waitForContainer(orchContainerId)
          logger.info { "Orchestrator for $workloadId exited with code $exitCode" }

          if (exitCode != 0) {
            reportStatus(workloadId, false, "Orchestrator exited with non-zero code: $exitCode")
          }
        } catch (e: Exception) {
          logger.error(e) { "Error during workload $workloadId background wait" }
          reportStatus(workloadId, false, e.message ?: "Background wait failed")
        } finally {
          containerLauncher.cleanupWorkload(workloadId, networkId, createdContainerIds, workDir)
          syncSemaphore.release()
          logger.info { "Released sync permit for $workloadId. Available permits: ${syncSemaphore.availablePermits()}" }
        }
      }
    } catch (e: Exception) {
      logger.error(e) { "Failed to launch replication workload $workloadId. Cleaning up." }
      containerLauncher.cleanupWorkload(workloadId, networkId, createdContainerIds, workDir)
      syncSemaphore.release()
      reportStatus(workloadId, false, e.message ?: "Unknown error")
    }
  }

  override fun launchReset(
    payload: SyncPayload,
    launcherInput: LauncherInput,
  ) {
    val replicationInput = payload.input
    val workloadId = launcherInput.workloadId
    val autoId = launcherInput.autoId
    logger.info { "Launching reset for workload $workloadId (autoId: $autoId) in Docker" }

    val labels =
      labeler.getSharedLabels(
        workloadId = workloadId,
        mutexKey = launcherInput.mutexKey,
        passThroughLabels = launcherInput.labels,
        autoId = autoId,
        workspaceId = replicationInput.workspaceId,
        networkSecurityTokens = replicationInput.networkSecurityTokens,
      )

    val networkName = "airbyte-workload-$workloadId"
    val workDir = File("/tmp/workloads/$workloadId")
    val sourceDir = File(workDir, "source")
    val destDir = File(workDir, "dest")
    sourceDir.mkdirs()
    destDir.mkdirs()

    val networkId = containerLauncher.createNetwork(networkName)
    val createdContainerIds = mutableListOf<String>()

    try {
      val catalog = getAndPatchCatalog(replicationInput)

      File(workDir, "input.json").writeText(Jsons.serialize(replicationInput as Any?))

      val destConfig = replicationInput.destinationConfiguration
      secretsRepositoryReader.hydrateConfigFromDefaultSecretPersistence(destConfig)
      File(destDir, "connectorConfig.json").writeText(Jsons.serialize(destConfig as Any?))
      File(destDir, "catalog.json").writeText(Jsons.serialize(catalog as Any?))

      // Files are written synchronously above; no sleep needed

      val javaOpts = airbyteContainerOrchestratorConfig.javaOpts.ifBlank { "-XX:+ExitOnOutOfMemoryError" }
      val deploymentMode = if (airbyteConfig.edition == Configs.AirbyteEdition.CLOUD) "CLOUD" else "OSS"
      val useFileTransfer = replicationInput.useFileTransfer == true

      // Start Orchestrator
      val orchEnvVars =
        listOf(
          "OPERATION_TYPE=RESET",
          "WORKLOAD_ID=$workloadId",
          "CONNECTION_ID=${replicationInput.connectionId}",
          "WORKSPACE_ID=${replicationInput.workspaceId}",
          "JOB_ID=${replicationInput.jobRunConfig?.jobId ?: ""}",
          "ATTEMPT_ID=${replicationInput.jobRunConfig?.attemptId ?: ""}",
          "USE_FILE_TRANSFER=$useFileTransfer",
          "DEPLOYMENT_MODE=$deploymentMode",
          "JAVA_OPTS=$javaOpts",
        )

      val orchContainerId =
        containerLauncher.createAndStartContainer(
          image = orchestratorImage,
          entrypoint = "sh",
          args = listOf("-c", ContainerCommandFactory.orchestrator()),
          name = "orchestrator",
          networkName = networkName,
          workDir = workDir,
          labels = labels,
          extraEnv = orchEnvVars,
        )
      createdContainerIds.add(orchContainerId)

      // Start Destination
      val destEnvVars = listOf("DEPLOYMENT_MODE=$deploymentMode", "JAVA_OPTS=$javaOpts")
      val destContainerId =
        containerLauncher.createAndStartContainer(
          image = replicationInput.destinationLauncherConfig.dockerImage,
          entrypoint = "sh",
          args = listOf("-c", ContainerCommandFactory.destination()),
          name = "destination",
          networkName = networkName,
          workDir = workDir,
          labels = labels,
          extraEnv = destEnvVars,
        )
      createdContainerIds.add(destContainerId)

      // Log aggregation
      val jobId = replicationInput.jobRunConfig?.jobId ?: "0"
      val attemptId = replicationInput.jobRunConfig?.attemptId ?: "0"
      val legacyLogPath = "/tmp/airbyte_local/airbyte-bucket/$jobId/$attemptId/logs.log"

      containerLauncher.aggregateLogs(
        workloadId,
        legacyLogPath,
        listOf(
          orchContainerId to "orchestrator",
          destContainerId to "destination",
        ),
      )

      // Wait for orchestrator (managed async)
      java.util.concurrent.CompletableFuture.runAsync {
        try {
          val result = containerLauncher.waitForContainer(orchContainerId)
          logger.info { "Orchestrator for reset $workloadId exited with code $result" }
          containerLauncher.cleanupWorkload(workloadId, networkId, createdContainerIds, workDir)

          val jobOutputFile = File(workDir, FileConstants.JOB_OUTPUT_FILE)
          var finalStatusIsSuccess = (result == 0)
          if (jobOutputFile.exists()) {
            try {
              val jobOutput = Jsons.deserialize(jobOutputFile.readText(), ConnectorJobOutput::class.java)
              val request = io.airbyte.api.client.model.generated.WorkloadOutputWriteRequest(workloadId, Jsons.serialize(jobOutput))
              airbyteApiClient.workloadOutputApi.writeWorkloadOutput(request)
              finalStatusIsSuccess = true
            } catch (e: Exception) {
              logger.error(e) { "Could not parse job output for workload $workloadId" }
            }
          }

          reportStatus(workloadId, finalStatusIsSuccess, if (finalStatusIsSuccess) null else "Orchestrator exited with code $result")
        } catch (e: Exception) {
          logger.error(e) { "Error during workload $workloadId background wait/cleanup" }
        }
      }
    } catch (e: Exception) {
      logger.error(e) { "Failed to launch reset for $workloadId" }
      containerLauncher.cleanupWorkload(workloadId, networkId, createdContainerIds, workDir)
      apiClient.updateStatusToFailed(workloadId, e.message ?: "Unknown error")
    }
  }

  override fun launchCheck(
    checkInput: CheckConnectionInput,
    launcherInput: LauncherInput,
  ) {
    val labels =
      labeler.getSharedLabels(
        workloadId = launcherInput.workloadId,
        mutexKey = launcherInput.mutexKey,
        passThroughLabels = launcherInput.labels,
        autoId = launcherInput.autoId,
        workspaceId = checkInput.launcherConfig.workspaceId,
        networkSecurityTokens = checkInput.checkConnectionInput.networkSecurityTokens,
      ) + labeler.getCheckLabels()

    val imageName = checkInput.launcherConfig.dockerImage
    launchConnector(imageName, "check", launcherInput, labels)
  }

  override fun launchDiscover(
    discoverCatalogInput: DiscoverCatalogInput,
    launcherInput: LauncherInput,
  ) {
    val labels =
      labeler.getSharedLabels(
        workloadId = launcherInput.workloadId,
        mutexKey = launcherInput.mutexKey,
        passThroughLabels = launcherInput.labels,
        autoId = launcherInput.autoId,
        workspaceId = discoverCatalogInput.launcherConfig.workspaceId,
        networkSecurityTokens = discoverCatalogInput.discoverCatalogInput.networkSecurityTokens,
      ) + labeler.getDiscoverLabels()

    val imageName = discoverCatalogInput.launcherConfig.dockerImage
    launchConnector(imageName, "discover", launcherInput, labels)
  }

  override fun launchSpec(
    specInput: SpecInput,
    launcherInput: LauncherInput,
  ) {
    val labels =
      labeler.getSharedLabels(
        workloadId = launcherInput.workloadId,
        mutexKey = launcherInput.mutexKey,
        passThroughLabels = launcherInput.labels,
        autoId = launcherInput.autoId,
        workspaceId = null,
        networkSecurityTokens = emptyList(),
      ) + labeler.getSpecLabels()

    val imageName = specInput.launcherConfig.dockerImage
    launchConnector(imageName, "spec", launcherInput, labels)
  }

  // --- Private helpers ---

  private fun launchConnector(
    imageName: String,
    operation: String,
    launcherInput: LauncherInput,
    labels: Map<String, String>,
  ) {
    val workloadId = launcherInput.workloadId
    containerLauncher.ensureImage(imageName)
    logger.info { "Launching $operation for workload $workloadId in Docker with image $imageName" }

    val hydratedConfig =
      try {
        val configNode =
          when (launcherInput.workloadType) {
            WorkloadType.CHECK -> deserializer.toCheckConnectionInput(launcherInput.workloadInput).checkConnectionInput.connectionConfiguration
            WorkloadType.DISCOVER -> deserializer.toDiscoverCatalogInput(launcherInput.workloadInput).discoverCatalogInput.connectionConfiguration
            WorkloadType.SPEC -> Jsons.emptyObject()
            else -> Jsons.deserialize(launcherInput.workloadInput)
          }
        secretsRepositoryReader.hydrateConfigFromDefaultSecretPersistence(configNode)
      } catch (e: Exception) {
        logger.warn(e) { "Failed to hydrate config for workload $workloadId, using raw input" }
        Jsons.deserialize(launcherInput.workloadInput)
      }

    val autoId = launcherInput.autoId
    val workDir = File("/tmp/workload-$autoId")
    workDir.mkdirs()
    File(workDir, "config.json").writeText(Jsons.serialize(hydratedConfig as Any?))

    var networkId: String? = null
    var containerId: String? = null

    try {
      val networkName = "airbyte-workload-$workloadId"
      networkId = containerLauncher.createNetwork(networkName)

      val container =
        containerLauncher.createAndStartContainer(
          image = imageName,
          entrypoint = null,
          args = listOf(operation, "--config", "/config/config.json"),
          name = "airbyte-$operation",
          networkName = networkName,
          workDir = workDir,
          labels = labels,
        )
      containerId = container
      logger.info { "Started container $containerId for workload $workloadId in network $networkId" }

      // Wait and process output in background (managed async)
      java.util.concurrent.CompletableFuture.runAsync {
        try {
          val result = containerLauncher.waitForContainer(containerId)
          if (result == 0) {
            try {
              processConnectorOutput(launcherInput, containerId)
            } catch (e: Exception) {
              logger.error(e) { "Failed to process output for workload $workloadId" }
            }
            apiClient.updateStatusToSuccess(workloadId)
          } else {
            apiClient.updateStatusToFailed(workloadId, "Container exited with code $result")
          }
        } catch (e: Exception) {
          logger.error(e) { "Error waiting for container $containerId or updating status" }
          apiClient.updateStatusToFailed(workloadId, e.message ?: "Wait failed")
        } finally {
          containerLauncher.cleanupWorkload(workloadId, networkId, listOfNotNull(containerId), workDir)
        }
      }
    } catch (e: Exception) {
      logger.error(e) { "Failed to launch connector workload $workloadId. Cleaning up." }
      containerLauncher.cleanupWorkload(workloadId, networkId, listOfNotNull(containerId), workDir)
      apiClient.updateStatusToFailed(workloadId, e.message ?: "Launch failed")
    }
  }

  private fun processConnectorOutput(
    launcherInput: LauncherInput,
    containerId: String,
  ) {
    // Read container logs for output
    val outputStr = containerLauncher.getContainerOutput(containerId)
    if (outputStr.isBlank()) {
      logger.warn { "No output captured for workload ${launcherInput.workloadId}" }
      return
    }

    val messages =
      outputStr
        .lines()
        .filter { it.isNotBlank() }
        .mapNotNull { line ->
          try {
            Jsons.deserialize(line, AirbyteMessage::class.java)
          } catch (e: Exception) {
            null
          }
        }

    val connectorJobOutput = ConnectorJobOutput().withOutputType(getConnectorOutputType(launcherInput.workloadType))

    when (launcherInput.workloadType) {
      WorkloadType.CHECK -> {
        messages.find { it.type == AirbyteMessage.Type.CONNECTION_STATUS }?.let { msg ->
          connectorJobOutput.checkConnection =
            io.airbyte.config.StandardCheckConnectionOutput()
              .withStatus(
                io.airbyte.config.StandardCheckConnectionOutput.Status.fromValue(
                  msg.connectionStatus.status.value().lowercase(),
                ),
              ).withMessage(msg.connectionStatus.message)
        }
      }
      WorkloadType.SPEC -> {
        messages.find { it.type == AirbyteMessage.Type.SPEC }?.let { msg ->
          connectorJobOutput.spec = msg.spec
        }
      }
      WorkloadType.DISCOVER -> {
        messages.find { it.type == AirbyteMessage.Type.CATALOG }?.let { msg ->
          val discoverInput = deserializer.toDiscoverCatalogInput(launcherInput.workloadInput)
          val writeRequestBody =
            io.airbyte.api.client.model.generated.SourceDiscoverSchemaWriteRequestBody(
              catalog = catalogClientConverters.toAirbyteCatalogClientApi(msg.catalog),
              sourceId = discoverInput.discoverCatalogInput.sourceId?.let { UUID.fromString(it) },
              connectorVersion = discoverInput.discoverCatalogInput.connectorVersion,
              configurationHash = discoverInput.discoverCatalogInput.configHash,
            )

          try {
            val result = airbyteApiClient.sourceApi.writeDiscoverCatalogResult(writeRequestBody)
            connectorJobOutput.discoverCatalogId = result?.catalogId
          } catch (e: Exception) {
            logger.error(e) { "Failed to write discovery catalog for workload ${launcherInput.workloadId}" }
          }
        }
      }
      else -> {}
    }

    val request = io.airbyte.api.client.model.generated.WorkloadOutputWriteRequest(launcherInput.workloadId, Jsons.serialize(connectorJobOutput))
    airbyteApiClient.workloadOutputApi.writeWorkloadOutput(request)
  }

  private fun getConnectorOutputType(workloadType: WorkloadType): ConnectorJobOutput.OutputType =
    when (workloadType) {
      WorkloadType.CHECK -> ConnectorJobOutput.OutputType.CHECK_CONNECTION
      WorkloadType.DISCOVER -> ConnectorJobOutput.OutputType.DISCOVER_CATALOG_ID
      WorkloadType.SPEC -> ConnectorJobOutput.OutputType.SPEC
      else -> ConnectorJobOutput.OutputType.CHECK_CONNECTION
    }

  private fun reportStatus(
    workloadId: String,
    success: Boolean,
    errorMessage: String? = null,
  ) {
    // Small delay to allow workload-api-server to persist final state
    Thread.sleep(500)

    try {
      if (success) {
        apiClient.updateStatusToSuccess(workloadId)
        logger.info { "Successfully reported SUCCESS for workload $workloadId" }
      } else {
        apiClient.updateStatusToFailed(workloadId, errorMessage ?: "Orchestrator exited with non-zero code")
        logger.warn { "Reported FAILURE for workload $workloadId: $errorMessage" }
      }
    } catch (e: Exception) {
      val errorMsg = e.toString()
      if (errorMsg.contains("410") || errorMsg.contains("Gone")) {
        logger.info { "Workload $workloadId was already finalized by the server (410 Gone). Status update skipped." }
      } else {
        logger.error(e) { "Failed to report status for workload $workloadId" }
      }
    }
  }

  private fun getAndPatchCatalog(replicationInput: ReplicationInput): ConfiguredAirbyteCatalog {
    if (replicationInput.catalog == null) {
      logger.info { "Catalog missing in input, fetching from Airbyte API for connection ${replicationInput.connectionId}..." }
      val connectionInfo =
        airbyteApiClient.connectionApi.getConnection(
          ConnectionIdRequestBody(replicationInput.connectionId),
        )
      val catalog = catalogClientConverters.toConfiguredAirbyteInternal(connectionInfo.syncCatalog)
      replicationInput.catalog = catalog
      logger.info { "Catalog fetched: ${catalog.streams.size} streams" }
    }

    val catalog = replicationInput.catalog ?: throw IllegalStateException("Catalog must not be null at this point")
    val prefix = replicationInput.prefix ?: ""

    // Patch generationId, syncId, and unsupported sync modes
    var patchedModes = 0
    catalog.streams.forEach { stream ->
      logger.info { "Stream in catalog before patch: namespace=${stream.stream?.namespace}, name=${stream.stream?.name}" }
      
      // Do NOT set namespace to null here; Source needs it (e.g. 'public') to avoid NPE.
      // We patch it specifically for the destination in launchReplication.
      
      if (stream.generationId == null) stream.generationId = 0L
      if (stream.minimumGenerationId == null) stream.minimumGenerationId = 0L
      if (stream.syncId == null) stream.syncId = 0L

      if (stream.destinationSyncMode == DestinationSyncMode.OVERWRITE_DEDUP) {
        stream.destinationSyncMode = DestinationSyncMode.OVERWRITE
        patchedModes++
      }
    }

    if (patchedModes > 0) {
      logger.info { "Patched OVERWRITE_DEDUP to OVERWRITE for $patchedModes streams" }
    }

    return catalog
  }
}

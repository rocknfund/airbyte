/*
* Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
*/

@file:Suppress("ktlint:standard:max-line-length")

package io.airbyte.workload.launcher.containers

import com.github.dockerjava.api.DockerClient
import com.github.dockerjava.api.async.ResultCallback
import com.github.dockerjava.api.async.ResultCallbackTemplate
import com.github.dockerjava.api.model.Bind
import com.github.dockerjava.api.model.Frame
import com.github.dockerjava.api.model.HostConfig
import com.github.dockerjava.api.model.Volume
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.context.annotation.Requires
import jakarta.inject.Singleton
import java.io.File
import java.util.UUID
import java.util.concurrent.TimeUnit

private val logger = KotlinLogging.logger {}

/**
* Low-level Docker container operations.
* Mirrors KubePodLauncher for Kubernetes — provides atomic Docker API operations
* that are composed by DockerContainerClient into business operations.
*/
@Singleton
@Requires(property = "worker.environment", value = "docker")
class DockerContainerLauncher(
private val dockerClient: DockerClient,
private val config: ContainerConfig,
) {
/**
* Creates and starts a Docker container with the given configuration.
* Returns the container ID.
*/
fun createAndStartContainer(
image: String,
entrypoint: String?,
args: List<String>,
name: String,
networkName: String,
workDir: File,
labels: Map<String, String>,
extraEnv: List<String> = emptyList(),
): String {
ensureImage(image)

// Pass-through environment variables from host to connector containers.
// Secrets (AUTH_SIGNATURE_SECRET, JWT_SECRET, DATAPLANE_CLIENT_*) are NOT defaulted
// here — they MUST be set in the host environment (e.g. via .env).
val passThrough = listOf(
"INTERNAL_API_HOST",
"WORKLOAD_API_HOST",
"CONTROL_PLANE_TOKEN_ENDPOINT",
"AIRBYTE_INTERNAL_API_AUTH_TYPE",
"AIRBYTE_INTERNAL_API_AUTH_SIGNATURE_SECRET",
"AB_JWT_SIGNATURE_SECRET",
"DATAPLANE_CLIENT_ID",
"DATAPLANE_CLIENT_SECRET",
"STORAGE_TYPE",
"WORKSPACE_ROOT",
"LOCAL_ROOT",
"STORAGE_BUCKET_ACTIVITY_PAYLOAD",
"STORAGE_BUCKET_AUDIT_LOGGING",
"STORAGE_BUCKET_LOG",
"STORAGE_BUCKET_STATE",
"STORAGE_BUCKET_WORKLOAD_OUTPUT",
)
val envFromHost = passThrough.mapNotNull { key ->
System.getenv(key)?.let { "${key}=${it}" }
}
val mergedEnv = extraEnv + envFromHost

val cmd =
dockerClient
.createContainerCmd(image)
.withName("$name-${UUID.randomUUID().toString().take(8)}")
.withLabels(labels)
.withAliases(name)
.withEnv(mergedEnv)
.withHostConfig(
HostConfig
.newHostConfig()
.withNetworkMode(networkName)
.withBinds(
Bind(File(workDir, "source").absolutePath, Volume("/source")),
Bind(File(workDir, "dest").absolutePath, Volume("/dest")),
Bind(workDir.absolutePath, Volume("/config")),
Bind(config.localMount, Volume("/storage")),
Bind(config.workspaceMount, Volume("/workspace")),
),
)

if (args.isNotEmpty()) {
cmd.withCmd(args)
}

if (entrypoint != null) {
cmd.withEntrypoint(entrypoint)
}

val containerId = cmd.exec().id

// Join the main Airbyte network for inter-service communication
try {
dockerClient
.connectToNetworkCmd()
.withContainerId(containerId)
.withNetworkId(config.network)
.exec()
} catch (e: Exception) {
logger.warn { "Could not attach container $containerId to ${config.network}: ${e.message}" }
}

dockerClient.startContainerCmd(containerId).exec()
return containerId
}

/**
* Creates an isolated Docker network for a workload.
*/
fun createNetwork(networkName: String): String =
dockerClient
.createNetworkCmd()
.withName(networkName)
.withCheckDuplicate(true)
.exec()
.id

/**
* Waits for a container to exit and returns the exit code.
*/
fun waitForContainer(containerId: String): Int =
dockerClient.waitContainerCmd(containerId).start().awaitStatusCode()

/**
* Checks if any containers with the given labels are running.
*/
fun containersRunning(labels: Map<String, String>): Boolean {
val containers =
dockerClient
.listContainersCmd()
.withLabelFilter(labels)
.withShowAll(true)
.exec()

return containers.any { it.state == "running" }
}

/**
* Removes containers matching the given labels. Returns true if any were deleted.
*/
fun removeContainersByLabels(labels: Map<String, String>): Boolean {
val containers =
dockerClient
.listContainersCmd()
.withLabelFilter(labels)
.exec()

var deletedAny = false
for (container in containers) {
logger.info { "Removing container ${container.id} with labels $labels" }
dockerClient.removeContainerCmd(container.id).withForce(true).exec()
deletedAny = true
}
return deletedAny
}

/**
* Pulls a Docker image if it's not available locally.
*/
fun ensureImage(imageName: String) {
try {
dockerClient.inspectImageCmd(imageName).exec()
} catch (e: com.github.dockerjava.api.exception.NotFoundException) {
logger.info { "Image $imageName not found locally, pulling..." }
dockerClient
.pullImageCmd(imageName)
.start()
.awaitCompletion(5, TimeUnit.MINUTES)
}
}

  /**
   * Reads the current logs of a container and returns them as a single string.
   */
  fun getContainerOutput(containerId: String): String {
    val outputStream = java.io.ByteArrayOutputStream()
    dockerClient
      .logContainerCmd(containerId)
      .withStdOut(true)
      .withStdErr(true)
      .withFollowStream(false)
      .withTailAll()
      .exec(
        object : ResultCallbackTemplate<ResultCallback<Frame>, Frame>() {
          override fun onNext(frame: Frame) {
            outputStream.write(frame.payload)
          }
        },
      ).awaitCompletion()
    return outputStream.toString(Charsets.UTF_8)
  }

/**
* Streams container logs into the launcher's log output and optionally to a file.
*/
fun aggregateLogs(
workloadId: String,
logPath: String,
containerInfo: List<Pair<String, String>>,
) {
val logFile = File(logPath)
containerInfo.forEach { (id, prefix) ->
dockerClient
.logContainerCmd(id)
.withStdOut(true)
.withStdErr(true)
.withFollowStream(true)
.exec(
object : ResultCallbackTemplate<ResultCallback<Frame>, Frame>() {
override fun onNext(frame: Frame) {
val message = String(frame.payload, Charsets.UTF_8).trim()
if (message.isNotEmpty()) {
val formattedMessage = "[$prefix] $message\n"
logger.info { formattedMessage.trim() }
try {
logFile.parentFile?.mkdirs()
logFile.appendText(formattedMessage)
} catch (e: Exception) {
logger.error(e) { "Failed to write log to $logPath" }
}
}
}
},
)
}
}

/**
* Cleans up workload resources: containers, network, and work directory.
*/
fun cleanupWorkload(
workloadId: String,
networkId: String?,
containerIds: List<String>,
workDir: File? = null,
) {
logger.info { "Cleaning up workload $workloadId" }

// Remove containers
for (containerId in containerIds) {
try {
dockerClient
.removeContainerCmd(containerId)
.withForce(true)
.withRemoveVolumes(true)
.exec()
} catch (e: Exception) {
logger.warn(e) { "Failed to remove container $containerId" }
}
}

// Remove network
if (networkId != null) {
try {
dockerClient.removeNetworkCmd(networkId).exec()
} catch (e: Exception) {
logger.warn(e) { "Failed to remove network $networkId" }
}
}

// Remove workDir containing hydrated secrets
if (workDir != null && workDir.exists()) {
try {
workDir.deleteRecursively()
logger.info { "Cleaned up workDir: $workDir" }
} catch (e: Exception) {
logger.warn { "Failed to clean up workDir $workDir: ${e.message}" }
}
}
}

/**
* Deletes a container and its associated resources (network, workDir) based on workloadId or autoId.
* Used by the sweepers to ensure consistent cleanup.
*/
fun deleteContainerAndResources(
containerId: String,
workloadId: String?,
autoId: String?
): Boolean {
var deletedAny = false
try {
logger.info { "Sweeping container $containerId (workloadId: $workloadId, autoId: $autoId)" }
dockerClient.removeContainerCmd(containerId)
.withForce(true)
.withRemoveVolumes(true)
.exec()
deletedAny = true
} catch (e: Exception) {
logger.warn(e) { "Failed to sweep container $containerId" }
}

if (workloadId != null) {
val networkName = "airbyte-workload-$workloadId"
try {
val networks = dockerClient.listNetworksCmd().withNameFilter(networkName).exec()
val networkId = networks.firstOrNull { it.name == networkName }?.id
if (networkId != null) {
dockerClient.removeNetworkCmd(networkId).exec()
}
} catch (e: Exception) {
logger.warn(e) { "Failed to remove network $networkName during sweep" }
}

// Cleanup workDir
val workDir = File("/tmp/workloads/$workloadId")
if (workDir.exists()) {
try { workDir.deleteRecursively() } catch (e: Exception) {}
}
}

if (autoId != null) {
val workDir = File("/tmp/workload-$autoId")
if (workDir.exists()) {
try { workDir.deleteRecursively() } catch (e: Exception) {}
}
}

return deletedAny
}
}

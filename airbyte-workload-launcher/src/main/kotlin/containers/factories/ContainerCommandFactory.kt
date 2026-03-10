/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

@file:Suppress("ktlint:standard:max-line-length")

package io.airbyte.workload.launcher.containers.factories

import io.airbyte.workers.pod.FileConstants

/**
 * Factory for generating shell scripts used as entry points in Docker containers.
 * Mirrors pods/factories/ContainerCommandFactory for Kubernetes.
 */
internal object ContainerCommandFactory {
  private const val ORCHESTRATOR_APPLICATION_EXECUTABLE = "/app/airbyte-app/bin/airbyte-container-orchestrator"

  private const val TERMINATION_CHECK_INTERVAL_SECONDS = 10

  // Shell variable names
  private const val SOURCE_DIR = "SOURCE_DIR"
  private const val DEST_DIR = "DEST_DIR"
  private const val STDIN_PIPE_FILE = "STDIN_PIPE_FILE"
  private const val STDOUT_PIPE_FILE = "STDOUT_PIPE_FILE"
  private const val STDERR_PIPE_FILE = "STDERR_PIPE_FILE"
  private const val TERMINATION_MARKER_FILE = "TERMINATION_MARKER_FILE"
  private const val EXIT_CODE_FILE = "EXIT_CODE_FILE"

  /**
   * Runs orchestrator and creates TERMINATION_MARKER_FILE on exit.
   */
  fun orchestrator() =
    """
export $SOURCE_DIR="${FileConstants.SOURCE_DIR}"
export $DEST_DIR="${FileConstants.DEST_DIR}"
export $STDIN_PIPE_FILE="${FileConstants.STDIN_PIPE_FILE}"
export $STDOUT_PIPE_FILE="${FileConstants.STDOUT_PIPE_FILE}"
export $STDERR_PIPE_FILE="${FileConstants.STDERR_PIPE_FILE}"
export $TERMINATION_MARKER_FILE="${FileConstants.TERMINATION_MARKER_FILE}"

trap "touch ${'$'}$SOURCE_DIR/${'$'}$TERMINATION_MARKER_FILE ${'$'}$DEST_DIR/${'$'}$TERMINATION_MARKER_FILE" EXIT
$ORCHESTRATOR_APPLICATION_EXECUTABLE
""".trimIndent()

  /**
   * Runs source connector with optional state file.
   */
  fun source() =
    connectorCommandWrapper(
      """
# only provide state flag if present
if [ ! -f ${'$'}$SOURCE_DIR/${FileConstants.INPUT_STATE_FILE} ]; then
eval "${'$'}AIRBYTE_ENTRYPOINT read --config ${'$'}$SOURCE_DIR/${FileConstants.CONNECTOR_CONFIG_FILE} --catalog ${'$'}$SOURCE_DIR/${FileConstants.CATALOG_FILE} 2> \"${'$'}$SOURCE_DIR/${'$'}$STDERR_PIPE_FILE\" > \"${'$'}$SOURCE_DIR/${'$'}$STDOUT_PIPE_FILE\""
else
eval "${'$'}AIRBYTE_ENTRYPOINT read --config ${'$'}$SOURCE_DIR/${FileConstants.CONNECTOR_CONFIG_FILE} --catalog ${'$'}$SOURCE_DIR/${FileConstants.CATALOG_FILE} --state ${'$'}$SOURCE_DIR/${FileConstants.INPUT_STATE_FILE} 2> \"${'$'}$SOURCE_DIR/${'$'}$STDERR_PIPE_FILE\" > \"${'$'}$SOURCE_DIR/${'$'}$STDOUT_PIPE_FILE\""
fi
""".trimIndent(),
    )

  /**
   * Runs destination connector which reads from STDIN_PIPE_FILE.
   */
  fun destination() =
    connectorCommandWrapper(
      """
eval "${'$'}AIRBYTE_ENTRYPOINT write --config ${'$'}$DEST_DIR/${FileConstants.CONNECTOR_CONFIG_FILE} --catalog ${'$'}$DEST_DIR/${FileConstants.CATALOG_FILE} 2> \"${'$'}$DEST_DIR/${'$'}$STDERR_PIPE_FILE\" > \"${'$'}$DEST_DIR/${'$'}$STDOUT_PIPE_FILE\" < \"${'$'}$DEST_DIR/${'$'}$STDIN_PIPE_FILE\""
""".trimIndent(),
      exitCodeDir = DEST_DIR,
    )

  /**
   * Fail loudly if entry point not set.
   */
  private fun failLoudlyIfEntrypointNotSet() =
    """
if [ -z "${"$"}{AIRBYTE_ENTRYPOINT}" ]; then
echo "Entrypoint was not set! AIRBYTE_ENTRYPOINT must be set in the container."
exit 127
else
echo "Using AIRBYTE_ENTRYPOINT: ${"$"}{AIRBYTE_ENTRYPOINT}"
fi
"""

  private fun connectorCommandWrapper(
    command: String,
    exitCodeDir: String = SOURCE_DIR,
  ): String =
    """
export $SOURCE_DIR="${FileConstants.SOURCE_DIR}"
export $DEST_DIR="${FileConstants.DEST_DIR}"
export $STDIN_PIPE_FILE="${FileConstants.STDIN_PIPE_FILE}"
export $STDOUT_PIPE_FILE="${FileConstants.STDOUT_PIPE_FILE}"
export $STDERR_PIPE_FILE="${FileConstants.STDERR_PIPE_FILE}"
export $TERMINATION_MARKER_FILE="${FileConstants.TERMINATION_MARKER_FILE}"
export $EXIT_CODE_FILE="${FileConstants.EXIT_CODE_FILE}"

${failLoudlyIfEntrypointNotSet()}
# run connector in background and store PID
($command) &
CHILD_PID=${'$'}!

# Monitor for termination marker and kill connector if found
while [ -d /proc/${'$'}CHILD_PID ] 2>/dev/null; do
if [ -f "${'$'}$SOURCE_DIR/${'$'}$TERMINATION_MARKER_FILE" ]; then
echo "Termination marker detected, killing connector PID ${'$'}CHILD_PID"
kill -TERM ${'$'}CHILD_PID 2>/dev/null || true
exit 0
fi
sleep $TERMINATION_CHECK_INTERVAL_SECONDS
done

# Wait for child process to exit and capture exit code
wait ${'$'}CHILD_PID
EXIT_CODE=${'$'}?

# Write exit code atomically
echo ${'$'}EXIT_CODE > "${'$'}$exitCodeDir/${'$'}$EXIT_CODE_FILE"
exit ${'$'}EXIT_CODE
""".trimIndent()
}

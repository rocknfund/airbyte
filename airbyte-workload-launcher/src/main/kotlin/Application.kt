/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.launcher

import io.micronaut.runtime.Micronaut.build

fun main(args: Array<String>) {
  val context = build(*args).deduceCloudEnvironment(false).deduceEnvironment(false).start()
  val type = context.getProperty("worker.environment", String::class.java).orElse("not set")
  println("HYBRID_LAUNCHER_TYPE: $type")
  println("ACTIVE_ENVIRONMENTS: ${context.environment.activeNames}")
}

/*
 * (C) Copyright 2018-2020 Intel Corporation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * GOVERNMENT LICENSE RIGHTS-OPEN SOURCE SOFTWARE
 * The Government's rights to use, modify, reproduce, release, perform, display,
 * or disclose this software are subject to the terms of the Apache License as
 * provided in Contract No. B609815.
 * Any reproduction of computer software, computer software documentation, or
 * portions thereof marked with this legend must also reproduce the markings.
 */

package org.apache.spark.shuffle.daos

import org.apache.spark.{SparkEnv, TaskContext}
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.MapStatus
import org.apache.spark.shuffle.{BaseShuffleHandle, ShuffleWriter}

class DaosShuffleWriter[K, V, C](
    handle: BaseShuffleHandle[K, V, C],
    mapId: Long,
    context: TaskContext,
    shuffleIO: DaosShuffleIO)
  extends ShuffleWriter[K, V] with Logging {

  private val dep = handle.dependency

  private var partitionsWriter: MapPartitionsWriter[K, V, _] = null

  private var stopping = false

  private var mapStatus: MapStatus = null

  private val blockManager = SparkEnv.get.blockManager

  override def write(records: Iterator[Product2[K, V]]): Unit = {
//    val start = System.nanoTime()
    partitionsWriter = if (dep.mapSideCombine) {
      new MapPartitionsWriter[K, V, C](
        handle.shuffleId,
        context,
        dep.aggregator,
        Some(dep.partitioner),
        dep.keyOrdering,
        dep.serializer,
        shuffleIO)
    } else {
      // In this case we pass neither an aggregator nor an ordering to the sorter, because we don't
      // care whether the keys get sorted in each partition; that will be done on the reduce side
      // if the operation being run is sortByKey.
      new MapPartitionsWriter[K, V, V](
        handle.shuffleId,
        context,
        aggregator = None,
        Some(dep.partitioner),
        ordering = None,
        dep.serializer,
        shuffleIO)
    }
    partitionsWriter.insertAll(records)
    val partitionLengths = partitionsWriter.commitAll

    // logInfo(context.taskAttemptId() + " all time: " + (System.nanoTime() - start)/1000000)

    mapStatus = MapStatus(blockManager.shuffleServerId, partitionLengths, mapId)
  }

  override def stop(success: Boolean): Option[MapStatus] = {
    try {
      if (stopping) {
        return None
      }
      stopping = true
      if (success) {
        Option(mapStatus)
      } else {
        None
      }
    } finally {
      // Clean up our sorter, which may have its own intermediate files
      if (partitionsWriter != null) {
        val startTime = System.nanoTime()
        partitionsWriter.close
        context.taskMetrics().shuffleWriteMetrics.incWriteTime(System.nanoTime - startTime)
        partitionsWriter = null
      }
    }
  }
}

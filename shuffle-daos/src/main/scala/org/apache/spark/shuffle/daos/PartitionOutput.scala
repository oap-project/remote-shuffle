/*
 * (C) Copyright 2018-2021 Intel Corporation.
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

import java.io.OutputStream

import org.apache.spark.serializer.SerializationStream
import org.apache.spark.shuffle.ShuffleWriteMetricsReporter
import org.apache.spark.storage.{ShuffleBlockId, TimeTrackingOutputStream}
import org.apache.spark.util.Utils

/**
  * Output for each partition.
  *
  * @param partitionId
  * @param parent
  * @param writeMetrics
  */
class PartitionOutput[K, V, C](
  partitionId: Int,
  parent: MapPartitionsWriter[K, V, C]#PartitionsBuffer,
  writeMetrics: ShuffleWriteMetricsReporter) {

  private val mapId = parent.taskContext.taskAttemptId()
  private val serializerManager = parent.serializerManager
  private val serializerInstance = parent.serInstance

  private var ds: DaosShuffleOutputStream = null

  private var ts: TimeTrackingOutputStream = null

  private var bs: OutputStream = null

  private var objOut: SerializationStream = null

  private var opened: Boolean = false

  private var numRecordsWritten = 0

  private var lastWrittenBytes = 0L

  private val flushRecords = parent.conf.get(SHUFFLE_DAOS_WRITE_FLUSH_RECORDS)

  def open: Unit = {
    ds = new DaosShuffleOutputStream(partitionId, parent.daosWriter)
    parent.daosWriter.incrementSeq(partitionId)
    ts = new TimeTrackingOutputStream(writeMetrics, ds)
    bs = serializerManager.wrapStream(ShuffleBlockId(parent.shuffleId, mapId, partitionId), ts)
    objOut = serializerInstance.serializeStream(bs)

    opened = true
  }

  def write(key: Any, value: Any): Unit = {
    if (!opened) {
      open
    }
    objOut.writeKey(key)
    objOut.writeValue(value)

    // update metrics
    numRecordsWritten += 1
    writeMetrics.incRecordsWritten(1)
    if (numRecordsWritten % 16384 == 0) {
      updateWrittenBytes
    }
  }

  def writeAutoFlush(key: Any, value: Any): Unit = {
    write(key, value)
    if (numRecordsWritten % flushRecords == 0) {
      flush
    }
  }

  private def updateWrittenBytes: Unit = {
    val writtenBytes = ds.getWrittenBytes
    writeMetrics.incBytesWritten(writtenBytes - lastWrittenBytes)
    lastWrittenBytes = writtenBytes
  }

  def flush: Unit = {
    if (opened) {
      objOut.flush()
      parent.daosWriter.flush(partitionId)
      updateWrittenBytes
    }
  }

  def close: Unit = {
    if (opened) {
      Utils.tryWithSafeFinally {
        objOut.close()
      } {
        updateWrittenBytes
        objOut = null
        bs = null
        ds = null
        ts = null
        opened = false
      }
    }
  }
}

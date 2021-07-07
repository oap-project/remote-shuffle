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

import org.apache.spark.serializer.{SerializationStream, SerializerInstance, SerializerManager}
import org.apache.spark.shuffle.ShuffleWriteMetricsReporter
import org.apache.spark.storage.{ShuffleBlockId, TimeTrackingOutputStream}
import org.apache.spark.util.Utils

/**
  * Output for each partition.
  *
  * @param shuffleId
  * @param mapId
  * @param partitionId
  * @param serializerManager
  * @param serializerInstance
  * @param daosWriter
  * @param writeMetrics
  */
class PartitionOutput[K, V, C](
  partitionId: Int,
  mapId: Long,
  parent: MapPartitionsWriter[K, V, C]#PartitionsBuffer,
  serializerManager: SerializerManager,
  serializerInstance: SerializerInstance,
  writeMetrics: ShuffleWriteMetricsReporter) {

  private var ds: DaosShuffleOutputStream = null

  private var ts: TimeTrackingOutputStream = null

  private var bs: OutputStream = null

  private var objOut: SerializationStream = null

  private var opened: Boolean = false

  private var numRecordsWritten = 0

  private var lastWrittenBytes = 0L

  def open: Unit = {
    ds = new DaosShuffleOutputStream(partitionId, parent.daosWriter, parent.needSpill)
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
    // writeMetrics.incRecordsWritten(1)
//    if (numRecordsWritten % 16384 == 0) {
//      updateWrittenBytes
//    }
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
//      updateWrittenBytes
    }
  }

  def setMerged(bool: Boolean): Unit = {
    parent.daosWriter.setMerged(partitionId)
  }

  def resetMetrics: Unit = {
    numRecordsWritten = 0
    ds.resetMetrics()
  }

  def close: Unit = {
    if (opened) {
      Utils.tryWithSafeFinally {
        objOut.close()
      } {
        updateWrittenBytes
        writeMetrics.incRecordsWritten(numRecordsWritten)
        objOut = null
        bs = null
        ds = null
        ts = null
        opened = false
      }
    }
  }
}

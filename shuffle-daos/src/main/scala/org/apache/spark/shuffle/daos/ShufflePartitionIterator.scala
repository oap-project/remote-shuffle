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

import java.io.{InputStream, IOException}
import java.util

import org.apache.commons.io.IOUtils

import org.apache.spark.{SparkException, TaskContext}
import org.apache.spark.internal.Logging
import org.apache.spark.shuffle.{FetchFailedException, ShuffleReadMetricsReporter}
import org.apache.spark.storage.{BlockId, BlockManagerId, ShuffleBlockBatchId, ShuffleBlockId}
import org.apache.spark.util.{CompletionIterator, TaskCompletionListener, Utils}

class ShufflePartitionIterator(
    context: TaskContext,
    blocksByAddress: Iterator[(BlockManagerId, Seq[(BlockId, Long, Int)])],
    streamWrapper: (BlockId, InputStream) => InputStream,
    maxBytesInFlight: Long,
    maxReqSizeShuffleToMem: Long,
    detectCorrupt: Boolean,
    detectCorruptUseExtraMemory: Boolean,
    shuffleMetrics: ShuffleReadMetricsReporter,
    daosReader: DaosReader) extends Iterator[(BlockId, InputStream)] with Logging {

  private var lastMapReduce: (String, Integer) = _
  private var lastMRBlock: (java.lang.Long, BlockId) = _

  val dummyBlkId = BlockManagerId("-1", "dummy-host", 1024)

  private[daos] var inputStream: DaosShuffleInputStream = null

  // (mapid, reduceid) -> (length, BlockId)
  private val mapReduceIdMap = new util.LinkedHashMap[(String, Integer), (java.lang.Long, BlockId)]

  private var mapReduceIt: util.Iterator[util.Map.Entry[(String, Integer), (java.lang.Long, BlockId)]] = _

  private val onCompleteCallback = new ShufflePartitionCompletionListener(this)

  initialize

  def initialize: Unit = {
    context.addTaskCompletionListener(onCompleteCallback)
    startReading
  }

  private def getMapReduceId(shuffleBlockId: ShuffleBlockId): (String, Integer) = {
    val name = shuffleBlockId.name.split("_")
    (name(2), Integer.valueOf(name(3)))
  }

  private def startReading: Unit = {
    blocksByAddress.foreach(t2 => {
      t2._2.foreach(t3 => {
        val mapReduceId = getMapReduceId(t3._1.asInstanceOf[ShuffleBlockId])
        if (mapReduceIdMap.containsKey(mapReduceId._1)) {
          throw new IllegalStateException("duplicate map id: " + mapReduceId._1)
        }
        mapReduceIdMap.put((mapReduceId._1, mapReduceId._2), (t3._2, t3._1))
      })
    })

    if (log.isDebugEnabled) {
      log.debug(s"total mapreduceId: ${mapReduceIdMap.size()}, they are, ")
      mapReduceIdMap.forEach((key, value) => logDebug(context.taskAttemptId() + ": " +
        key.toString() + " = " + value.toString))
    }

    inputStream = new DaosShuffleInputStream(daosReader, mapReduceIdMap,
      maxBytesInFlight, maxReqSizeShuffleToMem, shuffleMetrics)
    mapReduceIt = mapReduceIdMap.entrySet().iterator()
  }

  override def hasNext: Boolean = {
    (!inputStream.isCompleted()) & mapReduceIt.hasNext
  }

  override def next(): (BlockId, InputStream) = {
    if (!hasNext) {
      throw new NoSuchElementException
    }
    val entry = mapReduceIt.next()
    lastMapReduce = entry.getKey
    lastMRBlock = entry.getValue
    val lastBlockId = lastMRBlock._2.asInstanceOf[ShuffleBlockId]
    inputStream.nextMap()
    var input: InputStream = null
    var streamCompressedOrEncryptd = false
    try {
      input = streamWrapper(lastBlockId, inputStream)
      streamCompressedOrEncryptd = !input.eq(inputStream)
      if (streamCompressedOrEncryptd && detectCorruptUseExtraMemory) {
        input = Utils.copyStreamUpTo(input, maxBytesInFlight / 3)
      }
    } catch {
      case e: IOException =>
        logError(s"got an corrupted block ${inputStream.getCurBlockId} originated from " +
          s"${dummyBlkId}.", e)
        throw e
    } finally {
      if (input == null) {
        inputStream.close(false)
      }
    }
    (lastBlockId, new BufferReleasingInputStream(lastMapReduce, lastMRBlock, input, this,
      detectCorrupt && streamCompressedOrEncryptd))
  }

  def throwFetchFailedException(
      blockId: BlockId,
      mapIndex: Int,
      address: BlockManagerId,
      e: Throwable): Nothing = {
    blockId match {
      case ShuffleBlockId(shufId, mapId, reduceId) =>
        throw new FetchFailedException(address, shufId, mapId, mapIndex, reduceId, e)
      case ShuffleBlockBatchId(shuffleId, mapId, startReduceId, _) =>
        throw new FetchFailedException(address, shuffleId, mapId, mapIndex, startReduceId, e)
      case _ =>
        throw new SparkException(
          "Failed to get block " + blockId + ", which is not a shuffle block", e)
    }
  }

  def toCompletionIterator: Iterator[(BlockId, InputStream)] = {
    CompletionIterator[(BlockId, InputStream), this.type](this,
      onCompleteCallback.onTaskCompletion(context))
  }

  def cleanup: Unit = {
    if (inputStream != null) {
      inputStream.close(false)
      inputStream = null;
    }
  }

}

/**
 * Helper class that ensures a ManagedBuffer is released upon InputStream.close() and
 * also detects stream corruption if streamCompressedOrEncrypted is true
 */
private class BufferReleasingInputStream(
                                          // This is visible for testing
                                          private val mapreduce: (String, Integer),
                                          private val mrblock: (java.lang.Long, BlockId),
                                          private val delegate: InputStream,
                                          private val iterator: ShufflePartitionIterator,
                                          private val detectCorruption: Boolean)
  extends InputStream {

  private[this] var closed = false

  override def read(): Int = {
    try {
      delegate.read()
    } catch {
      case e: IOException if detectCorruption =>
        IOUtils.closeQuietly(this)
        iterator.throwFetchFailedException(mrblock._2, mapreduce._1.toInt,
          iterator.dummyBlkId, e)
    }
  }

  override def close(): Unit = {
    if (!closed) {
      delegate.close()
      closed = true
    }
  }

  override def available(): Int = delegate.available()

  override def mark(readlimit: Int): Unit = delegate.mark(readlimit)

  override def skip(n: Long): Long = {
    try {
      delegate.skip(n)
    } catch {
      case e: IOException if detectCorruption =>
        IOUtils.closeQuietly(this)
        iterator.throwFetchFailedException(mrblock._2, mapreduce._1.toInt,
          iterator.dummyBlkId, e)
    }
  }

  override def markSupported(): Boolean = delegate.markSupported()

  override def read(b: Array[Byte]): Int = {
    try {
      delegate.read(b)
    } catch {
      case e: IOException if detectCorruption =>
        IOUtils.closeQuietly(this)
        iterator.throwFetchFailedException(mrblock._2, mapreduce._1.toInt,
          iterator.dummyBlkId, e)
    }
  }

  override def read(b: Array[Byte], off: Int, len: Int): Int = {
    try {
      delegate.read(b, off, len)
    } catch {
      case e: IOException if detectCorruption =>
        IOUtils.closeQuietly(this)
        iterator.throwFetchFailedException(mrblock._2, mapreduce._1.toInt,
          iterator.dummyBlkId, e)
    }
  }

  override def reset(): Unit = delegate.reset()
}

private class ShufflePartitionCompletionListener(var data: ShufflePartitionIterator)
  extends TaskCompletionListener {

  override def onTaskCompletion(context: TaskContext): Unit = {
    if (data != null) {
      data.cleanup
    }
  }
}

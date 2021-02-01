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

import java.util.Comparator

import org.apache.spark._
import org.apache.spark.internal.Logging
import org.apache.spark.memory.{MemoryConsumer, TaskMemoryManager}
import org.apache.spark.serializer.Serializer

class MapPartitionsWriter[K, V, C](
    shuffleId: Int,
    context: TaskContext,
    aggregator: Option[Aggregator[K, V, C]] = None,
    partitioner: Option[Partitioner] = None,
    ordering: Option[Ordering[K]] = None,
    serializer: Serializer = SparkEnv.get.serializer,
    shuffleIO: DaosShuffleIO) extends Logging {

  private val conf = SparkEnv.get.conf

  private val numPartitions = partitioner.map(_.numPartitions).getOrElse(1)
  private val shouldPartition = numPartitions > 1
  private def getPartition(key: K): Int = {
    if (shouldPartition) partitioner.get.getPartition(key) else 0
  }

  private val serializerManager = SparkEnv.get.serializerManager
  private val serInstance = serializer.newInstance()

  private val daosWriter = shuffleIO.getDaosWriter(
    numPartitions,
    shuffleId,
    context.taskAttemptId())
  private val writeMetrics = context.taskMetrics().shuffleWriteMetrics

  /* key comparator if map-side combiner is defined */
  private val keyComparator: Comparator[K] = ordering.getOrElse((a: K, b: K) => {
    val h1 = if (a == null) 0 else a.hashCode()
    val h2 = if (b == null) 0 else b.hashCode()
    if (h1 < h2) -1 else if (h1 == h2) 0 else 1
  })

  private def comparator: Option[Comparator[K]] = {
    if (ordering.isDefined || aggregator.isDefined) {
      Some(keyComparator)
    } else {
      None
    }
  }

  // buffer by partition
  @volatile var writeBuffer = new PartitionsBuffer[K, C](
    numPartitions,
    comparator,
    conf,
    context.taskMemoryManager())

  private[this] var _elementsRead = 0

  private var _writtenBytes = 0L
  def writtenBytes: Long = _writtenBytes

  def peakMemoryUsedBytes: Long = writeBuffer.peakSize

  def insertAll(records: Iterator[Product2[K, V]]): Unit = {
    // TODO: stop combining if we find that the reduction factor isn't high
    val shouldCombine = aggregator.isDefined
    if (shouldCombine) {
      // Combine values in-memory first using our AppendOnlyMap
      val mergeValue = aggregator.get.mergeValue
      val createCombiner = aggregator.get.createCombiner
      var kv: Product2[K, V] = null
      val update = (hadValue: Boolean, oldValue: C) => {
        if (hadValue) mergeValue(oldValue, kv._2) else createCombiner(kv._2)
      }
      while (records.hasNext) {
        addElementsRead()
        kv = records.next()
        writeBuffer.changeValue(getPartition(kv._1), kv._1, update)
      }
    } else {
      // Stick values into our buffer
      while (records.hasNext) {
        addElementsRead()
        val kv = records.next()
        writeBuffer.insert(getPartition(kv._1), kv._1, kv._2.asInstanceOf[C])
      }
    }
    // logInfo(context.taskAttemptId() + " insert time: " + (System.nanoTime() - start)/1000000)
  }

  def commitAll: Array[Long] = {
    writeBuffer.flushAll
    writeBuffer.close
    daosWriter.getPartitionLens(numPartitions)
  }

  def close: Unit = {
    // serialize rest of records
    daosWriter.close
  }

  protected def addElementsRead(): Unit = { _elementsRead += 1 }

  /**
   * @param numPartitions
   * @param keyComparator
   * @param conf
   * @param taskMemManager
   * @tparam K
   * @tparam C
   */
  private[daos] class PartitionsBuffer[K, C](
      numPartitions: Int,
      val keyComparator: Option[Comparator[K]],
      val conf: SparkConf,
      val taskMemManager: TaskMemoryManager) extends MemoryConsumer(taskMemManager) {
    private val partBufferThreshold = conf.get(SHUFFLE_DAOS_WRITE_PARTITION_BUFFER_SIZE).toInt * 1024
    private val totalBufferThreshold = conf.get(SHUFFLE_DAOS_WRITE_BUFFER_SIZE).toInt * 1024 * 1024
    private val totalBufferInitial = conf.get(SHUFFLE_DAOS_WRITE_BUFFER_INITIAL_SIZE).toInt * 1024 * 1024
    private val forceWritePct = conf.get(SHUFFLE_DAOS_WRITE_BUFFER_FORCE_PCT)
    private val totalWriteValve = totalBufferThreshold * forceWritePct
    private[daos] val sampleStat = new SampleStat

    if (log.isDebugEnabled()) {
      log.debug("partBufferThreshold: " + partBufferThreshold)
      log.debug("totalBufferThreshold: " + totalBufferThreshold)
      log.debug("totalBufferInitial: " + totalBufferInitial)
      log.debug("forceWritePct: " + forceWritePct)
      log.debug("totalWriteValve: " + totalWriteValve)
    }

    if (totalBufferInitial > totalBufferThreshold) {
      throw new IllegalArgumentException("total buffer initial size (" + totalBufferInitial + ") should be no more " +
             "than total buffer threshold (" + totalBufferThreshold + ").")
    }

    private var totalSize = 0L
    private var memoryLimit = totalBufferInitial * 1L
    private var largestSize = 0L

    var peakSize = 0L

    private def initialize[T >: Linked[K, C] with SizeAware[K, C]]():
      (T, T, Array[SizeAwareMap[K, C]], Array[SizeAwareBuffer[K, C]]) = {
      // create virtual partition head and end, as well as all linked partitions
      val (partitionMapArray, partitionBufferArray) = if (comparator.isDefined) {
        (new Array[SizeAwareMap[K, C]](numPartitions), null)
      } else {
        (null, new Array[SizeAwareBuffer[K, C]](numPartitions))
      }
      val (head, end) = if (comparator.isDefined) {
        val mapHead = new SizeAwareMap[K, C](-1, partBufferThreshold,
          totalBufferInitial, taskMemManager, this)
        val mapEnd = new SizeAwareMap[K, C](-2, partBufferThreshold,
          totalBufferInitial, taskMemManager, this)
        (0 until numPartitions).foreach(i => {
          val map = new SizeAwareMap[K, C](i, partBufferThreshold, totalBufferInitial, taskMemManager, this)
          partitionMapArray(i) = map
          if (i > 0) {
            val prevMap = partitionMapArray(i - 1)
            prevMap.next = map
            map.prev = prevMap
          }
        })
        (mapHead, mapEnd)
      } else {
        val bufferHead = new SizeAwareBuffer[K, C](-1, partBufferThreshold,
          totalBufferInitial, taskMemManager, this)
        val bufferEnd = new SizeAwareBuffer[K, C](-2, partBufferThreshold,
          totalBufferInitial, taskMemManager, this)
        (0 until numPartitions).foreach(i => {
          val buffer = new SizeAwareBuffer[K, C](i, partBufferThreshold, totalBufferInitial, taskMemManager, this)
          partitionBufferArray(i) = buffer
          if (i > 0) {
            val prevBuffer = partitionBufferArray(i - 1)
            prevBuffer.next = buffer
            buffer.prev = prevBuffer
          }
        })
        (bufferHead, bufferEnd)
      }
      // link head and end
      val (first, last) = if (comparator.isDefined) (partitionMapArray(0), partitionMapArray(numPartitions - 1))
        else (partitionBufferArray(0), partitionBufferArray(numPartitions - 1))
      head.next = first
      first.prev = head
      end.prev = last
      last.next = end

      (head, end, partitionMapArray, partitionBufferArray)
    }

    private val (head, end, partitionMapArray, partitionBufferArray) = initialize()

    private def moveToFirst(node: Linked[K, C] with SizeAware[K, C]): Unit = {
      if (head.next != node) {
        // remove node from list
        node.prev.next = node.next
        node.next.prev = node.prev
        // move to first
        node.next = head.next
        head.next.prev = node
        head.next = node
        node.prev = head
        // set largestSize
        largestSize = head.next.estimatedSize
      }
    }

    private def moveToLast(node: Linked[K, C] with SizeAware[K, C]): Unit = {
      if (end.prev != node) {
        // remove node from list
        node.prev.next = node.next
        node.next.prev = node.prev
        // move to last
        node.prev = end.prev
        end.prev.next = node
        end.prev = node
        node.next = end
      }
    }

    def changeValue(partitionId: Int, key: K, updateFunc: (Boolean, C) => C): Unit = {
      val map = partitionMapArray(partitionId)
      val estSize = map.changeValue(key, updateFunc)
      afterUpdate(estSize, map)
    }

    def insert(partitionId: Int, key: K, value: C): Unit = {
      val buffer = partitionBufferArray(partitionId)
      val estSize = buffer.insert(key, value)
      afterUpdate(estSize, buffer)
    }

    def afterUpdate[T <: SizeAware[K, C] with Linked[K, C]](estSize: Long, buffer: T): Unit = {
      if (estSize > largestSize) {
        largestSize = estSize
        moveToFirst(buffer)
      } else if (estSize == 0) {
        moveToLast(buffer)
      } else {
        // check if total buffer exceeds memory limit
        maybeWriteTotal()
      }
    }

    private def writeFirst: Unit = {
      val buffer = head.next
      buffer.writeAndFlush
      moveToLast(buffer)
    }

    private def maybeWriteTotal(): Unit = {
      if (totalSize > totalWriteValve) {
        writeFirst
      }
      if (totalSize > memoryLimit) {
        val memRequest = 2 * totalSize - memoryLimit
        val granted = acquireMemory(memRequest)
        memoryLimit += granted
        if (totalSize >= memoryLimit) {
          writeFirst
        }
      }
    }

    def updateTotalSize(diff: Long): Unit = {
      totalSize += diff
      if (totalSize > peakSize) {
        peakSize = totalSize
      }
    }

    def releaseMemory(memory: Long): Unit = {
      memoryLimit -= memory
    }

    def flushAll: Unit = {
      val buffer = if (comparator.isDefined) partitionMapArray else partitionBufferArray
      buffer.foreach(e => e.writeAndFlush)
    }

    def close: Unit = {
      val buffer = if (comparator.isDefined) partitionMapArray else partitionBufferArray
      buffer.foreach(b => b.close)
    }

    def spill(size: Long, trigger: MemoryConsumer): Long = ???
  }

  private[daos] trait SizeAware[K, C] {
    this: MemoryConsumer =>

    protected var writeCount = 0

    protected var lastSize = 0L

    protected var _pairsWriter: PartitionOutput = null

    def partitionId: Int

    def writeThreshold: Int

    def estimatedSize: Long

    def totalBufferInitial: Long

    def iterator: Iterator[(K, C)]

    def reset: Unit

    def parent: PartitionsBuffer[K, C]

    def pairsWriter: PartitionOutput

    def updateTotalSize(estSize: Long): Unit = {
      val diff = estSize - lastSize
      if (diff > 0) {
        lastSize = estSize
        parent.updateTotalSize(diff)
      }
    }

    def releaseMemory(memory: Long): Unit = {
      freeMemory(memory)
      parent.releaseMemory(memory)
    }

    private def writeAndFlush(memory: Long): Unit = {
      val writer = if (_pairsWriter != null) _pairsWriter else pairsWriter
      var count = 0
      iterator.foreach(p => {
        writer.write(p._1, p._2)
        count += 1
      })
      if (count > 0) {
        writer.flush // force write
        writeCount += count
        lastSize = 0
        parent.updateTotalSize(-memory)
        releaseMemory(memory - totalBufferInitial)
        reset
      }
    }

    def writeAndFlush: Unit = {
      writeAndFlush(estimatedSize)
    }

    def maybeWrite(memory: Long): Boolean = {
      if (memory < writeThreshold) {
        false
      } else {
        writeAndFlush(memory)
        true
      }
    }

    def afterUpdate(estSize: Long): Long = {
      if (maybeWrite(estSize)) {
        0L
      } else {
        updateTotalSize(estSize)
        estSize
      }
    }

    def close: Unit = {
      if (_pairsWriter != null) {
        _pairsWriter.close
        _pairsWriter = null
      }
    }
  }

  private[daos] trait Linked[K, C] {
    this: SizeAware[K, C] =>

    var prev: Linked[K, C] with SizeAware[K, C] = null
    var next: Linked[K, C] with SizeAware[K, C] = null
  }

  private class SizeAwareMap[K, C](
      val partitionId: Int,
      val writeThreshold: Int,
      val totalBufferInitial: Long,
      taskMemoryManager: TaskMemoryManager,
      val parent: PartitionsBuffer[K, C]) extends MemoryConsumer(taskMemoryManager)
      with Linked[K, C] with SizeAware[K, C] {

    private var map = new SizeSamplerAppendOnlyMap[K, C](parent.sampleStat)
    private var _estSize: Long = _

    def estimatedSize: Long = _estSize

    def changeValue(key: K, updateFunc: (Boolean, C) => C): Long = {
      map.changeValue(key, updateFunc)
      _estSize = map.estimateSize()
      afterUpdate(_estSize)
    }

    def reset: Unit = {
      map = new SizeSamplerAppendOnlyMap[K, C](parent.sampleStat)
      _estSize = map.estimateSize()
    }

    def iterator(): Iterator[(K, C)] = {
      map.destructiveSortedIterator(parent.keyComparator.get)
    }

    def spill(size: Long, trigger: MemoryConsumer): Long = {
      val curSize = _estSize
      writeAndFlush
      curSize
    }

    def pairsWriter: PartitionOutput = {
      if (_pairsWriter == null) {
        _pairsWriter = new PartitionOutput(shuffleId, context.taskAttemptId(), partitionId, serializerManager,
          serInstance, daosWriter, writeMetrics)
      }
      _pairsWriter
    }
  }

  private class SizeAwareBuffer[K, C](
    val partitionId: Int,
    val writeThreshold: Int,
    val totalBufferInitial: Long,
    taskMemoryManager: TaskMemoryManager,
    val parent: PartitionsBuffer[K, C]) extends MemoryConsumer(taskMemoryManager)
    with Linked[K, C] with SizeAware[K, C] {

    private var buffer = new SizeSamplerPairBuffer[K, C](parent.sampleStat)
    private var _estSize: Long = _

    def estimatedSize: Long = _estSize

    def insert(key: K, value: C): Long = {
      buffer.insert(key, value)
      _estSize = buffer.estimateSize()
      afterUpdate(_estSize)
    }

    def reset: Unit = {
      buffer = new SizeSamplerPairBuffer[K, C](parent.sampleStat)
      _estSize = buffer.estimateSize()
    }

    def iterator(): Iterator[(K, C)] = {
      buffer.iterator()
    }

    def spill(size: Long, trigger: MemoryConsumer): Long = {
      val curSize = _estSize
      writeAndFlush
      curSize
    }

    def pairsWriter: PartitionOutput = {
      if (_pairsWriter == null) {
        _pairsWriter = new PartitionOutput(shuffleId, context.taskAttemptId(), partitionId, serializerManager,
          serInstance, daosWriter, writeMetrics)
      }
      _pairsWriter
    }
  }
}

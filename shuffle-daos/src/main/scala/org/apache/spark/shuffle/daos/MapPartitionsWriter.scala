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

import java.util.Comparator

import org.apache.spark._
import org.apache.spark.internal.Logging
import org.apache.spark.memory.{MemoryConsumer, TaskMemoryManager}
import org.apache.spark.serializer.Serializer
import org.apache.spark.shuffle.ShuffleWriteMetricsReporter
import org.apache.spark.shuffle.daos.MapPartitionsWriter._

class MapPartitionsWriter[K, V, C](
    shuffleId: Int,
    context: TaskContext,
    shuffleIO: DaosShuffleIO,
    aggregator: Option[Aggregator[K, V, C]] = None,
    partitioner: Option[Partitioner] = None,
    ordering: Option[Ordering[K]] = None,
    serializer: Serializer = SparkEnv.get.serializer) extends Logging {

  private val conf = SparkEnv.get.conf

  val spillFirst = conf.get(SHUFFLE_DAOS_SPILL_FIRST)
  val lowGrantWatermark = if (spillFirst) {
      val exeMem = conf.get(org.apache.spark.internal.config.EXECUTOR_MEMORY) * 1024 * 1024
      val memFraction = conf.get(org.apache.spark.internal.config.MEMORY_FRACTION)
      val execCores = conf.get(org.apache.spark.internal.config.EXECUTOR_CORES)
      val cpusPerTask = conf.get(org.apache.spark.internal.config.CPUS_PER_TASK)
      val maxMemPerTask = (exeMem - 300 * 1024 * 1024) * memFraction * cpusPerTask / execCores
      (maxMemPerTask * conf.get(SHUFFLE_DAOS_SPILL_GRANT_PCT)).toLong
    } else {
      0L
    }
  logInfo("lowGrantWatermark: " + lowGrantWatermark)

  private val numPartitions = partitioner.map(_.numPartitions).getOrElse(1)
  private val shouldPartition = numPartitions > 1
  private def getPartition(key: K): Int = {
    if (shouldPartition) partitioner.get.getPartition(key) else 0
  }

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
  @volatile var writeBuffer = new PartitionsBuffer(
    shuffleId,
    numPartitions,
    aggregator,
    comparator,
    conf,
    context.taskMemoryManager(),
    shuffleIO,
    serializer)

  private[this] var _elementsRead = 0

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
  }

  def commitAll: Array[Long] = {
    writeBuffer.flushAll
    writeBuffer.close
    writeBuffer.daosWriter.flushAll()
    writeBuffer.daosWriter.getPartitionLens(numPartitions)
  }

  def close: Unit = {
    // serialize rest of records
    writeBuffer.daosWriter.close
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
  private[daos] class PartitionsBuffer(
      val shuffleId: Int,
      numPartitions: Int,
      val aggregator: Option[Aggregator[K, V, C]],
      val keyComparator: Option[Comparator[K]],
      val conf: SparkConf,
      val taskMemManager: TaskMemoryManager,
      val shuffleIO: DaosShuffleIO,
      val serializer: Serializer) extends MemoryConsumer(taskMemManager) {
    private val totalBufferInitial = conf.get(SHUFFLE_DAOS_WRITE_BUFFER_INITIAL_SIZE).toInt * 1024 * 1024
    private val forceWritePct = conf.get(SHUFFLE_DAOS_WRITE_BUFFER_FORCE_PCT)
    private val partMoveInterval = conf.get(SHUFFLE_DAOS_WRITE_PARTITION_MOVE_INTERVAL)
    private val totalWriteInterval = conf.get(SHUFFLE_DAOS_WRITE_TOTAL_INTERVAL)
    private[daos] val sampleStat = new SampleStat

    // track spill status
    private[daos] var merging = false
    private var lowGranted = 0

    val taskContext = context
    val serializerManager = SparkEnv.get.serializerManager
    val serInstance = serializer.newInstance()
    val writeMetrics = context.taskMetrics().shuffleWriteMetrics
    val spillWriteMetrics = new DummyShuffleWriteMetrics

    val needSpill = aggregator.isDefined
    val daosWriter = shuffleIO.getDaosWriter(
      numPartitions,
      shuffleId,
      context.taskAttemptId())
    if (needSpill) {
      daosWriter.enableSpill()
    }

    if (log.isDebugEnabled()) {
      log.debug("totalBufferInitial: " + totalBufferInitial)
      log.debug("forceWritePct: " + forceWritePct)
      log.debug("partMoveInterval: " + partMoveInterval)
      log.debug("totalWriteInterval: " + totalWriteInterval)
    }

    private var totalSize = 0L
    private var memoryLimit = totalBufferInitial * 1L
    private var largestSize = 0L

    var peakSize = 0L

    private def initialize[T >: Linked[K, V, C] with SizeAware[K, V, C]]():
      (T, T, Array[SizeAwarePartMap], Array[SizeAwarePartBuffer]) = {
      // create virtual partition head and end, as well as all linked partitions
      val (partitionMapArray, partitionBufferArray) = if (comparator.isDefined) {
        (new Array[SizeAwarePartMap](numPartitions), null)
      } else {
        (null, new Array[SizeAwarePartBuffer](numPartitions))
      }
      val (head, end) = if (comparator.isDefined) {
        val mapHead = new SizeAwarePartMap(-1, this)
        val mapEnd = new SizeAwarePartMap(-2, this)
        (0 until numPartitions).foreach(i => {
          val map = new SizeAwarePartMap(i, this)
          partitionMapArray(i) = map
          if (i > 0) {
            val prevMap = partitionMapArray(i - 1)
            prevMap.next = map
            map.prev = prevMap
          }
        })
        (mapHead, mapEnd)
      } else {
        val bufferHead = new SizeAwarePartBuffer(-1, this)
        val bufferEnd = new SizeAwarePartBuffer(-2, this)
        (0 until numPartitions).foreach(i => {
          val buffer = new SizeAwarePartBuffer(i, this)
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

    private def moveToFirst(node: Linked[K, V, C] with SizeAware[K, V, C]): Unit = {
      if (head.next != node) {
        // remove node from list
        node.prev.next = node.next
        node.next.prev = node.prev
        // move to first
        node.next = head.next
        head.next.prev = node
        head.next = node
        node.prev = head
      }
    }

    private def moveToLast(node: Linked[K, V, C] with SizeAware[K, V, C]): Unit = {
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
      if (estSize == 0 || map.numOfRecords % partMoveInterval == 0) {
        movePartition(estSize, map)
      }
      if (totalSize > memoryLimit & (sampleStat.numUpdates % totalWriteInterval == 0)) {
        // check if total buffer exceeds memory limit
        maybeWriteTotal()
      }
    }

    def insert(partitionId: Int, key: K, value: C): Unit = {
      val buffer = partitionBufferArray(partitionId)
      val estSize = buffer.insert(key, value)
      if (estSize == 0 || buffer.numOfRecords % partMoveInterval == 0) {
        movePartition(estSize, buffer)
      }
      if (totalSize > memoryLimit & (sampleStat.numUpdates % totalWriteInterval == 0)) {
        // check if total buffer exceeds memory limit
        maybeWriteTotal()
      }
    }

    def movePartition[T <: SizeAware[K, V, C] with Linked[K, V, C]](estSize: Long, buffer: T): Unit = {
      if (estSize > largestSize) {
        largestSize = estSize
        moveToFirst(buffer)
      } else if (estSize == 0) {
        moveToLast(buffer)
      }
    }

    /**
     * move range of nodes from start to until nextNotFlushed which is before the end
     * @param nextNotFlushed
     */
    private def moveFromStartToLast(nextNotFlushed: Linked[K, V, C] with SizeAware[K, V, C]): Unit = {
      val start = head.next
      val last = nextNotFlushed.prev
      if (start != last) {
        // de-link
        head.next = nextNotFlushed
        nextNotFlushed.prev = head
        // move to last
        nextNotFlushed.next = start
        start.prev = nextNotFlushed
        last.next = end
        end.prev = last
      } else {
        moveToLast(start)
      }
    }

    /**
     * At least one node being written.
     * @param size
     * @return
     */
    private def writeFromHead(size: Long): Long = {
      var buffer = head.next
      var totalWritten = 0L
      while ((buffer != end) & totalWritten < size) {
        totalWritten += buffer.writeAndFlush
        buffer = buffer.next
      }
      if (buffer != end) {
        largestSize = buffer.estimatedSize
        moveFromStartToLast(buffer)
      } else {
        largestSize = 0L
      }
      totalWritten
    }

    private def maybeWriteTotal(): Unit = {
      val limit = 2 * totalSize
      val memRequest = limit - memoryLimit
      val granted = acquireMemory(memRequest)
      memoryLimit += granted
      if (granted < memRequest) {
        lowGranted += 1
        if (!spillFirst) {
          writeFromHead(memRequest - granted)
        } else if (granted < lowGrantWatermark | (lowGranted >= 2)) {
          writeFromHead(totalSize - totalBufferInitial)
        }
      } else {
        lowGranted = 0
      }
    }

    def updateTotalSize(diff: Long): Unit = {
      totalSize += diff
      if (totalSize > peakSize) {
        peakSize = totalSize
      }
    }

    def releaseMemory(memory: Long): Unit = {
      freeMemory(Math.min(memory, memoryLimit - totalBufferInitial))
      memoryLimit -= memory
      if (memoryLimit < totalBufferInitial) {
        memoryLimit = totalBufferInitial
      }
    }

    def flushAll: Unit = {
      val buffer = if (comparator.isDefined) partitionMapArray else partitionBufferArray
      if (!needSpill) {
        buffer.foreach(e => {
          e.writeAndFlush
          e.close
        })
      } else {
        // no more spill for existing in-mem data
        daosWriter.startMerging()
        merging = true
        var totalDiskSpilled = 0L
        var totalMemSpilled = 0L
        buffer.foreach(e => {
          totalDiskSpilled += e.merge
          totalMemSpilled += e.spillMemSize
        })
        context.taskMetrics().incDiskBytesSpilled(totalDiskSpilled)
        context.taskMetrics().incMemoryBytesSpilled(totalMemSpilled)
      }
      context.taskMetrics().incPeakExecutionMemory(peakSize)
    }

    def close: Unit = {
      val allocated = memoryLimit - totalBufferInitial
      // partitions already closed in flushAll
      if (allocated > 0) {
        freeMemory(allocated)
      }
    }

    override def spill(size: Long, trigger: MemoryConsumer): Long = {
      0L
    }
  }

  private class SizeAwarePartMap(
      val partitionId: Int,
      val parent: PartitionsBuffer) extends
      {
        val pairsDefaultWriter = new PartitionOutput[K, V, C](partitionId, parent, parent.writeMetrics)
      } with Linked[K, V, C] with SizeAware[K, V, C] {

    private var map = new SizeSamplerAppendOnlyMap[K, C](parent.sampleStat)
    private var _estSize: Long = _

    def estimatedSize: Long = _estSize

    def changeValue(key: K, updateFunc: (Boolean, C) => C): Long = {
      map.changeValue(key, updateFunc)
      _estSize = map.estimateSize()
      afterUpdate(_estSize)
    }

    override def numOfRecords: Int = map.numOfRecords

    def reset: Unit = {
      map = new SizeSamplerAppendOnlyMap[K, C](parent.sampleStat)
      _estSize = map.estimateSize()
    }

    def iterator(): Iterator[(K, C)] = {
      map.destructiveSortedIterator(parent.keyComparator.get)
    }
  }

  private class SizeAwarePartBuffer(
    val partitionId: Int,
    val parent: PartitionsBuffer) extends
    {
      val pairsDefaultWriter = new PartitionOutput[K, V, C](partitionId, parent, parent.writeMetrics)
    } with Linked[K, V, C] with SizeAware[K, V, C] {

    private var buffer = new SizeSamplerPairBuffer[K, C](parent.sampleStat)
    private var _estSize: Long = _

    def estimatedSize: Long = _estSize

    def insert(key: K, value: C): Long = {
      buffer.insert(key, value)
      _estSize = buffer.estimateSize()
      afterUpdate(_estSize)
    }

    override def numOfRecords: Int = buffer.numOfRecords

    def reset: Unit = {
      buffer = new SizeSamplerPairBuffer[K, C](parent.sampleStat)
      _estSize = buffer.estimateSize()
    }

    def iterator(): Iterator[(K, C)] = {
      buffer.iterator()
    }
  }
}

object MapPartitionsWriter {

  private[daos] trait SizeAware[K, V, C] {

    protected var totalSpillMem = 0L

    protected var lastSize = 0L

    val pairsDefaultWriter: PartitionOutput[K, V, C]

    val partitionId: Int

    val parent: MapPartitionsWriter[K, V, C]#PartitionsBuffer

    val shuffleId: Int = parent.shuffleId

    val daosWriter: DaosWriter = parent.daosWriter

    val aggregator: Option[Aggregator[K, V, C]] = parent.aggregator

    val keyComparator: Option[Comparator[K]] = parent.keyComparator

    val shuffleIO: DaosShuffleIO = parent.shuffleIO

    val serializer = parent.serializer

    def numOfRecords: Int

    def estimatedSize: Long

    def iterator: Iterator[(K, C)]

    def spillMemSize: Long = totalSpillMem

    def reset: Unit

    def updateTotalSize(estSize: Long): Unit = {
      val diff = estSize - lastSize
      if (diff > 0) {
        lastSize = estSize
        parent.updateTotalSize(diff)
      }
    }

    def releaseMemory(memory: Long): Unit = {
      parent.releaseMemory(memory)
      parent.updateTotalSize(-memory)
    }

    def pairsWriter: PartitionOutput[K, V, C] = {
      if ((!parent.needSpill) | parent.merging) pairsDefaultWriter
        else new PartitionOutput[K, V, C](partitionId, parent, parent.spillWriteMetrics)
    }

    def postFlush(memory: Long): Unit = {
      lastSize = 0
      releaseMemory(memory)
      reset
    }

    /**
     * supposed to be non-empty buffer.
     *
     * @param memory
     * @return
     */
    private def writeAndFlush(memory: Long): Long = {
      val pw = pairsWriter
      iterator.foreach(p => {
        pw.writeAutoFlush(p._1, p._2)
      })
      if (pw == pairsDefaultWriter) {
        pw.flush // force write
      } else {
        pw.close // writer for spill, so flush all and close
        totalSpillMem += memory
      }
      postFlush(memory)
      memory
    }

    def writeAndFlush: Long = {
      if (numOfRecords > 0) {
        writeAndFlush(estimatedSize)
      } else {
        0L
      }
    }

    def merge: Long = {
      if (daosWriter.isSpilled(partitionId)) { // partition actually spilled ?
        val merger = new PartitionMerger[K, V, C](this, shuffleIO, serializer)
        val spilledSize = merger.mergeAndOutput
        postFlush(estimatedSize)
        close
        spilledSize
      } else {
        writeAndFlush
        close
        0L
      }
    }

    def afterUpdate(estSize: Long): Long = {
      updateTotalSize(estSize)
      estSize
    }

    def close: Unit = {
      pairsDefaultWriter.close
    }
  }

  private[daos] trait Linked[K, V, C] {
    this: SizeAware[K, V, C] =>

    var prev: Linked[K, V, C] with SizeAware[K, V, C] = null
    var next: Linked[K, V, C] with SizeAware[K, V, C] = null
  }

  private[daos] class DummyShuffleWriteMetrics extends ShuffleWriteMetricsReporter {
    override private[spark] def incBytesWritten(v: Long): Unit = {}

    override private[spark] def incRecordsWritten(v: Long): Unit = {}

    override private[spark] def incWriteTime(v: Long): Unit = {}

    override private[spark] def decBytesWritten(v: Long): Unit = {}

    override private[spark] def decRecordsWritten(v: Long): Unit = {}
  }
}

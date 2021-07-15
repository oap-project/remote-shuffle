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

import java.util.{Comparator, UUID}

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.SparkEnv
import org.apache.spark.executor.TempShuffleReadMetrics
import org.apache.spark.shuffle.daos.DaosReader.ReaderConfig
import org.apache.spark.shuffle.daos.DaosWriter.SpillInfo
import org.apache.spark.shuffle.daos.MapPartitionsWriter.SizeAware
import org.apache.spark.storage.{BlockId, TempLocalBlockId}

class PartitionMerger[K, V, C] (
  val part: SizeAware[K, V, C],
  val io: DaosShuffleIO,
  val conf: ReaderConfig) {

  def mergeAndOutput: Long = {
    val pw = part.pairsWriter
    pw.resetMetrics
    val infoList = part.daosWriter.getSpillInfo(part.partitionId)
    val nbrOfSpill = infoList.size()
    var totalSpilled = 0L
    val iterators = infoList.asScala.map(info => {
      totalSpilled += info.getSize
      new SpillIterator[K, C](info, nbrOfSpill)
    }) ++ Seq(part.iterator)
    val it = mergeWithAggregation(iterators, part.aggregator.get.mergeCombiners, part.keyComparator.get)
    pw.setMerged
    while (it.hasNext) {
      val item = it.next()
      part.pairsWriter.writeAutoFlush(item._1, item._2)
    }
    pw.flush
    totalSpilled
  }

  /**
   * Merge a sequence of (K, C) iterators by aggregating values for each key, assuming that each
   * iterator is sorted by key with a given comparator. If the comparator is not a total ordering
   * (e.g. when we sort objects by hash code and different keys may compare as equal although
   * they're not), we still merge them by doing equality tests for all keys that compare as equal.
   */
  private def mergeWithAggregation(
    iterators: Seq[Iterator[Product2[K, C]]],
    mergeCombiners: (C, C) => C,
    comparator: Comparator[K]): Iterator[Product2[K, C]] = {
    // We only have a partial ordering, e.g. comparing the keys by hash code, which means that
    // multiple distinct keys might be treated as equal by the ordering. To deal with this, we
    // need to read all keys considered equal by the ordering at once and compare them.
    val it = new Iterator[Iterator[Product2[K, C]]] {
      val sorted = mergeSort(iterators, comparator).buffered

      // Buffers reused across elements to decrease memory allocation
      val keys = new ArrayBuffer[K]
      val combiners = new ArrayBuffer[C]

      override def hasNext: Boolean = sorted.hasNext

      override def next(): Iterator[Product2[K, C]] = {
        if (!hasNext) {
          throw new NoSuchElementException
        }
        keys.clear()
        combiners.clear()
        val firstPair = sorted.next()
        keys += firstPair._1
        combiners += firstPair._2
        val key = firstPair._1
        while (sorted.hasNext && comparator.compare(sorted.head._1, key) == 0) {
          val pair = sorted.next()
          var i = 0
          var foundKey = false
          while (i < keys.size && !foundKey) {
            if (keys(i) == pair._1) {
              combiners(i) = mergeCombiners(combiners(i), pair._2)
              foundKey = true
            }
            i += 1
          }
          if (!foundKey) {
            keys += pair._1
            combiners += pair._2
          }
        }

        // Note that we return an iterator of elements since we could've had many keys marked
        // equal by the partial order; we flatten this below to get a flat iterator of (K, C).
        keys.iterator.zip(combiners.iterator)
      }
    }
    it.flatten
  }

  private def mergeSort(iterators: Seq[Iterator[Product2[K, C]]], comparator: Comparator[K])
    : Iterator[Product2[K, C]] = {
    val bufferedIters = iterators.filter(_.hasNext).map(_.buffered)
    type Iter = BufferedIterator[Product2[K, C]]
    // Use the reverse order (compare(y,x)) because PriorityQueue dequeues the max
    val heap = new mutable.PriorityQueue[Iter]()(
      (x: Iter, y: Iter) => comparator.compare(y.head._1, x.head._1))
    heap.enqueue(bufferedIters: _*)  // Will contain only the iterators with hasNext = true
    new Iterator[Product2[K, C]] {
      override def hasNext: Boolean = heap.nonEmpty

      override def next(): Product2[K, C] = {
        if (!hasNext) {
          throw new NoSuchElementException
        }
        val firstBuf = heap.dequeue()
        val firstPair = firstBuf.next()
        if (firstBuf.hasNext) {
          heap.enqueue(firstBuf)
        }
        firstPair
      }
    }
  }

  class SpillIterator[K, C] (
    val info: SpillInfo,
    val nbrOfSpill: Int) extends Iterator[Product2[K, C]] {

    import PartitionMerger._

    private val reader = io.getDaosReader(part.shuffleId)
    private val map = new java.util.LinkedHashMap[(java.lang.Long, Integer), (java.lang.Long, BlockId)](1)
    map.put((java.lang.Long.valueOf(info.getMapId), Integer.valueOf(info.getReduceId)),
      (info.getSize, dummyBlockId))
    private val serializerManager = SparkEnv.get.serializerManager
    private val serializer = SparkEnv.get.serializer
    private val daosStream = new DaosShuffleInputStream(reader, map, conf.getMaxMem/nbrOfSpill, conf.getMaxMem,
      dummyReadMetrics)
    private val wrappedStream = serializerManager.wrapStream(dummyBlockId, daosStream)
    private val deStream = serializer.newInstance().deserializeStream(wrappedStream)
    private val it = deStream.asKeyValueIterator.asInstanceOf[Iterator[(K, C)]]

    override def hasNext: Boolean = {
      val ret = it.hasNext
      if (!ret) {
        deStream.close()
        daosStream.close(true)
      }
      ret
    }

    override def next(): Product2[K, C] = {
      it.next()
    }
  }
}

object PartitionMerger {
  val dummyBlockId = TempLocalBlockId(UUID.randomUUID())
  val dummyReadMetrics = new TempShuffleReadMetrics
}

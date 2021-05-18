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

import org.mockito.{Mock, Mockito, MockitoAnnotations}
import org.mockito.Answers._
import org.mockito.ArgumentMatchers.{any, anyInt}
import org.mockito.Mockito.{mock, never, when}
import org.scalatest.Matchers
import scala.collection.mutable

import org.apache.spark.{Partitioner, SharedSparkContext, ShuffleDependency, SparkFunSuite}
import org.apache.spark.memory.MemoryTestingUtils
import org.apache.spark.serializer.JavaSerializer
import org.apache.spark.shuffle.BaseShuffleHandle
import org.apache.spark.util.Utils

class DaosShuffleWriterSuite extends SparkFunSuite with SharedSparkContext with Matchers {

  @Mock(answer = RETURNS_SMART_NULLS)
  private var shuffleIO: DaosShuffleIO = _

  private val shuffleId = 0
  private val numMaps = 5
  private var shuffleHandle: BaseShuffleHandle[Int, Int, Int] = _
  private val serializer = new JavaSerializer(conf)

  private val singleBufSize = conf.get(SHUFFLE_DAOS_WRITE_SINGLE_BUFFER_SIZE) * 1024 * 1024
  private val minSize = conf.get(SHUFFLE_DAOS_WRITE_MINIMUM_SIZE) * 1024

  override def beforeEach(): Unit = {
    super.beforeEach()
    MockitoAnnotations.initMocks(this)
    val partitioner = new Partitioner() {
      def numPartitions = numMaps

      def getPartition(key: Any) = Utils.nonNegativeMod(key.hashCode, numPartitions)
    }
    shuffleHandle = {
      val dependency = mock(classOf[ShuffleDependency[Int, Int, Int]])
      when(dependency.partitioner).thenReturn(partitioner)
      when(dependency.serializer).thenReturn(serializer)
      when(dependency.aggregator).thenReturn(None)
      when(dependency.keyOrdering).thenReturn(None)
      new BaseShuffleHandle(shuffleId, dependency)
    }
  }

  test("write empty data") {
    val context = MemoryTestingUtils.fakeTaskContext(sc.env)
    val daosWriter: DaosWriter = Mockito.mock(classOf[DaosWriter])
    when(shuffleIO.getDaosWriter(5, shuffleId, context.taskAttemptId()))
      .thenReturn(daosWriter)
    val partitionLengths = Array[Long](5)
    when(daosWriter.getPartitionLens(numMaps)).thenReturn(partitionLengths)

    val writer = new DaosShuffleWriter[Int, Int, Int](shuffleHandle, shuffleId, context, shuffleIO)
    writer.write(Iterator.empty)
    writer.stop(success = true)
    val writeMetrics = context.taskMetrics().shuffleWriteMetrics
    assert(writeMetrics.bytesWritten === 0)
    assert(writeMetrics.recordsWritten === 0)

    Mockito.verify(daosWriter, never()).write(any, anyInt())
    Mockito.verify(daosWriter, never()).write(any, any(classOf[Array[Byte]]))
  }

  test("write with some records") {
    val context = MemoryTestingUtils.fakeTaskContext(sc.env)
    val dataList = new mutable.MutableList[(Int, Array[Byte])]()

    val records = List[(Int, Int)]((1, 2), (2, 3), (4, 4), (6, 5))

    val daosWriter: DaosWriter = Mockito.mock(classOf[DaosWriter])
    when(shuffleIO.getDaosWriter(5, shuffleId, context.taskAttemptId()))
      .thenReturn(daosWriter)
    val partitionLengths = Array[Long](5)
    when(daosWriter.getPartitionLens(numMaps)).thenReturn(partitionLengths)

    val writer = new DaosShuffleWriter[Int, Int, Int](shuffleHandle, shuffleId, context, shuffleIO)
    writer.write(records.toIterator)
    writer.stop(success = true)
    val writeMetrics = context.taskMetrics().shuffleWriteMetrics
    assert(395 === writeMetrics.bytesWritten)
    assert(records.size === writeMetrics.recordsWritten)
  }
}

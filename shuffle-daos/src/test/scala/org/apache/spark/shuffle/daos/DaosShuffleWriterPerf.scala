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
import org.mockito.Mockito.{mock, when}
import scala.collection.mutable
import scala.util.Random

import org.apache.spark.{Partitioner, SharedSparkContext, ShuffleDependency, SparkFunSuite}
import org.apache.spark.memory.MemoryTestingUtils
import org.apache.spark.serializer.JavaSerializer
import org.apache.spark.shuffle.BaseShuffleHandle
import org.apache.spark.util.Utils

class DaosShuffleWriterPerf extends SparkFunSuite with SharedSparkContext {

  @Mock(answer = RETURNS_SMART_NULLS)
  private var shuffleIO: DaosShuffleIO = _

  private val shuffleId = 0
  private val numMaps = 1000
  private var shuffleHandle: BaseShuffleHandle[Int, Array[Byte], Array[Byte]] = _
  private val serializer = new JavaSerializer(conf)

  private val singleBufSize = conf.get(SHUFFLE_DAOS_WRITE_SINGLE_BUFFER_SIZE) * 1024 * 1024
  private val minSize = conf.get(SHUFFLE_DAOS_WRITE_MINIMUM_SIZE) * 1024

  conf.set(SHUFFLE_DAOS_WRITE_PARTITION_BUFFER_SIZE, 100L)
  conf.set(SHUFFLE_DAOS_WRITE_BUFFER_SIZE, 80L)

  override def beforeEach(): Unit = {
    super.beforeEach()
    MockitoAnnotations.initMocks(this)
    val partitioner = new Partitioner() {
      def numPartitions = numMaps

      def getPartition(key: Any) = Utils.nonNegativeMod(key.hashCode, numPartitions)
    }
    shuffleHandle = {
      val dependency = mock(classOf[ShuffleDependency[Int, Array[Byte], Array[Byte]]])
      when(dependency.partitioner).thenReturn(partitioner)
      when(dependency.serializer).thenReturn(serializer)
      when(dependency.aggregator).thenReturn(None)
      when(dependency.keyOrdering).thenReturn(None)
      new BaseShuffleHandle(shuffleId, dependency)
    }
  }

  test("write with some records") {
    val context = MemoryTestingUtils.fakeTaskContext(sc.env)
    val bytes = new Array[Byte](100)
    (0 until 100).foreach(i => bytes(i) = (i + 1).toByte)
    val records = new mutable.MutableList[(Int, Array[Byte])]()
    val size = 128 * 1024 * 1024
    var count = 0
    var index = 0
    while (count < size) {
      records += ((index, bytes))
      index += 1
      count += 100
    }

//    val records = List[(Int, Int)]((1, 2), (2, 3), (4, 4), (6, 5))

    val daosWriter: DaosWriter = Mockito.mock(classOf[DaosWriter])
    when(shuffleIO.getDaosWriter(numMaps, shuffleId, context.taskAttemptId()))
      .thenReturn(daosWriter)
    val partitionLengths = Array[Long](numMaps)
    when(daosWriter.getPartitionLens(numMaps)).thenReturn(partitionLengths)

    val writer = new DaosShuffleWriter[Int, Array[Byte], Array[Byte]](shuffleHandle, shuffleId, context, shuffleIO)
    val start = System.currentTimeMillis()
    writer.write(records.map(k => {
      val p = new Random(util.hashing.byteswap32(k._1)).nextInt(numMaps)
      (p, k._2)
    }).iterator)
    logInfo(s"time: ${System.currentTimeMillis() - start}")
    writer.stop(success = true)
    val writeMetrics = context.taskMetrics().shuffleWriteMetrics
    assert(records.size === writeMetrics.recordsWritten)
  }
}

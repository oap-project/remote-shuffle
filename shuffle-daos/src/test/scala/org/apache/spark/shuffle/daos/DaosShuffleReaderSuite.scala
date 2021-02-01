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

import java.io.ByteArrayOutputStream
import java.util

import io.daos.BufferAllocator
import io.daos.obj.{DaosObject, IODataDesc}
import org.mockito._
import org.mockito.Mockito.{doNothing, mock, when}

import org.apache.spark._
import org.apache.spark.internal.config
import org.apache.spark.serializer.{JavaSerializer, SerializerManager}
import org.apache.spark.shuffle.BaseShuffleHandle
import org.apache.spark.shuffle.daos.DaosReader.ReaderConfig
import org.apache.spark.storage.{BlockManagerId, ShuffleBlockId}

class DaosShuffleReaderSuite extends SparkFunSuite with LocalSparkContext {

  override def beforeAll(): Unit = {
    super.beforeAll()
    logInfo("start executors in DaosReaderSync " + classOf[DaosReaderSync])
    MockitoAnnotations.initMocks(this)
  }

  private def mockObjectsForSingleDaosCall(reduceId: Int, numMaps: Int, byteOutputStream: ByteArrayOutputStream):
    (DaosReader, DaosShuffleIO, DaosObject) = {
    // mock
    val daosObject = Mockito.mock(classOf[DaosObject])
    val daosReader: DaosReaderSync = new DaosReaderSync(daosObject, new ReaderConfig(), null)
    val shuffleIO = Mockito.mock(classOf[DaosShuffleIO])

    val desc = Mockito.mock(classOf[IODataDesc])
    when(daosObject.createDataDescForFetch(String.valueOf(reduceId), IODataDesc.IodType.ARRAY, 1))
      .thenReturn(desc)
    doNothing().when(daosObject).fetch(desc)
    when(desc.getNbrOfEntries()).thenReturn(numMaps)

    (0 until numMaps).foreach(i => {
      val entry = Mockito.mock(classOf[desc.Entry])
      when(desc.getEntry(i)).thenReturn(entry)
      val buf = BufferAllocator.objBufWithNativeOrder(byteOutputStream.size())
      buf.writeBytes(byteOutputStream.toByteArray)
      when(entry.getFetchedData).thenReturn(buf)
      when(entry.getKey).thenReturn(String.valueOf(i))
    })
    (daosReader, shuffleIO, daosObject)
  }

  private def mockObjectsForMultipleDaosCall(reduceId: Int, numMaps: Int, byteOutputStream: ByteArrayOutputStream,
                                             executors: BoundThreadExecutors):
  (DaosReader, DaosShuffleIO, DaosObject) = {
    // mock
    val daosObject = Mockito.mock(classOf[DaosObject])
    val daosReader: DaosReaderSync =
      if (executors != null) Mockito.spy(new DaosReaderSync(daosObject, new DaosReader.ReaderConfig(),
        executors.nextExecutor()))
      else new DaosReaderSync(daosObject, new ReaderConfig(), null)
    val shuffleIO = Mockito.mock(classOf[DaosShuffleIO])
    val descList = new util.ArrayList[IODataDesc]

    (0 until numMaps).foreach(_ => {
      descList.add(Mockito.mock(classOf[IODataDesc]))
    })
    val times = Array[Int](1)
    times(0) = 0
    when(daosObject.createDataDescForFetch(String.valueOf(reduceId), IODataDesc.IodType.ARRAY, 1))
      .thenAnswer(i => {
        val desc = descList.get(times(0))
        times(0) += 1
        desc
      })

    (0 until numMaps).foreach(i => {
      val desc = descList.get(i)
      doNothing().when(daosObject).fetch(desc)
      when(desc.getNbrOfEntries()).thenReturn(1)
      when(desc.isSucceeded()).thenReturn(true)
      when(desc.getTotalRequestSize).thenReturn(byteOutputStream.toByteArray.length)
      val entry = Mockito.mock(classOf[desc.Entry])
      when(desc.getEntry(0)).thenReturn(entry)
      val buf = BufferAllocator.objBufWithNativeOrder(byteOutputStream.size())
      buf.writeBytes(byteOutputStream.toByteArray)
      when(entry.getFetchedData).thenReturn(buf)
      when(entry.getKey).thenReturn(String.valueOf(i))
    })
    (daosReader, shuffleIO, daosObject)
  }

  private def testRead(keyValuePairsPerMap: Int, numMaps: Int, singleCall: Boolean = true,
                       executors: BoundThreadExecutors = null): Unit = {
    val testConf = new SparkConf(false)
    testConf.set(SHUFFLE_DAOS_READ_FROM_OTHER_THREAD, executors != null)

    // Create a buffer with some randomly generated key-value pairs to use as the shuffle data
    // from each mappers (all mappers return the same shuffle data).
    val serializer = new JavaSerializer(testConf)
    val byteOutputStream = new ByteArrayOutputStream()
    val serializationStream = serializer.newInstance().serializeStream(byteOutputStream)
    (0 until keyValuePairsPerMap).foreach { i =>
      serializationStream.writeKey(i)
      serializationStream.writeValue(2*i)
    }

    if (!singleCall) {
      val value = math.ceil(byteOutputStream.toByteArray.length.toDouble / 1024).toInt
      testConf.set(SHUFFLE_DAOS_READ_MAX_BYTES_IN_FLIGHT, value.toLong)
      testConf.set(SHUFFLE_DAOS_READ_MINIMUM_SIZE, value.toLong)
    }

    val reduceId = 15
    val shuffleId = 22
    // Create a SparkContext as a convenient way of setting SparkEnv (needed because some of the
    // shuffle code calls SparkEnv.get()).
    sc = new SparkContext("local", "test", testConf)

    // Make a mocked MapOutputTracker for the shuffle reader to use to determine what
    // shuffle data to read.
    val mapOutputTracker = mock(classOf[MapOutputTracker])
    val localBlockManagerId = BlockManagerId("test-client", "test-client", 1)
    when(mapOutputTracker.getMapSizesByExecutorId(
      shuffleId, reduceId, reduceId + 1)).thenReturn {
      // Test a scenario where all data is local, to avoid creating a bunch of additional mocks
      // for the code to read data over the network.
      val shuffleBlockIdsAndSizes = (0 until numMaps).map { mapId =>
        val shuffleBlockId = ShuffleBlockId(shuffleId, mapId, reduceId)
        (shuffleBlockId, byteOutputStream.size().toLong, mapId)
      }
      Seq((localBlockManagerId, shuffleBlockIdsAndSizes)).toIterator
    }

    // Create a mocked shuffle handle to pass into HashShuffleReader.
    val shuffleHandle = {
      val dependency = mock(classOf[ShuffleDependency[Int, Int, Int]])
      when(dependency.serializer).thenReturn(serializer)
      when(dependency.aggregator).thenReturn(None)
      when(dependency.keyOrdering).thenReturn(None)
      new BaseShuffleHandle(shuffleId, dependency)
    }

    val serializerManager = new SerializerManager(
      serializer,
      new SparkConf()
        .set(config.SHUFFLE_COMPRESS, false)
        .set(config.SHUFFLE_SPILL_COMPRESS, false))

    val taskContext = TaskContext.empty()
    val metrics = taskContext.taskMetrics.createTempShuffleReadMetrics()
    val blocksByAddress = mapOutputTracker.getMapSizesByExecutorId(
      shuffleId, reduceId, reduceId + 1)

    val (daosReader, shuffleIO, daosObject) =
    if (singleCall) {
      mockObjectsForSingleDaosCall(reduceId, numMaps, byteOutputStream)
    } else {
      mockObjectsForMultipleDaosCall(reduceId, numMaps, byteOutputStream, executors)
    }

    when(shuffleIO.getDaosReader(shuffleId)).thenReturn(daosReader)
    //    when(daosReader.getObject).thenReturn(daosObject)

    val shuffleReader = new DaosShuffleReader[Int, Int](
      shuffleHandle,
      blocksByAddress,
      taskContext,
      metrics,
      shuffleIO,
      serializerManager,
      false)

    assert(shuffleReader.read().length === keyValuePairsPerMap * numMaps)

    // verify metrics
    assert(metrics.remoteBytesRead === numMaps * byteOutputStream.toByteArray.length)
    logInfo("remotes bytes: " + metrics.remoteBytesRead)
    assert(metrics.remoteBlocksFetched === numMaps)
  }

  test("test reader daos once") {
    testRead(10, 6)
  }

  test("test reader daos multiple times") {
    testRead(7168, 4, false)
  }

  test("test reader daos multiple times from other thread") {
    val executors = new BoundThreadExecutors("read_executors", 1, new DaosReaderSync.ReadThreadFactory)
    testRead(7168, 6, false, executors)
    executors.stop()
  }
}

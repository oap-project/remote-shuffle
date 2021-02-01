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

package org.apache.spark.shuffle.daos;

import io.daos.obj.DaosObjClient;
import io.daos.obj.DaosObject;
import io.daos.obj.DaosObjectId;
import io.daos.obj.IODataDesc;
import io.netty.buffer.ByteBuf;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.TaskContext;
import org.apache.spark.shuffle.ShuffleReadMetricsReporter;
import org.apache.spark.storage.BlockId;
import org.apache.spark.storage.BlockManagerId;
import org.apache.spark.storage.ShuffleBlockId;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.SuppressStaticInitializationFor;
import org.powermock.modules.junit4.PowerMockRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;
import scala.Tuple2;
import scala.Tuple3;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.mockito.ArgumentMatchers.any;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore("javax.management.*")
@SuppressStaticInitializationFor("io.daos.obj.DaosObjClient")
public class DaosShuffleInputStreamTest {

  private static final Logger LOG = LoggerFactory.getLogger(DaosShuffleInputStreamTest.class);

  @Test
  public void testReadFromOtherThreadCancelMultipleTimes1() throws Exception {
    Map<String, AtomicInteger> maps = new HashMap<>();
    maps.put("4", new AtomicInteger(0));
    maps.put("7", new AtomicInteger(0));
    maps.put("30", new AtomicInteger(0));
    readFromOtherThreadCancelMultipleTimes(maps);
  }

  @Test
  public void testReadFromOtherThreadCancelMultipleTimes2() throws Exception {
    Map<String, AtomicInteger> maps = new HashMap<>();
    maps.put("0", new AtomicInteger(0));
    maps.put("7", new AtomicInteger(0));
    maps.put("30", new AtomicInteger(0));
    readFromOtherThreadCancelMultipleTimes(maps);
  }

  @Test
  public void testReadFromOtherThreadCancelMultipleTimes3() throws Exception {
    Map<String, AtomicInteger> maps = new HashMap<>();
    maps.put("4", new AtomicInteger(0));
    maps.put("7", new AtomicInteger(0));
    maps.put("40", new AtomicInteger(0));
    readFromOtherThreadCancelMultipleTimes(maps);
  }

  @Test
  public void testReadFromOtherThreadCancelMultipleTimes4() throws Exception {
    Map<String, AtomicInteger> maps = new HashMap<>();
    maps.put("0", new AtomicInteger(0));
    maps.put("7", new AtomicInteger(0));
    maps.put("40", new AtomicInteger(0));
    readFromOtherThreadCancelMultipleTimes(maps);
  }

  @Test
  public void testReadFromOtherThreadCancelMultipleTimesConsecutiveLast() throws Exception {
    Map<String, AtomicInteger> maps = new HashMap<>();
    maps.put("0", new AtomicInteger(0));
    maps.put("39", new AtomicInteger(0));
    maps.put("40", new AtomicInteger(0));
    readFromOtherThreadCancelMultipleTimes(maps);
  }

  @Test
  public void testReadFromOtherThreadCancelMultipleTimesConsecutiveEarlier() throws Exception {
    Map<String, AtomicInteger> maps = new HashMap<>();
    maps.put("2", new AtomicInteger(0));
    maps.put("4", new AtomicInteger(0));
    maps.put("6", new AtomicInteger(0));
    maps.put("8", new AtomicInteger(0));
    readFromOtherThreadCancelMultipleTimes(maps);
  }

  @Test
  public void testReadFromOtherThreadCancelMultipleTimesLongWait() throws Exception {
    Map<String, AtomicInteger> maps = new HashMap<>();
    maps.put("2", new AtomicInteger(0));
    maps.put("4", new AtomicInteger(0));
    readFromOtherThreadCancelMultipleTimes(maps, 400);
  }

  @Test
  public void testReadFromOtherThreadCancelAll() throws Exception {
    Map<String, AtomicInteger> maps = new HashMap<>();
    maps.put("4", new AtomicInteger(0));
    maps.put("7", new AtomicInteger(0));
    maps.put("12", new AtomicInteger(0));
    maps.put("24", new AtomicInteger(0));
    maps.put("30", new AtomicInteger(0));
    maps.put("40", new AtomicInteger(0));
    readFromOtherThreadCancelMultipleTimes(maps, 500, false);
  }

  public void readFromOtherThreadCancelMultipleTimes(Map<String, AtomicInteger> maps) throws Exception {
    readFromOtherThreadCancelMultipleTimes(maps, 500);
  }

  public void readFromOtherThreadCancelMultipleTimes(Map<String, AtomicInteger> maps,
                                                     int addWaitTimeMs) throws Exception {
    readFromOtherThreadCancelMultipleTimes(maps, addWaitTimeMs, true);
  }

  public void readFromOtherThreadCancelMultipleTimes(Map<String, AtomicInteger> maps,
                                                     int addWaitTimeMs, boolean fromOtherThread) throws Exception {
    int waitDataTimeMs = (int)new SparkConf(false).get(package$.MODULE$.SHUFFLE_DAOS_READ_WAIT_DATA_MS());
    int expectedFetchTimes = 32;
    AtomicInteger fetchTimes = new AtomicInteger(0);
    boolean[] succeeded = new boolean[] {true};
    Method method = IODataDesc.class.getDeclaredMethod("succeed");
    method.setAccessible(true);
    CountDownLatch latch = new CountDownLatch(expectedFetchTimes);

    Thread callerThread = Thread.currentThread();

    Answer<InvocationOnMock> answer = (invocationOnMock ->
    {
      fetchTimes.getAndIncrement();
      IODataDesc desc = invocationOnMock.getArgument(0);
      desc.encode();
      method.invoke(desc);

      IODataDesc.Entry entry = desc.getEntry(0);
      String mapId = entry.getKey();
      if (maps.containsKey(mapId)) {
        // Thread thread = maps.get(mapId);
        if (callerThread != Thread.currentThread()) {
          // wait.incrementAndGet();
          // sleep to cause read timeout
          System.out.println("sleeping at " + mapId);
          Thread.sleep(waitDataTimeMs + addWaitTimeMs);
          if ("40".equals(mapId) && !fromOtherThread) { // return data finally in case fromOtherThread is disabled
            setLength(desc, succeeded, null);
            latch.countDown();
          }
          System.out.println("slept at " + mapId);

          return invocationOnMock;
        } else {
          latch.countDown();
          setLength(desc, succeeded, null);
          System.out.println("read again by self at " + mapId);
          return invocationOnMock;
        }
      }
      latch.countDown();
      setLength(desc, succeeded, null);
      return invocationOnMock;
    });
    read(42, answer, latch, fetchTimes, succeeded);
  }

  @Test
  public void testReadFromOtherThreadCancelOnceAtLast() throws Exception {
    testReadFromOtherThreadCancelOnce(40, 10240, 500);
  }

  @Test
  public void testReadFromOtherThreadCancelOnceAtMiddle() throws Exception {
    testReadFromOtherThreadCancelOnce(2, 0, 500);
  }

  @Test
  public void testReadFromOtherThreadCancelOnceAtFirst() throws Exception {
    testReadFromOtherThreadCancelOnce(0, 10*1024, 500);
  }

  private void testReadFromOtherThreadCancelOnce(int pos, int desiredOffset, int addWaitMs) throws Exception {
    int waitDataTimeMs = (int)new SparkConf(false).get(package$.MODULE$.SHUFFLE_DAOS_READ_WAIT_DATA_MS());
    int expectedFetchTimes = 32;
    AtomicInteger fetchTimes = new AtomicInteger(0);
    boolean[] succeeded = new boolean[] {true};
    AtomicInteger wait = new AtomicInteger(0);
    Method method = IODataDesc.class.getDeclaredMethod("succeed");
    method.setAccessible(true);
    CountDownLatch latch = new CountDownLatch(expectedFetchTimes);

    Answer<InvocationOnMock> answer = (invocationOnMock ->
    {
      fetchTimes.getAndIncrement();
      IODataDesc desc = invocationOnMock.getArgument(0);
      desc.encode();
      method.invoke(desc);

      IODataDesc.Entry entry = desc.getEntry(0);
      int offset = entry.getOffset();
      if (String.valueOf(pos).equals(entry.getKey()) && offset == desiredOffset) {
        if (wait.get() == 0) {
          wait.incrementAndGet();
          Thread.sleep(waitDataTimeMs + addWaitMs);
          System.out.println("sleep at " + pos);
          return invocationOnMock;
        } else {
          latch.countDown();
          checkAndSetSize(desc, succeeded, (15 * 1024 - desiredOffset), (5 * 1024) + desiredOffset);
          System.out.println("self read later at " + pos);
          return invocationOnMock;
        }
      }
      latch.countDown();
      setLength(desc, succeeded, null);
      return invocationOnMock;
    });
    read(42, answer, latch, fetchTimes, succeeded);
  }

  @Test
  public void testReadSmallMapFromOtherThread() throws Exception {
    int expectedFetchTimes = 32;
    AtomicInteger fetchTimes = new AtomicInteger(0);
    boolean[] succeeded = new boolean[] {true};
    Method method = IODataDesc.class.getDeclaredMethod("succeed");
    method.setAccessible(true);
    CountDownLatch latch = new CountDownLatch(expectedFetchTimes);

    Answer<InvocationOnMock> answer = (invocationOnMock ->
    {
      fetchTimes.getAndIncrement();
      IODataDesc desc = invocationOnMock.getArgument(0);
      desc.encode();
      method.invoke(desc);

      IODataDesc.Entry entry = desc.getEntry(0);
      int offset = entry.getOffset();
      if ("0".equals(entry.getKey()) && offset == 0) {
        latch.countDown();
        checkAndSetSize(desc, succeeded, 10*1024);
        return invocationOnMock;
      }
      if ("0".equals(entry.getKey()) && offset == 10*1024) {
        latch.countDown();
        checkAndSetSize(desc, succeeded, 5*1024, 15*1024);
        return invocationOnMock;
      }
      if ("0".equals(entry.getKey()) && offset == 30*1024) {
        latch.countDown();
        checkAndSetSize(desc, succeeded, 15*1024, 5*1024);
        return invocationOnMock;
      }
      latch.countDown();
      setLength(desc, succeeded, null);
      return invocationOnMock;
    });
    read(42, answer, latch, fetchTimes, succeeded);
  }

  private void read(int maps, Answer<InvocationOnMock> answer,
                    CountDownLatch latch, AtomicInteger fetchTimes, boolean[] succeeded) throws Exception {
    UserGroupInformation.setLoginUser(UserGroupInformation.createRemoteUser("test"));
    SparkConf testConf = new SparkConf(false);
    long minSize = 10;
    testConf.set(package$.MODULE$.SHUFFLE_DAOS_READ_MINIMUM_SIZE(), minSize);
    SparkContext context = new SparkContext("local", "test", testConf);
    TaskContext taskContext = TaskContextObj.emptyTaskContext();
    ShuffleReadMetricsReporter metrics = taskContext.taskMetrics().createTempShuffleReadMetrics();
    // daos object mock
    DaosObjectId id = Mockito.mock(DaosObjectId.class);
    Mockito.when(id.isEncoded()).thenReturn(true);
    DaosObjClient client = PowerMockito.mock(DaosObjClient.class);
    Constructor<DaosObject> objectConstructor =
        DaosObject.class.getDeclaredConstructor(DaosObjClient.class, DaosObjectId.class);
    objectConstructor.setAccessible(true);
    DaosObject daosObject = Mockito.spy(objectConstructor.newInstance(client, id));

    Mockito.doAnswer(answer).when(daosObject).fetch(any(IODataDesc.class));

    BoundThreadExecutors executors = new BoundThreadExecutors("read_executors", 1,
        new DaosReaderSync.ReadThreadFactory());
    DaosReaderSync daosReader = new DaosReaderSync(daosObject, new DaosReader.ReaderConfig(), executors.nextExecutor());
    LinkedHashMap<Tuple2<Long, Integer>, Tuple3<Long, BlockId, BlockManagerId>> partSizeMap = new LinkedHashMap<>();
    int shuffleId = 10;
    int reduceId = 1;
    int size = (int)(minSize + 5) * 1024;
    for (int i = 0; i < maps; i++) {
      partSizeMap.put(new Tuple2<>(Long.valueOf(i), 10), new Tuple3<>(Long.valueOf(size),
              new ShuffleBlockId(shuffleId, i, reduceId),
          BlockManagerId.apply("1", "localhost", 2, Option.empty())));
    }

    DaosShuffleInputStream is = new DaosShuffleInputStream(daosReader, partSizeMap, 2 * minSize * 1024,
        2 * 1024 * 1024, metrics);
    try {
      // verify cancelled task and continuing submission
      for (int i = 0; i < maps; i++) {
        byte[] bytes = new byte[size];
        is.read(bytes);
        for (int j = 0; j < 255; j++) {
          try {
            Assert.assertEquals((byte) j, bytes[j]);
          } catch (Throwable e) {
            LOG.error("error at map " + i + ", loc: " + j);
            throw e;
          }
        }
        Assert.assertEquals(-1, is.read());
        is.nextMap();
      }
      boolean alldone = latch.await(10, TimeUnit.SECONDS);
      System.out.println("fetch times: " + fetchTimes.get());
      Assert.assertTrue(alldone);
      Assert.assertTrue(succeeded[0]);
      TaskContextObj.mergeReadMetrics(taskContext);
      System.out.println("total fetch wait time: " +
              taskContext.taskMetrics().shuffleReadMetrics()._fetchWaitTime().sum());
    } finally {
      daosReader.close(true);
      is.close(true);
      context.stop();
      if (executors != null) {
        executors.stop();
      }
    }
  }

  private void checkAndSetSize(IODataDesc desc, boolean[] succeeded, int... sizes) {
    if (desc.getNbrOfEntries() != sizes.length) {
      succeeded[0] = false;
      throw new IllegalStateException("number of entries should be " + sizes.length +", not " + desc.getNbrOfEntries());
    }

    setLength(desc, succeeded, sizes);
  }

  private void setLength(IODataDesc desc, boolean[] succeeded, int[] sizes) {
    for (int i = 0; i < desc.getNbrOfEntries(); i++) {
      ByteBuf buf = desc.getEntry(i).getFetchedData();
      if (sizes != null) {
        if (buf.capacity() != sizes[i]) {
          succeeded[0] = false;
          throw new IllegalStateException("buf capacity should be " + sizes[i] + ", not " + buf.capacity());
        }
      }
      for (int j = 0; j < buf.capacity(); j++) {
        buf.writeByte((byte)j);
      }
    }
  }

  @AfterClass
  public static void afterAll() {
  }
}

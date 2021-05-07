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

package org.apache.spark.shuffle.daos;

import io.daos.DaosEventQueue;
import io.daos.obj.DaosObject;
import io.daos.obj.IOSimpleDDAsync;
import io.netty.buffer.ByteBuf;
import org.apache.spark.SparkConf;
import org.apache.spark.executor.TempShuffleReadMetrics;
import org.apache.spark.shuffle.ShuffleReadMetricsReporter;
import org.apache.spark.storage.BlockId;
import org.apache.spark.storage.BlockManagerId;
import org.apache.spark.storage.ShuffleBlockId;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.core.classloader.annotations.SuppressStaticInitializationFor;
import org.powermock.modules.junit4.PowerMockRunner;
import scala.Tuple2;
import scala.Tuple3;

import java.util.LinkedHashMap;
import java.util.List;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore("javax.management.*")
@SuppressStaticInitializationFor("io.daos.obj.DaosObjClient")
@PrepareForTest({DaosEventQueue.class, DaosReaderAsync.class})
public class DaosReaderAsyncTest {

  private DaosReaderAsync reader;

  private DaosObject object;

  private DaosEventQueue eq;

  private SparkConf conf;

  private DaosReader.ReaderConfig readerConfig;

  @Before
  public void setup() throws Exception {
    conf = new SparkConf(false);
    readerConfig = new DaosReader.ReaderConfig(conf);
    PowerMockito.mockStatic(DaosEventQueue.class);
    eq = PowerMockito.mock(DaosEventQueue.class);
    object = Mockito.mock(DaosObject.class);
    PowerMockito.when(DaosEventQueue.getInstance(0)).thenReturn(eq);
    reader = new DaosReaderAsync(object, readerConfig);
  }

  @Test
  public void testEmptyData() throws Exception {
    ByteBuf buf = reader.nextBuf();
    Assert.assertTrue(buf == null);
  }

  @Test
  public void testOneEntry() throws Exception {
    LinkedHashMap<Tuple2<Long, Integer>, Tuple3<Long, BlockId, BlockManagerId>> partSizeMap;
    partSizeMap = new LinkedHashMap<>();
    long mapId = 12345;
    int reduceId = 6789;
    long len = 1024;
    int shuffleId = 1000;
    long eqHandle = 1111L;
    IOSimpleDDAsync desc = Mockito.mock(IOSimpleDDAsync.class);
    IOSimpleDDAsync.AsyncEntry entry = Mockito.mock(IOSimpleDDAsync.AsyncEntry.class);
    ByteBuf buf = Mockito.mock(ByteBuf.class);
    Mockito.when(entry.getFetchedData()).thenReturn(buf);
    Mockito.when(buf.readableBytes()).thenReturn((int)len);
    Mockito.when(desc.getEntry(0)).thenReturn(entry);
    Mockito.when(desc.isSucceeded()).thenReturn(true);
    Mockito.when(eq.getEqWrapperHdl()).thenReturn(eqHandle);
    Mockito.when(object.createAsyncDataDescForFetch(String.valueOf(reduceId), eqHandle))
        .thenReturn(desc);
    Mockito.doAnswer(new Answer() {
      @Override
      public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
        List<DaosEventQueue.Attachment> list = invocationOnMock.getArgument(0);
        list.add(desc);
        return Integer.valueOf(1);
      }
    }).when(eq).pollCompleted(Mockito.any(), Mockito.anyInt(), Mockito.anyLong());
    BlockId blockId = new ShuffleBlockId(shuffleId, mapId, reduceId);
    partSizeMap.put(new Tuple2<>(mapId, reduceId), new Tuple3<>(len, blockId, null));
    ShuffleReadMetricsReporter metrics = new TempShuffleReadMetrics();
    reader.prepare(partSizeMap, 2 * 1024 * 1024, 2 * 1024 * 1024,
        metrics);
    ByteBuf realBuf = reader.nextBuf();
    Assert.assertTrue(realBuf == buf);
    Assert.assertEquals(len, ((TempShuffleReadMetrics) metrics).remoteBytesRead());

    Mockito.verify(eq).pollCompleted(Mockito.any(), Mockito.anyInt(), Mockito.anyLong());
    Mockito.verify(object).createAsyncDataDescForFetch(String.valueOf(reduceId), eqHandle);
  }

  @Test
  public void testTwoEntries() throws Exception {
    LinkedHashMap<Tuple2<Long, Integer>, Tuple3<Long, BlockId, BlockManagerId>> partSizeMap;
    partSizeMap = new LinkedHashMap<>();
    long mapIds[] = new long[] {12345, 12346};
    int reduceId = 6789;
    long lens[] = new long[] {2 * 1024 * 1024, 1023};
    int shuffleId = 1000;
    long eqHandle = 1111L;
    IOSimpleDDAsync descs[] = new IOSimpleDDAsync[] {Mockito.mock(IOSimpleDDAsync.class),
        Mockito.mock(IOSimpleDDAsync.class)};
    IOSimpleDDAsync.AsyncEntry entries[] = new IOSimpleDDAsync.AsyncEntry[] {
        Mockito.mock(IOSimpleDDAsync.AsyncEntry.class),
        Mockito.mock(IOSimpleDDAsync.AsyncEntry.class)
    };
    ByteBuf bufs[] = new ByteBuf[] {Mockito.mock(ByteBuf.class), Mockito.mock(ByteBuf.class)};
    boolean readAlready[] = new boolean[] {false, false};
    for (int i = 0; i < 2; i++) {
      Mockito.when(entries[i].getFetchedData()).thenReturn(bufs[i]);
      Mockito.when(entries[i].getKey()).thenReturn(String.valueOf(mapIds[i]));
      final int index = i;
      Mockito.doAnswer(new Answer() {
        @Override
        public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
          boolean isRead = readAlready[index];
          if (!isRead) {
            readAlready[index] = true;
          }
          return isRead ? 0 : (int) lens[index];
        }
      }).when(bufs[i]).readableBytes();
      Mockito.when(descs[i].getEntry(0)).thenReturn(entries[i]);
      Mockito.when(descs[i].isSucceeded()).thenReturn(true);
      BlockId blockId = new ShuffleBlockId(shuffleId, mapIds[i], reduceId);
      partSizeMap.put(new Tuple2<>(mapIds[i], reduceId), new Tuple3<>(lens[i], blockId, null));
    }
    Mockito.when(eq.getEqWrapperHdl()).thenReturn(eqHandle);

    int times[] = new int[] {0};
    Mockito.doAnswer(new Answer() {
      @Override
      public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
        List<DaosEventQueue.Attachment> list = invocationOnMock.getArgument(0);
        if (times[0] > 1) {
          throw new IllegalStateException("times should be no more than 1. actual is " + times[0]);
        }
        list.add(descs[times[0]]);
        times[0]++;
        return Integer.valueOf(1);
      }
    }).when(eq).pollCompleted(Mockito.any(), Mockito.anyInt(), Mockito.anyLong());

    int times2[] = new int[] {0};
    Mockito.doAnswer(new Answer() {
      @Override
      public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
        int i = times2[0];
        if (i > 1) {
          throw new IllegalStateException("times should be no more than 1. actual is " + i);
        }
        times2[0]++;
        return descs[i];
      }
    }).when(object).createAsyncDataDescForFetch(String.valueOf(reduceId), eqHandle);

    ShuffleReadMetricsReporter metrics = new TempShuffleReadMetrics();
    reader.prepare(partSizeMap, 2 * 1024 * 1024, 2 * 1024 * 1024,
        metrics);
    long totals = 0;
    for (int i = 0; i < 2; i++) {
      ByteBuf realBuf = reader.nextBuf();
      Assert.assertTrue(realBuf == bufs[i]);
      totals += lens[i];
      Assert.assertEquals(totals, ((TempShuffleReadMetrics) metrics).remoteBytesRead());
    }

    Mockito.verify(eq, Mockito.times(2))
        .pollCompleted(Mockito.any(), Mockito.anyInt(), Mockito.anyLong());
    Mockito.verify(object, Mockito.times(2))
        .createAsyncDataDescForFetch(String.valueOf(reduceId), eqHandle);
  }

}

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
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.SuppressStaticInitializationFor;
import org.powermock.modules.junit4.PowerMockRunner;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import static org.mockito.ArgumentMatchers.any;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore("javax.management.*")
@SuppressStaticInitializationFor("io.daos.obj.DaosObjClient")
public class DaosWriterTest {

  @Test
  public void testGetLensWithAllEmptyPartitions() {
    DaosWriter.WriterConfig writeConfig = new DaosWriter.WriterConfig();
    DaosWriterSync.WriteParam param = new DaosWriterSync.WriteParam();
    int numPart = 10;
    param.numPartitions(numPart)
        .shuffleId(1)
        .mapId(1)
        .config(writeConfig);
    DaosWriterSync writer = new DaosWriterSync(null, param, null);
    long[] lens = writer.getPartitionLens(numPart);
    Assert.assertEquals(numPart, lens.length);
    for (int i = 0; i < numPart; i++) {
      Assert.assertEquals(0, lens[i]);
    }
    writer.close();
  }

  @Test
  public void testGetLensWithPartialEmptyPartitions() throws Exception {
    DaosObjectId id = Mockito.mock(DaosObjectId.class);
    Mockito.when(id.isEncoded()).thenReturn(true);
    DaosObjClient client = PowerMockito.mock(DaosObjClient.class);
    Constructor<DaosObject> objectConstructor =
        DaosObject.class.getDeclaredConstructor(DaosObjClient.class, DaosObjectId.class);
    objectConstructor.setAccessible(true);
    DaosObject daosObject = Mockito.spy(objectConstructor.newInstance(client, id));

    Mockito.doNothing().when(daosObject).update(any(IODataDesc.class));

    DaosWriter.WriterConfig writeConfig = new DaosWriter.WriterConfig();
    DaosWriterSync.WriteParam param = new DaosWriterSync.WriteParam();
    int numPart = 10;
    param.numPartitions(numPart)
        .shuffleId(1)
        .mapId(1)
        .config(writeConfig);
    DaosWriterSync writer = new DaosWriterSync(daosObject, param, null);
    Map<Integer, Integer> expectedLens = new HashMap<>();
    Random random = new Random();
    for (int i = 0; i < 5; i++) {
      int idx = Math.abs(random.nextInt(numPart));
      while (expectedLens.containsKey(idx)) {
        idx = Math.abs(random.nextInt(numPart));
      }
      int size = Math.abs(random.nextInt(8000));
      writer.write(idx, new byte[size]);
      writer.flush(idx);
      expectedLens.put(idx, size);
    }
    long[] lens = writer.getPartitionLens(numPart);
    Assert.assertEquals(numPart, lens.length);
    for (int i = 0; i < numPart; i++) {
      int expected = expectedLens.get(i) == null ? 0 : expectedLens.get(i);
      Assert.assertEquals(expected, (int)(lens[i]));
    }
    writer.close();
  }

  @Test(expected = IllegalStateException.class)
  public void testWriteTaskFailed() throws Exception {
    DaosObjectId id = Mockito.mock(DaosObjectId.class);
    Mockito.when(id.isEncoded()).thenReturn(true);
    DaosObjClient client = PowerMockito.mock(DaosObjClient.class);
    Constructor<DaosObject> objectConstructor =
        DaosObject.class.getDeclaredConstructor(DaosObjClient.class, DaosObjectId.class);
    objectConstructor.setAccessible(true);
    DaosObject daosObject = Mockito.spy(objectConstructor.newInstance(client, id));

    AtomicInteger counter = new AtomicInteger(0);
    Method method = IODataDesc.class.getDeclaredMethod("succeed");
    method.setAccessible(true);
    Mockito.doAnswer(invoc -> {
      IODataDesc desc = invoc.getArgument(0);
      desc.encode();
      counter.incrementAndGet();
      if (counter.get() == 5) {
        method.invoke(desc);
      }
      return invoc;
    }).when(daosObject).update(any(IODataDesc.class));

    DaosWriter.WriterConfig writeConfig = new DaosWriter.WriterConfig();
    DaosWriterSync.WriteParam param = new DaosWriterSync.WriteParam();
    int numPart = 10;
    param.numPartitions(numPart)
        .shuffleId(1)
        .mapId(1)
        .config(writeConfig);

    BoundThreadExecutors executors = new BoundThreadExecutors("read_executors", 1,
        new DaosReaderSync.ReadThreadFactory());
    DaosWriterSync writer = new DaosWriterSync(daosObject, param, executors.nextExecutor());
    for (int i = 0; i < numPart; i++) {
      writer.write(i, new byte[100]);
      writer.flush(i);
    }

    writer.close();

    executors.stop();
  }
}

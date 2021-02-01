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
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.core.classloader.annotations.SuppressStaticInitializationFor;
import org.powermock.modules.junit4.PowerMockRunner;

import java.lang.reflect.Field;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore("javax.management.*")
@PrepareForTest(IOManager.class)
@SuppressStaticInitializationFor("io.daos.obj.DaosObjClient")
public class DaosShuffleIOTest {

  @Test
  public void testSingleObjectInstanceOpen() throws Exception {
    SparkConf testConf = new SparkConf(false);
    testConf.set(package$.MODULE$.SHUFFLE_DAOS_READ_FROM_OTHER_THREAD(), false);
    testConf.set(package$.MODULE$.SHUFFLE_DAOS_IO_ASYNC(), false);
    long appId = 1234567;
    int shuffleId = 1;
    testConf.set("spark.app.id", String.valueOf(appId));
    Field clientField = IOManager.class.getDeclaredField("objClient");
    clientField.setAccessible(true);

    UserGroupInformation.setLoginUser(UserGroupInformation.createRemoteUser("test"));
    SparkContext sc = new SparkContext("local", "test", testConf);

    DaosShuffleIO io = new DaosShuffleIO(testConf);

    DaosObjectId id = PowerMockito.mock(DaosObjectId.class);
    PowerMockito.whenNew(DaosObjectId.class).withArguments(appId, Long.valueOf(shuffleId)).thenReturn(id);
    Mockito.doNothing().when(id).encode();
    Mockito.when(id.isEncoded()).thenReturn(true);
    DaosObject daosObject = PowerMockito.mock(DaosObject.class);
    DaosObjClient client = PowerMockito.mock(DaosObjClient.class);
    Mockito.when(client.getObject(id)).thenReturn(daosObject);

    AtomicBoolean open = new AtomicBoolean(false);
    Mockito.when(daosObject.isOpen()).then(invocationOnMock ->
      open.get()
    );
    Mockito.doAnswer(invocationOnMock -> {
      open.compareAndSet(false, true);
      return invocationOnMock;
    }).when(daosObject).open();
    clientField.set(io.getIoManager(), client);

    int numThreads = 50;
    ExecutorService es = Executors.newFixedThreadPool(numThreads);
    AtomicInteger count = new AtomicInteger(0);

    Runnable r = () -> {
      try {
        DaosReader reader = io.getDaosReader(shuffleId);
        if (reader.getObject() == daosObject && reader.getObject().isOpen()) {
          count.incrementAndGet();
        }
      } catch (Exception e) {
        e.printStackTrace();
      }
    };

    for (int i = 0; i < numThreads; i++) {
      es.submit(r);
    }

    es.shutdown();
    es.awaitTermination(5, TimeUnit.SECONDS);

    Assert.assertEquals(50, count.intValue());
    sc.stop();
  }
}

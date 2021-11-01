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
import io.daos.TimedOutException;
import io.daos.obj.DaosObject;
import io.daos.obj.IOSimpleDDAsync;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class DaosWriterAsync extends DaosWriterBase {

  private DaosEventQueue eq;

  private Set<IOSimpleDDAsync> descSet = new LinkedHashSet<>();

  private List<DaosEventQueue.Attachment> completedList = new LinkedList<>();

  private static Logger log = LoggerFactory.getLogger(DaosWriterAsync.class);

  public DaosWriterAsync(DaosObject object, WriteParam param) throws IOException {
    super(object, param);
    eq = DaosEventQueue.getInstance(0);
  }

  @Override
  public void flush(int partitionId) throws IOException {
    NativeBuffer buffer = partitionBufArray[partitionId];
    if (buffer == null) {
      return;
    }
    List<IOSimpleDDAsync> descList = buffer.createUpdateDescAsyncs(eq.getEqWrapperHdl());
    flush(buffer, descList);
  }

  @Override
  public void flushAll(int partitionId) throws IOException {
    NativeBuffer buffer = partitionBufArray[partitionId];
    if (buffer == null) {
      return;
    }
    List<IOSimpleDDAsync> descList = buffer.createUpdateDescAsyncs(eq.getEqWrapperHdl(), false);
    flush(buffer, descList);
  }

  private void flush(NativeBuffer buffer, List<IOSimpleDDAsync> descList) throws IOException {
    if (!descList.isEmpty()) {
      assert Thread.currentThread().getId() == eq.getThreadId() : "current thread " + Thread.currentThread().getId() +
          "(" + Thread.currentThread().getName() + "), is not expected " + Thread.currentThread().getId() + "(" +
          eq.getThreadName() + ")";

      for (IOSimpleDDAsync desc : descList) {
        DaosEventQueue.Event event = acquireEvent();
        descSet.add(desc);
        desc.setEvent(event);
        try {
          object.updateAsync(desc);
        } catch (Exception e) {
          desc.release();
          descSet.remove(desc);
          throw e;
        }
      }
      buffer.reset(false);
      if (descSet.size() >= config.getAsyncWriteBatchSize()) {
        waitCompletion();
      }
    }
  }

  @Override
  public void flushAll() throws IOException {
    for (int i = 0; i < partitionBufArray.length; i++) {
      NativeBuffer buffer = partitionBufArray[i];
      if (buffer == null) {
        continue;
      }
      List<IOSimpleDDAsync> descList = buffer.createUpdateDescAsyncs(eq.getEqWrapperHdl(), false);
      flush(buffer, descList);
    }
    waitCompletion();
  }

  @Override
  protected void waitCompletion() throws IOException {
    int left;
    try {
      long dur;
      long start = System.currentTimeMillis();
      while ((left=descSet.size()) > 0 & ((dur = System.currentTimeMillis() - start) < config.getWaitTimeMs())) {
        completedList.clear();
        eq.pollCompleted(completedList, IOSimpleDDAsync.class, descSet, left, config.getWaitTimeMs() - dur);
        verifyCompleted();
      }
      if (!descSet.isEmpty()) {
        throw new TimedOutException("timed out after " + (System.currentTimeMillis() - start));
      }
    } catch (IOException e) {
      throw new IllegalStateException("failed to complete all running updates. ", e);
    } finally {
      descSet.forEach(desc -> desc.release());
      descSet.clear();
    }
    super.flushAll();
  }

  private DaosEventQueue.Event acquireEvent() throws IOException {
    completedList.clear();
    try {
      DaosEventQueue.Event event = eq.acquireEventBlocking(config.getWaitTimeMs(), completedList,
          IOSimpleDDAsync.class, descSet);
      verifyCompleted();
      return event;
    } catch (IOException e) {
      log.error("EQNBR: " + eq.getNbrOfEvents() + ", " + eq.getNbrOfAcquired() + ", " + descSet.size());
      throw e;
    }
  }

  private void verifyCompleted() throws IOException {
    IOSimpleDDAsync failed = null;
    int failedCnt = 0;
    for (DaosEventQueue.Attachment attachment : completedList) {
      descSet.remove(attachment);
      IOSimpleDDAsync desc = (IOSimpleDDAsync) attachment;
      if (!desc.isSucceeded()) {
        failedCnt++;
        if (failed == null) {
          failed = desc; // release after toString so that akey info is captured
          continue;
        }
      }
      desc.release();
    }
    if (failedCnt > 0) {
      IOException e = new IOException("failed to write " + failedCnt + " IOSimpleDDAsync. Return code is " +
          failed.getReturnCode() + ". First failed is " + failed);
      failed.release();
      throw e;
    }
  }

  @Override
  public void close() {
    if (writerMap != null) {
      writerMap.remove(this);
      writerMap = null;
    }

    if (completedList != null) {
      completedList.clear();
      completedList = null;
    }
    super.close();
  }

  public void setWriterMap(Map<DaosWriter, Integer> writerMap) {
    writerMap.put(this, 0);
    this.writerMap = writerMap;
  }
}

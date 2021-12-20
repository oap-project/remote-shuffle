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
import io.daos.obj.IODescUpdAsync;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class DaosWriterAsync extends DaosWriterBase {

  private DaosEventQueue eq;

  private Set<IODescUpdAsync> descSet = new LinkedHashSet<>();

  private List<DaosEventQueue.Attachment> completedList = new LinkedList<>();

  private AsyncDescCache cache;

  private static Logger log = LoggerFactory.getLogger(DaosWriterAsync.class);

  public DaosWriterAsync(DaosObject object, WriteParam param) throws IOException {
    super(object, param);
    eq = DaosEventQueue.getInstance(0);
    cache = new AsyncDescCache(param.getConfig().getIoDescCaches());
  }

  @Override
  public void flush(int partitionId) throws IOException {
    NativeBuffer buffer = partitionBufArray[partitionId];
    if (buffer == null) {
      return;
    }
    List<IODescUpdAsync> descList = buffer.createUpdateDescAsyncs(eq.getEqWrapperHdl(), cache);
    flush(buffer, descList);
  }

  @Override
  public void flushAll(int partitionId) throws IOException {
    NativeBuffer buffer = partitionBufArray[partitionId];
    if (buffer == null) {
      return;
    }
    List<IODescUpdAsync> descList = buffer.createUpdateDescAsyncs(eq.getEqWrapperHdl(), cache, false);
    flush(buffer, descList);
  }

  private void cacheOrRelease(IODescUpdAsync desc) {
    if (desc.isReusable()) {
      cache.put(desc);
    } else {
      desc.release();
    }
  }

  private void flush(NativeBuffer buffer, List<IODescUpdAsync> descList) throws IOException {
    if (!descList.isEmpty()) {
      assert Thread.currentThread().getId() == eq.getThreadId() : "current thread " + Thread.currentThread().getId() +
          "(" + Thread.currentThread().getName() + "), is not expected " + eq.getThreadId() + "(" +
          eq.getThreadName() + ")";

      for (IODescUpdAsync desc : descList) {
        DaosEventQueue.Event event = acquireEvent();
        descSet.add(desc);
        desc.setEvent(event);
        try {
          object.updateAsync(desc);
        } catch (Exception e) {
          cacheOrRelease(desc);
          desc.discard();
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
      List<IODescUpdAsync> descList = buffer.createUpdateDescAsyncs(eq.getEqWrapperHdl(), cache, false);
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
      while ((left = descSet.size()) > 0 & ((dur = System.currentTimeMillis() - start) < config.getWaitTimeMs())) {
        completedList.clear();
        eq.pollCompleted(completedList, IODescUpdAsync.class, descSet, left, config.getWaitTimeMs() - dur);
        verifyCompleted();
      }
      if (!descSet.isEmpty()) {
        throw new TimedOutException("timed out after " + (System.currentTimeMillis() - start));
      }
    } catch (IOException e) {
      throw new IllegalStateException("failed to complete all running updates. ", e);
    }
    super.flushAll();
  }

  private DaosEventQueue.Event acquireEvent() throws IOException {
    completedList.clear();
    try {
      DaosEventQueue.Event event = eq.acquireEventBlocking(config.getWaitTimeMs(), completedList,
          IODescUpdAsync.class, descSet);
      verifyCompleted();
      return event;
    } catch (IOException e) {
      log.error("EQNBR: " + eq.getNbrOfEvents() + ", " + eq.getNbrOfAcquired() + ", " + descSet.size());
      throw e;
    }
  }

  private void verifyCompleted() throws IOException {
    IODescUpdAsync failed = null;
    int failedCnt = 0;
    for (DaosEventQueue.Attachment attachment : completedList) {
      descSet.remove(attachment);
      IODescUpdAsync desc = (IODescUpdAsync) attachment;
      if (!desc.isSucceeded()) {
        failedCnt++;
        if (failed == null) {
          failed = desc; // release after toString so that akey info is captured
          continue;
        }
      }
      if (log.isDebugEnabled()) {
        log.debug("written desc: " + desc);
      }
      cacheOrRelease(desc);
    }
    if (failedCnt > 0) {
      IOException e = new IOException("failed to write " + failedCnt + " IOSimpleDDAsync. Return code is " +
          failed.getReturnCode() + ". First failed is " + failed);
      cacheOrRelease(failed);
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

    if (descSet.isEmpty()) { // all descs polled
      cache.release();
    } else {
      descSet.forEach(d -> d.discard()); // to be released when poll
      cache.release(descSet);
      descSet.clear();
    }
    super.close();
  }

  public void setWriterMap(Map<DaosWriter, Integer> writerMap) {
    writerMap.put(this, 0);
    this.writerMap = writerMap;
  }

  static class AsyncDescCache implements ObjectCache<IODescUpdAsync> {
    private int idx;
    private int total;
    private IODescUpdAsync[] array;

    public AsyncDescCache(int maxNbr) {
      this.array = new IODescUpdAsync[maxNbr];
    }

    @Override
    public IODescUpdAsync get() {
      if (idx < total) {
        return array[idx++];
      }
      if (idx < array.length) {
        array[idx] = newObject();
        total++;
        return array[idx++];
      }
      throw new IllegalStateException("cache is full, " + total);
    }

    @Override
    public IODescUpdAsync newObject() {
      return new IODescUpdAsync(32);
    }

    @Override
    public void put(IODescUpdAsync desc) {
      if (idx <= 0) {
        throw new IllegalStateException("more than actual number of IODescUpdAsyncs put back");
      }
      if (desc.isDiscarded()) {
        desc.release();
        desc = newObject();
      }
      array[--idx] = desc;
    }

    @Override
    public boolean isFull() {
      return total == array.length;
    }

    public void release() {
      release(Collections.emptySet());
    }

    private void release(Set<IODescUpdAsync> filterSet) {
      for (int i = 0; i < Math.min(total, array.length); i++) {
        IODescUpdAsync desc = array[i];
        if (desc != null && !filterSet.contains(desc)) {
          desc.release();
        }
      }
      array = null;
      idx = 0;
    }

    protected int getIdx() {
      return idx;
    }
  }
}

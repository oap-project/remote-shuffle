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
import io.daos.obj.IODataDescBase;
import io.daos.obj.IOSimpleDDAsync;
import io.netty.buffer.ByteBuf;

import java.io.IOException;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

public class DaosReaderAsync extends DaosReaderBase {

  private DaosEventQueue eq;

  private Set<IOSimpleDDAsync> runningDescSet = new LinkedHashSet<>();

  private LinkedList<IOSimpleDDAsync> readyList = new LinkedList<>();

  private List<DaosEventQueue.Attachment> completedList = new LinkedList<>();

  public DaosReaderAsync(DaosObject object, ReaderConfig config) throws IOException {
    super(object, config);
    eq = DaosEventQueue.getInstance(0);
  }

  @Override
  protected IODataDescBase createFetchDataDesc(String reduceId) throws IOException {
    return object.createAsyncDataDescForFetch(reduceId, eq.getEqWrapperHdl());
  }

  @Override
  protected void addFetchEntry(IODataDescBase desc, String mapId, long offset, long readSize) throws IOException {
    if (readSize > Integer.MAX_VALUE) {
      throw new IllegalArgumentException("readSize should not exceed " + Integer.MAX_VALUE);
    }
    ((IOSimpleDDAsync) desc).addEntryForFetch(mapId, offset, (int)readSize);
  }

  @Override
  public ByteBuf nextBuf() throws IOException {
    ByteBuf buf = tryCurrentEntry();
    if (buf != null) {
      return buf;
    }
    // next entry
    buf = tryCurrentDesc();
    if (buf != null) {
      return buf;
    }
    // next desc
    entryIdx = 0;
    return readFromDaos();
  }

  private ByteBuf enterNewDesc(IOSimpleDDAsync desc) throws IOException {
    if (currentDesc != null) {
      currentDesc.release();
    }
    currentDesc = desc;
    return validateLastEntryAndGetBuf(desc.getEntry(entryIdx));
  }

  private IOSimpleDDAsync nextDesc() throws IOException {
    return readyList.poll();
  }

  private void progress() throws IOException {
    if (runningDescSet.isEmpty()) {
      return;
    }
    completedList.clear();
    long timeOutMs = config.getWaitDataTimeMs();
    long start = System.currentTimeMillis();
    long dur = 0L;
    do {
      eq.pollCompleted(completedList, IOSimpleDDAsync.class, runningDescSet, runningDescSet.size(),
          timeOutMs - dur);
      if (completedList.isEmpty()) {
        dur = System.currentTimeMillis() - start;
        if (dur > timeOutMs) {
          throw new TimedOutException("timed out after " + dur);
        }
      }
    } while (completedList.isEmpty());
    verifyCompleted();
  }

  private ByteBuf readFromDaos() throws IOException {
    if (runningDescSet.isEmpty()) {
      DaosEventQueue.Event event = acquireEvent();
      IOSimpleDDAsync taskDesc = (IOSimpleDDAsync) createNextDesc(config.getMaxBytesInFlight());
      if (taskDesc != null) {
        runningDescSet.add(taskDesc);
        taskDesc.setEvent(event);
        try {
          object.fetchAsync(taskDesc);
        } catch (IOException e) {
          taskDesc.release();
          runningDescSet.remove(taskDesc);
          throw e;
        }
      } else {
        eq.returnEvent(event);
        return null;
      }
    }
    progress();
    IOSimpleDDAsync desc = nextDesc();
    if (desc != null) {
      return enterNewDesc(desc);
    }
    return null;
  }

  private DaosEventQueue.Event acquireEvent() throws IOException {
    completedList.clear();
    DaosEventQueue.Event event = eq.acquireEventBlocking(config.getWaitDataTimeMs(), completedList,
        IOSimpleDDAsync.class, runningDescSet);
    verifyCompleted();
    return event;
  }

  private void verifyCompleted() throws IOException {
    IOSimpleDDAsync failed = null;
    int failedCnt = 0;
    for (DaosEventQueue.Attachment attachment : completedList) {
      IOSimpleDDAsync desc = (IOSimpleDDAsync) attachment;
      runningDescSet.remove(attachment);
      if (desc.isSucceeded()) {
        readyList.add(desc);
        continue;
      }
      failedCnt++;
      if (failed == null) {
        failed = desc;
      }
    }
    if (failedCnt > 0) {
      throw new IOException("failed to read " + failedCnt + " IOSimpleDDAsync. First failed is " + failed);
    }
  }

  @Override
  public void close(boolean force) {
    IllegalStateException e = null;
    if (!(readyList.isEmpty() & runningDescSet.isEmpty())) {
      StringBuilder sb = new StringBuilder();
      sb.append(readyList.isEmpty() ? "" : "not all data consumed. ");
      sb.append(runningDescSet.isEmpty() ? "" : "some data is on flight ");
      e = new IllegalStateException(sb.toString());
    }
    readyList.forEach(desc -> desc.release());
    runningDescSet.forEach(desc -> desc.release());
    if (currentDesc != null) {
      currentDesc.release();
      currentDesc = null;
    }
    if (e != null) {
      throw e;
    }
  }
}

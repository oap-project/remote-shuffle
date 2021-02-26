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
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

public class DaosReaderAsync extends DaosReaderBase {

  private DaosEventQueue eq;

  private int running;

  private long totalInMemSize;

  private Queue<IOSimpleDDAsync> readyQueue = new LinkedList<>();

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
    IOSimpleDDAsync desc = readyQueue.poll();
    if (desc != null) {
      if (currentDesc != null) {
        currentDesc.release();
      }
      currentDesc = desc;
      return validateLastEntryAndGetBuf(desc.getEntry(entryIdx));
    }
    return readFromDaos();
  }

  private ByteBuf readFromDaos() throws IOException {
    while (running < config.getReadBatchSize() && totalInMemSize < config.getMaxMem()) {
      DaosEventQueue.Event event = acquireEvent();
      if (event == null) {
        break;
      }
      IOSimpleDDAsync taskDesc = (IOSimpleDDAsync) createNextDesc(config.getMaxBytesInFlight());
      if (taskDesc == null) {
        eq.releaseEvent(event);
        break;
      }
      taskDesc.setEvent(event);
      try {
        object.fetchAsync(taskDesc);
        running++;
      } catch (IOException e) {
        // TODO: putback event
        taskDesc.release();
        throw e;
      }
    }
    completedList.clear();
    int n = eq.pollCompleted(completedList, running, config.getWaitDataTimeMs());
    if (n == 0) {
      throw new TimedOutException("timed out after " + config.getWaitDataTimeMs());
    }
    verifyCompleted();
    IOSimpleDDAsync desc = readyQueue.poll();
    if (desc != null) {
      if (currentDesc != null) {
        currentDesc.release();
      }
      currentDesc = desc;
      return validateLastEntryAndGetBuf(desc.getEntry(entryIdx));
    }
    return null;
  }

  private DaosEventQueue.Event acquireEvent() throws IOException {
    completedList.clear();
    DaosEventQueue.Event event = eq.acquireEventBlocking(config.getWaitDataTimeMs(), completedList);
    verifyCompleted();
    return event;
  }

  private void verifyCompleted() throws IOException {
    try {
      for (DaosEventQueue.Attachment attachment : completedList) {
        if (!((IOSimpleDDAsync) attachment).isSucceeded()) {
          throw new IOException("failed to fetch " + attachment);
        }
        readyQueue.offer((IOSimpleDDAsync) attachment);
      }
    } finally {
      running -= completedList.size();
      completedList.forEach(attachment -> attachment.release());
    }
  }
}

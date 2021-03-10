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

import java.io.IOException;
import java.util.*;

public class DaosWriterAsync extends DaosWriterBase {

  private DaosEventQueue eq;

  private Set<IOSimpleDDAsync> descSet = new LinkedHashSet<>();

  private List<DaosEventQueue.Attachment> completedList = new LinkedList<>();

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
    DaosEventQueue.Event event = acquireEvent();
    IOSimpleDDAsync desc = buffer.createUpdateDescAsync(eq.getEqWrapperHdl());
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

  private DaosEventQueue.Event acquireEvent() throws IOException {
    completedList.clear();
    DaosEventQueue.Event event = eq.acquireEventBlocking(config.getWaitTimeMs(), completedList);
    verifyCompleted();
    return event;
  }

  private void verifyCompleted() throws IOException {
    IOSimpleDDAsync failed = null;
    int failedCnt = 0;
    for (DaosEventQueue.Attachment attachment : completedList) {
      if (descSet.contains(attachment)) {
        attachment.release();
        descSet.remove(attachment);
        IOSimpleDDAsync desc = (IOSimpleDDAsync) attachment;
        if (desc.isSucceeded()) {
          continue;
        }
        failedCnt++;
        if (failed == null) {
          failed = desc;
        }
      }
    }
    if (failedCnt > 0) {
      throw new IOException("failed to write " + failedCnt + " IOSimpleDDAsync. First failed is " + failed);
    }
  }

  @Override
  public void close() {
    int left;
    try {
      while ((left=descSet.size()) > 0) {
        completedList.clear();
        int n = eq.pollCompleted(completedList, left, config.getWaitTimeMs());
        if (n == 0) {
          throw new TimedOutException("timed out after " + config.getWaitTimeMs());
        }
        verifyCompleted();
      }
    } catch (IOException e) {
      throw new IllegalStateException("failed to complete all running updates. ", e);
    } finally {
      descSet.forEach(desc -> desc.release());
      descSet.clear();
    }

    if (writerMap != null) {
      writerMap.remove(this);
      writerMap = null;
    }

    if (completedList != null) {
      completedList.clear();
      completedList = null;
    }
  }

  public void setWriterMap(Map<DaosWriter, Integer> writerMap) {
    writerMap.put(this, 0);
    this.writerMap = writerMap;
  }
}

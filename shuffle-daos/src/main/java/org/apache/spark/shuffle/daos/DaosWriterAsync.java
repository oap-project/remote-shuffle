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
import io.daos.obj.IOSimpleDataDesc;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class DaosWriterAsync extends DaosWriterBase {

  private DaosEventQueue eq;

  private List<DaosEventQueue.Attachment> completedList = new LinkedList<>();

  public DaosWriterAsync(DaosObject object, WriteParam param) {
    super(object, param);
  }

  @Override
  public void flush(int partitionId) throws IOException {
    NativeBuffer buffer = partitionBufArray[partitionId];
    if (buffer == null) {
      return;
    }
    DaosEventQueue.Event event = acquireEvent();
    buffer.createUpdateDescAsync();
  }

  private DaosEventQueue.Event acquireEvent() throws IOException {
    completedList.clear();
    eq = DaosEventQueue.getInstance(0);
    DaosEventQueue.Event event = eq.acquireEventBlocking(config.getWaitTimeMs(), completedList);
    verifyCompleted();
    return event;
  }

  private void verifyCompleted() throws IOException {
    for (DaosEventQueue.Attachment attachment : completedList) {
      if (!((IOSimpleDataDesc)attachment).isSucceeded()) {
        throw new IOException("failed to write " + attachment);
      }
    }
  }

  @Override
  public void close() {

  }

  public void setWriterMap(Map<DaosWriter, Integer> writerMap) {
    writerMap.put(this, 0);
    this.writerMap = writerMap;
  }
}

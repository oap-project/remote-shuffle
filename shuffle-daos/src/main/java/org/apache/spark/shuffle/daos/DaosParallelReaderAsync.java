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
import io.daos.obj.IODataDesc;
import io.daos.obj.IOSimpleDDAsync;
import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Reader for reading content from dkey/akeys without knowing their lengths.
 *
 * determine end of content by checking returned actual size.
 */
public class DaosParallelReaderAsync extends DaosReaderAsync {

  private LinkedList<FetchState> mapFetchList = new LinkedList<>();

  private Set<IODescWithState> descSet = new HashSet<>();

  private FetchState currentState;

  private long totalInMemSize;

  private static final Logger log = LoggerFactory.getLogger(DaosParallelReaderAsync.class);

  public DaosParallelReaderAsync(DaosObject object, ReaderConfig config) throws IOException {
    super(object, config);
  }

  @Override
  public ByteBuf nextBuf() throws IOException {
    nextMap = false;
    ByteBuf buf = tryCurrentState();
    if (buf != null) {
      return buf;
    }
    // next entry
    buf = tryNextState();
    if (buf != null) {
      return buf;
    }
    readFromDaos();
    return tryNextState();
  }

  private ByteBuf tryNextState() throws IOException {
    if (currentState != null) {
      nextMap = true;
      if (!currentState.dataEntries.isEmpty()) {
        throw new IllegalStateException("dataEntries should be empty in current state");
      }
      FetchState first = mapFetchList.pollFirst();
      if (first != currentState) {
        throw new IllegalStateException("currentState should be the first entry of mapFetchList");
      }
    }
    currentState = mapFetchList.peekFirst();
    if (currentState != null) {
      return tryCurrentState();
    }
    return null;
  }

  private ByteBuf tryCurrentState() throws IOException {
    if (currentState != null) {
      return currentState.getBuffer();
    }
    return null;
  }

  @Override
  protected ByteBuf readFromDaos() throws IOException {
    try {
      return super.readFromDaos();
    } catch (IOException e) {
      releaseDescSet();
      throw e;
    }
  }

  private void releaseDescSet() {
    descSet.forEach(desc -> desc.discard());
  }

  @Override
  protected Class<IODescWithState> getIODescClass() {
    return IODescWithState.class;
  }

  @Override
  protected IODescWithState createFetchDataDesc(String reduceId) throws IOException {
    return new IODescWithState(reduceId, false, eq.getEqWrapperHdl());
  }

  @Override
  protected IOSimpleDDAsync createNextDesc(long sizeLimit) throws IOException {
    long remaining = sizeLimit;
    IODescWithState desc = null;
    // fetch more for existing states
    for (FetchState state : mapFetchList) {
      if (remaining == 0) {
        break;
      }
      if (desc == null) {
        desc = createFetchDataDesc(String.valueOf(state.mapReduceId._2));
      }
      long readSize = state.prepareFetch(desc, remaining);
      remaining -= readSize;
      totalInMemSize += readSize;
      if (totalInMemSize > config.getMaxMem()) {
        remaining = 0;
        break;
      }
    }
    // fetch more
    int reduceId = -1;
    while (remaining > 0) {
      curMapReduceId = null; // forward mapreduce id
      nextMapReduceId();
      if (curMapReduceId == null) {
        break;
      }
      if (reduceId > 0 & (curMapReduceId._2 != reduceId)) { // make sure entries under same reduce
        throw new IllegalStateException("multiple reduce ids");
      }
      reduceId = curMapReduceId._2;
      FetchState state = new FetchState(curMapReduceId);
      mapFetchList.add(state);
      if (desc == null) {
        desc = createFetchDataDesc(String.valueOf(reduceId));
      }
      long readSize = state.prepareFetch(desc, remaining);
      remaining -= readSize;
      totalInMemSize += readSize;
      if (totalInMemSize > config.getMaxMem()) {
        break;
      }
    }
    if (desc != null) {
      if (desc.getNbrOfEntries() == 0) {
        desc.release();
        return null;
      }
      descSet.add(desc);
    }
    return desc;
  }

  @Override
  public void checkPartitionSize() {
    if (lastMapReduceIdForReturn == null) {
      return;
    }
    metrics.incRemoteBlocksFetched(1);
  }

  @Override
  protected ByteBuf enterNewDesc(IOSimpleDDAsync desc) throws IOException {
    if (log.isDebugEnabled()) {
      StringBuilder sb = new StringBuilder();
      for (int i = 0; i < desc.getAkeyEntries().size(); i++) {
        sb.append(desc.getEntry(i).getFetchedData().readableBytes()).append(",");
      }
      log.debug("desc: " + desc + "\n returned lengths: " + sb);
    }
    List<IODataDesc.Entry> list = desc.getAkeyEntries();
    if (list != null && !list.isEmpty()) { // entries could be removed in verifyCompleted
      return list.get(0).getDataBuffer();
    }
    return null;
  }

  @Override
  protected void verifyCompleted() throws IOException {
    IODescWithState failed = null;
    int failedCnt = 0;
    for (DaosEventQueue.Attachment attachment : completedList) {
      IODescWithState desc = (IODescWithState) attachment;
      runningDescSet.remove(attachment);
      if (desc.isSucceeded()) {
        readyList.add(desc);
        desc.updateFetchState();
        continue;
      }
      failedCnt++;
      if (failed == null) {
        failed = desc;
      } else {
        desc.release();
      }
    }
    if (failedCnt > 0) {
      IOException e = new IOException("failed to read " + failedCnt + " IOSimpleDDAsync. Return code is " +
          failed.getReturnCode() + ". First failed is " + failed);
      releaseDescSet();
      throw e;
    }
  }

  @Override
  public void close(boolean force) {
    try {
      super.close(force);
      if (!(mapFetchList.isEmpty() && descSet.isEmpty())) {
        throw new IllegalStateException("not all data consumed");
      }
    } finally {
      releaseDescSet();
      mapFetchList.clear();
    }
  }

  private class FetchState {
    private long offset;
    private int times;
    private Tuple2<String, Integer> mapReduceId;
    private long size;
    private LinkedList<Tuple2<IODescWithState, IOSimpleDDAsync.AsyncEntry>> dataEntries = new LinkedList<>();

    private FetchState(Tuple2<String, Integer> mapReduceId) {
      this.mapReduceId = mapReduceId;
      this.size = partSizeMap.get(mapReduceId)._1 + 100; // +100 make less call
    }

    private long prepareFetch(IODescWithState desc, long remaining) throws IOException {
      if (size == 0L) {
        return 0L;
      }
      times++;
      long readSize = times * size;
      if (readSize > remaining) {
        readSize = remaining;
      }
      addFetchEntry(desc, mapReduceId._1, offset, readSize); // update offset after fetching
      IOSimpleDDAsync.AsyncEntry entry = desc.getEntry(desc.getNbrOfEntries() - 1);
      dataEntries.add(new Tuple2<>(desc, entry));
      desc.putState(entry, this);
      return readSize;
    }

    private boolean updateState(IOSimpleDDAsync.AsyncEntry entry) {
      Tuple2<IODescWithState, IOSimpleDDAsync.AsyncEntry> lastTuple = dataEntries.getLast();
      if (entry != lastTuple._2) {
        throw new IllegalStateException("entries mismatch");
      }
      int actualSize = entry.getActualSize();
      offset += actualSize;
      int requestSize = entry.getRequestSize();
      if (requestSize > actualSize) {
        size = 0L; // indicate end of akey content
      }
      if (actualSize == 0) {
        totalInMemSize -= entry.getDataBuffer().capacity();
        entry.releaseDataBuffer(); // release mem ASAP
        dataEntries.remove(lastTuple);
        return true;
      }
      return false;
    }

    private ByteBuf tryCurrentEntry() {
      if (currentEntry != null && (!currentEntry.isFetchBufReleased())) {
        ByteBuf buf = currentEntry.getFetchedData();
        if (buf.readableBytes() > 0) {
          return buf;
        }
        // release buffer as soon as possible
        currentEntry.releaseDataBuffer();
        totalInMemSize -= buf.capacity();
      }
      return null;
    }

    private ByteBuf getBuffer() throws IOException {
      while (true) {
        ByteBuf buf = readMore();
        if (buf != null) {
          return buf;
        }
        if (reachEnd()) {
          metrics.incRemoteBlocksFetched(1);
          break;
        }
        if (readFromDaos() == null) {
          break;
        }
      }
      return null;
    }

    private ByteBuf readMore() {
      while (true) {
        ByteBuf buf = tryCurrentEntry();
        if (buf != null) {
          return buf;
        }
        // remove and release entry
        if (currentEntry != null) {
          Tuple2<IODescWithState, IOSimpleDDAsync.AsyncEntry> tuple = dataEntries.removeFirst();
          tuple._1.removeState(tuple._2);
          currentEntry = null;
        }
        // get next tuple
        Tuple2<IODescWithState, IOSimpleDDAsync.AsyncEntry> nextTuple = dataEntries.peekFirst();
        if (nextTuple != null) {
          currentEntry = nextTuple._2;
          // update metrics
          metrics.incRemoteBytesRead(currentEntry.getFetchedData().readableBytes());
        } else {
          break;
        }
      }
      return null;
    }

    private boolean reachEnd() {
      return size == 0L;
    }
  }

  private class IODescWithState extends IOSimpleDDAsync {
    private Map<AsyncEntry, FetchState> entryStateMap = new HashMap<>();

    private IODescWithState(String dkey, boolean updateOrFetch, long eqWrapperHandle) throws IOException {
      super(dkey, updateOrFetch, eqWrapperHandle);
    }

    private void putState(IOSimpleDDAsync.AsyncEntry entry, FetchState state) {
      entryStateMap.put(entry, state);
    }

    private void removeState(IOSimpleDDAsync.AsyncEntry entry) {
      entry.releaseDataBuffer();
      if (entryStateMap.remove(entry) == null) {
        throw new IllegalStateException("failed to remove state from Desc");
      }
      tryReleaseState();
    }

    private void updateFetchState() {
      Iterator<Map.Entry<AsyncEntry, FetchState>> it = entryStateMap.entrySet().iterator();
      while (it.hasNext()) {
        Map.Entry<AsyncEntry, FetchState> entry = it.next();
        if (entry.getValue().updateState(entry.getKey())) {
          entry.getKey().releaseDataBuffer();
          it.remove();
        }
      }
      tryReleaseState();
    }

    private void tryReleaseState() {
      if (entryStateMap.isEmpty()) {
        release();
        if (!descSet.remove(this)) {
          throw new IllegalStateException("failed to remove desc from descset");
        }
      }
    }
  }
}

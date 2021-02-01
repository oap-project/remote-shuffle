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

import io.daos.obj.DaosObject;
import io.daos.obj.IODataDesc;
import io.netty.buffer.ByteBuf;
import io.netty.util.internal.ObjectPool;
import org.apache.spark.shuffle.ShuffleReadMetricsReporter;
import org.apache.spark.storage.BlockId;
import org.apache.spark.storage.BlockManagerId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;
import scala.Tuple3;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

/**
 * A class with {@link DaosObject} wrapped to read data from DAOS in either caller's thread or
 * dedicated executor thread. The actual read is performed by {@link DaosObject#fetch(IODataDesc)}.
 *
 * User just calls {@link #nextBuf()} and reads from buffer repeatedly until no buffer returned.
 * Reader determines when and how (caller thread or from dedicated thread) based on config, to read from DAOS
 * as well as controlling buffer size and task batch size. It also has some fault tolerance ability, like
 * re-read from caller thread if task doesn't respond from the dedicated threads.
 */
public class DaosReaderSync extends TaskSubmitter implements DaosReader {

  private DaosObject object;

  private Map<DaosReader, Integer> readerMap;

  private ReaderConfig config;

  protected LinkedHashMap<Tuple2<Long, Integer>, Tuple3<Long, BlockId, BlockManagerId>> partSizeMap;

  private Iterator<Tuple2<Long, Integer>> mapIdIt;

  private ShuffleReadMetricsReporter metrics;

  protected long currentPartSize;

  protected Tuple2<Long, Integer> curMapReduceId;
  protected Tuple2<Long, Integer> lastMapReduceIdForSubmit;
  protected Tuple2<Long, Integer> lastMapReduceIdForReturn;
  protected int curOffset;
  protected boolean nextMap;

  protected int totalParts;
  protected int partsRead;

  private ReadTaskContext selfCurrentCtx;
  private IODataDesc currentDesc;
  private IODataDesc.Entry currentEntry;

  private boolean fromOtherThread;

  private int entryIdx;

  private static Logger logger = LoggerFactory.getLogger(DaosReader.class);

  /**
   * construct DaosReader with <code>object</code> and dedicated read <code>executors</code>.
   *
   * @param object
   * opened DaosObject
   * @param config
   * reader configuration
   * @param executor
   * single thread executor
   */
  public DaosReaderSync(DaosObject object, ReaderConfig config, BoundThreadExecutors.SingleThreadExecutor executor) {
    super(executor);
    this.object = object;
    this.config = config;
    this.fromOtherThread = config.isFromOtherThread();
    if (fromOtherThread && executor == null) {
      throw new IllegalArgumentException("executor should not be null if read from other thread");
    }
  }

  @Override
  public DaosObject getObject() {
    return object;
  }

  @Override
  public void close(boolean force) {
    boolean allReleased = true;
    allReleased &= cleanupSubmitted(force);
    allReleased &= cleanupConsumed(force);
    if (allReleased) {
      if (readerMap != null) {
        readerMap.remove(this);
        readerMap = null;
      }
    }
  }

  @Override
  public void setReaderMap(Map<DaosReader, Integer> readerMap) {
    readerMap.put(this, 0);
    this.readerMap = readerMap;
  }

  public boolean hasExecutors() {
    return executor != null;
  }

  /**
   * invoke this method when fromOtherThread is false.
   *
   * @return
   * @throws {@link IOException}
   */
  public ByteBuf readBySelf() throws IOException {
    if (lastCtx != null) { // duplicated IODataDescs which were submitted to other thread, but cancelled
      ByteBuf buf = readDuplicated(false);
      if (buf != null) {
        return buf;
      }
    }
    // all submitted were duplicated. Now start from mapId iterator.
    IODataDesc desc = createNextDesc(config.getMaxBytesInFlight());
    return getBySelf(desc, lastMapReduceIdForSubmit);
  }

  /**
   * get available buffer after iterating current buffer, next buffer in current desc and next desc.
   *
   * @return buffer with data read from DAOS
   * @throws IOException
   */
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
    // from next partition
    if (fromOtherThread) {
      // next ready queue
      if (headCtx != null) {
        return tryNextTaskContext();
      }
      // get data by self and submit request for remaining data
      return getBySelfAndSubmitMore(config.getMinReadSize());
    }
    // get data by self after fromOtherThread disabled
    return readBySelf();
  }

  @Override
  public boolean isNextMap() {
    return nextMap;
  }

  @Override
  public void setNextMap(boolean nextMap) {
    this.nextMap = nextMap;
  }

  private ByteBuf tryNextTaskContext() throws IOException {
    // make sure there are still some read tasks waiting/running/returned from other threads
    // or they are readDuplicated by self
    if (totalSubmitted == 0 || selfCurrentCtx == lastCtx) {
      return getBySelfAndSubmitMore(config.getMaxBytesInFlight());
    }
    if (totalSubmitted < 0) {
      throw new IllegalStateException("total submitted should be no less than 0. " + totalSubmitted);
    }
    try {
      IODataDesc desc;
      if ((desc = tryGetFromOtherThread()) != null) {
        submitMore();
        return validateLastEntryAndGetBuf(desc.getEntry(entryIdx));
      }
      // duplicate and get data by self
      return readDuplicated(true);
    } catch (InterruptedException e) {
      throw new IOException("read interrupted.", e);
    }
  }

  /**
   * we have to duplicate submitted desc since mapId was moved.
   *
   * @return
   * @throws IOException
   */
  private ByteBuf readDuplicated(boolean expectNotNullCtx) throws IOException {
    ReadTaskContext context = getNextNonReturnedCtx();
    if (context == null) {
      if (expectNotNullCtx) {
        throw new IllegalStateException("context should not be null. totalSubmitted: " + totalSubmitted);
      }
      if (!fromOtherThread) {
        lastCtx = null;
      }
      return null;
    }
    IODataDesc newDesc = context.getDesc().duplicate();
    ByteBuf buf = getBySelf(newDesc, context.getMapReduceId());
    selfCurrentCtx = context;
    return buf;
  }

  private IODataDesc tryGetFromOtherThread() throws InterruptedException, IOException {
    IODataDesc desc = tryGetValidCompleted();
    if (desc != null) {
      return desc;
    }
    // check completion
    if ((!mapIdIt.hasNext()) && curMapReduceId == null && totalSubmitted == 0) {
      return null;
    }
    // wait for specified time
    desc = waitForValidFromOtherThread();
    if (desc != null) {
      return desc;
    }
    // check wait times and cancel task
    // TODO: stop reading from other threads?
    cancelTasks(false);
    return null;
  }

  private IODataDesc waitForValidFromOtherThread() throws InterruptedException, IOException {
    IODataDesc desc;
    while (true) {
      long start = System.nanoTime();
      boolean timeout = waitForCondition(config.getWaitDataTimeMs());
      metrics.incFetchWaitTime(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start));
      if (timeout) {
        exceedWaitTimes++;
        if (logger.isDebugEnabled()) {
          logger.debug("exceed wait: {}ms, times: {}", config.getWaitDataTimeMs(), exceedWaitTimes);
        }
        if (exceedWaitTimes >= config.getWaitTimeoutTimes()) {
          return null;
        }
      }
      // get some results after wait
      desc = tryGetValidCompleted();
      if (desc != null) {
        return desc;
      }
    }
  }

  protected IODataDesc tryGetValidCompleted() throws IOException {
    if (moveForward()) {
      return currentDesc;
    }
    return null;
  }

  private ByteBuf tryCurrentDesc() throws IOException {
    if (currentDesc != null) {
      ByteBuf buf;
      while (entryIdx < currentDesc.getNbrOfEntries()) {
        IODataDesc.Entry entry = currentDesc.getEntry(entryIdx);
        buf = validateLastEntryAndGetBuf(entry);
        if (buf.readableBytes() > 0) {
          return buf;
        }
        entryIdx++;
      }
      entryIdx = 0;
      // no need to release desc since all its entries are released in tryCurrentEntry and
      // internal buffers are released after object.fetch
      // reader.close will release all in case of failure
      currentDesc = null;
    }
    return null;
  }

  private ByteBuf tryCurrentEntry() {
    if (currentEntry != null && !currentEntry.isFetchBufReleased()) {
      ByteBuf buf = currentEntry.getFetchedData();
      if (buf.readableBytes() > 0) {
        return buf;
      }
      // release buffer as soon as possible
      currentEntry.releaseDataBuffer();
      entryIdx++;
    }
    // not null currentEntry since it will be used for size validation
    return null;
  }

  /**
   * for first read.
   *
   * @param selfReadLimit
   * @return
   * @throws IOException
   */
  private ByteBuf getBySelfAndSubmitMore(long selfReadLimit) throws IOException {
    entryIdx = 0;
    // fetch the next by self
    IODataDesc desc = createNextDesc(selfReadLimit);
    Tuple2<Long, Integer> mapreduceId = lastMapReduceIdForSubmit;
    try {
      if (fromOtherThread) {
        submitMore();
      }
    } catch (Exception e) {
      desc.release();
      if (e instanceof IOException) {
        throw (IOException)e;
      }
      throw new IOException("failed to submit more", e);
    }
    // first time read from reduce task
    return getBySelf(desc, mapreduceId);
  }

  private void submitMore() throws IOException {
    while (totalSubmitted < config.getReadBatchSize() && totalInMemSize < config.getMaxMem()) {
      IODataDesc taskDesc = createNextDesc(config.getMaxBytesInFlight());
      if (taskDesc == null) {
        break;
      }
      submit(taskDesc, lastMapReduceIdForSubmit);
    }
  }

  private ByteBuf getBySelf(IODataDesc desc, Tuple2<Long, Integer> mapreduceId) throws IOException {
    // get data by self, no need to release currentDesc
    if (desc == null) { // reach end
      return null;
    }
    boolean releaseBuf = false;
    try {
      object.fetch(desc);
      currentDesc = desc;
      ByteBuf buf = validateLastEntryAndGetBuf(desc.getEntry(entryIdx));
      lastMapReduceIdForReturn = mapreduceId;
      return buf;
    } catch (IOException | IllegalStateException e) {
      releaseBuf = true;
      throw e;
    } finally {
      desc.release(releaseBuf);
    }
  }

  private IODataDesc createNextDesc(long sizeLimit) throws IOException {
    long remaining = sizeLimit;
    int reduceId = -1;
    long mapId;
    IODataDesc desc = null;
    while (remaining > 0) {
      nextMapReduceId();
      if (curMapReduceId == null) {
        break;
      }
      if (reduceId > 0 && curMapReduceId._2 != reduceId) { // make sure entries under same reduce
        break;
      }
      reduceId = curMapReduceId._2;
      mapId = curMapReduceId._1;
      lastMapReduceIdForSubmit = curMapReduceId;
      long readSize = partSizeMap.get(curMapReduceId)._1() - curOffset;
      long offset = curOffset;
      if (readSize > remaining) {
        readSize = remaining;
        curOffset += readSize;
      } else {
        curOffset = 0;
        curMapReduceId = null;
      }
      if (desc == null) {
        desc = object.createDataDescForFetch(String.valueOf(reduceId), IODataDesc.IodType.ARRAY, 1);
      }
      desc.addEntryForFetch(String.valueOf(mapId), (int)offset, (int)readSize);
      remaining -= readSize;
    }
    return desc;
  }

  private void nextMapReduceId() {
    if (curMapReduceId != null) {
      return;
    }
    curOffset = 0;
    if (mapIdIt.hasNext()) {
      curMapReduceId = mapIdIt.next();
      partsRead++;
    } else {
      curMapReduceId = null;
    }
  }

  private ByteBuf validateLastEntryAndGetBuf(IODataDesc.Entry entry) throws IOException {
    ByteBuf buf = entry.getFetchedData();
    int byteLen = buf.readableBytes();
    nextMap = false;
    if (currentEntry != null && entry != currentEntry) {
      if (entry.getKey().equals(currentEntry.getKey())) {
        currentPartSize += byteLen;
      } else {
        checkPartitionSize();
        nextMap = true;
        currentPartSize = byteLen;
      }
    }
    currentEntry = entry;
    metrics.incRemoteBytesRead(byteLen);
    return buf;
  }

  @Override
  public void checkPartitionSize() throws IOException {
    if (lastMapReduceIdForReturn == null) {
      return;
    }
    // partition size is not accurate after compress/decompress
    long size = partSizeMap.get(lastMapReduceIdForReturn)._1();
    if (size < 35 * 1024 * 1024 * 1024 && currentPartSize * 1.1 < size) {
      throw new IOException("expect partition size " + partSizeMap.get(lastMapReduceIdForReturn) +
          ", actual size " + currentPartSize + ", mapId and reduceId: " + lastMapReduceIdForReturn);
    }
    metrics.incRemoteBlocksFetched(1);
  }

  @Override
  public void checkTotalPartitions() throws IOException {
    if (partsRead != totalParts) {
      throw new IOException("expect total partitions to be read: " + totalParts + ", actual read: " + partsRead);
    }
  }

  @Override
  public void prepare(LinkedHashMap<Tuple2<Long, Integer>, Tuple3<Long, BlockId, BlockManagerId>> partSizeMap,
                     long maxBytesInFlight, long maxReqSizeShuffleToMem,
                     ShuffleReadMetricsReporter metrics) {
    this.partSizeMap = partSizeMap;
    this.config = config.copy(maxBytesInFlight, maxReqSizeShuffleToMem);
    this.metrics = metrics;
    this.totalParts = partSizeMap.size();
    mapIdIt = partSizeMap.keySet().iterator();
  }

  @Override
  public Tuple2<Long, Integer> curMapReduceId() {
    return lastMapReduceIdForSubmit;
  }

  @Override
  protected ReadTaskContext getNextNonReturnedCtx() {
    // in case no even single return from other thread
    // check selfCurrentCtx since the wait could span multiple contexts/descs
    ReadTaskContext curCtx = selfCurrentCtx == null ?
        getCurrentCtx() : selfCurrentCtx;
    if (curCtx == null) {
      return getHeadCtx();
    }
    // no consumedStack push and no totalInMemSize and totalSubmitted update
    // since they will be updated when the task context finally returned
    return curCtx.getNext();
  }

  @Override
  protected boolean consumed(LinkedTaskContext consumed) {
    return !consumed.isCancelled();
  }

  @Override
  protected boolean validateReturned(LinkedTaskContext context) throws IOException {
    if (context.isCancelled()) {
      return false;
    }
    selfCurrentCtx = null; // non-cancelled currentCtx overrides selfCurrentCtx
    lastMapReduceIdForReturn = ((ReadTaskContext)context).getMapReduceId();
    IODataDesc desc = context.getDesc();
    if (!desc.isSucceeded()) {
      String msg = "failed to get data from DAOS, desc: " + desc.toString(4096);
      if (desc.getCause() != null) {
        throw new IOException(msg, desc.getCause());
      } else {
        throw new IllegalStateException(msg + "\nno exception got. logic error or crash?");
      }
    }
    currentDesc = desc;
    return true;
  }

  @Override
  protected Runnable newTask(LinkedTaskContext context) {
    return ReadTask.newInstance((ReadTaskContext) context);
  }

  @Override
  protected LinkedTaskContext createTaskContext(IODataDesc desc, Object morePara) {
    return new ReadTaskContext(object, counter, lock, condition, desc, morePara);
  }

  @Override
  public ReadTaskContext getCurrentCtx() {
    return (ReadTaskContext) currentCtx;
  }

  @Override
  public ReadTaskContext getHeadCtx() {
    return (ReadTaskContext) headCtx;
  }

  @Override
  public ReadTaskContext getLastCtx() {
    return (ReadTaskContext) lastCtx;
  }

  @Override
  public String toString() {
    return "DaosReaderSync{" +
        "object=" + object +
        '}';
  }

  /**
   * Task to read from DAOS. Task itself is cached to reduce GC time.
   * To reuse task for different reads, prepare and reset {@link ReadTaskContext} by calling
   * {@link #newInstance(ReadTaskContext)}
   */
  static final class ReadTask implements Runnable {
    private ReadTaskContext context;
    private final ObjectPool.Handle<ReadTask> handle;

    private static final ObjectPool<ReadTask> objectPool = ObjectPool.newPool(handle -> new ReadTask(handle));

    private static final Logger log = LoggerFactory.getLogger(ReadTask.class);

    static ReadTask newInstance(ReadTaskContext context) {
      ReadTask task = objectPool.get();
      task.context = context;
      return task;
    }

    private ReadTask(ObjectPool.Handle<ReadTask> handle) {
      this.handle = handle;
    }

    @Override
    public void run() {
      boolean cancelled = context.cancelled;
      try {
        if (!cancelled) {
          context.object.fetch(context.desc);
        }
      } catch (Exception e) {
        log.error("failed to read for " + context.desc, e);
      } finally {
        // release desc buffer and keep data buffer
        context.desc.release(cancelled);
        context.signal();
        context = null;
        handle.recycle(this);
      }
    }
  }

  /**
   * Context for read task. It holds all other object to read and sync between caller thread and read thread.
   * It should be cached in caller thread for reusing.
   */
  static final class ReadTaskContext extends LinkedTaskContext {

    /**
     * constructor with all parameters. Some of them can be reused later.
     *
     * @param object
     * DAOS object to fetch data from DAOS
     * @param counter
     * counter to indicate how many data ready for being consumed
     * @param takeLock
     * lock to work with <code>notEmpty</code> condition to signal caller thread there is data ready to be consumed
     * @param notEmpty
     * condition to signal there is some data ready
     * @param desc
     * desc object to describe which part of data to be fetch and hold returned data
     * @param mapReduceId
     * to track which map reduce ID this task fetches data for
     */
    ReadTaskContext(DaosObject object, AtomicInteger counter, Lock takeLock, Condition notEmpty,
                           IODataDesc desc, Object mapReduceId) {
      super(object, counter, takeLock, notEmpty);
      this.desc = desc;
      this.morePara = mapReduceId;
    }

    @Override
    public ReadTaskContext getNext() {
      return (ReadTaskContext) next;
    }

    public Tuple2<Long, Integer> getMapReduceId() {
      return (Tuple2<Long, Integer>) morePara;
    }
  }

  /**
   * Thread factory for DAOS read tasks.
   */
  protected static class ReadThreadFactory implements ThreadFactory {
    private AtomicInteger id = new AtomicInteger(0);

    @Override
    public Thread newThread(Runnable runnable) {
      Thread t;
      String name = "daos_read_" + id.getAndIncrement();
      if (runnable == null) {
        t = new Thread(name);
      } else {
        t = new Thread(runnable, name);
      }
      t.setDaemon(true);
      t.setUncaughtExceptionHandler((thread, throwable) ->
          logger.error("exception occurred in thread " + name, throwable));
      return t;
    }
  }
}

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

import io.daos.obj.DaosObject;
import io.daos.obj.IODataDescBase;
import io.daos.obj.IODataDescSync;
import io.netty.buffer.ByteBuf;
import io.netty.util.internal.ObjectPool;
import org.apache.spark.shuffle.ShuffleReadMetricsReporter;
import org.apache.spark.storage.BlockId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

/**
 * A class with {@link DaosObject} wrapped to read data from DAOS in either caller's thread or
 * dedicated executor thread. The actual read is performed by {@link DaosObject#fetch(IODataDescSync)}.
 *
 * User just calls {@link #nextBuf()} and reads from buffer repeatedly until no buffer returned.
 * Reader determines when and how (caller thread or from dedicated thread) based on config, to read from DAOS
 * as well as controlling buffer size and task batch size. It also has some fault tolerance ability, like
 * re-read from caller thread if task doesn't respond from the dedicated threads.
 */
public class DaosReaderSync extends TaskSubmitter implements DaosReader {

  private InnerReader reader;

  private ReadTaskContext selfCurrentCtx;

  private boolean fromOtherThread;

  private static Logger logger = LoggerFactory.getLogger(DaosReaderSync.class);

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
    reader = new InnerReader(object, config);
    this.fromOtherThread = config.isFromOtherThread();
    if (fromOtherThread && executor == null) {
      throw new IllegalArgumentException("executor should not be null if read from other thread");
    }
  }

  @Override
  public DaosObject getObject() {
    return reader.object;
  }

  @Override
  public void close(boolean force) {
    boolean allReleased = true;
    allReleased &= cleanupSubmitted(force);
    allReleased &= cleanupConsumed(force);
    if (allReleased) {
      reader.close(force);
    }
  }

  @Override
  public void setReaderMap(Map<DaosReader, Integer> readerMap) {
    reader.setReaderMap(readerMap);
  }

  public boolean hasExecutors() {
    return executor != null;
  }

  /**
   * get available buffer after iterating current buffer, next buffer in current desc and next desc.
   *
   * @return buffer with data read from DAOS
   * @throws IOException
   */
  @Override
  public ByteBuf nextBuf() throws IOException {
    return reader.nextBuf();
  }

  @Override
  public boolean isNextMap() {
    return reader.isNextMap();
  }

  @Override
  public void setNextMap(boolean nextMap) {
    reader.setNextMap(nextMap);
  }

  @Override
  public void nextMapReduceId() {
    reader.nextMapReduceId();
  }

  @Override
  public void checkPartitionSize() throws IOException {
    reader.checkPartitionSize();
  }

  @Override
  public void checkTotalPartitions() throws IOException {
    reader.checkTotalPartitions();
  }

  @Override
  public void prepare(LinkedHashMap<Tuple2<String, Integer>, Tuple2<Long, BlockId>> partSizeMap,
                     long maxBytesInFlight, long maxReqSizeShuffleToMem,
                     ShuffleReadMetricsReporter metrics) {
    reader.prepare(partSizeMap, maxBytesInFlight, maxReqSizeShuffleToMem, metrics);
  }

  @Override
  public Tuple2<String, Integer> curMapReduceId() {
    return reader.lastMapReduceIdForSubmit;
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
    reader.lastMapReduceIdForReturn = ((ReadTaskContext)context).getMapReduceId();
    IODataDescSync desc = context.getDesc();
    if (!desc.isSucceeded()) {
      String msg = "failed to get data from DAOS, desc: " + desc.toString(4096);
      if (desc.getCause() != null) {
        throw new IOException(msg, desc.getCause());
      } else {
        throw new IllegalStateException(msg + "\nno exception got. logic error or crash?");
      }
    }
    reader.currentDesc = desc;
    return true;
  }

  @Override
  protected Runnable newTask(LinkedTaskContext context) {
    return ReadTask.newInstance((ReadTaskContext) context);
  }

  @Override
  protected LinkedTaskContext createTaskContext(IODataDescSync desc, Object morePara) {
    return new ReadTaskContext(reader.object, counter, lock, condition, desc, morePara);
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
        "object=" + reader.object +
        '}';
  }

  private class InnerReader extends DaosReaderBase {
    InnerReader(DaosObject object, ReaderConfig config) {
      super(object, config);
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
      // from next partition
      if (fromOtherThread) {
        // next ready queue
        if (headCtx != null) {
          return tryNextTaskContext();
        }
        // get data by self and submit request for remaining data
        return getBySelfAndSubmitMore(reader.config.getMinReadSize());
      }
      // get data by self after fromOtherThread disabled
      return readBySelf();
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
      IODataDescSync desc = (IODataDescSync) createNextDesc(config.getMaxBytesInFlight());
      return getBySelf(desc, lastMapReduceIdForSubmit);
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
        IODataDescBase desc;
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
      IODataDescSync newDesc = context.getDesc().duplicate();
      ByteBuf buf = getBySelf(newDesc, context.getMapReduceId());
      selfCurrentCtx = context;
      return buf;
    }

    private IODataDescBase tryGetFromOtherThread() throws InterruptedException, IOException {
      IODataDescBase desc = tryGetValidCompleted();
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
      cancelTasks(false);
      return null;
    }

    private IODataDescBase waitForValidFromOtherThread() throws InterruptedException, IOException {
      IODataDescBase desc;
      long start = System.nanoTime();
      boolean timeout = waitForCondition(config.getWaitDataTimeMs());
      metrics.incFetchWaitTime(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start));
      if (timeout) {
        exceedWaitTimes++;
        if (logger.isDebugEnabled()) {
          logger.debug("exceed wait: {}ms, times: {}", config.getWaitDataTimeMs(), exceedWaitTimes);
        }
        return null;
      }
      // get some results after wait
      desc = tryGetValidCompleted();
      if (desc != null) {
        return desc;
      }
      return null;
    }

    private void submitMore() throws IOException {
      while (totalSubmitted < config.getReadBatchSize() && totalInMemSize < config.getMaxMem()) {
        IODataDescSync taskDesc = (IODataDescSync) createNextDesc(config.getMaxBytesInFlight());
        if (taskDesc == null) {
          break;
        }
        submit(taskDesc, lastMapReduceIdForSubmit);
      }
    }

    protected IODataDescSync tryGetValidCompleted() throws IOException {
      if (moveForward()) {
        return (IODataDescSync) currentDesc;
      }
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
      IODataDescSync desc = (IODataDescSync) createNextDesc(selfReadLimit);
      Tuple2<String, Integer> mapreduceId = reader.lastMapReduceIdForSubmit;
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

    @Override
    protected IODataDescBase createFetchDataDesc(String reduceId) throws IOException {
      return object.createDataDescForFetch(String.valueOf(reduceId), IODataDescSync.IodType.ARRAY,
          1);
    }

    @Override
    protected void addFetchEntry(IODataDescBase desc, String mapId, long offset, long readSize) throws IOException {
      if (readSize > Integer.MAX_VALUE) {
        throw new IllegalArgumentException("readSize should not exceed " + Integer.MAX_VALUE);
      }
      ((IODataDescSync)desc).addEntryForFetch(mapId, offset, (int)readSize);
    }

    private ByteBuf getBySelf(IODataDescSync desc, Tuple2<String, Integer> mapreduceId) throws IOException {
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
                           IODataDescSync desc, Object mapReduceId) {
      super(object, counter, takeLock, notEmpty);
      this.desc = desc;
      this.morePara = mapReduceId;
    }

    @Override
    public ReadTaskContext getNext() {
      return (ReadTaskContext) next;
    }

    public Tuple2<String, Integer> getMapReduceId() {
      return (Tuple2<String, Integer>) morePara;
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

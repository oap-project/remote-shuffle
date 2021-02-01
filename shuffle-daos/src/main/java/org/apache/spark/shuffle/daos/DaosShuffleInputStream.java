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
import org.apache.spark.SparkConf;
import org.apache.spark.SparkEnv;
import org.apache.spark.shuffle.ShuffleReadMetricsReporter;
import org.apache.spark.storage.BlockId;
import org.apache.spark.storage.BlockManagerId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;
import scala.Tuple3;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.concurrent.*;

@NotThreadSafe
/**
 * A inputstream for reading shuffled data being consisted of multiple map outputs.
 *
 * All records in one specific map output are from same KryoSerializer or Java serializer. To facilitate reading
 * multiple map outputs in this one inputstream, the read methods return -1 to indicate the completion of current
 * map output. Caller should call {@link DaosShuffleInputStream#isCompleted()} to check if all map outputs are read.
 *
 * To read more data from next map output, user should call {@link #nextMap()} before read.
 */
public class DaosShuffleInputStream extends InputStream {

  private DaosReader reader;

  private DaosObject object;

  private BoundThreadExecutors.SingleThreadExecutor executor;

  private ReaderConfig config;

  private ShuffleReadMetricsReporter metrics;

  private boolean fromOtherThread;

  private volatile boolean cleaned;

  private boolean completed;

  // ensure the order of partition
  // (mapid, reduceid) -> (length, BlockId, BlockManagerId)
  private LinkedHashMap<Tuple2<Long, Integer>, Tuple3<Long, BlockId, BlockManagerId>> partSizeMap;
  private Iterator<Tuple2<Long, Integer>> mapIdIt;

  private BufferSource source;

  private static final Logger log = LoggerFactory.getLogger(DaosShuffleInputStream.class);

  /**
   * constructor with ordered map outputs info. Check {@link ReaderConfig} for more paras controlling
   * how data being read from DAOS.
   *
   * @param reader
   * daos reader
   * @param partSizeMap
   * ordered map outputs info. They are organize as (mapid, reduceid) -> (length, BlockId, BlockManagerId)
   * @param maxBytesInFlight
   * how many bytes can be read concurrently
   * @param maxMem
   * maximum data can be put in memory
   * @param metrics
   * read metrics
   */
  public DaosShuffleInputStream(
      DaosReader reader,
      LinkedHashMap<Tuple2<Long, Integer>, Tuple3<Long, BlockId, BlockManagerId>> partSizeMap,
      long maxBytesInFlight, long maxMem, ShuffleReadMetricsReporter metrics) {
    this.partSizeMap = partSizeMap;
    this.reader = reader;
    this.config = new ReaderConfig(maxBytesInFlight, maxMem);
    this.fromOtherThread = config.fromOtherThread;
    if (fromOtherThread) {
      this.executor = reader.nextReaderExecutor();
    }
    this.source = new BufferSource(executor);
    reader.register(source);
    this.object = reader.getObject();
    this.metrics = metrics;
    this.mapIdIt = partSizeMap.keySet().iterator();
  }

  public BlockId getCurBlockId() {
    if (source.lastMapReduceIdForSubmit == null) {
      return null;
    }
    return partSizeMap.get(source.lastMapReduceIdForSubmit)._2();
  }

  public BlockManagerId getCurOriginAddress() {
    if (source.lastMapReduceIdForSubmit == null) {
      return null;
    }
    return partSizeMap.get(source.lastMapReduceIdForSubmit)._3();
  }

  public long getCurMapIndex() {
    if (source.lastMapReduceIdForSubmit == null) {
      return -1;
    }
    return source.lastMapReduceIdForSubmit._1;
  }

  @Override
  public int read() throws IOException {
    while (!completed) {
      ByteBuf buf = source.nextBuf();
      if (buf == null) { // reach end
        complete();
        return -1;
      }
      if (source.newMap) { // indication to close upper layer object inputstream
        return -1;
      }
      if (buf.readableBytes() >= 1) {
        return buf.readByte();
      }
    }
    return -1;
  }

  @Override
  public int read(byte[] bytes) throws IOException {
    return read(bytes, 0, bytes.length);
  }

  @Override
  public int read(byte[] bytes, int offset, int length) throws IOException {
    int len = length;
    while (!completed) {
      ByteBuf buf = source.nextBuf();
      if (buf == null) { // reach end
        complete();
        int r = length - len;
        return r == 0 ? -1 : r;
      }
      if (source.newMap) { // indication to close upper layer object inputstream
        int r = length - len;
        return r == 0 ? -1 : r;
      }
      if (len <= buf.readableBytes()) {
        buf.readBytes(bytes, offset, len);
        return length;
      }
      int maxRead = buf.readableBytes();
      buf.readBytes(bytes, offset, maxRead);
      offset += maxRead;
      len -= maxRead;
    }
    return -1;
  }

  /**
   * upper layer should call this method to read more map output
   */
  public void nextMap() {
    source.newMap = false;
  }

  private void complete() throws IOException {
    if (!completed) {
      source.checkPartitionSize();
      source.checkTotalPartitions();
      completed = true;
    }
  }

  private void cleanup() {
    if (!cleaned) {
      boolean allReleased = source.cleanup(false);
      if (allReleased) {
        reader.unregister(source);
      }
      source = null;
      cleaned = true;
      completed = true;
    }
  }

  /**
   * no actual close if it's not completed yet.
   */
  @Override
  public void close() {
    close(false);
  }

  /**
   * force close stream when task is competed or error is occurred.
   *
   * @param force
   */
  public void close(boolean force) {
    if (force || completed) {
      cleanup();
    }
  }

  public boolean isCompleted() {
    return completed;
  }

  /**
   * Source of map output data. User just calls {@link #nextBuf()} and reads from buffer repeatedly until no buffer
   * returned.
   * BufferSource does all other dirty things, like when and how (caller thread or from dedicated thread) to
   * read from DAOS as well as controlling buffer size and task batch size.
   * It also has some fault tolerance ability, like re-read from caller thread if task doesn't respond from the
   * dedicated threads.
   */
  public class BufferSource extends TaskSubmitter {
    private DaosReader.ReadTaskContext selfCurrentCtx;
    private IODataDesc currentDesc;
    private IODataDesc.Entry currentEntry;
    private long currentPartSize;

    private int entryIdx;
    private Tuple2<Long, Integer> curMapReduceId;
    private Tuple2<Long, Integer> lastMapReduceIdForSubmit;
    private Tuple2<Long, Integer> lastMapReduceIdForReturn;
    private int curOffset;
    private boolean newMap;

    private int totalParts = partSizeMap.size();
    private int partsRead;

    protected BufferSource(BoundThreadExecutors.SingleThreadExecutor executor) {
      super(executor);
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
      IODataDesc desc = createNextDesc(config.maxBytesInFlight);
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
        return getBySelfAndSubmitMore(config.minReadSize);
      }
      // get data by self after fromOtherThread disabled
      return readBySelf();
    }

    private ByteBuf tryNextTaskContext() throws IOException {
      // make sure there are still some read tasks waiting/running/returned from other threads
      // or they are readDuplicated by self
      if (totalSubmitted == 0 || selfCurrentCtx == lastCtx) {
        return getBySelfAndSubmitMore(config.maxBytesInFlight);
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
      DaosReader.ReadTaskContext context = getNextNonReturnedCtx();
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

    @Override
    protected DaosReader.ReadTaskContext getNextNonReturnedCtx() {
      // in case no even single return from other thread
      // check selfCurrentCtx since the wait could span multiple contexts/descs
      DaosReader.ReadTaskContext curCtx = selfCurrentCtx == null ?
          getCurrentCtx() : selfCurrentCtx;
      if (curCtx == null) {
        return getHeadCtx();
      }
      // no consumedStack push and no totalInMemSize and totalSubmitted update
      // since they will be updated when the task context finally returned
      return curCtx.getNext();
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
        boolean timeout = waitForCondition(config.waitDataTimeMs);
        metrics.incFetchWaitTime(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start));
        if (timeout) {
          exceedWaitTimes++;
          if (log.isDebugEnabled()) {
            log.debug("exceed wait: {}ms, times: {}", config.waitDataTimeMs, exceedWaitTimes);
          }
          if (exceedWaitTimes >= config.waitTimeoutTimes) {
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
      lastMapReduceIdForReturn = ((DaosReader.ReadTaskContext)context).getMapReduceId();
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
      while (totalSubmitted < config.readBatchSize && totalInMemSize < config.maxMem) {
        IODataDesc taskDesc = createNextDesc(config.maxBytesInFlight);
        if (taskDesc == null) {
          break;
        }
        submit(taskDesc, lastMapReduceIdForSubmit);
      }
    }

    @Override
    protected Runnable newTask(LinkedTaskContext context) {
      return DaosReader.ReadTask.newInstance((DaosReader.ReadTaskContext) context);
    }

    @Override
    protected LinkedTaskContext createTaskContext(IODataDesc desc, Object morePara) {
      return new DaosReader.ReadTaskContext(object, counter, lock, condition, desc, morePara);
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
      newMap = false;
      if (currentEntry != null && entry != currentEntry) {
        if (entry.getKey().equals(currentEntry.getKey())) {
          currentPartSize += byteLen;
        } else {
          checkPartitionSize();
          newMap = true;
          currentPartSize = byteLen;
        }
      }
      currentEntry = entry;
      metrics.incRemoteBytesRead(byteLen);
      return buf;
    }

    private void checkPartitionSize() throws IOException {
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

    public boolean cleanup(boolean force) {
      boolean allReleased = true;
      if (!cleaned) {
        allReleased &= cleanupSubmitted(force);
        allReleased &= cleanupConsumed(force);
      }
      return allReleased;
    }

    public void checkTotalPartitions() throws IOException {
      if (partsRead != totalParts) {
        throw new IOException("expect total partitions to be read: " + totalParts + ", actual read: " + partsRead);
      }
    }

    @Override
    public DaosReader.ReadTaskContext getCurrentCtx() {
      return (DaosReader.ReadTaskContext) currentCtx;
    }

    @Override
    public DaosReader.ReadTaskContext getHeadCtx() {
      return (DaosReader.ReadTaskContext) headCtx;
    }

    @Override
    public DaosReader.ReadTaskContext getLastCtx() {
      return (DaosReader.ReadTaskContext) lastCtx;
    }
  }

  /**
   * reader configurations, please check configs prefixed with SHUFFLE_DAOS_READ in {@link package$#MODULE$}.
   */
  private static final class ReaderConfig {
    private long minReadSize;
    private long maxBytesInFlight;
    private long maxMem;
    private int readBatchSize;
    private int waitDataTimeMs;
    private int waitTimeoutTimes;
    private boolean fromOtherThread;

    private ReaderConfig(long maxBytesInFlight, long maxMem) {
      SparkConf conf = SparkEnv.get().conf();
      minReadSize = (long)conf.get(package$.MODULE$.SHUFFLE_DAOS_READ_MINIMUM_SIZE()) * 1024;
      if (maxBytesInFlight < minReadSize) {
        this.maxBytesInFlight = minReadSize;
      } else {
        this.maxBytesInFlight = maxBytesInFlight;
      }
      this.maxMem = maxMem;
      this.readBatchSize = (int)conf.get(package$.MODULE$.SHUFFLE_DAOS_READ_BATCH_SIZE());
      this.waitDataTimeMs = (int)conf.get(package$.MODULE$.SHUFFLE_DAOS_READ_WAIT_DATA_MS());
      this.waitTimeoutTimes = (int)conf.get(package$.MODULE$.SHUFFLE_DAOS_READ_WAIT_DATA_TIMEOUT_TIMES());
      this.fromOtherThread = (boolean)conf.get(package$.MODULE$.SHUFFLE_DAOS_READ_FROM_OTHER_THREAD());
      if (log.isDebugEnabled()) {
        log.debug("minReadSize: " + minReadSize);
        log.debug("maxBytesInFlight: " + maxBytesInFlight);
        log.debug("maxMem: " + maxMem);
        log.debug("readBatchSize: " + readBatchSize);
        log.debug("waitDataTimeMs: " + waitDataTimeMs);
        log.debug("waitTimeoutTimes: " + waitTimeoutTimes);
        log.debug("fromOtherThread: " + fromOtherThread);
      }
    }
  }
}

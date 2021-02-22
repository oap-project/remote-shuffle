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

import io.daos.BufferAllocator;
import io.daos.DaosIOException;
import io.daos.obj.DaosObject;
import io.daos.obj.IODataDesc;
import io.netty.buffer.ByteBuf;
import io.netty.util.internal.ObjectPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

/**
 * A implementation of {@link DaosWriter} bases on synchronous DAOS Object API.
 *
 * For each partition, there is one corresponding {@link NativeBuffer} creates
 * {@link IODataDesc} and write to DAOS in either caller thread or other dedicated thread.
 */
public class DaosWriterSync extends TaskSubmitter implements DaosWriter {

  private DaosObject object;

  private String mapId;

  private WriteParam param;

  private WriterConfig config;

  private Map<DaosWriter, Integer> writerMap;

  private NativeBuffer[] partitionBufArray;

  private int totalTimeoutTimes;

  private int totalWriteTimes;

  private int totalBySelfTimes;

  private volatile boolean cleaned;

  private static Logger LOG = LoggerFactory.getLogger(DaosWriterSync.class);

  /**
   * construct DaosWriter with <code>object</code> and dedicated read <code>executors</code>.
   *
   * @param param
   * write parameters
   * @param object
   * opened DaosObject
   * @param executor
   * null means write in caller's thread. Submit {@link WriteTask} to it otherwise.
   */
  public DaosWriterSync(DaosObject object, DaosWriterSync.WriteParam param,
                        BoundThreadExecutors.SingleThreadExecutor executor) {
    super(executor);
    this.param = param;
    this.config = param.config;
    this.partitionBufArray = new NativeBuffer[param.numPartitions];
    this.mapId = String.valueOf(param.mapId);
    this.object = object;
  }

  private NativeBuffer getNativeBuffer(int partitionId) {
    NativeBuffer buffer = partitionBufArray[partitionId];
    if (buffer == null) {
      buffer = new NativeBuffer(partitionId, config.getBufferSize());
      partitionBufArray[partitionId] = buffer;
    }
    return buffer;
  }

  @Override
  public void write(int partitionId, int b) {
    getNativeBuffer(partitionId).write(b);
  }

  @Override
  public void write(int partitionId, byte[] array) {
    getNativeBuffer(partitionId).write(array);
  }

  @Override
  public void write(int partitionId, byte[] array, int offset, int len) {
    getNativeBuffer(partitionId).write(array, offset, len);
  }

  @Override
  public long[] getPartitionLens(int numPartitions) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("partition map size: " + partitionBufArray.length);
      for (int i = 0; i < numPartitions; i++) {
        NativeBuffer nb = partitionBufArray[i];
        if (nb != null) {
          LOG.debug("id: " + i + ", native buffer: " + nb.partitionId + ", " +
                  nb.totalSize + ", " + nb.roundSize);
        }
      }
    }
    long[] lens = new long[numPartitions];
    for (int i = 0; i < numPartitions; i++) {
      NativeBuffer nb = partitionBufArray[i];
      if (nb != null) {
        lens[i] = nb.totalSize;
        if (nb.roundSize != 0 || !nb.bufList.isEmpty()) {
          throw new IllegalStateException("round size should be 0, " + nb.roundSize + ", buflist should be empty, " +
                  nb.bufList.size());
        }
      } else {
        lens[i] = 0;
      }
    }
    return lens;
  }

  @Override
  public void flush(int partitionId) throws IOException {
    NativeBuffer buffer = partitionBufArray[partitionId];
    if (buffer == null) {
      return;
    }
    IODataDesc desc = buffer.createUpdateDesc();
    if (desc == null) {
      return;
    }
    totalWriteTimes++;
    if (config.isWarnSmallWrite() && buffer.roundSize < config.getMinSize()) {
      LOG.warn("too small partition size {}, shuffle {}, map {}, partition {}",
          buffer.roundSize, param.shuffleId, mapId, partitionId);
    }
    if (executor == null) { // run write by self
      runBySelf(desc, buffer);
      return;
    }
    submitToOtherThreads(desc, buffer);
  }

  private void runBySelf(IODataDesc desc, NativeBuffer buffer) throws IOException {
    totalBySelfTimes++;
    try {
      object.update(desc);
    } catch (IOException e) {
      throw new IOException("failed to write partition of " + desc, e);
    } finally {
      desc.release();
      buffer.reset(true);
    }
  }

  private void submitToOtherThreads(IODataDesc desc, NativeBuffer buffer) throws IOException {
    // move forward to release write buffers
    moveForward();
    // check if we need to wait submitted tasks to be executed
    if (goodForSubmit()) {
      submitAndReset(desc, buffer);
      return;
    }
    // to wait
    int timeoutTimes = 0;
    try {
      while (!goodForSubmit()) {
        boolean timeout = waitForCondition(config.getWaitTimeMs());
        moveForward();
        if (timeout) {
          timeoutTimes++;
          if (LOG.isDebugEnabled()) {
            LOG.debug("wait daos write timeout times: " + timeoutTimes);
          }
          if (timeoutTimes >= config.getTimeoutTimes()) {
            totalTimeoutTimes += timeoutTimes;
            runBySelf(desc, buffer);
            return;
          }
        }
      }
    } catch (InterruptedException e) {
      desc.release();
      Thread.currentThread().interrupt();
      throw new IOException("interrupted when wait daos write", e);
    }
    // submit write task after some wait
    totalTimeoutTimes += timeoutTimes;
    submitAndReset(desc, buffer);
  }

  private boolean goodForSubmit() {
    return totalInMemSize < config.getTotalInMemSize() && totalSubmitted < config.getTotalSubmittedLimit();
  }

  private void submitAndReset(IODataDesc desc, NativeBuffer buffer) {
    try {
      submit(desc, buffer.bufList);
    } finally {
      buffer.reset(false);
    }
  }

  private void cleanup(boolean force) {
    if (cleaned) {
      return;
    }
    boolean allReleased = true;
    allReleased &= cleanupSubmitted(force);
    allReleased &= cleanupConsumed(force);
    if (allReleased) {
      cleaned = true;
    }
  }

  /**
   * wait write task to be completed and clean up resources.
   */
  @Override
  public void close() {
    try {
      close(true);
    } catch (Exception e) {
      throw new IllegalStateException("failed to complete all write tasks and cleanup", e);
    }
  }

  private void close(boolean force) throws Exception {
    if (partitionBufArray != null) {
      waitCompletion(force);
      partitionBufArray = null;
      object = null;
      if (LOG.isDebugEnabled()) {
        LOG.debug("total writes: " + totalWriteTimes + ", total timeout times: " + totalTimeoutTimes +
            ", total write-by-self times: " + totalBySelfTimes + ", total timeout times/total writes: " +
            ((float) totalTimeoutTimes) / totalWriteTimes);
      }
    }
    cleanup(force);
    if (writerMap != null && (force || cleaned)) {
      writerMap.remove(this);
      writerMap = null;
    }
  }

  private void waitCompletion(boolean force) throws Exception {
    if (!force) {
      return;
    }
    try {
      while (totalSubmitted > 0) {
        waitForCondition(config.getWaitTimeMs());
        moveForward();
      }
    } catch (Exception e) {
      LOG.error("failed to wait completion of daos writing", e);
      throw e;
    }
  }

  public void setWriterMap(Map<DaosWriter, Integer> writerMap) {
    writerMap.put(this, 0);
    this.writerMap = writerMap;
  }

  @Override
  protected Runnable newTask(LinkedTaskContext context) {
    return WriteTask.newInstance((WriteTaskContext) context);
  }

  @Override
  protected LinkedTaskContext createTaskContext(IODataDesc desc, Object morePara) {
    return new WriteTaskContext(object, counter, lock, condition, desc, morePara);
  }

  @Override
  protected boolean validateReturned(LinkedTaskContext context) throws IOException {
    if (!context.desc.isSucceeded()) {
      throw new DaosIOException("write is not succeeded: " + context.desc);
    }
    return false;
  }

  @Override
  protected boolean consumed(LinkedTaskContext context) {
    // release write buffers
    @SuppressWarnings("unchecked")
    List<ByteBuf> bufList = (List<ByteBuf>) context.morePara;
    bufList.forEach(b -> b.release());
    bufList.clear();
    return true;
  }

  /**
   * Write data to one or multiple netty direct buffers which will be written to DAOS without copy
   */
  private class NativeBuffer implements Comparable<NativeBuffer> {
    private int partitionId;
    private String partitionIdKey;
    private int bufferSize;
    private int idx = -1;
    private List<ByteBuf> bufList = new ArrayList<>();
    private long totalSize;
    private long roundSize;

    NativeBuffer(int partitionId, int bufferSize) {
      this.partitionId = partitionId;
      this.partitionIdKey = String.valueOf(partitionId);
      this.bufferSize = bufferSize;
    }

    private ByteBuf addNewByteBuf(int len) {
      ByteBuf buf;
      try {
        buf = BufferAllocator.objBufWithNativeOrder(Math.max(bufferSize, len));
      } catch (OutOfMemoryError e) {
        LOG.error("too big buffer size: " + Math.max(bufferSize, len));
        throw e;
      }
      bufList.add(buf);
      idx++;
      return buf;
    }

    private ByteBuf getBuffer(int len) {
      if (idx < 0) {
        return addNewByteBuf(len);
      }
      return bufList.get(idx);
    }

    public void write(int b) {
      ByteBuf buf = getBuffer(1);
      if (buf.writableBytes() < 1) {
        buf = addNewByteBuf(1);
      }
      buf.writeByte(b);
      roundSize += 1;
    }

    public void write(byte[] b) {
      write(b, 0, b.length);
    }

    public void write(byte[] b, int offset, int len) {
      if (len <= 0) {
        return;
      }
      ByteBuf buf = getBuffer(len);
      int avail = buf.writableBytes();
      int gap = len - avail;
      if (gap <= 0) {
        buf.writeBytes(b, offset, len);
      } else {
        buf.writeBytes(b, offset, avail);
        buf = addNewByteBuf(gap);
        buf.writeBytes(b, avail, gap);
      }
      roundSize += len;
    }

    public IODataDesc createUpdateDesc() throws IOException {
      if (roundSize == 0 || bufList.isEmpty()) {
        return null;
      }
      long bufSize = 0;
      IODataDesc desc = object.createDataDescForUpdate(partitionIdKey, IODataDesc.IodType.ARRAY, 1);
      for (ByteBuf buf : bufList) {
        desc.addEntryForUpdate(mapId, (int) totalSize, buf);
        bufSize += buf.readableBytes();
      }
      if (roundSize != bufSize) {
        throw new IOException("expect update size: " + roundSize + ", actual: " + bufSize);
      }
      return desc;
    }

    public void reset(boolean release) {
      if (release) {
        bufList.forEach(b -> b.release());
      }
      // release==false, buffers will be released when tasks are executed and consumed
      bufList.clear();
      idx = -1;
      totalSize += roundSize;
      roundSize = 0;
    }

    @Override
    public int compareTo(NativeBuffer nativeBuffer) {
      return partitionId - nativeBuffer.partitionId;
    }
  }

  public static class WriteParam {
    private int numPartitions;
    private int shuffleId;
    private long mapId;
    private DaosWriter.WriterConfig config;

    public WriteParam numPartitions(int numPartitions) {
      this.numPartitions = numPartitions;
      return this;
    }

    public WriteParam shuffleId(int shuffleId) {
      this.shuffleId = shuffleId;
      return this;
    }

    public WriteParam mapId(long mapId) {
      this.mapId = mapId;
      return this;
    }

    public WriteParam config(DaosWriter.WriterConfig config) {
      this.config = config;
      return this;
    }
  }

  /**
   * Task to write data to DAOS. Task itself is cached to reduce GC time.
   * To reuse task for different writes, prepare and reset {@link WriteTaskContext} by calling
   * {@link #newInstance(WriteTaskContext)}
   */
  static final class WriteTask implements Runnable {
    private final ObjectPool.Handle<WriteTask> handle;
    private WriteTaskContext context;

    private static final ObjectPool<WriteTask> objectPool = ObjectPool.newPool(handle -> new WriteTask(handle));

    private static final Logger log = LoggerFactory.getLogger(WriteTask.class);

    static WriteTask newInstance(WriteTaskContext context) {
      WriteTask task = objectPool.get();
      task.context = context;
      return task;
    }

    private WriteTask(ObjectPool.Handle<WriteTask> handle) {
      this.handle = handle;
    }

    @Override
    public void run() {
      boolean cancelled = context.cancelled;
      try {
        if (!cancelled) {
          context.object.update(context.desc);
        }
      } catch (Exception e) {
        log.error("failed to write for " + context.desc, e);
      } finally {
        context.desc.release();
        context.signal();
        context = null;
        handle.recycle(this);
      }
    }
  }

  /**
   * Context for write task. It holds all other object to read and sync between caller thread and write thread.
   * It should be cached in caller thread for reusing.
   */
  static final class WriteTaskContext extends LinkedTaskContext {

    /**
     * constructor with all parameters. Some of them can be reused later.
     *
     * @param object
     * DAOS object to fetch data from DAOS
     * @param counter
     * counter to indicate how many write is on-going
     * @param writeLock
     * lock to work with <code>notFull</code> condition to signal caller thread to submit more write task
     * @param notFull
     * condition to signal caller thread
     * @param desc
     * desc object to describe where to write data
     * @param bufList
     * list of buffers to write to DAOS
     */
    WriteTaskContext(DaosObject object, AtomicInteger counter, Lock writeLock, Condition notFull,
                            IODataDesc desc, Object bufList) {
      super(object, counter, writeLock, notFull);
      this.desc = desc;
      @SuppressWarnings("unchecked")
      List<ByteBuf> myBufList = new ArrayList<>();
      myBufList.addAll((List<ByteBuf>) bufList);
      this.morePara = myBufList;
    }

    @Override
    public WriteTaskContext getNext() {
      @SuppressWarnings("unchecked")
      WriteTaskContext ctx = (WriteTaskContext) next;
      return ctx;
    }

    @Override
    public void reuse(IODataDesc desc, Object morePara) {
      @SuppressWarnings("unchecked")
      List<ByteBuf> myBufList = (List<ByteBuf>) this.morePara;
      if (!myBufList.isEmpty()) {
        throw new IllegalStateException("bufList in reusing write task context should be empty");
      }
      myBufList.addAll((List<ByteBuf>) morePara);
      super.reuse(desc, myBufList);
    }
  }

  /**
   * Thread factory for write
   */
  protected static class WriteThreadFactory implements ThreadFactory {
    private AtomicInteger id = new AtomicInteger(0);

    @Override
    public Thread newThread(Runnable runnable) {
      Thread t;
      String name = "daos_write_" + id.getAndIncrement();
      if (runnable == null) {
        t = new Thread(name);
      } else {
        t = new Thread(runnable, name);
      }
      t.setDaemon(true);
      t.setUncaughtExceptionHandler((thread, throwable) ->
          LOG.error("exception occurred in thread " + name, throwable));
      return t;
    }
  }
}

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

import io.daos.DaosIOException;
import io.daos.obj.DaosObject;
import io.daos.obj.IODataDesc;
import io.daos.obj.IODataDescSync;
import io.netty.util.internal.ObjectPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
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

  protected DaosObject object;

  protected WriterConfig config;

  protected Map<DaosWriter, Integer> writerMap;

  private InnerWriter iw;

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
  public DaosWriterSync(DaosObject object, DaosWriter.WriteParam param,
                        BoundThreadExecutors.SingleThreadExecutor executor) {
    super(executor);
    this.object = object;
    this.config = param.getConfig();
    this.iw = new InnerWriter(object, param);
  }

  @Override
  public void write(int partitionId, int b) {
    iw.write(partitionId, b);
  }

  @Override
  public void write(int partitionId, byte[] array) {
    iw.write(partitionId, array);
  }

  @Override
  public void write(int partitionId, byte[] array, int offset, int len) {
    iw.write(partitionId, array, offset, len);
  }

  @Override
  public long[] getPartitionLens(int numPartitions) {
    return iw.getPartitionLens(numPartitions);
  }

  @Override
  public void enableSpill() {
    iw.enableSpill();
  }

  @Override
  public void startMerging() {
    iw.startMerging();
  }

  @Override
  public void incrementSeq(int partitionId) {
    iw.incrementSeq(partitionId);
  }

  @Override
  public boolean isSpilled(int partitionId) {
    return iw.isSpilled(partitionId);
  }

  @Override
  public List<SpillInfo> getSpillInfo(int partitionId) {
    return iw.getSpillInfo(partitionId);
  }

  @Override
  public void flush(int partitionId) throws IOException {
    iw.flush(partitionId);
  }

  @Override
  public void flushAll(int partitionId) throws IOException {
    iw.flushAll();
  }

  @Override
  public void flushAll() throws IOException {
    iw.flushAll();
  }

  private void runBySelf(IODataDescSync desc) throws IOException {
    totalBySelfTimes++;
    try {
      object.update(desc);
    } catch (IOException e) {
      throw new IOException("failed to write partition of " + desc, e);
    } finally {
      desc.release();
    }
  }

  private void submitToOtherThreads(IODataDescSync desc) throws IOException {
    // move forward to release write buffers
    moveForward();
    // check if we need to wait submitted tasks to be executed
    if (goodForSubmit()) {
      submit(desc, null);
      return;
    }
    // to wait
    try {
      while (!goodForSubmit()) {
        boolean timeout = waitForCondition(config.getWaitTimeMs());
        moveForward();
        if (timeout) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("wait daos write timed out after " + config.getWaitTimeMs());
          }
          totalTimeoutTimes++;
          runBySelf(desc);
          return;
        }
      }
    } catch (InterruptedException e) {
      desc.release();
      Thread.currentThread().interrupt();
      throw new IOException("interrupted when wait daos write", e);
    }
    // submit task after some wait
    submit(desc, null);
  }

  private boolean goodForSubmit() {
    return totalInMemSize < config.getTotalInMemSize() && totalSubmitted < config.getTotalSubmittedLimit();
  }

  private void cleanup() {
    if (cleaned) {
      return;
    }
    boolean allReleased = true;
    allReleased &= cleanupSubmitted(true);
    allReleased &= cleanupConsumed(true);
    if (allReleased) {
      cleaned = true;
    }
  }

  /**
   * wait write task to be completed and clean up resources.
   */
  @Override
  public void close() {
    iw.close();
    if (writerMap != null && cleaned) {
      writerMap.remove(this);
      writerMap = null;
    }
  }

  private void waitCompletion() throws Exception {
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
  protected LinkedTaskContext createTaskContext(IODataDescSync desc, Object morePara) {
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
    context.desc.release();
    return true;
  }

  private final class InnerWriter extends DaosWriterBase {
    InnerWriter(DaosObject object, DaosWriter.WriteParam param) {
      super(object, param);
    }

    @Override
    public void flush(int partitionId) throws IOException {
      NativeBuffer buffer = partitionBufArray[partitionId];
      if (buffer == null) {
        return;
      }
      List<IODataDescSync> descList = buffer.createUpdateDescs();
      flush(buffer, descList);
    }

    @Override
    public void flushAll(int partitionId) throws IOException {
      NativeBuffer buffer = partitionBufArray[partitionId];
      if (buffer == null) {
        return;
      }
      List<IODataDescSync> descList = buffer.createUpdateDescs(false);
      flush(buffer, descList);
    }

    private void flush(NativeBuffer buffer, List<IODataDescSync> descList) throws IOException {
      for (IODataDescSync desc : descList) {
        totalWriteTimes++;
        if (config.isWarnSmallWrite() && buffer.getSubmittedSize() < config.getMinSize()) {
          LOG.warn("too small partition size {}, shuffle {}, map {}, partition {}",
              buffer.getSubmittedSize(), param.getShuffleId(), mapId, buffer.getPartitionIdKey());
        }
        if (executor == null) { // run write by self
          runBySelf(desc);
          continue;
        }
        submitToOtherThreads(desc);
      }
      buffer.reset(false);
    }

    @Override
    protected void waitCompletion() throws IOException {
      try {
        DaosWriterSync.this.waitCompletion();
      } catch (Exception e) {
        throw new IllegalStateException("failed to complete all write tasks and cleanup", e);
      }
    }

    @Override
    public void flushAll() throws IOException {
      for (int i = 0; i < partitionBufArray.length; i++) {
        NativeBuffer buffer = partitionBufArray[i];
        if (buffer == null) {
          continue;
        }
        List<IODataDescSync> descList = buffer.createUpdateDescs(false);
        flush(buffer, descList);
      }
      waitCompletion();
    }

    @Override
    public void close() {
      try {
        if (partitionBufArray != null) {
          waitCompletion();
          partitionBufArray = null;
          object = null;
          if (LOG.isDebugEnabled()) {
            LOG.debug("total writes: " + totalWriteTimes + ", total timeout times: " + totalTimeoutTimes +
                ", total write-by-self times: " + totalBySelfTimes + ", total timeout times/total writes: " +
                ((float) totalTimeoutTimes) / totalWriteTimes);
          }
        }
        cleanup();
        super.close();
      } catch (Exception e) {
        throw new IllegalStateException("failed to complete all write tasks and cleanup", e);
      }
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
     * @param morePara
     * list of buffers to write to DAOS
     */
    WriteTaskContext(DaosObject object, AtomicInteger counter, Lock writeLock, Condition notFull,
                            IODataDescSync desc, Object morePara) {
      super(object, counter, writeLock, notFull);
      this.desc = desc;
      this.morePara = morePara;
    }

    @Override
    public WriteTaskContext getNext() {
      @SuppressWarnings("unchecked")
      WriteTaskContext ctx = (WriteTaskContext) next;
      return ctx;
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

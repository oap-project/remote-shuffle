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
import io.netty.util.internal.ObjectPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

/**
 * A class with {@link DaosObject} wrapped to read data from DAOS in either caller's thread or
 * dedicated executor thread. The actual read is performed by {@link DaosObject#fetch(IODataDesc)}.
 */
public class DaosReader {

  private DaosObject object;

  private Map<DaosShuffleInputStream.BufferSource, Integer> bufferSourceMap = new ConcurrentHashMap<>();

  private BoundThreadExecutors executors;

  private Map<DaosReader, Integer> readerMap;

  private static Logger logger = LoggerFactory.getLogger(DaosReader.class);

  /**
   * construct DaosReader with <code>object</code> and dedicated read <code>executors</code>.
   *
   * @param object
   * opened DaosObject
   * @param executors
   * null means read in caller's thread. Submit {@link ReadTask} to dedicate executor retrieved by
   * {@link #nextReaderExecutor()} otherwise.
   */
  public DaosReader(DaosObject object, BoundThreadExecutors executors) {
    this.object = object;
    this.executors = executors;
  }

  public DaosObject getObject() {
    return object;
  }

  public boolean hasExecutors() {
    return executors != null;
  }

  /**
   * next executor. null if there is no executors being set.
   *
   * @return shareable executor instance. null means no executor set.
   */
  public BoundThreadExecutors.SingleThreadExecutor nextReaderExecutor() {
    if (executors != null) {
      return executors.nextExecutor();
    }
    return null;
  }

  /**
   * release resources of all {@link org.apache.spark.shuffle.daos.DaosShuffleInputStream.BufferSource}
   * bound with this reader.
   */
  public void close() {
    // force releasing
    bufferSourceMap.forEach((k, v) -> k.cleanup(true));
    bufferSourceMap.clear();
    if (readerMap != null) {
      readerMap.remove(this);
      readerMap = null;
    }
  }

  @Override
  public String toString() {
    return "DaosReader{" +
        "object=" + object +
        '}';
  }

  /**
   * register buffer source for resource cleanup.
   *
   * @param source
   * BufferSource instance
   */
  public void register(DaosShuffleInputStream.BufferSource source) {
    bufferSourceMap.put(source, 1);
  }

  /**
   * unregister buffer source if <code>source</code> is release already.
   *
   * @param source
   * BufferSource instance
   */
  public void unregister(DaosShuffleInputStream.BufferSource source) {
    bufferSourceMap.remove(source);
  }

  /**
   * set global <code>readMap</code> and hook this reader for releasing resources.
   *
   * @param readerMap
   * global reader map
   */
  public void setReaderMap(Map<DaosReader, Integer> readerMap) {
    readerMap.put(this, 0);
    this.readerMap = readerMap;
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

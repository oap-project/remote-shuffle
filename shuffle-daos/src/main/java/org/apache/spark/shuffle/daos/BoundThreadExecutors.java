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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Pool of simple shared executors.
 * User should call {@link #nextExecutor()} to get next sharable executor which is a instance
 * of {@link SingleThreadExecutor}.
 *
 * Before JVM exiting, {@link #stop()} should be called to release all executors.
 */
public class BoundThreadExecutors {

  private final int threads;

  private final String name;

  private final SingleThreadExecutor[] executors;

  private AtomicInteger idx = new AtomicInteger(0);

  private ThreadFactory threadFactory;

  private volatile boolean stopped;

  public static final int NOT_STARTED = 0;
  public static final int STARTING = 1;
  public static final int STARTED = 2;
  public static final int STOPPING = 3;
  public static final int STOPPED = 4;

  private static final Logger logger = LoggerFactory.getLogger(BoundThreadExecutors.class);

  public BoundThreadExecutors(String name, int threads, ThreadFactory threadFactory) {
    this.name = name;
    this.threads = threads;
    this.threadFactory = threadFactory;
    this.executors = new SingleThreadExecutor[threads];
  }

  /**
   * get next executor in a round-robin way.
   * User should submit all its tasks to the returned executor instead of getting more executor.
   *
   * @return instance of {@link SingleThreadExecutor}
   */
  public SingleThreadExecutor nextExecutor() {
    int i = idx.getAndIncrement()%threads;
    SingleThreadExecutor executor = executors[i];
    if (executor == null) {
      synchronized (this) {
        executor = new SingleThreadExecutor(threadFactory);
        executors[i] = executor;
      }
    }
    executor.startThread();
    return executor;
  }

  /**
   * interrupt all running tasks and stop all executors.
   */
  public void stop() {
    if (stopped) {
      return;
    }
    for (SingleThreadExecutor executor : executors) {
      if (executor != null) {
        executor.interrupt();
      }
    }
    boolean allStopped;
    boolean timeout = false;
    int count = 0;
    while (true) {
      allStopped = true;
      for (SingleThreadExecutor executor : executors) {
        if (executor != null) {
          allStopped &= (executor.state.get() == STOPPED);
        }
      }
      if (allStopped) {
        break;
      }
      if (count >= 5) {
        timeout = true;
        break;
      }
      try {
        Thread.sleep(200);
      } catch (InterruptedException e) {
        logger.warn("interrupted when waiting for all executors stopping, " + name, e);
        timeout = true;
        break;
      }
      count++;
    }
    for (int i = 0; i < executors.length; i++) {
      executors[i] = null;
    }
    stopped = true;
    logger.info("BoundThreadExecutors stopped" + (timeout ? " with some threads still running." : "."));
  }

  /**
   * An executor backed by single thread
   */
  public static class SingleThreadExecutor implements Executor {
    private Thread thread;
    private String name;
    private AtomicInteger state = new AtomicInteger(0);
    private BlockingQueue<Runnable> queue = new LinkedBlockingQueue<>();
    // TODO: handle task failure and restart of thread
    private Runnable parentTask = () -> {
      Runnable runnable;
      try {
        while (!Thread.currentThread().isInterrupted()) {
          runnable = queue.take();
          try {
            runnable.run();
          } catch (Exception e) {
            logger.info("failed to run " + runnable, e);
          }
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        if (logger.isDebugEnabled()) {
          logger.debug("thread interrupted. " + getName());
        }
      } finally {
        state.set(STOPPED);
      }
    };

    public SingleThreadExecutor(ThreadFactory threadFactory) {
      this.thread = threadFactory.newThread(parentTask);
      name = thread.getName();
    }

    private String getName() {
      return name;
    }

    public void interrupt() {
      thread.interrupt();
      state.set(STOPPING);
      thread = null;
      queue.clear();
      queue = null;
    }

    @Override
    public void execute(Runnable runnable) {
      try {
        queue.put(runnable);
      } catch (InterruptedException e) {
        throw new RuntimeException("cannot add task to thread " + thread.getName(), e);
      }
    }

    private void startThread() {
      if (state.get() == NOT_STARTED) {
        if (state.compareAndSet(NOT_STARTED, STARTING)) {
          try {
            thread.start();
          } finally {
            if (!state.compareAndSet(STARTING, STARTED)) {
              throw new IllegalStateException("failed to start thread " + thread.getName());
            }
          }
        }
      }
    }
  }

}

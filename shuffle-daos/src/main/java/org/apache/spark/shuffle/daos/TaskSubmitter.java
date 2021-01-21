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

import io.daos.obj.IODataDesc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Deque;
import java.util.LinkedList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Parent class to submit task to {@link org.apache.spark.shuffle.daos.BoundThreadExecutors.SingleThreadExecutor}.
 */
public abstract class TaskSubmitter {

  protected final BoundThreadExecutors.SingleThreadExecutor executor;

  protected LinkedTaskContext headCtx;
  protected LinkedTaskContext currentCtx;
  protected LinkedTaskContext lastCtx;
  protected Deque<LinkedTaskContext> consumedStack = new LinkedList<>();

  protected Lock lock = new ReentrantLock();
  protected Condition condition = lock.newCondition();
  protected AtomicInteger counter = new AtomicInteger(0);

  protected int exceedWaitTimes;
  protected long totalInMemSize;
  protected int totalSubmitted;

  private static final Logger log = LoggerFactory.getLogger(TaskSubmitter.class);

  protected TaskSubmitter(BoundThreadExecutors.SingleThreadExecutor executor) {
    this.executor = executor;
  }

  /**
   * submit read task with <code>taskDesc</code> and <code>morePara</code>.
   *
   * @param taskDesc
   * IO description object
   * @param morePara
   * additional parameter
   */
  protected void submit(IODataDesc taskDesc, Object morePara) {
    LinkedTaskContext context = tryReuseContext(taskDesc, morePara);
    executor.execute(newTask(context));
    totalInMemSize += taskDesc.getTotalRequestSize();
    totalSubmitted++;
  }

  protected abstract Runnable newTask(LinkedTaskContext context);

  /**
   * wait for condition to be met.
   *
   * @param waitDataTimeMs
   * @return true for timeout, false otherwise.
   * @throws {@link InterruptedException}
   */
  protected boolean waitForCondition(long waitDataTimeMs) throws InterruptedException {
    lock.lockInterruptibly();
    try {
      if (!condition.await(waitDataTimeMs, TimeUnit.MILLISECONDS)) {
        return true;
      }
      return false;
    } finally {
      lock.unlock();
    }
  }

  /**
   * reuse task context in caller thread.
   *
   * @param desc
   * desc object to override old desc in reused task context
   * @param morePara
   * additional parameter to override old desc in reused task context
   * @return reused linked task context
   */
  protected LinkedTaskContext tryReuseContext(IODataDesc desc, Object morePara) {
    LinkedTaskContext context = consumedStack.poll();
    if (context != null) {
      context.reuse(desc, morePara);
    } else {
      context = createTaskContext(desc, morePara);
    }
    if (lastCtx != null) {
      lastCtx.setNext(context);
    }
    lastCtx = context;
    if (headCtx == null) {
      headCtx = context;
    }
    return context;
  }

  /**
   * create new task context if there is no for reusing.
   *
   * @param desc
   * desc object
   * @param morePara
   * additional parameter
   * @return Linked task context
   */
  protected abstract LinkedTaskContext createTaskContext(IODataDesc desc, Object morePara);

  /**
   * move forward by checking if there is any returned task available.
   * consume and validate each of them.
   *
   * @return true if we consume some task and {@link #consume()} returns true. false otherwise.
   * @throws IOException
   */
  protected boolean moveForward() throws IOException {
    int c;
    while ((c = counter.decrementAndGet()) >= 0) {
      if (consume()) {
        return true;
      }
    }
    if (c < 0) {
      if (log.isDebugEnabled()) {
        log.debug("spurious wakeup");
      }
      counter.incrementAndGet();
    }
    return false;
  }

  /**
   * consume returned task.
   *
   * @return true to stop consuming more returned task. false to continue.
   * @throws IOException
   */
  private boolean consume() throws IOException {
    if (currentCtx != null) {
      totalInMemSize -= currentCtx.getDesc().getTotalRequestSize();
      if (consumed(currentCtx)) { // check if consumed context can be reused
        consumedStack.offer(currentCtx);
      }
      currentCtx = currentCtx.getNext();
    } else { // returned first time
      currentCtx = headCtx;
    }
    totalSubmitted -= 1;
    return validateReturned(currentCtx);
  }

  /**
   * validate if returned task is ok.
   *
   * @param context
   * task context
   * @return true to stop consuming more returned task. false to continue.
   * @throws IOException
   */
  protected abstract boolean validateReturned(LinkedTaskContext context) throws IOException;

  /**
   * post action after task data being consumed.
   *
   * @param context
   * task context
   * @return true for reusing consumed task context. false means it cannot be reused.
   */
  protected abstract boolean consumed(LinkedTaskContext context);

  /**
   * cancel tasks.
   *
   * @param cancelAll
   * true to cancel all tasks. false to cancel just one task.
   */
  protected void cancelTasks(boolean cancelAll) {
    LinkedTaskContext ctx = getNextNonReturnedCtx();
    if (ctx == null) { // reach to end
      return;
    }
    ctx.cancel();
    if (cancelAll) {
      ctx = ctx.getNext();
      while (ctx != null) {
        ctx.cancel();
        ctx = ctx.getNext();
      }
    }
  }

  /**
   * get next task context sequentially without waiting signal from other threads.
   *
   * @return task context
   */
  protected LinkedTaskContext getNextNonReturnedCtx() {
    LinkedTaskContext ctx = currentCtx;
    if (ctx == null) {
      return getHeadCtx();
    }
    return ctx.getNext();
  }

  /**
   * cancel and clean up all submitted task contexts.
   *
   * @param force
   * force cleanup?
   * @return true if all is cleaned up. false otherwise.
   */
  protected boolean cleanupSubmitted(boolean force) {
    boolean allReleased = true;
    LinkedTaskContext ctx = currentCtx;
    while (ctx != null) {
      if (!ctx.isCancelled()) {
        ctx.cancel();
      }
      allReleased &= cleanupTaskContext(ctx, force);
      ctx = ctx.getNext();
    }
    return allReleased;
  }

  /**
   * clean up all linked consumed task contexts.
   *
   * @param force
   * force cleanup?
   * @return true if all is cleaned up. false otherwise.
   */
  protected boolean cleanupConsumed(boolean force) {
    boolean allReleased = true;
    for (LinkedTaskContext ctx : consumedStack) {
      allReleased &= cleanupTaskContext(ctx, force);
    }
    if (allReleased) {
      consumedStack.clear();
    }
    return allReleased;
  }

  /**
   * clean up task context.
   *
   * @param ctx
   * task context
   * @param force
   * force close?
   * @return true for successful cleanup, false otherwise
   */
  protected boolean cleanupTaskContext(LinkedTaskContext ctx, boolean force) {
    if (ctx == null) {
      return true;
    }
    IODataDesc desc = ctx.getDesc();
    if (desc != null && (force || desc.isSucceeded())) {
      desc.release();
      return true;
    }
    return false;
  }

  public LinkedTaskContext getCurrentCtx() {
    return currentCtx;
  }

  public LinkedTaskContext getHeadCtx() {
    return headCtx;
  }

  public LinkedTaskContext getLastCtx() {
    return lastCtx;
  }
}

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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

/**
 * Linked reusable task context for read and write.
 * Task context acts like a interface between caller thread and task executor thread.
 * It wraps task parameters, synchronization blocks and links.
 */
public abstract class LinkedTaskContext {

  protected final DaosObject object;
  protected final AtomicInteger counter;
  protected final Lock lock;
  protected final Condition condition;

  protected IODataDesc desc;
  protected LinkedTaskContext next;

  protected volatile boolean cancelled; // for multi-thread
  protected boolean cancelledByCaller; // for accessing by caller

  protected Object morePara;

  private static final Logger logger = LoggerFactory.getLogger(LinkedTaskContext.class);

  /**
   * constructor with parameters can be reused for all tasks.
   *
   * @param object
   * daos object for either read or write
   * @param counter
   * counter to indicate how many data is ready to be consumed or produced
   * @param lock
   * lock to work with <code>condition</code> to signal caller thread there is data ready
   * @param condition
   * condition to signal caller thread
   */
  protected LinkedTaskContext(DaosObject object, AtomicInteger counter, Lock lock, Condition condition) {
    this.object = object;
    this.counter = counter;
    this.lock = lock;
    this.condition = condition;
  }

  /**
   * reuse this context by setting task specific data and resetting some status.
   *
   * @param desc
   * data description
   * @param morePara
   * additional data
   */
  protected void reuse(IODataDesc desc, Object morePara) {
    this.desc = desc;
    this.next = null;
    this.morePara = morePara;
    cancelled = false;
    cancelledByCaller = false;
  }

  /**
   * link task context.
   *
   * @param next
   */
  protected void setNext(LinkedTaskContext next) {
    this.next = next;
  }

  protected LinkedTaskContext getNext() {
    return next;
  }

  protected IODataDesc getDesc() {
    return desc;
  }

  /**
   * signal caller thread on condition
   */
  protected void signal() {
    counter.getAndIncrement();
    try {
      lock.lockInterruptibly();
      try {
        condition.signal();
      } finally {
        lock.unlock();
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      logger.error("interrupted when signal task completion for " + desc, e);
    }
  }

  /**
   * cancel task
   */
  public void cancel() {
    cancelled = true;
    cancelledByCaller = true;
  }

  /**
   * check if task is cancelled. It's for caller thread to avoid volatile access.
   *
   * @return true for cancelled. false otherwise.
   */
  public boolean isCancelled() {
    return cancelledByCaller;
  }
}

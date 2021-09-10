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

package org.apache.spark.shuffle.daos

import scala.collection.mutable

import org.apache.spark.util.SizeEstimator

/**
 * A trait to sample size of object. It mimics what {@link SizeTracker} does. The differences between them are,
 * - this trait is for sampling size of each partition buffer.
 * - this trait lets caller control when to sample size.
 * - bytesPerUpdate is calculated and shared among all buffers.
 */
private[spark] trait SizeSampler {

  import SizeSampler._

  /** Samples taken since last resetSamples(). Only the last two are kept for extrapolation. */
  private val samples = new mutable.Queue[Sample]

  /** Total number of insertions and updates into the map since the last resetSamples(). */
  private var numUpdates: Long = _

  private var stat: SampleStat = _

  private var buffer: Boolean = _

  protected var curSize = 0

  protected def setSampleStat(stat: SampleStat, buffer: Boolean): Unit = {
    this.stat = stat
    this.buffer = buffer
  }

  /**
   * Reset samples collected so far.
   * This should be called after the collection undergoes a dramatic change in size.
   * After growing size, it's only called by buffer, not map.
   */
  protected def resetSamples(): Unit = {
    numUpdates = 1
    samples.clear()
    var inced = false
    if (stat.numUpdates == 0) {
      stat.numUpdates = 1
      inced = true
    }
    takeSample
    if (inced) {
      stat.numUpdates = 0
    }
  }

  protected def afterUpdate(): Unit = {
    numUpdates += 1
    curSize += 1
    stat.incNumUpdates
    if (stat.needSample) {
      takeSample
    }
  }

  /**
   * @return number of records consumed
   */
  def numOfRecords: Int = curSize

  /**
   * @return number of elements in map or buffer
   */
  def size: Int

  /**
   * Take a new sample of the current collection's size.
   */
  protected def takeSample: Unit = {
    samples.enqueue(Sample(SizeEstimator.estimate(this), numUpdates))
    // Only use the last two samples to extrapolate
    if (samples.size > 2) {
      samples.dequeue()
    }
    val bytesDelta = samples.toList.reverse match {
      case latest :: previous :: _ =>
        val updateDelta = latest.numUpdates - previous.numUpdates
        if (buffer) (latest.size - previous.size).toDouble / updateDelta
        else latest.size / latest.numUpdates // possible case for map-combine
      // If fewer than 2 samples, assume no change
      case _ => 0
    }
    stat.updateStat(bytesDelta)
  }

  /**
   * Estimate the current size of the collection in bytes. O(1) time.
   */
  def estimateSize(): Long = {
    assert(samples.nonEmpty)
    val nbr = numUpdates - samples.last.numUpdates
    val extrapolatedDelta = stat.bytesPerUpdate * nbr
    (samples.last.size + extrapolatedDelta).toLong
  }
}

private[spark] class SampleStat {
  /**
   * Controls the base of the exponential which governs the rate of sampling.
   * E.g., a value of 2 would mean we sample at 1, 2, 4, 8, ... elements.
   */
  private val SAMPLE_GROWTH_RATE = 1.1

  private[daos] var numUpdates: Long = 0
  private[daos] var lastNumUpdates: Long = 0
  private[daos] var nextSampleNum: Long = 1
  private[daos] var bytesPerUpdate: Double = 0

  def updateStat(partBpu: Double): Unit = {
    bytesPerUpdate = partBpu
    lastNumUpdates = numUpdates
    nextSampleNum = math.ceil(numUpdates * SAMPLE_GROWTH_RATE).toLong
  }

  def needSample: Boolean = {
    numUpdates == nextSampleNum
  }

  def incNumUpdates: Unit = {
    numUpdates += 1
  }
}

private object SizeSampler {
  case class Sample(size: Long, numUpdates: Long)
}

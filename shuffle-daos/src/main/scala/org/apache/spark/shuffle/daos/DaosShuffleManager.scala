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

import java.lang.reflect.Method
import java.util.concurrent.ConcurrentHashMap

import io.daos.DaosClient
import scala.collection.JavaConverters._

import org.apache.spark.{ShuffleDependency, SparkConf, SparkEnv, TaskContext}
import org.apache.spark.internal.{config, Logging}
import org.apache.spark.shuffle._
import org.apache.spark.shuffle.sort.SortShuffleManager.canUseBatchFetch
import org.apache.spark.util.{ShutdownHookManager, Utils}
import org.apache.spark.util.collection.OpenHashSet

/**
 * A shuffle manager to write and read map data from DAOS using DAOS object API.
 *
 * @param conf
 * spark configuration
 */
class DaosShuffleManager(conf: SparkConf) extends ShuffleManager with Logging {

  logInfo("loaded " + classOf[DaosShuffleManager])

  if (conf.get(config.SHUFFLE_USE_OLD_FETCH_PROTOCOL)) {
    throw new IllegalArgumentException("DaosShuffleManager doesn't support old fetch protocol. Please remove " +
      config.SHUFFLE_USE_OLD_FETCH_PROTOCOL.key)
  }

  def findHadoopFs: Method = {
    try {
      val fsClass = Utils.classForName("org.apache.hadoop.fs.FileSystem")
      fsClass.getMethod("closeAll")
    } catch {
      case _: Throwable => null
    }
  }

  val closeAllFsMethod = findHadoopFs

  def closeAllHadoopFs: Unit = {
    if (closeAllFsMethod == null) {
      return
    }
    try {
      closeAllFsMethod.invoke(null)
    } catch {
      case _: Throwable => // ignore all exceptions
    }
  }

  val daosShuffleIO = new DaosShuffleIO(conf)
  daosShuffleIO.initialize(
    conf.getAllWithPrefix(ShuffleDataIOUtils.SHUFFLE_SPARK_CONF_PREFIX).toMap.asJava)

  // stop all executor threads when shutdown
  ShutdownHookManager.addShutdownHook(() => daosShuffleIO.close())

  val daosFinalizer = DaosClient.FINALIZER

  val finalizer = () => {
    closeAllHadoopFs
    daosFinalizer.run()
  }

  if (io.daos.ShutdownHookManager.removeHook(daosFinalizer) ||
    org.apache.hadoop.util.ShutdownHookManager.get.removeShutdownHook(daosFinalizer)) {
    ShutdownHookManager.addShutdownHook(finalizer)
    logInfo("relocated daos finalizer")
  } else {
    logWarning("failed to relocate daos finalizer")
  }

  /**
   * A mapping from shuffle ids to the task ids of mappers producing output for those shuffles.
   */
  private[this] val taskIdMapsForShuffle = new ConcurrentHashMap[Int, OpenHashSet[Long]]()

  /**
   * register {@link ShuffleDependency} to pass to tasks.
   *
   * @param shuffleId
   * unique ID of shuffle in job
   * @param dependency
   * shuffle dependency
   * @tparam K
   * type of KEY
   * @tparam V
   * type of VALUE
   * @tparam C
   * type of combined value
   * @return {@link BaseShuffleHandle}
   */
  override def registerShuffle[K, V, C](
      shuffleId: Int,
      dependency: ShuffleDependency[K, V, C]): BaseShuffleHandle[K, V, C]
    = {
    new BaseShuffleHandle(shuffleId, dependency)
  }

  override def getWriter[K, V](
      handle: ShuffleHandle,
      mapId: Long,
      context: TaskContext,
      metrics: ShuffleWriteMetricsReporter): DaosShuffleWriter[K, V, _]
    = {
    val mapTaskIds = taskIdMapsForShuffle.computeIfAbsent(
      handle.shuffleId, _ => new OpenHashSet[Long](16))
    mapTaskIds.synchronized { mapTaskIds.add(context.taskAttemptId()) }
    new DaosShuffleWriter(handle.asInstanceOf[BaseShuffleHandle[K, V, _]], mapId, context, daosShuffleIO)
  }

  override def getReader[K, C](
      handle: ShuffleHandle,
      startMapIndex: Int,
      endMapIndex: Int,
      startPartition: Int,
      endPartition: Int,
      context: TaskContext,
      metrics: ShuffleReadMetricsReporter): DaosShuffleReader[K, C]
    = {
    val blocksByAddress = SparkEnv.get.mapOutputTracker.getMapSizesByExecutorId(
      handle.shuffleId, startMapIndex, endMapIndex, startPartition, endPartition)
    new DaosShuffleReader(handle.asInstanceOf[BaseShuffleHandle[K, _, C]], blocksByAddress, context,
      metrics, daosShuffleIO, SparkEnv.get.serializerManager,
      shouldBatchFetch = canUseBatchFetch(startPartition, endPartition, context))
  }

  override def unregisterShuffle(shuffleId: Int): Boolean = {
    logInfo("unregistering shuffle: " + shuffleId)
    taskIdMapsForShuffle.remove(shuffleId)
    daosShuffleIO.removeShuffle(shuffleId)
  }

  override def shuffleBlockResolver: ShuffleBlockResolver = null

  override def stop(): Unit = {
    daosShuffleIO.close()
    finalizer()
    ShutdownHookManager.removeShutdownHook(finalizer)
    logInfo("stopped " + classOf[DaosShuffleManager])
  }
}

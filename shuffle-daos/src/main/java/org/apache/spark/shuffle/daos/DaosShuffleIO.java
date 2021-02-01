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

import io.daos.obj.DaosObjClient;
import io.daos.obj.DaosObject;
import org.apache.spark.SparkConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * IO object for read and write to DAOS. It initializes and creates following resources.
 * - DAOS client, {@link DaosObjClient}
 * - read/write executors (if fromOtherThreads configured true), {@link BoundThreadExecutors}
 * - {@link DaosReader} objects
 * - {@link DaosWriter} objects
 *
 * It does the cleanup of all above resources in {@link #close()}.
 *
 */
public class DaosShuffleIO {

  private DaosObjClient objClient;

  private Map<String, DaosObject> objectMap = new ConcurrentHashMap<>();

  private SparkConf conf;

  private Map<String, String> driverConf;

  private String poolId;

  private String contId;

  private boolean removeShuffleData;

  private IOManager ioManager;

  private static final Logger logger = LoggerFactory.getLogger(DaosShuffleIO.class);

  /**
   * constructor with {@link SparkConf}.
   * reader and writer are created here if fromOtherThread is true.
   *
   * @param conf
   */
  public DaosShuffleIO(SparkConf conf) {
    this.conf = conf;
    boolean async = (boolean)conf.get(package$.MODULE$.SHUFFLE_DAOS_IO_ASYNC());
    this.ioManager = async ? new IOManagerAsync(conf, objectMap) : new IOManagerSync(conf, objectMap);
    this.removeShuffleData = (boolean)conf.get(package$.MODULE$.SHUFFLE_DAOS_REMOVE_SHUFFLE_DATA());
  }

  /**
   * connect DAOS server.
   *
   * @param driverConf
   * @throws IOException
   */
  public void initialize(Map<String, String> driverConf) throws IOException {
    this.driverConf = driverConf;
    poolId = conf.get(package$.MODULE$.SHUFFLE_DAOS_POOL_UUID());
    contId = conf.get(package$.MODULE$.SHUFFLE_DAOS_CONTAINER_UUID());
    if (poolId == null || contId == null) {
      throw new IllegalArgumentException("DaosShuffleManager needs pool id and container id");
    }

    objClient = new DaosObjClient.DaosObjClientBuilder()
      .poolId(poolId).containerId(contId)
      .build();
    ioManager.setObjClient(objClient);
  }

  /**
   * get DaosWriter with shuffle object created and opened.
   *
   * Should be called in the Driver after TaskScheduler registration and
   * from the start in the Executor.
   *
   * @param numPartitions
   * @param shuffleId
   * @param mapId
   * @return DaosWriter object for specific shuffle and map
   * @throws {@link IOException}
   */
  public DaosWriter getDaosWriter(int numPartitions, int shuffleId, long mapId)
      throws IOException {
    return ioManager.getDaosWriter(numPartitions, shuffleId, mapId);
  }

  /**
   * get DaosReader with shuffle object created and opened.
   *
   * @param shuffleId
   * @return DaosReader
   * @throws IOException
   */
  public DaosReader getDaosReader(int shuffleId) throws IOException {
    return ioManager.getDaosReader(shuffleId);
  }

  private String getKey(long appId, int shuffleId) {
    return appId + "" + shuffleId;
  }

  public IOManager getIoManager() {
    return ioManager;
  }

  /**
   * remove shuffle object.
   *
   * @param shuffleId
   * @return
   */
  public boolean removeShuffle(int shuffleId) {
    long appId = IOManager.parseAppId(conf.getAppId());
    logger.info("punching daos object for app id: " + appId + ", shuffle id: " + shuffleId);
    try {
      DaosObject object = objectMap.remove(getKey(appId, shuffleId));
      if (object != null) {
        if (removeShuffleData) {
          object.punch();
        }
        object.close();
      }
    } catch (Exception e) {
      logger.error("failed to punch object", e);
      return false;
    }
    return true;
  }

  /**
   * releasing all resources, including disconnect from DAOS server.
   *
   * @throws IOException
   */
  public void close() throws IOException {
    ioManager.close();
    objClient.forceClose();
  }
}

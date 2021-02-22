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

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * A abstract class with {@link DaosObject} wrapped to read data from DAOS.
 */
public interface DaosReader {


  DaosObject getObject();

  /**
   * release resources bound with this reader.
   *
   * @param force
   * force close even if there is on-going read
   */
  void close(boolean force);

  /**
   * set global <code>readMap</code> and hook this reader for releasing resources.
   *
   * @param readerMap
   * global reader map
   */
  void setReaderMap(Map<DaosReader, Integer> readerMap);

  /**
   * prepare read with some parameters.
   *
   * @param partSizeMap
   * @param maxBytesInFlight
   * how many bytes can be read concurrently
   * @param maxReqSizeShuffleToMem
   * maximum data can be put in memory
   * @param metrics
   * @return
   */
  void prepare(LinkedHashMap<Tuple2<Long, Integer>, Tuple3<Long, BlockId, BlockManagerId>> partSizeMap,
               long maxBytesInFlight, long maxReqSizeShuffleToMem, ShuffleReadMetricsReporter metrics);

  /**
   * current map/reduce id being requested.
   *
   * @return map/reduce id tuple
   */
  Tuple2<Long, Integer> curMapReduceId();

  /**
   * get available buffer after iterating current buffer, next buffer in current desc and next desc.
   *
   * @return buffer with data read from DAOS
   * @throws IOException
   */
  ByteBuf nextBuf() throws IOException;

  /**
   * All data from current map output is read and
   * reach to data from next map?
   *
   * @return true or false
   */
  boolean isNextMap();

  /**
   * upper layer should call this method to read more map output
   */
  void setNextMap(boolean b);

  /**
   * check if all data from current map output is read.
   */
  void checkPartitionSize() throws IOException;

  /**
   * check if all map outputs are read.
   *
   * @throws IOException
   */
  void checkTotalPartitions() throws IOException;

  /**
   * reader configurations, please check configs prefixed with SHUFFLE_DAOS_READ in {@link package$#MODULE$}.
   */
  final class ReaderConfig {
    private long minReadSize;
    private long maxBytesInFlight;
    private long maxMem;
    private int readBatchSize;
    private int waitDataTimeMs;
    private int waitTimeoutTimes;
    private boolean fromOtherThread;

    private static final Logger log = LoggerFactory.getLogger(ReaderConfig.class);

    public ReaderConfig() {
      this(true);
    }

    private ReaderConfig(boolean load) {
      if (load) {
        initialize();
      }
    }

    private void initialize() {
      SparkConf conf = SparkEnv.get().conf();
      minReadSize = (long)conf.get(package$.MODULE$.SHUFFLE_DAOS_READ_MINIMUM_SIZE()) * 1024;
      this.maxBytesInFlight = -1L;
      this.maxMem = -1L;
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

    public ReaderConfig copy(long maxBytesInFlight, long maxMem) {
      ReaderConfig rc = new ReaderConfig(false);
      rc.maxMem = maxMem;
      rc.minReadSize = minReadSize;
      rc.readBatchSize = readBatchSize;
      rc.waitDataTimeMs = waitDataTimeMs;
      rc.waitTimeoutTimes = waitTimeoutTimes;
      rc.fromOtherThread = fromOtherThread;
      if (maxBytesInFlight < rc.minReadSize) {
        rc.maxBytesInFlight = minReadSize;
      } else {
        rc.maxBytesInFlight = maxBytesInFlight;
      }
      return rc;
    }

    public int getReadBatchSize() {
      return readBatchSize;
    }

    public int getWaitDataTimeMs() {
      return waitDataTimeMs;
    }

    public int getWaitTimeoutTimes() {
      return waitTimeoutTimes;
    }

    public long getMaxBytesInFlight() {
      return maxBytesInFlight;
    }

    public long getMaxMem() {
      return maxMem;
    }

    public long getMinReadSize() {
      return minReadSize;
    }

    public boolean isFromOtherThread() {
      return fromOtherThread;
    }
  }
}

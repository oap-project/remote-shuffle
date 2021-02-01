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

import org.apache.spark.SparkConf;
import org.apache.spark.SparkEnv;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * A DAOS writer per map task which may have multiple map output partitions.
 * Each partition has one corresponding buffer which caches records until
 * a specific {@link #flush(int)} call being made. Data is written to DAOS
 * in either caller thread or other dedicated thread.
 */
public interface DaosWriter {

  /**
   * write to buffer.
   *
   * @param partitionId
   * @param b
   */
  void write(int partitionId, int b);

  /**
   * write to buffer.
   *
   * @param partitionId
   * @param array
   */
  void write(int partitionId, byte[] array);

  /**
   * write to buffer.
   *
   * @param partitionId
   * @param array
   * @param offset
   * @param len
   */
  void write(int partitionId, byte[] array, int offset, int len);

  /**
   * get length of all partitions.
   * 0 for empty partition.
   *
   * @param numPartitions
   * @return array of partition lengths
   */
  long[] getPartitionLens(int numPartitions);

  /**
   * Flush specific partition to DAOS.
   *
   * @param partitionId
   * @throws IOException
   */
  void flush(int partitionId) throws IOException;

  /**
   * close writer.
   */
  void close();

  /**
   * Write configurations. Please check configs prefixed with SHUFFLE_DAOS_WRITE in {@link package$#MODULE$}.
   */
  class WriterConfig {
    private int bufferSize;
    private int minSize;
    private boolean warnSmallWrite;
    private long waitTimeMs;
    private int timeoutTimes;
    private long totalInMemSize;
    private int totalSubmittedLimit;
    private int threads;
    private boolean fromOtherThreads;

    private static final Logger logger = LoggerFactory.getLogger(WriterConfig.class);

    WriterConfig() {
      SparkConf conf = SparkEnv.get().conf();
      warnSmallWrite = (boolean) conf.get(package$.MODULE$.SHUFFLE_DAOS_WRITE_WARN_SMALL_SIZE());
      bufferSize = (int) ((long) conf.get(package$.MODULE$.SHUFFLE_DAOS_WRITE_SINGLE_BUFFER_SIZE())
          * 1024 * 1024);
      minSize = (int) ((long)conf.get(package$.MODULE$.SHUFFLE_DAOS_WRITE_MINIMUM_SIZE()) * 1024);
      timeoutTimes = (int)conf.get(package$.MODULE$.SHUFFLE_DAOS_WRITE_WAIT_DATA_TIMEOUT_TIMES());
      waitTimeMs = (int)conf.get(package$.MODULE$.SHUFFLE_DAOS_WRITE_WAIT_MS());
      totalInMemSize = (long)conf.get(package$.MODULE$.SHUFFLE_DAOS_WRITE_MAX_BYTES_IN_FLIGHT()) * 1024;
      totalSubmittedLimit = (int)conf.get(package$.MODULE$.SHUFFLE_DAOS_WRITE_SUBMITTED_LIMIT());
      threads = (int)conf.get(package$.MODULE$.SHUFFLE_DAOS_WRITE_THREADS());
      fromOtherThreads = (boolean)conf
          .get(package$.MODULE$.SHUFFLE_DAOS_WRITE_IN_OTHER_THREAD());
      if (logger.isDebugEnabled()) {
        logger.debug(toString());
      }
    }


    public int getBufferSize() {
      return bufferSize;
    }

    public int getMinSize() {
      return minSize;
    }

    public boolean isWarnSmallWrite() {
      return warnSmallWrite;
    }

    public long getWaitTimeMs() {
      return waitTimeMs;
    }

    public int getTimeoutTimes() {
      return timeoutTimes;
    }

    public long getTotalInMemSize() {
      return totalInMemSize;
    }

    public int getTotalSubmittedLimit() {
      return totalSubmittedLimit;
    }

    public int getThreads() {
      return threads;
    }

    public boolean isFromOtherThreads() {
      return fromOtherThreads;
    }

    @Override
    public String toString() {
      return "WriteConfig{" +
          "bufferSize=" + bufferSize +
          ", minSize=" + minSize +
          ", warnSmallWrite=" + warnSmallWrite +
          ", waitTimeMs=" + waitTimeMs +
          ", timeoutTimes=" + timeoutTimes +
          ", totalInMemSize=" + totalInMemSize +
          ", totalSubmittedLimit=" + totalSubmittedLimit +
          ", threads=" + threads +
          ", fromOtherThreads=" + fromOtherThreads +
          '}';
    }
  }
}

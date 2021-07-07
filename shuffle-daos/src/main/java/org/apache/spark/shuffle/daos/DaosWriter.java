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

import io.daos.BufferAllocator;
import io.daos.obj.DaosObject;
import io.daos.obj.IODataDescSync;
import io.daos.obj.IOSimpleDDAsync;
import io.netty.buffer.ByteBuf;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkEnv;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

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
   * set if need to spill buffer to DAOS or just append buffer.
   *
   * @param needSpill
   */
  void setNeedSpill(boolean needSpill);

  /**
   * mark all records being consumed in map task. Later writing will be final output.
   */
  void setFinal();

  /**
   * mark it's final write after all spills being merged.
   */
  void setMerged(int partitionId);

  /**
   * is spilling actual happened on specific partition.
   *
   * @param partitionId
   */
  boolean isSpilled(int partitionId);

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
   * Flush all pending writes.
   *
   * @throws IOException
   */
  void flushAll() throws IOException;

  /**
   * close writer.
   */
  void close();

  /**
   * reset metrics for merging spilled records.
   *
   * @param partitionId
   */
  void resetMetrics(int partitionId);

  /**
   * get list of spilled.
   *
   * @param partitionId
   * @return
   */
  List<SpillInfo> getSpillInfo(int partitionId);

  /**
   * Write parameters, including mapId, shuffleId, number of partitions and write config.
   */
  class WriteParam {
    private int numPartitions;
    private int shuffleId;
    private long mapId;
    private DaosWriter.WriterConfig config;

    public WriteParam numPartitions(int numPartitions) {
      this.numPartitions = numPartitions;
      return this;
    }

    public WriteParam shuffleId(int shuffleId) {
      this.shuffleId = shuffleId;
      return this;
    }

    public WriteParam mapId(long mapId) {
      this.mapId = mapId;
      return this;
    }

    public WriteParam config(DaosWriter.WriterConfig config) {
      this.config = config;
      return this;
    }

    public int getNumPartitions() {
      return numPartitions;
    }

    public int getShuffleId() {
      return shuffleId;
    }

    public long getMapId() {
      return mapId;
    }

    public WriterConfig getConfig() {
      return config;
    }
  }

  class SpillInfo {
    private String reduceId;
    private String mapId;
    private long size;

    public SpillInfo(String partitionId, String cmapId, long roundSize) {
      this.reduceId = partitionId;
      this.mapId = cmapId;
      this.size = roundSize;
    }

    public String getReduceId() {
      return reduceId;
    }

    public String getMapId() {
      return mapId;
    }

    public long getSize() {
      return size;
    }
  }

  /**
   * Write data to one or multiple netty direct buffers which will be written to DAOS without copy
   */
  class NativeBuffer implements Comparable<NativeBuffer> {
    private String mapId;
    private int seq;
    private boolean needSpill;
    private int partitionId;
    private String partitionIdKey;
    private int bufferSize;
    private int idx = -1;
    private List<ByteBuf> bufList = new ArrayList<>();
    private List<SpillInfo> spillInfos;
    private long totalSize;
    private long roundSize;

    private DaosObject object;

    private static final Logger LOG = LoggerFactory.getLogger(NativeBuffer.class);

    NativeBuffer(DaosObject object, String mapId, int partitionId, int bufferSize, boolean needSpill) {
      this.object = object;
      this.mapId = mapId;
      this.partitionId = partitionId;
      this.partitionIdKey = String.valueOf(partitionId);
      this.bufferSize = bufferSize;
      this.needSpill = needSpill;
      if (needSpill) {
        spillInfos = new ArrayList<>();
      }
    }

    private ByteBuf addNewByteBuf(int len) {
      ByteBuf buf;
      try {
        buf = BufferAllocator.objBufWithNativeOrder(Math.max(bufferSize, len));
      } catch (OutOfMemoryError e) {
        LOG.error("too big buffer size: " + Math.max(bufferSize, len));
        throw e;
      }
      bufList.add(buf);
      idx++;
      return buf;
    }

    private ByteBuf getBuffer(int len) {
      if (idx < 0) {
        return addNewByteBuf(len);
      }
      return bufList.get(idx);
    }

    public void write(int b) {
      ByteBuf buf = getBuffer(1);
      if (buf.writableBytes() < 1) {
        buf = addNewByteBuf(1);
      }
      buf.writeByte(b);
      roundSize += 1;
    }

    public void write(byte[] b) {
      write(b, 0, b.length);
    }

    public void write(byte[] b, int offset, int len) {
      if (len <= 0) {
        return;
      }
      ByteBuf buf = getBuffer(len);
      int avail = buf.writableBytes();
      int gap = len - avail;
      if (gap <= 0) {
        buf.writeBytes(b, offset, len);
      } else {
        buf.writeBytes(b, offset, avail);
        buf = addNewByteBuf(gap);
        buf.writeBytes(b, avail, gap);
      }
      roundSize += len;
    }

    private String currentMapId() {
      if (needSpill) {
        seq++;
      }
      return seq > 0 ? mapId + "_" + seq : mapId;
    }

    private boolean isEmpty() {
      return roundSize == 0 || bufList.isEmpty();
    }

    /**
     * create list of {@link IODataDescSync} each of them has only one akey entry.
     * DAOS has a constraint that same akey cannot be referenced twice in one IO.
     *
     * @return list of {@link IODataDescSync}
     * @throws IOException
     */
    public List<IODataDescSync> createUpdateDescs() throws IOException {
      if (isEmpty()) {
        return Collections.emptyList();
      }
      List<IODataDescSync> descList = new ArrayList<>(bufList.size());
      String cmapId = currentMapId();
      long bufSize = 0;
      for (ByteBuf buf : bufList) {
        IODataDescSync desc = object.createDataDescForUpdate(partitionIdKey, IODataDescSync.IodType.ARRAY, 1);
        desc.addEntryForUpdate(cmapId, totalSize + bufSize, buf);
        bufSize += buf.readableBytes();
        descList.add(desc);
      }
      if (roundSize != bufSize) {
        throw new IOException("expect update size: " + roundSize + ", actual: " + bufSize);
      }
      addSpill(cmapId, roundSize);
      return descList;
    }

    /**
     * create list of {@link IOSimpleDDAsync} each of them has only one akey entry.
     * DAOS has a constraint that same akey cannot be referenced twice in one IO.
     *
     * @return list of {@link IOSimpleDDAsync}
     * @throws IOException
     */
    public List<IOSimpleDDAsync> createUpdateDescAsyncs(long eqHandle) throws IOException {
      if (isEmpty()) {
        return Collections.emptyList();
      }
      List<IOSimpleDDAsync> descList = new ArrayList<>();
      String cmapId = currentMapId();
      long bufSize = 0;
      for (ByteBuf buf : bufList) {
        IOSimpleDDAsync desc = object.createAsyncDataDescForUpdate(partitionIdKey, eqHandle);
        desc.addEntryForUpdate(cmapId, totalSize + bufSize, buf);
        bufSize += buf.readableBytes();
        descList.add(desc);
      }
      if (roundSize != bufSize) {
        throw new IOException("expect update size: " + roundSize + ", actual: " + bufSize);
      }
      addSpill(cmapId, roundSize);
      return descList;
    }

    private void addSpill(String cmapId, long roundSize) {
      if (needSpill) {
        if (seq > 1) {
          LOG.info("reduce ID: " + partitionIdKey + ", map ID: " + cmapId + ", spilling to DAOS, size: " + roundSize);
        }
        spillInfos.add(new SpillInfo(partitionIdKey, cmapId, roundSize));
      }
    }

    public List<SpillInfo> getSpillInfo() {
      return spillInfos;
    }

    public void reset(boolean release) {
      if (release) {
        bufList.forEach(b -> b.release());
      }
      // release==false, buffers will be released when tasks are executed and consumed
      bufList.clear();
      idx = -1;
      totalSize += roundSize;
      roundSize = 0;
    }

    @Override
    public int compareTo(NativeBuffer nativeBuffer) {
      return partitionId - nativeBuffer.partitionId;
    }

    public int getPartitionId() {
      return partitionId;
    }

    public long getTotalSize() {
      return totalSize;
    }

    public long getRoundSize() {
      return roundSize;
    }

    public List<ByteBuf> getBufList() {
      return bufList;
    }

    public boolean isSpilled() {
      return seq > 1 || (seq > 0 && !isEmpty());
    }

    public void resetMetrics() {
      reset(true);
      totalSize = 0;
    }

    public void resetSeq() {
      this.seq = 0;
    }

    public void setNeedSpill(boolean needSpill) {
      this.needSpill = needSpill;
    }
  }

  /**
   * Write configurations. Please check configs prefixed with SHUFFLE_DAOS_WRITE in {@link package$#MODULE$}.
   */
  class WriterConfig {
    private int bufferSize;
    private int minSize;
    private boolean warnSmallWrite;
    private int asyncWriteBatchSize;
    private long waitTimeMs;
    private int timeoutTimes;
    private long totalInMemSize;
    private int totalSubmittedLimit;
    private int threads;
    private boolean fromOtherThreads;
    private SparkConf conf;

    private static final Logger logger = LoggerFactory.getLogger(WriterConfig.class);

    WriterConfig(SparkConf conf) {
      this.conf = conf;
      if (this.conf == null) {
        this.conf = SparkEnv.get().conf();
      }
      warnSmallWrite = (boolean) conf.get(package$.MODULE$.SHUFFLE_DAOS_WRITE_WARN_SMALL_SIZE());
      bufferSize = (int) ((long) conf.get(package$.MODULE$.SHUFFLE_DAOS_WRITE_SINGLE_BUFFER_SIZE())
          * 1024 * 1024);
      bufferSize += bufferSize * 0.1; // 10% more for metadata overhead and upper layer deviation
      minSize = (int) ((long)conf.get(package$.MODULE$.SHUFFLE_DAOS_WRITE_MINIMUM_SIZE()) * 1024);
      asyncWriteBatchSize = (int)conf.get(package$.MODULE$.SHUFFLE_DAOS_ASYNC_WRITE_BATCH_SIZE());
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

    public int getAsyncWriteBatchSize() {
      return asyncWriteBatchSize;
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

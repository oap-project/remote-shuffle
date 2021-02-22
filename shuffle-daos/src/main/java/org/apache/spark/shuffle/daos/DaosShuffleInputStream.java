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
import org.apache.spark.shuffle.ShuffleReadMetricsReporter;
import org.apache.spark.storage.BlockId;
import org.apache.spark.storage.BlockManagerId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;
import scala.Tuple3;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;

@NotThreadSafe
/**
 * A inputstream for reading shuffled data being consisted of multiple map outputs.
 *
 * All records in one specific map output are from same KryoSerializer or Java serializer. To facilitate reading
 * multiple map outputs in this one inputstream, the read methods return -1 to indicate the completion of current
 * map output. Caller should call {@link DaosShuffleInputStream#isCompleted()} to check if all map outputs are read.
 *
 * To read more data from next map output, user should call {@link #nextMap()} before read.
 */
public class DaosShuffleInputStream extends InputStream {

  private DaosReader reader;

  private DaosObject object;

  private ShuffleReadMetricsReporter metrics;

  private volatile boolean cleaned;

  private boolean completed;

  // ensure the order of partition
  // (mapid, reduceid) -> (length, BlockId, BlockManagerId)
  private LinkedHashMap<Tuple2<Long, Integer>, Tuple3<Long, BlockId, BlockManagerId>> partSizeMap;

  private static final Logger log = LoggerFactory.getLogger(DaosShuffleInputStream.class);

  /**
   * constructor with ordered map outputs info. Check {@link DaosReader.ReaderConfig} for more paras controlling
   * how data being read from DAOS.
   *
   * @param reader
   * daos reader
   * @param partSizeMap
   * ordered map outputs info. They are organize as (mapid, reduceid) -> (length, BlockId, BlockManagerId)
   * @param maxBytesInFlight
   * how many bytes can be read concurrently
   * @param maxReqSizeShuffleToMem
   * maximum data can be put in memory
   * @param metrics
   * read metrics
   */
  public DaosShuffleInputStream(
      DaosReader reader,
      LinkedHashMap<Tuple2<Long, Integer>, Tuple3<Long, BlockId, BlockManagerId>> partSizeMap,
      long maxBytesInFlight, long maxReqSizeShuffleToMem,
      ShuffleReadMetricsReporter metrics) {
    this.partSizeMap = partSizeMap;
    this.reader = reader;
    reader.prepare(partSizeMap, maxBytesInFlight, maxReqSizeShuffleToMem, metrics);
    this.object = reader.getObject();
    this.metrics = metrics;
  }

  public BlockId getCurBlockId() {
    if (reader.curMapReduceId() == null) {
      return null;
    }
    return partSizeMap.get(reader.curMapReduceId())._2();
  }

  public BlockManagerId getCurOriginAddress() {
    if (reader.curMapReduceId() == null) {
      return null;
    }
    return partSizeMap.get(reader.curMapReduceId())._3();
  }

  public long getCurMapIndex() {
    if (reader.curMapReduceId() == null) {
      return -1;
    }
    return reader.curMapReduceId()._1;
  }

  @Override
  public int read() throws IOException {
    while (!completed) {
      ByteBuf buf = reader.nextBuf();
      if (buf == null) { // reach end
        complete();
        return -1;
      }
      if (reader.isNextMap()) { // indication to close upper layer object inputstream
        return -1;
      }
      if (buf.readableBytes() >= 1) {
        return buf.readByte();
      }
    }
    return -1;
  }

  @Override
  public int read(byte[] bytes) throws IOException {
    return read(bytes, 0, bytes.length);
  }

  @Override
  public int read(byte[] bytes, int offset, int length) throws IOException {
    int len = length;
    while (!completed) {
      ByteBuf buf = reader.nextBuf();
      if (buf == null) { // reach end
        complete();
        int r = length - len;
        return r == 0 ? -1 : r;
      }
      if (reader.isNextMap()) { // indication to close upper layer object inputstream
        int r = length - len;
        return r == 0 ? -1 : r;
      }
      if (len <= buf.readableBytes()) {
        buf.readBytes(bytes, offset, len);
        return length;
      }
      int maxRead = buf.readableBytes();
      buf.readBytes(bytes, offset, maxRead);
      offset += maxRead;
      len -= maxRead;
    }
    return -1;
  }

  /**
   * upper layer should call this method to read more map output
   */
  public void nextMap() {
    reader.setNextMap(false);
  }

  private void complete() throws IOException {
    if (!completed) {
      reader.checkPartitionSize();
      reader.checkTotalPartitions();
      completed = true;
    }
  }

  private void cleanup() {
    if (!cleaned) {
      reader.close(false);
      cleaned = true;
      completed = true;
    }
  }

  /**
   * no actual close if it's not completed yet.
   */
  @Override
  public void close() {
    close(false);
  }

  /**
   * force close stream when task is competed or error is occurred.
   *
   * @param force
   */
  public void close(boolean force) {
    if (force || completed) {
      cleanup();
    }
  }

  public boolean isCompleted() {
    return completed;
  }
}

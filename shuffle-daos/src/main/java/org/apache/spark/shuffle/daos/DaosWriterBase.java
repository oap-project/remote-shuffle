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

import io.daos.obj.DaosObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

public abstract class DaosWriterBase implements DaosWriter {

  protected DaosObject object;

  protected String mapId;

  protected WriteParam param;

  protected WriterConfig config;

  protected Map<DaosWriter, Integer> writerMap;

  protected NativeBuffer[] partitionBufArray;

  private static final Logger LOG = LoggerFactory.getLogger(DaosWriterBase.class);

  protected DaosWriterBase(DaosObject object, WriteParam param) {
    this.param = param;
    this.config = param.getConfig();
    this.partitionBufArray = new NativeBuffer[param.getNumPartitions()];
    this.mapId = String.valueOf(param.getMapId());
    this.object = object;
  }

  protected NativeBuffer getNativeBuffer(int partitionId) {
    NativeBuffer buffer = partitionBufArray[partitionId];
    if (buffer == null) {
      buffer = new NativeBuffer(object, mapId, partitionId, config.getBufferSize());
      partitionBufArray[partitionId] = buffer;
    }
    return buffer;
  }

  @Override
  public void write(int partitionId, int b) {
    getNativeBuffer(partitionId).write(b);
  }

  @Override
  public void write(int partitionId, byte[] array) {
    getNativeBuffer(partitionId).write(array);
  }

  @Override
  public void write(int partitionId, byte[] array, int offset, int len) {
    getNativeBuffer(partitionId).write(array, offset, len);
  }

  @Override
  public void flushAll() throws IOException {}

  @Override
  public long[] getPartitionLens(int numPartitions) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("partition map size: " + partitionBufArray.length);
      for (int i = 0; i < numPartitions; i++) {
        NativeBuffer nb = partitionBufArray[i];
        if (nb != null) {
          LOG.debug("id: " + i + ", native buffer: " + nb.getPartitionId() + ", " +
              nb.getTotalSize() + ", " + nb.getRoundSize());
        }
      }
    }
    long[] lens = new long[numPartitions];
    for (int i = 0; i < numPartitions; i++) {
      NativeBuffer nb = partitionBufArray[i];
      if (nb != null) {
        lens[i] = nb.getTotalSize();
        if (nb.getRoundSize() != 0 || !nb.getBufList().isEmpty()) {
          throw new IllegalStateException("round size should be 0, " + nb.getRoundSize() +
              ", buflist should be empty, " +
              nb.getBufList().size());
        }
      } else {
        lens[i] = 0;
      }
    }
    return lens;
  }
}

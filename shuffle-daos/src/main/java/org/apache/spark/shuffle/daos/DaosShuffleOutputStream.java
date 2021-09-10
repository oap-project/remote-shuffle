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

import java.io.IOException;
import java.io.OutputStream;

/**
 * OutputStream wrapper of {@link DaosWriter} for each map partition
 */
public class DaosShuffleOutputStream extends OutputStream {

  private int partitionId;
  private DaosWriter daosWriter;

  private long writtenBytes = 0L;

  public DaosShuffleOutputStream(int partitionId, DaosWriter daosWriter) {
    this.partitionId = partitionId;
    this.daosWriter = daosWriter;
  }

  @Override
  public void write(int b) {
    daosWriter.write(partitionId, (byte)b);
    writtenBytes += 1;
  }

  @Override
  public void write(byte[] b) {
    daosWriter.write(partitionId, b);
    writtenBytes += b.length;
  }

  @Override
  public void write(byte[] b, int off, int len) {
    daosWriter.write(partitionId, b, off, len);
    writtenBytes += len;
  }

  @Override
  public void flush() throws IOException {
    // do nothing since we want to control the actual DAOS write, not objectoutputstream/kryo
  }

  @Override
  public void close() throws IOException {
    daosWriter.flushAll(partitionId);
  }

  public long getWrittenBytes() {
    return writtenBytes;
  }
}

package org.apache.spark.shuffle.daos;

import io.daos.obj.DaosObject;
import io.daos.obj.IODataDescBase;
import io.netty.buffer.ByteBuf;
import org.apache.spark.shuffle.ShuffleReadMetricsReporter;
import org.apache.spark.storage.BlockId;
import scala.Tuple2;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

public abstract class DaosReaderBase implements DaosReader {

  protected DaosObject object;

  protected Map<DaosReader, Integer> readerMap;

  protected ReaderConfig config;

  protected LinkedHashMap<Tuple2<Long, Integer>, Tuple2<Long, BlockId>> partSizeMap;

  protected Iterator<Tuple2<Long, Integer>> mapIdIt;

  protected ShuffleReadMetricsReporter metrics;

  protected long currentPartSize;

  protected Tuple2<Long, Integer> curMapReduceId;
  protected Tuple2<Long, Integer> lastMapReduceIdForSubmit;
  protected Tuple2<Long, Integer> lastMapReduceIdForReturn;
  protected int curOffset;
  protected boolean nextMap;

  protected IODataDescBase currentDesc;
  protected IODataDescBase.BaseEntry currentEntry;
  protected int entryIdx;

  protected int totalParts;
  protected int partsRead;

  protected DaosReaderBase(DaosObject object, ReaderConfig config) {
    this.object = object;
    this.config = config;
  }

  @Override
  public DaosObject getObject() {
    return object;
  }

  @Override
  public void close(boolean force) {
    if (readerMap != null) {
      readerMap.remove(this);
      readerMap = null;
    }
  }

  @Override
  public void setReaderMap(Map<DaosReader, Integer> readerMap) {
    readerMap.put(this, 0);
    this.readerMap = readerMap;
  }

  @Override
  public void prepare(LinkedHashMap<Tuple2<Long, Integer>, Tuple2<Long, BlockId>> partSizeMap,
                      long maxBytesInFlight, long maxReqSizeShuffleToMem, ShuffleReadMetricsReporter metrics) {
    this.partSizeMap = partSizeMap;
    this.config = config.copy(maxBytesInFlight, maxReqSizeShuffleToMem);
    this.metrics = metrics;
    this.totalParts = partSizeMap.size();
    mapIdIt = partSizeMap.keySet().iterator();
  }

  @Override
  public Tuple2<Long, Integer> curMapReduceId() {
    return lastMapReduceIdForSubmit;
  }

  @Override
  public void nextMapReduceId() {
    if (curMapReduceId != null) {
      return;
    }
    curOffset = 0;
    if (mapIdIt.hasNext()) {
      curMapReduceId = mapIdIt.next();
      partsRead++;
    } else {
      curMapReduceId = null;
    }
  }

  protected ByteBuf tryCurrentDesc() throws IOException {
    if (currentDesc != null) {
      ByteBuf buf;
      while (entryIdx < currentDesc.getNbrOfEntries()) {
        IODataDescBase.BaseEntry entry = currentDesc.getEntry(entryIdx);
        buf = validateLastEntryAndGetBuf(entry);
        if (buf.readableBytes() > 0) {
          return buf;
        }
        entryIdx++;
      }
      entryIdx = 0;
      // no need to release desc since all its entries are released in tryCurrentEntry and
      // internal buffers are released after object.fetch
      // reader.close will release all in case of failure
      currentDesc = null;
    }
    return null;
  }

  protected ByteBuf validateLastEntryAndGetBuf(IODataDescBase.BaseEntry entry) throws IOException {
    ByteBuf buf = entry.getFetchedData();
    int byteLen = buf.readableBytes();
    nextMap = false;
    if (currentEntry != null && entry != currentEntry) {
      if (entry.getKey().equals(currentEntry.getKey())) {
        currentPartSize += byteLen;
      } else {
        checkPartitionSize();
        nextMap = true;
        currentPartSize = byteLen;
      }
    }
    currentEntry = entry;
    metrics.incRemoteBytesRead(byteLen);
    return buf;
  }

  protected ByteBuf tryCurrentEntry() {
    if (currentEntry != null && !currentEntry.isFetchBufReleased()) {
      ByteBuf buf = currentEntry.getFetchedData();
      if (buf.readableBytes() > 0) {
        return buf;
      }
      // release buffer as soon as possible
      currentEntry.releaseDataBuffer();
      entryIdx++;
    }
    // not null currentEntry since it will be used for size validation
    return null;
  }

  protected IODataDescBase createNextDesc(long sizeLimit) throws IOException {
    long remaining = sizeLimit;
    int reduceId = -1;
    long mapId;
    IODataDescBase desc = null;
    while (remaining > 0) {
      nextMapReduceId();
      if (curMapReduceId == null) {
        break;
      }
      if (reduceId > 0 && curMapReduceId._2 != reduceId) { // make sure entries under same reduce
        break;
      }
      reduceId = curMapReduceId._2;
      mapId = curMapReduceId._1;
      lastMapReduceIdForSubmit = curMapReduceId;
      long readSize = partSizeMap.get(curMapReduceId)._1() - curOffset;
      long offset = curOffset;
      if (readSize > remaining) {
        readSize = remaining;
        curOffset += readSize;
      } else {
        curOffset = 0;
        curMapReduceId = null;
      }
      if (desc == null) {
        desc = createFetchDataDesc(String.valueOf(reduceId));
      }
      addFetchEntry(desc, String.valueOf(mapId), offset, readSize);
      remaining -= readSize;
    }
    return desc;
  }

  protected abstract IODataDescBase createFetchDataDesc(String reduceId) throws IOException;

  protected abstract void addFetchEntry(IODataDescBase desc, String mapId, long offset, long readSize)
      throws IOException ;

  @Override
  public boolean isNextMap() {
    return nextMap;
  }

  @Override
  public void setNextMap(boolean nextMap) {
    this.nextMap = nextMap;
  }

  @Override
  public void checkPartitionSize() throws IOException {
    if (lastMapReduceIdForReturn == null) {
      return;
    }
    // partition size is not accurate after compress/decompress
    long size = partSizeMap.get(lastMapReduceIdForReturn)._1();
    if (size < 35 * 1024 * 1024 * 1024 && currentPartSize * 1.1 < size) {
      throw new IOException("expect partition size " + partSizeMap.get(lastMapReduceIdForReturn) +
          ", actual size " + currentPartSize + ", mapId and reduceId: " + lastMapReduceIdForReturn);
    }
    metrics.incRemoteBlocksFetched(1);
  }

  @Override
  public void checkTotalPartitions() throws IOException {
    if (partsRead != totalParts) {
      throw new IOException("expect total partitions to be read: " + totalParts + ", actual read: " + partsRead);
    }
  }
}

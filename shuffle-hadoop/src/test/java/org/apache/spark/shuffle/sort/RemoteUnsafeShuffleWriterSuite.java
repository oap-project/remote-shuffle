/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.shuffle.sort;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.*;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.*;
import org.apache.spark.shuffle.ShuffleWriteMetricsReporter;
import org.apache.spark.shuffle.remote.RemoteShuffleBlockResolver;
import scala.Option;
import scala.Product2;
import scala.Tuple2;
import scala.collection.Iterator;

import com.google.common.collect.HashMultiset;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import org.apache.spark.executor.ShuffleWriteMetrics;
import org.apache.spark.executor.TaskMetrics;
import org.apache.spark.io.CompressionCodec$;
import org.apache.spark.io.LZ4CompressionCodec;
import org.apache.spark.io.LZFCompressionCodec;
import org.apache.spark.io.SnappyCompressionCodec;
import org.apache.spark.memory.TaskMemoryManager;
import org.apache.spark.memory.TestMemoryManager;
import org.apache.spark.network.util.LimitedInputStream;
import org.apache.spark.scheduler.MapStatus;
import org.apache.spark.serializer.*;
import org.apache.spark.storage.*;
import org.apache.spark.shuffle.remote.*;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;
import static org.junit.Assert.*;
import static org.mockito.Answers.RETURNS_SMART_NULLS;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.*;

public class RemoteUnsafeShuffleWriterSuite {

  static final int NUM_PARTITITONS = 4;
  TestMemoryManager memoryManager;
  TaskMemoryManager taskMemoryManager;
  final HashPartitioner hashPartitioner = new HashPartitioner(NUM_PARTITITONS);
  Path mergedOutputFile;
  long[] partitionSizesInMergedFile;
  final LinkedList<Path> spillFilesCreated = new LinkedList<>();
  SparkConf conf;
  final Serializer serializer = new KryoSerializer(new SparkConf());
  TaskMetrics taskMetrics;

  BlockManager blockManager;

  @Mock(answer = RETURNS_SMART_NULLS)
  RemoteShuffleBlockResolver shuffleBlockResolver;

  RemoteShuffleManager shuffleManager;
  SparkContext sc;

  @Mock(answer = RETURNS_SMART_NULLS)
  TaskContext taskContext;

  @Mock(answer = RETURNS_SMART_NULLS)
  ShuffleDependency<Object, Object, Object> shuffleDep;

  @Mock(answer = RETURNS_SMART_NULLS)
  ShuffleWriteMetricsReporter metrics;

  FileSystem fs;

  @After
  public void tearDown() throws IOException {
    sc.stop();
    shuffleBlockResolver.stop();
    final long leakedMemory = taskMemoryManager.cleanUpAllAllocatedMemory();
    if (leakedMemory != 0) {
      fail("Test leaked " + leakedMemory + " bytes of managed memory");
    }
  }

  // We cannot incorporate this into the setUp function due to for most tests, SparkContext should
  // be constructed 'not before' but inside the UT cases, to get necessary Spark configuration
  private void setUpSparkContextPrivate() {
    sc = new SparkContext(conf);

    blockManager = SparkEnv.get().blockManager();

    shuffleManager = (RemoteShuffleManager) SparkEnv.get().shuffleManager();
    fs = shuffleManager.shuffleBlockResolver().fs();

    when(shuffleBlockResolver.getDataFile(anyInt(), anyInt())).thenReturn(mergedOutputFile);
    doAnswer(
            invocationOnMock -> {
              partitionSizesInMergedFile = (long[]) invocationOnMock.getArguments()[2];
              Path tmp = (Path) invocationOnMock.getArguments()[3];
              fs.delete(mergedOutputFile, true);
              fs.rename(tmp, mergedOutputFile);
              return null;
            })
        .when(shuffleBlockResolver)
        .writeIndexFileAndCommit(anyInt(), anyInt(), any(long[].class), any(Path.class));

    when(shuffleBlockResolver.createTempShuffleBlock())
        .thenAnswer(
            invocationOnMock -> {
              Tuple2<TempShuffleBlockId, Path> result =
                  shuffleManager.shuffleBlockResolver().createTempShuffleBlock();
              spillFilesCreated.add(result._2);
              return result;
            });
  }

  @Before
  public void setUp() throws IOException {
    MockitoAnnotations.initMocks(this);
    spillFilesCreated.clear();
    String rootDir = "/tmp";
    conf =
        new SparkConf(true)
            .setMaster("local[1]")
            .setAppName("Friday")
            .set("spark.shuffle.manager", RemoteShuffleManager.class.getCanonicalName())
            .set("spark.shuffle.remote.storageMasterUri", "file://")
            .set("spark.shuffle.remote.filesRootDirectory", rootDir)
            .set("spark.buffer.pageSize", "1m")
            .set("spark.memory.offHeap.enabled", "false");

    mergedOutputFile = new Path(rootDir + "/shuffle/someFileAndItsFridayLOL");
    taskMetrics = new TaskMetrics();
    memoryManager = new TestMemoryManager(conf);
    taskMemoryManager = new TaskMemoryManager(memoryManager, 0);

    when(taskContext.taskMetrics()).thenReturn(taskMetrics);
    when(shuffleDep.serializer()).thenReturn(serializer);
    when(shuffleDep.partitioner()).thenReturn(hashPartitioner);
  }

  @Test(expected = IllegalStateException.class)
  public void mustCallWriteBeforeSuccessfulStop() throws IOException {
    setUpSparkContextPrivate();
    createWriter(false).stop(true);
  }

  @Test
  public void doNotNeedToCallWriteBeforeUnsuccessfulStop() throws IOException {
    setUpSparkContextPrivate();
    createWriter(false).stop(false);
  }

  static class PandaException extends RuntimeException {}

  @Test(expected = PandaException.class)
  public void writeFailurePropagates() throws Exception {
    setUpSparkContextPrivate();
    class BadRecords extends scala.collection.AbstractIterator<Product2<Object, Object>> {
      @Override
      public boolean hasNext() {
        throw new PandaException();
      }

      @Override
      public Product2<Object, Object> next() {
        return null;
      }
    }
    final RemoteUnsafeShuffleWriter<Object, Object> writer = createWriter(true);
    writer.write(new BadRecords());
  }

  @Test
  public void writeEmptyIterator() throws Exception {
    setUpSparkContextPrivate();
    final RemoteUnsafeShuffleWriter<Object, Object> writer = createWriter(true);
    writer.write(Collections.emptyIterator());
    final Option<MapStatus> mapStatus = writer.stop(true);
    assertTrue(mapStatus.isDefined());
    assertTrue(fs.exists(mergedOutputFile));
    assertArrayEquals(new long[NUM_PARTITITONS], partitionSizesInMergedFile);
    assertEquals(0, taskMetrics.shuffleWriteMetrics().recordsWritten());
    assertEquals(0, taskMetrics.shuffleWriteMetrics().bytesWritten());
    assertEquals(0, taskMetrics.diskBytesSpilled());
    assertEquals(0, taskMetrics.memoryBytesSpilled());
  }

  @Test
  public void writeWithoutSpilling() throws Exception {
    setUpSparkContextPrivate();
    // In this example, each partition should have exactly one record:
    final ArrayList<Product2<Object, Object>> dataToWrite = new ArrayList<>();
    for (int i = 0; i < NUM_PARTITITONS; i++) {
      dataToWrite.add(new Tuple2<>(i, i));
    }
    final RemoteUnsafeShuffleWriter<Object, Object> writer = createWriter(true);
    writer.write(dataToWrite.iterator());
    final Option<MapStatus> mapStatus = writer.stop(true);
    assertTrue(mapStatus.isDefined());
    assertTrue(fs.exists(mergedOutputFile));

    long sumOfPartitionSizes = 0;
    for (long size : partitionSizesInMergedFile) {
      // All partitions should be the same size:
      assertEquals(partitionSizesInMergedFile[0], size);
      sumOfPartitionSizes += size;
    }
    assertEquals(fs.getFileStatus(mergedOutputFile).getLen(), sumOfPartitionSizes);
    assertEquals(HashMultiset.create(dataToWrite), HashMultiset.create(readRecordsFromFile()));
    assertSpillFilesWereCleanedUp();
    ShuffleWriteMetrics shuffleWriteMetrics = taskMetrics.shuffleWriteMetrics();
    assertEquals(dataToWrite.size(), shuffleWriteMetrics.recordsWritten());
    assertEquals(0, taskMetrics.diskBytesSpilled());
    assertEquals(0, taskMetrics.memoryBytesSpilled());
    assertEquals(fs.getFileStatus(mergedOutputFile).getLen(), shuffleWriteMetrics.bytesWritten());
  }

  @Test
  public void mergeSpillsWithTransferToAndLZF() throws Exception {
    testMergingSpills(true, LZFCompressionCodec.class.getName(), false);
  }

  @Test
  public void mergeSpillsWithFileStreamAndLZF() throws Exception {
    testMergingSpills(false, LZFCompressionCodec.class.getName(), false);
  }

  @Test
  public void mergeSpillsWithTransferToAndLZ4() throws Exception {
    testMergingSpills(true, LZ4CompressionCodec.class.getName(), false);
  }

  @Test
  public void mergeSpillsWithFileStreamAndLZ4() throws Exception {
    testMergingSpills(false, LZ4CompressionCodec.class.getName(), false);
  }

  @Test
  public void mergeSpillsWithTransferToAndSnappy() throws Exception {
    testMergingSpills(true, SnappyCompressionCodec.class.getName(), false);
  }

  @Test
  public void mergeSpillsWithFileStreamAndSnappy() throws Exception {
    testMergingSpills(false, SnappyCompressionCodec.class.getName(), false);
  }

  @Test
  public void mergeSpillsWithTransferToAndNoCompression() throws Exception {
    testMergingSpills(true, null, false);
  }

  @Test
  public void mergeSpillsWithFileStreamAndNoCompression() throws Exception {
    testMergingSpills(false, null, false);
  }

  @Test
  public void mergeSpillsWithCompressionAndEncryption() throws Exception {
    // This should actually be translated to a "file stream merge" internally, just have the
    // test to make sure that it's the case.
    testMergingSpills(true, LZ4CompressionCodec.class.getName(), true);
  }

  @Test
  public void mergeSpillsWithFileStreamAndCompressionAndEncryption() throws Exception {
    testMergingSpills(false, LZ4CompressionCodec.class.getName(), true);
  }

  @Test
  public void mergeSpillsWithCompressionAndEncryptionSlowPath() throws Exception {
    conf.set("spark.shuffle.unsafe.fastMergeEnabled", "false");
    testMergingSpills(false, LZ4CompressionCodec.class.getName(), true);
  }

  @Test
  public void mergeSpillsWithEncryptionAndNoCompression() throws Exception {
    // This should actually be translated to a "file stream merge" internally, just have the
    // test to make sure that it's the case.
    testMergingSpills(true, null, true);
  }

  @Test
  public void mergeSpillsWithFileStreamAndEncryptionAndNoCompression() throws Exception {
    testMergingSpills(false, null, true);
  }

  @Test
  public void writeEnoughDataToTriggerSpill() throws Exception {
    setUpSparkContextPrivate();
    memoryManager.limit(PackedRecordPointer.MAXIMUM_PAGE_SIZE_BYTES);
    final RemoteUnsafeShuffleWriter<Object, Object> writer = createWriter(false);
    final ArrayList<Product2<Object, Object>> dataToWrite = new ArrayList<>();
    final byte[] bigByteArray = new byte[PackedRecordPointer.MAXIMUM_PAGE_SIZE_BYTES / 10];
    for (int i = 0; i < 10 + 1; i++) {
      dataToWrite.add(new Tuple2<>(i, bigByteArray));
    }
    writer.write(dataToWrite.iterator());
    assertEquals(2, spillFilesCreated.size());
    writer.stop(true);
    readRecordsFromFile();
    assertSpillFilesWereCleanedUp();
    ShuffleWriteMetrics shuffleWriteMetrics = taskMetrics.shuffleWriteMetrics();
    assertEquals(dataToWrite.size(), shuffleWriteMetrics.recordsWritten());
    assertThat(taskMetrics.diskBytesSpilled(), greaterThan(0L));
    assertThat(
        taskMetrics.diskBytesSpilled(), lessThan(fs.getFileStatus(mergedOutputFile).getLen()));
    assertThat(taskMetrics.memoryBytesSpilled(), greaterThan(0L));
    assertEquals(fs.getFileStatus(mergedOutputFile).getLen(), shuffleWriteMetrics.bytesWritten());
  }

  @Test
  public void writeEnoughRecordsToTriggerSortBufferExpansionAndSpillRadixOff() throws Exception {
    conf.set("spark.shuffle.sort.useRadixSort", "false");
    setUpSparkContextPrivate();
    writeEnoughRecordsToTriggerSortBufferExpansionAndSpill();
    assertEquals(2, spillFilesCreated.size());
  }

  @Test
  public void writeEnoughRecordsToTriggerSortBufferExpansionAndSpillRadixOn() throws Exception {
    conf.set("spark.shuffle.sort.useRadixSort", "true");
    setUpSparkContextPrivate();
    writeEnoughRecordsToTriggerSortBufferExpansionAndSpill();
    assertEquals(3, spillFilesCreated.size());
  }

  private void writeEnoughRecordsToTriggerSortBufferExpansionAndSpill() throws Exception {
    memoryManager.limit(UnsafeShuffleWriter.DEFAULT_INITIAL_SER_BUFFER_SIZE * 16);
    final RemoteUnsafeShuffleWriter<Object, Object> writer = createWriter(false);
    final ArrayList<Product2<Object, Object>> dataToWrite = new ArrayList<>();
    for (int i = 0; i < UnsafeShuffleWriter.DEFAULT_INITIAL_SER_BUFFER_SIZE + 1; i++) {
      dataToWrite.add(new Tuple2<>(i, i));
    }
    writer.write(dataToWrite.iterator());
    writer.stop(true);
    readRecordsFromFile();
    assertSpillFilesWereCleanedUp();
    ShuffleWriteMetrics shuffleWriteMetrics = taskMetrics.shuffleWriteMetrics();
    assertEquals(dataToWrite.size(), shuffleWriteMetrics.recordsWritten());
    assertThat(taskMetrics.diskBytesSpilled(), greaterThan(0L));
    assertThat(
        taskMetrics.diskBytesSpilled(), lessThan(fs.getFileStatus(mergedOutputFile).getLen()));
    assertThat(taskMetrics.memoryBytesSpilled(), greaterThan(0L));
    assertEquals(fs.getFileStatus(mergedOutputFile).getLen(), shuffleWriteMetrics.bytesWritten());
  }

  @Test
  public void writeRecordsThatAreBiggerThanDiskWriteBufferSize() throws Exception {
    setUpSparkContextPrivate();
    final RemoteUnsafeShuffleWriter<Object, Object> writer = createWriter(false);
    final ArrayList<Product2<Object, Object>> dataToWrite = new ArrayList<>();
    final byte[] bytes = new byte[(int) (ShuffleExternalSorter.DISK_WRITE_BUFFER_SIZE * 2.5)];
    new Random(42).nextBytes(bytes);
    dataToWrite.add(new Tuple2<>(1, ByteBuffer.wrap(bytes)));
    writer.write(dataToWrite.iterator());
    writer.stop(true);
    assertEquals(HashMultiset.create(dataToWrite), HashMultiset.create(readRecordsFromFile()));
    assertSpillFilesWereCleanedUp();
  }

  @Test
  public void writeRecordsThatAreBiggerThanMaxRecordSize() throws Exception {
    setUpSparkContextPrivate();
    final RemoteUnsafeShuffleWriter<Object, Object> writer = createWriter(false);
    final ArrayList<Product2<Object, Object>> dataToWrite = new ArrayList<>();
    dataToWrite.add(new Tuple2<>(1, ByteBuffer.wrap(new byte[1])));
    // We should be able to write a record that's right _at_ the max record size
    final byte[] atMaxRecordSize = new byte[(int) taskMemoryManager.pageSizeBytes() - 4];
    new Random(42).nextBytes(atMaxRecordSize);
    dataToWrite.add(new Tuple2<>(2, ByteBuffer.wrap(atMaxRecordSize)));
    // Inserting a record that's larger than the max record size
    final byte[] exceedsMaxRecordSize = new byte[(int) taskMemoryManager.pageSizeBytes()];
    new Random(42).nextBytes(exceedsMaxRecordSize);
    dataToWrite.add(new Tuple2<>(3, ByteBuffer.wrap(exceedsMaxRecordSize)));
    writer.write(dataToWrite.iterator());
    writer.stop(true);
    assertEquals(HashMultiset.create(dataToWrite), HashMultiset.create(readRecordsFromFile()));
    assertSpillFilesWereCleanedUp();
  }

  @Test
  public void spillFilesAreDeletedWhenStoppingAfterError() throws IOException {
    setUpSparkContextPrivate();
    final RemoteUnsafeShuffleWriter<Object, Object> writer = createWriter(false);
    writer.insertRecordIntoSorter(new Tuple2<>(1, 1));
    writer.insertRecordIntoSorter(new Tuple2<>(2, 2));
    writer.forceSorterToSpill();
    writer.insertRecordIntoSorter(new Tuple2<>(2, 2));
    writer.stop(false);
    assertSpillFilesWereCleanedUp();
  }

  @Test
  public void testPeakMemoryUsed() throws Exception {
    setUpSparkContextPrivate();
    final long recordLengthBytes = 8;
    final long pageSizeBytes = 256;
    final long numRecordsPerPage = pageSizeBytes / recordLengthBytes;
    taskMemoryManager = spy(taskMemoryManager);
    when(taskMemoryManager.pageSizeBytes()).thenReturn(pageSizeBytes);
    final RemoteUnsafeShuffleWriter<Object, Object> writer =
        new RemoteUnsafeShuffleWriter<>(
            blockManager,
            shuffleBlockResolver,
            taskMemoryManager,
            new SerializedShuffleHandle<>(0,  shuffleDep),
            0, // map id
            taskContext,
            conf,
            metrics);

    // Peak memory should be monotonically increasing. More specifically, every time
    // we allocate a new page it should increase by exactly the size of the page.
    long previousPeakMemory = writer.getPeakMemoryUsedBytes();
    long newPeakMemory;
    try {
      for (int i = 0; i < numRecordsPerPage * 10; i++) {
        writer.insertRecordIntoSorter(new Tuple2<Object, Object>(1, 1));
        newPeakMemory = writer.getPeakMemoryUsedBytes();
        if (i % numRecordsPerPage == 0) {
          // The first page is allocated in constructor, another page will be allocated after
          // every numRecordsPerPage records (peak memory should change).
          assertEquals(previousPeakMemory + pageSizeBytes, newPeakMemory);
        } else {
          assertEquals(previousPeakMemory, newPeakMemory);
        }
        previousPeakMemory = newPeakMemory;
      }

      // Spilling should not change peak memory
      writer.forceSorterToSpill();
      newPeakMemory = writer.getPeakMemoryUsedBytes();
      assertEquals(previousPeakMemory, newPeakMemory);
      for (int i = 0; i < numRecordsPerPage; i++) {
        writer.insertRecordIntoSorter(new Tuple2<Object, Object>(1, 1));
      }
      newPeakMemory = writer.getPeakMemoryUsedBytes();
      assertEquals(previousPeakMemory, newPeakMemory);

      // Closing the writer should not change peak memory
      writer.closeAndWriteOutput();
      newPeakMemory = writer.getPeakMemoryUsedBytes();
      assertEquals(previousPeakMemory, newPeakMemory);
    } finally {
      writer.stop(false);
    }
  }

  private RemoteUnsafeShuffleWriter<Object, Object> createWriter(boolean transferToEnabled)
      throws IOException {
    conf.set("spark.file.transferTo", String.valueOf(transferToEnabled));
    return new RemoteUnsafeShuffleWriter<>(
        blockManager,
        shuffleBlockResolver,
        taskMemoryManager,
        new SerializedShuffleHandle<>(0, shuffleDep),
        1, // map id
        taskContext,
        conf,
        metrics);
  }

  private List<Tuple2<Object, Object>> readRecordsFromFile() throws IOException {
    final ArrayList<Tuple2<Object, Object>> recordsList = new ArrayList<>();
    long startOffset = 0;
    for (int i = 0; i < NUM_PARTITITONS; i++) {
      final long partitionSize = partitionSizesInMergedFile[i];
      if (partitionSize > 0) {
        InputStream fin = fs.open(mergedOutputFile);
        ((FSDataInputStream) fin).seek(startOffset);
        InputStream in = new LimitedInputStream(fin, partitionSize);
        in = SparkEnv.get().serializerManager().wrapForEncryption(in);
        if (conf.getBoolean("spark.shuffle.compress", true)) {
          in = CompressionCodec$.MODULE$.createCodec(conf).compressedInputStream(in);
        }
        DeserializationStream recordsStream = serializer.newInstance().deserializeStream(in);
        Iterator<Tuple2<Object, Object>> records = recordsStream.asKeyValueIterator();
        while (records.hasNext()) {
          Tuple2<Object, Object> record = records.next();
          assertEquals(i, hashPartitioner.getPartition(record._1()));
          recordsList.add(record);
        }
        recordsStream.close();
        startOffset += partitionSize;
      }
    }
    return recordsList;
  }

  private void testMergingSpills(
      final boolean transferToEnabled, String compressionCodecName, boolean encrypt)
      throws Exception {
    if (compressionCodecName != null) {
      conf.set("spark.shuffle.compress", "true");
      conf.set("spark.io.compression.codec", compressionCodecName);
    } else {
      conf.set("spark.shuffle.compress", "false");
    }
    conf.set(org.apache.spark.internal.config.package$.MODULE$.IO_ENCRYPTION_ENABLED(), encrypt);

    setUpSparkContextPrivate();
    testMergingSpills(transferToEnabled, encrypt);
  }

  private void testMergingSpills(boolean transferToEnabled, boolean encrypted) throws IOException {
    final RemoteUnsafeShuffleWriter<Object, Object> writer = createWriter(transferToEnabled);
    final ArrayList<Product2<Object, Object>> dataToWrite = new ArrayList<>();
    for (int i : new int[] {1, 2, 3, 4, 4, 2}) {
      dataToWrite.add(new Tuple2<>(i, i));
    }
    writer.insertRecordIntoSorter(dataToWrite.get(0));
    writer.insertRecordIntoSorter(dataToWrite.get(1));
    writer.insertRecordIntoSorter(dataToWrite.get(2));
    writer.insertRecordIntoSorter(dataToWrite.get(3));
    writer.forceSorterToSpill();
    writer.insertRecordIntoSorter(dataToWrite.get(4));
    writer.insertRecordIntoSorter(dataToWrite.get(5));
    writer.closeAndWriteOutput();
    final Option<MapStatus> mapStatus = writer.stop(true);
    assertTrue(mapStatus.isDefined());
    assertTrue(fs.exists(mergedOutputFile));
    assertEquals(2, spillFilesCreated.size());

    long sumOfPartitionSizes = 0;
    for (long size : partitionSizesInMergedFile) {
      sumOfPartitionSizes += size;
    }

    assertEquals(sumOfPartitionSizes, fs.getFileStatus(mergedOutputFile).getLen());

    assertEquals(HashMultiset.create(dataToWrite), HashMultiset.create(readRecordsFromFile()));
    assertSpillFilesWereCleanedUp();
    ShuffleWriteMetrics shuffleWriteMetrics = taskMetrics.shuffleWriteMetrics();
    assertEquals(dataToWrite.size(), shuffleWriteMetrics.recordsWritten());
    assertThat(taskMetrics.diskBytesSpilled(), greaterThan(0L));
    assertThat(
        taskMetrics.diskBytesSpilled(), lessThan(fs.getFileStatus(mergedOutputFile).getLen()));
    assertThat(taskMetrics.memoryBytesSpilled(), greaterThan(0L));
    assertEquals(fs.getFileStatus(mergedOutputFile).getLen(), shuffleWriteMetrics.bytesWritten());
  }

  private void assertSpillFilesWereCleanedUp() throws IOException {
    for (Path spillFile : spillFilesCreated) {
      assertFalse(
          "Spill file " + spillFile.toString() + " was not cleaned up", fs.exists(spillFile));
    }
  }
}

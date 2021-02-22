package org.apache.spark.shuffle.daos;

import io.daos.obj.DaosObject;
import org.apache.spark.SparkConf;
import org.apache.spark.launcher.SparkLauncher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class IOManagerSync extends IOManager {

  private BoundThreadExecutors readerExes;

  private BoundThreadExecutors writerExes;

  private DaosReader.ReaderConfig readerConfig;

  private DaosWriter.WriterConfig writerConfig;

  private Map<DaosReader, Integer> readerMap = new ConcurrentHashMap<>();

  private Map<DaosWriter, Integer> writerMap = new ConcurrentHashMap<>();

  private Logger logger = LoggerFactory.getLogger(IOManagerSync.class);

  public IOManagerSync(SparkConf conf, Map<String, DaosObject> objectMap) {
    super(conf, objectMap);
    readerConfig = new DaosReader.ReaderConfig();
    writerConfig = new DaosWriter.WriterConfig();
    readerExes = createReaderExes();
    writerExes = createWriterExes();
  }

  private BoundThreadExecutors createWriterExes() {
    if (writerConfig.isFromOtherThreads()) {
      BoundThreadExecutors executors;
      int threads = writerConfig.getThreads();
      if (threads == -1) {
        threads = conf.getInt(SparkLauncher.EXECUTOR_CORES, 1);
      }
      executors = new BoundThreadExecutors("write_executors", threads,
          new DaosWriterSync.WriteThreadFactory());
      logger.info("created BoundThreadExecutors with " + threads + " threads for write");
      return executors;
    }
    return null;
  }

  private BoundThreadExecutors createReaderExes() {
    if (readerConfig.isFromOtherThread()) {
      BoundThreadExecutors executors;
      int threads = (int)conf.get(package$.MODULE$.SHUFFLE_DAOS_READ_THREADS());
      if (threads == -1) {
        threads = conf.getInt(SparkLauncher.EXECUTOR_CORES, 1);
      }
      executors = new BoundThreadExecutors("read_executors", threads,
          new DaosReaderSync.ReadThreadFactory());
      logger.info("created BoundThreadExecutors with " + threads + " threads for read");
      return executors;
    }
    return null;
  }

  @Override
  public DaosWriterSync getDaosWriter(int numPartitions, int shuffleId, long mapId) throws IOException {
    long appId = parseAppId(conf.getAppId());
    if (logger.isDebugEnabled()) {
      logger.debug("getting daoswriter for app id: " + appId + ", shuffle id: " + shuffleId + ", map id: " + mapId +
          ", numPartitions: " + numPartitions);
    }
    DaosWriterSync.WriteParam param = new DaosWriterSync.WriteParam();
    param.numPartitions(numPartitions)
        .shuffleId(shuffleId)
        .mapId(mapId)
        .config(writerConfig);
    DaosWriterSync writer = new DaosWriterSync(getObject(appId, shuffleId), param,
        writerExes == null ? null : writerExes.nextExecutor());
    writer.setWriterMap(writerMap);
    return writer;
  }

  @Override
  DaosReader getDaosReader(int shuffleId) throws IOException {
    long appId = parseAppId(conf.getAppId());
    if (logger.isDebugEnabled()) {
      logger.debug("getting daosreader for app id: " + appId + ", shuffle id: " + shuffleId);
    }
    DaosReaderSync reader = new DaosReaderSync(getObject(appId, shuffleId), readerConfig,
        readerExes == null ? null : readerExes.nextExecutor());
    reader.setReaderMap(readerMap);
    return reader;
  }

  @Override
  void close() throws IOException {
    if (readerExes != null) {
      readerExes.stop();
      readerExes = null;
    }
    readerMap.keySet().forEach(r -> r.close(true));
    readerMap.clear();
    if (writerExes != null) {
      writerExes.stop();
      writerExes = null;
    }
    writerMap.keySet().forEach(r -> r.close());
    writerMap.clear();
    objClient.forceClose();
  }
}

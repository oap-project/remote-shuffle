package org.apache.spark.shuffle.daos;

import io.daos.obj.DaosObject;
import org.apache.spark.SparkConf;

import java.io.IOException;
import java.util.Map;

public class IOManagerAsync extends IOManager {

  public IOManagerAsync(SparkConf conf, Map<String, DaosObject> objectMap) {
    super(conf, objectMap);
  }

  @Override
  DaosWriter getDaosWriter(int numPartitions, int shuffleId, long mapId) throws IOException {
    return null;
  }

  @Override
  DaosReader getDaosReader(int shuffleId) throws IOException {
    return null;
  }

  @Override
  void close() throws IOException {

  }
}

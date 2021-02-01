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

import io.daos.obj.DaosObjClient;
import io.daos.obj.DaosObject;
import io.daos.obj.DaosObjectException;
import io.daos.obj.DaosObjectId;
import org.apache.spark.SparkConf;

import java.io.IOException;
import java.util.Map;

public abstract class IOManager {

  protected Map<String, DaosObject> objectMap;

  protected SparkConf conf;

  protected DaosObjClient objClient;

  protected IOManager(SparkConf conf, Map<String, DaosObject> objectMap) {
    this.conf = conf;
    this.objectMap = objectMap;
  }

  private String getKey(long appId, int shuffleId) {
    return appId + "" + shuffleId;
  }

  protected static long parseAppId(String appId) {
    return Long.valueOf(appId.replaceAll("\\D", ""));
  }

  protected DaosObject getObject(long appId, int shuffleId) throws DaosObjectException {
    String key = getKey(appId, shuffleId);
    DaosObject object = objectMap.get(key);
    if (object == null) {
      DaosObjectId id = new DaosObjectId(appId, shuffleId);
      id.encode();
      object = objClient.getObject(id);
      objectMap.putIfAbsent(key, object);
      DaosObject activeObject = objectMap.get(key);
      if (activeObject != object) { // release just created DaosObject
        object.close();
        object = activeObject;
      }
    }
    // open just once in multiple threads
    if (!object.isOpen()) {
      synchronized (object) {
        object.open();
      }
    }
    return object;
  }

  public void setObjClient(DaosObjClient objClient) {
    this.objClient = objClient;
  }

  abstract DaosWriter getDaosWriter(int numPartitions, int shuffleId, long mapId) throws IOException;

  abstract DaosReader getDaosReader(int shuffleId) throws IOException;

  abstract void close() throws IOException;
}

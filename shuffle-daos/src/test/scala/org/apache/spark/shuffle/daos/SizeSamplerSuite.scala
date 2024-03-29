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

package org.apache.spark.shuffle.daos

import scala.util.Random

import org.apache.spark.SparkFunSuite

class SizeSamplerSuite extends SparkFunSuite {

  test("test sample AppendOnlyMap by update") {
    val stat = new SampleStat
    var grew = false
    val map = new SizeSamplerAppendOnlyMap[Int, Int](stat) {
      override def growTable(): Unit = {
        super.growTable()
        grew = true
      }
    }
    (1 to 65).foreach {
      i => map.update(i, i)
        assert(i === stat.numUpdates)
        assert(stat.lastNumUpdates <= stat.numUpdates)
        assert(stat.nextSampleNum >= stat.numUpdates)
        if (i == 15) {
          assert(stat.nextSampleNum === 17)
        }
        if (i == 45) {
          assert(grew === true)
        }
    }
  }

  test("test sample AppendOnlyMap by changeValue") {
    val stat = new SampleStat
    var grew = false;
    val map = new SizeSamplerAppendOnlyMap[Int, Int](stat) {
      override def growTable(): Unit = {
        super.growTable()
        grew = true
      }
    }
    val updateFun = (exist: Boolean, v: Int) => {
      new Random().nextInt(100) + v
    }
    (1 to 65).foreach {
      i => map.changeValue(i, updateFun)
        assert(i === stat.numUpdates)
        assert(stat.lastNumUpdates <= stat.numUpdates)
        assert(stat.nextSampleNum >= stat.numUpdates)
        if (i == 15) {
          assert(stat.nextSampleNum === 17)
        }
        if (i == 45) {
          assert(grew === true)
        }
    }
  }

  test("test sample PairBuffer by insert") {
    val stat = new SampleStat
    var grew = false;
    val buffer = new SizeSamplerPairBuffer[Int, Int](stat) {
      override def growArray(): Unit = {
        super.growArray()
        grew = true
      }
    }
    (1 to 73).foreach {
      i => buffer.insert(i, i)
      assert(i === stat.numUpdates)
      assert(stat.lastNumUpdates <= stat.numUpdates)
      assert(stat.nextSampleNum >= stat.numUpdates)
      if (i == 15) {
        assert(stat.nextSampleNum === 17)
      }
      if (i == 65) {
        assert(grew === true)
      }
    }
  }
}

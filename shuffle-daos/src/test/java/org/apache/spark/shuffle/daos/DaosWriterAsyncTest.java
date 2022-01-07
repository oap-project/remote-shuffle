package org.apache.spark.shuffle.daos;

import io.daos.obj.IODescUpdAsync;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.List;

public class DaosWriterAsyncTest {

  @Test
  public void testDescCacheNotFull() {
    DaosWriterAsync.AsyncDescCache cache = new DaosWriterAsync.AsyncDescCache(10) {
      public IODescUpdAsync newObject() {
        IODescUpdAsync desc = Mockito.mock(IODescUpdAsync.class);
        Mockito.when(desc.isReusable()).thenReturn(true);
        return desc;
      }
    };
    List<IODescUpdAsync> list = new ArrayList<>();
    try {
      for (int i = 0; i < 5; i++) {
        IODescUpdAsync desc = cache.get();
        Assert.assertTrue(desc.isReusable());
        Assert.assertEquals(i + 1, cache.getIdx());
        list.add(desc);
      }
      // test reuse
      IODescUpdAsync desc = list.remove(0);
      cache.put(desc);
      Assert.assertEquals(4, cache.getIdx());
      Assert.assertEquals(desc, cache.get());
      cache.put(desc);
      for (IODescUpdAsync d : list) {
        cache.put(d);
      }
      Assert.assertEquals(0, cache.getIdx());
    } finally {
      cache.release();
      list.forEach(d -> d.release());
    }
  }

  @Test
  public void testDescCacheFull() {
    DaosWriterAsync.AsyncDescCache cache = new DaosWriterAsync.AsyncDescCache(10) {
      public IODescUpdAsync newObject() {
        IODescUpdAsync desc = Mockito.mock(IODescUpdAsync.class);
        Mockito.when(desc.isReusable()).thenReturn(true);
        return desc;
      }
    };
    List<IODescUpdAsync> list = new ArrayList<>();
    Exception ee = null;
    try {
      for (int i = 0; i < 11; i++) {
        IODescUpdAsync desc = cache.get();
        Assert.assertTrue(desc.isReusable());
        Assert.assertEquals(i + 1, cache.getIdx());
        list.add(desc);
      }
    } catch (IllegalStateException e) {
      ee = e;
    }
    Assert.assertTrue(ee instanceof IllegalStateException);
    Assert.assertTrue(ee.getMessage().contains("cache is full"));
    Assert.assertTrue(cache.isFull());

    try {
      // test reuse
      IODescUpdAsync desc = list.remove(0);
      cache.put(desc);
      Assert.assertEquals(9, cache.getIdx());
      Assert.assertEquals(desc, cache.get());
      cache.put(desc);
      for (IODescUpdAsync d : list) {
        cache.put(d);
      }
      Assert.assertEquals(0, cache.getIdx());
      desc = cache.get();
      Assert.assertEquals(1, cache.getIdx());
      cache.put(desc);
      Assert.assertEquals(0, cache.getIdx());
    } finally {
      cache.release();
      list.forEach(d -> d.release());
    }
  }

  @Test
  public void testDescCachePut() {
    DaosWriterAsync.AsyncDescCache cache = new DaosWriterAsync.AsyncDescCache(10) {
      public IODescUpdAsync newObject() {
        IODescUpdAsync desc = Mockito.mock(IODescUpdAsync.class);
        Mockito.when(desc.isReusable()).thenReturn(true);
        return desc;
      }
    };
    List<IODescUpdAsync> list = new ArrayList<>();

    for (int i = 0; i < 10; i++) {
      IODescUpdAsync desc = cache.get();
      Assert.assertTrue(desc.isReusable());
      Assert.assertEquals(i + 1, cache.getIdx());
      list.add(desc);
    }
    Assert.assertTrue(cache.isFull());

    IODescUpdAsync desc = null;
    while (!list.isEmpty()) {
      desc = list.remove(0);
      cache.put(desc);
    }
    Exception ee = null;
    try {
      cache.put(desc);
    } catch (Exception e) {
      ee = e;
    } finally {
      cache.release();
      list.forEach(d -> d.release());
    }
    Assert.assertTrue(ee instanceof IllegalStateException);
    Assert.assertTrue(ee.getMessage().contains("more than actual"));
  }
}

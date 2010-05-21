/**
 * Copyright 2010 The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.regionserver;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestCase;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.ColumnCountGetFilter;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.regionserver.HRegion.RegionScanner;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class TestNSRE extends HBaseTestCase {
  static final Log LOG = LogFactory.getLog(TestHRegion.class);

  HRegion region = null;
  private final String DIR = HBaseTestingUtility.getTestDir() +
    "/TestHRegion/";

  private final int MAX_VERSIONS = 2;

  // Test names
  protected final byte[] tableName = Bytes.toBytes("testtable");;
  protected final byte[] qual1 = Bytes.toBytes("qual1");
  protected final byte[] qual2 = Bytes.toBytes("qual2");
  protected final byte[] qual3 = Bytes.toBytes("qual3");
  protected final byte[] value1 = Bytes.toBytes("value1");
  protected final byte[] value2 = Bytes.toBytes("value2");
  protected final byte [] row = Bytes.toBytes("rowA");

  /**
   * @see org.apache.hadoop.hbase.HBaseTestCase#setUp()
   */
  @Override
  protected void setUp() throws Exception {
    super.setUp();
  }

  //////////////////////////////////////////////////////////////////////////////
  // New tests that doesn't spin up a mini cluster but rather just test the
  // individual code pieces in the HRegion. Putting files locally in
  // /tmp/testtable
  //////////////////////////////////////////////////////////////////////////////

  public void testNoSuchRegion() throws IOException {
    HBaseConfiguration hc = initSplit();
    int numRows = 100;
    byte [][] families = {fam1, fam2, fam3};
    
    //Setting up region
    String method = this.getName();
    initHRegion(tableName, method, hc, families);

    // Put data in region
    final int startRow = 100;
    putData(startRow, numRows, qual1, families);
    putData(startRow, numRows, qual2, families);
    putData(startRow, numRows, qual3, families);
    // this.region.flushcache();
    final AtomicBoolean done = new AtomicBoolean(false);
    final AtomicInteger gets = new AtomicInteger(0);
    GetTillDoneOrException [] threads = new GetTillDoneOrException[10];
    try {
      // Set ten threads running concurrently getting from the region.
      for (int i = 0; i < threads.length / 2; i++) {
        threads[i] = new GetTillDoneOrException(i, Bytes.toBytes("" + startRow),
          done, gets);
        threads[i].setDaemon(true);
        threads[i].start();
      }
      // Artificially make the condition by setting closing flag explicitly.
      // I can't make the issue happen with a call to region.close().
      this.region.closing.set(true);
      for (int i = threads.length / 2; i < threads.length; i++) {
        threads[i] = new GetTillDoneOrException(i, Bytes.toBytes("" + startRow),
          done, gets);
        threads[i].setDaemon(true);
        threads[i].start();
      }
    } finally {
      if (this.region != null) {
        this.region.close();
        this.region.getLog().closeAndDelete();
      }
    }
    done.set(true);
    for (GetTillDoneOrException t: threads) {
      try {
        t.join();
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      if (t.e != null) {
        LOG.info("Exception=" + t.e);
        assertFalse("Found a NPE in " + t.getName(),
          t.e instanceof NullPointerException);
      }
    }
  }

  /*
   * Thread that does get on single row until 'done' flag is flipped.  If an
   * exception causes us to fail, it records it.
   */
  class GetTillDoneOrException extends Thread {
    private final Get g;
    private final AtomicBoolean done;
    private final AtomicInteger count;
    private Exception e;

    GetTillDoneOrException(final int i, final byte[] r, final AtomicBoolean d,
        final AtomicInteger c) {
      super("getter." + i);
      this.g = new Get(r);
      this.done = d;
      this.count = c;
    }

    @Override
    public void run() {
      while (!this.done.get()) {
        try {
          assertTrue(region.get(g, null).size() > 0);
          this.count.incrementAndGet();
        } catch (Exception e) {
          this.e = e;
          break;
        }
      }
    }
  }
  
  private void putData(int startRow, int numRows, byte [] qf,
      byte [] ...families)
  throws IOException {
    for(int i=startRow; i<startRow+numRows; i++) {
      Put put = new Put(Bytes.toBytes("" + i));
      for(byte [] family : families) {
        put.add(family, qf, null);
      }
      region.put(put);
    }
  }

  private void assertGet(final HRegion r, final byte [] family, final byte [] k)
  throws IOException {
    // Now I have k, get values out and assert they are as expected.
    Get get = new Get(k).addFamily(family).setMaxVersions();
    KeyValue [] results = r.get(get, null).raw();
    for (int j = 0; j < results.length; j++) {
      byte [] tmp = results[j].getValue();
      // Row should be equal to value every time.
      assertTrue(Bytes.equals(k, tmp));
    }
  }

  /*
   * Assert first value in the passed region is <code>firstValue</code>.
   * @param r
   * @param fs
   * @param firstValue
   * @throws IOException
   */
  private void assertScan(final HRegion r, final byte [] fs,
      final byte [] firstValue)
  throws IOException {
    byte [][] families = {fs};
    Scan scan = new Scan();
    for (int i = 0; i < families.length; i++) scan.addFamily(families[i]);
    InternalScanner s = r.getScanner(scan);
    try {
      List<KeyValue> curVals = new ArrayList<KeyValue>();
      boolean first = true;
      OUTER_LOOP: while(s.next(curVals)) {
        for (KeyValue kv: curVals) {
          byte [] val = kv.getValue();
          byte [] curval = val;
          if (first) {
            first = false;
            assertTrue(Bytes.compareTo(curval, firstValue) == 0);
          } else {
            // Not asserting anything.  Might as well break.
            break OUTER_LOOP;
          }
        }
      }
    } finally {
      s.close();
    }
  }

  protected HRegion [] split(final HRegion r, final byte [] splitRow)
  throws IOException {
    // Assert can get mid key from passed region.
    assertGet(r, fam3, splitRow);
    HRegion [] regions = r.splitRegion(splitRow);
    assertEquals(regions.length, 2);
    return regions;
  }

  private HBaseConfiguration initSplit() {
    HBaseConfiguration conf = new HBaseConfiguration();
    // Always compact if there is more than one store file.
    conf.setInt("hbase.hstore.compactionThreshold", 2);

    // Make lease timeout longer, lease checks less frequent
    conf.setInt("hbase.master.lease.thread.wakefrequency", 5 * 1000);

    conf.setInt(HConstants.HBASE_REGIONSERVER_LEASE_PERIOD_KEY, 10 * 1000);

    // Increase the amount of time between client retries
    conf.setLong("hbase.client.pause", 15 * 1000);

    // This size should make it so we always split using the addContent
    // below.  After adding all data, the first region is 1.3M
    conf.setLong("hbase.hregion.max.filesize", 1024 * 128);
    return conf;
  }

  private void initHRegion (byte [] tableName, String callingMethod,
    byte[] ... families)
  throws IOException {
    initHRegion(tableName, callingMethod, new HBaseConfiguration(), families);
  }

  private void initHRegion (byte [] tableName, String callingMethod,
    HBaseConfiguration conf, byte [] ... families)
  throws IOException{
    HTableDescriptor htd = new HTableDescriptor(tableName);
    for(byte [] family : families) {
      htd.addFamily(new HColumnDescriptor(family));
    }
    HRegionInfo info = new HRegionInfo(htd, null, null, false);
    Path path = new Path(DIR + callingMethod);
    region = HRegion.createHRegion(info, path, conf);
  }
}

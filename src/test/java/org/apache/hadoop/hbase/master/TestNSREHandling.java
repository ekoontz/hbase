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
package org.apache.hadoop.hbase.master;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.ipc.HMasterInterface;
import org.apache.hadoop.hbase.ipc.HRegionInterface;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.hbase.util.Writables;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWrapper;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Test transitions of state across the master.  Sets up the cluster once and
 * then runs a couple of tests.
 */

class NSREConnection implements HConnection {

    public ZooKeeperWrapper getZooKeeperWrapper()
            throws IOException {
          return null;
    }

    public HMasterInterface getMaster()
            throws MasterNotRunningException {
        return null;
    }

    public boolean isMasterRunning() {
        return true;
    }

    public boolean tableExists(final byte [] tableName)
    throws MasterNotRunningException {
        return true;
    }

    public boolean isTableEnabled(byte[] tableName)
                throws IOException {
        return true;
    }

    public boolean isTableDisabled(byte[] tableName)
            throws IOException {
        return true;
    }

    public boolean isTableAvailable(byte[] tableName)
            throws IOException {
        return true;
    }

    public HTableDescriptor[] listTables()
            throws IOException {
        return null;
    }

    public HTableDescriptor getHTableDescriptor(byte[] tableName)
        throws IOException {
        return null;
    }

    public HRegionLocation locateRegion(final byte [] tableName,
        final byte [] row)
        throws IOException {
        return null;
    }

    public void clearRegionCache() {
        return;
    }

    public HRegionLocation relocateRegion(final byte [] tableName,
        final byte [] row)
        throws IOException {
        return null;
    }

    public HRegionInterface getHRegionConnection(HServerAddress regionServer)
    throws IOException {
        return null;
    }

    public HRegionInterface getHRegionConnection(
        HServerAddress regionServer, boolean getMaster)
        throws IOException {
        return null;
    }

    public HRegionLocation getRegionLocation(byte [] tableName, byte [] row,
      boolean reload)
        throws IOException {
        return null;
    }

    public <T> T getRegionServerWithRetries(ServerCallable<T> callable)
        throws IOException, RuntimeException {
        return null;
    }

    public <T> T getRegionServerWithoutRetries(ServerCallable<T> callable)
    throws IOException, RuntimeException {
        return null;
    }

    public int processBatchOfRows(ArrayList<Put> list, byte[] tableName)
    throws IOException {
        return 0;
    }

    public int processBatchOfDeletes(List<Delete> list, byte[] tableName)
    throws IOException {
        return 0;
    }

    // the intention here is to ask the *wrong* regionserver
    // for a given region, which should cause the regionserver
    // to emit a NSRE.
    public void processBatchOfPuts(List<Put> list,
                                   final byte[] tableName, ExecutorService pool)
            throws IOException {

        for ( Put put : list ) {
                byte [] row = put.getRow();
                HRegionLocation loc = locateRegion(tableName, row);
                HServerAddress address = loc.getServerAddress();
                byte [] regionName = loc.getRegionInfo().getRegionName();
                return;
        }
    }

}

class NSRETable extends HTable {

    private final HConnection bad_connection;
    private List<Put> bad_writeBuffer;
    private byte[] bad_tableName;
    private ExecutorService bad_pool;


    public void flushCommits() throws IOException {
      // override method that tries to commit to wrong region server.
      bad_connection.processBatchOfPuts(bad_writeBuffer,bad_tableName, bad_pool);
    }

    public NSRETable(Configuration conf,final byte[] tableName)
            throws IOException {
        super(conf,tableName);
        bad_connection = new NSREConnection();
    }
    
}

public class TestNSREHandling {
  private static final Log LOG = LogFactory.getLog(TestMasterTransitions.class);
  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static final String TABLENAME = "nsre_test_table";
  private static final byte [][] FAMILIES = new byte [][] {Bytes.toBytes("a"),
    Bytes.toBytes("b"), Bytes.toBytes("c")};

  static boolean region_closed = false;

  /**
   * Start up a mini cluster and put a small table of many empty regions into it.
   * @throws Exception
   */
  @BeforeClass public static void beforeAllTests() throws Exception {
    TEST_UTIL.getConfiguration().setBoolean("dfs.support.append", true);
    // Start a cluster of two regionservers.
    TEST_UTIL.startMiniCluster(2);
    // Create a table of three families.  This will assign a region.
    TEST_UTIL.createTable(Bytes.toBytes(TABLENAME), FAMILIES);
    HTable t = new HTable(TEST_UTIL.getConfiguration(), TABLENAME);
    int countOfRegions = TEST_UTIL.createMultiRegions(t, getTestFamily());
    waitUntilAllRegionsAssigned(countOfRegions);
    addToEachStartKey(countOfRegions);
  }

  @AfterClass public static void afterAllTests() throws IOException {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Before public void setup() throws IOException {
    if (TEST_UTIL.getHBaseCluster().getLiveRegionServerThreads().size() < 2) {
      // Need at least two servers.
      LOG.info("Started new server=" +
        TEST_UTIL.getHBaseCluster().startRegionServer());
    }
  }

  /**
   * HBASE-2486: set up some scenarios to cause a region server to emit a NSRE,
   * and then assert that master has correctly responded in each scenario.
   */

  /* test for:
   * 3.a. (legitimate NSRE) : manipulate table object (used by client) : change apparent location of region A from correct regionserver R1 to
   *    incorrect regionserver R2.
   * 3.b. (transition) : ??
   * 3.c. (inconsistent NSRE): manipulate .META. table to change apparent location of region A from correct regionserver R1
   *       to incorrect regionserver R2.
   */



  @Test (timeout=300000) public void causeRSToEmitNSRE()
  throws Exception {
    LOG.info("Running causeRSToEmitNSRE()");
    MiniHBaseCluster cluster = TEST_UTIL.getHBaseCluster();
    final HMaster master = cluster.getMaster();

    int regionserverB_index = cluster.getServerWithMeta();
    // Figure the index of the server that is not server the .META.
    int regionserverA_index = -1;
    for (int i = 0; i < cluster.getRegionServerThreads().size(); i++) {
      if (i == regionserverB_index) continue;
      regionserverA_index = i;
      break;
    }

    final HRegionServer regionserverA = cluster.getRegionServer(regionserverA_index);
    final HRegionServer regionserverB = cluster.getRegionServer(regionserverB_index);

    // 1. Get a region on 'regionserverA'
    final HRegionInfo hri = regionserverA.getOnlineRegions().iterator().next().getRegionInfo();
    final String regionName = hri.getRegionNameAsString();

    // open a client connection to this region.
    NSRETable table = new NSRETable(TEST_UTIL.getConfiguration(),
                            Bytes.toBytes("nsre_test_table"));
    byte [] row = getStartKey(hri);
    Put p = new Put(row);
    p.add(getTestFamily(),getTestQualifier(),row);
    table.put(p);

    //Thread.sleep(10000);
    LOG.info("HBASE-2486: Done waiting.");
    assertEquals(42,42);
    LOG.info("EXITING TEST.");

  }

  /*
   * Wait until all rows in .META. have a non-empty info:server.  This means
   * all regions have been deployed, master has been informed and updated
   * .META. with the regions deployed server.
   * @param countOfRegions How many regions in .META.
   * @throws IOException
   */
  private static void waitUntilAllRegionsAssigned(final int countOfRegions)
  throws IOException {
    HTable meta = new HTable(TEST_UTIL.getConfiguration(),
      HConstants.META_TABLE_NAME);
    while (true) {
      int rows = 0;
      Scan scan = new Scan();
      scan.addColumn(HConstants.CATALOG_FAMILY, HConstants.SERVER_QUALIFIER);
      ResultScanner s = meta.getScanner(scan);
      for (Result r = null; (r = s.next()) != null;) {
        byte [] b =
          r.getValue(HConstants.CATALOG_FAMILY, HConstants.SERVER_QUALIFIER);
        if (b == null || b.length <= 0) break;
        rows++;
      }
      s.close();
      // If I get to here and all rows have a Server, then all have been assigned.
      if (rows == countOfRegions) break;
      LOG.info("Found=" + rows);
      Threads.sleep(1000); 
    }
  }

  /*
   * Add to each of the regions in .META. a value.  Key is the startrow of the
   * region (except its 'aaa' for first region).  Actual value is the row name.
   * @param expected
   * @return
   * @throws IOException
   */
  private static int addToEachStartKey(final int expected) throws IOException {
    HTable t = new HTable(TEST_UTIL.getConfiguration(), TABLENAME);
    HTable meta = new HTable(TEST_UTIL.getConfiguration(),
        HConstants.META_TABLE_NAME);
    int rows = 0;
    Scan scan = new Scan();
    scan.addColumn(HConstants.CATALOG_FAMILY, HConstants.REGIONINFO_QUALIFIER);
    ResultScanner s = meta.getScanner(scan);
    for (Result r = null; (r = s.next()) != null;) {
      byte [] b =
        r.getValue(HConstants.CATALOG_FAMILY, HConstants.REGIONINFO_QUALIFIER);
      if (b == null || b.length <= 0) break;
      HRegionInfo hri = Writables.getHRegionInfo(b);
      // If start key, add 'aaa'.
      byte [] row = getStartKey(hri);
      Put p = new Put(row);
      p.add(getTestFamily(), getTestQualifier(), row);
      t.put(p);
      rows++;
    }
    s.close();
    Assert.assertEquals(expected, rows);
    return rows;
  }

  /*
   * @return Count of rows in TABLENAME
   * @throws IOException
   */
  private static int count() throws IOException {
    HTable t = new HTable(TEST_UTIL.getConfiguration(), TABLENAME);
    int rows = 0;
    Scan scan = new Scan();
    ResultScanner s = t.getScanner(scan);
    for (Result r = null; (r = s.next()) != null;) {
      rows++;
    }
    s.close();
    LOG.info("Counted=" + rows);
    return rows;
  }

  /*
   * @param hri
   * @return Start key for hri (If start key is '', then return 'aaa'.
   */
  private static byte [] getStartKey(final HRegionInfo hri) {
    return Bytes.equals(HConstants.EMPTY_START_ROW, hri.getStartKey())?
        Bytes.toBytes("aaa"): hri.getStartKey();
  }

  private static byte [] getTestFamily() {
    return FAMILIES[0];
  }

  private static byte [] getTestQualifier() {
    return getTestFamily();
  }
}

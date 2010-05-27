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
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.BindException;
import java.util.Collection;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HMsg;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HServerAddress;
import org.apache.hadoop.hbase.HServerInfo;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.MiniHBaseCluster.MiniHBaseClusterRegionServer;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.hbase.util.Writables;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Test transitions of state across the master.  Sets up the cluster once and
 * then runs a couple of tests.
 */
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
   * Listener for regionserver events testing HBASE-2486:
   * Anti-entropy for No Such Region Exceptions.
   */
  static class HBase2486Listener implements RegionServerOperationListener {
    private final HRegionServer region_host;

    HBase2486Listener(final HRegionServer region_host) {
      this.region_host = region_host;
      return;
    }
 
    @Override
    public boolean process(HServerInfo serverInfo, HMsg incomingMsg) {
      LOG.info("HBASE-2486: PROCESS(1): " + serverInfo.toString());
      LOG.info("HBASE-2486: PROCESS(1): " + incomingMsg.toString());
      return true;
    }

    @Override
    public boolean process(RegionServerOperation op) throws IOException {
      LOG.info("HBASE-2486: PROCESS(2): " + op.toString());
      return true;
    }

    @Override
    public void processed(RegionServerOperation op) {
      LOG.info("HBASE-2486: PROCESSED: " + op.toString());
      // while() loop below in test will break out of the test
      // after this is true.
      region_closed = true;
      return;
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

    // add listener.
    MiniHBaseClusterRegionServer c_regionserverA =
      (MiniHBaseClusterRegionServer)regionserverA;
    HBase2486Listener listener = new HBase2486Listener(c_regionserverA);
    master.getRegionServerOperationQueue().
      registerRegionServerOperationListener(listener);

    // 1. Get a region on 'regionserverA'
    final HRegionInfo hri = regionserverA.getOnlineRegions().iterator().next().getRegionInfo();
    final String regionName = hri.getRegionNameAsString();

    // open a client connection to this region.
    HTable table = new HTable(TEST_UTIL.getConfiguration(),
                            Bytes.toBytes("nsre_test_table"));
    byte [] row = getStartKey(hri);
    Put p = new Put(row);
    p.add(getTestFamily(),getTestQualifier(),row);
    table.put(p);

    LOG.info("HBASE-2486: telling region server to close region.");

    // Close region 'hri' on server 'regionserverA'.
    // FIXME: do QUIESE, not MSG_REGION_CLOSE
    // Quiese will do a close but will also prevent re-assignment to the
    // this region server (which would avoid the NSRE that we want to 
    // trigger for our testing purposes.

    cluster.addMessageToSendRegionServer(c_regionserverA,
                                         new HMsg(HMsg.Type.MSG_REGION_CLOSE,hri,
                                                  Bytes.toBytes("Forcing close of hri.")));

    LOG.info("HBASE-2486: sent region; waiting for region server to close region..");

    // FIXME : use monitor or conditional variable or similar.
    while(true) {
      LOG.info("HBASE-2486: waiting for region to be closed..");
      Thread.sleep(1000);
      if (region_closed == true) {
        break;
      }
    }
    LOG.info("HBASE-2486: region closed.");

    LOG.info("HBASE-2486: attempting to cause region server to emit NSRE message..");
    // Try to access the region again, on the same region server: should cause a NSRE.
    Put p2 = new Put(row);
    p2.add(getTestFamily(),getTestQualifier(),row);
    table.put(p2);


    // Assert that the regionserver's NSRE message was received by the server.
    LOG.info("HBASE-2486: Wait for Master to receive NSRE message..");

    // How to check masters' receipt of NSRE message??
    // Is there a listener for masters?

    Thread.sleep(10000);
    LOG.info("HBASE-2486: Done waiting.");

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

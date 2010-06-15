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
  private static final Log LOG = LogFactory.getLog(TestNSREHandling.class);
  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static final String TABLENAME = "master_transitions";
  private static final byte [][] FAMILIES = new byte [][] {Bytes.toBytes("a")};

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
   * HBase2482 is about outstanding region openings.  If any are outstanding
   * when a regionserver goes down, then they'll never deploy.  They'll be
   * stuck in the regions-in-transition list for ever.  This listener looks
   * for a region opening HMsg and if its from the server passed on construction,
   * then we kill it.  It also looks out for a close message on the victim
   * server because that signifies start of the fireworks.
   */
  static class HBase2486Listener implements RegionServerOperationListener {
    private final HRegionServer victim;
    private boolean abortSent = false;
    // We closed regions on new server.
    private volatile boolean closed = false;
    // Copy of regions on new server
    private final Collection<HRegion> copyOfOnlineRegions;
    // This is the region that was in transition on the server we aborted. Test
    // passes if this region comes back online successfully.
    private HRegionInfo regionToFind;

    HBase2486Listener(final HRegionServer victim) {
      this.victim = victim;
      // Copy regions currently open on this server so I can notice when
      // there is a close.
      this.copyOfOnlineRegions =
        this.victim.getCopyOfOnlineRegionsSortedBySize().values();
    }
 
    @Override
    public boolean process(HServerInfo serverInfo, HMsg incomingMsg) {
      if (!victim.getServerInfo().equals(serverInfo) ||
          this.abortSent || !this.closed) {
        return true;
      }
      if (!incomingMsg.isType(HMsg.Type.MSG_REPORT_PROCESS_OPEN)) return true;
      // Save the region that is in transition so can test later it came back.
      this.regionToFind = incomingMsg.getRegionInfo();
      LOG.info("ABORTING " + this.victim + " because got a " +
        HMsg.Type.MSG_REPORT_PROCESS_OPEN + " on this server for " +
        incomingMsg.getRegionInfo().getRegionNameAsString());
      this.victim.abort();
      this.abortSent = true;
      return true;
    }

    @Override
    public boolean process(RegionServerOperation op) throws IOException {
      return true;
    }

    @Override
    public void processed(RegionServerOperation op) {
      if (this.closed || !(op instanceof ProcessRegionClose)) return;
      ProcessRegionClose close = (ProcessRegionClose)op;
      for (HRegion r: this.copyOfOnlineRegions) {
        if (r.getRegionInfo().equals(close.regionInfo)) {
          // We've closed one of the regions that was on the victim server.
          // Now can start testing for when all regions are back online again
          LOG.info("Found close of " +
            r.getRegionInfo().getRegionNameAsString() +
            "; setting close happened flag");
          this.closed = true;
          break;
        }
      }
    }
  }

  /**
   * Test behavior of master when a region server for a region it doesn't have.
   * @see <a href="https://issues.apache.org/jira/browse/HBASE-2486">HBASE-2486</a> 
   */
  @Test (timeout=300000) public void testNoSuchRegionServer2486() 
  throws Exception {
    LOG.info("Running testNoSuchRegionServer2486");
    MiniHBaseCluster cluster = TEST_UTIL.getHBaseCluster();
    if (cluster.getLiveRegionServerThreads().size() < 2) {
      // Need at least two servers.
      cluster.startRegionServer();
    }
    // Count how many regions are online.  They need to be all back online for
    // this test to succeed.
    int countOfMetaRegions = countOfMetaRegions();
    // Add a listener on the server.
    HMaster m = cluster.getMaster();
    // Start new regionserver.
    MiniHBaseClusterRegionServer hrs =
      (MiniHBaseClusterRegionServer)cluster.startRegionServer().getRegionServer();
    LOG.info("Started new regionserver: " + hrs.toString());
    // Wait until has some regions before proceeding.  Balancer will give it some.
    int minimumRegions =
      countOfMetaRegions/(cluster.getRegionServerThreads().size() * 2);
    while (hrs.getOnlineRegions().size() < minimumRegions) Threads.sleep(100);
    // Set the listener only after some regions have been opened on new server.
    HBase2486Listener listener = new HBase2486Listener(hrs);
    m.getRegionServerOperationQueue().
      registerRegionServerOperationListener(listener);
    try {
      // Go close all non-catalog regions on this new server
      closeAllNonCatalogRegions(cluster, hrs);
      // After all closes, add blocking message before the region opens start to
      // come in.
      cluster.addMessageToSendRegionServer(hrs,
        new HMsg(HMsg.Type.TESTING_MSG_BLOCK_RS));
      // Wait till one of the above close messages has an effect before we start
      // wait on all regions back online.
      while (!listener.closed) Threads.sleep(100);
      LOG.info("Past close");
      // Make sure the abort server message was sent.
      while(!listener.abortSent) Threads.sleep(100);
      LOG.info("Past abort send; waiting on all regions to redeploy");
      // Now wait for regions to come back online.
      assertRegionIsBackOnline(listener.regionToFind);
    } finally {
      m.getRegionServerOperationQueue().
        unregisterRegionServerOperationListener(listener);
    }
  }

  /*
   * @return Count of all non-catalog regions on the designated server
   */
  private int closeAllNonCatalogRegions(final MiniHBaseCluster cluster,
    final MiniHBaseCluster.MiniHBaseClusterRegionServer hrs)
  throws IOException {
    int countOfRegions = 0;
    for (HRegion r: hrs.getOnlineRegions()) {
      if (r.getRegionInfo().isMetaRegion()) continue;
      cluster.addMessageToSendRegionServer(hrs,
        new HMsg(HMsg.Type.MSG_REGION_CLOSE, r.getRegionInfo()));
      LOG.info("Sent close of " + r.getRegionInfo().getRegionNameAsString() +
        " on " + hrs.toString());
      countOfRegions++;
    }
    return countOfRegions;
  }

  private void assertRegionIsBackOnline(final HRegionInfo hri)
  throws IOException {
    // Region should have an entry in its startkey because of addRowToEachRegion.
    byte [] row = getStartKey(hri);
    HTable t = new HTable(TEST_UTIL.getConfiguration(), TABLENAME);
    Get g =  new Get(row);
    assertTrue((t.get(g)).size() > 0);
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
   * @return Count of regions in meta table.
   * @throws IOException
   */
  private static int countOfMetaRegions()
  throws IOException {
    HTable meta = new HTable(TEST_UTIL.getConfiguration(),
      HConstants.META_TABLE_NAME);
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
    return rows;
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

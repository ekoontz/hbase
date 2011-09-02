/*
 * Copyright 2011 The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hbase.coprocessor;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.JVMClusterUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperNodeTracker;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Tests unhandled exceptions thrown by coprocessors running on a regionserver..
 * Expected result is that the regionserver will abort with an informative
 * error message describing the set of its loaded coprocessors for crash
 * diagnosis. (HBASE-4014).
 */
public class TestRegionServerCoprocessorException {

  public boolean rsZKNodeWasDeleted = false;

  private class DeadRegionServerTracker extends ZooKeeperNodeTracker {

    private String rsNode;

    public DeadRegionServerTracker(ZooKeeperWatcher zkw, String rsNode,
                                   Abortable abortable) {
      super(zkw,rsNode,abortable);
      this.rsNode = rsNode;
    }

    @Override
    public synchronized void nodeDeleted(String path) {
      if (path.equals(rsNode)) {
        rsZKNodeWasDeleted = true;
      }
    }
  }

  private void writeToTable(HTable table) throws IOException {

  }

  private static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  private static ZooKeeperWatcher zkw = null;

  @BeforeClass
  public static void setupBeforeClass() throws Exception {
    // set configure to indicate which cp should be loaded
    Configuration conf = TEST_UTIL.getConfiguration();
    conf.set(CoprocessorHost.REGION_COPROCESSOR_CONF_KEY,
        BuggyRegionObserver.class.getName());
    conf.set("hbase.coprocessor.abortonerror", "true");
    TEST_UTIL.startMiniCluster(2);
  }

  @AfterClass
  public static void teardownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test(timeout=30000)
  public void testExceptionFromCoprocessorWhenCreatingTable()
      throws IOException {
    // Set watches on the zookeeper nodes for all of the regionservers in the
    // cluster. When we try to write to TEST_TABLE, the buggy coprocessor will
    // cause a NullPointerException, which will cause the regionserver (which
    // hosts the region we attempted to write to) to abort. In turn, this will
    // cause the nodeDeleted() method of the DeadRegionServer tracker to
    // execute, which will set the rsZKNodeDeleted flag to true, which will
    // pass this test.

    byte[] TEST_TABLE = Bytes.toBytes("observed_table");
    byte[] TEST_FAMILY = Bytes.toBytes("aaa");

    HTable table = TEST_UTIL.createTable(TEST_TABLE, TEST_FAMILY);
    TEST_UTIL.createMultiRegions(table, TEST_FAMILY);

    // Create a tracker for each RegionServer.
    // As a future improvement, should only be necessary to track a single
    // regionserver (the one hosting the region that we are going to attempt to
    // write to). For now, we don't know how to determine which RegionServer
    // this is, so we track all of them.
    List<JVMClusterUtil.RegionServerThread> threads =
        TEST_UTIL.getHBaseCluster().getRegionServerThreads();
    for(JVMClusterUtil.RegionServerThread rst : threads) {
      String rsNodeString = "/hbase/rs/" +
          rst.getRegionServer().getServerName();
      zkw = TEST_UTIL.getHBaseAdmin().getConnection().getZooKeeperWatcher();
      DeadRegionServerTracker regionServerTracker =
          new DeadRegionServerTracker(zkw, rsNodeString, new Abortable() {
          @Override
          public void abort(String why, Throwable e) {
            assertFalse("regionServerTracker failed because: " + why, true);
          }
        });
      regionServerTracker.start();
      // TODO: do we need to registerListener()? or is it already taken care of
      // in the super constructor?
      zkw.registerListener(regionServerTracker);
    }

    try {
      final byte[] ROW = Bytes.toBytes("bbb");
      Put put = new Put(ROW);
      put.add(TEST_FAMILY, ROW, ROW);
      table.put(put);
    } catch (IOException e) {
      assertFalse("put() failed: " + e,true);
    }

    // Wait up to 30 seconds for regionserver's ephemeral node to go away after
    // the regionserver aborts.
    for (int i = 0; i < 30; i++) {
      if (rsZKNodeWasDeleted == true) {
        break;
      }
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        assertFalse("InterruptedException while waiting for regionserver " +
            "zk node to be deleted.", true);
      }
    }
    assertTrue("RegionServer aborted on coprocessor exception, as expected.",
        rsZKNodeWasDeleted);
    TEST_UTIL.shutdownMiniCluster();
  }

    public static class BuggyRegionObserver extends SimpleRegionObserver {
    @Override
    public void prePut(final ObserverContext<RegionCoprocessorEnvironment> c,
                       final Map<byte[], List<KeyValue>> familyMap,
                       final boolean writeToWAL) {
      String tableName =
          c.getEnvironment().getRegion().getRegionInfo().getTableNameAsString();
      if (tableName.equals("observed_table")) {
        Integer i = null;
        i = i + 1;
      }
    }
}



}

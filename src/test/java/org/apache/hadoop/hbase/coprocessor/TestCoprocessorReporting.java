/*
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
package org.apache.hadoop.hbase.coprocessor;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HServerLoad;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * TestCoprocessorReporting: test cases to verify that loaded coprocessors on
 * master and regionservers are reported correctly.
 */
public class TestCoprocessorReporting {

  private static final byte[] TEST_TABLE = Bytes.toBytes("TestTable");
  private static final byte[] TEST_FAMILY = Bytes.toBytes("TestFamily");
  private static final byte[] TEST_QUALIFIER = Bytes.toBytes("TestQualifier");
  private static byte[] ROW = Bytes.toBytes("testRow");

  private static final int ROWSIZE = 20;
  private static final int rowSeparator1 = 5;
  private static final int rowSeparator2 = 12;
  private static byte[][] ROWS = makeN(ROW, ROWSIZE);

  private static HBaseTestingUtility util = new HBaseTestingUtility();
  private static MiniHBaseCluster cluster = null;

  private static Class coprocessor1 = ColumnAggregationEndpoint.class;
  private static Class coprocessor2 = GenericEndpoint.class;
  private static Class masterCoprocessor = BaseMasterObserver.class;

  @BeforeClass
  public static void setupBeforeClass() throws Exception {
    // set configure to indicate which cp should be loaded
    Configuration conf = util.getConfiguration();
    conf.setStrings(CoprocessorHost.REGION_COPROCESSOR_CONF_KEY,
        coprocessor1.getName(), coprocessor2.getName());

    conf.setStrings(CoprocessorHost.MASTER_COPROCESSOR_CONF_KEY,
        masterCoprocessor.getName());

    util.startMiniCluster(2);
    cluster = util.getMiniHBaseCluster();

    HTable table = util.createTable(TEST_TABLE, TEST_FAMILY);
    util.createMultiRegions(util.getConfiguration(), table, TEST_FAMILY,
                            new byte[][] { HConstants.EMPTY_BYTE_ARRAY,
                                ROWS[rowSeparator1], ROWS[rowSeparator2] });

    for (int i = 0; i < ROWSIZE; i++) {
      Put put = new Put(ROWS[i]);
      put.add(TEST_FAMILY, TEST_QUALIFIER, Bytes.toBytes(i));
      table.put(put);
    }

    // sleep here is an ugly hack to allow region transitions to finish
    Thread.sleep(5000);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    util.shutdownMiniCluster();
  }

  @Test
  public void testRegionServerCoprocessorsReported() {
    // HBASE 4070: Improve region server metrics to report loaded coprocessors
    // to master: verify that each regionserver is reporting the correct set of
    // loaded coprocessors.
    // TODO: test display of regionserver-level coprocessors
    // (e.g. SampleRegionWALObserver) versus region-level coprocessors
    // (e.g. GenericEndpoint), and
    // test CoprocessorHost.REGION_COPROCESSOR_CONF_KEY versus
    // CoprocessorHost.USER_REGION_COPROCESSOR_CONF_KEY.
    // Test enabling and disabling user tables to see if coprocessor display
    // changes as coprocessors are consequently loaded and unloaded.

    // We rely on the fact that getLoadedCoprocessors() will return a sorted
    // display of the coprocessors' names, so coprocessor1's name
    // "ColumnAggregationEndpoint" will appear before coprocessor2's name
    // "GenericEndpoint" because "C" is before "G" lexicographically.
    // Note the space [ ] after the comma in both constant strings:
    // must be present for success of this test.
    final String loadedCoprocessorsExpected =
      "[" + coprocessor1.getSimpleName() + ", " + coprocessor2.getSimpleName() + "]";
    for(Map.Entry<ServerName,HServerLoad> server :
        util.getMiniHBaseCluster().getMaster().getServerManager().getOnlineServers().entrySet()) {
      String[] regionServerCoprocessors = server.getValue().getLoadedCoprocessors();
      //assertTrue(regionServerCoprocessors.equals(loadedCoprocessorsExpected));
    }
  }

  @Test
  public void testMasterCoprocessorsReported() {
    // HBASE 4070: Improve region server metrics to report loaded coprocessors
    // to master: verify that the master is reporting the correct set of
    // loaded coprocessors.
    final String loadedMasterCoprocessorsVerify =
        "[" + masterCoprocessor.getSimpleName() + "]";
    String loadedMasterCoprocessors =
        util.getHBaseCluster().getMaster().getCoprocessors();
    assertEquals(loadedMasterCoprocessorsVerify, loadedMasterCoprocessors);
  }

  private static byte[][] makeN(byte[] base, int n) {
    byte[][] ret = new byte[n][];
    for (int i = 0; i < n; i++) {
      ret[i] = Bytes.add(base, Bytes.toBytes(String.format("%02d", i)));
    }
    return ret;
  }
}

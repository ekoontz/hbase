/*
 * Copyright 2009 The Apache Software Foundation
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

import org.apache.hadoop.hbase.HBaseClusterTestCase;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HServerAddress;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;

public class TestNSREHandling extends HBaseClusterTestCase {
   public void testHandleNSREInTransition()
   throws Exception {

     HTable meta = new HTable(HConstants.META_TABLE_NAME);
     HMaster master = this.cluster.getMaster();
     HServerAddress address = master.getMasterAddress();
     HTableDescriptor tableDesc = new HTableDescriptor(Bytes.toBytes("_MY_TABLE_"));

     // master.regionManager.onlineMetaRegions already contains first .META. region at key Bytes.toBytes("")
     byte[] startKey0 = Bytes.toBytes("f");
     byte[] endKey0 = Bytes.toBytes("h");
     HRegionInfo regionInfo0 = new HRegionInfo(tableDesc, startKey0, endKey0);

     // get region info for _MY_TABLE_.
     // 1. Put a region r (regionInfo0) into transition.

     // 2. Put a HMsg with a NSRE(r) in master message queue.

     // 3. See if server handles NSRE(r) correctly.


     assertEquals(42,42);
   }

   public void testHandleNSREOnDifferentRS()
     throws Exception {
       assertEquals(42,42);
   }

   public void testHandleNSREOnSameRS()
     throws Exception {
       assertEquals(42,42);
   }



}

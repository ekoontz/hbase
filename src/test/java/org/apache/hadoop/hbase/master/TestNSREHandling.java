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
   public void handleNSRE()
   throws Exception {
     HTable meta = new HTable(HConstants.META_TABLE_NAME);
     HMaster master = this.cluster.getMaster();
     HServerAddress address = master.getMasterAddress();

     RegionManager region_manager = master.getRegionManager();

     HTableDescriptor tableDesc = new HTableDescriptor(Bytes.toBytes("_MY_TABLE_"));
     HTableDescriptor metaTableDesc = meta.getTableDescriptor();
     // master.regionManager.onlineMetaRegions already contains first .META. region at key Bytes.toBytes("")
     byte[] startKey0 = Bytes.toBytes("f");
     byte[] endKey0 = Bytes.toBytes("h");
     HRegionInfo regionInfo0 = new HRegionInfo(tableDesc, startKey0, endKey0);

     // 1st .META. region will be something like .META.,,1253625700761
     HRegionInfo metaRegionInfo0 = new HRegionInfo(metaTableDesc, Bytes.toBytes(""), regionInfo0.getRegionName());
     MetaRegion meta0 = new MetaRegion(address, metaRegionInfo0);

     byte[] startKey1 = Bytes.toBytes("j");
     byte[] endKey1 = Bytes.toBytes("m");
     HRegionInfo regionInfo1 = new HRegionInfo(tableDesc, startKey1, endKey1);
     // 2nd .META. region will be something like .META.,_MY_TABLE_,f,1253625700761,1253625700761
     HRegionInfo metaRegionInfo1 = new HRegionInfo(metaTableDesc, regionInfo0.getRegionName(), regionInfo1.getRegionName());
     MetaRegion meta1 = new MetaRegion(address, metaRegionInfo1);


     // 3rd .META. region will be something like .META.,_MY_TABLE_,j,1253625700761,1253625700761
     HRegionInfo metaRegionInfo2 = new HRegionInfo(metaTableDesc, regionInfo1.getRegionName(), Bytes.toBytes(""));
     MetaRegion meta2 = new MetaRegion(address, metaRegionInfo2);

     byte[] startKeyX = Bytes.toBytes("h");
     byte[] endKeyX = Bytes.toBytes("j");
     HRegionInfo regionInfoX = new HRegionInfo(tableDesc, startKeyX, endKeyX);

     MetaRegion test_mr_0 = region_manager.getFirstMetaRegionForRegion(regionInfo0);
     MetaRegion test_mr_1 = region_manager.getFirstMetaRegionForRegion(regionInfo1);
     MetaRegion test_mr_x = region_manager.getFirstMetaRegionForRegion(regionInfoX);

     System.out.println("before manipulating online status..");

     region_manager.offlineMetaRegionWithStartKey(startKey0);
     region_manager.putMetaRegionOnline(meta0);
     region_manager.putMetaRegionOnline(meta1);
     region_manager.putMetaRegionOnline(meta2);

     test_mr_x = region_manager.getFirstMetaRegionForRegion(regionInfoX);

     byte[] a = metaRegionInfo1.getStartKey();
     byte[] b  = test_mr_x.getStartKey();


     test_mr_x = region_manager.getFirstMetaRegionForRegion(regionInfoX);

     assertEquals(metaRegionInfo1.getStartKey(),
       region_manager.getFirstMetaRegionForRegion(regionInfoX).getStartKey());
     assertEquals(metaRegionInfo1.getRegionName(),
      region_manager.getFirstMetaRegionForRegion(regionInfoX).getRegionName());
   }
}

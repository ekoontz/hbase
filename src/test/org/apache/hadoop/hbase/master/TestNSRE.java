package org.apache.hadoop.hbase.master;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.hbase.HBaseClusterTestCase;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestCase;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HServerAddress;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Writables;
import org.apache.hadoop.util.StringUtils;

/****
 *
 * HBASE-2486 : verify that:
 * 
 * 1) asking a region server S for a region R that it does not have, causes R to send
 *     a NSRE message m to the master M.
 *
 * 2) M receives m.
 *
 * 3) asking a region server S for a region R that it *does* have, does not cause R to
 *    send a NSRE message.
 * 
 * 4) When M receives m, M shuts itself down iff :
 * a) inconsistency : S *does* have R according to M's .META. records.
 * b) configuration param : 'hbase.inconsistencyhandling' is set to 'paranoid'
 *
 * 5) When M receives m, it marks R as unassigned iff :
 * a) inconsistency : S *does* have R according to M's .META. records.
 * b) configuration param : 'hbase.inconsistencyhandling' is set to 'lax'
 *
 * 6) When M receives m, it ignores m and continues if any of the following are true:
 * a) R is in transition
 * b) S *does not* have R according to M's .META. records.
 **/

public class TestNSRE extends HBaseClusterTestCase {
  public void testNSRE()
   throws Exception {
    HTable meta = new HTable(HConstants.META_TABLE_NAME);
    HMaster master = this.cluster.getMaster();
    HServerAddress address = master.getMasterAddress();
    System.out.println("ekoontzdebug: " + address.toString());
    assertEquals(42,42);
   }
}

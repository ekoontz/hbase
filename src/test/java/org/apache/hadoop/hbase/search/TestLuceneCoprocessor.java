package org.apache.hadoop.hbase.search;

/*
 * Copyright 2010 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Arrays;

import junit.framework.Assert;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.RegionCoprocessorHost;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.JVMClusterUtil;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestLuceneCoprocessor {
  static final Log LOG = LogFactory.getLog(TestLuceneCoprocessor.class);
  static final String DIR = "test/build/data/TestLuceneCoprocessor/";
  private static HBaseTestingUtility util = new HBaseTestingUtility();
  private static MiniHBaseCluster cluster = null;

  static byte[] headersFam = Bytes.toBytes("headers");
  static byte[] to = Bytes.toBytes("to");
  static byte[] bodyFam = Bytes.toBytes("body");
  static byte[][] families = new byte[][] { headersFam, bodyFam };
  static byte[] messages = Bytes.toBytes("messages");
  static byte[] tableName = Bytes.toBytes("lucene");

  @BeforeClass
  public static void setupBeforeClass() throws Exception {
    util.startMiniCluster(1);
    cluster = util.getMiniHBaseCluster();

    HTableDescriptor tableDesc = new HTableDescriptor(tableName);
    for (byte[] family : families) {
      tableDesc.addFamily(new HColumnDescriptor(family));
    }
    tableDesc.setValue("COPROCESSOR$1",
        String.format("/:%s:USER", LuceneCoprocessor.class.getName()));

    HBaseAdmin admin = util.getHBaseAdmin();
    admin.createTable(tableDesc);
    HTable table = new HTable(util.getConfiguration(), tableName);

    for (int x = 0; x < 20; x++) {
      byte[] row = Bytes.toBytes(Integer.toString(x));
      Put put = new Put(row);
      String s = "test hbase lucene";
      if (x % 2 == 0) {
        s += " apple";
      }
      put.add(headersFam, to, Bytes.toBytes(s));
      table.put(put);
    }
    // sleep here is an ugly hack to allow region transitions to finish
    Thread.sleep(5000);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    util.shutdownMiniCluster();
  }

  private void printDocs(HRegion region, IndexWriter writer) throws IOException {
    IndexReader reader = IndexReader.open(writer, true);
    HBaseIndexSearcher searcher = new HBaseIndexSearcher(reader, region);
    int num = reader.maxDoc();
    for (int x = 0; x < num; x++) {
      Document doc = reader.document(x);
      Document sdoc = searcher.doc(x);
      System.out.println("doc" + x + ":" + doc + " sdoc:" + sdoc);
    }
    reader.close();
  }

  @Test
  public void testRegionObserver() throws IOException {
    // System.out.println("testRegionObserver");
    for (JVMClusterUtil.RegionServerThread t : cluster.getRegionServerThreads()) {
      for (HRegionInfo r : t.getRegionServer().getOnlineRegions()) {
        if (!Arrays.equals(r.getTableDesc().getName(), tableName)) {
          continue;
        }
        RegionCoprocessorHost cph = t.getRegionServer()
            .getOnlineRegion(r.getRegionName()).getCoprocessorHost();
        HRegion region = t.getRegionServer().getOnlineRegion(r.getRegionName());
        LuceneCoprocessor c = (LuceneCoprocessor) cph
            .findCoprocessor(LuceneCoprocessor.class.getName());
        // System.out.println("LuceneCoprocessor:" + c);

        IndexReader reader = IndexReader.open(c.writer, true);
        try {
          HBaseIndexSearcher searcher = new HBaseIndexSearcher(reader, region);

          assertTrue(c.writer != null);

          TopDocs topDocs = searcher.search(new TermQuery(new Term("to",
              "lucene")), 100);
          Assert.assertEquals(20, topDocs.totalHits);

          topDocs = searcher
              .search(new TermQuery(new Term("to", "apple")), 100);
          Assert.assertEquals(10, topDocs.totalHits);
        } finally {
          reader.close();
        }
        // printDocs(region, c.writer);
      }
    }
  }
}

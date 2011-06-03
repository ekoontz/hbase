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

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Index;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.Field.TermVector;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LogByteSizeMergePolicy;
import org.apache.lucene.util.Version;
import org.apache.lucene.util._TestUtil;

public class TestHDFSDirectory extends TestCase {
  public IndexWriterConfig getConfig() {
    WhitespaceAnalyzer analyzer = new WhitespaceAnalyzer(Version.LUCENE_40);
    IndexWriterConfig config = new IndexWriterConfig(Version.LUCENE_40,
        analyzer);
    LogByteSizeMergePolicy mergePolicy = new LogByteSizeMergePolicy();
    mergePolicy.setUseCompoundFile(false);
    config.setMergePolicy(mergePolicy);
    config.setCodecProvider(new HDFSCodecProvider());
    return config;
  }

  /**
   * Use Lucene directly (rather than Solr) to test the HDFSDirectory.
   */
  public void testIndex() throws Exception {
    Configuration conf = new Configuration();
    MiniDFSCluster cluster = new MiniDFSCluster(conf, 2, true, null);
    FileSystem fileSys = cluster.getFileSystem();
    String dataDir = "/index";
    try {
      fileSys.mkdirs(new Path(dataDir));
      HDFSDirectory dir = new HDFSDirectory(fileSys, dataDir);
      IndexWriterConfig iwc = getConfig();
      IndexWriter writer = new IndexWriter(dir, iwc);
      for (int x = 0; x < 50; x++) {
        Document doc = createDocument(x, "", 5);
        writer.addDocument(doc);
      }
      writer.commit();
      for (int x = 0; x < 50; x++) {
        Document doc = createDocument(x, "", 5);
        writer.addDocument(doc);
      }
      writer.optimize();
      System.out.println("optimize");

      IndexReader reader = IndexReader.open(writer, true);
      assertEquals(100, reader.maxDoc());
      reader.close();
      writer.close();
      _TestUtil.checkIndex(dir, iwc.getCodecProvider());
    } finally {
      fileSys.close();
      cluster.shutdown();
    }
  }

  public static Document createDocument(int n, String indexName, int numFields) {
    StringBuilder sb = new StringBuilder();
    Document doc = new Document();
    doc.add(new Field("id", Integer.toString(n), Store.YES, Index.NOT_ANALYZED,
        TermVector.WITH_POSITIONS_OFFSETS));
    doc.add(new Field("indexname", indexName, Store.YES, Index.NOT_ANALYZED,
        TermVector.WITH_POSITIONS_OFFSETS));
    sb.append("a");
    sb.append(n);
    doc.add(new Field("field1", sb.toString(), Store.YES, Index.ANALYZED,
        TermVector.WITH_POSITIONS_OFFSETS));
    sb.append(" b");
    sb.append(n);
    for (int i = 1; i < numFields; i++) {
      doc.add(new Field("field" + (i + 1), sb.toString(), Store.YES,
          Index.ANALYZED, TermVector.WITH_POSITIONS_OFFSETS));
    }
    return doc;
  }
}

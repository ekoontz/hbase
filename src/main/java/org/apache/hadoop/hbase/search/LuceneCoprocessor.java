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

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.ipc.CoprocessorProtocol;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.wal.HLogKey;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LogByteSizeMergePolicy;
import org.apache.lucene.index.SerialMergeScheduler;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.codecs.CodecProvider;
import org.apache.lucene.index.codecs.appending.AppendingCodec;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.Version;

public class LuceneCoprocessor extends BaseRegionObserver implements
    CoprocessorProtocol {
  IndexWriter writer;
  public static final String ROW_FIELD = "row";
  public static final String UID_FIELD = "uid"; // rowid_timestamp
  DocumentTransformer documentTransformer = new DefaultDocumentTransformer();
  HRegion region;
  HDFSDirectory directory;

  public static String toUID(String row, long timestamp) {
    return row + "_" + timestamp;
  }

  public long getProtocolVersion(String protocol, long clientVersion)
      throws IOException {
    return 0L;
  }

  @Override
  public void preOpen(RegionCoprocessorEnvironment e) {
  }

  /**
   * Initialize Lucene's IndexWriter.
   */
  @Override
  public void postOpen(RegionCoprocessorEnvironment e) {
    try {
      region = e.getRegion();
      String encodedName = region.getRegionInfo().getEncodedName();
      String regionName = region.getRegionNameAsString();
      //System.out.println("regionName:"+regionName);
      String name = Bytes.toString(region.getStartKey())+Bytes.toString(region.getEndKey());
      FileSystem fileSystem = region.getFilesystem();
      String lucenePath = region.getConf().get("lucene.path");
      
      String regionLucenePath = lucenePath+"/"+encodedName;
      
      Analyzer analyzer = new WhitespaceAnalyzer(Version.LUCENE_40);
      IndexWriterConfig config = new IndexWriterConfig(Version.LUCENE_40,
          analyzer);
      LogByteSizeMergePolicy mergePolicy = new LogByteSizeMergePolicy();
      // compound files cannot be used with HDFS
      mergePolicy.setUseCompoundFile(false);
      config.setMergePolicy(mergePolicy);
      config.setMergeScheduler(new SerialMergeScheduler());
      config.setCodecProvider(new HDFSCodecProvider());
      fileSystem.delete(new Path(regionLucenePath));
      fileSystem.mkdirs(new Path(regionLucenePath));
      directory = new HDFSDirectory(fileSystem, regionLucenePath);
      writer = new IndexWriter(directory, config);
      //System.out.println("LuceneCoprocessor.postOpen");
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    }
  }

  @Override
  public void preClose(RegionCoprocessorEnvironment e, boolean abortRequested) {
  }

  @Override
  public void postClose(RegionCoprocessorEnvironment e, boolean abortRequested) {
   // System.out.println("LuceneCoprocessor.postClose");
    try {
      writer.close();
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    }
  }

  @Override
  public void postWALRestore(RegionCoprocessorEnvironment env,
      HRegionInfo info, HLogKey logKey, WALEdit logEdit) throws IOException {
    List<KeyValue> kvs = logEdit.getKeyValues();
    Document doc = documentTransformer.transform(this, kvs);
    Term uidTerm = getRowTerm(kvs.get(0));
    writer.updateDocument(uidTerm, doc);
  }

  private Term getRowTerm(KeyValue kv) {
    BytesRef row = new BytesRef(kv.getRow());
    return new Term(ROW_FIELD, row);
  }

  /**
   * Delete from IndexWriter.
   */
  @Override
  public void postDelete(final RegionCoprocessorEnvironment e,
      final Map<byte[], List<KeyValue>> familyMap, final boolean writeToWAL)
      throws IOException {
    try {
      Term delTerm = getRowTerm(familyMap);
      //System.out.println("LuceneCoprocessor.postDelete " + delTerm);
      writer.deleteDocuments(delTerm);
    } catch (CorruptIndexException cie) {
      throw new IOException("", cie);
    }
  }

  private Term getRowTerm(Map<byte[], List<KeyValue>> familyMap) {
    for (Map.Entry<byte[], List<KeyValue>> entry : familyMap.entrySet()) {
      KeyValue kv = entry.getValue().get(0);
      return getRowTerm(kv);
    }
    return null;
  }

  /**
   * Add a document to IndexWriter.
   */
  @Override
  public void postPut(final RegionCoprocessorEnvironment e,
      final Map<byte[], List<KeyValue>> familyMap, final boolean writeToWAL)
      throws IOException {
    // nocommit: we will build up too many delete terms if we
    // always try to delete the previous document
    // which may not actually be there
    // however looking up each document 
    // could be expensive, though with some of the
    // new FST work, it could become cheaper (see LUCENE-2948)
    Term rowTerm = getRowTerm(familyMap);
    Document doc = documentTransformer.transform(this, familyMap);
    if (doc != null ) {
      //System.out.println("LuceneCoprocessor.postPut term:"+rowTerm+" "+doc);
      if (rowTerm != null) {
        writer.updateDocument(rowTerm, doc);
      } else {
        writer.addDocument(doc);
      }
    }
  }

  @Override
  public void preFlush(RegionCoprocessorEnvironment e) {
  }

  @Override
  public void postFlush(RegionCoprocessorEnvironment e) {
    System.out.println("LuceneCoprocessor.postFlush");
    try {
      HRegion region = e.getRegion();
      long flushTime = region.getLastFlushTime();
      Map<String, String> commitUserData = new HashMap<String, String>();
      commitUserData.put("flushTime", Long.toString(flushTime));
      writer.commit(commitUserData);
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    }
  }
}

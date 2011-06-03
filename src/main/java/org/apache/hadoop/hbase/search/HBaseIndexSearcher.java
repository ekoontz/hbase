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
import java.util.Map;
import java.util.NavigableMap;
import java.util.concurrent.ExecutorService;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.StringUtils;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldSelector;
import org.apache.lucene.document.Field.Index;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexReader.ReaderContext;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.Directory;

public class HBaseIndexSearcher extends IndexSearcher {
  HRegion region;

  public HBaseIndexSearcher(IndexReader r, HRegion region) {
    super(r);
    this.region = region;
  }

  public HBaseIndexSearcher(Directory path, boolean readOnly)
      throws CorruptIndexException, IOException {
    super(path, readOnly);
  }

  public HBaseIndexSearcher(Directory path) throws CorruptIndexException,
      IOException {
    super(path);
  }

  public HBaseIndexSearcher(IndexReader r, ExecutorService executor) {
    super(r, executor);
  }

  public HBaseIndexSearcher(IndexReader r) {
    super(r);
  }

  public HBaseIndexSearcher(ReaderContext context, ExecutorService executor) {
    super(context, executor);
  }

  public HBaseIndexSearcher(ReaderContext context) {
    super(context);
  }

  public Document doc(int docID) throws CorruptIndexException, IOException {
    IndexReader reader = getIndexReader();
    return convert(reader.document(docID));
  }

  protected Document convert(Document d) throws CorruptIndexException,
      IOException {
    Field uidField = d.getField(LuceneCoprocessor.UID_FIELD);
    String uid = uidField.stringValue();
    String[] split = StringUtils.split(uid, '?', '_');
    byte[] row = Bytes.toBytes(split[0]);
    long timestamp = Long.parseLong(split[1]);
    Get get = new Get(row);
    get.setTimeStamp(timestamp);
    Document doc = new Document();
    Result result = region.get(get, null);
    NavigableMap<byte[], NavigableMap<byte[], byte[]>> map = result
        .getNoVersionMap();
    for (Map.Entry<byte[], NavigableMap<byte[], byte[]>> entry : map.entrySet()) {
      byte[] key = entry.getKey();
      NavigableMap<byte[], byte[]> value = entry.getValue();
      //String keyStr = Bytes.toString(key);
      for (Map.Entry<byte[],byte[]> entry2 : value.entrySet()) {
        String keyStr2 = Bytes.toString(entry2.getKey());
        String valStr2 = Bytes.toString(entry2.getValue());
        //System.out.println("keyStr:"+keyStr+" keyStr2:"+keyStr2+" valStr2:"+valStr2);
        Field field = new Field(keyStr2, valStr2, Store.YES, Index.ANALYZED);
        doc.add(field);
      }
    }
    doc.add(uidField);
    Field rowField = new Field("row", split[0], Store.YES, Index.NOT_ANALYZED);
    doc.add(rowField);
    //System.out.println(d + "");
    return doc;
  }

  public Document doc(int docID, FieldSelector fieldSelector)
      throws CorruptIndexException, IOException {
    IndexReader reader = getIndexReader();
    return convert(reader.document(docID, fieldSelector));
  }
}

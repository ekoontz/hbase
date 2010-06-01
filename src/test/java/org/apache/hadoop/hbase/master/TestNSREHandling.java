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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.ipc.HMasterInterface;
import org.apache.hadoop.hbase.ipc.HRegionInterface;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.SoftValueSortedMap;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.hbase.util.Writables;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWrapper;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Deliberately buggy implementation of client connection, 
 * intended to elicit NoSuchRegionExceptions from RegionServer.
 */

class NSREConnection implements org.apache.hadoop.hbase.client.HConnection {
    private HRegionLocation wrong_region_location;
    private final long pause;
    private final int numRetries;
    private static final Log LOG = LogFactory.getLog(TestNSREHandling.class);

    private final Map<Integer, SoftValueSortedMap<byte [], HRegionLocation>>
      cachedRegionLocations =
        new HashMap<Integer, SoftValueSortedMap<byte [], HRegionLocation>>();

    public NSREConnection(Configuration conf,HRegionLocation wrong_region_location) {
	this.pause = conf.getLong("hbase.client.pause", 1000);
	this.numRetries = conf.getInt("hbase.client.retries.number", 10);

	// this.wrong_region_location is returned by NSREConnection::locateRegion().
	this.wrong_region_location = wrong_region_location;
    }

    public ZooKeeperWrapper getZooKeeperWrapper()
            throws IOException {
          return null;
    }

    public HMasterInterface getMaster()
            throws MasterNotRunningException {
        return null;
    }

    public boolean isMasterRunning() {
        return true;
    }

    public boolean tableExists(final byte [] tableName)
    throws MasterNotRunningException {
        return true;
    }

    public boolean isTableEnabled(byte[] tableName)
                throws IOException {
        return true;
    }

    public boolean isTableDisabled(byte[] tableName)
            throws IOException {
        return true;
    }

    public boolean isTableAvailable(byte[] tableName)
            throws IOException {
        return true;
    }

    public HTableDescriptor[] listTables()
            throws IOException {
        return null;
    }

    public HTableDescriptor getHTableDescriptor(byte[] tableName)
        throws IOException {
        return null;
    }

    public HRegionLocation locateRegion(final byte [] tableName,
					final byte [] row,
					boolean reload) 
        throws IOException {
	return locateRegion(tableName,row);
    }

    public HRegionLocation locateRegion(final byte [] tableName,
        final byte [] row)
        throws IOException {
	//faulty version of org.apache.hadoop.hbase.client.HConnectionManager::locateRegion().
        return this.wrong_region_location;
    }

    public void clearRegionCache() {
        return;
    }

    public HRegionLocation relocateRegion(final byte [] tableName,
        final byte [] row)
        throws IOException {
        return null;
    }

    public HRegionInterface getHRegionConnection(HServerAddress regionServer)
    throws IOException {
        return null;
    }

    public HRegionInterface getHRegionConnection(
        HServerAddress regionServer, boolean getMaster)
        throws IOException {
        return null;
    }

    public HRegionLocation getRegionLocation(byte [] tableName, byte [] row,
      boolean reload)
        throws IOException {
        return null;
    }

    public <T> T getRegionServerWithRetries(ServerCallable<T> callable)
        throws IOException, RuntimeException {
        return null;
    }

    public <T> T getRegionServerWithoutRetries(ServerCallable<T> callable)
    throws IOException, RuntimeException {
        return null;
    }

    public int processBatchOfRows(ArrayList<Put> list, byte[] tableName)
    throws IOException {
        return 0;
    }

    public int processBatchOfDeletes(List<Delete> list, byte[] tableName)
    throws IOException {
        return 0;
    }

    
    // simplified version of HConnectionManager.java::processBatchOfPuts().
    // the intention here is to ask the *wrong* regionserver
    // for a given region, which should cause the regionserver
    // to emit a NSRE.
    public void processBatchOfPuts(List<Put> list,
                                   final byte[] tableName, ExecutorService pool)
            throws IOException {
	boolean singletonList = list.size() == 1;
	Throwable singleRowCause = null;
	for ( int tries = 0 ; tries < numRetries && !list.isEmpty(); ++tries) {
	    Collections.sort(list);
	    Map<HServerAddress, MultiPut> regionPuts =
		new HashMap<HServerAddress, MultiPut>();
	    // step 1:
	    //  break up into regionserver-sized chunks and build the data structs
	    for ( Put put : list ) {
		byte [] row = put.getRow();
		
		HRegionLocation loc = locateRegion(tableName, row, true);
		HServerAddress address = loc.getServerAddress();
		byte [] regionName = loc.getRegionInfo().getRegionName();
		
		MultiPut mput = regionPuts.get(address);
		if (mput == null) {
		    mput = new MultiPut(address);
		    regionPuts.put(address, mput);
		}
		mput.add(regionName, put);
	    }
	    
	    // step 2:
	    //  make the requests
	    // Discard the map, just use a list now, makes error recovery easier.
	    List<MultiPut> multiPuts = new ArrayList<MultiPut>(regionPuts.values());
	    
	    List<Future<MultiPutResponse>> futures =
		new ArrayList<Future<MultiPutResponse>>(regionPuts.size());
	    for ( MultiPut put : multiPuts ) {
		futures.add(pool.submit(createPutCallable(put.address,
							  put,
							  tableName)));
	    }
	    // RUN!
	    List<Put> failed = new ArrayList<Put>();
	    
	    // step 3:
	    //  collect the failures and tries from step 1.
	    for (int i = 0; i < futures.size(); i++ ) {
		Future<MultiPutResponse> future = futures.get(i);
		MultiPut request = multiPuts.get(i);
		try {
		    MultiPutResponse resp = future.get();
		    
		    // For each region
		    for (Map.Entry<byte[], List<Put>> e : request.puts.entrySet()) {
			Integer result = resp.getAnswer(e.getKey());
			if (result == null) {
			    // failed
			    LOG.debug("Failed all for region: " +
				      Bytes.toStringBinary(e.getKey()) + ", removing from cache");
			    failed.addAll(e.getValue());
			} else if (result >= 0) {
			    // some failures
			    List<Put> lst = e.getValue();
			    failed.addAll(lst.subList(result, lst.size()));
			    LOG.debug("Failed past " + result + " for region: " +
				      Bytes.toStringBinary(e.getKey()) + ", removing from cache");
			}
		    }
		} catch (InterruptedException e) {
		    // go into the failed list.
		    LOG.debug("Failed all from " + request.address, e);
		    failed.addAll(request.allPuts());
		} catch (ExecutionException e) {
		    // all go into the failed list.
		    LOG.debug("Failed all from " + request.address, e);
		    failed.addAll(request.allPuts());
		    
		    // Just give up, leaving the batch put list in an untouched/semi-committed state
		    if (e.getCause() instanceof DoNotRetryIOException) {
			throw (DoNotRetryIOException) e.getCause();
		    }
		    
		    if (singletonList) {
			// be richer for reporting in a 1 row case.
			singleRowCause = e.getCause();
		    }
		}
	    }
	    list.clear();
	    if (!failed.isEmpty()) {
		for (Put failedPut: failed) {
		    deleteCachedLocation(tableName, failedPut.getRow());
		}
		
		list.addAll(failed);
		
		long sleepTime = getPauseTime(tries);
		LOG.debug("processBatchOfPuts had some failures, sleeping for " + sleepTime +
			  " ms!");
		try {
		    Thread.sleep(sleepTime);
		} catch (InterruptedException ignored) {
		}
	    }
	}
	if (!list.isEmpty()) {
	    if (singletonList && singleRowCause != null) {
		throw new IOException(singleRowCause);
	    }
	    
	    // ran out of retries and didnt succeed everything!
	    throw new RetriesExhaustedException("Still had " + list.size() + " puts left after retrying " +
						numRetries + " times.");
	}
    }

    /*
     * Delete a cached location, if it satisfies the table name and row
     * requirements.
     */
    private void deleteCachedLocation(final byte [] tableName,
                                      final byte [] row) {
      synchronized (this.cachedRegionLocations) {
        SoftValueSortedMap<byte [], HRegionLocation> tableLocations =
            getTableLocations(tableName);

        // start to examine the cache. we can only do cache actions
        // if there's something in the cache for this table.
        if (!tableLocations.isEmpty()) {
          // cut the cache so that we only get the part that could contain
          // regions that match our key
          SoftValueSortedMap<byte [], HRegionLocation> matchingRegions =
              tableLocations.headMap(row);

          // if that portion of the map is empty, then we're done. otherwise,
          // we need to examine the cached location to verify that it is
          // a match by end key as well.
          if (!matchingRegions.isEmpty()) {
            HRegionLocation possibleRegion =
                matchingRegions.get(matchingRegions.lastKey());
            byte [] endKey = possibleRegion.getRegionInfo().getEndKey();

            // by nature of the map, we know that the start key has to be <
            // otherwise it wouldn't be in the headMap.
            if (Bytes.equals(endKey, HConstants.EMPTY_END_ROW) ||
                KeyValue.getRowComparator(tableName).compareRows(endKey, 0, endKey.length,
                    row, 0, row.length) > 0) {
              // delete any matching entry
              HRegionLocation rl =
                  tableLocations.remove(matchingRegions.lastKey());
              if (rl != null && LOG.isDebugEnabled()) {
                LOG.debug("Removed " + rl.getRegionInfo().getRegionNameAsString() +
                    " for tableName=" + Bytes.toString(tableName) + " from cache " +
                    "because of " + Bytes.toStringBinary(row));
              }
            }
          }
        }
      }
    }

    private long getPauseTime(int tries) {
      int ntries = tries;
      if (ntries >= HConstants.RETRY_BACKOFF.length)
        ntries = HConstants.RETRY_BACKOFF.length - 1;
      return this.pause * HConstants.RETRY_BACKOFF[ntries];
    }


    private Callable<MultiPutResponse> createPutCallable(
							 final HServerAddress address, final MultiPut puts,
							 final byte [] tableName) {
	final HConnection connection = this;
	return new Callable<MultiPutResponse>() {
	    public MultiPutResponse call() throws IOException {
		return getRegionServerWithoutRetries(
						     new ServerCallable<MultiPutResponse>(connection, tableName, null) {
                public MultiPutResponse call() throws IOException {
                  MultiPutResponse resp = server.multiPut(puts);
		  // resp.request = puts
                  resp.setRequest(puts);
                  return resp;
                }
                @Override
                public void instantiateServer(boolean reload) throws IOException {
                  server = connection.getHRegionConnection(address);
                }
              }
          );
        }
      };
    }

    /*
     * @param tableName
     * @return Map of cached locations for passed <code>tableName</code>
     */
    private SoftValueSortedMap<byte [], HRegionLocation> getTableLocations(
        final byte [] tableName) {
      // find the map of cached locations for this table
      Integer key = Bytes.mapKey(tableName);
      SoftValueSortedMap<byte [], HRegionLocation> result;
      synchronized (this.cachedRegionLocations) {
        result = this.cachedRegionLocations.get(key);
        // if tableLocations for this table isn't built yet, make one
        if (result == null) {
          result = new SoftValueSortedMap<byte [], HRegionLocation>(
              Bytes.BYTES_COMPARATOR);
          this.cachedRegionLocations.put(key, result);
        }
      }
      return result;
    }

}

class NSRETable extends HTable {

    private final byte [] nsre_tableName;
    private final ArrayList<Put> nsre_writeBuffer = new ArrayList<Put>();
    private ExecutorService nsre_pool;
    private int nsre_maxKeyValueSize;
    private long nsre_currentWriteBufferSize;
    private boolean nsre_autoFlush;
    private long nsre_writeBufferSize;

    private final NSREConnection nsre_connection;

    public NSRETable(Configuration conf,final byte[] tableName,HRegionLocation wrong_region_location)
            throws IOException {
	super(conf,tableName);
	nsre_tableName = tableName;
	nsre_connection = new NSREConnection(conf,wrong_region_location);

      // these 3 are needed for validateNSREPut(), doNSREPut(), and flushCommits().
	  nsre_writeBufferSize = conf.getLong("hbase.client.write.buffer", 2097152);
	  nsre_autoFlush = true;
	  nsre_currentWriteBufferSize = 0;
    }

    // validate for well-formedness: (super.validatePut() is private, so we cannot call it from doNSREPut(), so
    // we create a new method, validateNSREPut(), instead.)
    private void validateNSREPut(final Put put) throws IllegalArgumentException{
	if (put.isEmpty()) {
	    throw new IllegalArgumentException("No columns to insert");
	}
	if (nsre_maxKeyValueSize > 0) {
	    for (List<KeyValue> list : put.getFamilyMap().values()) {
		for (KeyValue kv : list) {
		    if (kv.getLength() > nsre_maxKeyValueSize) {
			throw new IllegalArgumentException("KeyValue size too large");
		    }
		}
	    }
	}
    }
    // doPut() is private, so we cannot call it from put(), so
    // we create a new method, doNSREPut(), instead.
    private void doNSREPut(final List<Put> puts) throws IOException {
	for (Put put : puts) {
	    validateNSREPut(put);
	    nsre_writeBuffer.add(put);
	    nsre_currentWriteBufferSize += put.heapSize();
	}
	if (nsre_autoFlush || nsre_currentWriteBufferSize > nsre_writeBufferSize) {
	    nsre_flushCommits();
	}
    }

    public void put(final Put put) throws IOException {
	  doNSREPut(Arrays.asList(put));
    }

    public void nsre_flushCommits() throws IOException {
	// override method that tries to commit to wrong region server.
	try {
	    nsre_connection.processBatchOfPuts(nsre_writeBuffer,
					  nsre_tableName, nsre_pool);
	} finally {
	    // the write buffer was adjusted by processBatchOfPuts
	    nsre_currentWriteBufferSize = 0;
	    /*	    for (Put aPut : nsre_writeBuffer) {
		nsre_currentWriteBufferSize += aPut.heapSize();
		}*/
	}
    }
    
}

public class TestNSREHandling {
  private static final Log LOG = LogFactory.getLog(TestNSREHandling.class);
  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static final String TABLENAME = "nsre_test_table";
  private static final byte [][] FAMILIES = new byte [][] {Bytes.toBytes("a"),
    Bytes.toBytes("b"), Bytes.toBytes("c")};

  static boolean region_closed = false;

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
   * HBASE-2486: set up some scenarios to cause a region server to emit a NSRE,
   * and then assert that master has correctly responded in each scenario.
   */

  /* test for:
   * 3.a. (legitimate NSRE) : manipulate table object (used by client) : change apparent location of region A from correct regionserver R1 to
   *    incorrect regionserver R2.
   * 3.b. (transition) : ??
   * 3.c. (inconsistent NSRE): manipulate .META. table to change apparent location of region A from correct regionserver R1
   *       to incorrect regionserver R2.
   */



  @Test (timeout=300000) public void causeRSToEmitNSRE()
  throws Exception {
    LOG.info("Running causeRSToEmitNSRE()");
    MiniHBaseCluster cluster = TEST_UTIL.getHBaseCluster();
    final HMaster master = cluster.getMaster();

    int regionserverB_index = cluster.getServerWithMeta();
    // Figure the index of the server that is not server the .META.
    int regionserverA_index = -1;
    for (int i = 0; i < cluster.getRegionServerThreads().size(); i++) {
      if (i == regionserverB_index) continue;
      regionserverA_index = i;
      break;
    }

    final HRegionServer regionserverA = cluster.getRegionServer(regionserverA_index);
    final HRegionServer regionserverB = cluster.getRegionServer(regionserverB_index);

    // 1. Get a region on 'regionserverA'
    final HRegionInfo hri = regionserverA.getOnlineRegions().iterator().next().getRegionInfo();
    final String regionName = hri.getRegionNameAsString();

    // wrong region:
    final HRegionLocation wrong_region_location = 
	new HRegionLocation(hri,regionserverB.getServerInfo().getServerAddress());

    // open a client connection to this region.
    NSRETable table = new NSRETable(TEST_UTIL.getConfiguration(),
				    Bytes.toBytes("nsre_test_table"),
				    wrong_region_location);
    byte [] row = getStartKey(hri);
    Put p = new Put(row);
    p.add(getTestFamily(),getTestQualifier(),row);
    table.put(p);

    Thread.sleep(10000);
    LOG.info("HBASE-2486: Done waiting.");
    assertEquals(42,42);
    LOG.info("EXITING TEST.");

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

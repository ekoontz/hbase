package org.apache.hadoop.hbase.permissions;

import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.coprocessor.Coprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionObserver;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.Coprocessor.Environment;

public class Permissions implements Coprocessor, RegionObserver {

  public void onOpen(final Environment e) {
    return;
  }

  public void onFlush(final Environment e) {
    return;
  }

  public void onCompact(final Environment e, final boolean complete,
			final boolean willSplit) {
    return;
  }

  public void onSplit(final Environment e, final HRegion l, final HRegion r) {
    return;
  }

  public Result onGetClosestRowBefore(final Environment e, final byte [] row,
				      final byte [] family, final Result result) {
    return result;
  }

  public List<KeyValue> onGet(final Environment e, final Get get,
			      final List<KeyValue> results) {
    return results;
  }

  public boolean onExists(final Environment e, final Get get,
			  final boolean exists) {
    return exists;
  }


  public Map<byte[], List<KeyValue>> onPut(final Environment e,
					   final Map<byte[], List<KeyValue>> familyMap) {
    return familyMap;
  }

  public KeyValue onPut(final Environment e, final KeyValue kv) {
    return kv;
  }

  public Map<byte[], List<KeyValue>> onDelete(final Environment e,
					      final Map<byte[], List<KeyValue>> familyMap) {
    return familyMap;
  }

  public void onScannerOpen(final Environment e, final Scan scan,
			    final long scannerId) {
    return;
  }

  public List<KeyValue> onScannerNext(final Environment e,
				      final long scannerId, final List<KeyValue> results) {
    return results;
  }

  public void onScannerClose(final Environment e, final long scannerId) {
    return;
  }

  public void onClose(final Environment e, boolean abortRequested) {
    return;
  }


}
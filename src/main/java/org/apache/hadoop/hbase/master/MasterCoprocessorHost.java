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
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hbase.master;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.coprocessor.*;

import java.io.IOException;

/**
 * Provides the coprocessor framework and environment for master oriented
 * operations.  {@link HMaster} interacts with the loaded coprocessors
 * through this class.
 */
public class MasterCoprocessorHost
    extends CoprocessorHost<MasterCoprocessorHost.MasterEnvironment> {

  /**
   * Coprocessor environment extension providing access to master related
   * services.
   */
  static class MasterEnvironment extends CoprocessorHost.Environment
      implements MasterCoprocessorEnvironment {
    private MasterServices masterServices;

    public MasterEnvironment(final Class<?> implClass, final Coprocessor impl,
        final int priority, final int seq, final Configuration conf,
        final MasterServices services) {
      super(impl, priority, seq, conf);
      this.masterServices = services;
    }

    public MasterServices getMasterServices() {
      return masterServices;
    }
  }

  private MasterServices masterServices;

  MasterCoprocessorHost(final MasterServices services, final Configuration conf) {
    this.masterServices = services;
    loadSystemCoprocessors(conf, MASTER_COPROCESSOR_CONF_KEY);
  }

  @Override
  public MasterEnvironment createEnvironment(final Class<?> implClass,
      final Coprocessor instance, final int priority, final int seq,
      final Configuration conf) {
    return new MasterEnvironment(implClass, instance, priority, seq, conf,
        masterServices);
  }

  @Override
  protected void abortServer(final CoprocessorEnvironment env, final Throwable e) {
    abortServer("master", masterServices, env, e);
  }

  private void handleCoprocessorThrowableAsUnknownRegionException(final CoprocessorEnvironment env, final Throwable e)
    // Similar to the handleCoprocessorThrowable(), but for those Master coprocessor hooks that throw specifically
    // a UnknownRegionException (rather than the more general IOException). In this case, treat any IOException that is
    // not an UnknownRegionException as we would a non-IOException Throwable: abort the master.
    throws UnknownRegionException {
    if (e instanceof UnknownRegionException) {
      // The coprocessor threw an UnknownRegionException, which should be passed back to the client.
      throw (UnknownRegionException)e;
    }
    else {
      // regardless of configuration, remove coprocessor.
      LOG.error("Removing coprocessor '" + env + "' from environment because it threw: " + e);
      coprocessors.remove(env);
      // e is not an UnknownRegionException: master should abort (depending on configuration).
      if (env.getConfiguration().get("hbase.coprocessor.abort_on_error").equals("true")) {
        // server is configured to abort.
        abortServer(env, e);
      }
    }
  }

  /* Implementation of hooks for invoking MasterObservers */
  void preCreateTable(HTableDescriptor desc, byte[][] splitKeys)
      throws IOException {
    ObserverContext<MasterCoprocessorEnvironment> ctx = null;
    for (MasterEnvironment env: coprocessors) {
      if (env.getInstance() instanceof MasterObserver) {
        ctx = ObserverContext.createAndPrepare(env, ctx);
        try {
          ((MasterObserver)env.getInstance()).preCreateTable(ctx, desc, splitKeys);
        }
        catch (Throwable e) {
          handleCoprocessorThrowable(env, e);
        }
        if (ctx.shouldComplete()) {
          break;
        }
      }
    }
  }

  void postCreateTable(HRegionInfo[] regions, boolean sync) throws IOException {
    ObserverContext<MasterCoprocessorEnvironment> ctx = null;
    for (MasterEnvironment env: coprocessors) {
      if (env.getInstance() instanceof MasterObserver) {
        ctx = ObserverContext.createAndPrepare(env, ctx);
        try {
          ((MasterObserver)env.getInstance()).postCreateTable(ctx, regions, sync);
        }
        catch (Throwable e) {
          handleCoprocessorThrowable(env, e);
        }
        if (ctx.shouldComplete()) {
          break;
        }
      }
    }
  }

  void preDeleteTable(byte[] tableName) throws IOException {
    ObserverContext<MasterCoprocessorEnvironment> ctx = null;
    for (MasterEnvironment env: coprocessors) {
      if (env.getInstance() instanceof MasterObserver) {
        ctx = ObserverContext.createAndPrepare(env, ctx);
        try {
          ((MasterObserver)env.getInstance()).preDeleteTable(ctx, tableName);
        }
        catch (Throwable e) {
          handleCoprocessorThrowable(env, e);
        }
        if (ctx.shouldComplete()) {
          break;
        }
      }
    }
  }

  void postDeleteTable(byte[] tableName) throws IOException {
    ObserverContext<MasterCoprocessorEnvironment> ctx = null;
    for (MasterEnvironment env: coprocessors) {
      if (env.getInstance() instanceof MasterObserver) {
        ctx = ObserverContext.createAndPrepare(env, ctx);
        try {
          ((MasterObserver)env.getInstance()).postDeleteTable(ctx, tableName);
        }
        catch (Throwable e) {
          handleCoprocessorThrowable(env, e);
        }
        if (ctx.shouldComplete()) {
          break;
        }
      }
    }
  }

  void preModifyTable(final byte[] tableName, HTableDescriptor htd)
      throws IOException {
    ObserverContext<MasterCoprocessorEnvironment> ctx = null;
    for (MasterEnvironment env: coprocessors) {
      if (env.getInstance() instanceof MasterObserver) {
        ctx = ObserverContext.createAndPrepare(env, ctx);
        try {
          ((MasterObserver)env.getInstance()).preModifyTable(ctx, tableName, htd);
        }
        catch (Throwable e) {
          handleCoprocessorThrowable(env, e);
        }
        if (ctx.shouldComplete()) {
          break;
        }
      }
    }
  }

  void postModifyTable(final byte[] tableName, HTableDescriptor htd)
      throws IOException {
    ObserverContext<MasterCoprocessorEnvironment> ctx = null;
    for (MasterEnvironment env: coprocessors) {
      if (env.getInstance() instanceof MasterObserver) {
        ctx = ObserverContext.createAndPrepare(env, ctx);
        try {
          ((MasterObserver)env.getInstance()).postModifyTable(ctx, tableName, htd);
        }
        catch (Throwable e) {
          handleCoprocessorThrowable(env, e);
        }
        if (ctx.shouldComplete()) {
          break;
        }
      }
    }
  }

  void preAddColumn(byte [] tableName, HColumnDescriptor column)
      throws IOException {
    ObserverContext<MasterCoprocessorEnvironment> ctx = null;
    for (MasterEnvironment env: coprocessors) {
      if (env.getInstance() instanceof MasterObserver) {
        ctx = ObserverContext.createAndPrepare(env, ctx);
        try {
          ((MasterObserver)env.getInstance()).preAddColumn(ctx, tableName, column);
        } catch (Throwable e) {
          handleCoprocessorThrowable(env, e);
        }
        if (ctx.shouldComplete()) {
          break;
        }
      }
    }
  }

  void postAddColumn(byte [] tableName, HColumnDescriptor column)
      throws IOException {
    ObserverContext<MasterCoprocessorEnvironment> ctx = null;
    for (MasterEnvironment env: coprocessors) {
      if (env.getInstance() instanceof MasterObserver) {
        ctx = ObserverContext.createAndPrepare(env, ctx);
        try {
          ((MasterObserver)env.getInstance()).postAddColumn(ctx, tableName, column);
        }
        catch (Throwable e) {
          handleCoprocessorThrowable(env, e);
        }
        if (ctx.shouldComplete()) {
          break;
        }
      }
    }
  }

  void preModifyColumn(byte [] tableName, HColumnDescriptor descriptor)
      throws IOException {
    ObserverContext<MasterCoprocessorEnvironment> ctx = null;
    for (MasterEnvironment env: coprocessors) {
      if (env.getInstance() instanceof MasterObserver) {
        ctx = ObserverContext.createAndPrepare(env, ctx);
        try {
          ((MasterObserver)env.getInstance()).preModifyColumn(
            ctx, tableName, descriptor);
        }
        catch (Throwable e) {
          handleCoprocessorThrowable(env, e);
        }
        if (ctx.shouldComplete()) {
          break;
        }
      }
    }
  }

  void postModifyColumn(byte [] tableName, HColumnDescriptor descriptor)
      throws IOException {
    ObserverContext<MasterCoprocessorEnvironment> ctx = null;
    for (MasterEnvironment env: coprocessors) {
      if (env.getInstance() instanceof MasterObserver) {
        ctx = ObserverContext.createAndPrepare(env, ctx);
        try {
          ((MasterObserver)env.getInstance()).postModifyColumn(
              ctx, tableName, descriptor);
        }
        catch (Throwable e) {
          handleCoprocessorThrowable(env, e);
        }
        if (ctx.shouldComplete()) {
          break;
        }
      }
    }
  }

  void preDeleteColumn(final byte [] tableName, final byte [] c)
      throws IOException {
    ObserverContext<MasterCoprocessorEnvironment> ctx = null;
    for (MasterEnvironment env: coprocessors) {
      if (env.getInstance() instanceof MasterObserver) {
        ctx = ObserverContext.createAndPrepare(env, ctx);
        try {
          ((MasterObserver)env.getInstance()).preDeleteColumn(ctx, tableName, c);
        }
        catch (Throwable e) {
          handleCoprocessorThrowable(env, e);
        }
        if (ctx.shouldComplete()) {
          break;
        }
      }
    }
  }

  void postDeleteColumn(final byte [] tableName, final byte [] c)
      throws IOException {
    ObserverContext<MasterCoprocessorEnvironment> ctx = null;
    for (MasterEnvironment env: coprocessors) {
      if (env.getInstance() instanceof MasterObserver) {
        ctx = ObserverContext.createAndPrepare(env, ctx);
        try {
          ((MasterObserver)env.getInstance()).postDeleteColumn(ctx, tableName, c);
        }
        catch (Throwable e) {
          handleCoprocessorThrowable(env, e);
        }
        if (ctx.shouldComplete()) {
          break;
        }
      }
    }
  }

  void preEnableTable(final byte [] tableName) throws IOException {
    ObserverContext<MasterCoprocessorEnvironment> ctx = null;
    for (MasterEnvironment env: coprocessors) {
      if (env.getInstance() instanceof MasterObserver) {
        ctx = ObserverContext.createAndPrepare(env, ctx);
        try {
          ((MasterObserver)env.getInstance()).preEnableTable(ctx, tableName);
        }
        catch (Throwable e) {
          handleCoprocessorThrowable(env, e);
        }
        if (ctx.shouldComplete()) {
          break;
        }
      }
    }
  }

  void postEnableTable(final byte [] tableName) throws IOException {
    ObserverContext<MasterCoprocessorEnvironment> ctx = null;
    for (MasterEnvironment env: coprocessors) {
      if (env.getInstance() instanceof MasterObserver) {
        ctx = ObserverContext.createAndPrepare(env, ctx);
        try {
          ((MasterObserver)env.getInstance()).postEnableTable(ctx, tableName);
        }
        catch (Throwable e) {
          handleCoprocessorThrowable(env, e);
        }
        if (ctx.shouldComplete()) {
          break;
        }
      }
    }
  }

  void preDisableTable(final byte [] tableName) throws IOException {
    ObserverContext<MasterCoprocessorEnvironment> ctx = null;
    for (MasterEnvironment env: coprocessors) {
      if (env.getInstance() instanceof MasterObserver) {
        ctx = ObserverContext.createAndPrepare(env, ctx);
        try {
          ((MasterObserver)env.getInstance()).preDisableTable(ctx, tableName);
        }
        catch (Throwable e) {
          handleCoprocessorThrowable(env, e);
        }
        if (ctx.shouldComplete()) {
          break;
        }
      }
    }
  }

  void postDisableTable(final byte [] tableName) throws IOException {
    ObserverContext<MasterCoprocessorEnvironment> ctx = null;
    for (MasterEnvironment env: coprocessors) {
      if (env.getInstance() instanceof MasterObserver) {
        ctx = ObserverContext.createAndPrepare(env, ctx);
        try {
          ((MasterObserver)env.getInstance()).postDisableTable(ctx, tableName);
        }
        catch (Throwable e) {
          handleCoprocessorThrowable(env, e);
        }
        if (ctx.shouldComplete()) {
          break;
        }
      }
    }
  }

  void preMove(final HRegionInfo region, final ServerName srcServer, final ServerName destServer)
      throws UnknownRegionException {
    ObserverContext<MasterCoprocessorEnvironment> ctx = null;
    for (MasterEnvironment env: coprocessors) {
      if (env.getInstance() instanceof MasterObserver) {
        ctx = ObserverContext.createAndPrepare(env, ctx);
        try {
          ((MasterObserver)env.getInstance()).preMove(
              ctx, region, srcServer, destServer);
        }
        catch (Throwable e) {
          handleCoprocessorThrowableAsUnknownRegionException(env, e);
        }
        if (ctx.shouldComplete()) {
          break;
        }
      }
    }
  }

  void postMove(final HRegionInfo region, final ServerName srcServer, final ServerName destServer)
      throws UnknownRegionException {
    ObserverContext<MasterCoprocessorEnvironment> ctx = null;
    for (MasterEnvironment env: coprocessors) {
      if (env.getInstance() instanceof MasterObserver) {
        ctx = ObserverContext.createAndPrepare(env, ctx);
        try {
          ((MasterObserver)env.getInstance()).postMove(
              ctx, region, srcServer, destServer);
        }
        catch (Throwable e) {
          handleCoprocessorThrowableAsUnknownRegionException(env,e);
        }
        if (ctx.shouldComplete()) {
          break;
        }
      }
    }
  }

  boolean preAssign(final byte [] regionName, final boolean force)
      throws IOException {
    boolean bypass = false;
    ObserverContext<MasterCoprocessorEnvironment> ctx = null;
    for (MasterEnvironment env: coprocessors) {
      if (env.getInstance() instanceof MasterObserver) {
        ctx = ObserverContext.createAndPrepare(env, ctx);
        try {
          ((MasterObserver)env.getInstance()).preAssign(ctx, regionName, force);
        }
        catch (Throwable e) {
          handleCoprocessorThrowable(env, e);
        }
        bypass |= ctx.shouldBypass();
        if (ctx.shouldComplete()) {
          break;
        }
      }
    }
    return bypass;
  }

  void postAssign(final HRegionInfo regionInfo) throws IOException {
    ObserverContext<MasterCoprocessorEnvironment> ctx = null;
    for (MasterEnvironment env: coprocessors) {
      if (env.getInstance() instanceof MasterObserver) {
        ctx = ObserverContext.createAndPrepare(env, ctx);
        try {
          ((MasterObserver)env.getInstance()).postAssign(ctx, regionInfo);
        }
        catch (Throwable e) {
          handleCoprocessorThrowable(env, e);
        }
        if (ctx.shouldComplete()) {
          break;
        }
      }
    }
  }

  boolean preUnassign(final byte [] regionName, final boolean force)
      throws IOException {
    boolean bypass = false;
    ObserverContext<MasterCoprocessorEnvironment> ctx = null;
    for (MasterEnvironment env: coprocessors) {
      if (env.getInstance() instanceof MasterObserver) {
        ctx = ObserverContext.createAndPrepare(env, ctx);
        try {
          ((MasterObserver)env.getInstance()).preUnassign(
              ctx, regionName, force);
        }
        catch (Throwable e) {
          handleCoprocessorThrowable(env, e);
        }
        bypass |= ctx.shouldBypass();
        if (ctx.shouldComplete()) {
          break;
        }
      }
    }
    return bypass;
  }

  void postUnassign(final HRegionInfo regionInfo, final boolean force)
      throws IOException {
    ObserverContext<MasterCoprocessorEnvironment> ctx = null;
    for (MasterEnvironment env: coprocessors) {
      if (env.getInstance() instanceof MasterObserver) {
        ctx = ObserverContext.createAndPrepare(env, ctx);
        try {
          ((MasterObserver)env.getInstance()).postUnassign(
              ctx, regionInfo, force);
        }
        catch (Throwable e) {
          handleCoprocessorThrowable(env, e);
        }
        if (ctx.shouldComplete()) {
          break;
        }
      }
    }
  }

  boolean preBalance() throws IOException {
    boolean bypass = false;
    ObserverContext<MasterCoprocessorEnvironment> ctx = null;
    for (MasterEnvironment env: coprocessors) {
      if (env.getInstance() instanceof MasterObserver) {
        ctx = ObserverContext.createAndPrepare(env, ctx);
        try {
          ((MasterObserver)env.getInstance()).preBalance(ctx);
        }
        catch (Throwable e) {
          handleCoprocessorThrowable(env, e);
        }
        bypass |= ctx.shouldBypass();
        if (ctx.shouldComplete()) {
          break;
        }
      }
    }
    return bypass;
  }

  void postBalance() throws IOException {
    ObserverContext<MasterCoprocessorEnvironment> ctx = null;
    for (MasterEnvironment env: coprocessors) {
      if (env.getInstance() instanceof MasterObserver) {
        ctx = ObserverContext.createAndPrepare(env, ctx);
        try {
          ((MasterObserver)env.getInstance()).postBalance(ctx);
        }
        catch (Throwable e) {
          handleCoprocessorThrowable(env, e);
        }
        if (ctx.shouldComplete()) {
          break;
        }
      }
    }
  }

  boolean preBalanceSwitch(final boolean b) throws IOException {
    boolean balance = b;
    ObserverContext<MasterCoprocessorEnvironment> ctx = null;
    for (MasterEnvironment env: coprocessors) {
      if (env.getInstance() instanceof MasterObserver) {
        ctx = ObserverContext.createAndPrepare(env, ctx);
        try {
          balance = ((MasterObserver)env.getInstance()).preBalanceSwitch(
              ctx, balance);
        }
        catch (Throwable e) {
          handleCoprocessorThrowable(env, e);
        }
        if (ctx.shouldComplete()) {
          break;
        }
      }
    }
    return balance;
  }

  void postBalanceSwitch(final boolean oldValue, final boolean newValue)
      throws IOException {
    ObserverContext<MasterCoprocessorEnvironment> ctx = null;
    for (MasterEnvironment env: coprocessors) {
      if (env.getInstance() instanceof MasterObserver) {
        ctx = ObserverContext.createAndPrepare(env, ctx);
        try {
          ((MasterObserver)env.getInstance()).postBalanceSwitch(
              ctx, oldValue, newValue);
        }
        catch (Throwable e) {
          handleCoprocessorThrowable(env, e);
        }
        if (ctx.shouldComplete()) {
          break;
        }
      }
    }
  }

  void preShutdown() throws IOException {
    ObserverContext<MasterCoprocessorEnvironment> ctx = null;
    for (MasterEnvironment env: coprocessors) {
      if (env.getInstance() instanceof MasterObserver) {
        ctx = ObserverContext.createAndPrepare(env, ctx);
        try {
          ((MasterObserver)env.getInstance()).preShutdown(ctx);
        }
        catch (Throwable e) {
          handleCoprocessorThrowable(env, e);
        }
        if (ctx.shouldComplete()) {
          break;
        }
      }
    }
  }

  void preStopMaster() throws IOException {
    ObserverContext<MasterCoprocessorEnvironment> ctx = null;
    for (MasterEnvironment env: coprocessors) {
      if (env.getInstance() instanceof MasterObserver) {
        ctx = ObserverContext.createAndPrepare(env, ctx);
        try {
          ((MasterObserver)env.getInstance()).preStopMaster(ctx);
        }
        catch (Throwable e) {
          handleCoprocessorThrowable(env, e);
        }
        if (ctx.shouldComplete()) {
          break;
        }
      }
    }
  }

  void postStartMaster() throws IOException {
    ObserverContext<MasterCoprocessorEnvironment> ctx = null;
    for (MasterEnvironment env: coprocessors) {
      if (env.getInstance() instanceof MasterObserver) {
        ctx = ObserverContext.createAndPrepare(env, ctx);
        try {
          ((MasterObserver)env.getInstance()).postStartMaster(ctx);
        }
        catch (Throwable e) {
          handleCoprocessorThrowable(env, e);
        }
        if (ctx.shouldComplete()) {
          break;
        }
      }
    }
  }
}

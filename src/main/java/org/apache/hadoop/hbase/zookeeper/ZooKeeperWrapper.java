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
package org.apache.hadoop.hbase.zookeeper;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HServerAddress;
import org.apache.hadoop.hbase.HServerInfo;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.StringUtils;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooKeeper.States;
import org.apache.zookeeper.data.Stat;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.Properties;

/**
 * Wraps a ZooKeeper instance and adds HBase specific functionality.
 *
 * This class provides methods to:
 * - read/write/delete the root region location in ZooKeeper.
 * - set/check out of safe mode flag.
 */
public class ZooKeeperWrapper implements HConstants {
  protected static final Log LOG = LogFactory.getLog(ZooKeeperWrapper.class);

  // TODO: Replace this with ZooKeeper constant when ZOOKEEPER-277 is resolved.
  private static final char ZNODE_PATH_SEPARATOR = '/';

  private String quorumServers = null;

  private final ZooKeeper zooKeeper;

  private final String parentZNode;
  private final String rootRegionZNode;
  private final String rsZNode;
  private final String masterElectionZNode;
  public final String clusterStateZNode;

  /**
   * Create a ZooKeeperWrapper.
   * @param conf Configuration to read settings from.
   * @param watcher ZooKeeper watcher to register.
   * @throws IOException If a connection error occurs.
   */
  public ZooKeeperWrapper(Configuration conf, Watcher watcher)
  throws IOException {
    Properties properties = HQuorumPeer.makeZKProps(conf);
    setQuorumServers(properties);
    if (quorumServers == null) {
      throw new IOException("Could not read quorum servers from " +
                            ZOOKEEPER_CONFIG_NAME);
    }

    int sessionTimeout = conf.getInt("zookeeper.session.timeout", 60 * 1000);
    try {
      zooKeeper = new ZooKeeper(quorumServers, sessionTimeout, watcher);
    } catch (IOException e) {
      LOG.error("Failed to create ZooKeeper object: " + e);
      throw new IOException(e);
    }

    parentZNode = conf.get(ZOOKEEPER_ZNODE_PARENT,
        DEFAULT_ZOOKEEPER_ZNODE_PARENT);

    String rootServerZNodeName = conf.get("zookeeper.znode.rootserver",
                                          "root-region-server");
    String rsZNodeName = conf.get("zookeeper.znode.rs", "rs");
    String masterAddressZNodeName = conf.get("zookeeper.znode.master",
      "master");
    String stateZNodeName = conf.get("zookeeper.znode.state",
    "shutdown");

    rootRegionZNode = getZNode(parentZNode, rootServerZNodeName);
    rsZNode = getZNode(parentZNode, rsZNodeName);
    masterElectionZNode = getZNode(parentZNode, masterAddressZNodeName);
    clusterStateZNode = getZNode(parentZNode, stateZNodeName);
  }

  private void setQuorumServers(Properties properties) {
    String clientPort = null;
    List<String> servers = new ArrayList<String>();

    // The clientPort option may come after the server.X hosts, so we need to
    // grab everything and then create the final host:port comma separated list.
    boolean anyValid = false;
    for (Entry<Object,Object> property : properties.entrySet()) {
      String key = property.getKey().toString().trim();
      String value = property.getValue().toString().trim();
      if (key.equals("clientPort")) {
        clientPort = value;
      }
      else if (key.startsWith("server.")) {
        String host = value.substring(0, value.indexOf(':'));
        servers.add(host);
        try {
          //noinspection ResultOfMethodCallIgnored
          InetAddress.getByName(host);
          anyValid = true;
        } catch (UnknownHostException e) {
          LOG.warn(StringUtils.stringifyException(e));
        }
      }
    }

    if (!anyValid) {
      LOG.error("no valid quorum servers found in " + ZOOKEEPER_CONFIG_NAME);
      return;
    }

    if (clientPort == null) {
      LOG.error("no clientPort found in " + ZOOKEEPER_CONFIG_NAME);
      return;
    }

    if (servers.isEmpty()) {
      LOG.fatal("No server.X lines found in conf/zoo.cfg. HBase must have a " +
                "ZooKeeper cluster configured for its operation.");
      return;
    }

    StringBuilder hostPortBuilder = new StringBuilder();
    for (int i = 0; i < servers.size(); ++i) {
      String host = servers.get(i);
      if (i > 0) {
        hostPortBuilder.append(',');
      }
      hostPortBuilder.append(host);
      hostPortBuilder.append(':');
      hostPortBuilder.append(clientPort);
    }

    quorumServers = hostPortBuilder.toString();
  }

  /** @return String dump of everything in ZooKeeper. */
  @SuppressWarnings({"ConstantConditions"})
  public String dump() {
    StringBuilder sb = new StringBuilder();
    sb.append("\nHBase tree in ZooKeeper is rooted at ").append(parentZNode);
    sb.append("\n  Cluster up? ").append(exists(clusterStateZNode));
    sb.append("\n  Master address: ").append(readMasterAddress(null));
    sb.append("\n  Region server holding ROOT: ").append(readRootRegionLocation());
    sb.append("\n  Region servers:");
    for (HServerAddress address : scanRSDirectory()) {
      sb.append("\n    - ").append(address);
    }
    sb.append("\n  Quorum Server Statistics:");
    String[] servers = quorumServers.split(",");
    for (String server : servers) {
      sb.append("\n    - ").append(server);
      try {
        String[] stat = getServerStats(server);
        for (String s : stat) {
          sb.append("\n        ").append(s);
        }
      } catch (Exception e) {
        sb.append("\n        ERROR: ").append(e.getMessage());
      }
    }
    return sb.toString();
  }

  /**
   * Gets the statistics from the given server. Uses a 1 minute timeout.
   *
   * @param server  The server to get the statistics from.
   * @return The array of response strings.
   * @throws IOException When the socket communication fails.
   */
  public String[] getServerStats(String server)
  throws IOException {
    return getServerStats(server, 60 * 1000);
  }

  /**
   * Gets the statistics from the given server.
   *
   * @param server  The server to get the statistics from.
   * @param timeout  The socket timeout to use.
   * @return The array of response strings.
   * @throws IOException When the socket communication fails.
   */
  public String[] getServerStats(String server, int timeout)
  throws IOException {
    String[] sp = server.split(":");
    Socket socket = new Socket(sp[0],
      sp.length > 1 ? Integer.parseInt(sp[1]) : 2181);
    socket.setSoTimeout(timeout);
    PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
    BufferedReader in = new BufferedReader(new InputStreamReader(
      socket.getInputStream()));
    out.println("stat");
    out.flush();
    ArrayList<String> res = new ArrayList<String>();
    while (true) {
      String line = in.readLine();
      if (line != null) res.add(line);
      else break;
    }
    socket.close();
    return res.toArray(new String[res.size()]);
  }

  private boolean exists(String znode) {
    try {
      return zooKeeper.exists(znode, null) != null;
    } catch (KeeperException e) {
      return false;
    } catch (InterruptedException e) {
      return false;
    }
  }

  /** @return ZooKeeper used by this wrapper. */
  public ZooKeeper getZooKeeper() {
    return zooKeeper;
  }

  /**
   * This is for testing KeeperException.SessionExpiredException.
   * See HBASE-1232.
   * @return long session ID of this ZooKeeper session.
   */
  public long getSessionID() {
    return zooKeeper.getSessionId();
  }

  /**
   * This is for testing KeeperException.SessionExpiredException.
   * See HBASE-1232.
   * @return byte[] password of this ZooKeeper session.
   */
  public byte[] getSessionPassword() {
    return zooKeeper.getSessionPasswd();
  }

  /** @return host:port list of quorum servers. */
  public String getQuorumServers() {
    return quorumServers;
  }

  /** @return true if currently connected to ZooKeeper, false otherwise. */
  public boolean isConnected() {
    return zooKeeper.getState() == States.CONNECTED;
  }

  /**
   * Read location of server storing root region.
   * @return HServerAddress pointing to server serving root region or null if
   *         there was a problem reading the ZNode.
   */
  public HServerAddress readRootRegionLocation() {
    return readAddress(rootRegionZNode, null);
  }

  /**
   * Read address of master server.
   * @return HServerAddress of master server.
   * @throws IOException if there's a problem reading the ZNode.
   */
  public HServerAddress readMasterAddressOrThrow() throws IOException {
    return readAddressOrThrow(masterElectionZNode, null);
  }

  /**
   * Read master address and set a watch on it.
   * @param watcher Watcher to set on master address ZNode if not null.
   * @return HServerAddress of master or null if there was a problem reading the
   *         ZNode. The watcher is set only if the result is not null.
   */
  public HServerAddress readMasterAddress(Watcher watcher) {
    return readAddress(masterElectionZNode, watcher);
  }

  /**
   * Watch the state of the cluster, up or down
   * @param watcher Watcher to set on cluster state node
   */
  public void setClusterStateWatch(Watcher watcher) {
    try {
      zooKeeper.exists(clusterStateZNode, watcher);
    } catch (InterruptedException e) {
      LOG.warn("Failed to check on ZNode " + clusterStateZNode, e);
    } catch (KeeperException e) {
      LOG.warn("Failed to check on ZNode " + clusterStateZNode, e);
    }
  }

  /**
   * Set the cluster state, up or down
   * @param up True to write the node, false to delete it
   * @return true if it worked, else it's false
   */
  public boolean setClusterState(boolean up) {
    if (!ensureParentExists(clusterStateZNode)) {
      return false;
    }
    try {
      if(up) {
        byte[] data = Bytes.toBytes("up");
        zooKeeper.create(clusterStateZNode, data,
            Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        LOG.debug("State node wrote in ZooKeeper");
      } else {
        zooKeeper.delete(clusterStateZNode, -1);
        LOG.debug("State node deleted in ZooKeeper");
      }
      return true;
    } catch (InterruptedException e) {
      LOG.warn("Failed to set state node in ZooKeeper", e);
    } catch (KeeperException e) {
      if(e.code() == KeeperException.Code.NODEEXISTS) {
        LOG.debug("State node exists.");
      } else {
        LOG.warn("Failed to set state node in ZooKeeper", e);
      }
    }

    return false;
  }

  /**
   * Set a watcher on the master address ZNode. The watcher will be set unless
   * an exception occurs with ZooKeeper.
   * @param watcher Watcher to set on master address ZNode.
   * @return true if watcher was set, false otherwise.
   */
  public boolean watchMasterAddress(Watcher watcher) {
    try {
      zooKeeper.exists(masterElectionZNode, watcher);
    } catch (KeeperException e) {
      LOG.warn("Failed to set watcher on ZNode " + masterElectionZNode, e);
      return false;
    } catch (InterruptedException e) {
      LOG.warn("Failed to set watcher on ZNode " + masterElectionZNode, e);
      return false;
    }
    LOG.debug("Set watcher on master address ZNode " + masterElectionZNode);
    return true;
  }

  private HServerAddress readAddress(String znode, Watcher watcher) {
    try {
      return readAddressOrThrow(znode, watcher);
    } catch (IOException e) {
      LOG.debug("Failed to read " + e.getMessage());
      return null;
    }
  }

  private HServerAddress readAddressOrThrow(String znode, Watcher watcher) throws IOException {
    byte[] data;
    try {
      data = zooKeeper.getData(znode, watcher, null);
    } catch (InterruptedException e) {
      throw new IOException(e);
    } catch (KeeperException e) {
      throw new IOException(e);
    }

    String addressString = Bytes.toString(data);
    LOG.debug("Read ZNode " + znode + " got " + addressString);
    return new HServerAddress(addressString);
  }

  /**
   * Make sure this znode exists by creating it if it's missing
   * @param znode full path to znode
   * @return true if it works
   */
  public boolean ensureExists(final String znode) {
    try {
      Stat stat = zooKeeper.exists(znode, false);
      if (stat != null) {
        return true;
      }
      zooKeeper.create(znode, new byte[0],
                       Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
      LOG.debug("Created ZNode " + znode);
      return true;
    } catch (KeeperException.NodeExistsException e) {
      return true;      // ok, move on.
    } catch (KeeperException.NoNodeException e) {
      return ensureParentExists(znode) && ensureExists(znode);
    } catch (KeeperException e) {
      LOG.warn("Failed to create " + znode +
        " -- check quorum servers, currently=" + this.quorumServers, e);
    } catch (InterruptedException e) {
      LOG.warn("Failed to create " + znode +
        " -- check quorum servers, currently=" + this.quorumServers, e);
    }
    return false;
  }

  private boolean ensureParentExists(final String znode) {
    int index = znode.lastIndexOf(ZNODE_PATH_SEPARATOR);
    if (index <= 0) {   // Parent is root, which always exists.
      return true;
    }
    return ensureExists(znode.substring(0, index));
  }

  /**
   * Delete ZNode containing root region location.
   * @return true if operation succeeded, false otherwise.
   */
  public boolean deleteRootRegionLocation()  {
    if (!ensureParentExists(rootRegionZNode)) {
      return false;
    }

    try {
      deleteZNode(rootRegionZNode);
      return true;
    } catch (KeeperException.NoNodeException e) {
      return true;    // ok, move on.
    } catch (KeeperException e) {
      LOG.warn("Failed to delete " + rootRegionZNode + ": " + e);
    } catch (InterruptedException e) {
      LOG.warn("Failed to delete " + rootRegionZNode + ": " + e);
    }

    return false;
  }

  /**
   * Unrecursive deletion of specified znode
   * @param znode
   * @throws KeeperException
   * @throws InterruptedException
   */
  public void deleteZNode(String znode)
      throws KeeperException, InterruptedException {
    deleteZNode(znode, false);
  }

  /**
   * Optionnally recursive deletion of specified znode
   * @param znode
   * @param recursive
   * @throws KeeperException
   * @throws InterruptedException
   */
  public void deleteZNode(String znode, boolean recursive)
    throws KeeperException, InterruptedException {
    if (recursive) {
      LOG.info("deleteZNode get children for " + znode);
      List<String> znodes = this.zooKeeper.getChildren(znode, false);
      if (znodes.size() > 0) {
        for (String child : znodes) {
          String childFullPath = getZNode(znode, child);
          LOG.info("deleteZNode recursive call " + childFullPath);
          this.deleteZNode(childFullPath, true);
        }
      }
    }
    this.zooKeeper.delete(znode, -1);
    LOG.debug("Deleted ZNode " + znode);
  }

  private boolean createRootRegionLocation(String address) {
    byte[] data = Bytes.toBytes(address);
    try {
      zooKeeper.create(rootRegionZNode, data, Ids.OPEN_ACL_UNSAFE,
                       CreateMode.PERSISTENT);
      LOG.debug("Created ZNode " + rootRegionZNode + " with data " + address);
      return true;
    } catch (KeeperException e) {
      LOG.warn("Failed to create root region in ZooKeeper: " + e);
    } catch (InterruptedException e) {
      LOG.warn("Failed to create root region in ZooKeeper: " + e);
    }

    return false;
  }

  private boolean updateRootRegionLocation(String address) {
    byte[] data = Bytes.toBytes(address);
    try {
      zooKeeper.setData(rootRegionZNode, data, -1);
      LOG.debug("SetData of ZNode " + rootRegionZNode + " with " + address);
      return true;
    } catch (KeeperException e) {
      LOG.warn("Failed to set root region location in ZooKeeper: " + e);
    } catch (InterruptedException e) {
      LOG.warn("Failed to set root region location in ZooKeeper: " + e);
    }

    return false;
  }

  /**
   * Write root region location to ZooKeeper. If address is null, delete ZNode.
   * containing root region location.
   * @param address HServerAddress to write to ZK.
   * @return true if operation succeeded, false otherwise.
   */
  public boolean writeRootRegionLocation(HServerAddress address) {
    if (address == null) {
      return deleteRootRegionLocation();
    }

    if (!ensureParentExists(rootRegionZNode)) {
      return false;
    }

    String addressString = address.toString();

    if (checkExistenceOf(rootRegionZNode)) {
      return updateRootRegionLocation(addressString);
    }

    return createRootRegionLocation(addressString);
  }

  /**
   * Write address of master to ZooKeeper.
   * @param address HServerAddress of master.
   * @return true if operation succeeded, false otherwise.
   */
  public boolean writeMasterAddress(final HServerAddress address) {
    if (!ensureParentExists(masterElectionZNode)) {
      return false;
    }

    String addressStr = address.toString();
    byte[] data = Bytes.toBytes(addressStr);
    try {
      zooKeeper.create(masterElectionZNode, data, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
      LOG.debug("Wrote master address " + address + " to ZooKeeper");
      return true;
    } catch (InterruptedException e) {
      LOG.warn("Failed to write master address " + address + " to ZooKeeper", e);
    } catch (KeeperException e) {
      LOG.warn("Failed to write master address " + address + " to ZooKeeper", e);
    }

    return false;
  }

  /**
   * Write in ZK this RS startCode and address.
   * Ensures that the full path exists.
   * @param info The RS info
   * @return true if the location was written, false if it failed
   */
  public boolean writeRSLocation(HServerInfo info) {
    ensureExists(rsZNode);
    byte[] data = Bytes.toBytes(info.getServerAddress().toString());
    String znode = joinPath(rsZNode, Long.toString(info.getStartCode()));
    try {
      zooKeeper.create(znode, data, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
      LOG.debug("Created ZNode " + znode
          + " with data " + info.getServerAddress().toString());
      return true;
    } catch (KeeperException e) {
      LOG.warn("Failed to create " + znode + " znode in ZooKeeper: " + e);
    } catch (InterruptedException e) {
      LOG.warn("Failed to create " + znode + " znode in ZooKeeper: " + e);
    }
    return false;
  }

  /**
   * Update the RS address and set a watcher on the znode
   * @param info The RS info
   * @param watcher The watcher to put on the znode
   * @return true if the update is done, false if it failed
   */
  public boolean updateRSLocationGetWatch(HServerInfo info, Watcher watcher) {
    byte[] data = Bytes.toBytes(info.getServerAddress().toString());
    String znode = rsZNode + ZNODE_PATH_SEPARATOR + info.getStartCode();
    try {
      zooKeeper.setData(znode, data, -1);
      LOG.debug("Updated ZNode " + znode
          + " with data " + info.getServerAddress().toString());
      zooKeeper.getData(znode, watcher, null);
      return true;
    } catch (KeeperException e) {
      LOG.warn("Failed to update " + znode + " znode in ZooKeeper: " + e);
    } catch (InterruptedException e) {
      LOG.warn("Failed to update " + znode + " znode in ZooKeeper: " + e);
    }

    return false;
  }

  /**
   * Scans the regions servers directory
   * @return A list of server addresses
   */
  public List<HServerAddress> scanRSDirectory() {
    return scanAddressDirectory(rsZNode, null);
  }

  /**
   * Scans the regions servers directory and sets a watch on each znode
   * @param watcher a watch to use for each znode
   * @return A list of server addresses
   */
  public List<HServerAddress> scanRSDirectory(Watcher watcher) {
    return scanAddressDirectory(rsZNode, watcher);
  }

  /**
   * Method used to make sure the region server directory is empty.
   *
   */
  public void clearRSDirectory() {
    try {
      List<String> nodes = zooKeeper.getChildren(rsZNode, false);
      for (String node : nodes) {
        LOG.debug("Deleting node: " + node);
        zooKeeper.delete(joinPath(this.rsZNode, node), -1);
      }
    } catch (KeeperException e) {
      LOG.warn("Failed to delete " + rsZNode + " znodes in ZooKeeper: " + e);
    } catch (InterruptedException e) {
      LOG.warn("Failed to delete " + rsZNode + " znodes in ZooKeeper: " + e);
    }
  }

  private boolean checkExistenceOf(String path) {
    Stat stat = null;
    try {
      stat = zooKeeper.exists(path, false);
    } catch (KeeperException e) {
      LOG.warn("checking existence of " + path, e);
    } catch (InterruptedException e) {
      LOG.warn("checking existence of " + path, e);
    }

    return stat != null;
  }

  /**
   * Close this ZooKeeper session.
   */
  public void close() {
    try {
      zooKeeper.close();
      LOG.debug("Closed connection with ZooKeeper; " + this.rootRegionZNode);
    } catch (InterruptedException e) {
      LOG.warn("Failed to close connection with ZooKeeper");
    }
  }

  public String getZNode(String parentZNode, String znodeName) {
    return znodeName.charAt(0) == ZNODE_PATH_SEPARATOR ?
        znodeName : joinPath(parentZNode, znodeName);
  }

  private String joinPath(String parent, String child) {
    return parent + ZNODE_PATH_SEPARATOR + child;
  }

  /**
   * Get the path of the masterElectionZNode
   * @return the path to masterElectionZNode
   */
  public String getMasterElectionZNode() {
    return masterElectionZNode;
  }

  /**
   * Get the path of the parent ZNode
   * @return path of that znode
   */
  public String getParentZNode() {
    return parentZNode;
  }

  /**
   * Scan a directory of address data.
   * @param znode The parent node
   * @param watcher The watcher to put on the found znodes, if not null
   * @return The directory contents
   */
  public List<HServerAddress> scanAddressDirectory(String znode,
      Watcher watcher) {
    List<HServerAddress> list = new ArrayList<HServerAddress>();
    List<String> nodes = this.listZnodes(znode, watcher);
    if(nodes == null) {
      return list;
    }
    for (String node : nodes) {
      String path = joinPath(znode, node);
      list.add(readAddress(path, watcher));
    }
    return list;
  }

  /**
   * List all znodes in the specified path and set a watcher on each
   * @param znode path to list
   * @param watcher watch to set, can be null
   * @return a list of all the znodes
   */
  public List<String> listZnodes(String znode, Watcher watcher) {
    List<String> nodes = null;
    try {
      if (checkExistenceOf(znode)) {
        if (watcher == null) {
          nodes = zooKeeper.getChildren(znode, false);
        } else {
          nodes = zooKeeper.getChildren(znode, watcher);
          for (String node : nodes) {
            getDataAndWatch(znode, node, watcher);
          }
        }

      }
    } catch (KeeperException e) {
      LOG.warn("Failed to read " + znode + " znode in ZooKeeper: " + e);
    } catch (InterruptedException e) {
      LOG.warn("Failed to read " + znode + " znode in ZooKeeper: " + e);
    }
    return nodes;
  }

  public String getData(String parentZNode, String znode) {
    return getDataAndWatch(parentZNode, znode, null);
  }

  public String getDataAndWatch(String parentZNode,
                                String znode, Watcher watcher) {
    String data = null;
    try {
      String path = joinPath(parentZNode, znode);
      if (checkExistenceOf(path)) {
        data = Bytes.toString(zooKeeper.getData(path, watcher, null));
      }
    } catch (KeeperException e) {
      LOG.warn("Failed to read " + znode + " znode in ZooKeeper: " + e);
    } catch (InterruptedException e) {
      LOG.warn("Failed to read " + znode + " znode in ZooKeeper: " + e);
    }
    return data;
  }

  /**
   * Write a znode and fail if it already exists
   * @param parentPath parent path to the new znode
   * @param child name of the znode
   * @param strData data to insert
   * @throws InterruptedException
   * @throws KeeperException
   */
  public void writeZNode(String parentPath, String child, String strData)
      throws InterruptedException, KeeperException {
    writeZNode(parentPath, child, strData, false);
  }


  /**
   * Write (and optionally over-write) a znode
   * @param parentPath parent path to the new znode
   * @param child name of the znode
   * @param strData data to insert
   * @param failOnWrite true if an exception should be returned if the znode
   * already exists, false if it should be overwritten
   * @throws InterruptedException
   * @throws KeeperException
   */
  public void writeZNode(String parentPath, String child, String strData,
      boolean failOnWrite) throws InterruptedException, KeeperException {
    String path = joinPath(parentPath, child);
    if (!ensureExists(parentPath)) {
      LOG.error("unable to ensure parent exists: " + parentPath);
    }
    byte[] data = Bytes.toBytes(strData);
    Stat stat = this.zooKeeper.exists(path, false);
    if (failOnWrite || stat == null) {
      this.zooKeeper.create(path, data,
          Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
      LOG.debug("Created " + path);
    } else {
      this.zooKeeper.setData(path, data, -1);
      LOG.debug("Updated " + path);
    }
  }

  public static String getZookeeperClusterKey(Configuration conf) {
    return conf.get(ZOOKEEPER_QUORUM)+":"+
          conf.get(ZOOKEEPER_ZNODE_PARENT);
  }

  /**
   * Get the path of this region server's znode
   * @return path to znode
   */
  public String getRsZNode() {
    return this.rsZNode;
  }

}

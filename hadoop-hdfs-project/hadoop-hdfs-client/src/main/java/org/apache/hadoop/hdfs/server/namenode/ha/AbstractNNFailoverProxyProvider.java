/**
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

package org.apache.hadoop.hdfs.server.namenode.ha;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.hdfs.DFSUtilClient;
import org.apache.hadoop.hdfs.HAUtilClient;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.io.retry.FailoverProxyProvider;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractNNFailoverProxyProvider<T> implements
    FailoverProxyProvider <T> {
  protected static final Logger LOG =
      LoggerFactory.getLogger(AbstractNNFailoverProxyProvider.class);

  protected Configuration conf;
  protected Class<T> xface;
  protected HAProxyFactory<T> factory;
  protected UserGroupInformation ugi;
  protected AtomicBoolean fallbackToSimpleAuth;

  protected AbstractNNFailoverProxyProvider() {
  }

  protected AbstractNNFailoverProxyProvider(Configuration conf, URI uri,
      Class<T> xface, HAProxyFactory<T> factory) {
    this.conf = new Configuration(conf);
    this.xface = xface;
    this.factory = factory;
    try {
      this.ugi = UserGroupInformation.getCurrentUser();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    int maxRetries = this.conf.getInt(
        HdfsClientConfigKeys.Failover.CONNECTION_RETRIES_KEY,
        HdfsClientConfigKeys.Failover.CONNECTION_RETRIES_DEFAULT);
    this.conf.setInt(
        CommonConfigurationKeysPublic.IPC_CLIENT_CONNECT_MAX_RETRIES_KEY,
        maxRetries);

    int maxRetriesOnSocketTimeouts = this.conf.getInt(
        HdfsClientConfigKeys
        .Failover.CONNECTION_RETRIES_ON_SOCKET_TIMEOUTS_KEY,
        HdfsClientConfigKeys
        .Failover.CONNECTION_RETRIES_ON_SOCKET_TIMEOUTS_DEFAULT);
    this.conf.setInt(
        CommonConfigurationKeysPublic
        .IPC_CLIENT_CONNECT_MAX_RETRIES_ON_SOCKET_TIMEOUTS_KEY,
        maxRetriesOnSocketTimeouts);
  }

  /**
   * Inquire whether logical HA URI is used for the implementation. If it is
   * used, a special token handling may be needed to make sure a token acquired
   * from a node in the HA pair can be used against the other node.
   *
   * @return true if logical HA URI is used. false, if not used.
   */
  public abstract boolean useLogicalURI();

  /**
   * Set for tracking if a secure client falls back to simple auth.  This method
   * is synchronized only to stifle a Findbugs warning.
   *
   * @param fallbackToSimpleAuth - set to true or false during this method to
   *   indicate if a secure client falls back to simple auth
   */
  public synchronized void setFallbackToSimpleAuth(
      AtomicBoolean fallbackToSimpleAuth) {
    this.fallbackToSimpleAuth = fallbackToSimpleAuth;
  }

  public synchronized AtomicBoolean getFallbackToSimpleAuth() {
    return fallbackToSimpleAuth;
  }

  /**
   * ProxyInfo to a NameNode. Includes its address.
   */
  public static class NNProxyInfo<T> extends ProxyInfo<T> {
    private InetSocketAddress address;

    public NNProxyInfo(InetSocketAddress address) {
      super(null, address.toString());
      this.address = address;
    }

    public InetSocketAddress getAddress() {
      return address;
    }
  }

  @Override
  public Class<T> getInterface() {
    return xface;
  }

  /**
   * Create a proxy if it has not been created yet.
   */
  protected NNProxyInfo<T> createProxyIfNeeded(NNProxyInfo<T> pi) {
    if (pi.proxy == null) {
      assert pi.getAddress() != null : "Proxy address is null";
      try {
        pi.proxy = factory.createProxy(conf,
            pi.getAddress(), xface, ugi, false, getFallbackToSimpleAuth());
      } catch (IOException ioe) {
        LOG.error("{} Failed to create RPC proxy to NameNode",
            this.getClass().getSimpleName(), ioe);
        throw new RuntimeException(ioe);
      }
    }
    return pi;
  }

  /**
   * Get list of configured NameNode proxy addresses.
   * Randomize the list if requested.
   */
  protected List<NNProxyInfo<T>> getProxyAddresses(URI uri, String addressKey) {
    final List<NNProxyInfo<T>> proxies = new ArrayList<NNProxyInfo<T>>();
    Map<String, Map<String, InetSocketAddress>> map =
        DFSUtilClient.getAddresses(conf, null, addressKey);
    Map<String, InetSocketAddress> addressesInNN = map.get(uri.getHost());

    if (addressesInNN == null || addressesInNN.size() == 0) {
      throw new RuntimeException("Could not find any configured addresses " +
          "for URI " + uri);
    }

    Collection<InetSocketAddress> addressesOfNns = addressesInNN.values();
    for (InetSocketAddress address : addressesOfNns) {
      proxies.add(new NNProxyInfo<T>(address));
    }
    // Randomize the list to prevent all clients pointing to the same one
    boolean randomized = getRandomOrder(conf, uri);
    if (randomized) {
      Collections.shuffle(proxies);
    }

    // The client may have a delegation token set for the logical
    // URI of the cluster. Clone this token to apply to each of the
    // underlying IPC addresses so that the IPC code can find it.
    HAUtilClient.cloneDelegationTokenForLogicalUri(ugi, uri, addressesOfNns);
    return proxies;
  }

  /**
   * Check whether random order is configured for failover proxy provider
   * for the namenode/nameservice.
   *
   * @param conf Configuration
   * @param nameNodeUri The URI of namenode/nameservice
   * @return random order configuration
   */
  public static boolean getRandomOrder(
      Configuration conf, URI nameNodeUri) {
    String host = nameNodeUri.getHost();
    String configKeyWithHost = HdfsClientConfigKeys.Failover.RANDOM_ORDER
        + "." + host;

    if (conf.get(configKeyWithHost) != null) {
      return conf.getBoolean(
          configKeyWithHost,
          HdfsClientConfigKeys.Failover.RANDOM_ORDER_DEFAULT);
    }

    return conf.getBoolean(
        HdfsClientConfigKeys.Failover.RANDOM_ORDER,
        HdfsClientConfigKeys.Failover.RANDOM_ORDER_DEFAULT);
  }
}

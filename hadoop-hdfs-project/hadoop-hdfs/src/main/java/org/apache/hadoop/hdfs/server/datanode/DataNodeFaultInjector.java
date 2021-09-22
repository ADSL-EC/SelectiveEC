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
package org.apache.hadoop.hdfs.server.datanode;

import com.google.common.annotations.VisibleForTesting;

import org.apache.hadoop.classification.InterfaceAudience;

import java.io.FileNotFoundException;
import java.io.IOException;

/**
 * Used for injecting faults in DFSClient and DFSOutputStream tests.
 * Calls into this are a no-op in production code. 
 */
@VisibleForTesting
@InterfaceAudience.Private
public class DataNodeFaultInjector {
  private static DataNodeFaultInjector instance = new DataNodeFaultInjector();

  public static DataNodeFaultInjector get() {
    return instance;
  }

  public static void set(DataNodeFaultInjector injector) {
    instance = injector;
  }

  public void getHdfsBlocksMetadata() {}

  public void writeBlockAfterFlush() throws IOException {}

  public void sendShortCircuitShmResponse() throws IOException {}

  public boolean dropHeartbeatPacket() {
    return false;
  }

  public void stopSendingPacketDownstream(final String mirrAddr)
      throws IOException {
  }

  /**
   * Used as a hook to intercept the latency of sending packet.
   */
  public void logDelaySendingPacketDownstream(
      final String mirrAddr,
      final long delayMs) throws IOException {
  }

  public void delaySendingAckToUpstream(final String upstreamAddr)
      throws IOException {
  }

  /**
   * Used as a hook to intercept the latency of sending ack.
   */
  public void logDelaySendingAckToUpstream(
      final String upstreamAddr,
      final long delayMs)
      throws IOException {
  }

  public void noRegistration() throws IOException { }

  public void failMirrorConnection() throws IOException { }

  public void failPipeline(ReplicaInPipeline replicaInfo,
      String mirrorAddr) throws IOException { }

  public void startOfferService() throws Exception {}

  public void endOfferService() throws Exception {}

  public void throwTooManyOpenFiles() throws FileNotFoundException {
  }

  /**
   * Used as a hook to inject failure in erasure coding reconstruction
   * process.
   */
  public void stripedBlockReconstruction() throws IOException {}
}

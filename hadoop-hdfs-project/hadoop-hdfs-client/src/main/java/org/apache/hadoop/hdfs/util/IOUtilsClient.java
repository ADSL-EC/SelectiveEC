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
package org.apache.hadoop.hdfs.util;

import org.apache.hadoop.hdfs.BlockReader;
import org.apache.hadoop.hdfs.ReadStatistics;
import org.slf4j.Logger;

import java.io.IOException;

public class IOUtilsClient {
  /**
   * Close the Closeable objects and <b>ignore</b> any {@link IOException} or
   * null pointers. Must only be used for cleanup in exception handlers.
   *
   * @param log the log to record problems to at debug level. Can be null.
   * @param closeables the objects to close
   */
  public static void cleanup(Logger log, java.io.Closeable... closeables) {
    for (java.io.Closeable c : closeables) {
      if (c != null) {
        try {
          c.close();
        } catch(Throwable e) {
          if (log != null && log.isDebugEnabled()) {
            log.debug("Exception in closing " + c, e);
          }
        }
      }
    }
  }

  public static void updateReadStatistics(ReadStatistics readStatistics,
                                      int nRead, BlockReader blockReader) {
    updateReadStatistics(readStatistics, nRead, blockReader.isShortCircuit(),
        blockReader.getNetworkDistance());
  }

  public static void updateReadStatistics(ReadStatistics readStatistics,
      int nRead, boolean isShortCircuit, int networkDistance) {
    if (nRead <= 0) {
      return;
    }

    if (isShortCircuit) {
      readStatistics.addShortCircuitBytes(nRead);
    } else if (networkDistance == 0) {
      readStatistics.addLocalBytes(nRead);
    } else {
      readStatistics.addRemoteBytes(nRead);
    }
  }
}

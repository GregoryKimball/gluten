/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.gluten.trino.server;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableMap;
import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.units.DataSize;

import java.util.Map;

public class GlutenTrinoConfig {
  private boolean enabled = true;
  private String libraryPath = "/usr/lib/gluten/libgluten_trino.so";
  private boolean cudfEnabled = true;
  private boolean ucxExchangeEnabled;
  private String spillDir = "/tmp/gluten-spill";
  private DataSize queueSize = DataSize.of(64, DataSize.Unit.MEGABYTE);
  private Map<String, String> nativeConf = ImmutableMap.of();

  public boolean isEnabled() {
    return enabled;
  }

  @Config("gluten.enabled")
  @ConfigDescription("Run fragments that translate to Substrait in Gluten")
  public GlutenTrinoConfig setEnabled(boolean enabled) {
    this.enabled = enabled;
    return this;
  }

  public String getLibraryPath() {
    return libraryPath;
  }

  @Config("gluten.library-path")
  public GlutenTrinoConfig setLibraryPath(String libraryPath) {
    this.libraryPath = libraryPath;
    return this;
  }

  public boolean isCudfEnabled() {
    return cudfEnabled;
  }

  @Config("gluten.cudf.enabled")
  public GlutenTrinoConfig setCudfEnabled(boolean cudfEnabled) {
    this.cudfEnabled = cudfEnabled;
    return this;
  }

  public boolean isUcxExchangeEnabled() {
    return ucxExchangeEnabled;
  }

  @Config("gluten.ucx-exchange.enabled")
  @ConfigDescription("Move data between native tasks over the cuDF UCX exchange; Trino's exchange carries only control")
  public GlutenTrinoConfig setUcxExchangeEnabled(boolean ucxExchangeEnabled) {
    this.ucxExchangeEnabled = ucxExchangeEnabled;
    return this;
  }

  public String getSpillDir() {
    return spillDir;
  }

  @Config("gluten.spill-dir")
  public GlutenTrinoConfig setSpillDir(String spillDir) {
    this.spillDir = spillDir;
    return this;
  }

  public DataSize getQueueSize() {
    return queueSize;
  }

  @Config("gluten.queue-size")
  @ConfigDescription("Bytes buffered between Trino drivers and each native task input or output")
  public GlutenTrinoConfig setQueueSize(DataSize queueSize) {
    this.queueSize = queueSize;
    return this;
  }

  public Map<String, String> getNativeConf() {
    return nativeConf;
  }

  @Config("gluten.native-conf")
  @ConfigDescription("Comma-separated key=value Gluten and Velox settings, such as spark.gluten.* and spark.hadoop.fs.s3a.*")
  public GlutenTrinoConfig setNativeConf(String nativeConf) {
    this.nativeConf = Splitter.on(',').trimResults().omitEmptyStrings().withKeyValueSeparator('=').split(nativeConf);
    return this;
  }
}

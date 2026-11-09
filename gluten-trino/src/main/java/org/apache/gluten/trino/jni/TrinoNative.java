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
package org.apache.gluten.trino.jni;

import java.util.Map;

/** Bindings for libgluten_trino.so (cpp/velox/trino/TrinoJniWrapper.cc). */
public final class TrinoNative {
  private static boolean loaded;

  private TrinoNative() {}

  /** Loads the native library and initializes the Velox backend once per JVM. */
  public static synchronized void load(String libraryPath, Map<String, String> backendConf) {
    if (loaded) {
      return;
    }
    System.load(libraryPath);
    String[][] conf = toArrays(backendConf);
    initialize(conf[0], conf[1]);
    loaded = true;
  }

  public static long createTask(
      byte[] plan,
      byte[][] splitInfos,
      NativePageSource[] inputs,
      byte[][] inputSchemas,
      Map<String, String> sessionConf,
      String spillDir) {
    String[][] conf = toArrays(sessionConf);
    return createTask(plan, splitInfos, inputs, inputSchemas, conf[0], conf[1], spillDir);
  }

  private static String[][] toArrays(Map<String, String> conf) {
    String[] keys = conf.keySet().toArray(String[]::new);
    String[] values = new String[keys.length];
    for (int i = 0; i < keys.length; i++) {
      values[i] = conf.get(keys[i]);
    }
    return new String[][] {keys, values};
  }

  private static native void initialize(String[] confKeys, String[] confValues);

  private static native long createTask(
      byte[] plan,
      byte[][] splitInfos,
      NativePageSource[] inputs,
      byte[][] inputSchemas,
      String[] confKeys,
      String[] confValues,
      String spillDir);

  public static native String planString(long handle);

  /** Returns the next output page in the {@link NativePageSource} layout, or null when done. */
  public static native byte[] next(long handle);

  /** Adds an upstream task's result location to the UCX exchange reading plan input {@code input}. */
  public static native void addRemoteSplit(long handle, int input, String location);

  public static native void noMoreRemoteSplits(long handle, int input);

  /** Sets the final destination count of a broadcast or arbitrary UCX output. */
  public static native void noMoreOutputBuffers(long handle, int numBuffers);

  public static native void close(long handle);
}

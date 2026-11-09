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

/**
 * Supplies pages to a native task input. Called from native threads; the native side resolves
 * {@link #next()} by name in JNI_OnLoad.
 */
public interface NativePageSource {
  /**
   * Blocks until a page is available and returns it as an int32 position count followed by the
   * raw page (see {@code RawPages}), or returns null at end of stream.
   */
  byte[] next();
}

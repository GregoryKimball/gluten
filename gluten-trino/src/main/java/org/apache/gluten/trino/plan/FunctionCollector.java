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
package org.apache.gluten.trino.plan;

import io.substrait.proto.Plan;
import io.substrait.proto.SimpleExtensionDeclaration;

import java.util.LinkedHashMap;
import java.util.Map;

/** Assigns Substrait function anchors and emits the plan's extension declarations. */
public final class FunctionCollector {
  private final Map<String, Integer> anchors = new LinkedHashMap<>();

  public int anchor(String signature) {
    return anchors.computeIfAbsent(signature, ignored -> anchors.size());
  }

  public void addTo(Plan.Builder plan) {
    anchors.forEach(
        (signature, anchor) ->
            plan.addExtensions(
                SimpleExtensionDeclaration.newBuilder()
                    .setExtensionFunction(
                        SimpleExtensionDeclaration.ExtensionFunction.newBuilder()
                            .setFunctionAnchor(anchor)
                            .setName(signature))));
  }
}

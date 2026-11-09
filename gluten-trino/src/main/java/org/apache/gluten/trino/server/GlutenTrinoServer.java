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

import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;
import io.trino.execution.TaskExecutionFactory;
import io.trino.server.Server;
import org.apache.gluten.trino.exec.GlutenTaskExecutionFactory;

import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static java.util.Objects.requireNonNullElse;

/** Trino server main class that runs eligible worker fragments in Gluten. */
public final class GlutenTrinoServer extends Server {
  public static void main(String[] args) {
    String version = Server.class.getPackage().getImplementationVersion();
    new GlutenTrinoServer().start(requireNonNullElse(version, "unknown"));
  }

  @Override
  protected Iterable<? extends Module> getAdditionalModules() {
    return ImmutableList.of(new GlutenModule());
  }

  private static final class GlutenModule implements Module {
    @Override
    public void configure(Binder binder) {
      configBinder(binder).bindConfig(GlutenTrinoConfig.class);
      newOptionalBinder(binder, TaskExecutionFactory.Decorator.class)
          .setBinding()
          .to(GlutenTaskExecutionFactory.class)
          .in(Scopes.SINGLETON);
    }
  }
}

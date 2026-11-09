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
package org.apache.gluten.trino.exec;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.opentelemetry.api.trace.Span;
import io.trino.Session;
import io.trino.exchange.ExchangeEncryptionKey;
import io.trino.exchange.ExchangeManagerRegistry;
import io.trino.execution.SqlTaskExecution;
import io.trino.execution.SqlTaskExecutionFactory;
import io.trino.execution.TaskExecution;
import io.trino.execution.TaskExecutionFactory;
import io.trino.execution.TaskManagerConfig;
import io.trino.execution.TaskStateMachine;
import io.trino.execution.buffer.OutputBuffer;
import io.trino.memory.QueryContext;
import io.trino.operator.DirectExchangeClientSupplier;
import io.trino.operator.DriverFactory;
import io.trino.operator.ExchangeOperator;
import io.trino.operator.OperatorFactory;
import io.trino.operator.TaskContext;
import io.trino.operator.output.PartitionedOutputOperator.PartitionedOutputFactory;
import io.trino.operator.output.PositionsAppenderFactory;
import io.trino.operator.output.TaskOutputOperator.TaskOutputFactory;
import io.trino.operator.OutputFactory;
import io.trino.spi.block.BlockEncodingSerde;
import io.trino.spi.connector.ConnectorTableCredentials;
import io.trino.spi.predicate.NullableValue;
import io.trino.spi.type.Type;
import io.trino.sql.planner.LocalExecutionPlanner.LocalExecutionPlan;
import io.trino.sql.planner.PartitionFunctionProvider;
import io.trino.sql.planner.PartitioningHandle;
import io.trino.sql.planner.PartitioningScheme;
import io.trino.sql.planner.PlanFragment;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.operator.RetryPolicy;
import io.trino.spi.TrinoException;
import io.trino.type.BlockTypeOperators;
import org.apache.gluten.trino.plan.PlanTranslator;
import org.apache.gluten.trino.plan.UnsupportedPlanException;
import org.apache.gluten.trino.server.GlutenTrinoConfig;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Function;

import static io.trino.SystemSessionProperties.getPagePartitioningBufferPoolSize;
import static io.trino.SystemSessionProperties.getRetryPolicy;
import static io.trino.execution.buffer.PagesSerdes.createExchangePagesSerdeFactory;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.sql.planner.SystemPartitioningHandle.COORDINATOR_DISTRIBUTION;
import static io.trino.sql.planner.SystemPartitioningHandle.FIXED_ARBITRARY_DISTRIBUTION;
import static io.trino.sql.planner.SystemPartitioningHandle.FIXED_BROADCAST_DISTRIBUTION;
import static io.trino.sql.planner.SystemPartitioningHandle.SINGLE_DISTRIBUTION;

/**
 * Runs each fragment that translates to Substrait as a native Gluten task, and every other
 * fragment with Trino's default execution.
 */
public final class GlutenTaskExecutionFactory implements TaskExecutionFactory.Decorator {
  private static final Logger log = Logger.get(GlutenTaskExecutionFactory.class);
  private static final Set<PartitioningHandle> UNPARTITIONED_OUTPUTS = Set.of(
      FIXED_BROADCAST_DISTRIBUTION, FIXED_ARBITRARY_DISTRIBUTION, SINGLE_DISTRIBUTION, COORDINATOR_DISTRIBUTION);

  private final GlutenTrinoConfig config;
  private final BlockEncodingSerde serde;
  private final DirectExchangeClientSupplier exchangeClientSupplier;
  private final ExchangeManagerRegistry exchangeManagerRegistry;
  private final PartitionFunctionProvider partitionFunctionProvider;
  private final PositionsAppenderFactory positionsAppenderFactory;
  private final TaskManagerConfig taskManagerConfig;
  private final ExecutorService nativeExecutor = Executors.newCachedThreadPool(runnable -> {
    Thread thread = new Thread(runnable);
    thread.setDaemon(true);
    return thread;
  });

  @Inject
  public GlutenTaskExecutionFactory(
      GlutenTrinoConfig config,
      BlockEncodingSerde serde,
      DirectExchangeClientSupplier exchangeClientSupplier,
      ExchangeManagerRegistry exchangeManagerRegistry,
      PartitionFunctionProvider partitionFunctionProvider,
      BlockTypeOperators blockTypeOperators,
      TaskManagerConfig taskManagerConfig) {
    this.config = config;
    this.serde = serde;
    this.exchangeClientSupplier = exchangeClientSupplier;
    this.exchangeManagerRegistry = exchangeManagerRegistry;
    this.partitionFunctionProvider = partitionFunctionProvider;
    this.positionsAppenderFactory = new PositionsAppenderFactory(blockTypeOperators);
    this.taskManagerConfig = taskManagerConfig;
    if (config.isEnabled()) {
      Map<String, String> backendConf = new HashMap<>(config.getNativeConf());
      backendConf.put("spark.gluten.sql.columnar.cudf", Boolean.toString(config.isCudfEnabled()));
      org.apache.gluten.trino.jni.TrinoNative.load(config.getLibraryPath(), backendConf);
    }
  }

  @Override
  public TaskExecutionFactory decorate(SqlTaskExecutionFactory defaultFactory) {
    return (session, span, queryContext, stateMachine, outputBuffer, fragment, credentials, notify) -> {
      Optional<PlanTranslator.Translation> translation = translate(session, fragment);
      if (translation.isEmpty()) {
        return defaultFactory.create(session, span, queryContext, stateMachine, outputBuffer, fragment, credentials, notify);
      }
      Map<String, String> sessionConf = new HashMap<>(config.getNativeConf());
      sessionConf.put("spark.gluten.sql.columnar.cudf", Boolean.toString(config.isCudfEnabled()));
      sessionConf.put("spark.sql.session.timeZone", session.getTimeZoneKey().getId());
      NativeTask nativeTask = new NativeTask(
          stateMachine.getTaskId().toString(),
          translation.get(),
          serde,
          sessionConf,
          config.getSpillDir(),
          nativeExecutor,
          config.getQueueSize().toBytes());
      SqlTaskExecution execution = defaultFactory.create(
          session, span, queryContext, stateMachine, outputBuffer, credentials, notify,
          taskContext -> plan(taskContext, fragment, translation.get(), nativeTask, outputBuffer));
      return new GlutenTaskExecution(execution, nativeTask, translation.get().scan().map(PlanNode::getId));
    };
  }

  private Optional<PlanTranslator.Translation> translate(Session session, PlanFragment fragment) {
    if (!config.isEnabled()) {
      return Optional.empty();
    }
    try {
      if (getRetryPolicy(session) != RetryPolicy.NONE) {
        throw new UnsupportedPlanException("retry policy " + getRetryPolicy(session));
      }
      if (fragment.getOutputSkewedBucketCount().isPresent()) {
        throw new UnsupportedPlanException("skewed output buckets");
      }
      PlanTranslator.Translation translation = PlanTranslator.translate(fragment);
      log.info("Fragment %s of query %s runs natively", fragment.getId(), session.getQueryId());
      return Optional.of(translation);
    } catch (UnsupportedPlanException e) {
      throw new TrinoException(NOT_SUPPORTED, "Fragment %s is not supported natively: %s".formatted(fragment.getId(), e.getMessage()), e);
    }
  }

  private LocalExecutionPlan plan(
      TaskContext taskContext,
      PlanFragment fragment,
      PlanTranslator.Translation translation,
      NativeTask nativeTask,
      OutputBuffer outputBuffer) {
    Session session = taskContext.getSession();
    var serdeFactory = createExchangePagesSerdeFactory(serde, session);
    ImmutableList.Builder<DriverFactory> drivers = ImmutableList.builder();
    int pipelineId = 0;
    for (PlanTranslator.RemoteInput input : translation.inputs()) {
      PlanNodeId nodeId = input.node().getId();
      List<OperatorFactory> operators = ImmutableList.of(
          new ExchangeOperator.ExchangeOperatorFactory(
              0, nodeId, exchangeClientSupplier, serdeFactory, RetryPolicy.NONE, exchangeManagerRegistry, input.types()),
          new NativeOperators.InputSinkFactory(1, nodeId, nativeTask, input.index()));
      drivers.add(new DriverFactory(pipelineId++, true, false, operators, OptionalInt.of(1)));
    }

    PlanNodeId rootId = fragment.getRoot().getId();
    OutputFactory output = outputFactory(taskContext, fragment.getOutputPartitioningScheme(), outputBuffer);
    List<OperatorFactory> operators = ImmutableList.of(
        new NativeOperators.OutputSourceFactory(0, rootId, nativeTask),
        output.createOutputOperator(1, rootId, translation.outputTypes(), Function.identity(), serdeFactory));
    drivers.add(new DriverFactory(pipelineId, true, true, operators, OptionalInt.of(1)));
    return new LocalExecutionPlan(drivers.build(), ImmutableList.of());
  }

  // Mirrors LocalExecutionPlanner.plan for outputs without skew rebalancing.
  private OutputFactory outputFactory(TaskContext taskContext, PartitioningScheme scheme, OutputBuffer outputBuffer) {
    if (UNPARTITIONED_OUTPUTS.contains(scheme.getPartitioning().getHandle())) {
      return new TaskOutputFactory(outputBuffer);
    }
    List<Symbol> layout = scheme.getOutputLayout();
    var arguments = scheme.getPartitioning().getArguments();
    List<Integer> channels = arguments.stream()
        .map(argument -> argument.isConstant() ? -1 : layout.indexOf(argument.getColumn()))
        .collect(ImmutableList.toImmutableList());
    List<Optional<NullableValue>> constants = arguments.stream()
        .map(argument -> argument.isConstant() ? Optional.of(argument.getConstant()) : Optional.<NullableValue>empty())
        .collect(ImmutableList.toImmutableList());
    List<Type> types = arguments.stream()
        .map(argument -> argument.isConstant() ? argument.getConstant().getType() : argument.getColumn().type())
        .collect(ImmutableList.toImmutableList());
    var partitionFunction = partitionFunctionProvider.getPartitionFunction(
        taskContext.getSession(),
        scheme.getPartitioning().getHandle(),
        types,
        scheme.getBucketToPartition().orElseThrow(() -> new IllegalArgumentException("Bucket to partition must be set")));
    OptionalInt nullChannel = OptionalInt.empty();
    Set<Symbol> columns = scheme.getPartitioning().getColumns();
    if (scheme.isReplicateNullsAndAny() && columns.size() == 1) {
      nullChannel = OptionalInt.of(layout.indexOf(columns.iterator().next()));
    }
    Optional<Slice> encryptionKey = ExchangeEncryptionKey.keyFor(taskContext.getSession(), outputBuffer);
    return new PartitionedOutputFactory(
        partitionFunction,
        channels,
        constants,
        scheme.isReplicateNullsAndAny(),
        nullChannel,
        outputBuffer,
        taskManagerConfig.getMaxPagePartitioningBufferSize(),
        positionsAppenderFactory,
        encryptionKey,
        taskContext.newAggregateMemoryContext(),
        getPagePartitioningBufferPoolSize(taskContext.getSession()),
        Optional.empty());
  }
}

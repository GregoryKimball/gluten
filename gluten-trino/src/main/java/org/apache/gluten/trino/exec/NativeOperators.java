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

import com.google.common.util.concurrent.ListenableFuture;
import io.trino.operator.DriverContext;
import io.trino.operator.Operator;
import io.trino.operator.OperatorContext;
import io.trino.operator.OperatorFactory;
import io.trino.spi.Page;
import io.trino.sql.planner.plan.PlanNodeId;

import static io.trino.operator.Operator.NOT_BLOCKED;

/** Trino operators at the boundary of a native task. */
public final class NativeOperators {
  private NativeOperators() {}

  /** Produces the native task's output pages; it is the first operator of the output pipeline. */
  public static final class OutputSourceFactory implements OperatorFactory {
    private final int operatorId;
    private final PlanNodeId planNodeId;
    private final NativeTask task;

    public OutputSourceFactory(int operatorId, PlanNodeId planNodeId, NativeTask task) {
      this.operatorId = operatorId;
      this.planNodeId = planNodeId;
      this.task = task;
    }

    @Override
    public Operator createOperator(DriverContext driverContext) {
      OperatorContext context =
          driverContext.addOperatorContext(operatorId, planNodeId, OutputSource.class.getSimpleName());
      return new OutputSource(context, task);
    }

    @Override
    public void noMoreOperators() {}

    @Override
    public OperatorFactory duplicate() {
      throw new UnsupportedOperationException();
    }
  }

  private static final class OutputSource implements Operator {
    private final OperatorContext context;
    private final NativeTask task;

    OutputSource(OperatorContext context, NativeTask task) {
      this.context = context;
      this.task = task;
    }

    @Override
    public OperatorContext getOperatorContext() {
      return context;
    }

    @Override
    public ListenableFuture<Void> isBlocked() {
      ListenableFuture<Void> notEmpty = task.output().whenNotEmpty();
      return notEmpty.isDone() ? NOT_BLOCKED : notEmpty;
    }

    @Override
    public boolean needsInput() {
      return false;
    }

    @Override
    public void addInput(Page page) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Page getOutput() {
      Page page = task.output().poll().orElse(null);
      if (page != null) {
        context.recordProcessedInput(page.getSizeInBytes(), page.getPositionCount());
      }
      return page;
    }

    @Override
    public void finish() {}

    @Override
    public boolean isFinished() {
      return task.output().isDrained();
    }

    @Override
    public void close() {
      task.close();
    }
  }

  /**
   * Serializes a remote source's pages into a native task input. The pipeline must run a single
   * driver, whose finish ends the input.
   */
  public static final class InputSinkFactory implements OperatorFactory {
    private final int operatorId;
    private final PlanNodeId planNodeId;
    private final NativeTask task;
    private final int input;

    public InputSinkFactory(int operatorId, PlanNodeId planNodeId, NativeTask task, int input) {
      this.operatorId = operatorId;
      this.planNodeId = planNodeId;
      this.task = task;
      this.input = input;
    }

    @Override
    public Operator createOperator(DriverContext driverContext) {
      OperatorContext context =
          driverContext.addOperatorContext(operatorId, planNodeId, InputSink.class.getSimpleName());
      return new InputSink(context, task, input);
    }

    @Override
    public void noMoreOperators() {}

    @Override
    public OperatorFactory duplicate() {
      throw new UnsupportedOperationException();
    }
  }

  /**
   * Ends a remote source whose data the native task reads over the UCX exchange. Trino's exchange
   * still runs so that upstream tasks see their (empty) output buffers consumed and finish.
   */
  public static final class DrainSinkFactory implements OperatorFactory {
    private final int operatorId;
    private final PlanNodeId planNodeId;

    public DrainSinkFactory(int operatorId, PlanNodeId planNodeId) {
      this.operatorId = operatorId;
      this.planNodeId = planNodeId;
    }

    @Override
    public Operator createOperator(DriverContext driverContext) {
      return new DrainSink(driverContext.addOperatorContext(operatorId, planNodeId, DrainSink.class.getSimpleName()));
    }

    @Override
    public void noMoreOperators() {}

    @Override
    public OperatorFactory duplicate() {
      throw new UnsupportedOperationException();
    }
  }

  private static final class DrainSink implements Operator {
    private final OperatorContext context;
    private boolean finished;

    DrainSink(OperatorContext context) {
      this.context = context;
    }

    @Override
    public OperatorContext getOperatorContext() {
      return context;
    }

    @Override
    public boolean needsInput() {
      return !finished;
    }

    @Override
    public void addInput(Page page) {
      if (page.getPositionCount() > 0) {
        throw new IllegalStateException("Upstream native task sent " + page.getPositionCount() + " rows through Trino's exchange");
      }
    }

    @Override
    public Page getOutput() {
      return null;
    }

    @Override
    public void finish() {
      finished = true;
    }

    @Override
    public boolean isFinished() {
      return finished;
    }

    @Override
    public void close() {}
  }

  private static final class InputSink implements Operator {
    private final OperatorContext context;
    private final NativeTask task;
    private final BoundedQueue<byte[]> queue;
    private boolean finished;

    InputSink(OperatorContext context, NativeTask task, int input) {
      this.context = context;
      this.task = task;
      this.queue = task.input(input);
    }

    @Override
    public OperatorContext getOperatorContext() {
      return context;
    }

    @Override
    public ListenableFuture<Void> isBlocked() {
      ListenableFuture<Void> notFull = queue.whenNotFull();
      return notFull.isDone() ? NOT_BLOCKED : notFull;
    }

    @Override
    public boolean needsInput() {
      return !finished && queue.whenNotFull().isDone();
    }

    @Override
    public void addInput(Page page) {
      byte[] bytes = RawPages.toBytes(task.serde(), page);
      queue.add(bytes, bytes.length);
    }

    @Override
    public Page getOutput() {
      return null;
    }

    @Override
    public void finish() {
      finished = true;
      queue.finish();
    }

    @Override
    public boolean isFinished() {
      return finished;
    }

    @Override
    public void close() {}
  }
}

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
import com.google.common.collect.ImmutableSet;
import io.trino.exchange.DirectExchangeInput;
import io.trino.execution.ScheduledSplit;
import io.trino.execution.SplitAssignment;
import io.trino.execution.SqlTaskExecution;
import io.trino.execution.TaskExecution;
import io.trino.metadata.Split;
import io.trino.operator.TaskContext;
import io.trino.split.RemoteSplit;
import io.trino.sql.planner.plan.PlanNodeId;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Runs a fragment natively. Remote-source splits go to the wrapped {@link SqlTaskExecution},
 * whose exchange pipelines feed the native task; table-scan splits are collected here and handed
 * to the native task once the coordinator has sent all of them. With the UCX exchange, remote-source
 * splits also go to the native task, which fetches their data over UCX.
 */
public final class GlutenTaskExecution implements TaskExecution {
  private final SqlTaskExecution delegate;
  private final NativeTask nativeTask;
  private final Optional<PlanNodeId> scanNode;
  private final Map<PlanNodeId, Integer> ucxInputs;
  private final Set<String> seenRemoteSplits = new HashSet<>();
  private final Set<PlanNodeId> noMoreRemoteSplits = new HashSet<>();
  private final Set<Long> seenSequenceIds = new HashSet<>();
  private final List<Split> scanSplits = new ArrayList<>();
  private boolean noMoreScanSplits;

  public GlutenTaskExecution(
      SqlTaskExecution delegate,
      NativeTask nativeTask,
      Optional<PlanNodeId> scanNode,
      Map<PlanNodeId, Integer> ucxInputs) {
    this.delegate = delegate;
    this.nativeTask = nativeTask;
    this.scanNode = scanNode;
    this.ucxInputs = Map.copyOf(ucxInputs);
  }

  @Override
  public TaskContext getTaskContext() {
    return delegate.getTaskContext();
  }

  @Override
  public void start() {
    delegate.start();
    if (scanNode.isEmpty()) {
      nativeTask.start(ImmutableList.of());
    }
  }

  @Override
  public void addSplitAssignments(List<SplitAssignment> splitAssignments) {
    List<SplitAssignment> remote = new ArrayList<>();
    boolean startNative = false;
    synchronized (this) {
      for (SplitAssignment assignment : splitAssignments) {
        if (!scanNode.map(assignment.getPlanNodeId()::equals).orElse(false)) {
          remote.add(assignment);
          Integer input = ucxInputs.get(assignment.getPlanNodeId());
          if (input != null) {
            addUcxSplits(input, assignment);
          }
          continue;
        }
        if (noMoreScanSplits) {
          continue;
        }
        // Updates resend splits already seen, so deduplicate by sequence id.
        for (ScheduledSplit split : assignment.getSplits()) {
          if (seenSequenceIds.add(split.sequenceId())) {
            scanSplits.add(split.split());
          }
        }
        if (assignment.isNoMoreSplits()) {
          noMoreScanSplits = true;
          startNative = true;
        }
      }
    }
    if (!remote.isEmpty()) {
      delegate.addSplitAssignments(remote);
    }
    if (startNative) {
      nativeTask.start(ImmutableList.copyOf(scanSplits));
    }
  }

  private void addUcxSplits(int input, SplitAssignment assignment) {
    if (noMoreRemoteSplits.contains(assignment.getPlanNodeId())) {
      return;
    }
    for (ScheduledSplit split : assignment.getSplits()) {
      if (seenRemoteSplits.add(assignment.getPlanNodeId() + ":" + split.sequenceId())) {
        RemoteSplit remoteSplit = (RemoteSplit) split.split().getConnectorSplit();
        nativeTask.addRemoteSplit(input, ((DirectExchangeInput) remoteSplit.getExchangeInput()).getLocation());
      }
    }
    if (assignment.isNoMoreSplits()) {
      noMoreRemoteSplits.add(assignment.getPlanNodeId());
      nativeTask.noMoreRemoteSplits(input);
    }
  }

  @Override
  public Set<PlanNodeId> getNoMoreSplits() {
    synchronized (this) {
      if (!noMoreScanSplits) {
        return delegate.getNoMoreSplits();
      }
    }
    return ImmutableSet.<PlanNodeId>builder()
        .addAll(delegate.getNoMoreSplits())
        .add(scanNode.orElseThrow())
        .build();
  }
}

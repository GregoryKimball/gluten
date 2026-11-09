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

import io.airlift.log.Logger;
import io.substrait.proto.ReadRel;
import io.trino.metadata.Split;
import io.trino.spi.Page;
import io.trino.spi.block.BlockEncodingSerde;
import org.apache.gluten.trino.jni.NativePageSource;
import org.apache.gluten.trino.jni.TrinoNative;
import org.apache.gluten.trino.plan.HiveSplits;
import org.apache.gluten.trino.plan.PlanTranslator;
import org.apache.gluten.trino.plan.TypeTranslator;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

/**
 * One native Gluten task per Trino task. Gluten fixes a task's splits when it is created, so the
 * task starts once all splits of its table scan have arrived. A dedicated thread pulls output from
 * Velox, which in turn pulls remote-source pages from the input queues.
 */
public final class NativeTask {
  private static final Logger log = Logger.get(NativeTask.class);

  private final String taskId;
  private final PlanTranslator.Translation translation;
  private final BlockEncodingSerde serde;
  private final Map<String, String> sessionConf;
  private final String spillDir;
  private final Executor executor;
  private final boolean ucxExchange;
  private final List<BoundedQueue<byte[]>> inputs = new ArrayList<>();
  private final BoundedQueue<Page> output;
  private final AtomicBoolean started = new AtomicBoolean();
  private volatile boolean closed;
  // UCX exchange updates go to the native task once it exists.
  private final List<Consumer<Long>> pendingUpdates = new ArrayList<>();
  private long handle;

  public NativeTask(
      String taskId,
      PlanTranslator.Translation translation,
      BlockEncodingSerde serde,
      Map<String, String> sessionConf,
      String spillDir,
      Executor executor,
      long queueBytes,
      boolean ucxExchange) {
    this.taskId = taskId;
    this.translation = translation;
    this.serde = serde;
    this.sessionConf = sessionConf;
    this.spillDir = spillDir;
    this.executor = executor;
    this.ucxExchange = ucxExchange;
    for (int i = 0; i < translation.inputs().size(); i++) {
      inputs.add(new BoundedQueue<>(queueBytes));
    }
    this.output = new BoundedQueue<>(queueBytes);
  }

  BoundedQueue<byte[]> input(int index) {
    return inputs.get(index);
  }

  BoundedQueue<Page> output() {
    return output;
  }

  BlockEncodingSerde serde() {
    return serde;
  }

  /** Adds an upstream task location for UCX exchange input {@code input}. */
  public void addRemoteSplit(int input, String location) {
    update(handle -> TrinoNative.addRemoteSplit(handle, input, location));
  }

  public void noMoreRemoteSplits(int input) {
    update(handle -> TrinoNative.noMoreRemoteSplits(handle, input));
  }

  /** Sets the final consumer count of a broadcast or arbitrary UCX output. */
  public void noMoreOutputBuffers(int numBuffers) {
    update(handle -> TrinoNative.noMoreOutputBuffers(handle, numBuffers));
  }

  private synchronized void update(Consumer<Long> update) {
    if (handle == 0) {
      pendingUpdates.add(update);
    } else {
      update.accept(handle);
    }
  }

  /** Starts the native task with the table scan's splits; does nothing if already started. */
  public void start(List<Split> splits) {
    if (!started.compareAndSet(false, true) || closed) {
      return;
    }
    List<byte[]> splitInfos = new ArrayList<>();
    if (translation.scan().isPresent()) {
      ReadRel.LocalFiles.Builder files = ReadRel.LocalFiles.newBuilder();
      for (Split split : splits) {
        files.addItems(HiveSplits.toFile(split));
      }
      splitInfos.add(files.build().toByteArray());
    }
    log.info("Starting native task %s with %s splits and %s inputs", taskId, splits.size(), inputs.size());
    executor.execute(() -> run(splitInfos));
  }

  private void run(List<byte[]> splitInfos) {
    Thread.currentThread().setName("gluten-native-" + taskId);
    int javaInputs = ucxExchange ? 0 : inputs.size();
    NativePageSource[] sources = new NativePageSource[javaInputs];
    byte[][] schemas = new byte[javaInputs][];
    for (int i = 0; i < javaInputs; i++) {
      BoundedQueue<byte[]> queue = inputs.get(i);
      sources[i] = () -> {
        try {
          return queue.take();
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new RuntimeException(e);
        }
      };
      PlanTranslator.RemoteInput input = translation.inputs().get(i);
      List<String> names = new ArrayList<>();
      for (int column = 0; column < input.types().size(); column++) {
        names.add("c" + column);
      }
      schemas[i] = TypeTranslator.namedStruct(names, input.types()).toByteArray();
    }

    long handle = 0;
    try {
      handle = TrinoNative.createTask(
          translation.plan(), splitInfos.toArray(byte[][]::new), sources, schemas, sessionConf, spillDir);
      synchronized (this) {
        this.handle = handle;
        for (Consumer<Long> update : pendingUpdates) {
          update.accept(handle);
        }
        pendingUpdates.clear();
      }
      log.info("Native plan for %s:%n%s", taskId, TrinoNative.planString(handle));
      long pages = 0;
      long rows = 0;
      while (!closed) {
        byte[] bytes = TrinoNative.next(handle);
        if (bytes == null) {
          break;
        }
        Page page = RawPages.toPage(serde, bytes);
        pages++;
        rows += page.getPositionCount();
        output.put(page, bytes.length);
      }
      log.info("Native task %s finished: %s pages, %s rows, closed=%s", taskId, pages, rows, closed);
      output.finish();
    } catch (Throwable t) {
      log.error(t, "Native task %s failed", taskId);
      output.fail(t);
    } finally {
      // Velox may stop before consuming all of its inputs, such as when a join build side is
      // empty; closing lets the exchange pipelines drain and finish.
      for (BoundedQueue<byte[]> input : inputs) {
        input.close();
      }
      if (handle != 0) {
        synchronized (this) {
          this.handle = 0;
          TrinoNative.close(handle);
        }
      }
    }
  }

  public void close() {
    log.info("Closing native task %s (started=%s)", taskId, started.get());
    closed = true;
    output.close();
    for (BoundedQueue<byte[]> input : inputs) {
      input.close();
    }
  }
}

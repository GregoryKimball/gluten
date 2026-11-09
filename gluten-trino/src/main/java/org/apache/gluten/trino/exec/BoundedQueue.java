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
import com.google.common.util.concurrent.SettableFuture;

import java.util.ArrayDeque;
import java.util.Optional;

import static com.google.common.util.concurrent.Futures.immediateVoidFuture;

/**
 * A byte-bounded queue between a Trino driver, which must never block a thread, and the native
 * task thread, which may. One side uses futures and the other blocking calls, depending on which
 * end is the native thread.
 */
final class BoundedQueue<T> {
  private record Entry<T>(T value, long bytes) {}

  private final long capacityBytes;
  private final ArrayDeque<Entry<T>> entries = new ArrayDeque<>();
  private long bytes;
  private boolean finished;
  private boolean closed;
  private Throwable failure;
  private SettableFuture<Void> notFull = SettableFuture.create();
  private SettableFuture<Void> notEmpty = SettableFuture.create();

  BoundedQueue(long capacityBytes) {
    this.capacityBytes = capacityBytes;
    notFull.set(null);
  }

  /** Adds without blocking; returns a future that completes once there is room again. */
  synchronized ListenableFuture<Void> add(T value, long size) {
    if (closed || finished) {
      return immediateVoidFuture();
    }
    entries.add(new Entry<>(value, size));
    bytes += size;
    notEmpty.set(null);
    notifyAll();
    if (bytes >= capacityBytes && notFull.isDone()) {
      notFull = SettableFuture.create();
    }
    return notFull;
  }

  /** Adds, waiting while the queue is full. */
  synchronized void put(T value, long size) throws InterruptedException {
    while (bytes >= capacityBytes && !closed) {
      wait();
    }
    add(value, size);
  }

  synchronized ListenableFuture<Void> whenNotFull() {
    return notFull;
  }

  /** Removes the next value without blocking, or returns empty. */
  synchronized Optional<T> poll() {
    rethrow();
    Entry<T> entry = entries.poll();
    if (entry == null) {
      if (!finished && !closed && notEmpty.isDone()) {
        notEmpty = SettableFuture.create();
      }
      return Optional.empty();
    }
    removed(entry);
    return Optional.of(entry.value());
  }

  /** Removes the next value, waiting for one; returns null once the queue is finished and empty. */
  synchronized T take() throws InterruptedException {
    while (entries.isEmpty() && !finished && !closed) {
      wait();
    }
    rethrow();
    Entry<T> entry = entries.poll();
    if (entry == null) {
      return null;
    }
    removed(entry);
    return entry.value();
  }

  synchronized ListenableFuture<Void> whenNotEmpty() {
    return notEmpty;
  }

  synchronized boolean isDrained() {
    rethrow();
    return (finished || closed) && entries.isEmpty();
  }

  /** No more values will be added. */
  synchronized void finish() {
    finished = true;
    wakeAll();
  }

  synchronized void fail(Throwable throwable) {
    if (failure == null) {
      failure = throwable;
    }
    finish();
  }

  /** Discards queued values; later adds are dropped and waiters return. */
  synchronized void close() {
    closed = true;
    entries.clear();
    bytes = 0;
    wakeAll();
  }

  private void removed(Entry<T> entry) {
    bytes -= entry.bytes();
    if (bytes < capacityBytes) {
      notFull.set(null);
      notifyAll();
    }
  }

  private void wakeAll() {
    notEmpty.set(null);
    notFull.set(null);
    notifyAll();
  }

  private void rethrow() {
    if (failure != null) {
      if (failure instanceof RuntimeException runtime) {
        throw runtime;
      }
      throw new RuntimeException(failure);
    }
  }
}

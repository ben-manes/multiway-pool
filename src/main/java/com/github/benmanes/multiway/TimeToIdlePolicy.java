/*
 * Copyright 2013 Ben Manes. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.benmanes.multiway;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import com.google.common.base.Ticker;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * A policy that enforces that a resource may reside idle in the pool for up to a specified time
 * limit. A resource is considered idle when it is available to be borrowed, resulting in expiration
 * if the duration since the previous release exceeds a threshold.
 *
 * @author Ben Manes (ben.manes@gmail.com)
 */
@ThreadSafe
final class TimeToIdlePolicy<K, R> {
  static final int AMORTIZED_THRESHOLD = 16;
  static final int TASK_QUEUE_MASK = ceilingNextPowerOfTwo(
      Runtime.getRuntime().availableProcessors()) - 1;

  static int ceilingNextPowerOfTwo(int x) {
    // From Hacker's Delight, Chapter 3, Harry S. Warren Jr.
    return 1 << (Integer.SIZE - Integer.numberOfLeadingZeros(x - 1));
  }

  final LinkedDeque<ResourceKey<K>> idleQueue;
  final EvictionListener<K> evictionListener;
  final Queue<Runnable>[] taskQueues;
  final long expireAfterAccessNanos;
  final Lock idleLock;
  final Ticker ticker;

  @SuppressWarnings("unchecked")
  TimeToIdlePolicy(long expireAfterAccessNanos,
      Ticker ticker, EvictionListener<K> evictionListener) {
    this.ticker = ticker;
    this.idleLock = new ReentrantLock();
    this.idleQueue = new LinkedDeque<>();
    this.taskQueues = new Queue[TASK_QUEUE_MASK + 1];
    this.expireAfterAccessNanos = expireAfterAccessNanos;
    this.evictionListener = checkNotNull(evictionListener);

    for (int i = 0; i < taskQueues.length; i++) {
      taskQueues[i] = new ConcurrentLinkedQueue<>();
    }
  }

  /** Adds an idle resource to be tracked for expiration. */
  void add(ResourceKey<K> key) {
    key.setAccessTime(ticker.read());
    schedule(key.getAddTask());
  }

  /** Removes a resource that is no longer idle. */
  void invalidate(ResourceKey<K> key) {
    schedule(key.getRemovalTask());
  }

  /** Schedules the task to be applied to the idle policy. */
  void schedule(Runnable task) {
    int index = taskQueueIndex();
    taskQueues[index].add(task);
    cleanUp(AMORTIZED_THRESHOLD);
  }

  /** Returns the index to the task queue that the task should be scheduled on. */
  static int taskQueueIndex() {
    // A queue is chosen by the thread's id so that tasks are distributed in a pseudo evenly manner
    return (int) Thread.currentThread().getId() & TASK_QUEUE_MASK;
  }

  /** Determines whether the resource has expired. */
  boolean hasExpired(ResourceKey<K> resourceKey) {
    return hasExpired(resourceKey, ticker.read());
  }

  /** Determines whether the resource has expired. */
  boolean hasExpired(ResourceKey<K> resourceKey, long currentTimeNanos) {
    return (currentTimeNanos - resourceKey.getAccessTime()) >= expireAfterAccessNanos;
  }

  /** Performs any pending maintenance operations needed by the policy. */
  void cleanUp(int threshold) {
    if (idleLock.tryLock()) {
      try {
        drainTaskQueue(threshold);
        evict(threshold);
      } finally {
        idleLock.unlock();
      }
    }
  }

  /** Applies the pending operations, up to the threshold limit, in the task queue. */
  @GuardedBy("idleLock")
  void drainTaskQueue(int threshold) {
    int ran = 0;
    for (Queue<Runnable> taskQueue : taskQueues) {
      for (; ran < threshold; ran++) {
        Runnable task = taskQueue.poll();
        if (task == null) {
          break;
        }
        task.run();
      }
      if (ran == threshold) {
        return;
      }
    }
  }

  /** Evicts the resources that have exceeded the threshold for remaining idle. */
  @GuardedBy("idleLock")
  void evict(int threshold) {
    long now = ticker.read();
    for (int i = 0; i < threshold; i++) {
      ResourceKey<K> resourceKey = idleQueue.peekFirst();
      if ((resourceKey == null) || !hasExpired(resourceKey, now)) {
        break;
      }
      idleQueue.remove();
      evictionListener.onEviction(resourceKey);
    }
  }

  /** A listener that is invoked when the policy has evicted an expired resource. */
  interface EvictionListener<K> {

    /** A call-back notification that the entry was evicted. */
    void onEviction(ResourceKey<K> resourceKey);
  }
}

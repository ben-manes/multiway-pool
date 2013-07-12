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

import java.util.concurrent.TransferQueue;
import java.util.concurrent.atomic.AtomicReference;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import com.google.common.base.Objects;
import com.google.common.util.concurrent.Atomics;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * A key to the resource in the cache and the transfer queue.
 *
 * @author Ben Manes (ben.manes@gmail.com)
 */
@ThreadSafe
abstract class ResourceKey<K> implements Linked<ResourceKey<K>> {

  /** The status of the resource in the pool. */
  enum Status {

    /** The resource is available to be used. */
    IDLE,

    /** The resource is in use and in the cache */
    IN_FLIGHT,

    /** The resource is in use and evicted from the cache */
    RETIRED,

    /** The resource is not in use and not cached */
    DEAD
  }

  final K key;
  final AtomicReference<Status> status;
  final TransferQueue<ResourceKey<K>> queue;

  ResourceKey(TransferQueue<ResourceKey<K>> queue, Status status, K key) {
    this.status = Atomics.newReference(status);
    this.queue = checkNotNull(queue);
    this.key = checkNotNull(key);
  }

  /** Retrieves the resource category key. */
  K getKey() {
    return key;
  }

  /** The status of the entry in the pool. */
  Status getStatus() {
    return status.get();
  }

  /** Retrieves the transfer queue the resource is associated with. */
  TransferQueue<ResourceKey<K>> getQueue() {
    return queue;
  }

  /** Removes the resource key from its transfer queue. */
  void removeFromTransferQueue() {
    getQueue().remove(this);
  }

  /** Retrieves the time, in nanoseconds, that the resource became idle in the pool. */
  abstract long getAccessTime();

  /** Sets the time, in nanoseconds, that the resource became idle in the pool. */
  abstract void setAccessTime(long accessTimeNanos);

  /* ---------------- IDLE --> ? -------------- */

  /** Attempts to transition the entry from idle to in-flight (when borrowing). */
  boolean goFromIdleToInFlight() {
    return status.compareAndSet(Status.IDLE, Status.IN_FLIGHT);
  }

  /** Attempts to transition the entry from idle to retired (when evicting from the idle cache). */
  boolean goFromIdleToRetired() {
    return status.compareAndSet(Status.IDLE, Status.RETIRED);
  }

  /** Attempts to transition the entry from idle to dead (when evicting from the cache). */
  boolean goFromIdleToDead() {
    return status.compareAndSet(Status.IDLE, Status.DEAD);
  }

  /* ---------------- IN_FLIGHT --> ? -------------- */

  /** Attempts to transition the entry from in-flight to retired (when evicting from the cache). */
  boolean goFromInFlightToRetired() {
    return status.compareAndSet(Status.IN_FLIGHT, Status.RETIRED);
  }

  /** Attempts to transition the entry from in-flight to retired (when releasing the handle). */
  boolean goFromInFlightToIdle() {
    return status.compareAndSet(Status.IN_FLIGHT, Status.IDLE);
  }

  /* ---------------- RETIRED --> ? -------------- */

  /** Attempts to transition the entry from retired to dead (when releasing the handle). */
  boolean goFromRetiredToDead() {
    return status.compareAndSet(Status.RETIRED, Status.DEAD);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("id", System.identityHashCode(this))
        .add("status", status)
        .add("key", key)
        .toString();
  }

  /** A resource key used when idle caching is disabled (does not support links). */
  static final class UnlinkedResourceKey<K> extends ResourceKey<K> {

    UnlinkedResourceKey(TransferQueue<ResourceKey<K>> queue, Status status, K key) {
      super(queue, status, key);
    }

    @Override
    long getAccessTime() {
      throw new UnsupportedOperationException();
    }

    @Override
    void setAccessTime(long accessTimeNanos) {
      throw new UnsupportedOperationException();
    }

    @Override
    public ResourceKey<K> getPrevious() {
      throw new UnsupportedOperationException();
    }

    @Override
    public void setPrevious(ResourceKey<K> prev) {
      throw new UnsupportedOperationException();
    }

    @Override
    public ResourceKey<K> getNext() {
      throw new UnsupportedOperationException();
    }

    @Override
    public void setNext(ResourceKey<K> next) {
      throw new UnsupportedOperationException();
    }
  }

  /** A resource key used when idle caching is enabled (access order). */
  static final class LinkedResourceKey<K> extends ResourceKey<K> {
    @GuardedBy("idleLock")
    ResourceKey<K> prev;
    @GuardedBy("idleLock")
    ResourceKey<K> next;
    @GuardedBy("idleLock")
    long accessTimeNanos;

    LinkedResourceKey(TransferQueue<ResourceKey<K>> queue, Status status, K key) {
      super(queue, status, key);
    }

    @Override
    @GuardedBy("idleLock")
    long getAccessTime() {
      return accessTimeNanos;
    }

    @Override
    @GuardedBy("idleLock")
    void setAccessTime(long accessTimeNanos) {
      this.accessTimeNanos = accessTimeNanos;
    }

    @Override
    @GuardedBy("idleLock")
    public ResourceKey<K> getPrevious() {
      return prev;
    }

    @Override
    @GuardedBy("idleLock")
    public void setPrevious(ResourceKey<K> prev) {
      this.prev = prev;
    }

    @Override
    @GuardedBy("idleLock")
    public ResourceKey<K> getNext() {
      return next;
    }

    @Override
    @GuardedBy("idleLock")
    public void setNext(ResourceKey<K> next) {
      this.next = next;
    }
  }
}

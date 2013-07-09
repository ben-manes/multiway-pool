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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

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
final class ResourceKey<K> {

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
  final AtomicBoolean initialized;
  final AtomicReference<Status> status;
  final TransferQueue<ResourceKey<K>> queue;

  ResourceKey(TransferQueue<ResourceKey<K>> queue, Status status, K key) {
    this.status = Atomics.newReference(status);
    this.initialized = new AtomicBoolean();
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

  /**
   * The key is initialized to indicate that it is associated with a created resource. This check
   * avoids race conditions where the key may be transfered while the resource has simultaneously
   * been evicted from the cache. This results in a cache miss, a new resource being created, and
   * additional race conditions with how state transitions are managed. These races are resolved
   * by rejecting the initialization when creating the new resource.
   *
   * @throws AlreadyInitializedException if the key was already assigned to a resource
   */
  void initialize() {
    if (initialized.get()) {
      throw new AlreadyInitializedException();
    }
    initialized.lazySet(true);
  }

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

  /** An exception to indicate that the key was already assigned to a resource. */
  static final class AlreadyInitializedException extends RuntimeException {
    private static final long serialVersionUID = 1L;
  }
}

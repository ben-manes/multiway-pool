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

import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import javax.annotation.concurrent.ThreadSafe;

import com.google.common.cache.CacheStats;

/**
 * An object pool that supports pooling multiple resources that are associated with a single key.
 * A resource is borrowed from the pool, used exclusively, and released back for reuse by another
 * caller. This implementation can optionally be bounded by maximum size, time-to-live, and/or
 * time-to-idle policies.
 * <p>
 * A traditional object pool is homogeneous; all of the resources are identical in the data and
 * capabilities offered. For example a database connection pool to a shared database instance. A
 * multiway object pool is heterogeneous; resources may differ in the data and capabilities offered.
 * For example a flat file database may pool random access readers to the database table files. The
 * relationship of a single-way to a multi-way object pool is similar to that of a map to a
 * multimap.
 * <p>
 * When this pool is bounded any resource is eligible for eviction regardless of the key that it is
 * associated with. A size based bound will evict resources by a best-effort LRU policy and a time
 * based policy will evict by either a time-to-idle and/or time-to-live policy. The resource's life
 * cycle can be instrumented, such as when cleaning up after eviction, by using the appropriate
 * {@link ResourceLifecycle} method.
 *
 * @author Ben Manes (ben.manes@gmail.com)
 */
@ThreadSafe
public interface MultiwayPool<K, R> {

  /**
   * Retrieves a resource from the pool, immediately. If a resource is not available then one is
   * created.
   *
   * @param key the category to qualify the type of resource to retrieve
   * @param loader a loader that creates a non-null resource
   * @return a handle to the resource
   */
  Handle<R> borrow(K key, Callable<? extends R> loader);

  /**
   * Retrieves a resource from the pool, waiting up to the specified wait time if necessary for one
   * to become available. If a resource is not available then one is created.
   *
   * @param key the category to qualify the type of resource to retrieve
   * @param loader a loader that creates a non-null resource
   * @param timeout how long to wait before giving up and creating the resource
   * @param unit a {@code TimeUnit} determining how to interpret the {@code duration} parameter
   * @return a handle to the resource
   */
  Handle<R> borrow(K key, Callable<? extends R> loader, long timeout, TimeUnit unit);

  /**
   * Discards any pooled resource associated with the {@code key}. Any resource currently in use
   * will be immediately discarded upon release.
   */
  void invalidate(Object key);

  /**
   * Discards all resources in the pool. Any resource currently in use will be immediately
   * discarded upon release.
   */
  void invalidateAll();

  /** Returns the approximate number of resources managed by the pool. */
  long size();

  /** Performs any pending maintenance operations needed by the pool. */
  void cleanUp();

  /** Returns a current snapshot of this pool's cumulative cache statistics, if enabled. */
  CacheStats stats();
}

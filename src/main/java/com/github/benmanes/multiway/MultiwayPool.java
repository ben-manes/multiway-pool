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

import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TransferQueue;
import java.util.logging.Logger;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

import com.github.benmanes.multiway.ResourceKey.AlreadyInitializedException;
import com.github.benmanes.multiway.ResourceKey.Status;
import com.google.common.base.Objects;
import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.common.base.Ticker;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.CacheStats;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalCause;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.common.cache.Weigher;
import com.google.common.util.concurrent.UncheckedExecutionException;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

/**
 * A concurrent object pool that supports pooling multiple resources that are associated with a
 * single key. A resource is borrowed from the pool, used exclusively, and released back for reuse
 * by another caller. This implementation can optionally be bounded by maximum size, time-to-live,
 * and/or time-to-idle policies.
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
public final class MultiwayPool<K, R> {

  /*
   * An object pool must be optimized around resources regularly being checked-out and returned by
   * multiple threads. A naive implementation that guards the pool with a single lock will suffer
   * contention by the frequent access if resources are used in short bursts.
   *
   * The basic strategy denormalizes the resources into a flattened cache, which provides the
   * maximum size and time-to-live policies. The resources are organized into single-way pools as a
   * view layered above the cache, with each pool represented as a collection of cache keys. These
   * pools are implemented as transfer queues, which utilize elimination to reduce contention. A
   * thread returning a resource to the pool will first attempting to exchange it with a thread
   * checking one out, falling back to storing it in the queue if a transfer is unsuccessful.
   *
   * The time-to-idle policy is implemented as an optional secondary cache. This is required in
   * order to count idle time as the duration when the resource is not being used, rather than the
   * duration that the resource has resided in the primary cache not being accessed. This secondary
   * cache is implemented naively, and therefore is more expensive to maintain. For efficiency, this
   * feature may need to be rewritten to use the lock amortization or similar techniques in the
   * future.
   *
   * The removal of unused queues is performed aggressively by using weak references. The resource's
   * cache key retains a strong reference to its queue, thereby retaining the pool while there are
   * associated resources in the cache or it is being used. When there are no resources referencing
   * to the queue then the garbage collector will eagerly discard the queue.
   */

  static final Logger log = Logger.getLogger(MultiwayPool.class.getName());

  final LoadingCache<K, TransferQueue<ResourceKey<K>>> transferQueues;
  final Optional<Cache<ResourceKey<K>, R>> idleCache;
  final LoadingCache<ResourceKey<K>, R> cache;
  final ResourceLifecycle<K, R> lifecycle;

  MultiwayPool(Builder<? super K, ? super R> builder, ResourceLifecycle<K, R> lifecycle) {
    this.transferQueues = makeTransferQueues();
    this.idleCache = makeIdleCache(builder);
    this.cache = makeCache(builder);
    this.lifecycle = lifecycle;
  }

  /** Constructs a new builder with no automatic eviction of any kind. */
  public static Builder<Object, Object> newBuilder() {
    return new Builder<Object, Object>();
  }

  /** Creates a mapping from the resource category to its transfer queue of available keys. */
  static <K> LoadingCache<K, TransferQueue<ResourceKey<K>>> makeTransferQueues() {
    return CacheBuilder.newBuilder().weakValues().build(
        new CacheLoader<K, TransferQueue<ResourceKey<K>>>() {
          @Override public TransferQueue<ResourceKey<K>> load(K key) throws Exception {
            return new LinkedTransferQueue<ResourceKey<K>>();
          }
        });
  }

  /** Creates a cache of resources based on a unique, per-instance key. */
  LoadingCache<ResourceKey<K>, R> makeCache(Builder<? super K, ? super R> builder) {
    CacheBuilder<Object, Object> cacheBuilder = CacheBuilder.newBuilder();
    if (builder.maximumSize != Builder.UNSET_INT) {
      cacheBuilder.maximumSize(builder.maximumSize);
    }
    if (builder.maximumWeight != Builder.UNSET_INT) {
      cacheBuilder.maximumWeight(builder.maximumWeight);
    }
    if (builder.weigher != null) {
      cacheBuilder.weigher(builder.weigher);
    }
    if (builder.expireAfterWriteNanos != Builder.UNSET_INT) {
      cacheBuilder.expireAfterWrite(builder.expireAfterWriteNanos, TimeUnit.NANOSECONDS);
    }
    if (builder.ticker != null) {
      cacheBuilder.ticker(builder.ticker);
    }
    if (builder.recordStats) {
      cacheBuilder.recordStats();
    }
    return cacheBuilder.removalListener(new CacheRemovalListener()).build(
        new CacheLoader<ResourceKey<K>, R>() {
          @Override public R load(ResourceKey<K> resourceKey) throws Exception {
            resourceKey.initialize();

            R resource = lifecycle.create(resourceKey.getKey());
            if (idleCache.isPresent()) {
              idleCache.get().put(resourceKey, resource);
            }
            return resource;
          }
        });
  }

  /** Creates a cache of the idle resources eligible for expiration. */
  Optional<Cache<ResourceKey<K>, R>> makeIdleCache(Builder<? super K, ? super R> builder) {
    if (builder.expireAfterAccessNanos == -1) {
      return Optional.absent();
    }
    CacheBuilder<Object, Object> cacheBuilder = CacheBuilder.newBuilder();
    if (builder.ticker != null) {
      cacheBuilder.ticker(builder.ticker);
    }
    Cache<ResourceKey<K>, R> idle = cacheBuilder
        .expireAfterAccess(builder.expireAfterAccessNanos, TimeUnit.NANOSECONDS)
        .removalListener(new IdleCacheRemovalListener())
        .build();
    return Optional.of(idle);
  }

  /**
   * Retrieves a resource from the pool, immediately. If a resource is not available then one is
   * created.
   *
   * @param key the category to qualify the type of resource to retrieve
   * @return a handle to the resource
   */
  public Handle<R> borrow(K key) {
    return borrow(key, 0L, TimeUnit.NANOSECONDS);
  }

  /**
   * Retrieves a resource from the pool, waiting up to the specified wait time if necessary for one
   * to become available. If a resource is not available then one is created.
   *
   * @param key the category to qualify the type of resource to retrieve
   * @param timeout how long to wait before giving up and creating the resource
   * @param unit a {@code TimeUnit} determining how to interpret the {@code duration} parameter
   * @return a handle to the resource
   */
  public Handle<R> borrow(K key, long timeout, TimeUnit unit) {
    ResourceHandle handle = getResourceHandle(key, timeout, unit);
    if (idleCache.isPresent()) {
      idleCache.get().invalidate(handle.resourceKey);
    }
    lifecycle.onBorrow(key, handle.resource);
    return handle;
  }

  /** Retrieves the next available handler, creating the resource if necessary. */
  ResourceHandle getResourceHandle(K key, long timeout, TimeUnit unit) {
    TransferQueue<ResourceKey<K>> queue = transferQueues.getUnchecked(key);
    long timeoutNanos = unit.toNanos(timeout);
    long startNanos = System.nanoTime();
    for (;;) {
      ResourceHandle handle = tryToGetResourceHandle(key, queue, timeoutNanos);
      if (handle == null) {
        long elapsed = System.nanoTime() - startNanos;
        timeoutNanos = Math.max(0, timeoutNanos - elapsed);
      } else {
        return handle;
      }
    }
  }

  /** Attempts to retrieves the next available handler, creating the resource if necessary. */
  @Nullable ResourceHandle tryToGetResourceHandle(K key,
      TransferQueue<ResourceKey<K>> queue, long timeoutNanos) {
    try {
      ResourceKey<K> resourceKey = (timeoutNanos == 0)
          ? queue.poll()
          : queue.poll(timeoutNanos, TimeUnit.NANOSECONDS);
      return (resourceKey == null)
          ? newResourceHandle(key, queue)
          : tryToGetPooledResourceHandle(resourceKey);
    } catch (InterruptedException e) {
      throw Throwables.propagate(e);
    }
  }

  /** Creates a new resource associated to the category key and queue. */
  ResourceHandle newResourceHandle(K key, TransferQueue<ResourceKey<K>> queue) {
    ResourceKey<K> resourceKey = new ResourceKey<K>(queue, Status.IN_FLIGHT, key);
    R resource = cache.getUnchecked(resourceKey);
    return new ResourceHandle(resourceKey, resource);
  }

  /** Attempts to get the pooled resource with the given key. */
  @Nullable ResourceHandle tryToGetPooledResourceHandle(ResourceKey<K> resourceKey) {
    try {
      R resource = cache.getUnchecked(resourceKey);
      Status status = resourceKey.getStatus();
      return (status == Status.IDLE) && resourceKey.goFromIdleToInFlight()
          ? new ResourceHandle(resourceKey, resource)
          : null;
    } catch (UncheckedExecutionException e) {
      if (e.getCause() instanceof AlreadyInitializedException) {
        // The resource associated with the key was discarded, but due to race conditions the
        // key was handed out and the cache attempted to create a new instance. This exception
        // was thrown to reject that operation and retry with a different key.
        return null;
      }
      throw e;
    }
  }

  /** Returns the approximate number of resources managed by the pool. */
  long size() {
    return cache.size();
  }

  /** Performs any pending maintenance operations needed by the pool. */
  public void cleanUp() {
    if (idleCache.isPresent()) {
      idleCache.get().cleanUp();
    }
    cache.cleanUp();
  }

  /** Returns a current snapshot of this pool's cumulative cache statistics, if enabled. */
  public CacheStats stats() {
    return cache.stats();
  }

  /** A handle to a resource in the cache. */
  final class ResourceHandle implements Handle<R> {
    final ResourceKey<K> resourceKey;
    @Nullable R resource;

    ResourceHandle(ResourceKey<K> resourceKey, R resource) {
      this.resourceKey = checkNotNull(resourceKey);
      this.resource = checkNotNull(resource);
    }

    @Override
    public R get() {
      validate();
      return resource;
    }

    @Override
    public void release() {
      release(0L, TimeUnit.NANOSECONDS);
    }

    @Override
    public void release(long timeout, TimeUnit unit) {
      validate();
      try {
        lifecycle.onRelease(resourceKey.getKey(), resource);
      } finally {
        recycle(timeout, unit);
      }
    }

    @Override
    public void invalidate() {
      validate();
      try {
        lifecycle.onRelease(resourceKey.getKey(), resource);
      } finally {
        cache.invalidate(resourceKey);
        recycle(0L, TimeUnit.NANOSECONDS);
      }
    }

    void validate() {
      checkState(resource != null, "Stale handle to the resource for %s", resourceKey.getKey());
    }

    /** Returns the resource to the pool or discards it if the resource is no longer cached. */
    void recycle(long timeout, TimeUnit unit) {
      for (;;) {
        Status status = resourceKey.getStatus();
        switch (status) {
          case IN_FLIGHT:
            if (resourceKey.goFromInFlightToIdle()) {
              releaseToPool(timeout, unit);
              return;
            }
            break;
          case RETIRED:
            if (resourceKey.goFromRetiredToDead()) {
              discardResource();
              return;
            }
            break;
          default:
            throw new IllegalStateException("Unnexpected state: " + status);
        }
      }
    }

    /** Returns the resource to the pool so it can be borrowed by another caller. */
    void releaseToPool(long timeout, TimeUnit unit) {
      // Add the resource to the idle cache if present. If the resource was removed for any
      // other reason while being added, it must then be discarded afterwards
      if (idleCache.isPresent()) {
        idleCache.get().put(resourceKey, resource);
        if (resourceKey.getStatus() != Status.IDLE) {
          idleCache.get().invalidate(resourceKey);
        }
      }

      // Attempt to transfer the resource to another thread, else return it to the queue
      TransferQueue<ResourceKey<K>> queue = resourceKey.getQueue();
      try {
        boolean transferred = (timeout == 0)
            ? queue.tryTransfer(resourceKey)
            : queue.tryTransfer(resourceKey, timeout, unit);
        if (!transferred) {
          queue.add(resourceKey);
        }
      } catch (InterruptedException e) {
        queue.add(resourceKey);
      }
      if (resourceKey.getStatus() == Status.DEAD) {
        resourceKey.removeFromTransferQueue();
      }

      resource = null;
    }

    /** Discards the resource after it has become dead. */
    void discardResource() {
      R old = resource;
      resource = null;
      lifecycle.onRemoval(resourceKey.getKey(), old);
    }

    @Override
    public String toString() {
      return Objects.toStringHelper(this)
          .add("status", resourceKey.getStatus())
          .add("key", resourceKey.getKey())
          .add("resource", resource)
          .toString();
    }

    /**
     * A last ditch effort to avoid resource leaks. This should not be relied upon and its mere
     * existence has negative performance implications for the garbage collector.
     */
    @Override
    protected void finalize() {
      if (resource != null) {
        String msg = String.format("Handle for %s -> %s was not properly released",
            resourceKey.getKey().getClass().getName(), resource.getClass().getName());
        log.warning(msg);
        release();
      }
    }
  }

  /** A removal listener for the resource cache. */
  final class CacheRemovalListener implements RemovalListener<ResourceKey<K>, R> {

    /**
     * Atomically transitions the resource to a state where it can no longer be used. If the
     * resource is idle or retired then it is immediately discarded. If the resource is
     * currently in use then it is marked to be discarded when it has been released.
     */
    @Override
    public void onRemoval(RemovalNotification<ResourceKey<K>, R> notification) {
      ResourceKey<K> resourceKey = notification.getKey();
      for (;;) {
        Status status = resourceKey.getStatus();
        switch (status) {
          case IDLE:
            // The resource is not being used and may be immediately discarded
            if (resourceKey.goFromIdleToDead()) {
              discardFromIdle(resourceKey, notification.getValue());
              return;
            }
            break;
          case IN_FLIGHT:
            // The resource is currently being used and should be discarded when released
            if (resourceKey.goFromInFlightToRetired()) {
              return;
            }
            break;
          case RETIRED:
            // A resource is already retired when it has been expired by the idle cache
            if (resourceKey.goFromRetiredToDead()) {
              discardFromRetired(resourceKey, notification.getValue());
              return;
            }
            break;
          default:
            throw new IllegalStateException("Unnexpected state: " + status);
        }
      }
    }

    /** Discards the resource after becoming dead from the idle state. */
    void discardFromIdle(ResourceKey<K> resourceKey, R resource) {
      resourceKey.removeFromTransferQueue();
      if (idleCache.isPresent()) {
        idleCache.get().invalidate(resourceKey);
      }
      lifecycle.onRemoval(resourceKey.getKey(), resource);
    }

    /** Discards the resource after becoming dead from the retired state. */
    void discardFromRetired(ResourceKey<K> resourceKey, R resource) {
      resourceKey.removeFromTransferQueue();
      lifecycle.onRemoval(resourceKey.getKey(), resource);
    }
  }

  /** A removal listener for the idle resources cache. */
  final class IdleCacheRemovalListener implements RemovalListener<ResourceKey<K>, R> {

    /**
     * Atomically transitions the resource to a state where it can no longer be used. If the
     * resource is idle then it is immediately discarded by invalidating it in the primary cache.
     */
    @Override
    public void onRemoval(RemovalNotification<ResourceKey<K>, R> notification) {
      boolean expired = notification.getCause() == RemovalCause.EXPIRED;
      if (!expired) {
        return;
      }
      ResourceKey<K> resourceKey = notification.getKey();
      for (;;) {
        Status status = resourceKey.getStatus();
        switch (status) {
          case IDLE:
            if (resourceKey.goFromIdleToRetired()) {
              cache.invalidate(resourceKey);
              return;
            }
            break;
          default:
            // do nothing
            return;
        }
      }
    }
  }

  /**
   * A builder of {@link MultiwayPool} instances with support for least-recently-used eviction and
   * time-based expiration of resources. A notification is made when a resource is created,
   * borrowed, released, or removed. By default instances will not perform any type of eviction.
   * <p>
   * Usage example:
   * <pre>   {@code
   *   MultiwayPool<File, RandomAccessFile> files = MultiwayPool.newBuilder()
   *       .expireAfterWrite(10, TimeUnit.MINUTES)
   *       .maximumSize(100)
   *       .build(
   *           new ResourceLifecycle<File, RandomAccessFile>() {
   *             public RandomAccessFile create(File file) throws AnyException {
   *               return new RandomAccessFile(file);
   *             }
   *           });
   * }</pre>
   */
  public static final class Builder<K, R> {
    static final int UNSET_INT = -1;

    boolean recordStats;

    long maximumSize = UNSET_INT;
    long maximumWeight = UNSET_INT;
    Weigher<? super K, ? super R> weigher;

    Ticker ticker;
    long expireAfterWriteNanos = UNSET_INT;
    long expireAfterAccessNanos = UNSET_INT;

    /**
     * Specifies the maximum number of resources the pool may contain, regardless of the category
     * it is associated with. Note that the pool <b>may evict a resource before this limit is
     * exceeded</b>. As the pool size grows close to the maximum, the pool evicts entries that are
     * less likely to be used again.
     *
     * @param size the maximum size of the cache
     * @throws IllegalArgumentException if {@code size} is negative
     */
    public Builder<K, R> maximumSize(long size) {
      checkState(maximumSize == UNSET_INT, "maximum size was already set to %s", maximumSize);
      checkArgument(size >= 0, "maximum size must not be negative");
      maximumSize = size;
      return this;
    }

    /**
     * Specifies the maximum weight of resources the pool may contain, regardless of the category
     * it is associated with. Weight is determined using the {@link Weigher} specified with
     * {@link #weigher}, and use of this method requires a corresponding call to {@link #weigher}
     * prior to calling {@link #build}.
     * <p>
     * Note that the cache <b>may evict a resource before this limit is exceeded</b>. As the pool
     * size grows close to the maximum, the pool evicts entries that are less likely to be used
     * again.
     * <p>
     * When {@code weight} is zero, resources will be evicted immediately after being loaded into
     * pool. This can be useful in testing, or to disable the pool temporarily without a code
     * change.
     * <p>
     * Note that weight is only used to determine whether the pool is over capacity; it has no
     * effect on selecting which resource should be evicted next.
     * <p>
     * This feature cannot be used in conjunction with {@link #maximumSize}.
     *
     * @param weight the maximum total weight of entries the cache may contain
     * @throws IllegalArgumentException if {@code weight} is negative
     * @throws IllegalStateException if a maximum weight or size was already set
     */
    public Builder<K, R> maximumWeight(long weight) {
      checkState(maximumWeight == UNSET_INT, "maximum weight was already set to %s", maximumWeight);
      checkState(maximumSize == UNSET_INT, "maximum size was already set to %s", maximumSize);
      checkArgument(weight >= 0, "maximum weight must not be negative");
      this.maximumWeight = weight;
      return this;
    }

    /**
     * Specifies the weigher to use in determining the weight of resources. The weight is taken
     * into consideration by {@link #maximumWeight(long)} when determining which resources to evict,
     * and use of this method requires a corresponding call to {@link #maximumWeight(long)} prior to
     * calling {@link #build}. Weights are measured and recorded when resources are inserted into
     * the pool, and are thus effectively static during the lifetime of the resource.
     *
     * <p>When the weight of a resource is zero it will not be considered for size-based eviction
     * (though it still may be evicted by other means).
     *
     * @param weigher the weigher to use in calculating the weight of cache entries
     * @throws IllegalArgumentException if {@code size} is negative
     * @throws IllegalStateException if a maximum size was already set
     */
    public <K1 extends K, R1 extends R> Builder<K1, R1> weigher(
        Weigher<? super K1, ? super R1> weigher) {
      checkState(this.weigher == null);
      checkState(this.maximumSize == UNSET_INT, "weigher can not be combined with maximum size");

      // safely limiting the kinds of caches this can produce
      @SuppressWarnings("unchecked")
      Builder<K1, R1> self = (Builder<K1, R1>) this;
      self.weigher = checkNotNull(weigher);
      return self;
    }

    /**
     * Specifies that each resource should be automatically removed from the pool once a fixed
     * duration has elapsed after the resource's creation.
     *
     * @param duration the length of time after a resource is created when it should
     *     be automatically removed
     * @param unit the unit that {@code duration} is expressed in
     * @throws IllegalArgumentException if {@code duration} is negative
     * @throws IllegalStateException if the time to live or time to idle was already set
     */
    public Builder<K, R> expireAfterWrite(long duration, TimeUnit unit) {
      checkState(expireAfterWriteNanos == UNSET_INT,
          "expireAfterWrite was already set to %s ns", expireAfterWriteNanos);
      checkArgument(duration >= 0, "duration cannot be negative: %s %s", duration, unit);
      expireAfterWriteNanos = unit.toNanos(duration);
      return this;
    }

    /**
     * Specifies that each entry should be automatically removed from the pool once a fixed duration
     * has elapsed after the resource's creation or its last access. Access time is reset when the
     * resource is borrowed or released. A resource is considered eligible for eviction when it is
     * idle in the pool, e.g. it is not being used.
     *
     * @param duration the length of time after a resource is last accessed that it should
     *     be automatically removed
     * @param unit the unit that {@code duration} is expressed in
     * @throws IllegalArgumentException if {@code duration} is negative
     */
    public Builder<K, R> expireAfterAccess(long duration, TimeUnit unit) {
      checkState(expireAfterAccessNanos == UNSET_INT,
          "expireAfterAccess was already set to %s ns", expireAfterAccessNanos);
      checkArgument(duration >= 0, "duration cannot be negative: %s %s", duration, unit);
      expireAfterAccessNanos = unit.toNanos(duration);
      return this;
    }

    /**
     * Specifies a nanosecond-precision time source for use in determining when entries should be
     * expired. By default, {@link System#nanoTime} is used.
     * <p>
     * The primary intent of this method is to facilitate testing of caches which have been
     * configured with {@link #expireAfterWrite} or {@link #expireAfterAccess}.
     *
     * @throws IllegalStateException if a ticker was already set
     */
    public Builder<K, R> ticker(Ticker ticker) {
      checkState(this.ticker == null);
      this.ticker = checkNotNull(ticker);
      return this;
    }

    /**
     * Enable the accumulation of {@link CacheStats} during the operation of the pool. Without this
     * {@link Cache#stats} will return zero for all statistics. Note that recording stats requires
     * bookkeeping to be performed with each operation, and thus imposes a performance penalty on
     * cache operation.
     */
    public Builder<K, R> recordStats() {
      recordStats = true;
      return this;
    }

    /**
     * Builds a multiway pool, which either returns an available resource for a given key or
     * atomically computes or retrieves it using the supplied {@code ResourceLifecycle}.
     *
     * @param lifecycle the resource life cycle used for creation and listener callbacks
     * @return a multiway pool having the requested features
     */
    public <K1 extends K, R1 extends R> MultiwayPool<K1, R1> build(
        ResourceLifecycle<K1, R1> lifecycle) {
      checkNotNull(lifecycle);
      return new MultiwayPool<K1, R1>(this, lifecycle);
    }
  }
}

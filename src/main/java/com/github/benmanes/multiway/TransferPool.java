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

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TransferQueue;
import java.util.logging.Logger;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

import com.github.benmanes.multiway.ResourceKey.LinkedResourceKey;
import com.github.benmanes.multiway.ResourceKey.Status;
import com.github.benmanes.multiway.ResourceKey.UnlinkedResourceKey;
import com.github.benmanes.multiway.TimeToIdlePolicy.EvictionListener;
import com.google.common.base.Objects;
import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.common.base.Ticker;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.CacheStats;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.common.collect.Lists;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

/**
 * A concurrent {@link MultiwayPool} that is optimized around transferring resources between threads
 * that borrow and release handles.
 *
 * @author Ben Manes (ben.manes@gmail.com)
 */
@ThreadSafe
class TransferPool<K, R> implements MultiwayPool<K, R> {

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
   * The time-to-idle policy is implemented as the duration when the resource is not being used,
   * rather than the duration that the resource has resided in the primary cache not being accessed.
   * This policy is implemented as a queue ordered by access time, where the head is the resource
   * that has remained idle in the pool the longest.
   *
   * The removal of unused queues is performed aggressively by using weak references. The resource's
   * cache key retains a strong reference to its queue, thereby retaining the pool while there are
   * associated resources in the cache or it is being used. When there are no resources referencing
   * to the queue then the garbage collector will eagerly discard the queue.
   */

  static final Logger log = Logger.getLogger(TransferPool.class.getName());
  static final ResourceLifecycle<Object, Object> DISCARDING_LIFECYCLE =
      new ResourceLifecycle<Object, Object>() {};

  final LoadingCache<K, TransferQueue<ResourceKey<K>>> transferQueues;
  final ResourceLifecycle<? super K, ? super R> lifecycle;
  final Optional<TimeToIdlePolicy<K, R>> timeToIdlePolicy;
  final Cache<ResourceKey<K>, R> cache;

  @SuppressWarnings("unchecked")
  TransferPool(MultiwayPoolBuilder<? super K, ? super R> builder) {
    timeToIdlePolicy = makeTimeToIdlePolicy(builder);
    transferQueues = makeTransferQueues();
    lifecycle = (ResourceLifecycle<? super K, ? super R>) Objects.firstNonNull(
        builder.lifecycle, DISCARDING_LIFECYCLE);
    cache = makeCache(builder);
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

  Cache<ResourceKey<K>, R> makeCache(MultiwayPoolBuilder<? super K, ? super R> builder) {
    CacheBuilder<Object, Object> cacheBuilder = CacheBuilder.newBuilder();
    if (builder.maximumSize != MultiwayPoolBuilder.UNSET_INT) {
      cacheBuilder.maximumSize(builder.maximumSize);
    }
    if (builder.maximumWeight != MultiwayPoolBuilder.UNSET_INT) {
      cacheBuilder.maximumWeight(builder.maximumWeight);
    }
    if (builder.weigher != null) {
      cacheBuilder.weigher(builder.weigher);
    }
    if (builder.expireAfterWriteNanos != MultiwayPoolBuilder.UNSET_INT) {
      cacheBuilder.expireAfterWrite(builder.expireAfterWriteNanos, TimeUnit.NANOSECONDS);
    }
    if (builder.ticker != null) {
      cacheBuilder.ticker(builder.ticker);
    }
    if (builder.recordStats) {
      cacheBuilder.recordStats();
    }
    cacheBuilder.removalListener(new CacheRemovalListener());
    return cacheBuilder.build();
  }

  /** Creates a cache of the idle resources eligible for expiration. */
  Optional<TimeToIdlePolicy<K, R>> makeTimeToIdlePolicy(
      MultiwayPoolBuilder<? super K, ? super R> builder) {
    if (builder.expireAfterAccessNanos == -1) {
      return Optional.absent();
    }
    Ticker ticker = (builder.ticker == null) ? Ticker.systemTicker() : builder.ticker;
    TimeToIdlePolicy<K, R> policy = new TimeToIdlePolicy<K, R>(
        builder.expireAfterAccessNanos, ticker, new IdleEvictionListener());
    return Optional.of(policy);
  }

  @Override
  public Handle<R> borrow(K key, Callable<? extends R> loader) {
    return borrow(key, loader, 0, TimeUnit.NANOSECONDS);
  }

  @Override
  public Handle<R> borrow(K key, Callable<? extends R> loader, long timeout, TimeUnit unit) {
    checkNotNull(key);
    checkNotNull(unit);
    checkNotNull(loader);

    ResourceHandle handle = getResourceHandle(key, loader, timeout, unit);
    if (timeToIdlePolicy.isPresent()) {
      timeToIdlePolicy.get().invalidate(handle.resourceKey);
    }
    try {
      lifecycle.onBorrow(key, handle.resource);
      return handle;
    } catch (Exception e) {
      handle.invalidate();
      throw e;
    }
  }

  /** Retrieves the next available handler, creating the resource if necessary. */
  ResourceHandle getResourceHandle(K key,
      Callable<? extends R> loader, long timeout, TimeUnit unit) {
    TransferQueue<ResourceKey<K>> queue = transferQueues.getUnchecked(key);
    long timeoutNanos = unit.toNanos(timeout);
    long startNanos = System.nanoTime();
    for (;;) {
      ResourceHandle handle = tryToGetResourceHandle(key, loader, queue, timeoutNanos);
      if (handle == null) {
        long elapsed = System.nanoTime() - startNanos;
        timeoutNanos = Math.max(0, timeoutNanos - elapsed);
      } else {
        return handle;
      }
    }
  }

  /** Attempts to retrieves the next available handler, creating the resource if necessary. */
  @Nullable ResourceHandle tryToGetResourceHandle(K key, Callable<? extends R> loader,
      TransferQueue<ResourceKey<K>> queue, long timeoutNanos) {
    try {
      ResourceKey<K> resourceKey = (timeoutNanos == 0)
          ? queue.poll()
          : queue.poll(timeoutNanos, TimeUnit.NANOSECONDS);

      if (resourceKey == null) {
        return newResourceHandle(key, loader, queue);
      }
      if (timeToIdlePolicy.isPresent() && timeToIdlePolicy.get().hasExpired(resourceKey)) {
        // Retry with another resource due to idle expiration
        timeToIdlePolicy.get().cleanUp();
        return null;
      }
      return tryToGetPooledResourceHandle(resourceKey);
    } catch (InterruptedException e) {
      throw Throwables.propagate(e);
    }
  }

  /** Creates a new resource associated to the category key and queue. */
  ResourceHandle newResourceHandle(K key, final Callable<? extends R> loader,
      TransferQueue<ResourceKey<K>> queue) {
    try {
      final ResourceKey<K> resourceKey = timeToIdlePolicy.isPresent()
          ? new LinkedResourceKey<K>(queue, Status.IN_FLIGHT, key)
          : new UnlinkedResourceKey<K>(queue, Status.IN_FLIGHT, key);
      R resource = cache.get(resourceKey, new Callable<R>() {
        @Override public R call() throws Exception {
          R resource = loader.call();
          try {
            lifecycle.onCreate(resourceKey.getKey(), resource);
            return resource;
          } catch (Exception e) {
            lifecycle.onRemoval(resourceKey.getKey(), resource);
            throw e;
          }
        }
      });
      return new ResourceHandle(resourceKey, resource);
    } catch (ExecutionException e) {
      throw Throwables.propagate(e.getCause());
    }
  }

  /** Attempts to get the pooled resource with the given key. */
  @Nullable ResourceHandle tryToGetPooledResourceHandle(ResourceKey<K> resourceKey) {
    R resource = cache.getIfPresent(resourceKey);
    if (resource == null) {
      return null;
    }
    return (resourceKey.getStatus() == Status.IDLE) && resourceKey.goFromIdleToInFlight()
        ? new ResourceHandle(resourceKey, resource)
        : null;
  }

  @Override
  public void invalidate(Object key) {
    checkNotNull(key);

    List<ResourceKey<K>> resourceKeys = Lists.newArrayList();
    for (ResourceKey<K> resourceKey : cache.asMap().keySet()) {
      if (resourceKey.getKey().equals(key)) {
        resourceKeys.add(resourceKey);
      }
    }
    cache.invalidateAll(resourceKeys);
  }

  @Override
  public void invalidateAll() {
    cache.invalidateAll();
  }

  @Override
  public long size() {
    return cache.size();
  }

  @Override
  public void cleanUp() {
    if (timeToIdlePolicy.isPresent()) {
      timeToIdlePolicy.get().cleanUp();
    }
    cache.cleanUp();
  }

  @Override
  public CacheStats stats() {
    return cache.stats();
  }

  static final class LoadingTransferPool<K, R> extends TransferPool<K, R>
      implements LoadingMultiwayPool<K, R> {
    final ResourceLoader<K, R> loader;

    LoadingTransferPool(MultiwayPoolBuilder<? super K, ? super R> builder,
        ResourceLoader<K, R> loader) {
      super(builder);
      this.loader = loader;
    }

    @Override
    public Handle<R> borrow(K key) {
      return borrow(key, 0L, TimeUnit.NANOSECONDS);
    }

    @Override
    public Handle<R> borrow(final K key, long timeout, TimeUnit unit) {
      Callable<R> callable = new Callable<R>() {
        @Override public R call() throws Exception {
          return loader.load(key);
        }
      };
      return borrow(key, callable, timeout, unit);
    }
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
    public void close() {
      release();
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
      } catch (Exception e) {
        cache.invalidate(resourceKey);
        throw e;
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
      if (timeToIdlePolicy.isPresent()) {
        timeToIdlePolicy.get().add(resourceKey);
        if (resourceKey.getStatus() != Status.IDLE) {
          timeToIdlePolicy.get().invalidate(resourceKey);
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
      if (timeToIdlePolicy.isPresent()) {
        timeToIdlePolicy.get().invalidate(resourceKey);
      }
      lifecycle.onRemoval(resourceKey.getKey(), resource);
    }

    /** Discards the resource after becoming dead from the retired state. */
    void discardFromRetired(ResourceKey<K> resourceKey, R resource) {
      resourceKey.removeFromTransferQueue();
      lifecycle.onRemoval(resourceKey.getKey(), resource);
    }
  }

  /** An eviction listener for the idle resources in the pool. */
  final class IdleEvictionListener implements EvictionListener<K> {

    /**
     * Atomically transitions the resource to a state where it can no longer be used. If the
     * resource is idle then it is immediately discarded by invalidating it in the primary cache.
     */
    @Override
    public void onEviction(ResourceKey<K> resourceKey) {
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
}

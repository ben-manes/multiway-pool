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

import java.util.Map.Entry;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TransferQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Logger;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.NotThreadSafe;
import javax.annotation.concurrent.ThreadSafe;

import com.google.common.base.Supplier;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.common.util.concurrent.Atomics;

import static com.github.benmanes.multiway.MultiwayPool.CleanUpStatus.IDLE;
import static com.github.benmanes.multiway.MultiwayPool.CleanUpStatus.PROCESSING;
import static com.github.benmanes.multiway.MultiwayPool.CleanUpStatus.REQUIRED;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

/**
 * A concurrent object pool that supports pooling multiple resources that are associated with a
 * single key. A resource is borrowed from the pool, used exclusively, and released back for reuse
 * by another client. This implementation can optionally be bounded by a maximum size, time-to-live,
 * or time-to-idle policies.
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
 * based policy will evict by either a time-to-idle or time-to-live policy. When a resource is
 * evicted the resource should be cleaned up, e.g. closing a network socket, by using an
 * {@link EvictionListener}.
 *
 * @author Ben Manes (ben.manes@gmail.com)
 */
@ThreadSafe
public final class MultiwayPool<K, V> {

  /*
   * An object pool must be optimized around resources regularly being checked-out and returned by
   * multiple threads. A naive implementation that guards the pool with a single lock will suffer
   * contention by the frequent access if resources are used in short bursts.
   *
   * The basic strategy denormalizes the resources into a flattened cache, which provides the
   * eviction policy. The resources are organized into pools as a view layered above the cache,
   * with each pool represented as a collection of cache keys. These pools are implemented as
   * transfer queues, which utilize elimination to reduce contention. A thread returning a resource
   * to the pool will first attempting to exchange it with a thread checking one out, falling back
   * to storing it in the queue if a transfer is unsuccessful.
   *
   * The removal of empty pools is performed lazily as a clean-up process. A pool may become empty
   * when the key is no longer relevant, e.g. representing a file that was deleted. The removal of
   * the empty queues has a benign race conditions, where the queue is discarded at the same instant
   * that a resource is added to it. This will result in the resource being evicted as it is only
   * weakly referenced by the cache.
   */

  static final Logger log = Logger.getLogger(MultiwayPool.class.getName());

  final EvictionListener<? super K, ? super V> evictionListener;
  final AtomicReference<CleanUpStatus> cleanUpStatus;
  final LoadingCache<K, TransferPool<K>> pools;
  final LoadingCache<PoolKey<K>, V> cache;
  final AtomicLong generator;
  final Lock cleanupLock;

  MultiwayPool(CacheBuilder<Object, Object> cacheBuilder,
      EvictionListener<? super K, ? super V> evictionListener, CacheLoader<K, V> loader) {
    this.cleanUpStatus = Atomics.newReference(IDLE);
    this.cache = makeCache(cacheBuilder, loader);
    this.evictionListener = evictionListener;
    this.cleanupLock = new ReentrantLock();
    this.generator = new AtomicLong();
    this.pools = makePools();
  }

  public static Builder<Object, Object> newBuilder() {
    return new Builder<Object, Object>();
  }

  private static <K> LoadingCache<K, TransferPool<K>> makePools() {
    return CacheBuilder.newBuilder().build(new CacheLoader<K, TransferPool<K>>() {
      @Override public TransferPool<K> load(K key) throws Exception {
        return new TransferPool<K>();
      }
    });
  }

  private LoadingCache<PoolKey<K>, V> makeCache(CacheBuilder<Object, Object> cacheBuilder,
      final CacheLoader<K, V> loader) {
    return cacheBuilder.weakKeys().removalListener(new CacheRemovalListener()).build(
        new CacheLoader<PoolKey<K>, V>() {
          @Override public V load(PoolKey<K> poolKey) throws Exception {
            return loader.load(poolKey.key);
          }
        });
  }

  private final class CacheRemovalListener implements RemovalListener<PoolKey<K>, V> {
    @Override public void onRemoval(RemovalNotification<PoolKey<K>, V> notification) {
      PoolKey<K> poolKey = notification.getKey();
      poolKey.alive = false;

      TransferPool<K> pool = pools.getIfPresent(poolKey.key);
      if (pool != null) {
        pool.queue.remove(poolKey);
      }
      if (pool.inflight.get() == 0) {
        cleanUpStatus.set(REQUIRED);
      }

      if (!poolKey.inflight) {
        evictionListener.onEviction(poolKey.key, notification.getValue());
      }
    }
  }

  /**
   * Retrieves a resource from the pool, creating it if necessary. The resource must be returned to
   * the pool using {@link Handle#release()}.
   *
   * @param key the key to qualify the type of resource to retrieve
   * @return a handle to the resource
   */
  public Handle<V> borrow(K key) {
    TransferPool<K> pool = pools.getUnchecked(key);
    PoolKey<K> poolKey = pool.queue.poll();

    if (poolKey == null) {
      poolKey = new PoolKey<K>(key, generator.incrementAndGet());
    }
    pool.inflight.incrementAndGet();
    V value = cache.getUnchecked(poolKey);
    return new ObjectHandle(poolKey, value);
  }

  private void tryToCleanUp() {
    if ((cleanUpStatus.get() != REQUIRED)) {
      return;
    }
    if (cleanupLock.tryLock()) {
      try {
        cleanUpStatus.set(PROCESSING);
      } finally {
        cleanUpStatus.compareAndSet(PROCESSING, IDLE);
        cleanupLock.unlock();
      }
    }
  }

  @GuardedBy("cleanupLock")
  private void cleanUp() {
    for (Entry<K, TransferPool<K>> entry : pools.asMap().entrySet()) {
      TransferPool<?> pool = entry.getValue();
      if (pool.queue.isEmpty() && (pool.inflight.get() == 0)) {
        pools.asMap().remove(entry.getKey(), pool);
      }
    }
  }

  enum CleanUpStatus {
    IDLE, REQUIRED, PROCESSING
  }

  static final class TransferPool<K> {
    final TransferQueue<PoolKey<K>> queue;
    final AtomicInteger inflight;

    TransferPool() {
      inflight = new AtomicInteger();
      queue = new LinkedTransferQueue<PoolKey<K>>();
    }
  }

  enum NullEvictionListener implements EvictionListener<Object, Object> {
    INSTANCE;

    @Override public void onEviction(Object key, Object value) {}
  }

  @NotThreadSafe
  public interface Handle<V> extends Supplier<V> {
    void release();
  }

  final class ObjectHandle implements Handle<V> {
    private final PoolKey<K> poolKey;
    private V value;

    ObjectHandle(PoolKey<K> poolKey, V value) {
      this.poolKey = poolKey;
      this.value = value;
    }

    @Override
    public V get() {
      checkState(value != null, "Stale handle to key %s", poolKey.key);
      return value;
    }

    @Override
    public void release() {
      poolKey.inflight = false;
      boolean alive = poolKey.alive;

      TransferPool<K> pool = pools.getIfPresent(poolKey.key);
      if (pool != null) {
        pool.inflight.decrementAndGet();
        if (alive) {
          pool.queue.add(poolKey);
        }
      }
      value = null;

      if (!poolKey.alive) {
        evictionListener.onEviction(poolKey.key, value);
      }
      tryToCleanUp();
    }

    @Override
    protected void finalize() {
      if (value != null) {
        String msg = String.format("Handle for %s -> %s was not properly released",
            poolKey.key.getClass().getName(), value.getClass().getName());
        log.warning(msg);
        release();
      }
    }
  }

  public static final class Builder<K, V> {
    private final CacheBuilder<Object, Object> cacheBuilder;
    private EvictionListener<? super K, ? super V> evictionListener;

    Builder() {
      this.cacheBuilder = CacheBuilder.newBuilder();
      this.evictionListener = NullEvictionListener.INSTANCE;
    }

    public Builder<K, V> maximumSize(long size) {
      cacheBuilder.maximumSize(size);
      return this;
    }

    public Builder<K, V> expireAfterWrite(long duration, TimeUnit unit) {
      cacheBuilder.expireAfterWrite(duration, unit);
      return this;
    }

    public Builder<K, V> expireAfterAccess(long duration, TimeUnit unit) {
      cacheBuilder.expireAfterAccess(duration, unit);
      return this;
    }

    public <K1 extends K, V1 extends V> Builder<K1, V1> evictionListener(
        EvictionListener<? super K1, ? super V1> evictionListener) {
      checkState(this.evictionListener == null);

      @SuppressWarnings("unchecked")
      Builder<K1, V1> self = (Builder<K1, V1>) this;
      self.evictionListener = checkNotNull(evictionListener);
      return self;
    }

    public <K1 extends K, V1 extends V> MultiwayPool<K1, V1> build(CacheLoader<K1, V1> loader) {
      return new MultiwayPool<K1, V1>(cacheBuilder, evictionListener, loader);
    }
  }
}

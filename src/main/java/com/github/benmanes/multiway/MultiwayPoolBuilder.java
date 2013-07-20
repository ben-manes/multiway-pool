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

import java.util.concurrent.TimeUnit;

import com.github.benmanes.multiway.TransferPool.LoadingTransferPool;
import com.google.common.base.Objects;
import com.google.common.base.Ticker;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheStats;
import com.google.common.cache.Weigher;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

/**
 * A MultiwayPoolBuilder of {@link MultiwayPool} instances with support for least-recently-used
 * eviction and time-based expiration of resources. A notification is made when a resource is
 * created, borrowed, released, or removed. By default instances will not perform any type of
 * eviction.
 * <p>
 * Usage example:
 * <pre>   {@code
 *   MultiwayPool<File, RandomAccessFile> files = MultiwayPoolBuilder.newBuilder()
 *       .expireAfterWrite(10, TimeUnit.MINUTES)
 *       .maximumSize(100)
 *       .build(
 *           new ResourceLoader<File, RandomAccessFile>() {
 *             public RandomAccessFile load(File file) throws AnyException {
 *               return new RandomAccessFile(file);
 *             }
 *           });
 * }</pre>
 *
 * @author Ben Manes (ben.manes@gmail.com)
 */
public final class MultiwayPoolBuilder<K, R> {
  static final ResourceLifecycle<Object, Object> DISCARDING_LIFECYCLE =
      new ResourceLifecycle<Object, Object>() {};
  static final int DEFAULT_CONCURRENCY_LEVEL = 4;
  static final int UNSET_INT = -1;

  boolean recordStats;

  long maximumSize = UNSET_INT;
  long maximumWeight = UNSET_INT;
  Weigher<? super K, ? super R> weigher;

  Ticker ticker;
  long expireAfterWriteNanos = UNSET_INT;
  long expireAfterAccessNanos = UNSET_INT;

  int concurrencyLevel = UNSET_INT;
  ResourceLifecycle<? super K, ? super R> lifecycle;

  MultiwayPoolBuilder() {}

  Ticker getTicker() {
    return Objects.firstNonNull(ticker, Ticker.systemTicker());
  }

  @SuppressWarnings("unchecked")
  ResourceLifecycle<? super K, ? super R> getResourceLifecycle() {
    return (ResourceLifecycle<? super K, ? super R>) Objects.firstNonNull(
        lifecycle, DISCARDING_LIFECYCLE);
  }

  int getConcurrencyLevel() {
    return (concurrencyLevel == UNSET_INT) ? DEFAULT_CONCURRENCY_LEVEL : concurrencyLevel;
  }

  /** Constructs a new builder with no automatic eviction of any kind. */
  public static MultiwayPoolBuilder<Object, Object> newBuilder() {
    return new MultiwayPoolBuilder<Object, Object>();
  }

  /**
   * Guides the allowed concurrency among update operations. Used as a hint for internal sizing.
   * <p>
   * Defaults to 4.
   *
   * @throws IllegalArgumentException if {@code concurrencyLevel} is not positive
   * @throws IllegalStateException if a concurrency level was already set
   */
  public MultiwayPoolBuilder<K, R> concurrencyLevel(int concurrencyLevel) {
    checkState(this.concurrencyLevel == UNSET_INT, "concurrency level was already set to %s",
        this.concurrencyLevel);
    checkArgument(concurrencyLevel > 0);
    this.concurrencyLevel = concurrencyLevel;
    return this;
  }

  /**
   * Specifies the maximum number of resources the pool may contain, regardless of the category
   * it is associated with. Note that the pool <b>may evict a resource before this limit is
   * exceeded</b>. As the pool size grows close to the maximum, the pool evicts entries that are
   * less likely to be used again.
   *
   * @param size the maximum size of the cache
   * @throws IllegalArgumentException if {@code size} is negative
   */
  public MultiwayPoolBuilder<K, R> maximumSize(long size) {
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
  public MultiwayPoolBuilder<K, R> maximumWeight(long weight) {
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
  public <K1 extends K, R1 extends R> MultiwayPoolBuilder<K1, R1> weigher(
      Weigher<? super K1, ? super R1> weigher) {
    checkState(this.weigher == null);
    checkState(this.maximumSize == UNSET_INT, "weigher can not be combined with maximum size");

    // safely limiting the kinds of caches this can produce
    @SuppressWarnings("unchecked")
    MultiwayPoolBuilder<K1, R1> self = (MultiwayPoolBuilder<K1, R1>) this;
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
  public MultiwayPoolBuilder<K, R> expireAfterWrite(long duration, TimeUnit unit) {
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
  public MultiwayPoolBuilder<K, R> expireAfterAccess(long duration, TimeUnit unit) {
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
  public MultiwayPoolBuilder<K, R> ticker(Ticker ticker) {
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
  public MultiwayPoolBuilder<K, R> recordStats() {
    recordStats = true;
    return this;
  }

  /**
   * Specifies a life cycle listener instance that pools should notify each time a resource is
   * created, borrowed, released, or removed.
   *
   * @param lifecycle the listener used for resource life cycle events
   */
  public <K1 extends K, R1 extends R> MultiwayPoolBuilder<K1, R1> lifecycle(
      ResourceLifecycle<? super K1, ? super R1> lifecycle) {
    checkState(this.lifecycle == null);

    // safely limiting the kinds of caches this can produce
    @SuppressWarnings("unchecked")
    MultiwayPoolBuilder<K1, R1> self = (MultiwayPoolBuilder<K1, R1>) this;
    self.lifecycle = checkNotNull(lifecycle);
    return self;
  }

  /**
   * Builds a multiway pool, which either returns an available resource for a given key or
   * atomically computes or retrieves it using the method supplied {@code Callable}.
   *
   * @return a multiway pool having the requested features
   */
  public <K1 extends K, R1 extends R> MultiwayPool<K1, R1> build() {
    return new TransferPool<K1, R1>(this);
  }

  /**
   * Builds a multiway pool, which either returns an available resource for a given key or
   * atomically computes or retrieves it using the supplied {@link ResourceLoader} or
   * method supplied {@code Callable}.
   *
   * @param loader the resource loader to create new instances
   * @return a multiway pool having the requested features
   */
  public <K1 extends K, R1 extends R> LoadingMultiwayPool<K1, R1> build(
      ResourceLoader<K1, R1> loader) {
    checkNotNull(loader);
    return new LoadingTransferPool<K1, R1>(this, loader);
  }

  @Override
  public String toString() {
    Objects.ToStringHelper s = Objects.toStringHelper(this);
    if (concurrencyLevel != UNSET_INT) {
      s.add("concurrencyLevel", concurrencyLevel);
    }
    if (maximumSize != UNSET_INT) {
      s.add("maximumSize", maximumSize);
    }
    if (maximumWeight != UNSET_INT) {
      s.add("maximumWeight", maximumWeight);
    }
    if (expireAfterWriteNanos != UNSET_INT) {
      s.add("expireAfterWrite", expireAfterWriteNanos + "ns");
    }
    if (expireAfterAccessNanos != UNSET_INT) {
      s.add("expireAfterAccess", expireAfterAccessNanos + "ns");
    }
    if (lifecycle != null) {
      s.addValue("resourceLifecycle");
    }
    return s.toString();
  }
}

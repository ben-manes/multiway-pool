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

import java.util.Deque;
import java.util.List;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReentrantLock;

import com.github.benmanes.multiway.ResourceKey.Status;
import com.github.benmanes.multiway.TransferPool.LoadingTransferPool;
import com.github.benmanes.multiway.TransferPool.ResourceHandle;
import com.google.common.base.Stopwatch;
import com.google.common.cache.Weigher;
import com.google.common.collect.Lists;
import com.google.common.collect.Queues;
import com.google.common.testing.FakeTicker;
import com.google.common.testing.GcFinalization;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static com.jayway.awaitility.Awaitility.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

/**
 * @author ben.manes@gmail.com (Ben Manes)
 */
public final class MultiwayPoolTest {
  private static final Integer KEY_1 = 1;

  private FakeTicker ticker;
  private TestResourceLifecycle lifecycle;
  private LoadingTransferPool<Integer, UUID> multiway;

  @BeforeMethod
  public void beforeMethod() {
    ticker = new FakeTicker();
    lifecycle = new TestResourceLifecycle();
    multiway = makeMultiwayPool(MultiwayPoolBuilder.newBuilder());
  }

  @SuppressWarnings("unchecked")
  LoadingTransferPool<Integer, UUID> makeMultiwayPool(MultiwayPoolBuilder<?, ?> builder) {
    MultiwayPoolBuilder<Integer, UUID> pools = (MultiwayPoolBuilder<Integer, UUID>) builder;
    if (pools.lifecycle == null) {
      pools.lifecycle(lifecycle);
    }
    return (LoadingTransferPool<Integer, UUID>) pools.build(new TestResourceLoader());
  }

  @Test
  public void borrow_concurrent() throws Exception {
    final int numThreads = 10;
    final int iterations = 100;

    ConcurrentTestHarness.timeTasks(numThreads, new Runnable() {
      @Override public void run() {
        for (int i = 0; i < iterations; i++) {
          Handle<UUID> handle = multiway.borrow(KEY_1);
          yield();
          handle.release();
          yield();
        }
      }
    });
    multiway.cleanUp();
    int cycles = numThreads * iterations;
    int size = (int) multiway.size();

    assertThat(lifecycle.created(), is(size));
    assertThat(lifecycle.borrows(), is(cycles));
    assertThat(lifecycle.releases(), is(cycles));
    assertThat(multiway.transferQueues.get(KEY_1).size(), is(size));
  }

  @Test
  public void borrow_sameInstance() {
    UUID expected = getAndRelease(KEY_1);
    UUID actual = getAndRelease(KEY_1);
    assertThat(expected, is(actual));
    assertThat(lifecycle.borrows(), is(2));
    assertThat(lifecycle.releases(), is(2));
    assertThat(lifecycle.removals(), is(0));
  }

  @Test(expectedExceptions = IllegalStateException.class)
  public void borrow_badHandle() {
    Handle<UUID> handle = multiway.borrow(KEY_1);
    handle.release();
    handle.get();
  }

  @Test
  public void borrow_fromTransfer() throws Exception {
    Stopwatch stopwatch = new Stopwatch().start();
    final AtomicBoolean start = new AtomicBoolean();
    final AtomicBoolean done = new AtomicBoolean();
    new Thread() {
      @Override public void run() {
        start.set(true);
        Handle<UUID> handle = multiway.borrow(KEY_1, 1, TimeUnit.NANOSECONDS);
        handle.release(1, TimeUnit.MINUTES);
        done.set(true);
      }
    }.start();

    await().untilTrue(start);
    assertThat(done.get(), is(false));
    Handle<?> handle = multiway.borrow(KEY_1);
    await().untilTrue(done);
    handle.release();

    assertThat(stopwatch.elapsed(TimeUnit.MINUTES), is(0L));
  }

  @Test
  public void borrow_callable() {
    MultiwayPool<Integer, UUID> pool = MultiwayPoolBuilder.newBuilder().build();

    final UUID expected = UUID.randomUUID();
    Handle<UUID> handle = pool.borrow(KEY_1, new Callable<UUID>() {
      @Override public UUID call() throws Exception {
        return expected;
      }
    });
    assertThat(handle.get(), is(expected));
    handle.release();
    pool.invalidateAll();
  }

  @Test
  public void evict_immediately() {
    multiway = makeMultiwayPool(MultiwayPoolBuilder.newBuilder().maximumSize(0));
    UUID first = getAndRelease(KEY_1);
    UUID second = getAndRelease(KEY_1);
    assertThat(first, is(not(second)));
    assertThat(lifecycle.removals(), is(2));
    assertThat(multiway.size(), is(0L));
    assertThat(multiway.transferQueues.getIfPresent(KEY_1), is(empty()));
  }

  @Test
  public void evict_whenIdle() {
    getAndRelease(KEY_1);
    ResourceKey<?> resourceKey = getResourceKey();
    assertThat(resourceKey.getStatus(), is(Status.IDLE));

    multiway.invalidateAll();
    assertThat(multiway.size(), is(0L));
    assertThat(resourceKey.getStatus(), is(Status.DEAD));
  }

  @Test
  public void evict_whenInFlight() {
    Handle<?> handle = multiway.borrow(KEY_1);
    ResourceKey<?> resourceKey = getResourceKey();
    assertThat(resourceKey.getStatus(), is(Status.IN_FLIGHT));

    multiway.invalidateAll();
    assertThat(multiway.size(), is(0L));
    assertThat(resourceKey.getStatus(), is(Status.RETIRED));

    handle.release();
    assertThat(resourceKey.getStatus(), is(Status.DEAD));
  }

  @Test
  public void evict_whenRetired() {
    getAndRelease(KEY_1);
    ResourceKey<?> resourceKey = getResourceKey();

    // Simulate transition due to idle cache expiration
    resourceKey.goFromIdleToRetired();

    multiway.invalidateAll();
    assertThat(multiway.size(), is(0L));
    assertThat(lifecycle.borrows(), is(1));
    assertThat(lifecycle.releases(), is(1));
    assertThat(lifecycle.removals(), is(1));
    assertThat(resourceKey.getStatus(), is(Status.DEAD));
  }

  @Test
  public void evict_multipleQueues() {
    for (int i = 0; i < 10; i++) {
      getAndRelease(i);
    }
    assertThat(multiway.size(), is(10L));

    multiway.invalidateAll();
    assertThat(multiway.size(), is(0L));
    assertThat(lifecycle.borrows(), is(10));
    assertThat(lifecycle.releases(), is(10));
    assertThat(lifecycle.removals(), is(10));
  }

  @Test
  public void evict_maximumSize() {
    multiway = makeMultiwayPool(MultiwayPoolBuilder.newBuilder().maximumSize(10));
    List<Handle<?>> handles = Lists.newArrayList();
    for (int i = 0; i < 100; i++) {
      handles.add(multiway.borrow(KEY_1));
    }
    for (Handle<?> handle : handles) {
      handle.release();
    }
    assertThat(multiway.size(), is(10L));
    assertThat(lifecycle.borrows(), is(100));
    assertThat(lifecycle.releases(), is(100));
    assertThat(lifecycle.removals(), is(90));
  }

  @Test
  public void evict_maximumWeight() {
    multiway = makeMultiwayPool(MultiwayPoolBuilder.newBuilder().maximumWeight(10)
        .weigher(new Weigher<Integer, UUID>() {
          @Override public int weigh(Integer key, UUID resource) {
            return 5;
          }
        }));
    List<Handle<?>> handles = Lists.newArrayList();
    for (int i = 0; i < 100; i++) {
      handles.add(multiway.borrow(KEY_1));
    }
    for (Handle<?> handle : handles) {
      handle.release();
    }
    assertThat(multiway.size(), is(2L));
    assertThat(lifecycle.borrows(), is(100));
    assertThat(lifecycle.releases(), is(100));
    assertThat(lifecycle.removals(), is(98));
  }

  @Test
  public void evict_expireAfterAccess() {
    multiway = makeMultiwayPool(MultiwayPoolBuilder.newBuilder()
        .ticker(ticker).expireAfterAccess(1, TimeUnit.MINUTES));
    List<Handle<?>> handles = Lists.newArrayList();
    for (int i = 0; i < 100; i++) {
      handles.add(multiway.borrow(KEY_1));
    }
    for (Handle<?> handle : handles) {
      handle.release();
    }
    ticker.advance(10, TimeUnit.MINUTES);
    multiway.cleanUp();

    assertThat(multiway.size(), is(0L));
    assertThat(lifecycle.borrows(), is(100));
    assertThat(lifecycle.releases(), is(100));
    assertThat(lifecycle.removals(), is(100));
  }

  @Test
  public void evict_expireAfterWrite() {
    multiway = makeMultiwayPool(MultiwayPoolBuilder.newBuilder()
        .ticker(ticker).expireAfterWrite(1, TimeUnit.MINUTES));
    List<Handle<?>> handles = Lists.newArrayList();
    for (int i = 0; i < 100; i++) {
      handles.add(multiway.borrow(KEY_1));
    }
    for (Handle<?> handle : handles) {
      handle.release();
    }
    ticker.advance(10, TimeUnit.MINUTES);
    multiway.cleanUp();

    assertThat(multiway.size(), is(0L));
    assertThat(lifecycle.borrows(), is(100));
    assertThat(lifecycle.releases(), is(100));
    assertThat(lifecycle.removals(), is(100));
  }

  @Test
  public void release_toPool() {
    @SuppressWarnings("rawtypes")
    ResourceHandle handle = (ResourceHandle) multiway.borrow(KEY_1);
    assertThat(multiway.size(), is(1L));
    assertThat(lifecycle.borrows(), is(1));
    assertThat(lifecycle.releases(), is(0));
    assertThat(lifecycle.removals(), is(0));
    assertThat(handle.resourceKey.getStatus(), is(Status.IN_FLIGHT));

    handle.release();
    assertThat(multiway.size(), is(1L));
    assertThat(lifecycle.releases(), is(1));
    assertThat(lifecycle.removals(), is(0));
    assertThat(handle.toString(), handle.resourceKey.getStatus(), is(Status.IDLE));
  }

  @Test
  public void release_andDiscard() {
    @SuppressWarnings("rawtypes")
    ResourceHandle handle = (ResourceHandle) multiway.borrow(KEY_1);

    multiway.invalidateAll();
    assertThat(multiway.size(), is(0L));
    assertThat(lifecycle.borrows(), is(1));
    assertThat(lifecycle.releases(), is(0));
    assertThat(lifecycle.removals(), is(0));
    assertThat(handle.resourceKey.getStatus(), is(Status.RETIRED));

    handle.release();
    assertThat(lifecycle.releases(), is(1));
    assertThat(lifecycle.removals(), is(1));
    assertThat(handle.resourceKey.getStatus(), is(Status.DEAD));
  }

  @Test
  public void release_finalize() {
    multiway.borrow(KEY_1);
    GcFinalization.awaitFullGc();
    multiway.cleanUp();
    await().until(new Callable<Boolean>() {
      @Override public Boolean call() throws Exception {
        return !multiway.transferQueues.getUnchecked(KEY_1).isEmpty();
      }
    });
    ResourceKey<?> resourceKey = getResourceKey();
    assertThat(resourceKey.toString(), resourceKey.getStatus(), is(Status.IDLE));
  }

  @Test
  public void invalidate_whenInFlight() {
    @SuppressWarnings("rawtypes")
    ResourceHandle handle = (ResourceHandle) multiway.borrow(KEY_1);
    assertThat(multiway.size(), is(1L));
    assertThat(lifecycle.borrows(), is(1));
    assertThat(lifecycle.releases(), is(0));
    assertThat(lifecycle.removals(), is(0));
    assertThat(handle.resourceKey.getStatus(), is(Status.IN_FLIGHT));

    handle.invalidate();
    assertThat(multiway.size(), is(0L));
    assertThat(lifecycle.releases(), is(1));
    assertThat(lifecycle.removals(), is(1));
    assertThat(handle.resourceKey.getStatus(), is(Status.DEAD));
  }

  @Test
  public void invalidate_whenRetired() {
    @SuppressWarnings("rawtypes")
    ResourceHandle handle = (ResourceHandle) multiway.borrow(KEY_1);
    multiway.invalidateAll();

    assertThat(multiway.size(), is(0L));
    assertThat(lifecycle.borrows(), is(1));
    assertThat(lifecycle.releases(), is(0));
    assertThat(lifecycle.removals(), is(0));
    assertThat(handle.resourceKey.getStatus(), is(Status.RETIRED));

    handle.invalidate();
    assertThat(multiway.size(), is(0L));
    assertThat(lifecycle.releases(), is(1));
    assertThat(lifecycle.removals(), is(1));
    assertThat(handle.resourceKey.getStatus(), is(Status.DEAD));
  }

  @Test
  public void tryWithResources() {
    try (Handle<UUID> handle = multiway.borrow(KEY_1)) {
      assertThat(getResourceKey().getStatus(), is(Status.IN_FLIGHT));
    }
    assertThat(getResourceKey().getStatus(), is(Status.IDLE));
  }

  @Test
  public void discardPool() {
    Handle<UUID> handle = multiway.borrow(KEY_1);
    GcFinalization.awaitFullGc();
    assertThat(multiway.transferQueues.size(), is(1L));

    handle.release();
    handle = null;
    multiway.invalidateAll();

    GcFinalization.awaitFullGc();
    multiway.transferQueues.cleanUp();
    assertThat(multiway.transferQueues.size(), is(0L));
  }

  @Test
  public void invalidate() {
    for (int i = 0; i < 10; i++) {
      getAndRelease(i);
    }
    multiway.invalidate(5);
    assertThat(multiway.size(), is(9L));
  }

  @Test
  public void invalidateAll() {
    for (int i = 0; i < 10; i++) {
      getAndRelease(i);
    }
    multiway.invalidateAll();
    assertThat(multiway.size(), is(0L));
  }

  @Test
  public void tryToGetPooledResourceHandle_notFound() {
    getAndRelease(KEY_1);
    ResourceKey<Integer> resourceKey = getResourceKey();

    multiway.invalidateAll();
    assertThat(multiway.tryToGetPooledResourceHandle(resourceKey), is(nullValue()));
  }

  @Test
  public void tryToGetPooledResourceHandle_notIdle() {
    getAndRelease(KEY_1);
    ResourceKey<Integer> resourceKey = getResourceKey();

    resourceKey.goFromIdleToRetired();
    assertThat(multiway.tryToGetPooledResourceHandle(resourceKey), is(nullValue()));
  }

  @Test
  public void stats() {
    multiway = makeMultiwayPool(MultiwayPoolBuilder.newBuilder().recordStats());

    getAndRelease(KEY_1);
    getAndRelease(KEY_1);
    assertThat(multiway.stats().hitCount(), is(1L));
    assertThat(multiway.stats().loadSuccessCount(), is(1L));
  }

  @Test
  public void lifecycle_onCreate_fail() {
    final AtomicBoolean onRemovalCalled = new AtomicBoolean();
    multiway = makeMultiwayPool(MultiwayPoolBuilder.newBuilder()
        .lifecycle(new ResourceLifecycle<Integer, UUID>() {
          @Override public void onCreate(Integer key, UUID resource) {
            throw new UnsupportedOperationException();
          }
          @Override public void onRemoval(Integer key, UUID resource) {
            onRemovalCalled.set(true);
          }
        }));
    try {
      getAndRelease(KEY_1);
      Assert.fail();
    } catch (Exception e) {
      assertThat(multiway.cache.size(), is(0L));
      assertThat(onRemovalCalled.get(), is(true));
    }
  }

  @Test
  public void lifecycle_onBorrow_fail() {
    final AtomicBoolean onRemovalCalled = new AtomicBoolean();
    multiway = makeMultiwayPool(MultiwayPoolBuilder.newBuilder()
        .lifecycle(new ResourceLifecycle<Integer, UUID>() {
          @Override public void onBorrow(Integer key, UUID resource) {
            throw new UnsupportedOperationException();
          }
          @Override public void onRemoval(Integer key, UUID resource) {
            onRemovalCalled.set(true);
          }
        }));
    try {
      getAndRelease(KEY_1);
      Assert.fail();
    } catch (Exception e) {
      assertThat(multiway.cache.size(), is(0L));
      assertThat(onRemovalCalled.get(), is(true));
    }
  }

  @Test
  public void lifecycle_onRelease_fail() {
    final AtomicBoolean onRemovalCalled = new AtomicBoolean();
    multiway = makeMultiwayPool(MultiwayPoolBuilder.newBuilder()
        .lifecycle(new ResourceLifecycle<Integer, UUID>() {
          @Override public void onRelease(Integer key, UUID resource) {
            throw new UnsupportedOperationException();
          }
          @Override public void onRemoval(Integer key, UUID resource) {
            onRemovalCalled.set(true);
          }
        }));
    try {
      getAndRelease(KEY_1);
      Assert.fail();
    } catch (Exception e) {
      assertThat(multiway.cache.size(), is(0L));
      assertThat(onRemovalCalled.get(), is(true));
    }
  }

  @Test
  public void lifecycle_onRemove_fail_pool() {
    multiway = makeMultiwayPool(MultiwayPoolBuilder.newBuilder()
        .lifecycle(new ResourceLifecycle<Integer, UUID>() {
          @Override public void onRemoval(Integer key, UUID resource) {
            throw new UnsupportedOperationException();
          }
        }));
    getAndRelease(KEY_1);
    multiway.invalidateAll();
    assertThat(multiway.cache.size(), is(0L));
  }

  @Test
  public void lifecycle_onRemove_fail_handle() {
    multiway = makeMultiwayPool(MultiwayPoolBuilder.newBuilder()
        .lifecycle(new ResourceLifecycle<Integer, UUID>() {
          @Override public void onRemoval(Integer key, UUID resource) {
            throw new UnsupportedOperationException();
          }
        }));
    Handle<UUID> handle = multiway.borrow(KEY_1);
    try {
      handle.invalidate();
      Assert.fail();
    } catch (UnsupportedOperationException e) {
      assertThat(multiway.size(), is(0L));
    }
  }

  @Test
  public void unlockOnCleanup() {
    multiway = makeMultiwayPool(MultiwayPoolBuilder.newBuilder()
        .expireAfterAccess(1, TimeUnit.MINUTES));
    Runnable runner = new Runnable() {
      @Override public void run() {
        throw new IllegalStateException();
      }
    };
    try {
      multiway.timeToIdlePolicy.get().schedule(runner);
      Assert.fail();
    } catch (IllegalStateException e) {
      assertThat(((ReentrantLock) multiway.timeToIdlePolicy.get().idleLock).isLocked(), is(false));
    }
  }

  @Test
  public void concurrent() throws Exception {
    long maxSize = 10;
    multiway = makeMultiwayPool(MultiwayPoolBuilder.newBuilder()
        .expireAfterAccess(100, TimeUnit.NANOSECONDS)
        .maximumSize(maxSize));
    ConcurrentTestHarness.timeTasks(10, new Runnable() {
      final ThreadLocalRandom random = ThreadLocalRandom.current();

      @Override
      public void run() {
        Deque<Handle<?>> handles = Queues.newArrayDeque();
        for (int i = 0; i < 100; i++) {
          execute(handles, i);
          yield();
        }
        for (Handle<?> handle : handles) {
          handle.release();
        }
      }

      void execute(Deque<Handle<?>> handles, int key) {
        if (random.nextBoolean()) {
          Handle<?> handle = multiway.borrow(key);
          handles.add(handle);
        } else if (!handles.isEmpty()) {
          Handle<?> handle = handles.remove();
          handle.release();
        }
      }
    });
    multiway.cleanUp();

    long queued = 0;
    long size = multiway.size();
    for (Queue<?> queue : multiway.transferQueues.asMap().values()) {
      queued += queue.size();
    }

    assertThat(queued, is(size));
    assertThat(size, lessThanOrEqualTo(maxSize));
    assertThat(lifecycle.releases(), is(lifecycle.borrows()));
    assertThat(lifecycle.created(), is((int) size + lifecycle.removals()));
  }

  private UUID getAndRelease(Integer key) {
    Handle<UUID> handle = multiway.borrow(key);
    UUID resource = handle.get();
    handle.release();
    return resource;
  }

  private ResourceKey<Integer> getResourceKey() {
    return multiway.cache.asMap().keySet().iterator().next();
  }

  private void yield() {
    Thread.yield();
    LockSupport.parkNanos(1L);
  }

  private static final class TestResourceLoader implements ResourceLoader<Integer, UUID> {
    @Override public UUID load(Integer key) throws Exception {
      return UUID.randomUUID();
    }
  }

  private static final class TestResourceLifecycle extends ResourceLifecycle<Integer, UUID> {
    final AtomicInteger created = new AtomicInteger();
    final AtomicInteger borrows = new AtomicInteger();
    final AtomicInteger releases = new AtomicInteger();
    final AtomicInteger removals = new AtomicInteger();

    @Override
    public void onCreate(Integer key, UUID resource) {
      created.incrementAndGet();
    }

    @Override
    public void onBorrow(Integer key, UUID resource) {
      borrows.incrementAndGet();
    }

    @Override
    public void onRelease(Integer key, UUID resource) {
      releases.incrementAndGet();
    }

    @Override
    public void onRemoval(Integer key, UUID resource) {
      removals.incrementAndGet();
    }

    public int created() {
      return created.get();
    }

    public int borrows() {
      return borrows.get();
    }

    public int releases() {
      return releases.get();
    }

    public int removals() {
      return removals.get();
    }
  }
}

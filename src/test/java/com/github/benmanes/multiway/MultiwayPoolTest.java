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

import com.github.benmanes.multiway.MultiwayPool.ResourceHandle;
import com.github.benmanes.multiway.ResourceKey.Status;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;
import com.google.common.collect.Queues;
import com.google.common.testing.FakeTicker;
import com.google.common.testing.GcFinalization;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static com.jayway.awaitility.Awaitility.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.not;

/**
 * @author ben.manes@gmail.com (Ben Manes)
 */
public final class MultiwayPoolTest {
  private static final Object KEY_1 = new Object();

  private FakeTicker ticker;
  private TestResourceLifecycle lifecycle;
  private MultiwayPool<Object, UUID> multiway;

  @BeforeMethod
  public void beforeMethod() {
    ticker = new FakeTicker();
    lifecycle = new TestResourceLifecycle();
    multiway = MultiwayPool.newBuilder().build(lifecycle);
  }

  @Test
  public void borrow_concurrent() throws Exception {
    ConcurrentTestHarness.timeTasks(10, new Runnable() {
      @Override public void run() {
        for (int i = 0; i < 100; i++) {
          getAndRelease(KEY_1);
        }
      }
    });
    long size = multiway.cache.size();
    assertThat(lifecycle.borrows(), is(1000));
    assertThat(lifecycle.releases(), is(1000));
    assertThat(multiway.cache.size(), is(size));
    assertThat(multiway.generator.get(), is(size));
    assertThat(multiway.cache.size(), lessThanOrEqualTo(10L));
    assertThat(multiway.transferQueues.get(KEY_1).size(), is((int) size));
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
    getAndRelease(KEY_1);
    long old = MultiwayPool.XFER_WAIT_TIME_MS;
    MultiwayPool.XFER_WAIT_TIME_MS = TimeUnit.MINUTES.toMillis(1);

    Stopwatch stopwatch = new Stopwatch().start();
    final AtomicBoolean start = new AtomicBoolean();
    final AtomicBoolean done = new AtomicBoolean();
    new Thread() {
      @Override public void run() {
        start.set(true);
        getAndRelease(KEY_1);
        done.set(true);
      }
    }.start();

    await().untilTrue(start);
    assertThat(done.get(), is(false));
    Handle<?> handle = multiway.borrow(KEY_1);
    await().untilTrue(done);
    MultiwayPool.XFER_WAIT_TIME_MS = old;
    handle.release();

    assertThat(stopwatch.elapsed(TimeUnit.MINUTES), is(0L));
  }

  @Test
  public void evict_immediately() {
    multiway = MultiwayPool.newBuilder().maximumSize(0).build(lifecycle);
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

    multiway.cache.invalidateAll();
    assertThat(multiway.size(), is(0L));
    assertThat(resourceKey.getStatus(), is(Status.DEAD));
  }

  @Test
  public void evict_whenInFlight() {
    getAndRelease(KEY_1);
    ResourceKey<?> resourceKey = getResourceKey();
    assertThat(resourceKey.getStatus(), is(Status.IDLE));

    multiway.cache.invalidateAll();
    assertThat(multiway.size(), is(0L));
    assertThat(resourceKey.getStatus(), is(Status.DEAD));
  }

  @Test
  public void evict_whenRetired() {
    getAndRelease(KEY_1);
    ResourceKey<?> resourceKey = getResourceKey();

    // Simulate transition due to idle cache expiration
    resourceKey.goFromIdleToRetired();

    multiway.cache.invalidateAll();
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

    multiway.cache.invalidateAll();
    assertThat(multiway.size(), is(0L));
    assertThat(lifecycle.borrows(), is(10));
    assertThat(lifecycle.releases(), is(10));
    assertThat(lifecycle.removals(), is(10));
  }

  @Test
  public void evict_maximumSize() {
    multiway = MultiwayPool.newBuilder().maximumSize(10).build(lifecycle);
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
  public void evict_expireAfterAccess() {
    multiway = MultiwayPool.newBuilder().ticker(ticker)
        .expireAfterAccess(1, TimeUnit.MINUTES)
        .build(lifecycle);
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
    multiway = MultiwayPool.newBuilder().ticker(ticker)
        .expireAfterWrite(1, TimeUnit.MINUTES)
        .build(lifecycle);
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
    assertThat(handle.resourceKey.getStatus(), is(Status.IDLE));
  }

  @Test
  public void release_andDiscard() {
    @SuppressWarnings("rawtypes")
    ResourceHandle handle = (ResourceHandle) multiway.borrow(KEY_1);

    multiway.cache.invalidateAll();
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
    multiway.cache.cleanUp();
    await().until(new Callable<Boolean>() {
      @Override public Boolean call() throws Exception {
        return !multiway.transferQueues.getUnchecked(KEY_1).isEmpty();
      }
    });
    ResourceKey<?> poolKey = multiway.transferQueues.getUnchecked(KEY_1).peek();
    assertThat(poolKey.getStatus(), is(Status.IDLE));
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
    multiway.cache.invalidateAll();

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
  public void discardPool() {
    Handle<UUID> handle = multiway.borrow(KEY_1);
    GcFinalization.awaitFullGc();
    assertThat(multiway.transferQueues.size(), is(1L));

    handle.release();
    handle = null;
    multiway.cache.invalidateAll();

    GcFinalization.awaitFullGc();
    multiway.transferQueues.cleanUp();
    assertThat(multiway.transferQueues.size(), is(0L));
  }

  @Test
  public void concurrent() throws Exception {
    long maxSize = 10;
    multiway = MultiwayPool.newBuilder()
        .expireAfterAccess(100, TimeUnit.NANOSECONDS)
        .maximumSize(maxSize)
        .build(lifecycle);

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

      void yield() {
        Thread.yield();
        LockSupport.parkNanos(1L);
      }
    });
    multiway.cleanUp();

    long queued = 0;
    long size = multiway.cache.size();
    for (Queue<?> queue : multiway.transferQueues.asMap().values()) {
      queued += queue.size();
    }

    assertThat(queued, is(size));
    assertThat(size, lessThanOrEqualTo(maxSize));
    assertThat(lifecycle.releases(), is(lifecycle.borrows()));
    assertThat(multiway.generator.get(), is(greaterThanOrEqualTo(size)));
    assertThat(lifecycle.created(), is((int) size + lifecycle.removals()));
  }

  private UUID getAndRelease(Object key) {
    Handle<UUID> handle = multiway.borrow(key);
    UUID resource = handle.get();
    handle.release();
    return resource;
  }

  private ResourceKey<Object> getResourceKey() {
    return multiway.cache.asMap().keySet().iterator().next();
  }

  private static final class TestResourceLifecycle extends ResourceLifecycle<Object, UUID> {
    final AtomicInteger created = new AtomicInteger();
    final AtomicInteger borrows = new AtomicInteger();
    final AtomicInteger releases = new AtomicInteger();
    final AtomicInteger removals = new AtomicInteger();

    @Override
    public UUID create(Object key) throws Exception {
      created.incrementAndGet();
      return UUID.randomUUID();
    }

    @Override
    public void onBorrow(Object key, UUID resource) {
      borrows.incrementAndGet();
    }

    @Override
    public void onRelease(Object key, UUID resource) {
      releases.incrementAndGet();
    }

    @Override
    public void onRemoval(Object key, UUID resource) {
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

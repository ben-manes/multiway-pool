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

import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;

import com.github.benmanes.multiway.ResourceKey.Status;
import com.google.common.testing.GcFinalization;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static com.jayway.awaitility.Awaitility.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

/**
 * @author ben.manes@gmail.com (Ben Manes)
 */
public final class MultiwayPoolTest {
  private static final Object KEY_1 = new Object();

  private TestResourceLifecycle lifecycle;
  private MultiwayPool<Object, UUID> multiway;

  @BeforeMethod
  public void beforeMethod() {
    lifecycle = new TestResourceLifecycle();
    multiway = MultiwayPool.newBuilder().build(lifecycle);
  }

  @Test
  public void borrow_concurrent() throws Exception {
    ConcurrentTestHarness.timeTasks(10, new Runnable() {
      @Override public void run() {
        for (int i = 0; i < 100; i++) {
          getValue(KEY_1);
        }
      }
    });
  }

  @Test
  public void borrow_sameInstance() {
    UUID expected = getValue(KEY_1);
    UUID actual = getValue(KEY_1);
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
  public void borrow_fromTransfer() {
    // TODO
  }

  @Test
  public void evict_immediately() {
    multiway = MultiwayPool.newBuilder().maximumSize(0).build(lifecycle);
    UUID first = getValue(KEY_1);
    UUID second = getValue(KEY_1);
    assertThat(first, is(not(second)));
    assertThat(lifecycle.removals(), is(2));
    assertThat(multiway.cache.size(), is(0L));
    assertThat(multiway.transferQueues.getIfPresent(KEY_1), is(empty()));
  }

  @Test
  public void evict_whenIdle() {
    // TODO
  }

  @Test
  public void evict_whenInFlight() {
    // TODO
  }

  @Test
  public void evict_maximumSize() {
    // TODO
  }

  @Test
  public void evict_timeToIdle() {
    // TODO
  }

  @Test
  public void evict_timeToLive() {
    // TODO
  }

  @Test
  public void release_toPool() {
    // TODO
  }

  @Test
  public void release_andDiscard() {
    // TODO
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

  private UUID getValue(Object key) {
    Handle<UUID> handle = multiway.borrow(KEY_1);
    UUID value = handle.get();
    handle.release();
    return value;
  }

  private static final class TestResourceLifecycle extends ResourceLifecycle<Object, UUID> {
    final AtomicInteger borrows = new AtomicInteger();
    final AtomicInteger releases = new AtomicInteger();
    final AtomicInteger removals = new AtomicInteger();

    @Override
    UUID create(Object key) throws Exception {
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
    public void onRemoval(Object key, UUID value) {
      removals.incrementAndGet();
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

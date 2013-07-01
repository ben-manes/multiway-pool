package com.github.benmanes.multiway;

import java.util.UUID;

import com.github.benmanes.multiway.MultiwayPool.Handle;
import com.google.common.cache.CacheLoader;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

public final class MultiwayPoolTest {
  private static final Object KEY_1 = new Object();

  private MultiwayPool<Object, UUID> pool;
  private CacheLoader<Object, UUID> loader;

  @BeforeMethod
  public void beforeMethod() {
    loader = new CacheLoader<Object, UUID>() {
      @Override public UUID load(Object key) throws Exception {
        return UUID.randomUUID();
      }
    };
    pool = MultiwayPool.newBuilder().build(loader);
  }

  @Test
  public void roundtrip() {
    UUID expected = getValue(KEY_1);
    UUID actual = getValue(KEY_1);
    assertThat(expected, is(actual));
  }

  @Test(expectedExceptions = IllegalStateException.class)
  public void roundtrip_stale() {
    Handle<UUID> handle = pool.borrow(KEY_1);
    handle.release();
    handle.get();
  }

  @Test
  public void roundtrip_evicted() {
    pool = MultiwayPool.newBuilder().maximumSize(0).build(loader);
    UUID first = getValue(KEY_1);
    UUID second = getValue(KEY_1);
    assertThat(first, is(not(second)));
    assertThat(pool.cache.size(), is(0L));
    assertThat(pool.pools.getIfPresent(KEY_1).queue, is(empty()));
    assertThat(pool.pools.getIfPresent(KEY_1).inflight.get(), is(0));
  }

  private UUID getValue(Object key) {
    Handle<UUID> handle = pool.borrow(KEY_1);
    UUID value = handle.get();
    handle.release();
    return value;
  }
}

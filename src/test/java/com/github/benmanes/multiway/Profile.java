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
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Stopwatch;
import com.twitter.jsr166e.LongAdder;

/**
 * A simple bootstrap for inspecting with an attached profiler.
 *
 * @author Ben Manes (ben.manes@gmail.com)
 */
public final class Profile {
  static final int DISPLAY_DELAY_SEC = 5;

  public static void main(String[] args) throws Exception {
    final MultiwayPool<Integer, Integer> multiway = MultiwayPoolBuilder.newBuilder()
        //.expireAfterAccess(2, TimeUnit.MINUTES)
        .maximumSize(100)
        .build();
    final Callable<Integer> resourceLoader = new Callable<Integer>() {
      @Override public Integer call() throws Exception {
        return Integer.MAX_VALUE;
      }
    };

    final LongAdder calls = new LongAdder();
    Executors.newSingleThreadScheduledExecutor().scheduleWithFixedDelay(new Runnable() {
      final Stopwatch stopwatch = new Stopwatch().start();
      @Override public void run() {
        long count = calls.longValue();
        long rate = count / stopwatch.elapsed(TimeUnit.SECONDS);
        System.out.printf("%s - %,d [%,d / sec]\n", stopwatch, count, rate);
      }
    }, DISPLAY_DELAY_SEC, DISPLAY_DELAY_SEC, TimeUnit.SECONDS);
    ConcurrentTestHarness.timeTasks(25, new Runnable() {
      @Override public void run() {
        Integer id = 1;
        for (;;) {
          Integer resource = multiway.borrow(id, resourceLoader, 100, TimeUnit.MICROSECONDS);
          multiway.release(resource, 100, TimeUnit.MICROSECONDS);
          calls.increment();
        }
      }
    });
  }
}

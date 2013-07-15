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
import java.util.concurrent.locks.LockSupport;

/**
 * A simple bootstrap for inspecting with an attached profiler.
 *
 * @author Ben Manes (ben@addepar.com)
 */
public final class Profile {

  public static void main(String[] args) throws Exception {
    final MultiwayPool<Integer, Integer> multiway = MultiwayPoolBuilder.newBuilder()
        .maximumSize(100)
        .expireAfterAccess(100, TimeUnit.NANOSECONDS)
        .build();
    final Callable<Integer> resourceLoader = new Callable<Integer>() {
      final Integer value = Integer.MAX_VALUE;

      @Override
      public Integer call() throws Exception {
        return value;
      }
    };
    ConcurrentTestHarness.timeTasks(25, new Runnable() {
      @Override
      public void run() {
        Integer id = (int) Thread.currentThread().getId();
        for (;;) {
          try (Handle<Integer> handle = multiway.borrow(id, resourceLoader)) {
            handle.get();
          }
          yield();
        }
      }
      void yield() {
        Thread.yield();
        LockSupport.parkNanos(1L);
      }
    });
  }
}

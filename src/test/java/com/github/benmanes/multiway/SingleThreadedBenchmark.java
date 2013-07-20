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

import com.google.caliper.Benchmark;
import com.google.caliper.runner.CaliperMain;

/**
 * A simple, single threaded benchmark.
 *
 * @author Ben Manes (ben.manes@gmail.com)
 */
public final class SingleThreadedBenchmark extends Benchmark {
  static final int MASK = 15; // power-of-two - 1

  LoadingMultiwayPool<Integer, Integer> multiway;
  Callable<Integer> resourceLoader;

  @Override
  protected void setUp() {
    multiway = MultiwayPoolBuilder.newBuilder()
        .build(new ResourceLoader<Integer, Integer>() {
          @Override public Integer load(Integer key) throws Exception {
            return key;
          }
        });
    resourceLoader = new Callable<Integer>() {
      final Integer value = Integer.MAX_VALUE;

      @Override
      public Integer call() throws Exception {
        return value;
      }
    };
  }

  public int timeBorrowAndRelease(int reps) {
    int dummy = 0;
    for (int i = 0; i < reps; i++) {
      Integer resource = multiway.borrow(i & MASK, resourceLoader);
      multiway.release(resource);
    }
    return dummy;
  }

  public static void main(String[] args) throws Exception {
    CaliperMain.main(SingleThreadedBenchmark.class, args);
  }
}

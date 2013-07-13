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

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.LinkedTransferQueue;

import com.google.caliper.Param;
import com.google.caliper.Runner;
import com.google.caliper.SimpleBenchmark;
import com.google.common.base.Supplier;

/**
 * A simple, single threaded benchmark.
 *
 * @author Ben Manes (ben@addepar.com)
 */
public final class SingleThreadedBenchmark extends SimpleBenchmark {
  static final int MASK = 15; // power-of-two

  LoadingMultiwayPool<Integer, Integer> multiway;
  @Param QueueType queueType;

  @Override
  protected void setUp() {
    multiway = MultiwayPoolBuilder.newBuilder()
        .queueSupplier(queueType)
        .build(new ResourceLoader<Integer, Integer>() {
          @Override public Integer load(Integer key) throws Exception {
            return key;
          }
        });
  }

  public int timeBorrowAndRelease(int reps) {
    int dummy = 0;
    for (int i = 0; i < reps; i++) {
      try (Handle<Integer> handle = multiway.borrow(i & MASK)) {
        dummy += handle.get();
      }
    }
    return dummy;
  }

  public static void main(String[] args) {
    Runner.main(SingleThreadedBenchmark.class, args);
  }

  /** The type of blocking queue to back the pool by. */
  enum QueueType implements Supplier<BlockingQueue<Object>> {
    ABQ() {
      @Override public BlockingQueue<Object> get() {
        return new ArrayBlockingQueue<>(1024);
      }
    },
    LBQ() {
      @Override public BlockingQueue<Object> get() {
        return new LinkedBlockingQueue<>();
      }
    },
    LTQ() {
      @Override public BlockingQueue<Object> get() {
        return new LinkedTransferQueue<Object>();
      }
    },
    LBD() {
      @Override public BlockingQueue<Object> get() {
        return new LinkedBlockingDeque<>();
      }
    };
  }
}

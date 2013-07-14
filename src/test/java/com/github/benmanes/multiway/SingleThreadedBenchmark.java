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

import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.TimeUnit;

import com.google.caliper.Benchmark;
import com.google.caliper.Param;
import com.google.caliper.runner.CaliperMain;
import com.google.common.base.Supplier;
import com.google.common.collect.ForwardingQueue;
import com.google.common.collect.Queues;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * A simple, single threaded benchmark.
 *
 * @author Ben Manes (ben@addepar.com)
 */
public final class SingleThreadedBenchmark extends Benchmark {
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
    CaliperMain.main(SingleThreadedBenchmark.class, args);
  }

  /** The type of blocking queue to back the pool by. */
  enum QueueType implements Supplier<BlockingQueue<Object>> {
    ABQ() {
      @Override public BlockingQueue<Object> get() {
        return new ArrayBlockingQueue<>(1024);
      }
    },
    SAQ() {
      @Override public BlockingQueue<Object> get() {
        return new BlockingQueueAdapter<>(Queues.synchronizedQueue(new ArrayDeque<>(1024)));
      }
    },
    CLQ() {
      @Override public BlockingQueue<Object> get() {
        return new BlockingQueueAdapter<>(new ConcurrentLinkedQueue<>());
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

  /** A non-robust adapter allowing a non-blocking queue to be under a blocking facade. */
  static final class BlockingQueueAdapter<E> extends ForwardingQueue<E>
      implements BlockingQueue<E> {
    final Queue<E> delegate;

    BlockingQueueAdapter(Queue<E> delegate) {
      this.delegate = checkNotNull(delegate);
    }

    @Override
    public void put(E e) throws InterruptedException {
      delegate.add(e);
    }

    @Override
    public boolean offer(E e, long timeout, TimeUnit unit) throws InterruptedException {
      return delegate.offer(e);
    }

    @Override
    public E take() throws InterruptedException {
      return delegate.poll();
    }

    @Override
    public E poll(long timeout, TimeUnit unit) throws InterruptedException {
      return delegate.poll();
    }

    @Override
    public int remainingCapacity() {
      return Integer.MAX_VALUE;
    }

    @Override
    public int drainTo(Collection<? super E> c) {
      return drainTo(c, Integer.MAX_VALUE);
    }

    @Override
    public int drainTo(Collection<? super E> c, int maxElements) {
      E next;
      int drained = 0;
      while (((next = poll()) != null) && (drained < maxElements)) {
        c.add(next);
        drained++;
      }
      return drained;
    }

    @Override
    protected Queue<E> delegate() {
      return delegate;
    }
  }
}

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
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedTransferQueue;

import com.google.caliper.Benchmark;
import com.google.caliper.runner.CaliperMain;

/**
 * A simple, single threaded producer/consumer benchmark.
 *
 * @author Ben Manes (ben.manes@gmail.com)
 */
public final class CollectionBenchmark extends Benchmark {
  static final int MASK = 15; // power-of-two - 1

  Queue<Integer> arrayDeque;
  Queue<Integer> linkedTransferQueue;
  Queue<Integer> concurrentLinkedQueue;
  EliminationStack<Integer> eliminationStack;

  @Override
  protected void setUp() {
    arrayDeque = new ArrayDeque<>();
    eliminationStack = new EliminationStack<>();
    linkedTransferQueue = new LinkedTransferQueue<>();
    concurrentLinkedQueue = new ConcurrentLinkedQueue<>();
  }

  public int timeArrayDeque(int reps) {
    return timeQueue(reps, arrayDeque);
  }

  public int timeLinkedTransferQueue(int reps) {
    return timeQueue(reps, linkedTransferQueue);
  }

  public int timeConcurrentLinkedQueue(int reps) {
    return timeQueue(reps, concurrentLinkedQueue);
  }

  public int timeEliminationStack(int reps) {
    int dummy = 0;
    for (int i = 0; i < reps; i++) {
      eliminationStack.push(i);
      eliminationStack.pop();
      dummy = i;
    }
    return dummy;
  }

  private int timeQueue(int reps, Queue<Integer> queue) {
    int dummy = 0;
    for (int i = 0; i < reps; i++) {
      queue.add(i);
      queue.poll();
      dummy = i;
    }
    return dummy;
  }

  public static void main(String[] args) throws Exception {
    CaliperMain.main(CollectionBenchmark.class, args);
  }
}

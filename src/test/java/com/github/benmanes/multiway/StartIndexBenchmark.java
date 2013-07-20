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

import java.util.concurrent.ThreadLocalRandom;

import com.google.caliper.Benchmark;
import com.google.caliper.runner.CaliperMain;

import static com.github.benmanes.multiway.EliminationStack.ARENA_LENGTH;

/**
 * A benchmark to compare the speed of start index selection strategies.
 *
 * @author Ben Manes (ben.manes@gmail.com)
 */
public final class StartIndexBenchmark extends Benchmark {
  static final int NBITS = (((0xfffffc00 >> ARENA_LENGTH) & 4)
      | ((0x000001f8 >>> ARENA_LENGTH) & 2)
      | ((0xffff00f2 >>> ARENA_LENGTH) & 1));

  public int timeThreadLocalRandom(int reps) {
    int dummy = 0;
    for (int i = 0; i < reps; i++) {
      dummy = Math.abs(ThreadLocalRandom.current().nextInt());
    }
    return dummy;
  }

  public int timeExchanger(long reps) {
    int dummy = 0;
    for (int i = 0; i < reps; i++) {
      int hash = ((i ^ (i >>> 32)) ^ 0x811c9dc5) * 0x01000193;

      int index = hash & ((1 << NBITS) - 1);
      while ((index = hash & ((1 << NBITS) - 1)) > ARENA_LENGTH) {
        // May retry on non-power-2 m
        hash = (hash >>> NBITS) | (hash << (33 - NBITS));
      }
      dummy = index;
    }
    return dummy;
  }

  public int timeOneStepFnv1a(long reps) {
    int dummy = 0;
    for (int i = 0; i < reps; i++) {
      dummy = ((i ^ (i >>> 32)) ^ 0x811c9dc5) * 0x01000193;
    }
    return dummy;
  }

  public static void main(String[] args) throws Exception {
    CaliperMain.main(StartIndexBenchmark.class, args);
  }
}

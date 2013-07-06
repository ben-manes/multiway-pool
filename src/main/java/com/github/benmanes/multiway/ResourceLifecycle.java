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

import javax.annotation.concurrent.ThreadSafe;

/**
 * The life cycle for a resource in the object pool. An instance may be called concurrently by
 * multiple threads to process different resources. An implementation should avoid performing
 * blocking calls or synchronizing on shared state.
 *
 * @author ben.manes@gmail.com (Ben Manes)
 */
@ThreadSafe
public abstract class ResourceLifecycle<K, R> {

  /** Creates a resource corresponding to the {@code key}. */
  public abstract R create(K key) throws Exception;

  /**
   * Notifies that a the resource was borrowed. This is called by the thread requesting the
   * resource, prior to being given the instance.
   */
  public void onBorrow(K key, R resource) {}

  /**
   * Notifies that the resource was released. This is called by the thread when releasing the
   * resource and may be called concurrently with a removal.
   */
  public void onRelease(K key, R resource) {}

  /** Notifies that the resource was removed, e.g. due to eviction. */
  public void onRemoval(K key, R resource) {}
}

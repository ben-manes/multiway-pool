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

import java.util.concurrent.TimeUnit;

import javax.annotation.concurrent.ThreadSafe;

/**
 * A {@link MultiwayPool} that can automatically load resources using a {@link ResourceLoader}.
 *
 * @author Ben Manes (ben.manes@gmail.com)
 */
@ThreadSafe
public interface LoadingMultiwayPool<K, R> extends MultiwayPool<K, R> {

  /**
   * Retrieves a resource from the pool, immediately. If a resource is not available then one is
   * created.
   *
   * @param key the category to qualify the type of resource to retrieve
   * @return a handle to the resource
   */
  Handle<R> borrow(K key);

  /**
   * Retrieves a resource from the pool, waiting up to the specified wait time if necessary for one
   * to become available. If a resource is not available then one is created.
   *
   * @param key the category to qualify the type of resource to retrieve
   * @param timeout how long to wait before giving up and creating the resource
   * @param unit a {@code TimeUnit} determining how to interpret the {@code duration} parameter
   * @return a handle to the resource
   */
  Handle<R> borrow(K key, long timeout, TimeUnit unit);
}

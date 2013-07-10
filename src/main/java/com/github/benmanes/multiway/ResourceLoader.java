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
 * Creates a resource based on a key, for use in populating a {@link LoadingMultiwayPool}.
 *
 * @author ben.manes@gmail.com (Ben Manes)
 */
@ThreadSafe
public interface ResourceLoader<K, R> {

  /**
   * Creates a resource corresponding to the {@code key}.
   *
   * @param key the non-null key whose value should be loaded
   * @return the resource associated with {@code key}; <b>must not be null</b>
   * @throws Exception if unable to create the result
   */
  R load(K key) throws Exception;
}

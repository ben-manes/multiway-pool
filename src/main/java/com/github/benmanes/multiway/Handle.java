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

import javax.annotation.concurrent.NotThreadSafe;

import com.google.common.base.Supplier;

/**
 * A proxy to a resource in the object pool. The resource <b>must</b> be returned to the pool after
 * it has been used by invoking {@link #release()}. The handle is not thread safe and may not be
 * shared across threads. The handle is not implicitly bounded to support nested scopes, e.g. a
 * subsequent call to retrieve a handle will return a new resource even if a handle for that
 * resource type is available at a higher scope. Support for scopes, such as used by database
 * transactions, may be added by a decorator to the {@link MultiwayPool}.
 *
 * In most cases, the following idiom should be used:
 * <pre>   {@code
 *   Handle<RandomAccessFile> handle = files.borrow(new File("db_table"));
 *   try {
 *     // access the resource protected by this handle
 *   } finally {
 *     handle.release();
 *   }
 * }</pre>
 *
 * @author ben.manes@gmail.com (Ben Manes)
 */
@NotThreadSafe
public interface Handle<R> extends Supplier<R> {

  /**
   * Returns the resource to the object pool. If the resource has been evicted by the pool, it
   * is immediately discarded. Otherwise the resource is available to be borrowed from the pool.
   */
  void release();

  /** Returns the resource to the object pool to be immediately discarded. */
  void invalidate();
}

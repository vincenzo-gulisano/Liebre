/*
 * Copyright (C) 2017-2018
 *   Vincenzo Gulisano
 *   Dimitris Palyvos-Giannas
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 * Contact:
 *   Vincenzo Gulisano info@vincenzogulisano.com
 *   Dimitris Palyvos-Giannas palyvos@chalmers.se
 */

package stream.smq.resource;

import java.util.concurrent.atomic.AtomicIntegerArray;

public abstract class AbstractResourceManager implements ResourceManager {
  private static final int FREE = 0;
  private static final int ACQUIRED = 1;
  private final AtomicIntegerArray acquired;
  protected final int size;


  public AbstractResourceManager(int size) {
    if (size <= 0) {
      throw new IllegalArgumentException("size");
    }
    this.acquired = new AtomicIntegerArray(size);
    this.size = size;
    initArray();
  }

  private void initArray() {
    for (int i = 0; i < acquired.length(); i++) {
      acquired.set(i, FREE);
    }
  }

  @Override
  public void acquire(int index) throws InterruptedException {
    if (acquired.compareAndSet(index, FREE, ACQUIRED)) {
      doAcquire();
    }
  }

  @Override
  public void release(int index) {
    // If acquired status for this queue set from true to false
    if (acquired.compareAndSet(index, ACQUIRED, FREE)) {
      doRelease();
    }
  }

  protected abstract void doAcquire() throws InterruptedException;
  protected abstract void doRelease();
}

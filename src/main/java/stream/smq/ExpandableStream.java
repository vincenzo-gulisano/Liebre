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

package stream.smq;

import common.tuple.Tuple;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import stream.Stream;
import stream.StreamDecorator;

public class ExpandableStream<T extends Tuple> extends StreamDecorator<T> {

  final Lock lock = new ReentrantLock(false);
  private final Queue<T> buffer = new ConcurrentLinkedQueue<>();
  private boolean isFull;

  public ExpandableStream(Stream<T> stream) {
    super(stream);
  }

  @Override
  public boolean offer(T tuple) {
    lock.lock();
    try {
      fullCopyBuffer();
      if (!super.offer(tuple)) {
        buffer.offer(tuple);
        return false;
      }
      return true;
    } finally {
      lock.unlock();
    }
  }

  @Override
  public T poll() {
    lock.lock();
    try {
      fullCopyBuffer();
      T value = super.poll();
      return value;
    } finally {
      lock.unlock();
    }
  }

  @Override
  public T peek() {
    lock.lock();
    try {
      T queuePeek = super.peek();
      T value = queuePeek != null ? queuePeek : buffer.peek();
      return value;
    } finally {
      lock.unlock();
    }
  }

  private final void fullCopyBuffer() {
    lock.lock();
    try {
      while (!buffer.isEmpty()) {
        T value = buffer.peek();
        if (!super.offer(value)) {
          isFull = true;
          return;
        }
        buffer.remove();
      }
      isFull = false;
    } finally {
      lock.unlock();
    }
  }

  @Override
  public int size() {
    lock.lock();
    try {
      return super.size() + buffer.size();
    } finally {
      lock.unlock();
    }
  }

  @Override
  public boolean isFull() {
    lock.lock();
    try {
      return isFull;
    } finally {
      lock.unlock();
    }
  }
}

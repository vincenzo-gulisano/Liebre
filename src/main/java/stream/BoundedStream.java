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

package stream;

import common.StreamConsumer;
import common.StreamProducer;
import common.tuple.Tuple;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;

public class BoundedStream<T extends Tuple> implements Stream<T> {

  private final String id;
  private final int index;
  private final StreamProducer<T> source;
  private final StreamConsumer<T> destination;
  private BlockingQueue<T> stream;
  private volatile boolean enabled;

  public BoundedStream(String id, int index, StreamProducer<T> source, StreamConsumer<T> destination, int capacity) {
    this.id = id;
    this.index = index;
    this.stream = new ArrayBlockingQueue<T>(capacity);
    this.source = source;
    this.destination = destination;
  }

  @Override
  public void addTuple(T tuple) {
    if (!isEnabled()) {
      return;
    }
    try {
      stream.put(tuple);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      disable();
    }
  }

  @Override
  public boolean offer(T tuple) {
    if (!isEnabled()) {
      return false;
    }
    boolean offered = stream.offer(tuple);
    return offered;
  }

  @Override
  public T getNextTuple() {
    if (!isEnabled()) {
      return null;
    }
    T nextTuple = null;
    try {
      nextTuple = stream.take();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      disable();
    }
    return nextTuple;
  }

  @Override
  public T poll() {
    if (!isEnabled()) {
      return null;
    }
    T tuple = stream.poll();
    return tuple;
  }

  @Override
  public void enable() {
    this.enabled = true;
  }

  @Override
  public boolean isEnabled() {
    return enabled;
  }

  @Override
  public void disable() {
    this.enabled = false;
  }

  @Override
  public T peek() {
    return isEnabled() ? stream.peek() : null;
  }

  @Override
  public int size() {
    return stream.size();
  }

  @Override
  public int remainingCapacity() {
    return stream.remainingCapacity();
  }

  @Override
  public boolean isFull() {
    throw new UnsupportedOperationException();
  }

  public StreamProducer<T> getSource() {
    return source;
  }

  public StreamConsumer<T> getDestination() {
    return destination;
  }

  @Override
  public String getId() {
    return this.id;
  }

  @Override
  public int getIndex() {
    return index;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    BoundedStream<?> that = (BoundedStream<?>) o;

    return new EqualsBuilder()
        .append(index, that.index)
        .append(id, that.id)
        .append(source, that.source)
        .append(destination, that.destination)
        .isEquals();
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder(17, 37)
        .append(id)
        .append(index)
        .append(source)
        .append(destination)
        .toHashCode();
  }

  @Override
  public String toString() {
    return new ToStringBuilder(this)
        .append("id", id)
        .append("index", index)
        .append("source", source)
        .append("destination", destination)
        .append("enabled", enabled)
        .toString();
  }
}
